# typed: true
# frozen_string_literal: true

module Durababble
  StepContext = Data.define(:workflow_id, :step_index, :attempt_number, :idempotency_key, :heartbeat)

  Heartbeat = Data.define(:cursor, :recorder) do
    #: (?untyped) -> untyped
    def record(cursor = self.cursor)
      recorder.call(cursor)
    end

    alias_method :heartbeat, :record
  end

  class WorkflowExecution
    #: untyped
    attr_reader :step_context

    #: (store: untyped, workflow_id: untyped, worker_id: untyped, lease_seconds: untyped, steps: untyped, ?crash_after: untyped) -> void
    def initialize(store:, workflow_id:, worker_id:, lease_seconds:, steps:, crash_after: nil)
      @store = store
      @workflow_id = workflow_id
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @steps_by_position = steps.to_h { |step| [step.fetch("position").to_i, step] }
      @crash_after = crash_after
      @mutex = Mutex.new
      @store_mutex = Mutex.new
      @position = 0
      @async_futures = []
    end

    #: () -> untyped
    def step_context
      Thread.current[step_context_key]
    end

    #: (untyped) { -> untyped } -> untyped
    def async(workflow, &block)
      position = next_position
      future = AsyncFuture.new(execution: self, workflow:, position:, block:)
      @mutex.synchronize { @async_futures << future }
      future
    end

    #: (Integer) { -> untyped } -> untyped
    def run_async_position(position, &block)
      key = async_position_key
      previous = Thread.current[key]
      reservation = { position:, consumed: false }
      Thread.current[key] = reservation
      result = block.call
      unless reservation.fetch(:consumed)
        raise AsyncBoundaryError, "async block at position #{position} must call exactly one durable workflow step"
      end

      result
    ensure
      Thread.current[key] = previous
    end

    #: (Integer) -> bool
    def cancel_async_position(position)
      @store_mutex.synchronize do
        step = @mutex.synchronize { @steps_by_position[position] }
        return true if step&.fetch("status", nil) == "canceled"
        return false if step && step.fetch("status") != "canceled"

        assert_workflow_lease!
        return false unless @store.record_step_canceled(workflow_id: @workflow_id, position:, name: "async")

        @mutex.synchronize do
          @steps_by_position[position] = {
            "workflow_id" => @workflow_id,
            "position" => position,
            "name" => "async",
            "status" => "canceled",
          }
        end
      end

      true
    end

    #: () -> void
    def assert_async_futures_settled!
      futures = @mutex.synchronize { @async_futures.dup }
      futures.each(&:wait_if_started)
      unobserved = futures.reject(&:observed?)
      return if unobserved.empty?

      positions = unobserved.map(&:position).join(", ")
      raise AsyncBoundaryError, "workflow completed with unawaited async step positions: #{positions}"
    end

    #: (untyped, method_name: untyped, args: untyped, kwargs: untyped) { -> untyped } -> untyped
    def call_step(instance, method_name:, args:, kwargs:, &block)
      position = consume_position
      step = instance.class.step_definition(method_name)

      known_step = @mutex.synchronize { @steps_by_position[position] }
      case known_step&.fetch("status")
      when "completed"
        validate_completed_step_shape!(known_step, step:, position:)
        return known_step.fetch("result")
      when "canceled"
        raise AsyncCanceled, "async step at position #{position} was canceled"
      end

      @store_mutex.synchronize do
        @store.record_step_started(workflow_id: @workflow_id, position:, name: step.name)
        @mutex.synchronize do
          @steps_by_position[position] = {
            "workflow_id" => @workflow_id,
            "position" => position,
            "name" => step.name,
            "status" => "running",
          }
        end
      end
      crash!(:step_started)
      heartbeat = build_heartbeat(position)
      attempt_number = @store_mutex.synchronize do
        @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == position }
      end
      previous_step_context = Thread.current[step_context_key]
      Thread.current[step_context_key] = StepContext.new(
        workflow_id: @workflow_id,
        step_index: position,
        attempt_number:,
        idempotency_key: "durababble:v1:workflow:#{@workflow_id}:step:#{position}",
        heartbeat:,
      )

      output = block.call
      if output.is_a?(WaitRequest)
        @store_mutex.synchronize do
          assert_workflow_lease!
          @store.record_wait(workflow_id: @workflow_id, position:, name: step.name, wait_request: output)
          @mutex.synchronize do
            @steps_by_position[position] = {
              "workflow_id" => @workflow_id,
              "position" => position,
              "name" => step.name,
              "status" => "waiting",
              "result" => output.context,
            }
          end
        end
        crash!(:wait_recorded)
        raise WorkflowSuspended
      end

      @store_mutex.synchronize do
        assert_workflow_lease!
        @store.record_step_completed(workflow_id: @workflow_id, position:, result: output)
        @mutex.synchronize do
          @steps_by_position[position] = {
            "workflow_id" => @workflow_id,
            "position" => position,
            "name" => step.name,
            "status" => "completed",
            "result" => output,
          }
        end
      end
      crash!(:step_completed)
      output
    rescue StandardError => e
      raise if e.is_a?(InjectedCrash) || e.is_a?(LeaseConflict) || e.is_a?(WorkflowSuspended) || e.is_a?(NonDeterminismError) || e.is_a?(AsyncBoundaryError) || e.is_a?(AsyncCanceled)

      message = "#{e.class}: #{e.message}"
      attempt_number = nil
      @store_mutex.synchronize do
        assert_workflow_lease!
        @store.record_step_failed(workflow_id: @workflow_id, position:, error: message)
        @mutex.synchronize do
          @steps_by_position[position] = {
            "workflow_id" => @workflow_id,
            "position" => position,
            "name" => step.name,
            "status" => "failed",
            "error" => message,
          }
        end
        attempt_number = @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == position }
      end
      if step.retry_policy.retryable?(e, attempt_number:)
        delay = step.retry_policy.delay_for_attempt(attempt_number)
        @store_mutex.synchronize do
          @store.schedule_workflow_retry(workflow_id: @workflow_id, worker_id: @worker_id, run_at: retry_run_at(delay))
        end
        raise StepRetryScheduled
      end
      raise
    ensure
      Thread.current[step_context_key] = previous_step_context
    end

    #: () -> void
    def validate_replay_complete!
      extra_steps = @mutex.synchronize do
        @steps_by_position
          .select { |position, step| position >= @position && step.fetch("status") == "completed" }
          .sort_by { |position, _step| position }
      end
      return if extra_steps.empty?

      rendered = extra_steps
        .map { |position, step| "#{position}:#{step.fetch("name")}" }
        .join(", ")
      raise NonDeterminismError, "workflow #{@workflow_id} replay completed without consuming durable step history: #{rendered}"
    end

    private

    #: () -> Integer
    def next_position
      @mutex.synchronize do
        position = @position
        @position += 1
        position
      end
    end

    #: () -> Integer
    def consume_position
      reservation = Thread.current[async_position_key]
      return next_position unless reservation

      raise AsyncBoundaryError, "async block at position #{reservation.fetch(:position)} called more than one durable workflow step" if reservation.fetch(:consumed)

      reservation[:consumed] = true
      reservation.fetch(:position)
    end

    #: () -> Symbol
    def step_context_key
      :"durababble_step_context_#{object_id}"
    end

    #: () -> Symbol
    def async_position_key
      :"durababble_async_position_#{object_id}"
    end

    #: (untyped, step: untyped, position: untyped) -> void
    def validate_completed_step_shape!(completed_step, step:, position:)
      persisted_name = completed_step.fetch("name")
      return if persisted_name == step.name

      message = "workflow #{@workflow_id} replay reached step #{position} named #{step.name.inspect}, " \
        "but durable history already completed #{persisted_name.inspect}"
      raise NonDeterminismError, message
    end

    #: (untyped) -> untyped
    def build_heartbeat(position)
      Heartbeat.new(
        cursor: @store_mutex.synchronize { @store.step_heartbeat_cursor(workflow_id: @workflow_id, position:) },
        recorder: lambda do |cursor|
          renewed = @store_mutex.synchronize do
            @store.heartbeat_step(workflow_id: @workflow_id, position:, worker_id: @worker_id, lease_seconds: @lease_seconds, cursor:)
          end
          raise LeaseConflict, "workflow #{@workflow_id} lease expired or moved before heartbeat" unless renewed

          true
        end,
      )
    end

    #: () -> untyped
    def assert_workflow_lease!
      return unless @store.respond_to?(:workflow_owned?)
      return if @store.workflow_owned?(workflow_id: @workflow_id, worker_id: @worker_id)

      raise LeaseConflict, "workflow #{@workflow_id} lease expired or moved before state update"
    end

    #: (untyped) -> untyped
    def retry_run_at(delay)
      base = @store.respond_to?(:current_time) ? @store.current_time : Time.now
      base + delay
    end

    #: (untyped) -> untyped
    def crash!(point)
      raise InjectedCrash, "injected crash after #{point}" if @crash_after == point
    end
  end

  class Engine
    DEFAULT_LEASE_SECONDS = 60

    #: (store: untyped, ?worker_id: untyped, ?lease_seconds: untyped, ?crash_after: untyped, ?migrate: untyped) -> void
    def initialize(store:, worker_id: "inline-worker", lease_seconds: DEFAULT_LEASE_SECONDS, crash_after: nil, migrate: true)
      @store = store
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @crash_after = crash_after
      @store.migrate! if migrate
    end

    #: (untyped, input: untyped) -> untyped
    def run(workflow_class, input:)
      workflow_id = @store.enqueue_workflow(name: workflow_class.workflow_name, input:)
      resume(workflow_class, workflow_id:)
    end

    #: (untyped, workflow_id: untyped, ?claimed: untyped) -> untyped
    def resume(workflow_class, workflow_id:, claimed: nil)
      current = claimed || @store.workflow(workflow_id)
      return run_from_row(current) if current.fetch("status") == "completed"

      claimed ||= @store.claim_workflow(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)
      raise LeaseConflict, "workflow #{workflow_id} is leased by another worker" unless claimed

      execute(workflow_class, workflow_id:, initial_input: claimed.fetch("input"))
    end

    private

    #: (untyped, workflow_id: untyped, ?initial_input: untyped) -> untyped
    def execute(workflow_class, workflow_id:, initial_input: nil)
      steps = @store.steps_for(workflow_id)
      execution = WorkflowExecution.new(
        store: @store,
        workflow_id:,
        worker_id: @worker_id,
        lease_seconds: @lease_seconds,
        steps:,
        crash_after: @crash_after,
      )
      workflow = workflow_class.new
      workflow.__durababble_execution__ = execution
      result = workflow.execute(initial_input || initial_context(workflow_id))
      execution.assert_async_futures_settled!
      execution.validate_replay_complete!
      assert_workflow_lease!(workflow_id)
      @store.complete_workflow(workflow_id, result:)
      crash!(:workflow_completed)
      snapshot(workflow_id)
    rescue WorkflowSuspended, StepRetryScheduled
      snapshot(workflow_id)
    rescue StandardError => e
      raise if e.is_a?(InjectedCrash) || e.is_a?(LeaseConflict)

      message = "#{e.class}: #{e.message}"
      assert_workflow_lease!(workflow_id)
      @store.fail_workflow(workflow_id, error: message)
      snapshot(workflow_id)
    ensure
      workflow.__durababble_execution__ = nil if workflow
    end

    #: (untyped) -> untyped
    def assert_workflow_lease!(workflow_id)
      return unless @store.respond_to?(:workflow_owned?)
      return if @store.workflow_owned?(workflow_id:, worker_id: @worker_id)

      raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before state update"
    end

    #: (untyped) -> untyped
    def initial_context(workflow_id)
      @store.workflow(workflow_id).fetch("input")
    end

    #: (untyped) -> untyped
    def snapshot(workflow_id)
      run_from_row(@store.workflow(workflow_id))
    end

    #: (untyped) -> untyped
    def run_from_row(row)
      Run.new(id: row.fetch("id"), status: row.fetch("status"), result: row["result"], error: row["error"])
    end

    #: (untyped) -> untyped
    def crash!(point)
      raise InjectedCrash, "injected crash after #{point}" if @crash_after == point
    end
  end
end
