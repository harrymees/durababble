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

    #: (store: untyped, workflow_id: untyped, worker_id: untyped, lease_seconds: untyped, completed_steps: untyped, ?crash_after: untyped) -> void
    def initialize(store:, workflow_id:, worker_id:, lease_seconds:, completed_steps:, crash_after: nil)
      @store = store
      @workflow_id = workflow_id
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @completed_steps = completed_steps
      @crash_after = crash_after
      @position = 0
      @step_context = nil
      @cancellation_delivered = false
    end

    #: () -> untyped
    def cancellation_delivered?
      @cancellation_delivered
    end

    #: (untyped, method_name: untyped, args: untyped, kwargs: untyped) { -> untyped } -> untyped
    def call_step(instance, method_name:, args:, kwargs:, &block)
      base_attributes = {}
      position = @position
      @position += 1
      step = instance.class.step_definition(method_name)
      base_attributes = {
        "durababble.workflow.id" => @workflow_id,
        "durababble.workflow.name" => instance.class.workflow_name,
        "durababble.step.name" => step.name,
        "durababble.step.index" => position,
        "durababble.worker.id" => @worker_id,
      }

      if @completed_steps.key?(position)
        completed_step = @completed_steps.fetch(position)
        validate_completed_step_shape!(completed_step, step:, position:)
        Observability.count("durababble.workflow.replay.steps", base_attributes)
        result = completed_step.fetch("result")
        raise_if_cancel_requested!
        return result
      end

      raise_if_cancel_requested!
      @store.record_step_started(workflow_id: @workflow_id, position:, name: step.name)
      crash!(:step_started)
      heartbeat = build_heartbeat(position)
      attempt_number = @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == position }
      @step_context = StepContext.new(
        workflow_id: @workflow_id,
        step_index: position,
        attempt_number:,
        idempotency_key: "durababble:v1:workflow:#{@workflow_id}:step:#{position}",
        heartbeat:,
      )
      attributes = base_attributes.merge("durababble.step.attempt" => attempt_number)
      Observability.count("durababble.workflow.step.attempts", attributes)

      Observability.trace("durababble.workflow.step", attributes) do
        output = block.call
        if output.is_a?(WaitRequest)
          assert_workflow_lease!
          @store.record_wait(workflow_id: @workflow_id, position:, name: step.name, wait_request: output)
          crash!(:wait_recorded)
          raise WorkflowSuspended
        end

        assert_workflow_lease!
        @store.record_step_completed(workflow_id: @workflow_id, position:, result: output)
        Observability.count("durababble.workflow.step.successes", attributes)
        crash!(:step_completed)
        raise_if_cancel_requested!
        output
      end
    rescue CancellationError => e
      assert_workflow_lease!
      @store.record_step_canceled(workflow_id: @workflow_id, position:, error: "#{e.class}: #{e.message}")
      raise
    rescue StandardError => e
      raise if e.is_a?(InjectedCrash) || e.is_a?(LeaseConflict) || e.is_a?(WorkflowSuspended) || e.is_a?(NonDeterminismError)

      message = "#{e.class}: #{e.message}"
      assert_workflow_lease!
      @store.record_step_failed(workflow_id: @workflow_id, position:, error: message)
      attempt_number = @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == position }
      failure_attributes = (base_attributes || {}).merge(
        "durababble.step.attempt" => attempt_number,
        "error.type" => e.class.name,
      )
      Observability.count("durababble.workflow.step.failures", failure_attributes)
      if step.retry_policy.retryable?(e, attempt_number:)
        delay = step.retry_policy.delay_for_attempt(attempt_number)
        Observability.count(
          "durababble.workflow.step.retries",
          failure_attributes.merge("durababble.retry.delay_ms" => (delay * 1000.0).round),
        )
        @store.schedule_workflow_retry(workflow_id: @workflow_id, worker_id: @worker_id, run_at: retry_run_at(delay))
        raise StepRetryScheduled
      end
      raise
    ensure
      @step_context = nil
    end

    #: () -> void
    def validate_replay_complete!
      extra_steps = @completed_steps
        .select { |position, _step| position >= @position }
        .sort_by { |position, _step| position }
      return if extra_steps.empty?

      rendered = extra_steps
        .map { |position, step| "#{position}:#{step.fetch("name")}" }
        .join(", ")
      raise NonDeterminismError, "workflow #{@workflow_id} replay completed without consuming durable step history: #{rendered}"
    end

    private

    #: () -> untyped
    def raise_if_cancel_requested!
      return if @cancellation_delivered

      cancellation = @store.workflow_cancellation(@workflow_id)
      return unless cancellation

      @cancellation_delivered = true
      @store.mark_workflow_cancellation_delivered(workflow_id: @workflow_id)
      reason = cancellation["reason"]
      raise CancellationError.new(reason, workflow_id: @workflow_id)
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
        cursor: @store.step_heartbeat_cursor(workflow_id: @workflow_id, position:),
        recorder: lambda do |cursor|
          renewed = @store.heartbeat_step(workflow_id: @workflow_id, position:, worker_id: @worker_id, lease_seconds: @lease_seconds, cursor:)
          attributes = {
            "durababble.workflow.id" => @workflow_id,
            "durababble.step.index" => position,
            "durababble.worker.id" => @worker_id,
            "durababble.lease.owner" => @worker_id,
          }
          unless renewed
            Observability.count("durababble.leases.conflicts", attributes)
            raise LeaseConflict, "workflow #{@workflow_id} lease expired or moved before heartbeat"
          end

          Observability.count("durababble.leases.heartbeats", attributes)
          raise_if_cancel_requested!
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
      attributes = {
        "durababble.workflow.name" => workflow_class.workflow_name,
        "durababble.worker.id" => @worker_id,
      }
      Observability.trace("durababble.workflow.start", attributes) do
        Observability.count("durababble.workflow.starts", attributes)
        workflow_id = @store.enqueue_workflow(name: workflow_class.workflow_name, input:)
        resume(workflow_class, workflow_id:)
      end
    end

    #: (untyped, workflow_id: untyped, ?claimed: untyped) -> untyped
    def resume(workflow_class, workflow_id:, claimed: nil)
      attributes = {
        "durababble.workflow.id" => workflow_id,
        "durababble.workflow.name" => workflow_class.workflow_name,
        "durababble.worker.id" => @worker_id,
      }
      Observability.trace("durababble.workflow.resume", attributes) do
        current = claimed || @store.workflow(workflow_id)
        return run_from_row(current) if ["completed", "canceled"].include?(current.fetch("status"))

        owned_claim = claimed || @store.claim_workflow(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)
        unless owned_claim
          Observability.count("durababble.leases.conflicts", attributes.merge("durababble.lease.owner" => @worker_id))
          raise LeaseConflict, "workflow #{workflow_id} is leased by another worker"
        end

        execute(workflow_class, workflow_id:, initial_input: owned_claim.fetch("input"))
      end
    end

    private

    #: (untyped, workflow_id: untyped, ?initial_input: untyped) -> untyped
    def execute(workflow_class, workflow_id:, initial_input: nil)
      attributes = {}
      completed_steps = @store.steps_for(workflow_id)
        .select { |step| step.fetch("status") == "completed" }
        .to_h { |step| [step.fetch("position").to_i, step] }
      attributes = {
        "durababble.workflow.id" => workflow_id,
        "durababble.workflow.name" => workflow_class.workflow_name,
        "durababble.worker.id" => @worker_id,
      }
      Observability.record("durababble.workflow.history.steps", completed_steps.length, attributes)
      execution = WorkflowExecution.new(
        store: @store,
        workflow_id:,
        worker_id: @worker_id,
        lease_seconds: @lease_seconds,
        completed_steps:,
        crash_after: @crash_after,
      )
      workflow = workflow_class.new
      workflow.__durababble_execution__ = execution
      Observability.trace("durababble.workflow.execute", attributes) do
        result = workflow.execute(initial_input || initial_context(workflow_id))
        execution.validate_replay_complete!
        assert_workflow_lease!(workflow_id)
        if execution.cancellation_delivered?
          @store.cancel_workflow(workflow_id, reason: cancellation_reason(workflow_id), result:)
          Observability.count("durababble.workflow.cancellations", attributes.merge("durababble.workflow.status" => "canceled"))
        else
          @store.complete_workflow(workflow_id, result:)
          Observability.count("durababble.workflow.completions", attributes.merge("durababble.workflow.status" => "completed"))
        end
        crash!(:workflow_completed)
        snapshot(workflow_id)
      end
    rescue WorkflowSuspended, StepRetryScheduled
      snapshot(workflow_id)
    rescue CancellationError => e
      assert_workflow_lease!(workflow_id)
      @store.cancel_workflow(workflow_id, reason: e.reason || cancellation_reason(workflow_id), result: nil)
      Observability.count("durababble.workflow.cancellations", (attributes || {}).merge("durababble.workflow.status" => "canceled"))
      snapshot(workflow_id)
    rescue StandardError => e
      raise if e.is_a?(InjectedCrash) || e.is_a?(LeaseConflict)

      message = "#{e.class}: #{e.message}"
      assert_workflow_lease!(workflow_id)
      @store.fail_workflow(workflow_id, error: message)
      Observability.count("durababble.workflow.failures", (attributes || {}).merge("durababble.workflow.status" => "failed", "error.type" => e.class.name))
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
    def cancellation_reason(workflow_id)
      @store.workflow_cancellation(workflow_id)&.fetch("reason", nil)
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
