# frozen_string_literal: true

module Durababble
  StepContext = Data.define(:workflow_id, :step_index, :attempt_number, :idempotency_key, :heartbeat)

  Heartbeat = Data.define(:cursor, :recorder) do
    def record(cursor = self.cursor)
      recorder.call(cursor)
    end

    alias heartbeat record
  end

  class WorkflowExecution
    attr_reader :step_context

    def initialize(store:, workflow_id:, worker_id:, lease_seconds:, completed_steps:, crash_after: nil)
      @store = store
      @workflow_id = workflow_id
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @completed_steps = completed_steps
      @crash_after = crash_after
      @position = 0
      @step_context = nil
    end

    def call_step(instance, method_name:, args:, kwargs:)
      position = @position
      @position += 1

      if @completed_steps.key?(position)
        return @completed_steps.fetch(position).fetch("result")
      end

      step = instance.class.step_definition(method_name)
      @store.record_step_started(workflow_id: @workflow_id, position:, name: step.name)
      crash!(:step_started)
      heartbeat = build_heartbeat(position)
      attempt_number = @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == position }
      @step_context = StepContext.new(
        workflow_id: @workflow_id,
        step_index: position,
        attempt_number:,
        idempotency_key: "durababble:v1:workflow:#{@workflow_id}:step:#{position}",
        heartbeat:
      )

      output = yield
      if output.is_a?(WaitRequest)
        assert_workflow_lease!
        @store.record_wait(workflow_id: @workflow_id, position:, name: step.name, wait_request: output)
        crash!(:wait_recorded)
        raise WorkflowSuspended
      end

      assert_workflow_lease!
      @store.record_step_completed(workflow_id: @workflow_id, position:, result: output)
      crash!(:step_completed)
      output
    rescue StandardError => e
      raise if e.is_a?(InjectedCrash) || e.is_a?(LeaseConflict) || e.is_a?(WorkflowSuspended)

      message = "#{e.class}: #{e.message}"
      assert_workflow_lease!
      @store.record_step_failed(workflow_id: @workflow_id, position:, error: message)
      attempt_number = @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == position }
      if step.retry_policy.retryable?(e, attempt_number:)
        delay = step.retry_policy.delay_for_attempt(attempt_number)
        @store.schedule_workflow_retry(workflow_id: @workflow_id, worker_id: @worker_id, run_at: retry_run_at(delay))
        raise StepRetryScheduled
      end
      raise
    ensure
      @step_context = nil
    end

    private

    def build_heartbeat(position)
      Heartbeat.new(
        cursor: @store.step_heartbeat_cursor(workflow_id: @workflow_id, position:),
        recorder: lambda do |cursor|
          renewed = @store.heartbeat_step(workflow_id: @workflow_id, position:, worker_id: @worker_id, lease_seconds: @lease_seconds, cursor:)
          raise LeaseConflict, "workflow #{@workflow_id} lease expired or moved before heartbeat" unless renewed

          true
        end
      )
    end

    def assert_workflow_lease!
      return unless @store.respond_to?(:workflow_owned?)
      return if @store.workflow_owned?(workflow_id: @workflow_id, worker_id: @worker_id)

      raise LeaseConflict, "workflow #{@workflow_id} lease expired or moved before state update"
    end

    def retry_run_at(delay)
      base = @store.respond_to?(:current_time) ? @store.current_time : Time.now
      base + delay
    end

    def crash!(point)
      raise InjectedCrash, "injected crash after #{point}" if @crash_after == point
    end
  end

  class Engine
    DEFAULT_LEASE_SECONDS = 60

    def initialize(store:, worker_id: "inline-worker", lease_seconds: DEFAULT_LEASE_SECONDS, crash_after: nil, migrate: true)
      @store = store
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @crash_after = crash_after
      @store.migrate! if migrate
    end

    def run(workflow_class, input:)
      workflow_id = @store.enqueue_workflow(name: workflow_class.workflow_name, input:)
      resume(workflow_class, workflow_id:)
    end

    def resume(workflow_class, workflow_id:, claimed: nil)
      current = claimed || @store.workflow(workflow_id)
      return run_from_row(current) if current.fetch("status") == "completed"

      claimed ||= @store.claim_workflow(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)
      raise LeaseConflict, "workflow #{workflow_id} is leased by another worker" unless claimed

      execute(workflow_class, workflow_id:, initial_input: claimed.fetch("input"))
    end

    private

    def execute(workflow_class, workflow_id:, initial_input: nil)
      completed_steps = @store.steps_for(workflow_id)
                              .select { |step| step.fetch("status") == "completed" }
                              .to_h { |step| [step.fetch("position").to_i, step] }
      execution = WorkflowExecution.new(
        store: @store,
        workflow_id:,
        worker_id: @worker_id,
        lease_seconds: @lease_seconds,
        completed_steps:,
        crash_after: @crash_after
      )
      workflow = workflow_class.new
      workflow.__durababble_execution__ = execution
      result = workflow.execute(initial_input || initial_context(workflow_id))
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

    def assert_workflow_lease!(workflow_id)
      return unless @store.respond_to?(:workflow_owned?)
      return if @store.workflow_owned?(workflow_id:, worker_id: @worker_id)

      raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before state update"
    end

    def initial_context(workflow_id)
      @store.workflow(workflow_id).fetch("input")
    end

    def snapshot(workflow_id)
      run_from_row(@store.workflow(workflow_id))
    end

    def run_from_row(row)
      Run.new(id: row.fetch("id"), status: row.fetch("status"), result: row["result"], error: row["error"])
    end

    def crash!(point)
      raise InjectedCrash, "injected crash after #{point}" if @crash_after == point
    end
  end
end
