# typed: true
# frozen_string_literal: true

require_relative "error_formatting"
require_relative "execution_context"

module Durababble
  class WorkflowStepRunner
    #: (store: Store, workflow_id: String, worker_id: String, lease_seconds: Numeric, root_task: Object, futures: Hash[Integer, Object], step_contexts: Hash[Object, StepContext], execution: WorkflowExecution) -> void
    def initialize(store:, workflow_id:, worker_id:, lease_seconds:, root_task:, futures:, step_contexts:, execution:)
      @store = store
      @workflow_id = workflow_id
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @root_task = root_task #: as untyped
      @futures = futures #: as untyped
      @step_contexts = step_contexts
      @execution = execution
    end

    #: (Integer, step: Object, attributes: Hash[String, Object?]) { -> Object? } -> void
    def dispatch(command_id, step:, attributes:, &block)
      step = step #: as untyped
      @root_task.async(transient: true) do |task|
        raise_if_cancel_requested!
        synchronize_store do
          @store.record_step_started(workflow_id: @workflow_id, command_id:, name: step.name, worker_id: @worker_id, event_index: allocate_history_event_index!)
        end
        crash!(:step_started)

        attempt_number = attempt_number_for(command_id)
        attempt_attributes = attributes.merge("durababble.step.attempt" => attempt_number)
        Observability.count("durababble.workflow.step.attempts", attempt_attributes)
        step_context = build_step_context(task, command_id, attempt_number)
        Observability.trace("durababble.workflow.step", attempt_attributes) do
          output = StepExecutionContext.with_current(step_context) { block.call }
          if output.is_a?(WaitRequest)
            record_wait(command_id, step:, wait_request: output)
            next
          end

          assert_workflow_lease!
          synchronize_store { @store.record_step_completed(workflow_id: @workflow_id, command_id:, result: output, worker_id: @worker_id, event_index: allocate_history_event_index!) }
          Observability.count("durababble.workflow.step.successes", attempt_attributes)
          crash!(:step_completed)
          raise_if_cancel_requested!
          future(command_id).resolve(output)
        end
      rescue StandardError => e
        reject_step_error(e, command_id:, step:, attributes:)
      ensure
        @step_contexts.delete(task)
      end
    end

    private

    #: (Object, Integer, Integer) -> StepContext
    def build_step_context(task, command_id, attempt_number)
      step_context = StepContext.new(
        workflow_id: @workflow_id,
        step_index: command_id,
        attempt_number:,
        idempotency_key: "durababble:v1:workflow:#{@workflow_id}:step:#{command_id}",
        heartbeat: build_heartbeat(command_id),
      )
      @step_contexts[task] = step_context
    end

    #: (Integer, step: Object, wait_request: WaitRequest) -> void
    def record_wait(command_id, step:, wait_request:)
      step = step #: as untyped
      suspend_workflow = @execution.suspend_workflow_immediately?
      due_timer = wait_request.kind == "timer" && wait_request.wake_at && @execution.timer_due?(wait_request.wake_at)
      next_run_at = @execution.next_run_at_for_wait(wait_request)
      synchronize_store do
        @store.record_wait(
          workflow_id: @workflow_id,
          command_id:,
          name: step.name,
          wait_request:,
          suspend_workflow: suspend_workflow && !due_timer,
          worker_id: @worker_id,
          next_run_at:,
          event_index: allocate_history_event_index!,
        )
        @execution.remember_step_waiting(command_id, name: step.name, wait_request:)
      end
      crash!(:wait_recorded)
      if due_timer
        @execution.complete_due_wait_timer!(future(command_id), command_id, reserved_history_event: true)
        return
      end

      @execution.defer_workflow_suspension(command_id)
    end

    # Control-flow errors are rejected onto the future unchanged; only an
    # ordinary step failure is run through retry/terminal handling. Cancellation
    # additionally records the canceled step before propagating.
    PASS_THROUGH_STEP_ERRORS = [InjectedCrash, LeaseConflict, WorkflowSuspended, StepRetryScheduled, ReplayDivergenceError].freeze

    #: (StandardError, command_id: Integer, step: Object, attributes: Hash[String, Object?]) -> void
    def reject_step_error(error, command_id:, step:, attributes:)
      future(command_id).reject(step_rejection_for(error, command_id:, step:, attributes:))
    end

    #: (StandardError, command_id: Integer, step: Object, attributes: Hash[String, Object?]) -> StandardError
    def step_rejection_for(error, command_id:, step:, attributes:)
      if error.is_a?(CancellationError)
        record_step_cancellation(command_id, error)
        return error
      end
      return error if PASS_THROUGH_STEP_ERRORS.any? { |klass| error.is_a?(klass) }

      handle_step_error(error, command_id:, step:, attributes:)
    end

    #: (Integer, CancellationError) -> void
    def record_step_cancellation(command_id, error)
      assert_workflow_lease!
      synchronize_store do
        @store.record_step_canceled(
          workflow_id: @workflow_id,
          command_id:,
          error: "#{error.class}: #{error.message}",
          worker_id: @worker_id,
          event_index: allocate_history_event_index!,
        )
      end
    end

    #: (StandardError, command_id: Integer, step: Object, attributes: Hash[String, Object?]) -> StandardError
    def handle_step_error(error, command_id:, step:, attributes:)
      step = step #: as untyped
      message = ErrorFormatting.format_error(error)
      attempt_number = attempt_number_for(command_id)
      attributes = attributes.merge(
        "durababble.step.attempt" => attempt_number,
        "error.type" => error.class.name,
      )
      Observability.count("durababble.workflow.step.failures", attributes)
      unless step.retry_policy.retryable?(error, attempt_number:)
        return record_final_step_failure(command_id, message:, fallback_error: error)
      end

      delay = step.retry_policy.delay_for_attempt(attempt_number)
      Observability.count(
        "durababble.workflow.step.retries",
        attributes.merge("durababble.retry.delay_ms" => (delay * 1000.0).round),
      )
      synchronize_store do
        run_at = @execution.retry_run_at(delay)
        @store.record_step_failed_and_schedule_retry(
          workflow_id: @workflow_id,
          command_id:,
          error: message,
          worker_id: @worker_id,
          run_at:,
          event_index: allocate_history_event_index!,
        )
      end
      crash_or(:step_failed_recorded, StepRetryScheduled.new(message))
    end

    #: (Integer, message: String, fallback_error: StandardError) -> StandardError
    def record_final_step_failure(command_id, message:, fallback_error:)
      synchronize_store do
        @store.record_step_failed(
          workflow_id: @workflow_id,
          command_id:,
          error: message,
          worker_id: @worker_id,
          terminal: true,
          error_class: fallback_error.class.name,
          error_message: fallback_error.message,
          event_index: allocate_history_event_index!,
        )
      end
      crash_or(:step_failed_recorded, fallback_error)
    end

    #: (Symbol, StandardError) -> StandardError
    def crash_or(point, error)
      crash!(point)
      error
    rescue InjectedCrash => crash
      crash
    end

    #: (Integer) -> Heartbeat
    def build_heartbeat(command_id)
      Heartbeat.new(
        cursor: synchronize_store { @store.step_heartbeat_cursor(workflow_id: @workflow_id, command_id:) },
        recorder: lambda do |cursor|
          attributes = {
            "durababble.workflow.id" => @workflow_id,
            "durababble.step.index" => command_id,
            "durababble.worker.id" => @worker_id,
            "durababble.lease.owner" => @worker_id,
          }
          renewed = synchronize_store do
            @store.heartbeat_step(workflow_id: @workflow_id, command_id:, worker_id: @worker_id, lease_seconds: @lease_seconds, cursor:)
          end
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

    #: (Integer) -> Integer
    def attempt_number_for(command_id)
      count = synchronize_store do
        @store.step_attempt_count_for(workflow_id: @workflow_id, command_id:)
      end
      count = count #: as untyped
      count.to_i
    end

    #: (Integer) -> untyped
    def future(command_id)
      @futures.fetch(command_id)
    end

    #: () { -> untyped } -> untyped
    def synchronize_store(&block)
      @execution.synchronize_store(&block)
    end

    #: () -> Integer
    def allocate_history_event_index!
      @execution.allocate_history_event_index!
    end

    #: () -> void
    def raise_if_cancel_requested!
      @execution.raise_if_cancel_requested!
    end

    #: () -> void
    def assert_workflow_lease!
      @execution.assert_workflow_lease!
    end

    #: (Symbol) -> Object?
    def crash!(point)
      @execution.crash!(point)
    end
  end
end
