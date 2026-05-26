# typed: true
# frozen_string_literal: true

require_relative "error_formatting"
require_relative "execution_context"

module Durababble
  class WorkflowStepRunner
    #: (store: Object, workflow_id: String, worker_id: String, lease_seconds: Integer, root_task: Object, execution: untyped) -> void
    def initialize(store:, workflow_id:, worker_id:, lease_seconds:, root_task:, execution:)
      @store = store #: as untyped
      @workflow_id = workflow_id
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @root_task = root_task #: as untyped
      @execution = execution
    end

    #: (Integer, step: Object, attributes: Hash[String, Object?]) { -> Object? } -> void
    def dispatch(command_id, step:, attributes:, &block)
      step = step #: as untyped
      @root_task.async(transient: true) do |task|
        @execution.raise_if_cancel_requested!
        @execution.synchronize_store do
          @store.record_step_started(workflow_id: @workflow_id, command_id:, name: step.name, worker_id: @worker_id)
        end
        @execution.crash!(:step_started)

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

          @execution.assert_workflow_lease!
          @execution.synchronize_store { @store.record_step_completed(workflow_id: @workflow_id, command_id:, result: output, worker_id: @worker_id) }
          Observability.count("durababble.workflow.step.successes", attempt_attributes)
          @execution.crash!(:step_completed)
          @execution.raise_if_cancel_requested!
          @execution.resolve_command(command_id, output)
        end
      rescue StandardError => e
        reject_step_error(e, command_id:, step:, attributes:)
      ensure
        @execution.clear_step_context(task)
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
      @execution.register_step_context(task, step_context)
      step_context
    end

    #: (Integer, step: Object, wait_request: WaitRequest) -> void
    def record_wait(command_id, step:, wait_request:)
      step = step #: as untyped
      suspend_workflow = @execution.suspend_workflow_immediately?
      @execution.synchronize_store do
        @store.record_wait(
          workflow_id: @workflow_id,
          command_id:,
          name: step.name,
          wait_request:,
          suspend_workflow:,
          worker_id: @worker_id,
        )
      end
      @execution.crash!(:wait_recorded)
      if suspend_workflow
        error = WorkflowSuspended.new("workflow #{@workflow_id} suspended at command #{command_id}")
        @execution.reject_command(command_id, error)
      else
        @execution.defer_workflow_suspension(command_id)
      end
    end

    #: (StandardError, command_id: Integer, step: Object, attributes: Hash[String, Object?]) -> void
    def reject_step_error(error, command_id:, step:, attributes:)
      if error.is_a?(CancellationError)
        @execution.assert_workflow_lease!
        @execution.synchronize_store do
          @store.record_step_canceled(
            workflow_id: @workflow_id,
            command_id:,
            error: "#{error.class}: #{error.message}",
            worker_id: @worker_id,
          )
        end
        @execution.reject_command(command_id, error)
        return
      end

      if error.is_a?(InjectedCrash) || error.is_a?(LeaseConflict)
        @execution.reject_command(command_id, error)
        return
      end

      if error.is_a?(WorkflowSuspended) || error.is_a?(StepRetryScheduled) || error.is_a?(NonDeterminismError)
        @execution.reject_command(command_id, error)
        return
      end

      @execution.reject_command(command_id, handle_step_error(error, command_id:, step:, attributes:))
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
      @execution.synchronize_store do
        run_at = @execution.retry_run_at(delay)
        @store.record_step_failed_and_schedule_retry(
          workflow_id: @workflow_id,
          command_id:,
          error: message,
          worker_id: @worker_id,
          run_at:,
        )
      end
      crash_or(:step_failed_recorded, StepRetryScheduled.new(message))
    end

    #: (Integer, message: String, fallback_error: StandardError) -> StandardError
    def record_final_step_failure(command_id, message:, fallback_error:)
      @execution.synchronize_store do
        @store.record_step_failed(
          workflow_id: @workflow_id,
          command_id:,
          error: message,
          worker_id: @worker_id,
          terminal: true,
          error_class: fallback_error.class.name,
          error_message: fallback_error.message,
        )
      end
      crash_or(:step_failed_recorded, fallback_error)
    end

    #: (Symbol, StandardError) -> StandardError
    def crash_or(point, error)
      @execution.crash!(point)
      error
    rescue InjectedCrash => crash
      crash
    end

    #: (Integer) -> Heartbeat
    def build_heartbeat(command_id)
      Heartbeat.new(
        cursor: @execution.synchronize_store { @store.step_heartbeat_cursor(workflow_id: @workflow_id, command_id:) },
        recorder: lambda do |cursor|
          attributes = {
            "durababble.workflow.id" => @workflow_id,
            "durababble.step.index" => command_id,
            "durababble.worker.id" => @worker_id,
            "durababble.lease.owner" => @worker_id,
          }
          renewed = @execution.synchronize_store do
            @store.heartbeat_step(workflow_id: @workflow_id, command_id:, worker_id: @worker_id, lease_seconds: @lease_seconds, cursor:)
          end
          unless renewed
            Observability.count("durababble.leases.conflicts", attributes)
            raise LeaseConflict, "workflow #{@workflow_id} lease expired or moved before heartbeat"
          end

          Observability.count("durababble.leases.heartbeats", attributes)
          @execution.raise_if_cancel_requested!
          true
        end,
      )
    end

    #: (Integer) -> Integer
    def attempt_number_for(command_id)
      count = @execution.synchronize_store do
        @store.step_attempt_count_for(workflow_id: @workflow_id, command_id:)
      end
      count = count #: as untyped
      count.to_i
    end
  end
end
