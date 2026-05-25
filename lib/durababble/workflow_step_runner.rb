# typed: true
# frozen_string_literal: true

require_relative "execution_context"

module Durababble
  class WorkflowStepRunner
    #: (store: untyped, workflow_id: untyped, worker_id: untyped, lease_seconds: untyped, root_task: untyped, futures: untyped, step_contexts: untyped, synchronize_store: untyped, raise_if_cancel_requested: untyped, assert_workflow_lease: untyped, suspend_workflow_immediately: untyped, retry_run_at: untyped, crash: untyped) -> void
    def initialize(store:, workflow_id:, worker_id:, lease_seconds:, root_task:, futures:, step_contexts:, synchronize_store:, raise_if_cancel_requested:, assert_workflow_lease:, suspend_workflow_immediately:, retry_run_at:, crash:)
      @store = store
      @workflow_id = workflow_id
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @root_task = root_task
      @futures = futures
      @step_contexts = step_contexts
      @synchronize_store = synchronize_store
      @raise_if_cancel_requested = raise_if_cancel_requested
      @assert_workflow_lease = assert_workflow_lease
      @suspend_workflow_immediately = suspend_workflow_immediately
      @retry_run_at = retry_run_at
      @crash = crash
    end

    #: (untyped, step: untyped, attributes: untyped) { -> untyped } -> void
    def dispatch(command_id, step:, attributes:, &block)
      @root_task.async(transient: true) do |task|
        raise_if_cancel_requested!
        synchronize_store do
          @store.record_step_started(workflow_id: @workflow_id, command_id:, name: step.name)
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
          synchronize_store { @store.record_step_completed(workflow_id: @workflow_id, command_id:, result: output) }
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

    #: (untyped, untyped, untyped) -> untyped
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

    #: (untyped, step: untyped, wait_request: untyped) -> void
    def record_wait(command_id, step:, wait_request:)
      assert_workflow_lease!
      suspend_workflow = @suspend_workflow_immediately.call
      synchronize_store do
        @store.record_wait(
          workflow_id: @workflow_id,
          command_id:,
          name: step.name,
          wait_request:,
          suspend_workflow:,
        )
      end
      crash!(:wait_recorded)
      error = WorkflowSuspended.new("workflow #{@workflow_id} suspended at command #{command_id}")
      future(command_id).reject(error)
    end

    #: (untyped, command_id: untyped, step: untyped, attributes: untyped) -> void
    def reject_step_error(error, command_id:, step:, attributes:)
      if error.is_a?(CancellationError)
        assert_workflow_lease!
        synchronize_store do
          @store.record_step_canceled(
            workflow_id: @workflow_id,
            command_id:,
            error: "#{error.class}: #{error.message}",
          )
        end
        future(command_id).reject(error)
        return
      end

      if error.is_a?(InjectedCrash) || error.is_a?(LeaseConflict)
        future(command_id).reject(error)
        return
      end

      if error.is_a?(WorkflowSuspended) || error.is_a?(StepRetryScheduled) || error.is_a?(NonDeterminismError)
        future(command_id).reject(error)
        return
      end

      future(command_id).reject(handle_step_error(error, command_id:, step:, attributes:))
    end

    #: (untyped, command_id: untyped, step: untyped, attributes: untyped) -> untyped
    def handle_step_error(error, command_id:, step:, attributes:)
      message = "#{error.class}: #{error.message}"
      assert_workflow_lease!
      synchronize_store { @store.record_step_failed(workflow_id: @workflow_id, command_id:, error: message) }
      attempt_number = attempt_number_for(command_id)
      attributes = attributes.merge(
        "durababble.step.attempt" => attempt_number,
        "error.type" => error.class.name,
      )
      Observability.count("durababble.workflow.step.failures", attributes)
      return error unless step.retry_policy.retryable?(error, attempt_number:)

      delay = step.retry_policy.delay_for_attempt(attempt_number)
      Observability.count(
        "durababble.workflow.step.retries",
        attributes.merge("durababble.retry.delay_ms" => (delay * 1000.0).round),
      )
      synchronize_store do
        @store.schedule_workflow_retry(workflow_id: @workflow_id, worker_id: @worker_id, run_at: @retry_run_at.call(delay))
      end
      StepRetryScheduled.new(message)
    end

    #: (untyped) -> untyped
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

    #: (untyped) -> untyped
    def attempt_number_for(command_id)
      synchronize_store do
        @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == command_id }
      end
    end

    #: (untyped) -> untyped
    def future(command_id)
      @futures.fetch(command_id)
    end

    #: () { -> untyped } -> untyped
    def synchronize_store(&block)
      @synchronize_store.call(&block)
    end

    #: () -> void
    def raise_if_cancel_requested!
      @raise_if_cancel_requested.call
    end

    #: () -> void
    def assert_workflow_lease!
      @assert_workflow_lease.call
    end

    #: (untyped) -> untyped
    def crash!(point)
      @crash.call(point)
    end
  end
end
