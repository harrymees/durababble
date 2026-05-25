# typed: true
# frozen_string_literal: true

require "async"

require_relative "workflow_execution"

module Durababble
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
        return run_from_row(current) if terminal_workflow_row?(current)

        owned_claim = claimed || @store.claim_workflow(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)
        unless owned_claim
          Observability.count("durababble.leases.conflicts", attributes.merge("durababble.lease.owner" => @worker_id))
          raise LeaseConflict, "workflow #{workflow_id} is leased by another worker"
        end

        execute(workflow_class, workflow_id:, initial_input: owned_claim.fetch("input"))
      end
    end

    #: (untyped, workflow_id: untyped, ?claimed: untyped, ?limit: untyped) -> untyped
    def drain_workflow_inbox(workflow_class, workflow_id:, claimed: nil, limit: 10)
      current = claimed || @store.workflow(workflow_id)
      return 0 if terminal_workflow_row?(current)

      claimed ||= @store.claim_workflow_for_activation(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)
      raise LeaseConflict, "workflow #{workflow_id} is leased by another worker" unless claimed

      workflow = workflow_class.new
      drained = 0
      while drained < limit
        messages = @store.claim_inbox_messages(
          target_kind: "workflow",
          target_type: workflow_class.workflow_name,
          target_id: workflow_id,
          worker_id: @worker_id,
          lease_seconds: @lease_seconds,
          limit: 1,
        )
        break if messages.empty?

        messages.each do |message|
          drained += 1
          dispatch_workflow_command(workflow_class, workflow, workflow_id:, message:)
        end
      end
      @store.suspend_workflow(workflow_id:, worker_id: @worker_id)
      drained
    end

    private

    #: (untyped) -> bool
    def terminal_workflow_row?(row)
      WorkflowStatus.terminal?(row)
    end

    #: (untyped, untyped, workflow_id: untyped, message: untyped) -> untyped
    def dispatch_workflow_command(workflow_class, workflow, workflow_id:, message:)
      unless message.fetch("message_kind") == "workflow_command"
        @store.fail_workflow_command(message_id: message.fetch("id"), workflow_id:, error: "Durababble::Error: unsupported workflow inbox message #{message.fetch("message_kind")}", worker_id: @worker_id)
        return
      end

      payload = message.fetch("payload")
      method_name = (message["method_name"] || payload.fetch("method")).to_sym
      unless workflow_class.exposed_commands.key?(method_name)
        @store.fail_workflow_command(message_id: message.fetch("id"), workflow_id:, error: "Durababble::WorkflowRpc::UnknownCommand: #{method_name}", worker_id: @worker_id)
        return
      end

      args = payload.fetch("args", [])
      kwargs = payload.fetch("kwargs", {})
      result = kwargs.empty? ? workflow.public_send(method_name, *args) : workflow.public_send(method_name, *args, **kwargs)
      @store.complete_workflow_command(message_id: message.fetch("id"), workflow_id:, result:, worker_id: @worker_id)
    rescue StandardError => e
      @store.fail_workflow_command(message_id: message.fetch("id"), workflow_id:, error: "#{e.class}: #{e.message}", worker_id: @worker_id)
    end

    #: (untyped, workflow_id: untyped, ?initial_input: untyped) -> untyped
    def execute(workflow_class, workflow_id:, initial_input: nil)
      attributes = {
        "durababble.workflow.id" => workflow_id,
        "durababble.workflow.name" => workflow_class.workflow_name,
        "durababble.worker.id" => @worker_id,
      }
      Observability.trace("durababble.workflow.execute", attributes) do
        workflow = nil #: untyped
        root_error = nil #: StandardError?
        root = Async do |root_task|
          history = @store.workflow_history_for(workflow_id)
          Observability.record(
            "durababble.workflow.history.steps",
            history.count { |event| event.fetch("kind") == "step_completed" },
            attributes,
          )
          execution = WorkflowExecution.new(
            store: @store,
            workflow_id:,
            worker_id: @worker_id,
            lease_seconds: @lease_seconds,
            history:,
            root_task:,
            crash_after: @crash_after,
          )
          workflow = workflow_class.new
          workflow.__durababble_execution__ = execution
          result = WorkflowExecutionContext.with_current(execution) do
            WorkflowDeterminism.enforce(workflow_id:) do
              workflow.execute(initial_input || initial_context(workflow_id))
            end
          end
          WorkflowExecutionContext.with_current(execution) do
            execution.validate_replay_complete!
            if execution.cancellation_delivered?
              @store.cancel_workflow(workflow_id, reason: cancellation_reason(workflow_id), result:, worker_id: @worker_id)
              Observability.count("durababble.workflow.cancellations", attributes.merge("durababble.workflow.status" => "canceled"))
            else
              @store.complete_workflow(workflow_id, result:, worker_id: @worker_id)
              Observability.count("durababble.workflow.completions", attributes.merge("durababble.workflow.status" => "completed"))
            end
          end
          crash!(:workflow_completed)
        rescue StandardError => e
          root_error = e
        end
        root.wait
        raise root_error if root_error

        snapshot(workflow_id)
      ensure
        workflow.__durababble_execution__ = nil if workflow
      end
    rescue WorkflowSuspended
      raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before workflow suspension" unless @store.suspend_workflow(workflow_id:, worker_id: @worker_id)

      snapshot(workflow_id)
    rescue StepRetryScheduled
      snapshot(workflow_id)
    rescue CancellationError => e
      @store.cancel_workflow(workflow_id, reason: e.reason || cancellation_reason(workflow_id), result: nil, worker_id: @worker_id)
      Observability.count("durababble.workflow.cancellations", (attributes || {}).merge("durababble.workflow.status" => "canceled"))
      snapshot(workflow_id)
    rescue StandardError => e
      raise if e.is_a?(InjectedCrash) || e.is_a?(LeaseConflict)

      message = "#{e.class}: #{e.message}"
      @store.fail_workflow(workflow_id, error: message, worker_id: @worker_id)
      Observability.count("durababble.workflow.failures", (attributes || {}).merge("durababble.workflow.status" => "failed", "error.type" => e.class.name))
      snapshot(workflow_id)
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
