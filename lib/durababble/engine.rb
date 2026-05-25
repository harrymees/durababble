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
      workflow_id = @store.enqueue_workflow(name: workflow_class.workflow_name, input:)
      resume(workflow_class, workflow_id:)
    end

    #: (untyped, workflow_id: untyped, ?claimed: untyped) -> untyped
    def resume(workflow_class, workflow_id:, claimed: nil)
      current = claimed || @store.workflow(workflow_id)
      return run_from_row(current) if ["completed", "canceled"].include?(current.fetch("status"))

      claimed ||= @store.claim_workflow(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)
      raise LeaseConflict, "workflow #{workflow_id} is leased by another worker" unless claimed

      execute(workflow_class, workflow_id:, initial_input: claimed.fetch("input"))
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
      return true if ["completed", "canceled"].include?(row.fetch("status"))

      row.fetch("status") == "failed" && row["next_run_at"].nil?
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
      workflow = nil #: untyped
      root_error = nil #: StandardError?
      root = Async do |root_task|
        history = @store.workflow_history_for(workflow_id)
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
          workflow.execute(initial_input || initial_context(workflow_id))
        end
        WorkflowExecutionContext.with_current(execution) do
          execution.validate_replay_complete!
          assert_workflow_lease!(workflow_id)
          if execution.cancellation_delivered?
            @store.cancel_workflow(workflow_id, reason: cancellation_reason(workflow_id), result:)
          else
            @store.complete_workflow(workflow_id, result:)
          end
        end
        crash!(:workflow_completed)
      rescue StandardError => e
        root_error = e
      end
      root.wait
      raise root_error if root_error

      snapshot(workflow_id)
    rescue WorkflowSuspended
      @store.suspend_workflow(workflow_id:, worker_id: @worker_id)
      snapshot(workflow_id)
    rescue StepRetryScheduled
      snapshot(workflow_id)
    rescue CancellationError => e
      assert_workflow_lease!(workflow_id)
      @store.cancel_workflow(workflow_id, reason: e.reason || cancellation_reason(workflow_id), result: nil)
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
