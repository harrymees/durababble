# typed: true
# frozen_string_literal: true

require "async"
require "securerandom"

require_relative "error_formatting"
require_relative "workflow_execution"

module Durababble
  class Engine
    DEFAULT_LEASE_SECONDS = 60

    #: Store
    attr_reader :store
    #: String
    attr_reader :worker_pool

    #: (store: Store, ?worker_id: String, ?lease_seconds: Numeric, ?crash_after: Symbol?, ?migrate: bool, ?worker_pool: String, ?workflow_query_registry: Object?) -> void
    def initialize(store:, worker_id: "inline-worker", lease_seconds: DEFAULT_LEASE_SECONDS, crash_after: nil, migrate: true, worker_pool: "default", workflow_query_registry: nil)
      @store = store
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @crash_after = crash_after
      @worker_pool = worker_pool
      @workflow_query_registry = workflow_query_registry #: as untyped
    end

    #: (Class, input: Object?, ?id: String?) -> String
    def enqueue(workflow_class, input:, id: nil)
      workflow_class = workflow_class #: as untyped
      workflow_id = id || SecureRandom.uuid
      @store.enqueue_workflow(name: workflow_class.workflow_name, input:, id: workflow_id, worker_pool: @worker_pool)
    end

    #: (Class, input: Object?) -> Run
    def run(workflow_class, input:)
      workflow_class = workflow_class #: as untyped
      attributes = {
        "durababble.workflow.name" => workflow_class.workflow_name,
        "durababble.worker.id" => @worker_id,
      }
      Observability.trace("durababble.workflow.start", attributes) do
        Observability.count("durababble.workflow.starts", attributes)
        workflow_id = @store.create_workflow(name: workflow_class.workflow_name, input:, worker_id: @worker_id, lease_seconds: @lease_seconds, worker_pool: @worker_pool)
        execute(workflow_class, workflow_id:, initial_input: input)
      end
    end

    #: (Class, workflow_id: String, ?claimed: Hash[String, Object?]?) -> Run
    def resume(workflow_class, workflow_id:, claimed: nil)
      workflow_class = workflow_class #: as untyped
      attributes = execute_attributes(workflow_class, workflow_id)
      Observability.trace("durababble.workflow.resume", attributes) do
        current = claimed || @store.workflow(workflow_id)
        return run_from_row(current) if terminal_workflow_row?(current)

        owned_claim = claimed || @store.claim_workflow(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds, worker_pool: @worker_pool)
        unless owned_claim
          Observability.count("durababble.leases.conflicts", attributes.merge("durababble.lease.owner" => @worker_id))
          raise LeaseConflict, "workflow #{workflow_id} is leased by another worker"
        end

        execute(workflow_class, workflow_id:, initial_input: owned_claim.fetch("input"))
      end
    end

    private

    #: (untyped) -> bool
    def terminal_workflow_row?(row)
      WorkflowStatus.terminal?(row)
    end

    #: (untyped, ?history_count: untyped) -> bool
    def check_workflow_history_limit!(workflow_id, history_count: nil)
      history_count ||= @store.workflow_history_count_for(workflow_id)
      max_history_events = Durababble.max_workflow_history_events
      warned = Durababble.warn_workflow_history_events(
        workflow_id:,
        history_events: history_count,
        max_history_events:,
      )
      return warned if history_count <= max_history_events

      raise WorkflowHistoryLimitExceeded.new(
        workflow_id,
        history_events: history_count,
        max_history_events: max_history_events,
      )
    end

    #: (untyped, untyped) -> Hash[String, untyped]
    def execute_attributes(workflow_class, workflow_id)
      {
        "durababble.workflow.id" => workflow_id,
        "durababble.workflow.name" => workflow_class.workflow_name,
        "durababble.worker.id" => @worker_id,
      }
    end

    # Runs a workflow to a terminal-or-suspension boundary and returns a Run
    # snapshot. The happy path drives the workflow inside the reactor and
    # snapshots the persisted result; each rescue maps one boundary outcome
    # (suspended, retry scheduled, lease lost, canceled, failed) onto the right
    # persisted state. `attributes` is computed before the trace block so it is
    # always available to the rescue handlers.
    #: (untyped, workflow_id: untyped, ?initial_input: untyped) -> untyped
    def execute(workflow_class, workflow_id:, initial_input: nil)
      # Deferred boot check: at workflow execution time the host's initializers have run
      # and ActiveSupport::IsolatedExecutionState.isolation_level should be :fiber. Running
      # the Async reactor below with :thread isolation would share one AR connection across
      # fan-out fibers and corrupt the wire protocol. See Durababble.assert_fiber_isolation!.
      Durababble.assert_fiber_isolation!
      attributes = execute_attributes(workflow_class, workflow_id)
      Observability.trace("durababble.workflow.execute", attributes) do
        drive_workflow_to_completion(workflow_class, workflow_id:, initial_input:, attributes:)
        snapshot(workflow_id)
      end
    rescue WorkflowSuspended => e
      persist_workflow_suspension(workflow_id, next_run_at: e.next_run_at)
    rescue StepRetryScheduled
      snapshot(workflow_id)
    rescue LeaseConflict
      snapshot_if_terminal_or_reraise(workflow_id)
    rescue CancellationError => e
      persist_workflow_cancellation(workflow_id, e, attributes)
    rescue StandardError => e
      persist_workflow_failure(workflow_id, e, attributes)
    end

    # Drives one reactor run of the workflow. The workflow object is created and
    # owned here so the ensure can always break its reference back to the
    # execution, and errors raised inside the reactor are surfaced to the
    # boundary rescues via root_error rather than escaping the Async task.
    #: (untyped, workflow_id: untyped, initial_input: untyped, attributes: untyped) -> void
    def drive_workflow_to_completion(workflow_class, workflow_id:, initial_input:, attributes:)
      workflow = nil #: untyped
      root_error = nil #: StandardError?
      root = Async do |root_task|
        history, history_warning_logged = load_workflow_history(workflow_id, attributes)
        workflow = workflow_class.new
        execution = WorkflowExecution.new(
          store: @store,
          workflow_id:,
          worker_id: @worker_id,
          lease_seconds: @lease_seconds,
          history:,
          root_task:,
          workflow_class:,
          workflow:,
          worker_pool: @worker_pool,
          crash_after: @crash_after,
          history_warning_logged:,
        )
        workflow.__durababble_execution__ = execution
        result = run_workflow_body(workflow, execution, workflow_class, workflow_id:, initial_input:)
        persist_terminal_state(execution, workflow_id:, result:, attributes:)
        crash!(:workflow_completed)
      rescue StandardError => e
        root_error = e
      end
      root.wait
      raise root_error if root_error
    ensure
      workflow.__durababble_execution__ = nil if workflow
    end

    #: (untyped, untyped) -> [untyped, bool]
    def load_workflow_history(workflow_id, attributes)
      history_warning_logged = check_workflow_history_limit!(workflow_id)
      history = @store.workflow_history_for(workflow_id)
      Observability.record(
        "durababble.workflow.history.steps",
        history.count { |event| event.fetch("kind") == "step_completed" },
        attributes,
      )
      [history, history_warning_logged]
    end

    #: (untyped, untyped, untyped, workflow_id: untyped, initial_input: untyped) -> untyped
    def run_workflow_body(workflow, execution, workflow_class, workflow_id:, initial_input:)
      register_query_owner(workflow_id, workflow_class, workflow) do
        WorkflowExecutionContext.with_current(execution) do
          WorkflowDeterminism.enforce(workflow_id:) do
            workflow.execute(initial_input || initial_context(workflow_id))
          end
        end
      end
    end

    #: (untyped, workflow_id: untyped, result: untyped, attributes: untyped) -> void
    def persist_terminal_state(execution, workflow_id:, result:, attributes:)
      WorkflowExecutionContext.with_current(execution) do
        execution.validate_replay_complete!
        WorkflowDeterminism.allow_host_operations do
          if execution.cancellation_delivered?
            @store.cancel_workflow(workflow_id, reason: cancellation_reason(workflow_id), result:, worker_id: @worker_id)
            Observability.count("durababble.workflow.cancellations", attributes.merge("durababble.workflow.status" => "canceled"))
          else
            @store.complete_workflow(workflow_id, result:, worker_id: @worker_id)
            Observability.count("durababble.workflow.completions", attributes.merge("durababble.workflow.status" => "completed"))
          end
        end
        nil
      end
    end

    #: (untyped, ?next_run_at: Object?) -> untyped
    def persist_workflow_suspension(workflow_id, next_run_at: nil)
      suspended = WorkflowDeterminism.allow_host_operations do
        @store.suspend_workflow(workflow_id:, worker_id: @worker_id, next_run_at:)
      end
      unless suspended
        row = WorkflowDeterminism.allow_host_operations { @store.workflow(workflow_id) }
        return run_from_row(row) if terminal_workflow_row?(row)

        raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before workflow suspension"
      end

      snapshot(workflow_id)
    end

    #: (untyped) -> untyped
    def snapshot_if_terminal_or_reraise(workflow_id)
      row = WorkflowDeterminism.allow_host_operations { @store.workflow(workflow_id) }
      return run_from_row(row) if terminal_workflow_row?(row)

      raise
    end

    #: (untyped, CancellationError, untyped) -> untyped
    def persist_workflow_cancellation(workflow_id, error, attributes)
      WorkflowDeterminism.allow_host_operations do
        @store.cancel_workflow(workflow_id, reason: error.reason || cancellation_reason(workflow_id), result: nil, worker_id: @worker_id)
        Observability.count("durababble.workflow.cancellations", attributes.merge("durababble.workflow.status" => "canceled"))
      end
      snapshot(workflow_id)
    end

    #: (untyped, StandardError, untyped) -> untyped
    def persist_workflow_failure(workflow_id, error, attributes)
      raise if error.is_a?(InjectedCrash) || error.is_a?(LeaseConflict) || error.is_a?(ConfigurationError)

      message = ErrorFormatting.format_error(error)
      WorkflowDeterminism.allow_host_operations do
        @store.fail_workflow(workflow_id, error: message, worker_id: @worker_id)
        Observability.count("durababble.workflow.failures", attributes.merge("durababble.workflow.status" => "failed", "error.type" => error.class.name))
      end
      snapshot(workflow_id)
    end

    #: (untyped) -> untyped
    def initial_context(workflow_id)
      WorkflowDeterminism.allow_host_operations { @store.workflow(workflow_id).fetch("input") }
    end

    #: (untyped, untyped, untyped) { () -> untyped } -> untyped
    def register_query_owner(workflow_id, workflow_class, workflow, &block)
      return block.call unless @workflow_query_registry

      @workflow_query_registry.register(workflow_id, workflow_class, workflow, &block)
    end

    #: (untyped) -> untyped
    def cancellation_reason(workflow_id)
      WorkflowDeterminism.allow_host_operations { @store.workflow_cancellation(workflow_id)&.fetch("reason", nil) }
    end

    #: (untyped) -> untyped
    def snapshot(workflow_id)
      WorkflowDeterminism.allow_host_operations { run_from_row(@store.workflow(workflow_id)) }
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
