# typed: true
# frozen_string_literal: true

require "async"
require "thread"
require "digest"
require "time"

require_relative "command_future"
require_relative "durable_time"
require_relative "execution_context"
require_relative "workflow_determinism"
require_relative "workflow_replay_history"
require_relative "workflow_step_runner"
require_relative "child_workflow_reuse"
require_relative "workflow"

module Durababble
  class WorkflowExecution
    CHILD_WORKFLOW_AWAIT_PARK_SECONDS = 10 * 365 * 24 * 60 * 60

    #: Store
    attr_reader :store

    #: (store: Store, workflow_id: String, worker_id: String, lease_seconds: Numeric, history: Array[Hash[String, Object?]], root_task: Object, workflow_class: Class, workflow: Object, worker_pool: String, ?crash_after: Symbol?, ?history_warning_logged: bool, ?claimed_next_run_at: Object?) -> void
    def initialize(store:, workflow_id:, worker_id:, lease_seconds:, history:, root_task:, workflow_class:, workflow:, worker_pool:, crash_after: nil, history_warning_logged: false, claimed_next_run_at: nil)
      @store = store
      @workflow_id = workflow_id
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @worker_pool = worker_pool
      @root_task = root_task #: as untyped
      @workflow_class = workflow_class #: as untyped
      @workflow = workflow #: as untyped
      @crash_after = crash_after
      @next_command_id = 0
      @futures = {}
      @workflow_tasks = {}
      @blocked_workflow_tasks = {}
      @workflow_task_count = 0
      @step_contexts = {}
      @deferred_suspension_command_ids = {}
      @deferred_suspension_check_scheduled = false
      @claimed_next_run_at = claimed_next_run_at
      @store_mutex = Mutex.new
      @replay_history = WorkflowReplayHistory.new(history)
      @history_warning_logged = history_warning_logged
      @cancellation_delivered = false
      @delivering_workflow_command = false
      @workflow_commands_delivered_count = 0
      @step_runner = WorkflowStepRunner.new(
        store: @store,
        workflow_id: @workflow_id,
        worker_id: @worker_id,
        lease_seconds: @lease_seconds,
        root_task: @root_task,
        futures: @futures,
        step_contexts: @step_contexts,
        execution: self,
      )
      register_workflow_task(root_task)
    end

    #: () -> bool
    def cancellation_delivered?
      @cancellation_delivered
    end

    #: () -> StepContext
    def step_context
      context = StepExecutionContext.current || @step_contexts.fetch(Async::Task.current)
      context #: as StepContext
    end

    #: (Object) -> void
    def register_workflow_task(task)
      return if @workflow_tasks.key?(task)

      @workflow_tasks[task] = true
      @workflow_task_count += 1
      @futures.each_value(&:wake)
    end

    #: (Object) -> void
    def unregister_workflow_task(task)
      return unless @workflow_tasks.delete(task)

      @blocked_workflow_tasks.delete(task)
      @workflow_task_count -= 1
      schedule_deferred_suspension_check
      @futures.each_value(&:wake)
    end

    #: () { () -> Object? } -> Object?
    def block_current_workflow_task(&block)
      task = Async::Task.current
      return block.call unless @workflow_tasks.key?(task)

      @blocked_workflow_tasks[task] = true
      @futures.each_value(&:wake)
      schedule_deferred_suspension_check
      block.call
    ensure
      @blocked_workflow_tasks.delete(task) if task
      @futures.each_value(&:wake) if task
    end

    #: (Object, method_name: Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?]) { () -> Object? } -> Object?
    def call_step(instance, method_name:, args:, kwargs:, &block)
      instance = instance #: as untyped
      step = instance.class.step_definition(method_name)
      call_step_command(step, instance:, args:, kwargs:, &block)
    end

    #: (Step, args: Array[Object?], kwargs: Hash[Symbol, Object?]) -> Object?
    def call_step_object(step, args:, kwargs:)
      call_step_command(step, instance: @workflow, args:, kwargs:) do
        step.call_body(@workflow, args:, kwargs:)
      end
    end

    #: (Step, instance: Object, args: Array[Object?], kwargs: Hash[Symbol, Object?]) { () -> Object? } -> Object?
    def call_step_command(step, instance:, args:, kwargs:, &block)
      assert_workflow_task!("durable step #{step.name}")
      shape = step_command_shape(step:, args:, kwargs:)
      raise_if_cancel_requested! if deliver_cancellation_before_command?(shape)
      command_id, future, scheduled_from_history = allocate_command(name: step.name, shape:)
      attributes = step_attributes(instance, step:, command_id:)
      # [DURABABBLE-STEP-1] Completed commands resolve from durable replay history instead of rerunning user step code.
      if @replay_history.terminal_recorded?(command_id)
        Observability.count("durababble.workflow.replay.steps", attributes)
      else
        reserve_scheduled_followup_events!(scheduled_from_history)
        dispatch_command!(command_id, step:, attributes:, &block)
      end
      complete_due_wait_timer!(future, command_id)
      await_command_result(future, command_id)
    end

    #: (WaitRequest, name: String, ?args: Array[Object?], ?kwargs: Hash[Symbol, Object?], ?interrupt_on_command: bool) -> Object?
    def call_wait(wait_request, name:, args: [], kwargs: {}, interrupt_on_command: false)
      assert_workflow_task!("durable wait #{name}")
      shape = wait_command_shape(name:, wait_request:, args:, kwargs:)
      raise_if_cancel_requested! if deliver_cancellation_before_command?(shape)
      command_id, future, scheduled_from_history = allocate_command(name:, shape:)

      unless @replay_history.terminal_recorded?(command_id)
        reserve_scheduled_followup_events!(scheduled_from_history)
        record_wait_command!(future, command_id, name:, wait_request:)
      end
      complete_due_wait_timer!(future, command_id)
      await_command_result(future, command_id, interrupt_on_command:)
    end

    #: [Result] (target_kind: String, target_type: String, target_id: String, method_name: Symbol, rpc_kind: String, args: Array[Object?], kwargs: Hash[Symbol, Object?], ?retry_policy: RetryPolicy?) { (idempotency_key: String, args: Array[Object?], kwargs: Hash[Symbol, Object?]) -> Result } -> Result
    def call_handle_rpc(target_kind:, target_type:, target_id:, method_name:, rpc_kind:, args:, kwargs:, retry_policy: nil, &block)
      assert_workflow_task!("durable handle RPC #{target_kind}:#{target_type}##{method_name}")
      command_kwargs = kwargs.dup
      caller_idempotency_key = command_kwargs.delete(:idempotency_key)
      name = handle_rpc_command_name(target_kind:, target_type:, method_name:)
      shape = handle_rpc_command_shape(
        name:,
        target_kind:,
        target_type:,
        target_id:,
        method_name:,
        rpc_kind:,
        args:,
        kwargs: command_kwargs,
        idempotency_key: caller_idempotency_key,
      )
      raise_if_cancel_requested! if deliver_cancellation_before_command?(shape)
      command_id, future, scheduled_from_history = allocate_command(name:, shape:)
      synthetic_step = Step.new(name:, retry_policy: RetryPolicy.from(retry_policy))
      attributes = handle_rpc_attributes(
        target_kind:,
        target_type:,
        target_id:,
        method_name:,
        rpc_kind:,
        command_id:,
      )
      count_replay_step(command_id, attributes)

      unless @replay_history.terminal_recorded?(command_id)
        reserve_scheduled_followup_events!(scheduled_from_history)
        # The handle-RPC block runs OUTSIDE synchronize_store on purpose: it calls
        # wait_for_inbox_message, which polls with a yielding sleep between SELECTs. Holding
        # the store mutex across that poll would serialize the whole workflow. Concurrent
        # sibling fan-out tasks issuing handle RPCs in parallel is safe because each fiber
        # checks out its OWN ActiveRecord connection — that requires
        # ActiveSupport::IsolatedExecutionState.isolation_level = :fiber, which
        # Durababble.assert_fiber_isolation! enforces at Engine#execute entry. Under the
        # default :thread isolation fibers would share one connection and the trilogy/pg
        # driver's mid-query fiber yield (via rb_wait_for_single_fd) would interleave
        # packets and corrupt the wire protocol.
        dispatch_command!(command_id, step: synthetic_step, attributes:) do
          idempotency_key = caller_idempotency_key || handle_rpc_idempotency_key(command_id)
          handle_call = block #: as untyped
          result = handle_call.call(idempotency_key:, args:, kwargs: command_kwargs.dup)
          crash!(:handle_rpc_completed)
          result
        end
      end
      deliver_recorded_resolutions!
      result = await_with_command_delivery(future, command_id)
      raise_if_cancel_requested!
      result #: as Result
    end

    #: (Class, Object?, cancellation_policy: Symbol | String, ?id: String?, ?worker_pool: String?, ?idempotency_key: String?) -> ChildWorkflowHandle
    def call_child_workflow_start(workflow_class, input, cancellation_policy:, id: nil, worker_pool: nil, idempotency_key: nil)
      workflow_class = workflow_class #: as untyped
      assert_workflow_task!("child workflow start #{workflow_class.workflow_name}")
      child_workflow_name = workflow_class.workflow_name
      child_worker_pool = worker_pool || @worker_pool
      normalized_policy = normalize_child_cancellation_policy(cancellation_policy)
      name = child_workflow_command_name(child_workflow_name, "start")
      shape = child_workflow_start_shape(
        name:,
        child_workflow_name:,
        child_workflow_id: id,
        input:,
        worker_pool: child_worker_pool,
        idempotency_key:,
        cancellation_policy: normalized_policy,
      )
      raise_if_cancel_requested! if deliver_cancellation_before_command?(shape)
      command_id, future, scheduled_from_history = allocate_command(name:, shape:)
      synthetic_step = Step.new(name:, retry_policy: RetryPolicy.from(nil))
      attributes = child_workflow_attributes(child_workflow_name:, child_workflow_id: id, command_id:, operation: "start")
      count_replay_step(command_id, attributes)

      unless @replay_history.terminal_recorded?(command_id)
        reserve_scheduled_followup_events!(scheduled_from_history)
        dispatch_command!(command_id, step: synthetic_step, attributes:) do
          resolved_key = idempotency_key || child_workflow_idempotency_key(command_id)
          resolved_id = id || generated_child_workflow_id(command_id:, child_workflow_name:, input:, worker_pool: child_worker_pool, idempotency_key: resolved_key)
          link = @store.start_child_workflow(
            origin_kind: "workflow",
            parent_workflow_id: @workflow_id,
            parent_command_id: command_id,
            parent_worker_id: @worker_id,
            child_workflow_name:,
            child_workflow_id: resolved_id,
            input:,
            worker_pool: child_worker_pool,
            cancellation_policy: normalized_policy,
          )
          ChildWorkflowReuse.validate!(
            link,
            origin_kind: "workflow",
            parent_workflow_id: @workflow_id,
            parent_command_id: command_id,
            child_workflow_name:,
            child_workflow_id: resolved_id,
            input:,
            worker_pool: child_worker_pool,
            cancellation_policy: normalized_policy,
          )
          link.slice(
            "child_workflow_id",
            "child_workflow_name",
            "worker_pool",
            "cancellation_policy",
            "status",
            "result",
            "error",
          )
        end
      end

      observed = await_command_result(future, command_id)
      ChildWorkflowHandle.new(
        workflow_class,
        observed.fetch("child_workflow_id"),
        store: @store,
        worker_pool: observed.fetch("worker_pool"),
        cancellation_policy: observed.fetch("cancellation_policy"),
      )
    end

    #: (ChildWorkflowHandle, ?await_deadline: Time?) -> Hash[String, Object?]
    def call_child_workflow_observe(handle, await_deadline: nil)
      assert_workflow_task!("child workflow observe #{handle.workflow_id}")
      name = child_workflow_command_name("workflow", "observe")
      shape = child_workflow_observe_shape(name:, child_workflow_id: handle.workflow_id, await_deadline:)
      raise_if_cancel_requested! if deliver_cancellation_before_command?(shape)
      command_id, future, scheduled_from_history = allocate_command(name:, shape:)
      synthetic_step = Step.new(name:, retry_policy: RetryPolicy.from(nil))
      attributes = child_workflow_attributes(child_workflow_name: "workflow", child_workflow_id: handle.workflow_id, command_id:, operation: "observe")
      count_replay_step(command_id, attributes)

      unless @replay_history.terminal_recorded?(command_id)
        reserve_scheduled_followup_events!(scheduled_from_history)
        dispatch_command!(command_id, step: synthetic_step, attributes:) do
          @store.observe_child_workflow(handle.workflow_id).slice("status", "result", "error")
        end
      end

      await_command_result(future, command_id)
    end

    #: (ChildWorkflowHandle, poll_interval: Numeric, timeout: Numeric?) -> Object?
    def await_child_workflow(handle, poll_interval:, timeout:)
      deadline = child_workflow_await_deadline(timeout)
      await_time = if deadline && timeout
        deadline - timeout
      else
        WorkflowDeterminism.allow_host_operations { @store.current_time }
      end
      loop do
        observed = call_child_workflow_observe(handle, await_deadline: deadline)
        return handle.await_result_from(observed) if WorkflowStatus.terminal?(observed)

        if deadline && await_time && await_time >= deadline
          raise CommandTimeout, "timed out waiting for child workflow #{handle.workflow_id}"
        end

        current_time = await_time #: as Time
        await_time = wait_for_child_workflow_poll(handle, poll_interval:, current_time:, deadline:)
      end
    end

    # Awaits a command future, delivering pending workflow commands at the resulting
    # safe point. With interrupt_on_command a freshly-delivered command must re-evaluate
    # its condition rather than leave the wait parked, so WorkflowCommandDelivered can be
    # raised from one of two complementary points:
    #
    #   1. While the future is still alive — await_command_future raises as soon as its
    #      delivery counter advances, catching commands delivered by this wait's safe point
    #      OR by a concurrent sibling task before suspension is decided. This is the path
    #      that fires in the common case.
    #   2. After the future was already rejected with WorkflowSuspended at quiescence — the
    #      counter never advanced, so we make one final delivery attempt here and, if it
    #      lands a command, convert the suspension into a retry instead of propagating it.
    #
    # Without (2) a command committed in the same instant the workflow went quiescent could
    # be stranded behind an already-rejected future; without (1) a sibling's delivery would
    # not interrupt a wait that is about to suspend.
    #: (Object, Integer, ?interrupt_on_command: bool) -> Object?
    def await_with_command_delivery(future, command_id, interrupt_on_command: false)
      result = begin
        await_command_future(future, command_id, interrupt_on_command:)
      rescue WorkflowSuspended
        delivered = deliver_workflow_commands_at_safe_point!
        if interrupt_on_command && delivered.positive?
          raise WorkflowCommandDelivered, "workflow #{@workflow_id} command delivered while waiting at command #{command_id}"
        end

        raise
      end
      deliver_workflow_commands_at_safe_point!
      result
    end

    #: (?timeout: Numeric?) { () -> bool } -> bool
    def wait_condition(timeout: nil, &block)
      deadline = wait_condition_deadline(timeout)
      loop do
        deliver_workflow_commands_at_safe_point!
        if scheduled_history_for_next_command?
          if wait_condition_command_delivered?(timeout, deadline)
            raise_if_cancel_requested!
            return true if block.call

            next
          end

          return !!block.call if timeout

          next
        end

        raise_if_cancel_requested!
        return true if block.call

        if wait_condition_command_delivered?(timeout, deadline)
          raise_if_cancel_requested!
          return true if block.call

          next
        end

        return !!block.call if timeout
      end
    end

    #: (Numeric?, Time?) -> bool
    def wait_condition_command_delivered?(timeout, deadline)
      wait_request = WaitRequest.new(
        kind: "timer",
        wake_at: wait_condition_wake_at(timeout, deadline),
        event_key: nil,
        context: wait_condition_context(deadline),
      )
      call_wait(wait_request, name: "wait_condition", kwargs: { timeout: }, interrupt_on_command: true)
      false
    rescue WorkflowCommandDelivered
      true
    end

    #: (Numeric) -> Time
    def timer_after(duration)
      WorkflowDeterminism.allow_host_operations { retry_run_at(duration) }
    end

    #: () -> void
    def validate_replay_complete!
      @replay_history.validate_complete!(workflow_id: @workflow_id, next_command_id: @next_command_id)
    end

    # Allocates the next physical history event index from the in-memory replay
    # counter. The step runner calls this so every append it writes is a single
    # plain insert with a Ruby-supplied index.
    #: () -> Integer
    def allocate_history_event_index!
      @replay_history.allocate_event_index!
    end

    #: (Integer, name: String, wait_request: WaitRequest) -> void
    def remember_step_waiting(command_id, name:, wait_request:)
      @replay_history.remember_step_waiting(command_id, name:, wait_request:)
    end

    private

    # Reserves the next command id, registers its future, and replays or records
    # the schedule. Returns [command_id, future, scheduled_from_history] where
    # scheduled_from_history is true when the schedule matched recorded history.
    #: (name: untyped, shape: untyped) -> [Integer, CommandFuture, bool]
    def allocate_command(name:, shape:)
      synchronize_store do
        command_id = @next_command_id
        @next_command_id += 1
        future = CommandFuture.new(command_id)
        @futures[command_id] = future
        scheduled_from_history = schedule_command!(command_id, name:, shape:, event_budget: 3)
        [command_id, future, scheduled_from_history]
      end
    end

    # A command replayed from a recorded schedule has two follow-up events (the
    # start + terminal records) that were budgeted at schedule time; reserve them
    # against the history limit before they are written.
    #: (bool) -> void
    def reserve_scheduled_followup_events!(scheduled_from_history)
      return unless scheduled_from_history

      ensure_history_limit_allows!(additional_events: 2)
      @replay_history.reserve_events!(2)
    end

    #: (CommandFuture, Integer, ?interrupt_on_command: bool) -> untyped
    def await_command_result(future, command_id, interrupt_on_command: false)
      deliver_recorded_resolutions!
      result = await_with_command_delivery(future, command_id, interrupt_on_command:)
      raise_if_cancel_requested!
      result
    end

    #: () -> untyped
    def raise_if_cancel_requested!
      return if @cancellation_delivered

      cancellation = synchronize_store { @store.workflow_cancellation(@workflow_id) }
      return unless cancellation

      raise cancellation_error_from(cancellation)
    end

    #: (untyped) -> void
    def assert_workflow_task!(operation)
      return if @workflow_tasks.key?(Async::Task.current)

      raise Error, "#{operation} must run inside a Durababble-managed workflow task"
    end

    #: (Numeric?) -> Time?
    def child_workflow_await_deadline(timeout)
      return unless timeout

      recorded = @replay_history.recorded_schedule(@next_command_id)
      deadline = recorded_child_workflow_await_deadline(recorded)
      return deadline if deadline

      WorkflowDeterminism.allow_host_operations { store_current_time + timeout }
    end

    #: (ChildWorkflowHandle, poll_interval: Numeric, current_time: Time, deadline: Time?) -> Time
    def wait_for_child_workflow_poll(handle, poll_interval:, current_time:, deadline:)
      wait_request = recorded_child_workflow_await_wait_request || begin
        wake_at = child_workflow_poll_wake_at(current_time:, poll_interval:, deadline:)
        WaitRequest.new(
          kind: "child_workflow",
          wake_at:,
          event_key: nil,
          context: {
            "child_workflow_id" => handle.workflow_id,
            "child_workflow_wake_at" => wake_at,
          }.tap { |context| context["child_workflow_deadline_at"] = deadline if deadline },
        )
      end
      result = call_wait(
        wait_request,
        name: child_workflow_command_name("workflow", "await"),
        args: [handle.workflow_id],
        kwargs: { poll_interval: },
      )
      context = result.is_a?(Hash) ? result : wait_request.context
      context.fetch("child_workflow_wake_at") #: as Time
    end

    #: (current_time: Time, poll_interval: Numeric, deadline: Time?) -> Time?
    def child_workflow_poll_wake_at(current_time:, poll_interval:, deadline:)
      unless poll_interval.positive?
        return deadline if deadline

        return current_time + CHILD_WORKFLOW_AWAIT_PARK_SECONDS
      end

      poll_wake_at = current_time + poll_interval
      deadline ? [poll_wake_at, deadline].min : poll_wake_at
    end

    #: (Hash[String, Object?]?) -> Time?
    def recorded_child_workflow_await_deadline(recorded)
      payload = recorded&.fetch("payload", nil)
      return unless payload.is_a?(Hash)

      child_workflow = payload["child_workflow"]
      return unless child_workflow.is_a?(Hash)

      child_workflow["await_deadline_at"]
    end

    #: () -> WaitRequest?
    def recorded_child_workflow_await_wait_request
      scheduled = @replay_history.recorded_schedule(@next_command_id)
      payload = scheduled&.fetch("payload", nil)
      return unless payload.is_a?(Hash)
      return unless payload["name"] == child_workflow_command_name("workflow", "await")

      wait = payload["wait"]
      return unless wait.is_a?(Hash)

      WaitRequest.new(
        kind: wait.fetch("kind"),
        wake_at: wait["wake_at"],
        event_key: wait["event_key"],
        context: wait.fetch("context"),
      )
    end

    #: (?untyped, step: untyped, command_id: untyped) -> untyped
    def step_attributes(instance = nil, step:, command_id:)
      attributes = {
        "durababble.workflow.id" => @workflow_id,
        "durababble.step.name" => step.name,
        "durababble.step.index" => command_id,
        "durababble.worker.id" => @worker_id,
      }
      attributes["durababble.workflow.name"] = instance.class.workflow_name if instance
      attributes
    end

    #: (step: untyped, args: untyped, kwargs: untyped) -> untyped
    def step_command_shape(step:, args:, kwargs:)
      {
        "name" => step.name,
        "args" => args,
        "kwargs" => kwargs,
        "retry" => retry_shape(step.retry_policy),
      }
    end

    #: (name: untyped, wait_request: untyped, args: untyped, kwargs: untyped) -> untyped
    def wait_command_shape(name:, wait_request:, args:, kwargs:)
      {
        "name" => name,
        "args" => args,
        "kwargs" => kwargs,
        "wait" => {
          "kind" => wait_request.kind,
          "event_key" => wait_request.event_key,
          "wake_at" => replay_stable_wait_wake_at(name, wait_request),
          "context" => wait_request.context,
        },
      }
    end

    #: (name: String, child_workflow_name: String, child_workflow_id: String?, input: Object?, worker_pool: String, idempotency_key: String?, cancellation_policy: String) -> Hash[String, Object?]
    def child_workflow_start_shape(name:, child_workflow_name:, child_workflow_id:, input:, worker_pool:, idempotency_key:, cancellation_policy:)
      {
        "name" => name,
        "args" => [input],
        "kwargs" => {
          "id" => child_workflow_id,
          "worker_pool" => worker_pool,
          "idempotency_key" => idempotency_key,
          "cancellation_policy" => cancellation_policy,
        },
        "child_workflow" => {
          "operation" => "start",
          "workflow_name" => child_workflow_name,
          "workflow_id" => child_workflow_id,
          "worker_pool" => worker_pool,
          "cancellation_policy" => cancellation_policy,
          "idempotency_key" => idempotency_key,
        },
      }
    end

    #: (name: String, child_workflow_id: String, await_deadline: Time?) -> Hash[String, Object?]
    def child_workflow_observe_shape(name:, child_workflow_id:, await_deadline:)
      child_workflow = {
        "operation" => "observe",
        "workflow_id" => child_workflow_id,
      }
      child_workflow["await_deadline_at"] = await_deadline if await_deadline
      {
        "name" => name,
        "args" => [],
        "kwargs" => {},
        "child_workflow" => child_workflow,
      }
    end

    #: (name: String, target_kind: String, target_type: String, target_id: String, method_name: Symbol, rpc_kind: String, args: Array[Object?], kwargs: Hash[Symbol, Object?], idempotency_key: Object?) -> Hash[String, Object?]
    def handle_rpc_command_shape(name:, target_kind:, target_type:, target_id:, method_name:, rpc_kind:, args:, kwargs:, idempotency_key:)
      {
        "name" => name,
        "args" => args,
        "kwargs" => kwargs,
        "handle_rpc" => {
          "target_kind" => target_kind,
          "target_type" => target_type,
          "target_id" => target_id,
          "method" => method_name.to_s,
          "rpc_kind" => rpc_kind,
          "idempotency_key" => idempotency_key,
        },
      }
    end

    #: (untyped) -> untyped
    def retry_shape(retry_policy)
      {
        "initial_interval" => retry_policy.initial_interval,
        "backoff_coefficient" => retry_policy.backoff_coefficient,
        "maximum_interval" => retry_policy.maximum_interval,
        "maximum_attempts" => retry_policy.maximum_attempts,
        "schedule" => retry_policy.schedule,
        "non_retryable_errors" => retry_policy.non_retryable_errors.map(&:to_s),
      }
    end

    #: (target_kind: String, target_type: String, method_name: Symbol) -> String
    def handle_rpc_command_name(target_kind:, target_type:, method_name:)
      "handle_rpc:#{target_kind}:#{target_type}:#{method_name}"
    end

    #: (String, String) -> String
    def child_workflow_command_name(child_workflow_name, operation)
      "child_workflow:#{child_workflow_name}:#{operation}"
    end

    #: (target_kind: String, target_type: String, target_id: String, method_name: Symbol, rpc_kind: String, command_id: Integer) -> Hash[String, Object?]
    def handle_rpc_attributes(target_kind:, target_type:, target_id:, method_name:, rpc_kind:, command_id:)
      {
        "durababble.workflow.id" => @workflow_id,
        "durababble.step.name" => handle_rpc_command_name(target_kind:, target_type:, method_name:),
        "durababble.step.index" => command_id,
        "durababble.worker.id" => @worker_id,
        "durababble.handle_rpc.target_kind" => target_kind,
        "durababble.handle_rpc.target_type" => target_type,
        "durababble.handle_rpc.target_id" => target_id,
        "durababble.handle_rpc.method" => method_name.to_s,
        "durababble.handle_rpc.kind" => rpc_kind,
      }
    end

    #: (child_workflow_name: String, child_workflow_id: String?, command_id: Integer, operation: String) -> Hash[String, Object?]
    def child_workflow_attributes(child_workflow_name:, child_workflow_id:, command_id:, operation:)
      {
        "durababble.workflow.id" => @workflow_id,
        "durababble.step.name" => child_workflow_command_name(child_workflow_name, operation),
        "durababble.step.index" => command_id,
        "durababble.worker.id" => @worker_id,
        "durababble.child_workflow.name" => child_workflow_name,
        "durababble.child_workflow.id" => child_workflow_id,
        "durababble.child_workflow.operation" => operation,
      }
    end

    #: (Integer, Hash[String, Object?]) -> void
    def count_replay_step(command_id, attributes)
      return unless @replay_history.terminal_recorded?(command_id)

      observability_attributes = attributes #: as untyped
      Observability.count("durababble.workflow.replay.steps", observability_attributes)
    end

    #: (Integer) -> String
    def handle_rpc_idempotency_key(command_id)
      "durababble:v1:workflow:#{@workflow_id}:handle-rpc:#{command_id}"
    end

    #: (Integer) -> String
    def child_workflow_idempotency_key(command_id)
      "durababble:v1:workflow:#{@workflow_id}:child-workflow:#{command_id}"
    end

    #: (command_id: Integer, child_workflow_name: String, input: Object?, worker_pool: String, idempotency_key: String?) -> String
    def generated_child_workflow_id(command_id:, child_workflow_name:, input:, worker_pool:, idempotency_key:)
      digest = Digest::SHA256.hexdigest(Store::SERIALIZER.dump({
        "parent_workflow_id" => @workflow_id,
        "command_id" => command_id,
        "child_workflow_name" => child_workflow_name,
        "input" => input,
        "worker_pool" => worker_pool,
        "idempotency_key" => idempotency_key,
      }))
      "child-#{digest[0, 48]}"
    end

    #: (Symbol | String) -> String
    def normalize_child_cancellation_policy(policy)
      normalized = policy.to_s
      return normalized if ["request_cancel", "abandon"].include?(normalized)

      raise ArgumentError, "unknown child workflow cancellation policy: #{policy.inspect}"
    end

    #: (untyped, name: untyped, shape: untyped, event_budget: Integer) -> bool
    def schedule_command!(command_id, name:, shape:, event_budget:)
      return true if @replay_history.validate_scheduled_shape!(workflow_id: @workflow_id, command_id:, shape:)

      # [DURABABBLE-CONCURRENCY-1] Workflow fibers append ordered command history before side-effect execution.
      ensure_history_limit_allows!(additional_events: event_budget)
      @store.record_step_scheduled(
        workflow_id: @workflow_id,
        command_id:,
        name:,
        args: shape.fetch("args"),
        kwargs: shape.fetch("kwargs"),
        metadata: shape.reject { |key, _value| ["name", "args", "kwargs"].include?(key) },
        worker_id: @worker_id,
        event_index: @replay_history.allocate_event_index!,
      )
      crash!(:step_scheduled)
      @replay_history.remember_scheduled(command_id, step_name: name, shape:)
      @replay_history.reserve_events!(event_budget - 1)
      false
    end

    #: (additional_events: Integer) -> void
    def ensure_history_limit_allows!(additional_events:)
      max_history_events = Durababble.max_workflow_history_events
      projected_events = @replay_history.event_count + additional_events
      unless @history_warning_logged
        @history_warning_logged = Durababble.warn_workflow_history_events(
          workflow_id: @workflow_id,
          history_events: projected_events,
          max_history_events:,
        )
      end
      return if projected_events <= max_history_events

      raise WorkflowHistoryLimitExceeded.new(
        @workflow_id,
        history_events: projected_events,
        max_history_events: max_history_events,
      )
    end

    #: (untyped, step: untyped, attributes: untyped) { -> untyped } -> void
    def dispatch_command!(command_id, step:, attributes:, &block)
      @step_runner.dispatch(command_id, step:, attributes:, &block)
    end

    #: (untyped future, Integer command_id, name: String, wait_request: WaitRequest) -> void
    def record_wait_command!(future, command_id, name:, wait_request:)
      suspend_workflow = suspend_workflow_immediately?
      due_timer = wait_request.kind == "timer" && wait_request.wake_at && timer_due?(wait_request.wake_at)
      next_run_at = next_run_at_for_wait(wait_request)
      synchronize_store do
        @store.record_wait(
          workflow_id: @workflow_id,
          command_id:,
          name:,
          wait_request:,
          suspend_workflow: suspend_workflow && !due_timer,
          worker_id: @worker_id,
          next_run_at:,
          event_index: @replay_history.allocate_event_index!,
        )
        @replay_history.remember_step_waiting(command_id, name:, wait_request:)
      end
      crash!(:wait_recorded)
      if due_timer
        complete_due_wait_timer!(future, command_id, reserved_history_event: true)
        return
      end

      defer_workflow_suspension(command_id)
    end

    #: () -> bool
    def suspend_workflow_immediately?
      @workflow_task_count <= 1
    end

    #: (Integer) -> void
    def defer_workflow_suspension(command_id)
      @deferred_suspension_command_ids[command_id] = true
      schedule_deferred_suspension_check
    end

    #: () -> void
    def schedule_deferred_suspension_check
      return if @deferred_suspension_command_ids.empty?
      return if @deferred_suspension_check_scheduled

      @deferred_suspension_check_scheduled = true
      @root_task.async(transient: true) do
        Kernel.sleep(0)
        @deferred_suspension_check_scheduled = false
        reject_deferred_suspensions_if_quiescent!
      end
    end

    #: () -> void
    def reject_deferred_suspensions_if_quiescent!
      return if @deferred_suspension_command_ids.empty?
      # @blocked_workflow_tasks is always a subset of @workflow_tasks, so a registered
      # task is still runnable iff the live count exceeds the blocked count. This avoids
      # scanning every task on each defer/unregister/block to find one that is unblocked.
      return if @workflow_task_count > @blocked_workflow_tasks.size
      return if @futures.any? { |command_id, future| !@deferred_suspension_command_ids.key?(command_id) && !future.done? }

      command_ids = @deferred_suspension_command_ids.keys
      @deferred_suspension_command_ids.clear
      command_ids.each { |command_id| reject_wait_for_suspension(command_id) }
    end

    #: (Integer) -> void
    def reject_wait_for_suspension(command_id)
      error = WorkflowSuspended.new("workflow #{@workflow_id} suspended at command #{command_id}", next_run_at: @replay_history.earliest_unresolved_timer_wake_at)
      @futures.fetch(command_id).reject(error)
    end

    #: (WaitRequest) -> Object?
    def next_run_at_for_wait(wait_request)
      return unless ["timer", "child_workflow"].include?(wait_request.kind)

      earliest_time([@replay_history.earliest_unresolved_timer_wake_at, wait_request.wake_at].compact)
    end

    #: (untyped future, Integer command_id, ?reserved_history_event: bool) -> void
    def complete_due_wait_timer!(future, command_id, reserved_history_event: false)
      # [DURABABBLE-WAIT-1] Due workflow timers complete only while this worker
      # holds the workflow lease; timer readiness is represented by the
      # workflow row's next_run_at claim path, not by a separate wait scan.
      wait = @replay_history.waiting_timer_or_child_workflow(command_id)
      return unless wait

      return unless wait_ready?(wait)

      cancellation = synchronize_store { @store.workflow_cancellation(@workflow_id) }
      if cancellation
        future.reject(cancellation_error_from(cancellation))
        return
      end

      result = wait.fetch("context", {})
      synchronize_store do
        @store.record_step_completed(
          workflow_id: @workflow_id,
          command_id:,
          result:,
          worker_id: @worker_id,
          event_index: @replay_history.allocate_event_index!,
        )
        @replay_history.remember_step_completed(command_id, payload: result, reserved_history_event:)
      end
      future.resolve(result)
    end

    #: (Hash[String, Object?]) -> bool
    def wait_ready?(wait)
      wake_at = wait["wake_at"]
      return true if wait.fetch("kind", nil) == "child_workflow" && child_workflow_wait_ready?(wait)

      !!(wake_at && (timer_due?(wake_at) || claimed_wake_due?(wake_at)))
    end

    #: (Object) -> bool
    def claimed_wake_due?(wake_at)
      return false unless @claimed_next_run_at

      comparable_time(wake_at, durable: true) <= comparable_time(@claimed_next_run_at, durable: true)
    end

    #: (Hash[String, Object?]) -> bool
    def child_workflow_wait_ready?(wait)
      context = wait.fetch("context", {})
      return false unless context.is_a?(Hash)

      child_workflow_id = context["child_workflow_id"]
      return false unless child_workflow_id

      child = synchronize_store { @store.observe_child_workflow(child_workflow_id.to_s) }
      WorkflowStatus.terminal?(child)
    rescue KeyError
      false
    end

    #: (Object) -> bool
    def timer_due?(wake_at)
      now = synchronize_store { @store.current_time }
      comparable_time(wake_at, durable: true) <= comparable_time(now, durable: true)
    end

    #: (Object, ?durable: bool) -> untyped
    def comparable_time(value, durable: false)
      durable ? DurableTime.durable_comparable(value) : DurableTime.comparable(value)
    end

    #: (Array[Object]) -> Object?
    def earliest_time(values)
      values.min_by { |value| comparable_time(value) }
    end

    #: (untyped, untyped, ?interrupt_on_command: bool) -> untyped
    def await_command_future(future, command_id, interrupt_on_command: false)
      deliveries_seen = @workflow_commands_delivered_count
      loop do
        deliver_recorded_resolutions!(interrupt_on_command:)
        if interrupt_on_command
          deliver_workflow_commands_at_safe_point!
          # Interrupt when any command has been delivered since this wait began — including
          # one delivered by a concurrent task — so a satisfied condition is re-evaluated
          # rather than left parked behind this wait's already-rejected future.
          if @workflow_commands_delivered_count > deliveries_seen
            raise WorkflowCommandDelivered, "workflow #{@workflow_id} command delivered while waiting at command #{command_id}"
          end
        end

        return future.value if future.done?

        missing_command_id = @replay_history.next_undeliverable_command_id(@futures)
        if missing_command_id && !other_workflow_task_can_schedule?(Async::Task.current)
          message = "workflow #{@workflow_id} history resolved command #{missing_command_id} before command #{command_id}, " \
            "but current replay has not scheduled command #{missing_command_id}"
          raise ReplayDivergenceError, message
        end

        block_current_workflow_task { WorkflowDeterminism.allow_host_operations { future.wait } }
      end
    end

    #: (?interrupt_on_command: bool) -> void
    def deliver_recorded_resolutions!(interrupt_on_command: false)
      @replay_history.deliver_resolutions(@futures) do |event, future|
        command_id = event.fetch("command_id").to_s.to_i
        case event.fetch("kind")
        when "step_completed"
          future.resolve(event.fetch("payload"))
        when "step_waiting"
          cancellation = synchronize_store { @store.workflow_cancellation(@workflow_id) }
          if cancellation
            future.reject(cancellation_error_from(cancellation))
          elsif @replay_history.waiting_timer_or_child_workflow(command_id)
            @replay_history.forget_waiting_timer(command_id) if interrupt_on_command
            next if interrupt_on_command

            if suspend_workflow_immediately?
              future.reject(WorkflowSuspended.new("workflow #{@workflow_id} suspended at command #{command_id}", next_run_at: @replay_history.earliest_unresolved_timer_wake_at))
            else
              defer_workflow_suspension(command_id)
            end
          elsif suspend_workflow_immediately?
            future.reject(WorkflowSuspended.new("workflow #{@workflow_id} suspended at command #{command_id}", next_run_at: @replay_history.earliest_unresolved_timer_wake_at))
          else
            # On the original run this wait deferred suspension behind concurrent tasks. Replay
            # it the same way rather than rejecting outright: a workflow command recorded after
            # this step_waiting may still satisfy the condition before the workflow goes
            # quiescent. Quiescence (or the satisfying command) resolves the future from here.
            defer_workflow_suspension(command_id)
          end
        when "step_canceled"
          cancellation = synchronize_store { @store.workflow_cancellation(@workflow_id) }
          future.reject(cancellation_error_from(cancellation, fallback_reason: event.fetch("error")))
        when "step_failed"
          future.reject(step_failure_error_from(event))
        end
      end
    end

    #: (?limit: Integer) -> Integer
    def deliver_workflow_commands_at_safe_point!(limit: 10)
      return 0 if @delivering_workflow_command

      delivered = 0
      @delivering_workflow_command = true
      begin
        delivered += deliver_recorded_workflow_commands
        return delivered if @replay_history.blocked_recorded_workflow_command?
        return delivered if @replay_history.blocked_by_replay_history?

        while delivered < limit
          message = claim_next_workflow_command_message
          break unless message

          dispatch_workflow_command_message(message)
          delivered += 1
        end
        delivered
      ensure
        @delivering_workflow_command = false
        @workflow_commands_delivered_count += delivered
      end
    end

    #: () -> Integer
    def deliver_recorded_workflow_commands
      @replay_history.deliver_workflow_commands do |event|
        replay_workflow_command_event(event)
      end
    end

    #: () -> Hash[String, Object?]?
    def claim_next_workflow_command_message
      synchronize_store do
        @store.claim_next_workflow_command(
          worker_pool: @worker_pool,
          workflow_name: @workflow_class.workflow_name,
          workflow_id: @workflow_id,
          worker_id: @worker_id,
          lease_seconds: @lease_seconds,
        )
      end
    rescue LeaseConflict
      return unless workflow_running?

      raise
    end

    #: () -> bool
    def workflow_running?
      WorkflowStatus.running?(synchronize_store { @store.workflow(@workflow_id) })
    end

    #: (Hash[String, Object?]) -> void
    def dispatch_workflow_command_message(message)
      unless message.fetch("message_kind") == "workflow_command"
        fail_workflow_command_message(message, "Durababble::Error: unsupported workflow inbox message #{message.fetch("message_kind")}")
        return
      end

      method_name, args, kwargs = workflow_command_call_shape(message)
      retry_policy = @workflow_class.exposed_commands[method_name]
      unless retry_policy
        fail_workflow_command_message(message, "Durababble::WorkflowRpc::UnknownCommand: #{method_name}")
        return
      end

      result = invoke_workflow_command(method_name, args:, kwargs:)
      reserve_workflow_command_history_event!
      message_id = message.fetch("id").to_s
      synchronize_store do
        @store.complete_workflow_command(
          message_id:,
          workflow_id: @workflow_id,
          result:,
          worker_id: @worker_id,
          event_index: @replay_history.allocate_event_index!,
        )
      end
    rescue StandardError => e
      handle_workflow_command_error(message, retry_policy:, error: e)
    end

    #: (Hash[String, Object?]) -> void
    def replay_workflow_command_event(event)
      payload = event["payload"] || {}
      payload = payload #: as untyped
      method_name = (payload["method"] || event["name"]).to_sym
      args = payload.fetch("args", [])
      kwargs = payload.fetch("kwargs", {})

      unless @workflow_class.exposed_commands.key?(method_name)
        raise ReplayDivergenceError, "workflow #{@workflow_id} replay reached unknown workflow command #{method_name}"
      end

      case event.fetch("kind")
      when "workflow_command_completed"
        result = invoke_workflow_command(method_name, args:, kwargs:)
        # complete_workflow_command always records the result (include_result: true), so a
        # completed event without one is malformed history, not a divergence we can skip.
        unless payload.key?("result")
          raise ReplayDivergenceError, "workflow #{@workflow_id} replay reached workflow command #{method_name} with no recorded result"
        end
        return if result == payload.fetch("result")

        raise ReplayDivergenceError, "workflow #{@workflow_id} replay reached workflow command #{method_name} with a different result than recorded history"
      when "workflow_command_failed"
        begin
          invoke_workflow_command(method_name, args:, kwargs:)
        rescue StandardError => e
          expected = event["error"]
          return if expected.nil? || expected == "#{e.class}: #{e.message}"

          raise ReplayDivergenceError, "workflow #{@workflow_id} replay reached workflow command #{method_name} with a different error than recorded history"
        end
        raise ReplayDivergenceError, "workflow #{@workflow_id} replay expected workflow command #{method_name} to fail"
      end
    end

    #: (Hash[String, Object?]) -> [Symbol, Array[Object?], Hash[Symbol, Object?]]
    def workflow_command_call_shape(message)
      payload = message.fetch("payload")
      payload = payload #: as untyped
      method_name = (message["method_name"] || payload.fetch("method")).to_sym
      [method_name, payload.fetch("args", []), payload.fetch("kwargs", {})]
    end

    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?]) -> Object?
    def invoke_workflow_command(method_name, args:, kwargs:)
      kwargs.empty? ? @workflow.public_send(method_name, *args) : @workflow.public_send(method_name, *args, **kwargs)
    end

    #: (Hash[String, Object?], String) -> void
    def fail_workflow_command_message(message, error)
      reserve_workflow_command_history_event!
      message_id = message.fetch("id").to_s
      synchronize_store do
        @store.fail_workflow_command(
          message_id:,
          workflow_id: @workflow_id,
          error:,
          worker_id: @worker_id,
          event_index: @replay_history.allocate_event_index!,
        )
      end
    end

    #: (Hash[String, Object?], retry_policy: RetryPolicy?, error: StandardError) -> void
    def handle_workflow_command_error(message, retry_policy:, error:)
      serialized_error = "#{error.class}: #{error.message}"
      attempt_value = message.fetch("attempts") #: as untyped
      attempt_number = attempt_value.to_i
      if retry_policy&.retryable?(error, attempt_number:)
        retry_workflow_command_message(message, serialized_error, retry_policy.delay_for_attempt(attempt_number))
      else
        fail_workflow_command_message(message, serialized_error)
      end
    end

    #: (Hash[String, Object?], String, Numeric) -> void
    def retry_workflow_command_message(message, error, delay)
      reserve_workflow_command_history_event!
      message_id = message.fetch("id").to_s
      synchronize_store do
        @store.retry_workflow_command(
          message_id:,
          workflow_id: @workflow_id,
          error:,
          worker_id: @worker_id,
          ready_at: retry_run_at(delay),
          event_index: @replay_history.allocate_event_index!,
        )
      end
    end

    #: () -> void
    def reserve_workflow_command_history_event!
      ensure_history_limit_allows!(additional_events: 1)
      @replay_history.reserve_events!(1)
    end

    #: (Hash[String, Object?]) -> StandardError
    def step_failure_error_from(event)
      error = event.fetch("error").to_s
      payload = event["payload"]
      error_class = nil #: String?
      error_message = nil #: String?
      if payload.is_a?(Hash)
        error_class = payload["error_class"]&.to_s
        error_message = payload["error_message"]&.to_s if payload.key?("error_message")
      end
      error_class, error_message = parse_step_failure_error(error) unless error_class
      build_step_failure_error(error_class:, error_message:, fallback: error)
    end

    #: (String) -> [String?, String?]
    def parse_step_failure_error(error)
      # Persisted errors carry a trailing backtrace (see ErrorFormatting), so
      # only the first line holds the "Class: message" we want to reconstruct.
      first_line = error.lines.first&.chomp || error
      error_class, error_message = first_line.split(": ", 2)
      return [nil, nil] unless error_message

      [error_class, error_message]
    end

    #: (error_class: String?, error_message: String?, fallback: String) -> StandardError
    def build_step_failure_error(error_class:, error_message:, fallback:)
      klass = step_failure_error_class(error_class)
      return Error.new(fallback) unless klass

      klass.new(error_message || fallback) #: as StandardError
    rescue StandardError
      Error.new(fallback)
    end

    #: (String?) -> Class?
    def step_failure_error_class(class_name)
      return if class_name.nil? || class_name.empty?

      # Terminal step replay persists exception class names so crash recovery can
      # preserve user-visible rescue semantics.
      # rubocop:disable Sorbet/ConstantsFromStrings -- reconstructing persisted user exception classes
      klass = Object.const_get(class_name)
      # rubocop:enable Sorbet/ConstantsFromStrings
      klass if klass.is_a?(Class) && klass <= StandardError
    rescue NameError
      nil
    end

    #: (untyped, ?fallback_reason: untyped) -> untyped
    def cancellation_error_from(cancellation, fallback_reason: nil)
      @cancellation_delivered = true
      propagate_child_cancellation!(cancellation&.fetch("reason", fallback_reason))
      synchronize_store { @store.mark_workflow_cancellation_delivered(workflow_id: @workflow_id) } if cancellation
      reason = cancellation&.fetch("reason", fallback_reason)
      CancellationError.new(reason, workflow_id: @workflow_id)
    end

    #: (Object?) -> void
    def propagate_child_cancellation!(reason)
      children = synchronize_store { @store.child_workflow_rows_for_parent(parent_workflow_id: @workflow_id) }
      children.each do |child|
        next unless child.fetch("cancellation_policy") == "request_cancel"
        next if WorkflowStatus.terminal?(child)

        synchronize_store do
          @store.request_workflow_cancellation(
            workflow_id: child.fetch("child_workflow_id"),
            reason: reason.to_s.empty? ? "parent workflow #{@workflow_id} canceled" : reason,
          )
        end
      rescue KeyError
        next
      end
    end

    #: (untyped) -> bool
    def other_workflow_task_can_schedule?(current_task)
      @workflow_tasks.any? do |task, _registered|
        task != current_task && !@blocked_workflow_tasks.key?(task)
      end
    end

    #: (untyped) -> bool
    def deliver_cancellation_before_command?(shape)
      return false if @cancellation_delivered
      return false unless synchronize_store { @store.workflow_cancellation(@workflow_id) }

      !@replay_history.recorded_schedule_matches?(@next_command_id, shape)
    end

    #: () -> untyped
    def assert_workflow_lease!
      return if synchronize_store { @store.workflow_owned?(workflow_id: @workflow_id, worker_id: @worker_id) }

      # [DURABABBLE-LEASE-4] Stale workers cannot pass the final workflow/step commit fence.
      raise LeaseConflict, "workflow #{@workflow_id} lease expired or moved before state update"
    end

    #: (untyped) -> untyped
    def retry_run_at(delay)
      store_current_time + delay
    end

    #: (Numeric?) -> Time?
    def wait_condition_deadline(timeout)
      return unless timeout

      recorded_deadline = recorded_wait_condition_deadline
      return recorded_deadline if recorded_deadline

      WorkflowDeterminism.allow_host_operations { @store.current_time + timeout }
    end

    #: () -> Time?
    def recorded_wait_condition_deadline
      wait = recorded_wait_condition_wait
      context = wait&.fetch("context", nil)
      return unless context.is_a?(Hash)

      context["wait_condition_deadline_at"]
    end

    #: () -> Hash[String, Object?]?
    def recorded_wait_condition_wait
      scheduled = @replay_history.recorded_schedule(@next_command_id)
      payload = scheduled&.fetch("payload", nil)
      return unless payload.is_a?(Hash)
      return unless payload["name"] == "wait_condition"

      wait = payload["wait"]
      wait if wait.is_a?(Hash)
    end

    #: (Time?) -> Hash[String, Object?]
    def wait_condition_context(deadline)
      return {} if legacy_recorded_wait_condition_wait?

      deadline ? { "wait_condition_deadline_at" => deadline } : {}
    end

    #: () -> bool
    def legacy_recorded_wait_condition_wait?
      wait = recorded_wait_condition_wait
      return false unless wait

      context = wait["context"]
      !context.is_a?(Hash) || !context.key?("wait_condition_deadline_at")
    end

    #: (untyped, Time?) -> untyped
    def wait_condition_wake_at(timeout, deadline)
      return deadline if deadline

      WorkflowDeterminism.allow_host_operations do
        current_time = store_current_time
        timeout ? current_time + timeout : current_time + 1
      end
    end

    #: () -> untyped
    def store_current_time
      DurableTime.comparable(@store.current_time)
    end

    #: () -> bool
    def scheduled_history_for_next_command?
      !!@replay_history.recorded_schedule(@next_command_id)
    end

    #: (untyped, untyped) -> untyped
    def replay_stable_wait_wake_at(name, wait_request)
      return if ["sleep", "wait_condition"].include?(name.to_s)

      wait_request.wake_at
    end

    # Serializes framework store writes for this workflow execution so that concurrent
    # reactor fibers (steps, handle-RPC dispatch blocks, raw-Async fan-out tasks) can't
    # observe each other's partially-applied state changes. Each fiber checks out its OWN
    # ActiveRecord connection (Durababble requires
    # ActiveSupport::IsolatedExecutionState.isolation_level = :fiber — enforced at
    # Engine#execute entry); the mutex protects the in-memory bookkeeping (@futures,
    # @next_command_id, replay history reservations) plus the small SQL bursts that record
    # a command being scheduled.
    #
    # Load-bearing rule for any block reachable here: it must be SYNCHRONOUS and
    # NON-YIELDING. This is a plain Mutex; wrapping a sleep/await/cross-node RPC inside it
    # would serialize the whole workflow and risk a thread-level self-deadlock. Long-running
    # work (handle-RPC dispatch blocks calling wait_for_inbox_message, etc.) runs OUTSIDE
    # this mutex on purpose. Pool sizing: per-fiber checkout means each in-flight fan-out
    # branch consumes a connection, so the pool must be >= max concurrent in-flight
    # commands + 1.
    #: () { -> untyped } -> untyped
    def synchronize_store(&block)
      WorkflowDeterminism.allow_host_operations { @store_mutex.synchronize(&block) }
    end

    #: (untyped) -> untyped
    def crash!(point)
      raise InjectedCrash, "injected crash after #{point}" if @crash_after == point
    end

    # Operations the WorkflowStepRunner drives back through its owning execution.
    # Defined private above for internal callers; re-exported here so the step
    # runner can invoke them on the execution instead of being handed a bag of
    # lambdas.
    public(
      :synchronize_store,
      :raise_if_cancel_requested!,
      :assert_workflow_lease!,
      :suspend_workflow_immediately?,
      :defer_workflow_suspension,
      :next_run_at_for_wait,
      :timer_due?,
      :complete_due_wait_timer!,
      :retry_run_at,
      :crash!,
    )
  end
end
