# typed: true
# frozen_string_literal: true

require "async"
require "thread"

require_relative "command_future"
require_relative "execution_context"
require_relative "workflow_determinism"
require_relative "workflow_replay_history"
require_relative "workflow_step_runner"

module Durababble
  class WorkflowExecution
    #: (store: untyped, workflow_id: untyped, worker_id: untyped, lease_seconds: untyped, history: untyped, root_task: untyped, ?crash_after: untyped) -> void
    def initialize(store:, workflow_id:, worker_id:, lease_seconds:, history:, root_task:, crash_after: nil)
      @store = store
      @workflow_id = workflow_id
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @root_task = root_task
      @crash_after = crash_after
      @next_command_id = 0
      @futures = {}
      @workflow_tasks = {}
      @blocked_workflow_tasks = {}
      @workflow_task_count = 0
      @step_contexts = {}
      @store_mutex = Mutex.new
      @replay_history = WorkflowReplayHistory.new(history)
      @cancellation_delivered = false
      @step_runner = WorkflowStepRunner.new(
        store: @store,
        workflow_id: @workflow_id,
        worker_id: @worker_id,
        lease_seconds: @lease_seconds,
        root_task: @root_task,
        futures: @futures,
        step_contexts: @step_contexts,
        synchronize_store: ->(&block) { synchronize_store(&block) },
        raise_if_cancel_requested: -> { raise_if_cancel_requested! },
        assert_workflow_lease: -> { assert_workflow_lease! },
        suspend_workflow_immediately: -> { suspend_workflow_immediately? },
        retry_run_at: ->(delay) { retry_run_at(delay) },
        crash: ->(point) { crash!(point) },
      )
      register_workflow_task(root_task)
    end

    #: () -> bool
    def cancellation_delivered?
      @cancellation_delivered
    end

    #: () -> untyped
    def step_context
      StepExecutionContext.current || @step_contexts[Async::Task.current]
    end

    #: (untyped) -> void
    def register_workflow_task(task)
      return if @workflow_tasks.key?(task)

      @workflow_tasks[task] = true
      @workflow_task_count += 1
    end

    #: (untyped) -> void
    def unregister_workflow_task(task)
      return unless @workflow_tasks.delete(task)

      @blocked_workflow_tasks.delete(task)
      @workflow_task_count -= 1
      @futures.each_value(&:wake)
    end

    #: () { (?) -> untyped } -> untyped
    def block_current_workflow_task(&block)
      task = Async::Task.current
      return block.call unless @workflow_tasks.key?(task)

      @blocked_workflow_tasks[task] = true
      @futures.each_value(&:wake)
      block.call
    ensure
      @blocked_workflow_tasks.delete(task) if task
      @futures.each_value(&:wake) if task
    end

    #: (untyped, method_name: untyped, args: untyped, kwargs: untyped) { -> untyped } -> untyped
    def call_step(instance, method_name:, args:, kwargs:, &block)
      assert_workflow_task!("durable step #{method_name}")
      step = instance.class.step_definition(method_name)
      shape = step_command_shape(step:, args:, kwargs:)
      raise_if_cancel_requested! if deliver_cancellation_before_command?(shape)
      command_id, future = synchronize_store do
        command_id = @next_command_id
        @next_command_id += 1
        future = CommandFuture.new(command_id)
        @futures[command_id] = future
        schedule_command!(command_id, name: step.name, shape:)
        [command_id, future]
      end
      attributes = step_attributes(instance, step:, command_id:)
      # [DURABABBLE-STEP-1] Completed commands resolve from durable replay history instead of rerunning user step code.
      Observability.count("durababble.workflow.replay.steps", attributes) if @replay_history.terminal_recorded?(command_id)

      dispatch_command!(command_id, step:, attributes:, &block) unless @replay_history.terminal_recorded?(command_id)
      deliver_recorded_resolutions!
      result = await_command_future(future, command_id)
      raise_if_cancel_requested!
      result
    end

    #: (untyped, name: untyped, ?args: untyped, ?kwargs: untyped) -> untyped
    def call_wait(wait_request, name:, args: [], kwargs: {})
      assert_workflow_task!("durable wait #{name}")
      shape = wait_command_shape(name:, wait_request:, args:, kwargs:)
      raise_if_cancel_requested! if deliver_cancellation_before_command?(shape)
      command_id, future = synchronize_store do
        command_id = @next_command_id
        @next_command_id += 1
        future = CommandFuture.new(command_id)
        @futures[command_id] = future
        schedule_command!(command_id, name:, shape:)
        [command_id, future]
      end

      record_wait_command!(command_id, name:, wait_request:) unless @replay_history.terminal_recorded?(command_id)
      deliver_recorded_resolutions!
      result = await_command_future(future, command_id)
      raise_if_cancel_requested!
      result
    end

    #: (?timeout: untyped) { -> bool } -> bool
    def wait_condition(timeout: nil, &block)
      loop do
        if scheduled_history_for_next_command?
          wait_request = WaitRequest.new(
            kind: "timer",
            wake_at: wait_condition_wake_at(timeout),
            event_key: nil,
            context: {},
          )
          call_wait(wait_request, name: "wait_condition", kwargs: { timeout: })
          return !!block.call if timeout

          next
        end

        raise_if_cancel_requested!
        return true if block.call

        wait_request = WaitRequest.new(
          kind: "timer",
          wake_at: wait_condition_wake_at(timeout),
          event_key: nil,
          context: {},
        )
        call_wait(wait_request, name: "wait_condition", kwargs: { timeout: })
        return !!block.call if timeout
      end
    end

    #: (untyped) -> untyped
    def timer_after(duration)
      WorkflowDeterminism.allow_host_operations { retry_run_at(duration) }
    end

    #: () -> void
    def validate_replay_complete!
      @replay_history.validate_complete!(workflow_id: @workflow_id, next_command_id: @next_command_id)
    end

    private

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

    #: (untyped, name: untyped, shape: untyped) -> void
    def schedule_command!(command_id, name:, shape:)
      return if @replay_history.validate_scheduled_shape!(workflow_id: @workflow_id, command_id:, shape:)

      # [DURABABBLE-CONCURRENCY-1] Workflow fibers append ordered command history before side-effect execution.
      @store.record_step_scheduled(
        workflow_id: @workflow_id,
        command_id:,
        name:,
        args: shape.fetch("args"),
        kwargs: shape.fetch("kwargs"),
        metadata: shape.reject { |key, _value| ["name", "args", "kwargs"].include?(key) },
        worker_id: @worker_id,
      )
      crash!(:step_scheduled)
      @replay_history.remember_scheduled(command_id, step_name: name, shape:)
    end

    #: (untyped, step: untyped, attributes: untyped) { -> untyped } -> void
    def dispatch_command!(command_id, step:, attributes:, &block)
      @step_runner.dispatch(command_id, step:, attributes:, &block)
    end

    #: (untyped, name: untyped, wait_request: untyped) -> void
    def record_wait_command!(command_id, name:, wait_request:)
      suspend_workflow = suspend_workflow_immediately?
      synchronize_store do
        @store.record_wait(
          workflow_id: @workflow_id,
          command_id:,
          name:,
          wait_request:,
          suspend_workflow:,
          worker_id: @worker_id,
        )
      end
      crash!(:wait_recorded)
      error = WorkflowSuspended.new("workflow #{@workflow_id} suspended at command #{command_id}")
      @futures.fetch(command_id).reject(error)
    end

    #: () -> bool
    def suspend_workflow_immediately?
      @workflow_task_count <= 1
    end

    #: (untyped, untyped) -> untyped
    def await_command_future(future, command_id)
      loop do
        deliver_recorded_resolutions!
        return future.value if future.done?

        missing_command_id = @replay_history.next_undeliverable_command_id(@futures)
        if missing_command_id && !other_workflow_task_can_schedule?(Async::Task.current)
          message = "workflow #{@workflow_id} history resolved command #{missing_command_id} before command #{command_id}, " \
            "but current replay has not scheduled command #{missing_command_id}"
          raise NonDeterminismError, message
        end

        block_current_workflow_task { WorkflowDeterminism.allow_host_operations { future.wait } }
      end
    end

    #: () -> void
    def deliver_recorded_resolutions!
      @replay_history.deliver_resolutions(@futures) do |event, future|
        command_id = event.fetch("command_id").to_i
        case event.fetch("kind")
        when "step_completed"
          future.resolve(event.fetch("payload"))
        when "step_waiting"
          cancellation = synchronize_store { @store.workflow_cancellation(@workflow_id) }
          if cancellation
            future.reject(cancellation_error_from(cancellation))
          else
            future.reject(WorkflowSuspended.new("workflow #{@workflow_id} suspended at command #{command_id}"))
          end
        when "step_canceled"
          cancellation = synchronize_store { @store.workflow_cancellation(@workflow_id) }
          future.reject(cancellation_error_from(cancellation, fallback_reason: event.fetch("error")))
        when "step_failed"
          future.reject(Error.new(event.fetch("error")))
        end
      end
    end

    #: (untyped, ?fallback_reason: untyped) -> untyped
    def cancellation_error_from(cancellation, fallback_reason: nil)
      @cancellation_delivered = true
      synchronize_store { @store.mark_workflow_cancellation_delivered(workflow_id: @workflow_id) } if cancellation
      reason = cancellation&.fetch("reason", fallback_reason)
      CancellationError.new(reason, workflow_id: @workflow_id)
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
      @store.current_time + delay
    end

    #: (untyped) -> untyped
    def wait_condition_wake_at(timeout)
      WorkflowDeterminism.allow_host_operations do
        timeout ? @store.current_time + timeout : @store.current_time + 1
      end
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

    #: () { -> untyped } -> untyped
    def synchronize_store(&block)
      WorkflowDeterminism.allow_host_operations { @store_mutex.synchronize(&block) }
    end

    #: (untyped) -> untyped
    def crash!(point)
      raise InjectedCrash, "injected crash after #{point}" if @crash_after == point
    end
  end
end
