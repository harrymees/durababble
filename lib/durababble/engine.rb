# typed: true
# frozen_string_literal: true

require "async"
require "async/condition"
require "thread"

Fiber.attr_accessor(:durababble_workflow_execution) unless Fiber.method_defined?(:durababble_workflow_execution)
Fiber.attr_accessor(:durababble_step_context) unless Fiber.method_defined?(:durababble_step_context)

module Durababble
  StepContext = Data.define(:workflow_id, :step_index, :attempt_number, :idempotency_key, :heartbeat)

  Heartbeat = Data.define(:cursor, :recorder) do
    #: (?untyped) -> untyped
    def record(cursor = self.cursor)
      recorder.call(cursor)
    end

    alias_method :heartbeat, :record
  end

  module WorkflowExecutionContext
    class << self
      #: () -> untyped
      def current
        fiber = Fiber.current #: as untyped
        fiber.durababble_workflow_execution
      end

      #: (untyped) { (?) -> untyped } -> untyped
      def with_current(execution, &block)
        fiber = Fiber.current #: as untyped
        previous = fiber.durababble_workflow_execution
        fiber.durababble_workflow_execution = execution
        block.call
      ensure
        fiber.durababble_workflow_execution = previous
      end
    end
  end

  module StepExecutionContext
    class << self
      #: () -> untyped
      def current
        fiber = Fiber.current #: as untyped
        fiber.durababble_step_context
      end

      #: (untyped) { (?) -> untyped } -> untyped
      def with_current(context, &block)
        fiber = Fiber.current #: as untyped
        previous = fiber.durababble_step_context
        fiber.durababble_step_context = context
        block.call
      ensure
        fiber.durababble_step_context = previous
      end
    end
  end

  module AsyncTaskWorkflowContextPatch
    #: () { (?) -> untyped } -> untyped
    def schedule(&block)
      task = self #: as untyped
      step_context = StepExecutionContext.current
      if task.transient?
        execution = WorkflowExecutionContext.current
        return super(&block) unless execution || step_context

        return super do
          WorkflowExecutionContext.with_current(nil) do
            StepExecutionContext.with_current(step_context) { block.call }
          end
        end
      end

      execution = WorkflowExecutionContext.current
      return super(&block) unless execution || step_context

      execution&.register_workflow_task(self)
      super do
        WorkflowExecutionContext.with_current(execution) do
          StepExecutionContext.with_current(step_context) { block.call }
        end
      ensure
        execution&.unregister_workflow_task(self)
      end
    end

    #: () -> untyped
    def wait
      execution = WorkflowExecutionContext.current
      return super() unless execution

      execution.block_current_workflow_task { super() }
    end
  end

  Async::Task.prepend(AsyncTaskWorkflowContextPatch) unless Async::Task < AsyncTaskWorkflowContextPatch

  class CommandFuture
    #: (untyped) -> void
    def initialize(command_id)
      @command_id = command_id
      @condition = Async::Condition.new
      @done = false
      @result = nil
      @error = nil
    end

    #: () -> bool
    def done?
      @done
    end

    #: () -> void
    def wait
      @condition.wait unless @done
    end

    #: () -> void
    def wake
      @condition.signal
    end

    #: () -> untyped
    def value
      raise @error if @error

      @result
    end

    #: (untyped) -> void
    def resolve(result)
      return if @done

      @done = true
      @result = result
      @condition.signal
    end

    #: (untyped) -> void
    def reject(error)
      return if @done

      @done = true
      @error = error
      @condition.signal
    end
  end

  class WorkflowExecution
    TERMINAL_HISTORY_KINDS = ["step_completed", "step_waiting", "step_canceled"].freeze

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
      @resolution_index = 0
      @scheduled_history = {}
      @terminal_history = {}
      @terminal_events = []
      @cancellation_delivered = false
      history.each { |event| index_history_event(event) }
      @terminal_events = @terminal_history.values.sort_by { |event| event.fetch("event_index").to_i }
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

      dispatch_command!(command_id, step:, shape:, &block) unless @terminal_history.key?(command_id)
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

      record_wait_command!(command_id, name:, wait_request:) unless @terminal_history.key?(command_id)
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
      retry_run_at(duration)
    end

    #: () -> void
    def validate_replay_complete!
      extra = @scheduled_history
        .keys
        .select { |command_id| command_id >= @next_command_id }
        .sort
      return if extra.empty?

      rendered = extra.map { |command_id| "#{command_id}:#{@scheduled_history.fetch(command_id).fetch("name")}" }.join(", ")
      raise NonDeterminismError, "workflow #{@workflow_id} replay completed without consuming durable command history: #{rendered}"
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
    def index_history_event(event)
      command_id = event["command_id"]&.to_i
      return unless command_id

      case event.fetch("kind")
      when "step_scheduled"
        @scheduled_history[command_id] = event
      when *TERMINAL_HISTORY_KINDS
        @terminal_history[command_id] = event
      end
    end

    #: (untyped) -> void
    def assert_workflow_task!(operation)
      return if @workflow_tasks.key?(Async::Task.current)

      raise Error, "#{operation} must run inside a Durababble-managed workflow task"
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
      scheduled = @scheduled_history[command_id]
      if scheduled
        validate_scheduled_shape!(scheduled, shape:, command_id:)
        return
      end

      @store.record_step_scheduled(
        workflow_id: @workflow_id,
        command_id:,
        name:,
        args: shape.fetch("args"),
        kwargs: shape.fetch("kwargs"),
        metadata: shape.reject { |key, _value| ["name", "args", "kwargs"].include?(key) },
      )
      crash!(:step_scheduled)
      @scheduled_history[command_id] = {
        "kind" => "step_scheduled",
        "command_id" => command_id,
        "name" => name,
        "payload" => shape,
      }
    end

    #: (untyped, shape: untyped, command_id: untyped) -> void
    def validate_scheduled_shape!(scheduled, shape:, command_id:)
      payload = scheduled.fetch("payload")
      return if payload == shape

      message = "workflow #{@workflow_id} replay reached command #{command_id} #{shape.fetch("name").inspect} " \
        "with a different durable command shape than recorded history"
      raise NonDeterminismError, message
    end

    #: (untyped, step: untyped, shape: untyped) { -> untyped } -> void
    def dispatch_command!(command_id, step:, shape:, &block)
      @root_task.async(transient: true) do |task|
        raise_if_cancel_requested!
        synchronize_store do
          @store.record_step_started(workflow_id: @workflow_id, command_id:, name: step.name)
        end
        crash!(:step_started)
        attempt_number = synchronize_store do
          @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == command_id }
        end
        step_context = StepContext.new(
          workflow_id: @workflow_id,
          step_index: command_id,
          attempt_number:,
          idempotency_key: "durababble:v1:workflow:#{@workflow_id}:step:#{command_id}",
          heartbeat: build_heartbeat(command_id),
        )
        @step_contexts[task] = step_context

        output = StepExecutionContext.with_current(step_context) { block.call }
        if output.is_a?(WaitRequest)
          assert_workflow_lease!
          suspend_workflow = suspend_workflow_immediately?
          synchronize_store do
            @store.record_wait(
              workflow_id: @workflow_id,
              command_id:,
              name: step.name,
              wait_request: output,
              suspend_workflow:,
            )
          end
          crash!(:wait_recorded)
          error = WorkflowSuspended.new("workflow #{@workflow_id} suspended at command #{command_id}")
          @futures.fetch(command_id).reject(error)
          next
        end

        assert_workflow_lease!
        synchronize_store { @store.record_step_completed(workflow_id: @workflow_id, command_id:, result: output) }
        crash!(:step_completed)
        raise_if_cancel_requested!
        @futures.fetch(command_id).resolve(output)
      rescue StandardError => e
        if e.is_a?(CancellationError)
          assert_workflow_lease!
          synchronize_store do
            @store.record_step_canceled(
              workflow_id: @workflow_id,
              command_id:,
              error: "#{e.class}: #{e.message}",
            )
          end
          @futures.fetch(command_id).reject(e)
          next
        end
        if e.is_a?(InjectedCrash) || e.is_a?(LeaseConflict)
          @futures.fetch(command_id).reject(e)
          next
        end
        if e.is_a?(WorkflowSuspended) || e.is_a?(StepRetryScheduled) || e.is_a?(NonDeterminismError)
          @futures.fetch(command_id).reject(e)
          next
        end

        error = handle_step_error(e, command_id:, step:)
        @futures.fetch(command_id).reject(error)
      ensure
        @step_contexts.delete(task)
      end
    end

    #: (untyped, name: untyped, wait_request: untyped) -> void
    def record_wait_command!(command_id, name:, wait_request:)
      assert_workflow_lease!
      suspend_workflow = suspend_workflow_immediately?
      synchronize_store do
        @store.record_wait(
          workflow_id: @workflow_id,
          command_id:,
          name:,
          wait_request:,
          suspend_workflow:,
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

    #: (untyped, command_id: untyped, step: untyped) -> untyped
    def handle_step_error(error, command_id:, step:)
      message = "#{error.class}: #{error.message}"
      assert_workflow_lease!
      synchronize_store { @store.record_step_failed(workflow_id: @workflow_id, command_id:, error: message) }
      attempt_number = synchronize_store do
        @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == command_id }
      end
      return error unless step.retry_policy.retryable?(error, attempt_number:)

      delay = step.retry_policy.delay_for_attempt(attempt_number)
      synchronize_store do
        @store.schedule_workflow_retry(workflow_id: @workflow_id, worker_id: @worker_id, run_at: retry_run_at(delay))
      end
      StepRetryScheduled.new(message)
    end

    #: (untyped, untyped) -> untyped
    def await_command_future(future, command_id)
      loop do
        deliver_recorded_resolutions!
        return future.value if future.done?

        missing_command_id = next_undeliverable_resolution_command_id
        if missing_command_id && !other_workflow_task_can_schedule?(Async::Task.current)
          message = "workflow #{@workflow_id} history resolved command #{missing_command_id} before command #{command_id}, " \
            "but current replay has not scheduled command #{missing_command_id}"
          raise NonDeterminismError, message
        end

        block_current_workflow_task { future.wait }
      end
    end

    #: () -> void
    def deliver_recorded_resolutions!
      while @resolution_index < @terminal_events.length
        event = @terminal_events.fetch(@resolution_index)
        command_id = event.fetch("command_id").to_i
        future = @futures[command_id]
        break unless future

        @resolution_index += 1
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

    #: () -> untyped
    def next_undeliverable_resolution_command_id
      return if @resolution_index >= @terminal_events.length

      command_id = @terminal_events.fetch(@resolution_index).fetch("command_id").to_i
      command_id unless @futures[command_id]
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

      scheduled = @scheduled_history[@next_command_id]
      return true unless scheduled

      scheduled.fetch("payload") != shape
    end

    #: (untyped) -> untyped
    def build_heartbeat(command_id)
      Heartbeat.new(
        cursor: synchronize_store { @store.step_heartbeat_cursor(workflow_id: @workflow_id, command_id:) },
        recorder: lambda do |cursor|
          renewed = synchronize_store do
            @store.heartbeat_step(workflow_id: @workflow_id, command_id:, worker_id: @worker_id, lease_seconds: @lease_seconds, cursor:)
          end
          raise LeaseConflict, "workflow #{@workflow_id} lease expired or moved before heartbeat" unless renewed

          raise_if_cancel_requested!
          true
        end,
      )
    end

    #: () -> untyped
    def assert_workflow_lease!
      return if synchronize_store { @store.workflow_owned?(workflow_id: @workflow_id, worker_id: @worker_id) }

      raise LeaseConflict, "workflow #{@workflow_id} lease expired or moved before state update"
    end

    #: (untyped) -> untyped
    def retry_run_at(delay)
      @store.current_time + delay
    end

    #: (untyped) -> untyped
    def wait_condition_wake_at(timeout)
      timeout ? @store.current_time + timeout : @store.current_time + 1
    end

    #: () -> bool
    def scheduled_history_for_next_command?
      @scheduled_history.key?(@next_command_id)
    end

    #: (untyped, untyped) -> untyped
    def replay_stable_wait_wake_at(name, wait_request)
      return if ["sleep", "wait_condition"].include?(name.to_s)

      wait_request.wake_at
    end

    #: () { -> untyped } -> untyped
    def synchronize_store(&block)
      @store_mutex.synchronize(&block)
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
