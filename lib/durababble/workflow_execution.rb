# typed: true
# frozen_string_literal: true

require "async"
require "thread"

require_relative "command_future"
require_relative "execution_context"
require_relative "workflow_replay_history"

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
      shape = command_shape(step:, args:, kwargs:)
      raise_if_cancel_requested! if deliver_cancellation_before_command?(shape)
      command_id, future = synchronize_store do
        command_id = @next_command_id
        @next_command_id += 1
        future = CommandFuture.new(command_id)
        @futures[command_id] = future
        schedule_command!(command_id, step:, shape:)
        [command_id, future]
      end
      attributes = step_attributes(instance, step:, command_id:)
      Observability.count("durababble.workflow.replay.steps", attributes) if @replay_history.terminal_recorded?(command_id)

      dispatch_command!(command_id, step:, shape:, attributes:, &block) unless @replay_history.terminal_recorded?(command_id)
      deliver_recorded_resolutions!
      result = await_command_future(future, command_id)
      raise_if_cancel_requested!
      result
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
    def command_shape(step:, args:, kwargs:)
      {
        "name" => step.name,
        "args" => args,
        "kwargs" => kwargs,
        "retry" => retry_shape(step.retry_policy),
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

    #: (untyped, step: untyped, shape: untyped) -> void
    def schedule_command!(command_id, step:, shape:)
      return if @replay_history.validate_scheduled_shape!(workflow_id: @workflow_id, command_id:, shape:)

      @store.record_step_scheduled(
        workflow_id: @workflow_id,
        command_id:,
        name: step.name,
        args: shape.fetch("args"),
        kwargs: shape.fetch("kwargs"),
        metadata: { "retry" => shape.fetch("retry") },
      )
      crash!(:step_scheduled)
      @replay_history.remember_scheduled(command_id, step_name: step.name, shape:)
    end

    #: (untyped, step: untyped, shape: untyped, attributes: untyped) { -> untyped } -> void
    def dispatch_command!(command_id, step:, shape:, attributes:, &block)
      @root_task.async(transient: true) do |task|
        raise_if_cancel_requested!
        synchronize_store do
          @store.record_step_started(workflow_id: @workflow_id, command_id:, name: step.name)
        end
        crash!(:step_started)
        attempt_number = synchronize_store do
          @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == command_id }
        end
        attempt_attributes = attributes.merge("durababble.step.attempt" => attempt_number)
        Observability.count("durababble.workflow.step.attempts", attempt_attributes)
        step_context = StepContext.new(
          workflow_id: @workflow_id,
          step_index: command_id,
          attempt_number:,
          idempotency_key: "durababble:v1:workflow:#{@workflow_id}:step:#{command_id}",
          heartbeat: build_heartbeat(command_id),
        )
        @step_contexts[task] = step_context

        Observability.trace("durababble.workflow.step", attempt_attributes) do
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
          Observability.count("durababble.workflow.step.successes", attempt_attributes)
          crash!(:step_completed)
          raise_if_cancel_requested!
          @futures.fetch(command_id).resolve(output)
        end
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

        error = handle_step_error(e, command_id:, step:, attributes:)
        @futures.fetch(command_id).reject(error)
      ensure
        @step_contexts.delete(task)
      end
    end

    #: () -> bool
    def suspend_workflow_immediately?
      @workflow_task_count <= 1
    end

    #: (untyped, command_id: untyped, step: untyped, attributes: untyped) -> untyped
    def handle_step_error(error, command_id:, step:, attributes:)
      message = "#{error.class}: #{error.message}"
      assert_workflow_lease!
      synchronize_store { @store.record_step_failed(workflow_id: @workflow_id, command_id:, error: message) }
      attempt_number = synchronize_store do
        @store.step_attempts_for(@workflow_id).count { |attempt| attempt.fetch("position").to_i == command_id }
      end
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
        @store.schedule_workflow_retry(workflow_id: @workflow_id, worker_id: @worker_id, run_at: retry_run_at(delay))
      end
      StepRetryScheduled.new(message)
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

        block_current_workflow_task { future.wait }
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

    #: () -> untyped
    def assert_workflow_lease!
      return if synchronize_store { @store.workflow_owned?(workflow_id: @workflow_id, worker_id: @worker_id) }

      raise LeaseConflict, "workflow #{@workflow_id} lease expired or moved before state update"
    end

    #: (untyped) -> untyped
    def retry_run_at(delay)
      @store.current_time + delay
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
end
