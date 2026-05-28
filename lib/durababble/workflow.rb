# typed: true
# frozen_string_literal: true

require "securerandom"
require "digest"

require_relative "durable_method_dsl"
require_relative "execution_context"

module Durababble
  class Step
    #: String
    attr_reader :name
    #: RetryPolicy
    attr_reader :retry_policy

    #: (name: String | Symbol, retry_policy: RetryPolicy, ?body: Proc?) -> void
    def initialize(name:, retry_policy:, body: nil)
      @name = name.to_s
      @retry_policy = retry_policy
      @body = body
    end

    #: (*Object?, **Object?) -> Object?
    def call(*args, **kwargs)
      execution = WorkflowExecutionContext.current || raise(Error, "durable step #{name} called outside workflow execution")

      execution.call_step_object(self, args:, kwargs:)
    end

    #: (Object, args: Array[Object?], kwargs: Hash[Symbol, Object?]) -> Object?
    def call_body(receiver, args:, kwargs:)
      body = @body || raise(Error, "durable step #{name} has no callable body")
      args = args #: as untyped
      kwargs = kwargs #: as untyped
      receiver = receiver #: as untyped
      receiver.instance_exec(*args, **kwargs, &body)
    end
  end

  class StepRetryScheduled < Error; end
  class WorkflowSuspended < Error
    #: Object?
    attr_reader :next_run_at

    #: (?String, ?next_run_at: Object?) -> void
    def initialize(message = "workflow suspended", next_run_at: nil)
      @next_run_at = next_run_at
      super(message)
    end
  end
  class WorkflowCommandDelivered < Error; end

  class Workflow
    extend DurableMethodDSL

    class << self
      #: Hash[Symbol, Step]
      attr_reader :steps

      #: (Class) -> void
      def inherited(subclass)
        super
        subclass.instance_variable_set(:@steps, {})
        subclass.instance_variable_set(:@step_order, [])
        initialize_durable_method_dsl(subclass)
      end

      #: (?String?) -> String
      def workflow_name(value = nil)
        @workflow_name = String(value) if value
        ruby_name = Module.instance_method(:name).bind_call(self)
        @workflow_name || underscore((ruby_name || object_id.to_s).split("::").last)
      end

      #: () -> String
      def name
        workflow_name
      end

      #: (Object?, ?id: String?, ?store: Store?, ?engine: Engine?, ?worker_pool: String?, ?idempotency_key: String?, ?cancellation: Symbol | String?) -> (String | ChildWorkflowHandle)
      def enqueue(input, id: nil, store: nil, engine: nil, worker_pool: nil, idempotency_key: nil, cancellation: nil)
        raise Error, "cannot start workflows from an exposed query" if ObjectQueryExecutionContext.current

        if (execution = WorkflowExecutionContext.current)
          raise ArgumentError, "workflow child enqueue cannot specify store: or engine:" if store || engine

          return execution.call_child_workflow_start(
            self,
            input,
            id:,
            worker_pool:,
            idempotency_key:,
            cancellation_policy: cancellation.nil? ? :request_cancel : cancellation,
          )
        end

        if (object = ObjectCommandExecutionContext.current)
          raise ArgumentError, "object-command workflow enqueue cannot specify store: or engine:" if store || engine

          object = object #: as untyped
          return object.start_workflow(
            self,
            input,
            id:,
            worker_pool:,
            idempotency_key:,
            cancellation: cancellation.nil? ? :abandon : cancellation,
          )
        end

        raise ArgumentError, "idempotency_key: is only supported when enqueue is called from workflow execution or an object command" unless idempotency_key.nil?
        raise ArgumentError, "cancellation: is only supported when enqueue is called from workflow execution or an object command" unless cancellation.nil?

        if worker_pool
          workflow_id = id || SecureRandom.uuid
          Durababble.store_for(store:, engine:).enqueue_workflow(name: workflow_name, input:, id: workflow_id, worker_pool:)
        else
          Durababble.engine_for(store:, engine:).enqueue(self, input:, id:)
        end
      end

      #: (Object?, ?id: String?, ?store: Store?, ?engine: Engine?, ?worker_pool: String?, ?idempotency_key: String?, ?cancellation: Symbol | String?) -> (WorkflowRef | ChildWorkflowHandle)
      def start(input, id: nil, store: nil, engine: nil, worker_pool: nil, idempotency_key: nil, cancellation: nil)
        raise Error, "cannot start workflows from an exposed query" if ObjectQueryExecutionContext.current

        if (execution = WorkflowExecutionContext.current)
          raise ArgumentError, "workflow child start cannot specify store: or engine:" if store || engine

          return execution.call_child_workflow_start(
            self,
            input,
            id:,
            worker_pool:,
            idempotency_key:,
            cancellation_policy: cancellation.nil? ? :request_cancel : cancellation,
          )
        end

        if (object = ObjectCommandExecutionContext.current)
          raise ArgumentError, "object-command workflow start cannot specify store: or engine:" if store || engine

          object = object #: as untyped
          return object.start_workflow(
            self,
            input,
            id:,
            worker_pool:,
            idempotency_key:,
            cancellation: cancellation.nil? ? :abandon : cancellation,
          )
        end

        raise ArgumentError, "idempotency_key: is only supported when start is called from workflow execution or an object command" unless idempotency_key.nil?
        raise ArgumentError, "cancellation: is only supported when start is called from workflow execution or an object command" unless cancellation.nil?

        if worker_pool
          resolved_store = Durababble.store_for(store:, engine:)
          workflow_id = id || SecureRandom.uuid
          resolved_store.enqueue_workflow(name: workflow_name, input:, id: workflow_id, worker_pool:)
          handle(workflow_id, store: resolved_store, worker_pool:)
        else
          resolved_engine = Durababble.engine_for(store:, engine:)
          workflow_id = resolved_engine.enqueue(self, input:, id:)
          handle(workflow_id, engine: resolved_engine)
        end
      end

      #: (String, ?store: Store?, ?engine: Engine?, ?worker_pool: String?) -> WorkflowRef
      def at(workflow_id, store: nil, engine: nil, worker_pool: nil)
        handle(workflow_id, store:, engine:, worker_pool:)
      end

      #: (String, ?store: Store?, ?engine: Engine?, ?worker_pool: String?) -> WorkflowRef
      def handle(workflow_id, store: nil, engine: nil, worker_pool: nil)
        WorkflowRef.new(self, workflow_id, store: Durababble.store_for(store:, engine:), worker_pool:)
      end

      #: (?Symbol?, **Object?) -> Symbol?
      def step(method_name = nil, **options)
        retry_policy = options.fetch(:retry_policy, options[:retry])
        if method_name
          register_step(method_name, retry_policy:)
          method_name
        else
          set_pending_durable_macro(:step, retry_policy:)
        end
      end

      #: () -> Array[Symbol]
      def step_order
        @step_order ||= []
      end

      #: (Symbol | String) -> Step
      def step_definition(method_name)
        @steps.fetch(method_name.to_sym)
      end

      private

      #: (Symbol, Symbol, Hash[Symbol, Object?]) -> Symbol?
      def handle_pending_durable_macro(kind, method_name, options)
        return register_step(method_name, **options) if kind == :step

        super
      end

      #: (Symbol | String, ?retry_policy: Object?) -> Symbol
      def register_step(method_name, retry_policy: nil)
        method_name = method_name.to_sym
        retry_policy = retry_policy #: as untyped
        @steps[method_name] = Step.new(name: method_name.to_s, retry_policy: RetryPolicy.from(retry_policy))
        @step_order << method_name unless @step_order.include?(method_name)
        wrap_step_method(method_name)
        method_name
      end

      #: (Symbol) -> void
      def wrap_step_method(method_name)
        original = "__durababble_original_#{method_name}".to_sym
        return if method_defined?(original)

        @__durababble_wrapping = true
        alias_method(original, method_name)
        define_method(method_name) do |*args, **kwargs, &block|
          workflow = self #: as untyped
          raise Error, "cannot call workflow steps from an exposed query" if WorkflowQueryContext.current

          execution = workflow.__durababble_execution__
          execution.call_step(workflow, method_name:, args:, kwargs:) do
            workflow.send(original, *args, **kwargs, &block)
          end
        end
      ensure
        @__durababble_wrapping = false
      end
    end

    #: (WorkflowExecution?) -> WorkflowExecution?
    attr_writer :__durababble_execution__

    #: () -> WorkflowExecution
    def __durababble_execution__
      location = caller_locations(1, 1)&.first
      label = location&.label || "unknown"
      @__durababble_execution__ || raise(Error, "durable step #{self.class.name}##{label} called outside workflow execution")
    end

    #: () -> StepContext
    def step_context
      __durababble_execution__.step_context
    end

    #: (Time, ?Object?) -> Object?
    def wait_until(time, context = {})
      raise Error, "cannot schedule workflow waits from an exposed query" if WorkflowQueryContext.current

      Durababble.wait_until(time, context)
    end

    #: (Time, ?Object?) -> Object?
    def sleep_until(time, context = {})
      raise Error, "cannot schedule workflow waits from an exposed query" if WorkflowQueryContext.current

      Durababble.sleep_until(time, context)
    end

    #: (Numeric, ?Object?) -> Object?
    def sleep(duration, context = {})
      raise Error, "cannot schedule workflow waits from an exposed query" if WorkflowQueryContext.current

      Durababble.sleep(duration, context)
    end

    #: (?timeout: Numeric?) { -> bool } -> bool
    def wait_condition(timeout: nil, &block)
      raise Error, "cannot schedule workflow waits from an exposed query" if WorkflowQueryContext.current

      Durababble.wait_condition(timeout:, &block)
    end

    #: (Class, Object?, ?id: String?, ?worker_pool: String?, ?idempotency_key: String?, ?cancellation: Symbol | String) -> ChildWorkflowHandle
    def start_child(workflow_class, input, id: nil, worker_pool: nil, idempotency_key: nil, cancellation: :request_cancel)
      raise Error, "cannot start child workflows from an exposed query" if @__durababble_query_context

      __durababble_execution__.call_child_workflow_start(
        workflow_class,
        input,
        id:,
        worker_pool:,
        idempotency_key:,
        cancellation_policy: cancellation,
      )
    end

    alias_method :start_workflow, :start_child
  end

  class ChildWorkflowHandle
    TERMINAL_FAILURE_ERRORS = {
      "failed" => ChildWorkflowFailed,
      "canceled" => ChildWorkflowCanceled,
      "terminated" => ChildWorkflowTerminated,
    }.freeze

    #: (Class, String, store: Store, worker_pool: String, cancellation_policy: String) -> void
    def initialize(workflow_class, workflow_id, store:, worker_pool:, cancellation_policy:)
      @workflow_class = workflow_class #: as untyped
      @workflow_id = workflow_id
      @store = store #: as untyped
      @worker_pool = worker_pool
      @cancellation_policy = cancellation_policy
    end

    #: String
    attr_reader :workflow_id
    #: String
    attr_reader :worker_pool
    #: String
    attr_reader :cancellation_policy

    #: () -> WorkflowRef
    def ref
      @workflow_class.handle(@workflow_id, store: @store, worker_pool: @worker_pool)
    end

    #: () -> String
    def status
      if (execution = WorkflowExecutionContext.current)
        return execution.call_child_workflow_observe(self).fetch("status").to_s
      end

      @store.observe_child_workflow(@workflow_id).fetch("status").to_s
    end

    #: () -> Object?
    def result
      if (execution = WorkflowExecutionContext.current)
        return execution.await_child_workflow(self, poll_interval: 1, timeout: nil)
      end

      @store.observe_child_workflow(@workflow_id)["result"]
    end

    #: () -> String?
    def error
      if (execution = WorkflowExecutionContext.current)
        error = execution.call_child_workflow_observe(self)["error"]
        return error&.to_s
      end

      error = @store.observe_child_workflow(@workflow_id)["error"]
      error&.to_s
    end

    #: (?poll_interval: Numeric, ?timeout: Numeric?) -> Object?
    def await(poll_interval: 1, timeout: nil)
      if (execution = WorkflowExecutionContext.current)
        return execution.await_child_workflow(self, poll_interval:, timeout:)
      end

      deadline = timeout && Time.now + timeout
      loop do
        observed = @store.observe_child_workflow(@workflow_id)
        return await_result_from(observed) if WorkflowStatus.terminal?(observed)
        raise CommandTimeout, "timed out waiting for child workflow #{@workflow_id}" if deadline && Time.now >= deadline

        sleep(poll_interval)
      end
    end

    #: (?reason: Object?) -> Run
    def cancel(reason: nil)
      ref.cancel(reason:)
    end

    #: (?reason: Object?) -> Run
    def terminate(reason: nil)
      ref.terminate(reason:)
    end

    #: (Symbol, *Object?, **Object?) ?{ (Object?) -> Object? } -> Object?
    def method_missing(method_name, *args, **kwargs, &block)
      reference = ref #: as untyped
      return reference.public_send(method_name, *args, **kwargs, &block) if reference.respond_to?(method_name)

      super
    end

    #: (Symbol, ?bool) -> bool
    def respond_to_missing?(method_name, include_private = false)
      reference = ref #: as untyped
      reference.respond_to?(method_name, include_private) || super
    end

    #: (Hash[String, Object?]) -> Object?
    def await_result_from(observed)
      status = observed.fetch("status")
      return observed["result"] if status == "completed"

      error_class = TERMINAL_FAILURE_ERRORS.fetch(status, ChildWorkflowError)
      error = observed["error"]
      message = error.to_s.empty? ? "child workflow #{@workflow_id} #{status}" : error.to_s
      raise error_class, message
    end
  end

  class WorkflowRef
    COMMAND_WAIT_TIMEOUT_SLACK_SECONDS = 10

    #: (Class, String, store: Store, ?worker_pool: String?) -> void
    def initialize(workflow_class, workflow_id, store:, worker_pool: nil)
      @workflow_class = workflow_class #: as untyped
      @workflow_id = workflow_id
      @store = store
      @worker_pool = worker_pool
    end

    #: String
    attr_reader :workflow_id

    #: () -> String
    def status
      if (execution = WorkflowExecutionContext.current)
        rpc_status = execution.call_handle_rpc(
          target_kind: "workflow",
          target_type: @workflow_class.workflow_name,
          target_id: @workflow_id,
          method_name: :status,
          rpc_kind: "workflow_status",
          args: [],
          kwargs: {},
        ) { @store.workflow(@workflow_id).fetch("status") }
        return rpc_status #: as String
      end

      @store.workflow(@workflow_id).fetch("status").to_s
    end

    #: () -> Object?
    def result
      if (execution = WorkflowExecutionContext.current)
        return execution.call_handle_rpc(
          target_kind: "workflow",
          target_type: @workflow_class.workflow_name,
          target_id: @workflow_id,
          method_name: :result,
          rpc_kind: "workflow_result",
          args: [],
          kwargs: {},
        ) { @store.workflow(@workflow_id)["result"] }
      end

      @store.workflow(@workflow_id)["result"]
    end

    #: () -> String?
    def error
      if (execution = WorkflowExecutionContext.current)
        rpc_error = execution.call_handle_rpc(
          target_kind: "workflow",
          target_type: @workflow_class.workflow_name,
          target_id: @workflow_id,
          method_name: :error,
          rpc_kind: "workflow_error",
          args: [],
          kwargs: {},
        ) { @store.workflow(@workflow_id)["error"] }
        return rpc_error #: as String?
      end

      @store.workflow(@workflow_id)["error"]&.to_s
    end

    #: (?reason: Object?) -> Run
    def cancel(reason: nil)
      if (execution = WorkflowExecutionContext.current)
        rpc_run = execution.call_handle_rpc(
          target_kind: "workflow",
          target_type: @workflow_class.workflow_name,
          target_id: @workflow_id,
          method_name: :cancel,
          rpc_kind: "workflow_cancel",
          args: [],
          kwargs: { reason: },
        ) do
          cancel(reason:)
        end
        return rpc_run #: as Run
      end

      row = @store.request_workflow_cancellation(workflow_id: @workflow_id, reason:)
      Run.new(id: row.fetch("id"), status: row.fetch("status"), result: row["result"], error: row["error"])
    end

    #: (?reason: Object?) -> Run
    def terminate(reason: nil)
      if (execution = WorkflowExecutionContext.current)
        rpc_run = execution.call_handle_rpc(
          target_kind: "workflow",
          target_type: @workflow_class.workflow_name,
          target_id: @workflow_id,
          method_name: :terminate,
          rpc_kind: "workflow_terminate",
          args: [],
          kwargs: { reason: },
        ) do
          terminate(reason:)
        end
        return rpc_run #: as Run
      end

      row = @store.request_workflow_termination(workflow_id: @workflow_id, reason:)
      Run.new(id: row.fetch("id"), status: row.fetch("status"), result: row["result"], error: row["error"])
    end

    #: (Symbol, *Object?, **Object?) ?{ (Object?) -> Object? } -> Object?
    def method_missing(method_name, *args, **kwargs, &block)
      if @workflow_class.exposed_queries.key?(method_name)
        raise ArgumentError, "workflow query #{method_name} does not accept idempotency_key:" if kwargs.key?(:idempotency_key)
        raise ArgumentError, "workflow query #{method_name} does not accept blocks" if block

        if (execution = WorkflowExecutionContext.current)
          return execution.call_handle_rpc(
            target_kind: "workflow",
            target_type: @workflow_class.workflow_name,
            target_id: @workflow_id,
            method_name:,
            rpc_kind: "workflow_query",
            args:,
            kwargs:,
          ) do |args:, kwargs:, **|
            rpc_args = args #: as Array[Object?]
            rpc_kwargs = kwargs #: as Hash[Symbol, Object?]
            route_query(method_name, args: rpc_args, kwargs: rpc_kwargs)
          end
        end

        route_query(method_name, args:, kwargs:)
      elsif (retry_policy = @workflow_class.exposed_commands[method_name])
        raise ArgumentError, "blocks cannot be passed to workflow command ##{method_name}: command arguments are serialized across nodes and blocks cannot be" if block

        if (execution = WorkflowExecutionContext.current)
          return execution.call_handle_rpc(
            target_kind: "workflow",
            target_type: @workflow_class.workflow_name,
            target_id: @workflow_id,
            method_name:,
            rpc_kind: "workflow_command",
            args:,
            kwargs:,
            retry_policy:,
          ) do |idempotency_key:, args:, kwargs:|
            rpc_args = args #: as Array[Object?]
            rpc_kwargs = kwargs #: as Hash[Symbol, Object?]
            invoke_command(method_name, retry_policy:, args: rpc_args, kwargs: rpc_kwargs, idempotency_key:)
          end
        end

        invoke_command(method_name, retry_policy:, args:, kwargs:)
      else
        super
      end
    end

    #: (String) -> String
    def inbox_worker_pool(message_id)
      message = @store.inbox_message(message_id) if @store.respond_to?(:inbox_message)
      message&.fetch("worker_pool", @worker_pool || "default") || @worker_pool || "default"
    end

    #: (Symbol, ?bool) -> bool
    def respond_to_missing?(method_name, include_private = false)
      @workflow_class.exposed_queries.key?(method_name) || @workflow_class.exposed_commands.key?(method_name) || super
    end

    private

    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?]) -> Object?
    def route_query(method_name, args:, kwargs:)
      payload = {
        "workflow_id" => @workflow_id,
        "method" => method_name.to_s,
        "args" => args,
        "kwargs" => kwargs,
        "rpc_kind" => "workflow_query",
      }
      router = WorkflowRpc::Router.new(
        store: @store,
        rpc_client_factory: method(:workflow_rpc_client_for),
        retry_on_stale: true,
        start_on_no_active_lease: false,
      )
      router.request(workflow_id: @workflow_id, command: method_name.to_s, payload:)
    end

    #: (String, worker_pool: String) -> Object
    def workflow_rpc_client_for(worker_id, worker_pool:)
      handlers = @store.local_workflow_rpc_handlers
      if @store.local_workflow_rpc_node_id == worker_id && handlers
        return WorkflowRpc::LocalClient.new(
          store: @store,
          node_id: worker_id,
          handlers:,
        )
      end

      factory = @store.workflow_rpc_client_factory
      factory = factory #: as untyped
      factory.call(WorkerIdentity.address_for(worker_id), worker_pool:)
    end

    #: (Symbol, retry_policy: RetryPolicy, args: Array[Object?], kwargs: Hash[Symbol, Object?], ?idempotency_key: String?) -> Object?
    def invoke_command(method_name, retry_policy:, args:, kwargs:, idempotency_key: nil)
      command_kwargs = kwargs.dup
      idempotency_key ||= string_idempotency_key(command_kwargs.delete(:idempotency_key)) if command_kwargs.key?(:idempotency_key)
      payload = { "method" => method_name.to_s, "args" => args, "kwargs" => command_kwargs }
      message_id = @store.enqueue_workflow_command(
        workflow_id: @workflow_id,
        workflow_name: @workflow_class.workflow_name,
        method_name: method_name.to_s,
        payload:,
        idempotency_key:,
        max_attempts: retry_policy.maximum_attempts_limit,
      )
      @store.deliver_target_message(
        worker_pool: inbox_worker_pool(message_id),
        target_kind: "workflow",
        target_type: @workflow_class.workflow_name,
        target_id: @workflow_id,
      )
      @store.wait_for_inbox_message(message_id, timeout: command_wait_timeout(retry_policy))
    end

    #: (Object?) -> String?
    def string_idempotency_key(value)
      value&.to_s
    end

    #: (RetryPolicy) -> Numeric?
    def command_wait_timeout(retry_policy)
      attempts = retry_policy.maximum_attempts
      return unless attempts.finite?

      retry_delay = (1...attempts.to_i).sum { |attempt_number| retry_policy.delay_for_attempt(attempt_number) }
      COMMAND_WAIT_TIMEOUT_SLACK_SECONDS + retry_delay
    end
  end
end
