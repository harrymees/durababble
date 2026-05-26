# typed: true
# frozen_string_literal: true

require_relative "durable_method_dsl"

module Durababble
  Step = Data.define(:name, :retry_policy)

  class StepRetryScheduled < Error; end
  class WorkflowSuspended < Error; end

  class Workflow
    extend DurableMethodDSL

    class << self
      #: untyped
      attr_reader :steps

      #: (untyped) -> untyped
      def inherited(subclass)
        super
        subclass.instance_variable_set(:@steps, {})
        subclass.instance_variable_set(:@step_order, [])
        initialize_durable_method_dsl(subclass)
      end

      #: (?untyped) -> untyped
      def workflow_name(value = nil)
        @workflow_name = String(value) if value
        ruby_name = Module.instance_method(:name).bind_call(self)
        @workflow_name || underscore((ruby_name || object_id.to_s).split("::").last)
      end

      #: () -> untyped
      def name
        workflow_name
      end

      #: (untyped, ?store: untyped, ?engine: untyped, ?worker_pool: String?) -> untyped
      def enqueue(input, store: nil, engine: nil, worker_pool: nil)
        if worker_pool
          Durababble.store_for(store:, engine:).enqueue_workflow(name: workflow_name, input:, worker_pool:)
        else
          Durababble.engine_for(store:, engine:).enqueue(self, input:)
        end
      end

      #: (untyped, ?store: untyped, ?engine: untyped, ?worker_pool: String?) -> untyped
      def start(input, store: nil, engine: nil, worker_pool: nil)
        if worker_pool
          resolved_store = Durababble.store_for(store:, engine:)
          workflow_id = resolved_store.enqueue_workflow(name: workflow_name, input:, worker_pool:)
          handle(workflow_id, store: resolved_store, worker_pool:)
        else
          resolved_engine = Durababble.engine_for(store:, engine:)
          workflow_id = resolved_engine.enqueue(self, input:)
          handle(workflow_id, engine: resolved_engine)
        end
      end

      #: (untyped, ?store: untyped, ?engine: untyped, ?worker_pool: String?) -> untyped
      def at(workflow_id, store: nil, engine: nil, worker_pool: nil)
        handle(workflow_id, store:, engine:, worker_pool:)
      end

      #: (untyped, ?store: untyped, ?engine: untyped, ?worker_pool: String?) -> untyped
      def handle(workflow_id, store: nil, engine: nil, worker_pool: nil)
        WorkflowRef.new(self, workflow_id, store: Durababble.store_for(store:, engine:), worker_pool:)
      end

      #: (?untyped, **untyped) -> untyped
      def step(method_name = nil, **options)
        if method_name
          register_step(method_name, retry_policy: options[:retry])
          method_name
        else
          set_pending_durable_macro(:step, retry_policy: options[:retry])
        end
      end

      #: () -> untyped
      def step_order
        @step_order ||= []
      end

      #: (untyped) -> untyped
      def step_definition(method_name)
        @steps.fetch(method_name.to_sym)
      end

      private

      #: (untyped, untyped, untyped) -> untyped
      def handle_pending_durable_macro(kind, method_name, options)
        return register_step(method_name, **options) if kind == :step

        super
      end

      #: (untyped, ?retry_policy: untyped) -> untyped
      def register_step(method_name, retry_policy: nil)
        method_name = method_name.to_sym
        @steps[method_name] = Step.new(name: method_name.to_s, retry_policy: RetryPolicy.from(retry_policy))
        @step_order << method_name unless @step_order.include?(method_name)
        wrap_step_method(method_name)
      end

      #: (untyped) -> untyped
      def wrap_step_method(method_name)
        original = "__durababble_original_#{method_name}".to_sym
        return if method_defined?(original)

        @__durababble_wrapping = true
        alias_method(original, method_name)
        define_method(method_name) do |*args, **kwargs, &block|
          workflow = self #: as untyped
          execution = workflow.__durababble_execution__
          execution.call_step(workflow, method_name:, args:, kwargs:) do
            workflow.send(original, *args, **kwargs, &block)
          end
        end
      ensure
        @__durababble_wrapping = false
      end
    end

    #: (untyped) -> void
    attr_writer :__durababble_execution__

    #: () -> untyped
    def __durababble_execution__
      location = caller_locations(1, 1)&.first
      label = location&.label || "unknown"
      @__durababble_execution__ || raise(Error, "durable step #{self.class.name}##{label} called outside workflow execution")
    end

    #: () -> untyped
    def step_context
      __durababble_execution__.step_context
    end

    #: (untyped, ?untyped) -> untyped
    def wait_until(time, context = {})
      Durababble.wait_until(time, context)
    end

    #: (untyped, ?untyped) -> untyped
    def sleep_until(time, context = {})
      Durababble.sleep_until(time, context)
    end

    #: (untyped, ?untyped) -> untyped
    def sleep(duration, context = {})
      Durababble.sleep(duration, context)
    end

    #: (?timeout: untyped) { -> bool } -> bool
    def wait_condition(timeout: nil, &block)
      Durababble.wait_condition(timeout:, &block)
    end
  end

  class WorkflowRef
    #: (untyped, untyped, store: untyped, ?worker_pool: String?) -> void
    def initialize(workflow_class, workflow_id, store:, worker_pool: nil)
      @workflow_class = workflow_class
      @workflow_id = workflow_id
      @store = store
      @worker_pool = worker_pool
    end

    #: untyped
    attr_reader :workflow_id

    #: () -> untyped
    def status
      @store.workflow(@workflow_id).fetch("status")
    end

    #: () -> untyped
    def result
      @store.workflow(@workflow_id)["result"]
    end

    #: () -> untyped
    def error
      @store.workflow(@workflow_id)["error"]
    end

    #: (?reason: untyped) -> untyped
    def cancel(reason: nil)
      row = @store.request_workflow_cancellation(workflow_id: @workflow_id, reason:)
      Run.new(id: row.fetch("id"), status: row.fetch("status"), result: row["result"], error: row["error"])
    end

    #: (?reason: untyped) -> untyped
    def terminate(reason: nil)
      row = @store.request_workflow_termination(workflow_id: @workflow_id, reason:)
      Run.new(id: row.fetch("id"), status: row.fetch("status"), result: row["result"], error: row["error"])
    end

    #: (untyped, *untyped, **untyped) { (?) -> untyped } -> untyped
    def method_missing(method_name, *args, **kwargs, &block)
      if @workflow_class.exposed_queries.key?(method_name)
        instance = @workflow_class.new #: as untyped
        instance.instance_variable_set(:@__durababble_ref_store, @store)
        instance.instance_variable_set(:@__durababble_ref_workflow_id, @workflow_id)
        instance.public_send(method_name, *args, **kwargs, &block)
      elsif @workflow_class.exposed_commands.key?(method_name)
        idempotency_key = kwargs.delete(:idempotency_key)
        payload = { "method" => method_name.to_s, "args" => args, "kwargs" => kwargs }
        message_id = @store.enqueue_workflow_command(
          workflow_id: @workflow_id,
          workflow_name: @workflow_class.workflow_name,
          method_name: method_name.to_s,
          payload:,
          idempotency_key:,
        )
        @store.deliver_target_message(
          worker_pool: inbox_worker_pool(message_id),
          target_kind: "workflow",
          target_type: @workflow_class.workflow_name,
          target_id: @workflow_id,
        )
        @store.wait_for_inbox_message(message_id)
      else
        super
      end
    end

    #: (untyped) -> String
    def inbox_worker_pool(message_id)
      @worker_pool || @store.inbox_message(message_id)&.fetch("worker_pool", "default") || "default"
    end

    #: (untyped, ?untyped) -> untyped
    def respond_to_missing?(method_name, include_private = false)
      @workflow_class.exposed_queries.key?(method_name) || @workflow_class.exposed_commands.key?(method_name) || super
    end
  end
end
