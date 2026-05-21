# frozen_string_literal: true

module Durababble
  Step = Data.define(:name, :retry_policy)

  class StepRetryScheduled < Error; end
  class WorkflowSuspended < Error; end

  class Workflow
    class << self
      attr_reader :steps, :exposed_queries, :exposed_commands

      def inherited(subclass)
        super
        subclass.instance_variable_set(:@steps, {})
        subclass.instance_variable_set(:@step_order, [])
        subclass.instance_variable_set(:@exposed_queries, {})
        subclass.instance_variable_set(:@exposed_commands, {})
      end

      def workflow_name(value = nil)
        @workflow_name = String(value) if value
        ruby_name = Module.instance_method(:name).bind_call(self)
        @workflow_name || underscore((ruby_name || object_id.to_s).split("::").last)
      end

      def name
        workflow_name
      end

      def enqueue(input, store: Durababble.store)
        store.migrate!
        store.enqueue_workflow(name: workflow_name, input:)
      end

      def ref(workflow_id, store: Durababble.store)
        WorkflowRef.new(self, workflow_id, store:)
      end

      def step(method_name = nil, **options)
        if method_name
          register_step(method_name, retry_policy: options[:retry])
          method_name
        else
          @pending_durable_macro = [:step, { retry_policy: options[:retry] }]
          nil
        end
      end

      def expose(method_name = nil)
        if method_name
          @exposed_queries[method_name.to_sym] = true
          method_name
        else
          @pending_durable_macro = [:expose, {}]
          nil
        end
      end

      def expose_command(method_name = nil, **options)
        if method_name
          @exposed_commands[method_name.to_sym] = RetryPolicy.from(options.fetch(:retry_policy, options[:retry]))
          method_name
        else
          @pending_durable_macro = [:expose_command, { retry_policy: options[:retry] }]
          nil
        end
      end

      def method_added(method_name)
        return if @__durababble_wrapping
        pending = @pending_durable_macro
        return unless pending

        @pending_durable_macro = nil
        kind, options = pending
        case kind
        when :step
          register_step(method_name, **options)
        when :expose
          @exposed_queries[method_name.to_sym] = true
        when :expose_command
          @exposed_commands[method_name.to_sym] = RetryPolicy.from(options.fetch(:retry_policy, options[:retry]))
        end
      end

      def step_order
        @step_order ||= []
      end

      def step_definition(method_name)
        @steps.fetch(method_name.to_sym)
      end

      private

      def register_step(method_name, retry_policy: nil)
        method_name = method_name.to_sym
        @steps[method_name] = Step.new(name: method_name.to_s, retry_policy: RetryPolicy.from(retry_policy))
        @step_order << method_name unless @step_order.include?(method_name)
        wrap_step_method(method_name)
      end

      def wrap_step_method(method_name)
        original = "__durababble_original_#{method_name}".to_sym
        return if method_defined?(original)

        @__durababble_wrapping = true
        alias_method original, method_name
        define_method(method_name) do |*args, **kwargs, &block|
          execution = __durababble_execution__
          execution.call_step(self, method_name:, args:, kwargs:) do
            if kwargs.empty?
              send(original, *args, &block)
            else
              send(original, *args, **kwargs, &block)
            end
          end
        end
      ensure
        @__durababble_wrapping = false
      end

      def underscore(value)
        value.gsub(/([A-Z]+)([A-Z][a-z])/, "\\1_\\2")
             .gsub(/([a-z\d])([A-Z])/, "\\1_\\2")
             .tr("-", "_")
             .downcase
      end
    end

    attr_writer :__durababble_execution__

    def __durababble_execution__
      @__durababble_execution__ || raise(Error, "durable step #{self.class.name}##{caller_locations(1, 1).first.label} called outside workflow execution")
    end

    def step_context
      __durababble_execution__.step_context
    end

    def wait_until(time, context = {})
      Durababble.wait_until(time, context)
    end

    def wait_event(event_key, context = {})
      Durababble.wait_event(event_key, context)
    end
  end

  class WorkflowRef
    def initialize(workflow_class, workflow_id, store:)
      @workflow_class = workflow_class
      @workflow_id = workflow_id
      @store = store
    end

    attr_reader :workflow_id

    def method_missing(method_name, *args, **kwargs, &block)
      if @workflow_class.exposed_queries.key?(method_name)
        instance = @workflow_class.new
        instance.instance_variable_set(:@__durababble_ref_store, @store)
        instance.instance_variable_set(:@__durababble_ref_workflow_id, @workflow_id)
        kwargs.empty? ? instance.public_send(method_name, *args, &block) : instance.public_send(method_name, *args, **kwargs, &block)
      elsif @workflow_class.exposed_commands.key?(method_name)
        # For now exposed workflow commands are persisted as events; lease-routed RPC can back this later.
        payload = { "method" => method_name.to_s, "args" => args, "kwargs" => kwargs }
        @store.signal_event("workflow:#{@workflow_id}:command:#{method_name}", payload:)
      else
        super
      end
    end

    def respond_to_missing?(method_name, include_private = false)
      @workflow_class.exposed_queries.key?(method_name) || @workflow_class.exposed_commands.key?(method_name) || super
    end
  end
end
