# typed: true
# frozen_string_literal: true

module Durababble
  Step = Data.define(:name, :retry_policy)

  class StepRetryScheduled < Error; end
  class WorkflowSuspended < Error; end

  class Workflow
    class << self
      #: untyped
      attr_reader :steps, :exposed_queries, :exposed_commands

      #: (untyped) -> untyped
      def inherited(subclass)
        super
        subclass.instance_variable_set(:@steps, {})
        subclass.instance_variable_set(:@step_order, [])
        subclass.instance_variable_set(:@exposed_queries, {})
        subclass.instance_variable_set(:@exposed_commands, {})
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

      #: (untyped, ?store: untyped) -> untyped
      def enqueue(input, store: Durababble.store)
        store.migrate!
        store.enqueue_workflow(name: workflow_name, input:)
      end

      #: (untyped, ?store: untyped) -> untyped
      def start(input, store: Durababble.store)
        workflow_id = enqueue(input, store:)
        handle(workflow_id, store:)
      end

      #: (untyped, ?store: untyped) -> untyped
      def ref(workflow_id, store: Durababble.store)
        WorkflowRef.new(self, workflow_id, store:)
      end

      #: (untyped, ?store: untyped) -> untyped
      def handle(workflow_id, store: Durababble.store)
        ref(workflow_id, store:)
      end

      #: (?untyped, **untyped) -> untyped
      def step(method_name = nil, **options)
        if method_name
          register_step(method_name, retry_policy: options[:retry])
          method_name
        else
          @pending_durable_macro = [:step, { retry_policy: options[:retry] }]
          nil
        end
      end

      #: (?untyped) -> untyped
      def expose(method_name = nil)
        if method_name
          @exposed_queries[method_name.to_sym] = true
          method_name
        else
          @pending_durable_macro = [:expose, {}]
          nil
        end
      end

      #: (?untyped, **untyped) -> untyped
      def expose_command(method_name = nil, **options)
        if method_name
          @exposed_commands[method_name.to_sym] = RetryPolicy.from(options.fetch(:retry_policy, options[:retry]))
          method_name
        else
          @pending_durable_macro = [:expose_command, { retry_policy: options[:retry] }]
          nil
        end
      end

      #: (untyped) -> untyped
      def method_added(method_name)
        super

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

      #: () -> untyped
      def step_order
        @step_order ||= []
      end

      #: (untyped) -> untyped
      def step_definition(method_name)
        @steps.fetch(method_name.to_sym)
      end

      private

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
            if kwargs.empty?
              workflow.send(original, *args, &block)
            else
              workflow.send(original, *args, **kwargs, &block)
            end
          end
        end
      ensure
        @__durababble_wrapping = false
      end

      #: (untyped) -> untyped
      def underscore(value)
        value.gsub(/([A-Z]+)([A-Z][a-z])/, "\\1_\\2")
          .gsub(/([a-z\d])([A-Z])/, "\\1_\\2")
          .tr("-", "_")
          .downcase
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
    def wait_event(event_key, context = {})
      Durababble.wait_event(event_key, context)
    end
  end

  class WorkflowRef
    #: (untyped, untyped, store: untyped) -> void
    def initialize(workflow_class, workflow_id, store:)
      @workflow_class = workflow_class
      @workflow_id = workflow_id
      @store = store
    end

    #: untyped
    attr_reader :workflow_id

    #: (?reason: untyped) -> untyped
    def cancel(reason: nil)
      row = @store.request_workflow_cancellation(workflow_id: @workflow_id, reason:)
      Run.new(id: row.fetch("id"), status: row.fetch("status"), result: row["result"], error: row["error"])
    end

    #: (untyped, *untyped, **untyped) { (?) -> untyped } -> untyped
    def method_missing(method_name, *args, **kwargs, &block)
      if @workflow_class.exposed_queries.key?(method_name)
        instance = @workflow_class.new #: as untyped
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

    #: (untyped, ?untyped) -> untyped
    def respond_to_missing?(method_name, include_private = false)
      @workflow_class.exposed_queries.key?(method_name) || @workflow_class.exposed_commands.key?(method_name) || super
    end
  end
end
