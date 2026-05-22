# frozen_string_literal: true

module Durababble
  CommandContext = Data.define(:object_type, :durable_id, :command_id, :attempt_number, :idempotency_key)

  class DurableObject
    class << self
      attr_reader :exposed_queries, :exposed_commands

      def inherited(subclass)
        super
        subclass.instance_variable_set(:@exposed_queries, {})
        subclass.instance_variable_set(:@exposed_commands, {})
      end

      def object_type(value = nil)
        @object_type = String(value) if value
        @object_type || underscore(name.split("::").last)
      end

      def ref(durable_id, store: Durababble.store)
        DurableObjectRef.new(self, String(durable_id), store:)
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
        when :expose
          @exposed_queries[method_name.to_sym] = true
        when :expose_command
          @exposed_commands[method_name.to_sym] = RetryPolicy.from(options.fetch(:retry_policy, options[:retry]))
        end
      end

      private

      def underscore(value)
        value.gsub(/([A-Z]+)([A-Z][a-z])/, "\\1_\\2")
             .gsub(/([a-z\d])([A-Z])/, "\\1_\\2")
             .tr("-", "_")
             .downcase
      end
    end

    attr_reader :durable_id, :command_context

    def initialize(durable_id: nil, state: nil, store: nil, command_context: nil)
      @durable_id = durable_id
      @current_state = state
      @store = store
      @command_context = command_context
      @state_dirty = false
    end

    def initialize_state
      nil
    end

    def current_state
      @current_state.nil? ? initialize_state : @current_state
    end

    def update_state(new_state)
      @current_state = new_state
      @state_dirty = true
      @store&.save_object_state(object_type: self.class.object_type, object_id: durable_id, state: new_state) unless command_context
      new_state
    end

    def state_dirty? = @state_dirty
  end

  class DurableObjectRef
    def initialize(object_class, durable_id, store:)
      @object_class = object_class
      @durable_id = durable_id
      @store = store
    end

    attr_reader :durable_id

    def method_missing(method_name, *args, **kwargs, &block)
      if @object_class.exposed_queries.key?(method_name)
        invoke_query(method_name, args:, kwargs:, block:)
      elsif (retry_policy = @object_class.exposed_commands[method_name])
        invoke_command(method_name, retry_policy:, args:, kwargs:, block:)
      else
        super
      end
    end

    def respond_to_missing?(method_name, include_private = false)
      @object_class.exposed_queries.key?(method_name) || @object_class.exposed_commands.key?(method_name) || super
    end

    private

    def invoke_query(method_name, args:, kwargs:, block:)
      @store.migrate!
      state = @store.object_state(object_type: @object_class.object_type, object_id: @durable_id)
      object = @object_class.new(durable_id: @durable_id, state:, store: @store)
      kwargs.empty? ? object.public_send(method_name, *args, &block) : object.public_send(method_name, *args, **kwargs, &block)
    end

    def invoke_command(method_name, retry_policy:, args:, kwargs:, block:)
      @store.migrate!
      command_id = @store.enqueue_object_command(object_type: @object_class.object_type, object_id: @durable_id, method_name: method_name.to_s, args:, kwargs:)
      run_command(command_id, method_name, retry_policy:, args:, kwargs:, block:)
    end

    def run_command(command_id, method_name, retry_policy:, args:, kwargs:, block:)
      attempt = 0
      worker_id = "inline-object-worker"
      begin
        attempt += 1
        claimed = @store.claim_object_command(command_id:, worker_id:)
        raise LeaseConflict, "could not claim durable object command #{command_id}" unless claimed

        state = @store.object_state(object_type: @object_class.object_type, object_id: @durable_id)
        context = CommandContext.new(
          object_type: @object_class.object_type,
          durable_id: @durable_id,
          command_id:,
          attempt_number: attempt,
          idempotency_key: "durababble:v1:object:#{@object_class.object_type}:#{@durable_id}:command:#{command_id}"
        )
        object = @object_class.new(durable_id: @durable_id, state:, store: @store, command_context: context)
        result = kwargs.empty? ? object.public_send(method_name, *args, &block) : object.public_send(method_name, *args, **kwargs, &block)
        completed = if object.state_dirty?
                      @store.complete_object_command(command_id:, result:, object_type: @object_class.object_type, object_id: @durable_id, state: object.current_state, worker_id:)
                    else
                      @store.complete_object_command(command_id:, result:, worker_id:)
                    end
        raise LeaseConflict, "lost durable object command lease #{command_id}" unless completed && (!completed.respond_to?(:cmd_tuples) || completed.cmd_tuples.to_i.positive?)

        result
      rescue StandardError => e
        @store.fail_object_command(command_id:, error: "#{e.class}: #{e.message}", worker_id:) if claimed
        retry if retry_policy.retryable?(e, attempt_number: attempt)
        raise
      end
    end
  end
end
