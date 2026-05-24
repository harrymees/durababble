# typed: true
# frozen_string_literal: true

module Durababble
  CommandContext = Data.define(:object_type, :durable_id, :command_id, :attempt_number, :idempotency_key)
  ObjectSleepChange = Data.define(:action, :wake_at, :payload)

  class DurableObject
    class << self
      #: untyped
      attr_reader :exposed_queries, :exposed_commands

      #: (untyped) -> untyped
      def inherited(subclass)
        super
        subclass.instance_variable_set(:@exposed_queries, {})
        subclass.instance_variable_set(:@exposed_commands, {})
      end

      #: (?untyped) -> untyped
      def object_type(value = nil)
        @object_type = String(value) if value
        ruby_name = Module.instance_method(:name).bind_call(self)
        @object_type || underscore((ruby_name || object_id.to_s).split("::").last)
      end

      #: (untyped, ?store: untyped) -> untyped
      def ref(durable_id, store: Durababble.store)
        DurableObjectRef.new(self, String(durable_id), store:)
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
        when :expose
          @exposed_queries[method_name.to_sym] = true
        when :expose_command
          @exposed_commands[method_name.to_sym] = RetryPolicy.from(options.fetch(:retry_policy, options[:retry]))
        end
      end

      private

      #: (untyped) -> untyped
      def underscore(value)
        value.gsub(/([A-Z]+)([A-Z][a-z])/, "\\1_\\2")
          .gsub(/([a-z\d])([A-Z])/, "\\1_\\2")
          .tr("-", "_")
          .downcase
      end
    end

    #: untyped
    attr_reader :durable_id, :command_context, :sleep_change

    #: (?durable_id: untyped, ?state: untyped, ?store: untyped, ?command_context: untyped) -> void
    def initialize(durable_id: nil, state: nil, store: nil, command_context: nil)
      @durable_id = durable_id
      @current_state = state
      @store = store
      @command_context = command_context
      @state_dirty = false
      @sleep_change = nil
    end

    #: () -> untyped
    def initialize_state
      nil
    end

    #: () -> untyped
    def current_state
      @current_state.nil? ? initialize_state : @current_state
    end

    #: (untyped) -> untyped
    def update_state(new_state)
      @current_state = new_state
      @state_dirty = true
      @store&.save_object_state(object_type: self.class.object_type, object_id: durable_id, state: new_state) unless command_context
      new_state
    end

    #: (at: untyped, ?payload: untyped) -> untyped
    def sleep_until(at:, payload: nil)
      ensure_command_context!(:sleep_until)
      @sleep_change = ObjectSleepChange.new(action: :schedule, wake_at: at, payload:)
      nil
    end

    #: () -> untyped
    def cancel_sleep
      ensure_command_context!(:cancel_sleep)
      @sleep_change = ObjectSleepChange.new(action: :cancel, wake_at: nil, payload: nil)
      nil
    end

    #: (?payload: untyped) -> untyped
    def on_wake(payload: nil)
      nil
    end

    #: () -> untyped
    def state_dirty? = @state_dirty

    private

    #: (untyped) -> void
    def ensure_command_context!(method_name)
      return if command_context

      raise Error, "#{method_name} is only available while a durable object command is executing"
    end
  end

  class DurableObjectRef
    WAKE_METHOD_NAME = "__durababble_wake__"

    #: (untyped, untyped, store: untyped) -> void
    def initialize(object_class, durable_id, store:)
      @object_class = object_class
      @durable_id = durable_id
      @store = store
    end

    #: untyped
    attr_reader :durable_id

    #: (untyped, *untyped, **untyped) { (?) -> untyped } -> untyped
    def method_missing(method_name, *args, **kwargs, &block)
      if @object_class.exposed_queries.key?(method_name)
        invoke_query(method_name, args:, kwargs:, block:)
      elsif (retry_policy = @object_class.exposed_commands[method_name])
        invoke_command(method_name, retry_policy:, args:, kwargs:, block:)
      else
        super
      end
    end

    #: (untyped, ?untyped) -> untyped
    def respond_to_missing?(method_name, include_private = false)
      @object_class.exposed_queries.key?(method_name) || @object_class.exposed_commands.key?(method_name) || super
    end

    private

    #: (untyped, args: untyped, kwargs: untyped, block: untyped) -> untyped
    def invoke_query(method_name, args:, kwargs:, block:)
      @store.migrate!
      state = @store.object_state(object_type: @object_class.object_type, object_id: @durable_id)
      object = @object_class.new(durable_id: @durable_id, state:, store: @store) #: as untyped
      kwargs.empty? ? object.public_send(method_name, *args, &block) : object.public_send(method_name, *args, **kwargs, &block)
    end

    #: (untyped, retry_policy: untyped, args: untyped, kwargs: untyped, block: untyped) -> untyped
    def invoke_command(method_name, retry_policy:, args:, kwargs:, block:)
      @store.migrate!
      command_id = @store.enqueue_object_command(object_type: @object_class.object_type, object_id: @durable_id, method_name: method_name.to_s, args:, kwargs:)
      drain_mailbox_until(command_id, target_method_name: method_name, target_args: args, target_kwargs: kwargs, default_retry_policy: retry_policy, block:)
    end

    #: (untyped, target_method_name: untyped, target_args: untyped, target_kwargs: untyped, default_retry_policy: untyped, block: untyped) -> untyped
    def drain_mailbox_until(target_command_id, target_method_name:, target_args:, target_kwargs:, default_retry_policy:, block:)
      if @store.respond_to?(:claim_next_object_command)
        loop do
          claimed = @store.claim_next_object_command(object_type: @object_class.object_type, object_id: @durable_id, worker_id: worker_id)
          raise LeaseConflict, "could not claim durable object command #{target_command_id}" unless claimed

          command_result = run_claimed_command(claimed, default_retry_policy:, block:)
          break command_result if claimed.fetch("id") == target_command_id
        end
      else
        claimed = @store.claim_object_command(command_id: target_command_id, worker_id: worker_id)
        raise LeaseConflict, "could not claim durable object command #{target_command_id}" unless claimed

        run_claimed_command(
          claimed.merge("id" => target_command_id, "method_name" => target_method_name.to_s, "args" => target_args, "kwargs" => target_kwargs),
          default_retry_policy:,
          block:,
        )
      end
    end

    #: (untyped, default_retry_policy: untyped, block: untyped) -> untyped
    def run_claimed_command(claimed, default_retry_policy:, block:)
      command_id = claimed.fetch("id")
      method_name = claimed.fetch("method_name").to_sym
      args = claimed.fetch("args")
      kwargs = claimed.fetch("kwargs")
      retry_policy = if claimed.fetch("method_name", nil) == WAKE_METHOD_NAME
        RetryPolicy.from(nil)
      elsif method_name && @object_class.exposed_commands[method_name]
        @object_class.exposed_commands.fetch(method_name)
      else
        default_retry_policy
      end
      attempt = 0
      begin
        attempt += 1
        state = @store.object_state(object_type: @object_class.object_type, object_id: @durable_id)
        context = CommandContext.new(
          object_type: @object_class.object_type,
          durable_id: @durable_id,
          command_id:,
          attempt_number: attempt,
          idempotency_key: "durababble:v1:object:#{@object_class.object_type}:#{@durable_id}:command:#{command_id}",
        )
        object = @object_class.new(durable_id: @durable_id, state:, store: @store, command_context: context) #: as untyped
        result = if claimed.fetch("method_name", nil) == WAKE_METHOD_NAME
          object.on_wake(payload: kwargs.fetch(:payload, kwargs["payload"]))
        elsif kwargs.empty?
          object.public_send(method_name, *args, &block)
        else
          object.public_send(method_name, *args, **kwargs, &block)
        end
        completion = {
          command_id:,
          result:,
          object_type: @object_class.object_type,
          object_id: @durable_id,
          state: object.state_dirty? ? object.current_state : Store::NO_OBJECT_STATE,
          worker_id: worker_id,
        }
        completion[:sleep_change] = object.sleep_change if object.sleep_change
        completed = @store.complete_object_command(**completion)
        unless completed && (!completed.respond_to?(:cmd_tuples) || completed.cmd_tuples.to_i.positive?)
          raise LeaseConflict, "lost durable object command lease #{command_id}"
        end

        result
      rescue StandardError => e
        @store.fail_object_command(command_id:, error: "#{e.class}: #{e.message}", worker_id: worker_id)
        if retry_policy.retryable?(e, attempt_number: attempt)
          claimed = @store.claim_object_command(command_id:, worker_id: worker_id)
          raise LeaseConflict, "could not reclaim durable object command #{command_id}" unless claimed

          retry
        end
        raise
      end
    end

    #: () -> String
    def worker_id = "inline-object-worker"
  end
end
