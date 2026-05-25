# typed: true
# frozen_string_literal: true

require_relative "durable_method_dsl"

module Durababble
  CommandContext = Data.define(:object_type, :durable_id, :command_id, :attempt_number, :idempotency_key)

  class DurableObject
    extend DurableMethodDSL

    class << self
      #: (untyped) -> untyped
      def inherited(subclass)
        super
        initialize_durable_method_dsl(subclass)
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

      #: (untyped, ?store: untyped, ?worker_pool: untyped, ?idempotency_key: untyped) -> untyped
      def at(durable_id, store: Durababble.store, worker_pool: nil, idempotency_key: nil)
        DurableObjectRef.new(self, String(durable_id), store:)
      end

      #: (untyped, untyped, *untyped, ?store: untyped, ?idempotency_key: untyped, **untyped) -> untyped
      def tell(durable_id, method_name, *args, store: Durababble.store, idempotency_key: nil, **kwargs)
        method_name = method_name.to_sym
        retry_policy = @exposed_commands[method_name]
        raise NoMethodError, "undefined durable object command `#{method_name}` for #{self}" unless retry_policy

        store.migrate!
        attributes = object_command_attributes(object_id: String(durable_id), method_name:)
        Observability.trace("durababble.object.command.enqueue", attributes) do
          message_id = store.enqueue_object_command(
            object_type: object_type,
            object_id: String(durable_id),
            method_name: method_name.to_s,
            args:,
            kwargs:,
            message_kind: "tell",
            idempotency_key:,
            max_attempts: inbox_max_attempts(retry_policy),
          )
          store.deliver_target_message(target_kind: "object", target_type: object_type, target_id: String(durable_id))
          message_id
        end
      end

      private

      #: (untyped) -> untyped
      def inbox_max_attempts(retry_policy)
        attempts = retry_policy.maximum_attempts
        attempts.finite? ? attempts : nil
      end

      #: (object_id: untyped, method_name: untyped) -> untyped
      def object_command_attributes(object_id:, method_name:)
        {
          "durababble.object.type" => object_type,
          "durababble.object.id" => object_id,
          "durababble.object.method" => method_name,
        }
      end
    end

    #: untyped
    attr_reader :durable_id, :command_context

    #: (?durable_id: untyped, ?state: untyped, ?store: untyped, ?command_context: untyped) -> void
    def initialize(durable_id: nil, state: nil, store: nil, command_context: nil)
      @durable_id = durable_id
      @current_state = state
      @store = store
      @command_context = command_context
      @state_dirty = false
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

    #: () -> untyped
    def state_dirty? = @state_dirty
  end

  class DurableObjectRef
    COMMAND_WAIT_TIMEOUT_SLACK_SECONDS = 10

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
      attributes = object_attributes(method_name:)
      Observability.trace("durababble.object.query", attributes) do
        state = @store.object_state(object_type: @object_class.object_type, object_id: @durable_id)
        object = @object_class.new(durable_id: @durable_id, state:, store: @store) #: as untyped
        kwargs.empty? ? object.public_send(method_name, *args, &block) : object.public_send(method_name, *args, **kwargs, &block)
      end
    end

    #: (untyped, retry_policy: untyped, args: untyped, kwargs: untyped, block: untyped) -> untyped
    def invoke_command(method_name, retry_policy:, args:, kwargs:, block:)
      @store.migrate!
      attributes = object_attributes(method_name:)
      Observability.trace("durababble.object.command.enqueue", attributes) do
        idempotency_key = kwargs.delete(:idempotency_key)
        command_id = @store.enqueue_object_command(
          object_type: @object_class.object_type,
          object_id: @durable_id,
          method_name: method_name.to_s,
          args:,
          kwargs:,
          message_kind: "ask",
          idempotency_key:,
          max_attempts: self.class.inbox_max_attempts(retry_policy),
        )
        @store.deliver_target_message(target_kind: "object", target_type: @object_class.object_type, target_id: @durable_id)
        @store.wait_for_inbox_message(command_id, timeout: self.class.command_wait_timeout(retry_policy))
      end
    end

    #: (untyped) -> untyped
    def self.inbox_max_attempts(retry_policy)
      attempts = retry_policy.maximum_attempts
      attempts.finite? ? attempts : nil
    end

    #: (untyped) -> untyped
    def self.command_wait_timeout(retry_policy)
      attempts = retry_policy.maximum_attempts
      return nil unless attempts.finite?

      retry_delay = (1...attempts.to_i).sum { |attempt_number| retry_policy.delay_for_attempt(attempt_number) }
      COMMAND_WAIT_TIMEOUT_SLACK_SECONDS + retry_delay
    end

    #: (method_name: untyped) -> untyped
    def object_attributes(method_name:)
      {
        "durababble.object.type" => @object_class.object_type,
        "durababble.object.id" => @durable_id,
        "durababble.object.method" => method_name,
      }
    end
  end

  class DurableObjectExecutor
    #: (store: untyped, objects: untyped, worker_id: untyped, lease_seconds: untyped) -> void
    def initialize(store:, objects:, worker_id:, lease_seconds:)
      @store = store
      @objects = normalize_objects(objects)
      @worker_id = worker_id
      @lease_seconds = lease_seconds
    end

    #: (untyped, object_id: untyped, ?limit: untyped) -> untyped
    def drain_object_inbox(object_type, object_id:, limit: 10)
      object_class = @objects.fetch(object_type)
      drained = 0
      while drained < limit
        messages = @store.claim_inbox_messages(
          target_kind: "object",
          target_type: object_type,
          target_id: object_id,
          worker_id: @worker_id,
          lease_seconds: @lease_seconds,
          limit: 1,
        )
        break if messages.empty?

        messages.each do |message|
          drained += 1
          dispatch_message(object_class, object_id:, message:)
        end
      end
      drained
    end

    private

    #: (untyped) -> untyped
    def normalize_objects(objects)
      case objects
      when Hash
        objects.transform_keys(&:to_s)
      else
        Array(objects).to_h { |object_class| [object_class.object_type, object_class] }
      end
    end

    #: (untyped, object_id: untyped, message: untyped) -> untyped
    def dispatch_message(object_class, object_id:, message:)
      case message.fetch("message_kind")
      when "ask", "tell"
        dispatch_command(object_class, object_id:, message:)
      when "wake"
        dispatch_wake(object_class, object_id:, message:)
      else
        terminal_failure(message, "Durababble::Error: unsupported object inbox message #{message.fetch("message_kind")}")
      end
    end

    #: (untyped, object_id: untyped, message: untyped) -> untyped
    def dispatch_command(object_class, object_id:, message:)
      method_name = object_method_name(message)
      retry_policy = object_class.exposed_commands[method_name]
      unless retry_policy
        terminal_failure(message, "Durababble::WorkflowRpc::UnknownCommand: #{method_name}")
        return
      end

      attributes = command_attributes(object_class, object_id:, message:, method_name:)
      Observability.count("durababble.object.command.attempts", attributes)
      Observability.trace("durababble.object.command", attributes) do
        object = build_object(object_class, object_id:, message:)
        args, kwargs = object_args(message)
        result = kwargs.empty? ? object.public_send(method_name, *args) : object.public_send(method_name, *args, **kwargs)
        complete_message(object, message, result:, attributes:)
        Observability.count("durababble.object.command.successes", attributes)
        result
      end
    rescue LeaseConflict
      raise
    rescue StandardError => e
      handle_command_error(message, retry_policy:, error: e, attributes: attributes || {})
    end

    #: (untyped, object_id: untyped, message: untyped) -> untyped
    def dispatch_wake(object_class, object_id:, message:)
      object = build_object(object_class, object_id:, message:)
      result = if object.respond_to?(:on_wake)
        object.public_send(:on_wake, payload: message.fetch("payload"))
      end
      complete_message(object, message, result:)
    rescue LeaseConflict
      raise
    rescue StandardError => e
      terminal_failure(message, "#{e.class}: #{e.message}")
    end

    #: (untyped, object_id: untyped, message: untyped) -> untyped
    def build_object(object_class, object_id:, message:)
      state = @store.object_state(object_type: object_class.object_type, object_id:)
      context = CommandContext.new(
        object_type: object_class.object_type,
        durable_id: object_id,
        command_id: message.fetch("id"),
        attempt_number: message.fetch("attempts").to_i,
        idempotency_key: "durababble:v1:object:#{object_class.object_type}:#{object_id}:command:#{message.fetch("id")}",
      )
      object_class.new(durable_id: object_id, state:, store: @store, command_context: context) #: as untyped
    end

    #: (untyped) -> untyped
    def object_method_name(message)
      payload = message.fetch("payload")
      (message["method_name"] || payload.fetch("method_name")).to_sym
    end

    #: (untyped) -> untyped
    def object_args(message)
      payload = message.fetch("payload")
      [payload.fetch("args", []), payload.fetch("kwargs", {})]
    end

    #: (untyped, untyped, result: untyped, ?attributes: untyped) -> untyped
    def complete_message(object, message, result:, attributes: {})
      completed = if object.state_dirty?
        @store.complete_object_command(
          command_id: message.fetch("id"),
          result:,
          object_type: object.class.object_type,
          object_id: object.durable_id,
          state: object.current_state,
          worker_id: @worker_id,
        )
      else
        @store.complete_object_command(command_id: message.fetch("id"), result:, worker_id: @worker_id)
      end
      return if completed&.affected_rows.to_i.positive?

      Observability.count("durababble.leases.conflicts", attributes.merge("durababble.lease.owner" => @worker_id))
      raise LeaseConflict, "lost durable object command lease #{message.fetch("id")}"
    end

    #: (untyped, retry_policy: untyped, error: untyped, attributes: untyped) -> untyped
    def handle_command_error(message, retry_policy:, error:, attributes:)
      attempt_number = message.fetch("attempts").to_i
      serialized_error = "#{error.class}: #{error.message}"
      Observability.count("durababble.object.command.failures", attributes.merge("error.type" => error.class.name))
      if retry_policy&.retryable?(error, attempt_number:)
        delay = retry_policy.delay_for_attempt(attempt_number)
        @store.retry_object_command(command_id: message.fetch("id"), error: serialized_error, worker_id: @worker_id, ready_at: retry_run_at(delay))
      else
        terminal_failure(message, serialized_error)
      end
    end

    #: (untyped, untyped) -> untyped
    def terminal_failure(message, error)
      @store.fail_object_command(command_id: message.fetch("id"), error:, worker_id: @worker_id, terminal: true)
    end

    #: (untyped) -> untyped
    def retry_run_at(delay)
      base = @store.respond_to?(:current_time) ? @store.current_time : Time.now
      base + delay
    end

    #: (untyped, object_id: untyped, message: untyped, method_name: untyped) -> untyped
    def command_attributes(object_class, object_id:, message:, method_name:)
      {
        "durababble.object.type" => object_class.object_type,
        "durababble.object.id" => object_id,
        "durababble.object.method" => method_name,
        "durababble.object.command.id" => message.fetch("id"),
        "durababble.object.command.attempt" => message.fetch("attempts").to_i,
        "durababble.worker.id" => @worker_id,
      }
    end
  end
end
