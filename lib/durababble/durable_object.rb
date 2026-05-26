# typed: true
# frozen_string_literal: true

require_relative "durable_method_dsl"

module Durababble
  CommandContext = Data.define(:object_type, :durable_id, :command_id, :attempt_number, :idempotency_key)

  class DurableObject
    extend DurableMethodDSL

    UNINITIALIZED = Object.new.freeze

    class << self
      #: (Class) -> void
      def inherited(subclass)
        super
        initialize_durable_method_dsl(subclass)
      end

      #: (?String?) -> String
      def object_type(value = nil)
        @object_type = String(value) if value
        ruby_name = Module.instance_method(:name).bind_call(self)
        @object_type || underscore((ruby_name || object_id.to_s).split("::").last)
      end

      #: (Object?, ?store: Store?, ?engine: Engine?, ?worker_pool: String?, ?idempotency_key: String?) -> DurableObjectRef
      def at(durable_id, store: nil, engine: nil, worker_pool: nil, idempotency_key: nil)
        handle(durable_id, store:, engine:, worker_pool:, idempotency_key:)
      end

      #: (Object?, ?store: Store?, ?engine: Engine?, ?worker_pool: String?, ?idempotency_key: String?) -> DurableObjectRef
      def handle(durable_id, store: nil, engine: nil, worker_pool: nil, idempotency_key: nil)
        DurableObjectRef.new(
          self,
          String(durable_id),
          store: Durababble.store_for(store:, engine:),
          worker_pool: worker_pool || engine_worker_pool(engine),
          idempotency_key:,
        )
      end

      #: (Object?, Symbol | String, *Object?, ?store: Store?, ?engine: Engine?, ?worker_pool: String?, ?idempotency_key: String?, **Object?) -> String
      def tell(durable_id, method_name, *args, store: nil, engine: nil, worker_pool: nil, idempotency_key: nil, **kwargs)
        store = Durababble.store_for(store:, engine:)
        store = store #: as untyped
        worker_pool ||= engine_worker_pool(engine)
        method_name = method_name.to_sym
        retry_policy = @exposed_commands[method_name]
        raise NoMethodError, "undefined durable object command `#{method_name}` for #{self}" unless retry_policy

        attributes = object_command_attributes(object_id: String(durable_id), method_name:)
        Observability.trace("durababble.object.command.enqueue", attributes) do
          message_id = store.enqueue_object_command(
            worker_pool:,
            object_type: object_type,
            object_id: String(durable_id),
            method_name: method_name.to_s,
            args:,
            kwargs:,
            message_kind: "tell",
            idempotency_key:,
            max_attempts: retry_policy.maximum_attempts_limit,
          )
          store.deliver_target_message(worker_pool:, target_kind: "object", target_type: object_type, target_id: String(durable_id))
          message_id
        end
      end

      #: (Object, object_type: String, object_id: String, ?worker_pool: String) -> Object?
      def state_from_store(store, object_type:, object_id:, worker_pool: "default")
        store = store #: as untyped
        state = store.object_state_entry(worker_pool:, object_type:, object_id:)
        state.equal?(Store::NO_OBJECT_STATE) ? UNINITIALIZED : state
      end

      # Single source for the object-command observability attribute bundle, so
      # the attribute key names are defined once and shared by both the class
      # (tell) and instance (DurableObjectRef) command paths.
      #: (object_type: String, object_id: String, method_name: Symbol | String) -> Hash[String, Object?]
      def command_attributes(object_type:, object_id:, method_name:)
        {
          "durababble.object.type" => object_type,
          "durababble.object.id" => object_id,
          "durababble.object.method" => method_name,
        }
      end

      private

      #: (Engine?) -> String
      def engine_worker_pool(engine)
        return "default" unless engine

        String(engine.worker_pool)
      end

      #: (object_id: String, method_name: Symbol | String) -> Hash[String, Object?]
      def object_command_attributes(object_id:, method_name:)
        command_attributes(object_type:, object_id:, method_name:)
      end
    end

    #: String?
    attr_reader :durable_id
    #: CommandContext?
    attr_reader :command_context
    #: String
    attr_reader :worker_pool

    #: (?durable_id: String?, ?state: Object?, ?store: Store?, ?command_context: CommandContext?, ?worker_pool: String) -> void
    def initialize(durable_id: nil, state: UNINITIALIZED, store: nil, command_context: nil, worker_pool: "default")
      @durable_id = durable_id
      @current_state = state
      @store = store #: as untyped
      @command_context = command_context
      @worker_pool = worker_pool
      @state_dirty = false
      @__durababble_query_context = false
    end

    #: () -> Object?
    def initialize_state
      nil
    end

    #: () -> Object?
    def current_state
      return @current_state unless @current_state.equal?(UNINITIALIZED)

      @current_state = initialize_state
    end

    #: (Object?) -> Object?
    def update_state(new_state)
      raise Error, "cannot update durable object state from an exposed query" if @__durababble_query_context

      @current_state = new_state
      @state_dirty = true
      @store&.save_object_state(worker_pool: @worker_pool, object_type: self.class.object_type, object_id: durable_id, state: new_state) unless command_context
      new_state
    end

    #: () -> bool
    def state_dirty? = @state_dirty
  end

  class DurableObjectRef
    COMMAND_WAIT_TIMEOUT_SLACK_SECONDS = 10

    #: (Object, String, store: Store, ?worker_pool: String?, ?idempotency_key: String?) -> void
    def initialize(object_class, durable_id, store:, worker_pool: nil, idempotency_key: nil)
      @object_class = object_class #: as untyped
      @durable_id = durable_id
      @store = store #: as untyped
      @worker_pool = worker_pool || "default"
      @idempotency_key = idempotency_key
    end

    #: String
    attr_reader :durable_id

    #: (Symbol, *Object?, **Object?) ?{ (Object?) -> Object? } -> Object?
    def method_missing(method_name, *args, **kwargs, &block)
      if @object_class.exposed_queries.key?(method_name)
        invoke_query(method_name, args:, kwargs:, block:)
      elsif (retry_policy = @object_class.exposed_commands[method_name])
        invoke_command(method_name, retry_policy:, args:, kwargs:, block:)
      else
        super
      end
    end

    #: (Symbol, ?bool) -> bool
    def respond_to_missing?(method_name, include_private = false)
      @object_class.exposed_queries.key?(method_name) || @object_class.exposed_commands.key?(method_name) || super
    end

    private

    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?], block: Object?) -> Object?
    def invoke_query(method_name, args:, kwargs:, block:)
      attributes = object_attributes(method_name:)
      Observability.trace("durababble.object.query", attributes) do
        state = DurableObject.state_from_store(@store, worker_pool: @worker_pool, object_type: @object_class.object_type, object_id: @durable_id)
        object = @object_class.new(durable_id: @durable_id, state:, store: @store, worker_pool: @worker_pool) #: as untyped
        object.instance_variable_set(:@__durababble_query_context, true)
        object.public_send(method_name, *args, **kwargs, &block)
      end
    end

    #: (Symbol, retry_policy: RetryPolicy, args: Array[Object?], kwargs: Hash[Symbol, Object?], block: Object?) -> Object?
    def invoke_command(method_name, retry_policy:, args:, kwargs:, block:)
      attributes = object_attributes(method_name:)
      Observability.trace("durababble.object.command.enqueue", attributes) do
        idempotency_key = kwargs.key?(:idempotency_key) ? kwargs.delete(:idempotency_key) : @idempotency_key
        command_id = @store.enqueue_object_command(
          worker_pool: @worker_pool,
          object_type: @object_class.object_type,
          object_id: @durable_id,
          method_name: method_name.to_s,
          args:,
          kwargs:,
          message_kind: "ask",
          idempotency_key:,
          max_attempts: retry_policy.maximum_attempts_limit,
        )
        @store.deliver_target_message(target_kind: "object", target_type: @object_class.object_type, target_id: @durable_id, worker_pool: @worker_pool)
        @store.wait_for_inbox_message(command_id, timeout: command_wait_timeout(retry_policy))
      end
    end

    #: (RetryPolicy) -> Numeric?
    def command_wait_timeout(retry_policy)
      attempts = retry_policy.maximum_attempts
      return unless attempts.finite?

      retry_delay = (1...attempts.to_i).sum { |attempt_number| retry_policy.delay_for_attempt(attempt_number) }
      COMMAND_WAIT_TIMEOUT_SLACK_SECONDS + retry_delay
    end

    #: (method_name: Symbol | String) -> Hash[String, Object?]
    def object_attributes(method_name:)
      DurableObject.command_attributes(object_type: @object_class.object_type, object_id: @durable_id, method_name:)
    end
  end

  class DurableObjectExecutor
    #: (store: untyped, objects: untyped, worker_id: untyped, lease_seconds: untyped, ?worker_pool: String) -> void
    def initialize(store:, objects:, worker_id:, lease_seconds:, worker_pool: "default")
      @store = store
      @objects = normalize_objects(objects)
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @worker_pool = worker_pool
    end

    #: (untyped, object_id: untyped, ?limit: untyped) -> untyped
    def drain_object_inbox(object_type, object_id:, limit: 10)
      object_class = @objects.fetch(object_type)
      drained = 0
      while drained < limit
        messages = @store.claim_inbox_messages(
          worker_pool: @worker_pool,
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
        result = object.public_send(method_name, *args, **kwargs)
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
      worker_pool = message.fetch("worker_pool", @worker_pool)
      state = DurableObject.state_from_store(@store, worker_pool:, object_type: object_class.object_type, object_id:)
      context = CommandContext.new(
        object_type: object_class.object_type,
        durable_id: object_id,
        command_id: message.fetch("id"),
        attempt_number: message.fetch("attempts").to_i,
        idempotency_key: "durababble:v1:object:#{object_class.object_type}:#{object_id}:command:#{message.fetch("id")}",
      )
      object_class.new(durable_id: object_id, state:, store: @store, command_context: context, worker_pool:) #: as untyped
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
      @store.current_time + delay
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
