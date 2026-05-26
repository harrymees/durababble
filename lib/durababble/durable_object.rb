# typed: true
# frozen_string_literal: true

require_relative "durable_method_dsl"

module Durababble
  CommandContext = Data.define(:object_type, :durable_id, :command_id, :attempt_number, :idempotency_key)
  class ObjectReadBlocked < Error; end

  class DurableObject
    extend DurableMethodDSL

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
        DurableObjectRef.new(self, String(durable_id), store: Durababble.store_for(store:, engine:), worker_pool:, idempotency_key:)
      end

      #: (Object?, Symbol | String, *Object?, ?store: Store?, ?engine: Engine?, ?idempotency_key: String?, **Object?) -> String
      def tell(durable_id, method_name, *args, store: nil, engine: nil, idempotency_key: nil, **kwargs)
        store = Durababble.store_for(store:, engine:)
        store = store #: as untyped
        method_name = method_name.to_sym
        retry_policy = @exposed_commands[method_name]
        raise NoMethodError, "undefined durable object command `#{method_name}` for #{self}" unless retry_policy

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

      #: (RetryPolicy) -> Integer?
      def inbox_max_attempts(retry_policy)
        attempts = retry_policy.maximum_attempts
        attempts.finite? ? attempts.to_i : nil
      end

      #: (object_id: String, method_name: Symbol | String) -> Hash[String, Object?]
      def object_command_attributes(object_id:, method_name:)
        {
          "durababble.object.type" => object_type,
          "durababble.object.id" => object_id,
          "durababble.object.method" => method_name,
        }
      end
    end

    #: String?
    attr_reader :durable_id
    #: CommandContext?
    attr_reader :command_context

    #: (?durable_id: String?, ?state: Object?, ?store: Store?, ?command_context: CommandContext?) -> void
    def initialize(durable_id: nil, state: nil, store: nil, command_context: nil)
      @durable_id = durable_id
      @current_state = state
      @store = store #: as untyped
      @command_context = command_context
      @state_dirty = false
      @__durababble_query_context = false
    end

    #: () -> Object?
    def initialize_state
      nil
    end

    #: () -> Object?
    def current_state
      @current_state.nil? ? initialize_state : @current_state
    end

    #: (Object?) -> Object?
    def update_state(new_state)
      raise Error, "cannot update durable object state from an exposed query" if @__durababble_query_context

      @current_state = new_state
      @state_dirty = true
      @store&.save_object_state(object_type: self.class.object_type, object_id: durable_id, state: new_state) unless command_context
      new_state
    end

    #: () -> bool
    def state_dirty? = @state_dirty
  end

  class DurableObjectRef
    COMMAND_WAIT_TIMEOUT_SLACK_SECONDS = 10
    TRANSIENT_ROUTE_ATTEMPTS = 2

    class TransientRequest
      #: String
      attr_reader :class_name

      #: (class_name: String, object_id: String, method: String) -> void
      def initialize(class_name:, object_id:, method:)
        @class_name = class_name
        @object_id = object_id
        @method = method
      end

      #: (String | Symbol) -> String?
      def [](key)
        case key.to_s
        when "method"
          @method
        when "object_id"
          @object_id
        end
      end
    end

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
        attempts = 0
        begin
          if (lease = current_object_lease)
            return invoke_owned_query(method_name, args:, kwargs:, block:) if current_runtime_owns?(lease)

            return invoke_remote_query(method_name, args:, kwargs:, lease:)
          end

          DurableObjectTransientHandler.assert_read_gate_open!(@store, object_type: @object_class.object_type, object_id: @durable_id)
          invoke_local_query(method_name, args:, kwargs:, block:)
        rescue WorkflowRpc::NoActiveLease, WorkflowRpc::StaleLease
          attempts += 1
          retry if attempts < TRANSIENT_ROUTE_ATTEMPTS

          raise
        end
      end
    end

    #: () -> Hash[String, Object?]?
    def current_object_lease
      return unless @store.respond_to?(:current_object_lease)

      @store.current_object_lease(@object_class.object_type, @durable_id)
    end

    #: (Hash[String, Object?]) -> bool
    def current_runtime_owns?(lease)
      worker_id = @store.local_worker_id if @store.respond_to?(:local_worker_id)
      worker_id = worker_id.call if worker_id.respond_to?(:call)
      !!(!worker_id.nil? && lease.fetch("worker_id") == worker_id)
    end

    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?], block: Object?) -> Object?
    def invoke_owned_query(method_name, args:, kwargs:, block:)
      handler = @store.local_transient_handler if @store.respond_to?(:local_transient_handler)
      if handler
        return handler.call(
          request: TransientRequest.new(class_name: @object_class.object_type, object_id: @durable_id, method: method_name.to_s),
          args: { "args" => args, "kwargs" => kwargs },
        )
      end

      DurableObjectTransientHandler.assert_read_gate_open!(@store, object_type: @object_class.object_type, object_id: @durable_id)
      invoke_local_query(method_name, args:, kwargs:, block:)
    end

    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?], lease: Hash[String, Object?]) -> Object?
    def invoke_remote_query(method_name, args:, kwargs:, lease:)
      worker_id = lease.fetch("worker_id")
      client = @store.rpc_client_factory.call(worker_id)
      client.call_transient(
        worker_pool: @worker_pool,
        class_name: @object_class.object_type,
        object_id: @durable_id,
        method: method_name.to_s,
        args: { "args" => args, "kwargs" => kwargs },
      )
    rescue Durababble::Rpc::Unavailable => e
      raise WorkflowRpc::NodeUnavailable, e.message
    end

    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?], block: Object?) -> Object?
    def invoke_local_query(method_name, args:, kwargs:, block:)
      state = @store.object_state(object_type: @object_class.object_type, object_id: @durable_id)
      object = @object_class.new(durable_id: @durable_id, state:, store: @store)
      object.instance_variable_set(:@__durababble_query_context, true)
      kwargs.empty? ? object.public_send(method_name, *args, &block) : object.public_send(method_name, *args, **kwargs, &block)
    end

    #: (Symbol, retry_policy: RetryPolicy, args: Array[Object?], kwargs: Hash[Symbol, Object?], block: Object?) -> Object?
    def invoke_command(method_name, retry_policy:, args:, kwargs:, block:)
      attributes = object_attributes(method_name:)
      Observability.trace("durababble.object.command.enqueue", attributes) do
        idempotency_key = kwargs.key?(:idempotency_key) ? kwargs.delete(:idempotency_key) : @idempotency_key
        command_id = @store.enqueue_object_command(
          object_type: @object_class.object_type,
          object_id: @durable_id,
          method_name: method_name.to_s,
          args:,
          kwargs:,
          message_kind: "ask",
          idempotency_key:,
          max_attempts: inbox_max_attempts(retry_policy),
        )
        @store.deliver_target_message(target_kind: "object", target_type: @object_class.object_type, target_id: @durable_id, worker_pool: @worker_pool)
        @store.wait_for_inbox_message(command_id, timeout: command_wait_timeout(retry_policy))
      end
    end

    #: (RetryPolicy) -> Integer?
    def inbox_max_attempts(retry_policy)
      attempts = retry_policy.maximum_attempts
      attempts.finite? ? attempts.to_i : nil
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
      {
        "durababble.object.type" => @object_class.object_type,
        "durababble.object.id" => @durable_id,
        "durababble.object.method" => method_name,
      }
    end
  end

  class DurableObjectTransientHandler
    BLOCKING_READ_STATUSES = ["pending", "failed", "running", "dead_lettered"].freeze

    class << self
      #: (untyped, object_type: untyped, object_id: untyped) -> void
      def assert_read_gate_open!(store, object_type:, object_id:)
        blocker = blocking_read_message(store, object_type:, object_id:)
        return unless blocker

        raise ObjectReadBlocked, "durable object #{object_type}/#{object_id} transient read is blocked by #{blocker.fetch("status")} mailbox head #{blocker.fetch("id")}"
      end

      private

      #: (untyped, object_type: untyped, object_id: untyped) -> untyped
      def blocking_read_message(store, object_type:, object_id:)
        return unless store.respond_to?(:inbox_messages_for)

        messages = store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
        messages.find { |message| BLOCKING_READ_STATUSES.include?(message.fetch("status").to_s) }
      end
    end

    #: (store: untyped, objects: untyped, node_id: untyped) -> void
    def initialize(store:, objects:, node_id:)
      @store = store
      @objects = normalize_objects(objects)
      @node_id = node_id
    end

    #: (request: untyped, args: untyped) -> untyped
    def call(request:, args:)
      object_type = request.class_name
      object_id = request["object_id"]
      method_name = request["method"].to_sym
      object_class = @objects.fetch(object_type) do
        raise WorkflowRpc::UnknownCommand, "unknown durable object type #{object_type}"
      end
      unless object_class.exposed_queries.key?(method_name)
        raise WorkflowRpc::UnknownCommand, "unknown durable object transient method #{object_type}##{method_name}"
      end

      assert_current_lease!(object_type:, object_id:)
      self.class.assert_read_gate_open!(@store, object_type:, object_id:)
      result = invoke_query(object_class, object_id:, method_name:, payload: args || {})
      assert_current_lease!(object_type:, object_id:)
      result
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

    #: (object_type: untyped, object_id: untyped) -> void
    def assert_current_lease!(object_type:, object_id:)
      lease = @store.current_object_lease(object_type, object_id)
      raise WorkflowRpc::NoActiveLease, "durable object #{object_type}/#{object_id} has no active owner" unless lease

      expected = node_id
      return if lease.fetch("worker_id") == expected

      raise WorkflowRpc::StaleLease, "#{expected} no longer owns durable object #{object_type}/#{object_id}; current owner is #{lease.fetch("worker_id")}"
    end

    #: () -> untyped
    def node_id
      @node_id.respond_to?(:call) ? @node_id.call : @node_id
    end

    #: (untyped, object_id: untyped, method_name: untyped, payload: untyped) -> untyped
    def invoke_query(object_class, object_id:, method_name:, payload:)
      payload = payload || {}
      args = payload.fetch("args", [])
      kwargs = payload.fetch("kwargs", {})
      state = @store.object_state(object_type: object_class.object_type, object_id:)
      object = object_class.new(durable_id: object_id, state:, store: @store)
      object.instance_variable_set(:@__durababble_query_context, true)
      kwargs.empty? ? object.public_send(method_name, *args) : object.public_send(method_name, *args, **kwargs)
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
