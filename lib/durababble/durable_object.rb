# typed: true
# frozen_string_literal: true

require_relative "durable_method_dsl"
require_relative "execution_context"
require_relative "error_formatting"

module Durababble
  CommandContext = Data.define(:object_type, :durable_id, :command_id, :attempt_number, :idempotency_key)
  ObjectWakeupChange = Data.define(:action, :name, :wake_at, :payload)

  class DurableObject
    extend DurableMethodDSL

    UNINITIALIZED = Object.new.freeze

    # Wake names are stored in the indexed `name` column on `object_wakeups`
    # (VARCHAR(191) / text). Bound the byte length so a name never silently
    # truncates against the index width.
    WAKE_NAME_MAX_BYTES = 191

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

      #: (Object?, Symbol | String, *Object?, ?store: Store?, ?engine: Engine?, ?worker_pool: String?, ?idempotency_key: String?, **Object?) ?{ (Object?) -> Object? } -> String
      def tell(durable_id, method_name, *args, store: nil, engine: nil, worker_pool: nil, idempotency_key: nil, **kwargs, &block)
        raise ArgumentError, "blocks cannot be passed to durable object command `#{method_name}`: command arguments are serialized across nodes and blocks cannot be" if block

        store = Durababble.store_for(store:, engine:)
        store = store #: as untyped
        worker_pool ||= engine_worker_pool(engine)
        method_name = method_name.to_sym
        retry_policy = @exposed_commands[method_name]
        raise NoMethodError, "undefined durable object command `#{method_name}` for #{self}" unless retry_policy

        if (execution = WorkflowExecutionContext.current)
          rpc_result = execution.call_handle_rpc(
            target_kind: "object",
            target_type: object_type,
            target_id: String(durable_id),
            method_name:,
            rpc_kind: "object_tell",
            args:,
            kwargs: kwargs.merge(idempotency_key:),
            retry_policy:,
          ) do |idempotency_key:, args:, kwargs:|
            rpc_args = args #: as Array[Object?]
            rpc_kwargs = kwargs #: as Hash[Symbol, Object?]
            enqueue_tell(
              store:,
              worker_pool:,
              object_id: String(durable_id),
              method_name:,
              args: rpc_args,
              kwargs: rpc_kwargs,
              idempotency_key:,
              retry_policy:,
            )
          end
          return rpc_result #: as String
        end

        enqueue_tell(
          store:,
          worker_pool:,
          object_id: String(durable_id),
          method_name:,
          args:,
          kwargs:,
          idempotency_key:,
          retry_policy:,
        )
      end

      #: (store: Store, worker_pool: String, object_id: String, method_name: Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?], idempotency_key: String?, retry_policy: RetryPolicy) -> String
      def enqueue_tell(store:, worker_pool:, object_id:, method_name:, args:, kwargs:, idempotency_key:, retry_policy:)
        attributes = object_command_attributes(object_id:, method_name:)
        Observability.trace("durababble.object.command.enqueue", attributes) do
          message_id = store.enqueue_object_command(
            worker_pool:,
            object_type: object_type,
            object_id:,
            method_name: method_name.to_s,
            args:,
            kwargs:,
            message_kind: "tell",
            idempotency_key:,
            max_attempts: retry_policy.maximum_attempts_limit,
          )
          store.deliver_target_message(worker_pool: inbox_worker_pool(store, message_id, fallback: worker_pool), target_kind: "object", target_type: object_type, target_id: object_id)
          message_id
        end
      end

      # Object state is keyed by (object_type, object_id) alone; worker_pool is
      # routing metadata, not identity, so reads never need a worker pool.
      #: (Object, object_type: String, object_id: String) -> Object?
      def state_from_store(store, object_type:, object_id:)
        store = store #: as untyped
        state = store.object_state_entry(object_type:, object_id:)
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

      #: (Store, String, fallback: String) -> String
      def inbox_worker_pool(store, message_id, fallback:)
        message = store.inbox_message(message_id) if store.respond_to?(:inbox_message)
        message&.fetch("worker_pool", fallback) || fallback
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
    #: Array[ObjectWakeupChange]
    attr_reader :wakeup_changes

    #: (?durable_id: String?, ?state: Object?, ?store: Store?, ?command_context: CommandContext?, ?worker_pool: String) -> void
    def initialize(durable_id: nil, state: UNINITIALIZED, store: nil, command_context: nil, worker_pool: "default")
      @durable_id = durable_id
      @current_state = state
      @store = store
      @command_context = command_context
      @worker_pool = worker_pool
      @state_dirty = false
      @wakeup_changes = []
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
      if @store && !command_context
        object_id = durable_id || raise(Error, "durable object state cannot be saved without a durable id")
        @store.save_object_state(worker_pool: @worker_pool, object_type: self.class.object_type, object_id:, state: new_state)
      end
      new_state
    end

    # Schedule a named wake for this object. Re-scheduling the same name before
    # it fires replaces that name's wake time and payload; other names are left
    # untouched. Wakes are committed atomically with the command, so a failed or
    # retried command never leaves behind an orphaned process-local timer.
    #: (name: String, at: Time, ?payload: Object?) -> Time
    def schedule_wake(name:, at:, payload: nil)
      raise Error, "cannot schedule durable object wakeups from an exposed query" if @__durababble_query_context
      raise Error, "durable object wakeups can only be scheduled from object commands" unless command_context

      @wakeup_changes << ObjectWakeupChange.new(:schedule, coerce_wake_name(name), at, payload)
      at
    end

    # Cancel a single named wake. Wakes that are not scheduled are a no-op.
    #: (name: String) -> bool
    def cancel_wake(name:)
      raise Error, "cannot cancel durable object wakeups from an exposed query" if @__durababble_query_context
      raise Error, "durable object wakeups can only be canceled from object commands" unless command_context

      @wakeup_changes << ObjectWakeupChange.new(:cancel, coerce_wake_name(name), nil, nil)
      true
    end

    # Cancel every pending wake for this object.
    #: () -> bool
    def cancel_all_wakes
      raise Error, "cannot cancel durable object wakeups from an exposed query" if @__durababble_query_context
      raise Error, "durable object wakeups can only be canceled from object commands" unless command_context

      @wakeup_changes << ObjectWakeupChange.new(:cancel_all, nil, nil, nil)
      true
    end

    #: () -> bool
    def state_dirty? = @state_dirty

    # True when the consumer of an in-progress `expose_stream` has gone away.
    # Indefinite stream producers should poll this and return when it flips.
    #: () -> bool
    def stream_cancelled? = Durababble.stream_cancelled?

    private

    #: (Object?) -> String
    def coerce_wake_name(name)
      name = String(name)
      raise Error, "durable object wake name cannot be empty" if name.empty?
      raise Error, "durable object wake name cannot exceed #{WAKE_NAME_MAX_BYTES} bytes" if name.bytesize > WAKE_NAME_MAX_BYTES

      name
    end
  end

  class DurableObjectRef
    COMMAND_WAIT_TIMEOUT_SLACK_SECONDS = 10

    #: (Object, String, store: Store, ?worker_pool: String?, ?idempotency_key: String?) -> void
    def initialize(object_class, durable_id, store:, worker_pool: nil, idempotency_key: nil)
      @object_class = object_class #: as untyped
      @durable_id = durable_id
      @store = store
      @worker_pool = worker_pool || "default"
      @idempotency_key = idempotency_key
    end

    #: String
    attr_reader :durable_id

    #: (Symbol, *Object?, **Object?) ?{ (Object?) -> Object? } -> Object?
    def method_missing(method_name, *args, **kwargs, &block)
      if @object_class.exposed_queries.key?(method_name)
        if (execution = WorkflowExecutionContext.current)
          return execution.call_handle_rpc(
            target_kind: "object",
            target_type: @object_class.object_type,
            target_id: @durable_id,
            method_name:,
            rpc_kind: "object_query",
            args:,
            kwargs:,
          ) do |args:, kwargs:, **|
            rpc_args = args #: as Array[Object?]
            rpc_kwargs = kwargs #: as Hash[Symbol, Object?]
            invoke_query(method_name, args: rpc_args, kwargs: rpc_kwargs, block:)
          end
        end

        invoke_query(method_name, args:, kwargs:, block:)
      elsif @object_class.exposed_streams.key?(method_name)
        open_object_stream(method_name, args:, kwargs:)
      elsif (retry_policy = @object_class.exposed_commands[method_name])
        raise ArgumentError, "blocks cannot be passed to durable object command ##{method_name}: command arguments are serialized across nodes and blocks cannot be" if block

        if (execution = WorkflowExecutionContext.current)
          return execution.call_handle_rpc(
            target_kind: "object",
            target_type: @object_class.object_type,
            target_id: @durable_id,
            method_name:,
            rpc_kind: "object_command",
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

    #: (Symbol, ?bool) -> bool
    def respond_to_missing?(method_name, include_private = false)
      @object_class.exposed_queries.key?(method_name) ||
        @object_class.exposed_streams.key?(method_name) ||
        @object_class.exposed_commands.key?(method_name) || super
    end

    private

    # An object stream routes to the unified `durable_objects` lease holder when
    # one exists, so producers (and the commands they observe) all run on the
    # single exclusive owner node. When no live lease exists but this process is
    # itself a worker, the consumer self-routes to its own RPC server via
    # `Durababble.local_stream_host`; the server-side dispatcher then claims the
    # lease as part of `with_lease`, so the first opener becomes the owner.
    # Plain consumers without a local host fall back to a local state snapshot
    # (same semantics as exposed queries).
    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?]) -> ResultStream
    def open_object_stream(method_name, args:, kwargs:)
      lease = @store.current_object_lease(@object_class.object_type, @durable_id)
      if lease
        open_remote_object_stream(method_name, args:, kwargs:, lease:)
      elsif (host = Durababble.local_stream_host)
        synthetic = { "worker_id" => host.worker_id, "worker_pool" => host.worker_pool }
        open_remote_object_stream(method_name, args:, kwargs:, lease: synthetic)
      else
        open_local_object_stream(method_name, args:, kwargs:)
      end
    end

    # Opens the stream on the node that currently holds the object lease. The
    # lease's `worker_id` is a worker *identity* (`id@host:port`); we resolve it
    # to the bare RPC address before building the client, since the identity's
    # `id` segment is for fencing, not addressing. The pool is taken from the
    # lease, falling back to the ref's pool and then "default".
    #
    # A bounded re-resolve handles two narrow races: (1) the previous owner
    # crashed but its row has not expired yet (consumer sees `NodeUnavailable`
    # on connect), and (2) a fresh claim arrived between the consumer's lookup
    # and the server-side `with_lease` (server raises `StaleLease` before any
    # value has been emitted). One retry off `current_object_lease` lets a
    # follow-on open land on the new owner; we only retry when nothing has been
    # delivered yet so a mid-stream failure still surfaces.
    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?], lease: Hash[String, Object?]) -> ResultStream
    def open_remote_object_stream(method_name, args:, kwargs:, lease:)
      object_class = @object_class
      durable_id = @durable_id
      store = @store
      ref_pool = @worker_pool
      ResultStream.new do |writer|
        attempts = 0
        emitted = 0
        current = lease
        begin
          attempts += 1
          emitted = run_object_remote_request(method_name, args:, kwargs:, lease: current, store:, writer:, object_class:, durable_id:, ref_pool:)
        rescue WorkflowRpc::StaleLease, WorkflowRpc::NodeUnavailable
          # Once any frame has been forwarded to the consumer, a retry would
          # double-emit, so propagate. Otherwise re-resolve the lease once and
          # try a fresh owner.
          raise if emitted.positive? || attempts >= 2

          refreshed = store.current_object_lease(object_class.object_type, durable_id)
          raise unless refreshed

          current = refreshed
          retry
        end
      end
    end

    # Inner RPC call — pulled out so `open_remote_object_stream` can re-resolve
    # on a transient failure before any value has been forwarded. Returns the
    # number of values forwarded so the caller can decide whether retrying is
    # still safe.
    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?], lease: Hash[String, Object?], store: untyped, writer: untyped, object_class: untyped, durable_id: String, ref_pool: String) -> Integer
    def run_object_remote_request(method_name, args:, kwargs:, lease:, store:, writer:, object_class:, durable_id:, ref_pool:)
      worker_id = lease.fetch("worker_id") #: as String
      worker_pool = lease["worker_pool"] || ref_pool #: as String
      client = store.rpc_client_factory.call(WorkerIdentity.address_for(worker_id)) #: as untyped
      remote = client.call_transient_stream(
        worker_pool:,
        class_name: object_class.object_type,
        durable_object_id: durable_id,
        method: method_name.to_s,
        args: { "args" => args, "kwargs" => kwargs },
      )
      count = 0
      remote.each do |value|
        writer.emit(value)
        count += 1
      end
      count
    end

    # No live lease and no local host: run the `expose_stream` method against a
    # local snapshot instance as the `ResultStream`'s producer task. Publishes
    # the writer so `Durababble.stream_cancelled?` works inside the method body.
    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?]) -> ResultStream
    def open_local_object_stream(method_name, args:, kwargs:)
      object_class = @object_class
      store = @store
      durable_id = @durable_id
      worker_pool = @worker_pool
      ResultStream.new do |writer|
        StreamExecutionContext.with_current(writer) do
          state = DurableObject.state_from_store(store, object_type: object_class.object_type, object_id: durable_id)
          object = object_class.new(durable_id:, state:, store:, worker_pool:) #: as untyped
          object.instance_variable_set(:@__durababble_query_context, true)
          emit = ->(value) { writer.emit(value) }
          object.public_send(method_name, *args, **kwargs, &emit)
        end
      end
    end

    #: (Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?], block: Object?) -> Object?
    def invoke_query(method_name, args:, kwargs:, block:)
      attributes = object_attributes(method_name:)
      Observability.trace("durababble.object.query", attributes) do
        state = DurableObject.state_from_store(@store, object_type: @object_class.object_type, object_id: @durable_id)
        object = @object_class.new(durable_id: @durable_id, state:, store: @store, worker_pool: @worker_pool) #: as untyped
        object.instance_variable_set(:@__durababble_query_context, true)
        object.public_send(method_name, *args, **kwargs, &block)
      end
    end

    #: (Symbol, retry_policy: RetryPolicy, args: Array[Object?], kwargs: Hash[Symbol, Object?], ?idempotency_key: Object?) -> Object?
    def invoke_command(method_name, retry_policy:, args:, kwargs:, idempotency_key: nil)
      attributes = object_attributes(method_name:)
      Observability.trace("durababble.object.command.enqueue", attributes) do
        command_kwargs = kwargs.dup
        command_idempotency_key = string_idempotency_key(idempotency_key) || (command_kwargs.key?(:idempotency_key) ? string_idempotency_key(command_kwargs.delete(:idempotency_key)) : @idempotency_key)
        command_id = @store.enqueue_object_command(
          worker_pool: @worker_pool,
          object_type: @object_class.object_type,
          object_id: @durable_id,
          method_name: method_name.to_s,
          args:,
          kwargs: command_kwargs,
          message_kind: "ask",
          idempotency_key: command_idempotency_key,
          max_attempts: retry_policy.maximum_attempts_limit,
        )
        @store.deliver_target_message(
          target_kind: "object",
          target_type: @object_class.object_type,
          target_id: @durable_id,
          worker_pool: inbox_worker_pool(command_id),
        )
        @store.wait_for_inbox_message(command_id, timeout: command_wait_timeout(retry_policy))
      end
    end

    #: (Object?) -> String?
    def string_idempotency_key(value)
      value&.to_s
    end

    #: (String) -> String
    def inbox_worker_pool(message_id)
      message = @store.inbox_message(message_id) if @store.respond_to?(:inbox_message)
      message&.fetch("worker_pool", @worker_pool) || @worker_pool
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
    #: (store: Store, objects: Object, worker_id: String, lease_seconds: Numeric, ?worker_pool: String) -> void
    def initialize(store:, objects:, worker_id:, lease_seconds:, worker_pool: "default")
      @store = store
      @objects = normalize_objects(objects)
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @worker_pool = worker_pool
    end

    #: (String, object_id: String, ?limit: Integer) -> Integer
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
    ensure
      # The gated claim inside `claim_inbox_messages` acquires the unified object
      # lease as part of taking command rows; releasing it when drain returns hands
      # ownership back so another worker (or this one's stream lane on a future
      # opener) can claim. If a streaming `ObjectStreamHost` is currently using the
      # same lease, its renewal task will lose this row and the stream ends with
      # `StaleLease`, which is the documented graceful-eviction behavior. Skipping
      # this release would strand the lease for `lease_seconds` after the worker
      # idles, blocking any other node from picking up the next command. Every
      # Store implementation provides `release_object_lease`; the base no-op
      # falls through cleanly when no lease was actually taken.
      @store.release_object_lease(object_type:, object_id:, worker_id: @worker_id)
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
        object.public_send(:on_wake, name: message.fetch("method_name"), payload: message.fetch("payload"))
      end
      complete_message(object, message, result:)
    rescue LeaseConflict
      raise
    rescue StandardError => e
      terminal_failure(message, ErrorFormatting.format_error(e))
    end

    #: (untyped, object_id: untyped, message: untyped) -> untyped
    def build_object(object_class, object_id:, message:)
      worker_pool = message.fetch("worker_pool", @worker_pool)
      state = DurableObject.state_from_store(@store, object_type: object_class.object_type, object_id:)
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
      completed = if object.state_dirty? || object.wakeup_changes.any?
        @store.complete_object_command(
          command_id: message.fetch("id"),
          result:,
          object_type: object.class.object_type,
          object_id: object.durable_id,
          state: object.state_dirty? ? object.current_state : Store::NO_OBJECT_STATE,
          wakeup_changes: object.wakeup_changes,
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
      serialized_error = ErrorFormatting.format_error(error)
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
