# typed: true
# frozen_string_literal: true

require_relative "execution_context"

module Durababble
  # Server-side handler for `call_transient_stream` RPCs. Resolves the target
  # workflow/durable-object class from the request, builds an owner-local
  # instance, then invokes the `expose_stream` method, forwarding each yielded
  # value to the RPC writer.
  #
  # Both lanes re-check the lease before every emit, once up front, and after the
  # stream method returns: if this node has lost the lease the dispatcher raises
  # `StaleLease`, which the server turns into a terminal error frame so the
  # consumer re-raises it. Workflow streams use `current_workflow_lease`; object
  # streams hold the unified `durable_objects` lease through an
  # `ObjectStreamHost` (refcounted on this node, renewed by an Async task). When
  # no host is wired the dispatcher
  # still requires the current object lease to belong to this node; it never
  # serves a lease-free local snapshot.
  #
  # The wire `args` payload is `{ "args" => [...positional], "kwargs" => {...} }`,
  # matching what `WorkflowRef`/`DurableObjectRef` send when routing remotely.
  class StreamDispatcher
    #: (store: untyped, workflows: untyped, objects: untyped, node_id: String, ?object_stream_host: ObjectStreamHost?, ?lease_seconds: Integer) -> void
    def initialize(store:, workflows:, objects:, node_id:, object_stream_host: nil, lease_seconds: ObjectStreamHost::DEFAULT_LEASE_SECONDS)
      @store = store
      @workflows = normalize_registry(workflows, &:workflow_name)
      @objects = normalize_registry(objects, &:object_type)
      @node_id = node_id
      @object_stream_host = object_stream_host
      @lease_seconds = lease_seconds
    end

    # Matches the `stream_handler` contract: called with the decoded request, the
    # decoded `args` payload, and the `Rpc::StreamWriter` to emit through.
    #: (request: Rpc::Messages::TransientRequest, args: Hash[String, untyped]?, writer: untyped) -> void
    def call(request:, args:, writer:)
      if request.workflow_id.to_s.empty?
        dispatch_object_stream(request:, args:, writer:)
      else
        dispatch_workflow_stream(request:, args:, writer:)
      end
    end

    private

    #: (request: Rpc::Messages::TransientRequest, args: Hash[String, untyped]?, writer: untyped) -> void
    def dispatch_object_stream(request:, args:, writer:)
      object_class = @objects.fetch(request.class_name) do
        raise WorkflowRpc::UnknownCommand, "unknown durable object #{request.class_name}"
      end
      worker_pool = request.worker_pool.to_s.empty? ? "default" : request.worker_pool
      object_type = object_class.object_type
      object_id = request.durable_object_id

      if @object_stream_host
        @object_stream_host.with_lease(worker_pool:, object_type:, object_id:, lease_seconds: @lease_seconds) do |entry|
          run_object_producer(object_class:, object_id:, worker_pool:, request:, args:, writer:, entry:)
          # Eviction with no in-flight emit must still surface as `StaleLease`
          # rather than as a clean end, so the consumer can reconnect onto the
          # new owner instead of believing the stream finished.
          raise WorkflowRpc::StaleLease, "object lease for #{worker_pool}/#{object_type}/#{object_id} lost" if entry.lost
        end
      else
        assert_object_lease!(object_type:, object_id:)
        run_object_producer(object_class:, object_id:, worker_pool:, request:, args:, writer:, entry: nil)
        assert_object_lease!(object_type:, object_id:)
      end
    end

    #: (object_class: untyped, object_id: String, worker_pool: String, request: Rpc::Messages::TransientRequest, args: Hash[String, untyped]?, writer: untyped, entry: ObjectStreamHost::Entry?) -> void
    def run_object_producer(object_class:, object_id:, worker_pool:, request:, args:, writer:, entry:)
      state = DurableObject.state_from_store(@store, object_type: object_class.object_type, object_id:)
      object = object_class.new(durable_id: object_id, state:, store: @store, worker_pool:)
      object.instance_variable_set(:@__durababble_query_context, true)
      lease_writer = entry ? ObjectStreamLeaseWriter.new(writer, entry:) : writer
      invoke_stream(object, request:, args:, writer: lease_writer)
    end

    #: (request: Rpc::Messages::TransientRequest, args: Hash[String, untyped]?, writer: untyped) -> void
    def dispatch_workflow_stream(request:, args:, writer:)
      workflow_id = request.workflow_id
      workflow_class = @workflows.fetch(request.class_name) do
        raise WorkflowRpc::UnknownCommand, "unknown workflow #{request.class_name}"
      end
      assert_workflow_lease!(workflow_id)
      instance = workflow_class.new #: as untyped
      instance.instance_variable_set(:@__durababble_ref_store, @store)
      instance.instance_variable_set(:@__durababble_ref_workflow_id, workflow_id)
      lease_writer = LeaseCheckingWriter.new(writer, store: @store, workflow_id:, node_id: @node_id)
      invoke_stream(instance, request:, args:, writer: lease_writer)
      assert_workflow_lease_still_held!(workflow_id)
    end

    #: (untyped, request: Rpc::Messages::TransientRequest, args: Hash[String, untyped]?, writer: untyped) -> void
    def invoke_stream(target, request:, args:, writer:)
      method_name = request["method"].to_s.to_sym
      assert_exposed_stream!(target.class, method_name)
      payload = args || {}
      positional = Array(payload["args"])
      keywords = symbolize_keys(payload["kwargs"])
      emit = ->(value) { writer.emit(value) }
      StreamExecutionContext.with_current(writer) do
        with_stream_query_context(target) do
          if keywords.empty?
            target.public_send(method_name, *positional, &emit)
          else
            target.public_send(method_name, *positional, **keywords, &emit)
          end
        end
      end
    end

    #: (untyped) { () -> Object? } -> Object?
    def with_stream_query_context(target, &block)
      case target
      when DurableObject
        ObjectQueryExecutionContext.with_current(target, &block)
      when Workflow
        WorkflowQueryContext.with_current(true, &block)
      else
        block.call
      end
    end

    # Only methods registered with `expose_stream` may be invoked over the wire.
    # Without this guard a `call_transient_stream` request could `public_send` any
    # public method on the owner-local instance. Mirrors the `exposed_commands` guard
    # on the durable-object command path.
    #: (untyped, Symbol) -> void
    def assert_exposed_stream!(target_class, method_name)
      return if target_class.exposed_streams.key?(method_name)

      raise WorkflowRpc::UnknownCommand, "#{method_name} is not an exposed stream on #{target_class}"
    end

    #: (String) -> void
    def assert_workflow_lease!(workflow_id)
      lease = @store.current_workflow_lease(workflow_id)
      raise WorkflowRpc::NoActiveLease, "workflow #{workflow_id} has no active lease" unless lease
      return if lease.fetch("worker_id") == @node_id

      raise WorkflowRpc::StaleLease, "#{@node_id} does not own workflow #{workflow_id}; current owner is #{lease.fetch("worker_id")}"
    end

    #: (String) -> void
    def assert_workflow_lease_still_held!(workflow_id)
      lease = @store.current_workflow_lease(workflow_id)
      return if lease && lease.fetch("worker_id") == @node_id

      raise WorkflowRpc::StaleLease, "#{@node_id} no longer owns workflow #{workflow_id}"
    end

    #: (object_type: String, object_id: String) -> void
    def assert_object_lease!(object_type:, object_id:)
      lease = @store.current_object_lease(object_type, object_id)
      raise WorkflowRpc::NoActiveLease, "durable object #{object_type}/#{object_id} has no active lease" unless lease
      return if lease.fetch("worker_id") == @node_id

      raise WorkflowRpc::StaleLease, "#{@node_id} does not own durable object #{object_type}/#{object_id}; current owner is #{lease.fetch("worker_id")}"
    end

    #: (untyped) -> Hash[Symbol, untyped]
    def symbolize_keys(value)
      return {} unless value.is_a?(Hash)

      value.transform_keys(&:to_sym)
    end

    #: (untyped) { (untyped) -> untyped } -> Hash[String, untyped]
    def normalize_registry(registry, &name_for)
      case registry
      when Hash
        registry.transform_keys(&:to_s)
      else
        Array(registry).to_h { |klass| [name_for.call(klass), klass] }
      end
    end

    # Wraps the RPC writer for a workflow stream so emits periodically confirm
    # this node still holds the lease; a lost lease ends the stream with
    # `StaleLease`. Delegates `cancelled?` so `Durababble.stream_cancelled?` keeps
    # working.
    #
    # The re-check is throttled to at most once per `RECHECK_INTERVAL`: a lease
    # returned by `current_workflow_lease` is valid through its `locked_until`
    # (a non-expired lease only changes hands via an explicit release+reclaim, far
    # rarer than the per-emit cadence), so re-querying the store on every quack is
    # wasteful. `RECHECK_INTERVAL` (well under `lease_seconds`) still catches an
    # explicit hand-off within a second. The dispatcher already verified ownership
    # up front, so the first interval trusts that check.
    class LeaseCheckingWriter
      RECHECK_INTERVAL = 1.0 #: Float

      #: (untyped, store: untyped, workflow_id: String, node_id: String) -> void
      def initialize(inner, store:, workflow_id:, node_id:)
        @inner = inner
        @store = store
        @workflow_id = workflow_id
        @node_id = node_id
        @next_check_at = monotonic_now + RECHECK_INTERVAL #: Float
      end

      #: (Object?) -> void
      def emit(value)
        assert_lease! if due_for_recheck?
        @inner.emit(value)
      end

      #: () -> bool
      def cancelled?
        @inner.cancelled?
      end

      private

      # True at most once per `RECHECK_INTERVAL`. Advances the next-check deadline
      # as a side effect so the throttle is monotonic regardless of emit cadence.
      #: () -> bool
      def due_for_recheck?
        now = monotonic_now
        return false if now < @next_check_at

        @next_check_at = now + RECHECK_INTERVAL
        true
      end

      #: () -> void
      def assert_lease!
        lease = @store.current_workflow_lease(@workflow_id)
        return if lease && lease.fetch("worker_id") == @node_id

        raise WorkflowRpc::StaleLease, "#{@node_id} no longer owns workflow #{@workflow_id}"
      end

      #: () -> Float
      def monotonic_now
        Process.clock_gettime(Process::CLOCK_MONOTONIC).to_f
      end
    end

    # Wraps the RPC writer for an object stream so emits fail fast when this
    # node has lost the unified object lease. `ObjectStreamHost` flips
    # `entry.lost` in its renewal task; on the next emit we raise
    # `StaleLease`, which the server turns into a terminal error frame.
    #
    # No store re-check happens here; the renewal task already drives that
    # in-process, so emits stay cheap. `cancelled?` ORs `entry.lost` with the
    # inner cancel signal so poll-and-emit producers (`sleep until cancelled?`)
    # also unblock on lease loss, not just on consumer disconnect.
    class ObjectStreamLeaseWriter
      #: (untyped, entry: ObjectStreamHost::Entry) -> void
      def initialize(inner, entry:)
        @inner = inner
        @entry = entry
      end

      #: (Object?) -> void
      def emit(value)
        raise WorkflowRpc::StaleLease, "object lease lost" if @entry.lost

        @inner.emit(value)
      end

      #: () -> bool
      def cancelled?
        @entry.lost || @inner.cancelled?
      end
    end
  end
end
