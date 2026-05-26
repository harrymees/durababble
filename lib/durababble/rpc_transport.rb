# typed: true
# frozen_string_literal: true

require "paquito"
require "concurrent-ruby"
require "grpc"
require_relative "worker_identity"
require_relative "rpc_proto"

module Durababble
  module Rpc
    SERIALIZER = Paquito::SingleBytePrefixVersion.new(1, 1 => Marshal)
    DEFAULT_TIMEOUT = 5.0

    class Error < Durababble::Error; end
    class Unavailable < Error; end
    class Unauthenticated < Error; end
    class RemoteError < Error; end

    class << self
      #: (Object?, ?surface: Symbol?, ?context: String?) -> String
      def dump(value, surface: nil, context: nil)
        serialized = SERIALIZER.dump(value)
        Durababble.enforce_payload_limit!(surface:, bytesize: serialized.bytesize, context:) if surface
        serialized
      end

      #: (String?) -> Object?
      def load(bytes) = bytes.nil? || bytes.empty? ? nil : SERIALIZER.load(bytes)
    end

    class NodeDirectory
      #: (?Hash[String, String]) -> void
      def initialize(entries = {})
        @entries = {}
        entries.each { |node_id, rpc_address| register(node_id:, rpc_address:) }
      end

      #: (node_id: String, rpc_address: String) -> String
      def register(node_id:, rpc_address:)
        @entries[node_id] = rpc_address
      end

      #: (String) -> String?
      def rpc_address_for(node_id)
        @entries[node_id] || WorkerIdentity.address_for(node_id)
      end
    end

    class Client
      #: String
      attr_reader :address

      class << self
        #: (Object) -> Object?
        def decode_transient_response(response)
          response = response #: as untyped
          case response.result
          when :ok
            Rpc.load(response.ok)
          when :err
            raise_remote_error(response.err)
          when :not_running
            raise WorkflowRpc::NoActiveLease, "transient target is not running"
          when :moved
            moved = response.moved
            raise WorkflowRpc::StaleLease, "lease moved to #{moved.new_node_id} at #{moved.new_rpc_address}"
          end
        end

        private

        #: (Object) -> bot
        def raise_remote_error(error)
          error = error #: as untyped
          typed = WorkflowRpc.remote_error_from_fields(error.klass, error.message)
          raise typed if typed

          raise Durababble::Rpc::RemoteError, "#{error.klass}: #{error.message}"
        end
      end

      #: (address: String, ?credentials: Symbol, ?timeout: Numeric, ?stub: Object?) -> void
      def initialize(address:, credentials: :this_channel_is_insecure, timeout: DEFAULT_TIMEOUT, stub: nil)
        @address = address
        @timeout = timeout
        @stub = stub || Proto::Stub.new(address, credentials)
      end

      #: (worker_pool: String, workflow_ids: Array[String]) -> bool
      def awaken_batch(worker_pool:, workflow_ids:)
        Observability.trace("durababble.rpc.client.awaken_batch", "durababble.worker.pool" => worker_pool) do
          with_rpc_errors do
            @stub.awaken_batch(
              Proto::AwakenBatchRequest.new(worker_pool:, workflow_ids:),
              deadline: deadline,
            )
          end
        end
        true
      end

      #: (worker_pool: String, target_kind: String, target_id: String, ?target_class: String, ?expected_worker_id: String) -> bool
      def evict_lease(worker_pool:, target_kind:, target_id:, target_class: "", expected_worker_id: "")
        Observability.trace("durababble.rpc.client.evict_lease", "durababble.worker.pool" => worker_pool, "durababble.rpc.target_kind" => target_kind, "durababble.rpc.target_class" => target_class) do
          with_rpc_errors do
            @stub.evict_lease(
              Proto::EvictLeaseRequest.new(worker_pool:, target_kind:, target_class:, target_id:, expected_worker_id:),
              deadline: deadline,
            )
          end
        end
        true
      end

      #: (worker_pool: String, target_kind: String, target_id: String, ?target_class: String, ?expected_worker_id: String) -> bool
      def deliver_message(worker_pool:, target_kind:, target_id:, target_class: "", expected_worker_id: "")
        Observability.trace("durababble.rpc.client.deliver_message", "durababble.worker.pool" => worker_pool, "durababble.rpc.target_kind" => target_kind, "durababble.rpc.target_class" => target_class) do
          with_rpc_errors do
            @stub.deliver_message(
              Proto::DeliverMessageRequest.new(worker_pool:, target_kind:, target_class:, target_id:, expected_worker_id:),
              deadline: deadline,
            )
          end
        end
        true
      end

      #: (worker_pool: Object?, method: Object?, args: Object?, ?class_name: Object?, ?object_id: Object?, ?workflow_id: Object?, ?deadline_ms: Object?, ?expected_worker_id: Object?) -> Object
      def call_transient_response(worker_pool:, method:, args:, class_name: "", object_id: "", workflow_id: "", deadline_ms: 0, expected_worker_id: "")
        Observability.trace("durababble.rpc.client.call_transient", "durababble.worker.pool" => worker_pool, "durababble.rpc.method" => method, "durababble.workflow.id" => workflow_id, "durababble.object.type" => class_name, "durababble.object.id" => object_id) do
          with_rpc_errors do
            @stub.call_transient(
              Proto::TransientRequest.new(
                worker_pool:,
                class_name:,
                object_id:,
                workflow_id:,
                method:,
                args: Rpc.dump(args, surface: :rpc_argument, context: "CallTransient #{method} args"),
                deadline_ms:,
                expected_worker_id:,
              ),
              deadline: deadline,
            )
          end
        end
      end

      #: (**Object?) -> Object?
      def call_transient(**kwargs)
        self.class.decode_transient_response(
          call_transient_response(
            worker_pool: kwargs.fetch(:worker_pool),
            method: kwargs.fetch(:method),
            args: kwargs.fetch(:args),
            class_name: kwargs.fetch(:class_name, ""),
            object_id: kwargs.fetch(:object_id, ""),
            workflow_id: kwargs.fetch(:workflow_id, ""),
            deadline_ms: kwargs.fetch(:deadline_ms, 0),
            expected_worker_id: kwargs.fetch(:expected_worker_id, ""),
          ),
        )
      rescue Unavailable => e
        raise WorkflowRpc::NodeUnavailable, e.message
      end

      private

      #: () -> Time
      def deadline
        Time.now + @timeout
      end

      #: () { () -> Object? } -> Object?
      def with_rpc_errors(&block)
        block.call
      rescue GRPC::Unauthenticated => e
        raise Unauthenticated, e.details
      rescue GRPC::Unavailable, GRPC::DeadlineExceeded => e
        raise Unavailable, e.details
      rescue GRPC::BadStatus => e
        raise Error, e.details
      end
    end

    class WorkflowClient
      #: (address: String, ?worker_pool: String, ?credentials: Symbol, ?timeout: Numeric) -> void
      def initialize(address:, worker_pool: "default", credentials: :this_channel_is_insecure, timeout: DEFAULT_TIMEOUT)
        @client = Client.new(address:, credentials:, timeout:)
        @worker_pool = worker_pool
      end

      #: (String, Hash[String, Object?]) -> Object?
      def request(command, payload)
        raise WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

        @client.call_transient(
          worker_pool: @worker_pool,
          workflow_id: payload.fetch("workflow_id"),
          method: payload.fetch("command"),
          args: payload.fetch("payload", {}),
          expected_worker_id: payload.fetch("expected_worker_id"),
        )
      end
    end

    class Server
      # Cap how long #stop blocks waiting for the serving task to drain. A hung
      # request must not wedge shutdown forever; we log and move on past this.
      STOP_DRAIN_TIMEOUT = 5.0

      #: String?
      attr_reader :node_id
      #: String
      attr_reader :host
      #: Integer?
      attr_reader :port

      #: (node_id: String?, store: Store, ?worker_pool: String, ?workflow_handlers: Hash[String, Object], ?transient_handler: (Proc | Method)?, ?node_directory: NodeDirectory, ?host: String, ?port: Integer, ?credentials: Symbol, ?pool_size: Integer, ?authorize: (Proc | Method)?, ?awaken_batch: (Proc | Method)?, ?evict_lease: (Proc | Method)?, ?deliver_message: (Proc | Method)?, ?verify_deliver_message_owner: bool, ?identity_id: String?, ?stop_drain_timeout: Float) -> void
      def initialize(
        node_id:,
        store:,
        worker_pool: "default",
        workflow_handlers: {},
        transient_handler: nil,
        node_directory: NodeDirectory.new,
        host: "127.0.0.1",
        port: 50_051,
        credentials: :this_port_is_insecure,
        pool_size: 4,
        authorize: nil,
        awaken_batch: nil,
        evict_lease: nil,
        deliver_message: nil,
        verify_deliver_message_owner: true,
        identity_id: nil,
        stop_drain_timeout: STOP_DRAIN_TIMEOUT
      )
        @node_id = node_id
        @store = store
        @worker_pool = worker_pool
        @workflow_handlers = workflow_handlers
        @transient_handler = transient_handler
        @node_directory = node_directory
        @host = host
        @requested_port = port
        @credentials = credentials
        @pool_size = pool_size
        @authorize = authorize
        @awaken_batch = awaken_batch
        @evict_lease = evict_lease
        @deliver_message = deliver_message
        @verify_deliver_message_owner = verify_deliver_message_owner
        @identity_id = identity_id
        @stop_drain_timeout = stop_drain_timeout
      end

      #: () -> Server
      def start
        return self if @server

        @server = GRPC::RpcServer.new(pool_size: @pool_size)
        @port = @server.add_http2_port("#{host}:#{@requested_port}", @credentials)
        @node_id ||= WorkerIdentity.generate(address:, id: @identity_id)
        current_node_id = @node_id
        @server.handle(Service.new(
          node_id: current_node_id,
          store: @store,
          worker_pool: @worker_pool,
          workflow_handlers: @workflow_handlers,
          transient_handler: @transient_handler,
          node_directory: @node_directory,
          authorize: @authorize,
          awaken_batch: @awaken_batch,
          evict_lease: @evict_lease,
          deliver_message: @deliver_message,
          verify_deliver_message_owner: @verify_deliver_message_owner,
        ))
        @server_task = Concurrent::Promises.future { @server.run }
        self
      end

      #: () -> void
      def stop
        @server&.stop
        task = @server_task
        if task
          task.wait(@stop_drain_timeout)
          if task.pending?
            Durababble.logger&.warn(
              "Durababble::Rpc::Server#stop timed out after #{@stop_drain_timeout}s " \
                "waiting for the serving task to drain; abandoning it",
            )
          end
        end
      ensure
        @server = nil
        @server_task = nil
      end

      #: () -> String
      def address
        "#{host}:#{port || @requested_port}"
      end
    end

    class Service < Proto::Service
      #: (node_id: String, store: Store, worker_pool: String, workflow_handlers: Hash[String, Object], transient_handler: (Proc | Method)?, node_directory: NodeDirectory, authorize: (Proc | Method)?, awaken_batch: (Proc | Method)?, evict_lease: (Proc | Method)?, deliver_message: (Proc | Method)?, ?verify_deliver_message_owner: bool) -> void
      def initialize(
        node_id:,
        store:,
        worker_pool:,
        workflow_handlers:,
        transient_handler:,
        node_directory:,
        authorize:,
        awaken_batch:,
        evict_lease:,
        deliver_message:,
        verify_deliver_message_owner: true
      )
        super()

        @node_id = node_id
        @store = store
        @worker_pool = worker_pool
        @workflow_handlers = workflow_handlers
        @transient_handler = transient_handler
        @node_directory = node_directory
        @authorize = authorize
        @awaken_batch = awaken_batch
        @evict_lease = evict_lease
        @deliver_message = deliver_message
        @verify_deliver_message_owner = verify_deliver_message_owner
      end

      #: (Object, Object) -> Proto::AwakenBatchResponse
      def awaken_batch(request, call)
        request = request #: as untyped
        Observability.trace("durababble.rpc.server.awaken_batch", "durababble.worker.pool" => request.worker_pool, "durababble.worker.id" => @node_id) do
          authorize!(call)
          @awaken_batch&.call(worker_pool: request.worker_pool, workflow_ids: request.workflow_ids.to_a)
          Proto::AwakenBatchResponse.new
        end
      end

      #: (Object, Object) -> Proto::EvictLeaseResponse
      def evict_lease(request, call)
        request = request #: as untyped
        Observability.trace("durababble.rpc.server.evict_lease", "durababble.worker.pool" => request.worker_pool, "durababble.worker.id" => @node_id, "durababble.rpc.target_kind" => request.target_kind, "durababble.rpc.target_class" => request.target_class) do
          authorize!(call)
          return Proto::EvictLeaseResponse.new if expected_worker_mismatch?(request)

          @evict_lease&.call(
            worker_pool: request.worker_pool,
            target_kind: request.target_kind,
            target_class: request.target_class,
            target_id: request.target_id,
          )
          Proto::EvictLeaseResponse.new
        end
      end

      #: (Object, Object) -> Proto::DeliverMessageResponse
      def deliver_message(request, call)
        request = request #: as untyped
        Observability.trace("durababble.rpc.server.deliver_message", "durababble.worker.pool" => request.worker_pool, "durababble.worker.id" => @node_id, "durababble.rpc.target_kind" => request.target_kind, "durababble.rpc.target_class" => request.target_class) do
          authorize!(call)
          unless expected_worker_mismatch?(request) || (@verify_deliver_message_owner && stale_workflow_message?(request))
            @deliver_message&.call(
              worker_pool: request.worker_pool,
              target_kind: request.target_kind,
              target_class: request.target_class,
              target_id: request.target_id,
            )
          end
          Proto::DeliverMessageResponse.new
        end
      end

      #: (Object, Object) -> Proto::TransientResponse
      def call_transient(request, call)
        request = request #: as untyped
        Observability.trace("durababble.rpc.server.call_transient", "durababble.worker.pool" => request.worker_pool, "durababble.worker.id" => @node_id, "durababble.rpc.method" => request["method"], "durababble.workflow.id" => request.workflow_id, "durababble.object.type" => request.class_name, "durababble.object.id" => request.object_id) do
          authorize!(call)
          result = if request.workflow_id.empty?
            call_custom_transient(request)
          else
            call_workflow_transient(request)
          end
          Proto::TransientResponse.new(ok: Rpc.dump(result))
        end
      rescue WorkflowRpc::NoActiveLease
        Proto::TransientResponse.new(not_running: true)
      rescue WorkflowRpc::StaleLease => e
        moved_response(request) || remote_error_response(e)
      rescue GRPC::BadStatus
        raise
      rescue StandardError => e
        remote_error_response(e)
      end

      private

      #: (Object) -> void
      def authorize!(call)
        return unless @authorize
        return if @authorize.call(call)

        raise GRPC::Unauthenticated, "durababble RPC peer is not authorized"
      end

      #: (Object) -> Object?
      def call_workflow_transient(request)
        request = request #: as untyped
        expected_worker_id = request.expected_worker_id.to_s.empty? ? @node_id : request.expected_worker_id
        payload = {
          "workflow_id" => request.workflow_id,
          "expected_worker_id" => expected_worker_id,
          "command" => request["method"],
          "payload" => load_request_args(request, context: "CallTransient #{request["method"]} args") || {},
        }
        with_store do |store|
          WorkflowRpc::Handler.new(
            store:,
            node_id: @node_id,
            handlers: @workflow_handlers,
          ).call(payload)
        end
      end

      #: () { (untyped) -> untyped } -> untyped
      def with_store(&block)
        if @store.respond_to?(:with_dedicated_connection)
          @store.with_dedicated_connection(&block)
        else
          block.call(@store)
        end
      end

      #: (Object) -> Object?
      def call_custom_transient(request)
        request = request #: as untyped
        unless @transient_handler
          raise WorkflowRpc::UnknownCommand, "unknown transient RPC method #{request["method"]}"
        end

        @transient_handler.call(request:, args: load_request_args(request, context: "CallTransient #{request["method"]} args"))
      end

      #: (Object, context: String) -> Object?
      def load_request_args(request, context:)
        request = request #: as untyped
        bytes = request.args.to_s
        Durababble.enforce_payload_limit!(surface: :rpc_argument, bytesize: bytes.bytesize, context:)
        Rpc.load(request.args)
      end

      #: (Object) -> bool
      def stale_workflow_message?(request)
        request = request #: as untyped
        return false unless request.target_kind == "workflow"

        lease = @store.current_workflow_lease(request.target_id, worker_pool: request.worker_pool)
        !lease || lease.fetch("worker_id") != @node_id
      end

      #: (Object) -> bool
      def expected_worker_mismatch?(request)
        request = request #: as untyped
        expected_worker_id = request.expected_worker_id.to_s
        !expected_worker_id.empty? && expected_worker_id != @node_id
      end

      #: (Object) -> Proto::TransientResponse?
      def moved_response(request)
        request = request #: as untyped
        lease = @store.current_workflow_lease(request.workflow_id, worker_pool: request.worker_pool)
        return unless lease

        new_node_id = lease.fetch("worker_id").to_s
        return if new_node_id == @node_id

        Proto::TransientResponse.new(
          moved: Proto::LeaseMoved.new(
            new_node_id:,
            new_rpc_address: @node_directory.rpc_address_for(new_node_id).to_s,
          ),
        )
      end

      #: (StandardError) -> Proto::TransientResponse
      def remote_error_response(error)
        Proto::TransientResponse.new(
          err: Proto::RemoteError.new(
            klass: error.class.name,
            message: error.message,
            backtrace: error.backtrace || [],
          ),
        )
      end
    end
  end
end
