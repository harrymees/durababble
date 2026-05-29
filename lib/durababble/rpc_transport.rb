# typed: true
# frozen_string_literal: true

require "paquito"
require "async"
require "async/grpc"
require "async/http"
require "async/http/endpoint"
require "protocol/http/middleware"
require "protocol/grpc"
require_relative "rpc_messages"

module Durababble
  module Rpc
    SERIALIZER = Paquito::SingleBytePrefixVersion.new(1, 1 => Marshal)
    DEFAULT_TIMEOUT = 5.0

    SERVICE_NAME = "durababble.v1.Rpc"

    class Error < Durababble::Error; end
    class Unavailable < Error; end
    class Unauthenticated < Error; end
    class RemoteError < Error; end

    GRPC_CLIENT_CACHE_KEY = :__durababble_rpc_grpc_clients

    class Interface < Protocol::GRPC::Interface
      rpc :AwakenBatch, Messages::AwakenBatchRequest, Messages::AwakenBatchResponse
      rpc :EvictLease, Messages::EvictLeaseRequest, Messages::EvictLeaseResponse
      rpc :DeliverMessage, Messages::DeliverMessageRequest, Messages::DeliverMessageResponse
      rpc :CallTransient, Messages::TransientRequest, Messages::TransientResponse
    end

    class << self
      # `surface:`/`context:` opt this dump into `Durababble.enforce_payload_limit!`
      # (see #71). Only the request-argument leg passes a surface; response and
      # message-envelope encodings leave it nil and stay unenforced.
      #: (Object?, ?surface: Symbol?, ?context: String?) -> String
      def dump(value, surface: nil, context: nil)
        serialized = SERIALIZER.dump(value)
        Durababble.enforce_payload_limit!(surface:, bytesize: serialized.bytesize, context:) if surface
        serialized
      end

      #: (String?) -> Object?
      def load(bytes) = bytes.nil? || bytes.empty? ? nil : SERIALIZER.load(bytes)

      # Thread-local cache of `Async::GRPC::Client` keyed by remote address so
      # repeated RPC calls to the same peer reuse the same HTTP/2 connection
      # pool. The cache is per-thread because each client is tied to whichever
      # fiber scheduler created it; sharing one across threads would cross
      # schedulers.
      #
      # There is no per-entry eviction: the underlying HTTP client reconnects a
      # downed peer on the next request. The cache retires entries on
      # `shutdown_grpc_clients!` (process/thread shutdown) or process exit.
      #: (String) -> Async::GRPC::Client
      def grpc_client_for(address)
        cache = grpc_client_cache
        cache[address] ||= begin
          endpoint = Async::HTTP::Endpoint.parse(address.include?("://") ? address : "http://#{address}", protocol: Async::HTTP::Protocol::HTTP2)
          http_client = Async::HTTP::Client.new(endpoint, protocol: Async::HTTP::Protocol::HTTP2)
          Async::GRPC::Client.new(http_client)
        end
      end

      #: (String) -> bool
      def grpc_client_cached?(address)
        cache = Thread.current.thread_variable_get(GRPC_CLIENT_CACHE_KEY)
        return false unless cache.is_a?(Hash)

        cache.key?(address)
      end

      #: () -> void
      def shutdown_grpc_clients!
        cache = Thread.current.thread_variable_get(GRPC_CLIENT_CACHE_KEY)
        return unless cache.is_a?(Hash)

        cache.each_value do |client|
          client.close
        rescue StandardError
          # Best-effort: a client whose underlying scheduler is already gone
          # will raise here; the cache entry is being dropped anyway.
        end
        cache.clear
      end

      #: (Object) -> bool
      def deadline_exceeded?(call)
        call = call #: as untyped
        call.respond_to?(:deadline_exceeded?) && call.deadline_exceeded?
      end

      private

      #: () -> Hash[String, untyped]
      def grpc_client_cache
        cache = Thread.current.thread_variable_get(GRPC_CLIENT_CACHE_KEY)
        return cache if cache.is_a?(Hash)

        cache = {}
        Thread.current.thread_variable_set(GRPC_CLIENT_CACHE_KEY, cache)
        cache
      end
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
        @entries[node_id]
      end
    end

    class Client
      #: String
      attr_reader :address

      class << self
        # Accepts any value that ducks `Messages::TransientResponse` (the test
        # suite passes a Struct with the same `result`/`ok`/`err`/`moved`
        # surface), so the parameter is a duck-typed `Object` rather than the
        # concrete class. The `result` accessor returns one of `:ok`/`:err`/`:not_running`/
        # `:moved` matching the populated field, and `nil` for an empty response.
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

        # Same duck-typed contract as `decode_transient_response`; accepts the
        # concrete `Messages::RemoteError` or the test's lookalike Struct.
        #: (Object) -> bot
        def raise_remote_error(error)
          error = error #: as untyped
          typed = WorkflowRpc.remote_error_from_fields(error.klass, error.message)
          raise typed if typed

          raise Durababble::Rpc::RemoteError, "#{error.klass}: #{error.message}"
        end
      end

      # `grpc_client:` is an injection seam for tests; production callers leave
      # it nil and a cached `Async::GRPC::Client` is built inside the request
      # `Sync` block.
      #: (address: String, ?timeout: Numeric, ?grpc_client: Object?) -> void
      def initialize(address:, timeout: DEFAULT_TIMEOUT, grpc_client: nil)
        @address = address
        @timeout = timeout
        @injected_client = grpc_client
      end

      #: (worker_pool: String, workflow_ids: Array[String]) -> bool
      def awaken_batch(worker_pool:, workflow_ids:)
        Observability.trace("durababble.rpc.client.awaken_batch", "durababble.worker.pool" => worker_pool) do
          unary(:awaken_batch, Messages::AwakenBatchRequest.new(worker_pool:, workflow_ids:))
        end
        true
      end

      #: (worker_pool: String, target_kind: String, target_id: String, ?target_class: String, ?expected_worker_id: String) -> bool
      def evict_lease(worker_pool:, target_kind:, target_id:, target_class: "", expected_worker_id: "")
        Observability.trace("durababble.rpc.client.evict_lease", "durababble.worker.pool" => worker_pool, "durababble.rpc.target_kind" => target_kind, "durababble.rpc.target_class" => target_class) do
          unary(:evict_lease, Messages::EvictLeaseRequest.new(worker_pool:, target_kind:, target_class:, target_id:, expected_worker_id:))
        end
        true
      end

      #: (worker_pool: String, target_kind: String, target_id: String, ?target_class: String, ?expected_worker_id: String) -> bool
      def deliver_message(worker_pool:, target_kind:, target_id:, target_class: "", expected_worker_id: "")
        Observability.trace("durababble.rpc.client.deliver_message", "durababble.worker.pool" => worker_pool, "durababble.rpc.target_kind" => target_kind, "durababble.rpc.target_class" => target_class) do
          unary(:deliver_message, Messages::DeliverMessageRequest.new(worker_pool:, target_kind:, target_class:, target_id:, expected_worker_id:))
        end
        true
      end

      #: (worker_pool: String, method: String, args: Object?, ?class_name: String, ?durable_object_id: String, ?workflow_id: String, ?deadline_ms: Integer, ?expected_worker_id: String) -> Object?
      def call_transient_response(worker_pool:, method:, args:, class_name: "", durable_object_id: "", workflow_id: "", deadline_ms: 0, expected_worker_id: "")
        Observability.trace("durababble.rpc.client.call_transient", "durababble.worker.pool" => worker_pool, "durababble.rpc.method" => method, "durababble.workflow.id" => workflow_id, "durababble.object.type" => class_name, "durababble.object.id" => durable_object_id) do
          unary(
            :call_transient,
            Messages::TransientRequest.new(
              worker_pool:,
              class_name:,
              durable_object_id:,
              workflow_id:,
              method:,
              args: Rpc.dump(args, surface: :rpc_argument, context: "CallTransient #{method} args"),
              deadline_ms:,
              expected_worker_id:,
            ),
          )
        end
      end

      #: (worker_pool: String, method: String, args: Object?, ?class_name: String, ?durable_object_id: String, ?workflow_id: String, ?deadline_ms: Integer, ?expected_worker_id: String) -> Object?
      def call_transient(worker_pool:, method:, args:, class_name: "", durable_object_id: "", workflow_id: "", deadline_ms: 0, expected_worker_id: "")
        self.class.decode_transient_response(
          call_transient_response(worker_pool:, method:, args:, class_name:, durable_object_id:, workflow_id:, deadline_ms:, expected_worker_id:),
        )
      rescue Unavailable => e
        raise WorkflowRpc::NodeUnavailable, e.message
      end

      private

      # Performs one unary gRPC request. Runs inside `Sync` so it is callable
      # both from a plain thread (worker control-plane) and from the ambient
      # reactor (engine/server handler). Injected clients bypass the cache and
      # are owned by the caller.
      #: (Symbol, Object) -> Object?
      def unary(method, request_value)
        with_rpc_errors do
          Sync do |task|
            task.with_timeout(@timeout) do
              client = @injected_client || Rpc.grpc_client_for(@address) #: as untyped
              stub = client.stub(Interface, SERVICE_NAME)
              stub.public_send(method, request_value)
            end
          end
        end
      end

      #: () { () -> Object? } -> Object?
      def with_rpc_errors(&block)
        block.call
      rescue Unauthenticated, Unavailable, Error
        raise
      rescue Async::TimeoutError, Errno::ECONNREFUSED, Errno::ECONNRESET, Errno::EPIPE, Errno::ETIMEDOUT, Errno::EHOSTUNREACH, SocketError, IOError => e
        raise Unavailable, e.message
      rescue Protocol::GRPC::Unauthenticated => e
        raise Unauthenticated, grpc_error_message(e, "durababble RPC peer is not authorized")
      rescue Protocol::GRPC::Unavailable, Protocol::GRPC::DeadlineExceeded, Protocol::GRPC::Cancelled => e
        raise Unavailable, grpc_error_message(e, "durababble RPC node is unavailable")
      rescue Protocol::GRPC::Internal => e
        raise Error, grpc_error_message(e, "durababble RPC handler raised on the peer")
      rescue Protocol::GRPC::Error => e
        raise Error, grpc_error_message(e, "durababble RPC failed with gRPC status #{e.status_code}")
      rescue StandardError => e
        raise Unavailable, e.message if e.is_a?(Protocol::HTTP2::Error)

        raise
      end

      #: (Exception, String) -> String
      def grpc_error_message(error, default)
        cause = error.cause
        message = cause&.message.to_s
        message = error.message.to_s if message.empty? || message == cause.class.name.to_s
        message.empty? ? default : message
      end
    end

    class WorkflowClient
      #: (address: String, ?worker_pool: String, ?timeout: Numeric) -> void
      def initialize(address:, worker_pool: "default", timeout: DEFAULT_TIMEOUT)
        @client = Client.new(address:, timeout:)
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
      #: String?
      attr_reader :node_id
      #: String
      attr_reader :host
      #: Integer?
      attr_reader :port

      # `identity_id:` seeds the generated `node_id` (`<id>@<address>`) so a worker
      # keeps a stable identity across address reuse (see #68/#69).
      #: (node_id: String?, store: Store, ?worker_pool: String, ?workflow_handlers: Hash[String, Object], ?transient_handler: (Proc | Method)?, ?node_directory: NodeDirectory, ?host: String, ?port: Integer, ?authorize: (Proc | Method)?, ?awaken_batch: (Proc | Method)?, ?evict_lease: (Proc | Method)?, ?deliver_message: (Proc | Method)?, ?verify_deliver_message_owner: bool, ?identity_id: String?) -> void
      def initialize(
        node_id:,
        store:,
        worker_pool: "default",
        workflow_handlers: {},
        transient_handler: nil,
        node_directory: NodeDirectory.new,
        host: "127.0.0.1",
        port: 50_051,
        authorize: nil,
        awaken_batch: nil,
        evict_lease: nil,
        deliver_message: nil,
        verify_deliver_message_owner: true,
        identity_id: nil
      )
        @node_id = node_id
        @store = store
        @worker_pool = worker_pool
        @workflow_handlers = workflow_handlers
        @transient_handler = transient_handler
        @node_directory = node_directory
        @host = host
        @requested_port = port
        @authorize = authorize
        @awaken_batch = awaken_batch
        @evict_lease = evict_lease
        @deliver_message = deliver_message
        @verify_deliver_message_owner = verify_deliver_message_owner
        @identity_id = identity_id
        @reactor = nil
      end

      class ReactorCallback
        #: () { () -> Object? } -> void
        def initialize(&block)
          @block = block #: Proc?
        end

        #: () -> bool
        def alive?
          !@block.nil?
        end

        #: () -> Object?
        def transfer
          block = @block
          @block = nil
          block&.call #: as Object?
        end
      end
      private_constant :ReactorCallback

      #: () -> Server
      def start
        start_async(parent: current_async_task!("Durababble::Rpc::Server#start"))
      end

      #: (?parent: Object) -> Server
      def start_async(parent: nil)
        parent ||= current_async_task!("Durababble::Rpc::Server#start_async")
        return self if @task

        async_parent = parent #: as untyped
        server = build_http_server
        @task = async_parent.async(transient: true, finished: false) { server.run.wait }
        @reactor = @task.reactor
        self
      end

      #: () -> void
      def stop
        task = @task
        reactor = @reactor || task&.reactor
        bound = @bound #: as untyped

        @task = nil
        @reactor = nil
        @bound = nil
        @port = nil

        stop_task(task, reactor, bound)
      end

      #: () -> String
      def address
        "#{host}:#{@port || @requested_port}"
      end

      private

      #: (String) -> Object
      def current_async_task!(operation)
        Async::Task.current || raise(RuntimeError)
      rescue RuntimeError
        raise ConfigurationError, "#{operation} must be called from inside a running Async reactor; pass parent: from your application's Async supervisor"
      end

      #: (Object?, Object?, Object?) -> void
      def stop_task(task, reactor, bound)
        bound = bound #: as untyped
        unless task
          bound&.close
          return
        end

        task = task #: as untyped
        reactor = reactor #: as untyped
        if reactor && Fiber.scheduler.equal?(reactor)
          task.stop
          bound&.close
        elsif reactor&.respond_to?(:unblock)
          reactor.unblock(nil, ReactorCallback.new do
            task.stop
          ensure
            bound&.close
          end)
        else
          task.stop
          bound&.close
        end
      end

      #: () -> Async::HTTP::Server
      def build_http_server
        http_endpoint = Async::HTTP::Endpoint.parse("http://#{@host}:#{@requested_port}", protocol: Async::HTTP::Protocol::HTTP2)
        @bound = http_endpoint.bound
        @port = @bound.sockets.first.local_address.ip_port
        @node_id ||= WorkerIdentity.generate(address:, id: @identity_id)
        dispatcher = Async::GRPC::Dispatcher.new(Protocol::HTTP::Middleware::NotFound)
        dispatcher.register(build_grpc_service)
        Async::HTTP::Server.new(dispatcher, @bound, protocol: http_endpoint.protocol, scheme: http_endpoint.scheme)
      end

      #: () -> RpcService
      def build_grpc_service
        RpcService.new(build_service)
      end

      #: () -> Service
      def build_service
        node_id = @node_id #: as String
        Service.new(
          node_id:,
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
        )
      end
    end

    class RpcService < Async::GRPC::Service
      #: (Service) -> void
      def initialize(service)
        super(Interface, SERVICE_NAME)
        @service = service
      end

      #: (Object, Object, Object) -> void
      def awaken_batch(input, output, call)
        dispatch(:awaken_batch, input, output, call)
      end

      #: (Object, Object, Object) -> void
      def evict_lease(input, output, call)
        dispatch(:evict_lease, input, output, call)
      end

      #: (Object, Object, Object) -> void
      def deliver_message(input, output, call)
        dispatch(:deliver_message, input, output, call)
      end

      #: (Object, Object, Object) -> void
      def call_transient(input, output, call)
        dispatch(:call_transient, input, output, call)
      end

      private

      #: (Symbol, Object, Object, Object) -> void
      def dispatch(method, input, output, call)
        rpc_input = input #: as untyped
        rpc_output = output #: as untyped
        rpc_output.write(@service.public_send(method, rpc_input.read, call))
      rescue Unauthenticated => e
        raise Protocol::GRPC::Unauthenticated, e.message
      rescue Async::TimeoutError => e
        raise_deadline_timeout_or_internal!(e, call)
      rescue StandardError => e
        raise Protocol::GRPC::Internal, e.message
      end

      #: (Async::TimeoutError, Object) -> bot
      def raise_deadline_timeout_or_internal!(error, call)
        raise error if Rpc.deadline_exceeded?(call)

        raise Protocol::GRPC::Internal, error.message
      end
    end

    class Service
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

      #: (Messages::AwakenBatchRequest, Object) -> Messages::AwakenBatchResponse
      def awaken_batch(request, call)
        Observability.trace("durababble.rpc.server.awaken_batch", "durababble.worker.pool" => request.worker_pool, "durababble.worker.id" => @node_id) do
          authorize!(call)
          @awaken_batch&.call(worker_pool: request.worker_pool, workflow_ids: request.workflow_ids.to_a)
          Messages::AwakenBatchResponse.new
        end
      end

      #: (Messages::EvictLeaseRequest, Object) -> Messages::EvictLeaseResponse
      def evict_lease(request, call)
        Observability.trace("durababble.rpc.server.evict_lease", "durababble.worker.pool" => request.worker_pool, "durababble.worker.id" => @node_id, "durababble.rpc.target_kind" => request.target_kind, "durababble.rpc.target_class" => request.target_class) do
          authorize!(call)
          return Messages::EvictLeaseResponse.new if expected_worker_mismatch?(request)

          @evict_lease&.call(
            worker_pool: request.worker_pool,
            target_kind: request.target_kind,
            target_class: request.target_class,
            target_id: request.target_id,
          )
          Messages::EvictLeaseResponse.new
        end
      end

      #: (Messages::DeliverMessageRequest, Object) -> Messages::DeliverMessageResponse
      def deliver_message(request, call)
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
          Messages::DeliverMessageResponse.new
        end
      end

      #: (Messages::TransientRequest, Object) -> Messages::TransientResponse
      def call_transient(request, call)
        Observability.trace("durababble.rpc.server.call_transient", "durababble.worker.pool" => request.worker_pool, "durababble.worker.id" => @node_id, "durababble.rpc.method" => request["method"], "durababble.workflow.id" => request.workflow_id, "durababble.object.type" => request.class_name, "durababble.object.id" => request.durable_object_id) do
          authorize!(call)
          result = if request.workflow_id.empty?
            call_custom_transient(request)
          else
            call_workflow_transient(request)
          end
          Messages::TransientResponse.new(ok: Rpc.dump(result))
        end
      rescue WorkflowRpc::NoActiveLease
        Messages::TransientResponse.new(not_running: true)
      rescue WorkflowRpc::StaleLease => e
        moved_response(request) || remote_error_response(e)
      rescue Unauthenticated
        # Let the wire-level unauthenticated status stand. Without this re-raise the catch-all
        # `rescue StandardError` below would convert authorization failures
        # into a 200 with an `err` frame, defeating the status-code contract
        # that `RpcService` translates `Unauthenticated` into gRPC unauthenticated.
        raise
      rescue Async::TimeoutError => e
        raise if Rpc.deadline_exceeded?(call)

        observe_transient_error(request, e)
        remote_error_response(e)
      rescue StandardError => e
        observe_transient_error(request, e)
        remote_error_response(e)
      end

      private

      # The remote caller learns about the failure through remote_error_response,
      # but the serving node would otherwise have no local trace of an unexpected
      # error it absorbed. Count it and log it so node-side operators can see it.
      #: (Messages::TransientRequest, StandardError) -> void
      def observe_transient_error(request, error)
        request = request #: as untyped
        method = request["method"]
        Observability.count(
          "durababble.rpc.server.errors",
          "durababble.worker.id" => @node_id,
          "durababble.rpc.method" => method,
          "durababble.workflow.id" => request.workflow_id,
          "error.type" => error.class.name,
        )
        Durababble.logger&.warn(
          "Durababble RPC server #{@node_id} returning remote error for #{method}: " \
            "#{error.class}: #{error.message}",
        )
      end

      #: (Object) -> void
      def authorize!(call)
        return unless @authorize
        return if @authorize.call(call)

        raise Unauthenticated, "durababble RPC peer is not authorized"
      end

      #: (Messages::TransientRequest) -> Object?
      def call_workflow_transient(request)
        expected_worker_id = request.expected_worker_id.to_s.empty? ? @node_id : request.expected_worker_id
        payload = {
          "workflow_id" => request.workflow_id,
          "expected_worker_id" => expected_worker_id,
          "command" => request["method"],
          "payload" => load_request_args(request, context: "CallTransient #{request["method"]} args") || {},
        }
        WorkflowRpc.with_store(@store) do |store|
          WorkflowRpc::Handler.new(
            store:,
            node_id: @node_id,
            handlers: @workflow_handlers,
          ).call(payload)
        end
      end

      #: (Messages::TransientRequest) -> Object?
      def call_custom_transient(request)
        unless @transient_handler
          raise WorkflowRpc::UnknownCommand, "unknown transient RPC method #{request["method"]}"
        end

        @transient_handler.call(request:, args: load_request_args(request, context: "CallTransient #{request["method"]} args"))
      end

      # Inbound counterpart to the client's `Rpc.dump(..., surface: :rpc_argument)`:
      # re-checks the byte limit on the receiving node before deserializing, so a
      # peer that skipped the client-side guard still can't push an oversized
      # payload through. A raised `PayloadTooLarge` is turned into an `err` frame
      # by `call_transient`'s rescue (see #71).
      #: (Messages::TransientRequest, context: String) -> Object?
      def load_request_args(request, context:)
        bytes = request.args.to_s
        Durababble.enforce_payload_limit!(surface: :rpc_argument, bytesize: bytes.bytesize, context:)
        Rpc.load(request.args)
      end

      #: (Messages::DeliverMessageRequest) -> bool
      def stale_workflow_message?(request)
        return false unless request.target_kind == "workflow"

        lease = @store.current_workflow_lease(request.target_id, worker_pool: request.worker_pool)
        !lease || lease.fetch("worker_id") != @node_id
      end

      # Fences recycled worker addresses (see #68/#69): when the caller named the
      # worker it expected to reach and this node is not that worker, the message
      # was routed to a reused address and must be ignored. Both
      # `Messages::EvictLeaseRequest` and `Messages::DeliverMessageRequest` are
      # `Messages::TargetRequest` subclasses and share the `expected_worker_id`
      # field, so the helper takes the common base.
      #: (Messages::TargetRequest) -> bool
      def expected_worker_mismatch?(request)
        expected_worker_id = request.expected_worker_id.to_s
        !expected_worker_id.empty? && expected_worker_id != @node_id
      end

      #: (Messages::TransientRequest) -> Messages::TransientResponse?
      def moved_response(request)
        lease = @store.current_workflow_lease(request.workflow_id, worker_pool: request.worker_pool)
        return unless lease

        new_node_id = lease.fetch("worker_id") #: as String
        return if new_node_id == @node_id

        Messages::TransientResponse.new(
          moved: Messages::LeaseMoved.new(
            new_node_id:,
            new_rpc_address: @node_directory.rpc_address_for(new_node_id).to_s,
          ),
        )
      end

      #: (StandardError) -> Messages::TransientResponse
      def remote_error_response(error)
        Messages::TransientResponse.new(
          err: Messages::RemoteError.new(
            klass: error.class.name.to_s,
            message: error.message,
            backtrace: error.backtrace || [],
          ),
        )
      end
    end
  end
end
