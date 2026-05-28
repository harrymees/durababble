# typed: true
# frozen_string_literal: true

require "paquito"
require "async"
require "async/http"
require "async/http/endpoint"
require "protocol/http/response"
require "protocol/http/body/writable"
require_relative "worker_identity"
require_relative "rpc_messages"

module Durababble
  module Rpc
    SERIALIZER = Paquito::SingleBytePrefixVersion.new(1, 1 => Marshal)
    DEFAULT_TIMEOUT = 5.0

    # Bounded buffer between a stream producer and the wire (server side) and
    # between the wire and the consumer (client side). Provides back-pressure on
    # top of HTTP/2 flow control: a producer that outpaces the reader parks.
    STREAM_BUFFER = 16
    # How long a streaming consumer parks in a single `read` before waking to
    # re-check whether it has been cancelled. Keeps cancellation responsive for
    # idle/indefinite streams without busy-looping.
    STREAM_POLL_TIMEOUT = 0.25

    # One HTTP path per RPC method. The wire payload on both legs is
    # `Rpc.dump`/`Rpc.load` (Paquito/Marshal), Ruby-to-Ruby — durababble never
    # used protobuf for cross-language interop, so HTTP/2 + Paquito carries the
    # same opaque bytes the gRPC transport did. `call_transient_stream` differs
    # only in its response: a stream of length-prefixed `StreamFrame`s rather
    # than a single response body.
    PATHS = {
      awaken_batch: "/durababble/v1/awaken_batch",
      evict_lease: "/durababble/v1/evict_lease",
      deliver_message: "/durababble/v1/deliver_message",
      call_transient: "/durababble/v1/call_transient",
      call_transient_stream: "/durababble/v1/call_transient_stream",
    }.freeze #: Hash[Symbol, String]

    ROUTES = PATHS.invert.freeze #: Hash[String, Symbol]

    OCTET_HEADERS = { "content-type" => "application/octet-stream" }.freeze #: Hash[String, String]

    class Error < Durababble::Error; end
    class Unavailable < Error; end
    class Unauthenticated < Error; end
    class RemoteError < Error; end

    # The cache lives in a *thread* variable (not a fiber-local `Thread.current[]`)
    # because `unary` runs inside `Sync { … }`, which creates a fresh fiber per
    # call. `Thread.current[]` is fiber-local in Ruby; `thread_variable_get/set`
    # is genuinely thread-scoped and shared across all fibers on the thread.
    HTTP_CLIENT_CACHE_KEY = :__durababble_rpc_http_clients

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

      #: () -> Hash[String, String]
      def octet_headers
        OCTET_HEADERS
      end

      # Thread-local cache of `Async::HTTP::Client` keyed by remote address so
      # repeated RPC calls to the same peer reuse the same HTTP/2 connection
      # (multiplexed across streams) instead of paying a fresh TCP + h2c
      # handshake per call. `Async::HTTP::Client` is itself a connection pool —
      # the cache just keeps the pool object alive across the short-lived
      # `Sync { … }` blocks that wrap each `unary` call so the underlying
      # connection (when present) can be reused. The cache is per-thread (not
      # global) because each `Async::HTTP::Client` is tied to whichever fiber
      # scheduler created it; sharing one across threads would cross schedulers.
      # `shutdown_http_clients!` lets the calling thread close everything
      # deterministically on shutdown.
      #
      # There is no per-entry eviction: we rely on `Async::HTTP::Client`'s own
      # internal pool to reconnect a downed peer's underlying TCP/H2 connection
      # on the next request. A `Sync` call site that catches `Rpc::Unavailable`
      # gets the next `client.post` attempt over a fresh connection without us
      # touching the cache. The cache only retires entries on `shutdown_http_clients!`
      # (process/thread shutdown) or via process exit.
      #: (String) -> Async::HTTP::Client
      def http_client_for(address)
        cache = http_client_cache
        cache[address] ||= begin
          endpoint = Async::HTTP::Endpoint.parse(address.include?("://") ? address : "http://#{address}")
          Async::HTTP::Client.new(endpoint, protocol: Async::HTTP::Protocol::HTTP2)
        end
      end

      #: (String) -> bool
      def http_client_cached?(address)
        cache = Thread.current.thread_variable_get(HTTP_CLIENT_CACHE_KEY)
        return false unless cache.is_a?(Hash)

        cache.key?(address)
      end

      #: () -> void
      def shutdown_http_clients!
        cache = Thread.current.thread_variable_get(HTTP_CLIENT_CACHE_KEY)
        return unless cache.is_a?(Hash)

        cache.each_value do |client|
          client.close
        rescue StandardError
          # Best-effort: a client whose underlying scheduler is already gone
          # will raise here; the cache entry is being dropped anyway.
        end
        cache.clear
      end

      # Maps a remote error's class name + message back to a typed local error.
      # Shared by the unary `decode_transient_response` path and the streaming
      # consumer's terminal error frame. Returns the error object (the caller
      # decides whether to raise now or carry it).
      #: (String, String) -> Exception
      def build_remote_error(klass, message)
        WorkflowRpc.remote_error_from_fields(klass, message) ||
          Durababble::Rpc::RemoteError.new("#{klass}: #{message}")
      end

      private

      #: () -> Hash[String, untyped]
      def http_client_cache
        cache = Thread.current.thread_variable_get(HTTP_CLIENT_CACHE_KEY)
        return cache if cache.is_a?(Hash)

        cache = {}
        Thread.current.thread_variable_set(HTTP_CLIENT_CACHE_KEY, cache)
        cache
      end
    end

    # Server-side producer handle passed to a `call_transient_stream` handler.
    # Wraps the `Protocol::HTTP::Body::Writable` the response streams: `emit`
    # encodes one value `StreamFrame` and writes it as a length-prefixed frame;
    # `cancelled?` reports whether the consumer has reset the stream (the body's
    # queue is closed). A handler is expected to check `cancelled?` between
    # quacks; a write after cancellation raises, which unwinds the handler.
    class StreamWriter
      #: (Protocol::HTTP::Body::Writable) -> void
      def initialize(body)
        @body = body
      end

      #: (Object?) -> void
      def emit(value)
        @body.write(FrameCodec.frame(Rpc.dump(Messages::StreamFrame.new(kind: :value, value:))))
      end

      #: () -> bool
      def cancelled?
        @body.closed?
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
        @entries[node_id] || WorkerIdentity.address_for(node_id)
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
          raise Rpc.build_remote_error(error.klass.to_s, error.message.to_s)
        end
      end

      # `http_client:` is an injection seam for tests (it must respond to
      # `#post(path, headers, body)` and `#close`); production callers leave it
      # nil and a per-call `Async::HTTP::Client` is built and closed inside the
      # request `Sync` block. `credentials:` is accepted for call-site
      # compatibility but unused now that the transport is cleartext H2C.
      #: (address: String, ?credentials: Object?, ?timeout: Numeric, ?http_client: Object?) -> void
      def initialize(address:, credentials: nil, timeout: DEFAULT_TIMEOUT, http_client: nil)
        @address = address
        @timeout = timeout
        @injected_client = http_client
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

      #: (**Object?) -> Object?
      def call_transient(**kwargs)
        worker_pool = kwargs.fetch(:worker_pool) #: as String
        method = kwargs.fetch(:method) #: as String
        args = kwargs.fetch(:args)
        class_name = kwargs.fetch(:class_name, "") #: as String
        durable_object_id = kwargs.fetch(:durable_object_id, "") #: as String
        workflow_id = kwargs.fetch(:workflow_id, "") #: as String
        deadline_ms = kwargs.fetch(:deadline_ms, 0) #: as Integer
        expected_worker_id = kwargs.fetch(:expected_worker_id, "") #: as String
        self.class.decode_transient_response(
          call_transient_response(worker_pool:, method:, args:, class_name:, durable_object_id:, workflow_id:, deadline_ms:, expected_worker_id:),
        )
      rescue Unavailable => e
        raise WorkflowRpc::NodeUnavailable, e.message
      end

      # Opens a streaming-result RPC and returns a `ResultStream`. The HTTP
      # request is not sent until the stream is iterated (`each`/`read`): the
      # `ResultStream` runs the block below as a child task on the consumer's
      # reactor, where `Sync` reuses that task, POSTs the request, and pumps
      # decoded frames into the stream. Closing the `ResultStream` (or its reactor
      # scope ending) stops the producer task: `cancelled?` flips, the poll loop
      # breaks, and the response is closed (resetting the HTTP/2 stream so the
      # server-side producer observes the cancellation and unwinds).
      #
      # Fencing note: unlike unary `call_transient`, this intentionally does not
      # send `expected_worker_id`. Higher-level stream dispatchers fence the
      # producer server-side by target ownership, so consumers observe hand-off
      # via a terminal `StaleLease`, not via a rejected request.
      #: (**Object?) -> ResultStream
      def call_transient_stream(**kwargs)
        worker_pool = kwargs.fetch(:worker_pool) #: as String
        method = kwargs.fetch(:method) #: as String
        args = kwargs.fetch(:args)
        class_name = kwargs.fetch(:class_name, "") #: as String
        durable_object_id = kwargs.fetch(:durable_object_id, "") #: as String
        workflow_id = kwargs.fetch(:workflow_id, "") #: as String
        deadline_ms = kwargs.fetch(:deadline_ms, 0) #: as Integer
        request = Messages::TransientRequest.new(
          worker_pool:,
          class_name:,
          durable_object_id:,
          workflow_id:,
          method:,
          args: Rpc.dump(args, surface: :rpc_argument, context: "CallTransientStream #{method} args"),
          deadline_ms:,
        )
        ResultStream.new { |writer| open_stream(request, writer) }
      end

      private

      # Runs on the `ResultStream`'s producer task (the consumer's reactor). Sends
      # the request, then decodes frames until the stream ends, errors, or the
      # consumer cancels. Transport-level failures map to `NodeUnavailable` so the
      # consumer sees a typed error; application errors arrive as terminal error
      # frames.
      #: (Messages::TransientRequest, ResultStream::Writer) -> void
      def open_stream(request, writer)
        Sync do |task|
          # HTTP/2 multiplexes streams over a single connection — the cached
          # client serves both `unary` and `open_stream`. We never close it
          # here: ownership belongs to the cache (or the test injecting it).
          client = @injected_client || Rpc.http_client_for(@address) #: as untyped
          response = nil
          begin
            response = task.with_timeout(@timeout) do
              client.post(PATHS.fetch(:call_transient_stream), OCTET_HEADERS, [Rpc.dump(request)])
            end
            consume_stream(response, writer, task)
          rescue Async::TimeoutError, Errno::ECONNREFUSED, Errno::ECONNRESET, Errno::EPIPE, Errno::ETIMEDOUT, Errno::EHOSTUNREACH, SocketError, IOError => e
            raise WorkflowRpc::NodeUnavailable, e.message
          rescue Protocol::HTTP2::Error => e
            raise WorkflowRpc::NodeUnavailable, e.message
          ensure
            cancel_stream(response)
          end
        end
      end

      # Forcefully resets the HTTP/2 stream when it is still open, then closes
      # the response. On a graceful end the server already sent END_STREAM, so
      # the stream is closed and the reset is skipped; on consumer cancellation
      # the stream is still open and the RST_STREAM tells the server to stop
      # producing (its body closes and `StreamWriter#cancelled?` flips true).
      # Without this, `client.close` would block draining an unfinished stream.
      #: (untyped) -> void
      def cancel_stream(response)
        return unless response

        stream = response.respond_to?(:stream) ? response.stream : nil
        if stream.respond_to?(:send_reset_stream) && !stream.closed?
          stream.send_reset_stream(Protocol::HTTP2::Error::CANCEL)
        end
      rescue StandardError
        nil
      ensure
        response&.close
      end

      #: (untyped, ResultStream::Writer, untyped) -> void
      def consume_stream(response, writer, task)
        ensure_stream_ok!(response)
        buffer = FrameCodec::Buffer.new
        loop do
          break if writer.cancelled?

          chunk = read_stream_chunk(response, task)
          next if chunk == :timeout
          break if chunk.nil?

          bytes = chunk #: as String
          buffer << bytes
          while (payload = buffer.shift)
            deliver_frame(Rpc.load(payload), writer)
          end
        end
      end

      # Reads the next body chunk, waking after `STREAM_POLL_TIMEOUT` so the loop
      # can re-check cancellation even when the server is idle. A timeout is not
      # an error: it returns the `:timeout` sentinel and the read is retried.
      #: (untyped, untyped) -> (String | Symbol)?
      def read_stream_chunk(response, task)
        task.with_timeout(STREAM_POLL_TIMEOUT) { response.body&.read }
      rescue Async::TimeoutError
        :timeout
      end

      #: (Object?, ResultStream::Writer) -> void
      def deliver_frame(frame, writer)
        frame = frame #: as Messages::StreamFrame
        if frame.error?
          error = frame.error #: as untyped
          raise Rpc.build_remote_error(error.klass.to_s, error.message.to_s)
        end

        writer.emit(frame.value)
      end

      # A streaming response is HTTP 200 with a frame body; any other status is a
      # transport-level failure carried in the (small, fully-read) body.
      #: (untyped) -> void
      def ensure_stream_ok!(response)
        status = response.status
        return if status == 200

        body = response.read
        case status
        when 401
          raise Unauthenticated, error_message(body, "durababble RPC peer is not authorized")
        else
          raise Unavailable, error_message(body, "durababble RPC node is unavailable")
        end
      end

      # Performs one unary request: encode the value object, POST it over HTTP/2,
      # then decode the response value object. Runs inside `Sync` so it is
      # callable both from a plain thread (worker control-plane) and from the
      # ambient reactor (engine/server handler), and applies the request timeout.
      # Uses the thread-local `Rpc.http_client_for` cache so back-to-back calls
      # to the same peer reuse the cached HTTP/2 connection pool — see the docs
      # on `Rpc.http_client_for`. Injected clients (for tests) bypass the cache
      # and are owned by the caller.
      #: (Symbol, Object) -> Object?
      def unary(method, request_value)
        with_rpc_errors do
          Sync do |task|
            task.with_timeout(@timeout) do
              client = @injected_client || Rpc.http_client_for(@address) #: as untyped
              response = client.post(PATHS.fetch(method), OCTET_HEADERS, [Rpc.dump(request_value)])
              handle_response(response)
            end
          end
        end
      end

      # Status-to-exception mapping; `call_transient` re-raises `Unavailable` as
      # `WorkflowRpc::NodeUnavailable` (which the router retries) and lets `Error`
      # propagate untouched (no retry). Only 503 maps to `Unavailable` — an
      # unexpected 500 from a peer signals a bug, not a healthy-but-busy node, so
      # retrying it would just amplify the failure. See the "Retry Semantics"
      # section in docs/content/cluster-rpc.md.
      #: (Object) -> Object?
      def handle_response(response)
        response = response #: as untyped
        status = response.status
        body = response.read
        case status
        when 200
          Rpc.load(body)
        when 401
          raise Unauthenticated, error_message(body, "durababble RPC peer is not authorized")
        when 503
          raise Unavailable, error_message(body, "durababble RPC node is unavailable")
        when 500
          raise Error, error_message(body, "durababble RPC handler raised on the peer")
        else
          raise Error, error_message(body, "durababble RPC failed with status #{status}")
        end
      end

      #: (String?, String) -> String
      def error_message(body, default)
        body.nil? || body.empty? ? default : body
      end

      #: () -> Async::HTTP::Endpoint
      def endpoint
        @endpoint ||= Async::HTTP::Endpoint.parse(@address.include?("://") ? @address : "http://#{@address}")
      end

      #: () { () -> Object? } -> Object?
      def with_rpc_errors(&block)
        block.call
      rescue Unauthenticated, Unavailable, Error
        raise
      rescue Async::TimeoutError, Errno::ECONNREFUSED, Errno::ECONNRESET, Errno::EPIPE, Errno::ETIMEDOUT, Errno::EHOSTUNREACH, SocketError, IOError => e
        raise Unavailable, e.message
      rescue StandardError => e
        raise Unavailable, e.message if e.is_a?(Protocol::HTTP2::Error)

        raise
      end
    end

    class WorkflowClient
      #: (address: String, ?worker_pool: String, ?credentials: Object?, ?timeout: Numeric) -> void
      def initialize(address:, worker_pool: "default", credentials: nil, timeout: DEFAULT_TIMEOUT)
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

      # `credentials:`/`pool_size:` are accepted for call-site compatibility with
      # the former gRPC server but unused: the transport is cleartext H2C and the
      # reactor multiplexes connections on one fiber scheduler (no thread pool).
      #: (node_id: String?, store: Store, ?worker_pool: String, ?workflow_handlers: Hash[String, Object], ?transient_handler: (Proc | Method)?, ?stream_handler: (Proc | Method)?, ?node_directory: NodeDirectory, ?host: String, ?port: Integer, ?credentials: Object?, ?pool_size: Integer?, ?authorize: (Proc | Method)?, ?awaken_batch: (Proc | Method)?, ?evict_lease: (Proc | Method)?, ?deliver_message: (Proc | Method)?, ?verify_deliver_message_owner: bool, ?identity_id: String?, ?stop_drain_timeout: Float) -> void
      def initialize(
        node_id:,
        store:,
        worker_pool: "default",
        workflow_handlers: {},
        transient_handler: nil,
        stream_handler: nil,
        node_directory: NodeDirectory.new,
        host: "127.0.0.1",
        port: 50_051,
        credentials: nil,
        pool_size: nil,
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
        @stream_handler = stream_handler
        @node_directory = node_directory
        @host = host
        @requested_port = port
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
        return self if @thread || @task

        server = build_http_server

        # The socket is already bound/listening (above, on this thread); the
        # reactor thread only runs the accept loop. Hand the scheduler back
        # through a Queue so `stop` can interrupt it thread-safely.
        ready = Thread::Queue.new
        @thread = Thread.new do
          Async do
            ready << Fiber.scheduler
            server.run
          end
        end
        @scheduler = ready.pop
        self
      end

      #: (?parent: Object) -> Server
      def start_async(parent: Async::Task.current)
        return self if @thread || @task

        server = build_http_server
        @task = parent.async { server.run }
        self
      end

      # Interrupt the reactor and join its thread, bounding the wait by
      # `@stop_drain_timeout` so a wedged in-flight request/stream cannot block
      # shutdown forever; if the drain times out we log and abandon it.
      #: () -> void
      def stop
        @scheduler&.interrupt
        @task&.stop
        if @thread && !@thread.join(@stop_drain_timeout)
          Durababble.logger&.warn(
            "Durababble::Rpc::Server#stop timed out after #{@stop_drain_timeout}s " \
              "waiting for the reactor thread to drain; abandoning it",
          )
        end
      ensure
        @bound&.close
        @thread = nil
        @task = nil
        @scheduler = nil
        @bound = nil
        @port = nil
      end

      #: () -> String
      def address
        "#{host}:#{@port || @requested_port}"
      end

      private

      #: () -> Async::HTTP::Server
      def build_http_server
        http_endpoint = Async::HTTP::Endpoint.parse("http://#{@host}:#{@requested_port}")
        @bound = http_endpoint.bound
        @port = @bound.sockets.first.local_address.ip_port
        @node_id ||= WorkerIdentity.generate(address:, id: @identity_id)
        service = build_service
        app = ->(request) { route(service, request) }
        Async::HTTP::Server.new(app, @bound, protocol: http_endpoint.protocol, scheme: http_endpoint.scheme)
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
          stream_handler: @stream_handler,
          node_directory: @node_directory,
          authorize: @authorize,
          awaken_batch: @awaken_batch,
          evict_lease: @evict_lease,
          deliver_message: @deliver_message,
          verify_deliver_message_owner: @verify_deliver_message_owner,
        )
      end

      # Maps an HTTP request to a service method. Application-level errors are
      # already encoded as discriminated response bodies by `Service`
      # (`call_transient` returns `not_running`/`moved`/`err` frames with a 200);
      # only authorization failures and unexpected raises become status codes.
      #
      # The case/when below is the one place where decoded wire bytes (`Object`
      # from `Rpc.load`) cross into the typed `Service` API — each branch's
      # `#: as` cast is the single boundary between "we trust the bytes match
      # the path we routed" and the concrete `Messages::*` shape. Service
      # methods are typed in terms of those `Messages::*` classes downstream and
      # need no per-method casts.
      #
      # The catch-all `StandardError` -> 503 below means an *unexpected* bug in a
      # handler is reported to the caller as `Unavailable` (which it may retry).
      # For `call_transient` that path is effectively unreachable — `Service`
      # already converts application errors into 200 `err` frames — but the
      # best-effort control-plane methods (`awaken_batch`/`evict_lease`/
      # `deliver_message`) have no inner rescue, so a deterministic bug there would
      # be masked as transient. That is an accepted trade-off for control-plane
      # RPCs (the caller retries a few times and gives up); revisit if a handler
      # ever needs to surface a non-retryable error to the caller.
      #: (Service, Object) -> Object
      def route(service, request)
        request = request #: as untyped
        method = ROUTES[request.path]
        return Protocol::HTTP::Response[404, Rpc.octet_headers, []] unless method

        # The wire boundary: bytes become a typed `Messages::*` value here, and the
        # path dictates which one. The unary methods go through `public_send`
        # (untyped dispatch), but the stream call is direct, so cast the decoded
        # value to the `TransientRequest` its handler expects.
        request_value = Rpc.load(request.read)
        if method == :call_transient_stream
          stream_request = request_value #: as Messages::TransientRequest
          body = service.open_stream(stream_request, request)
          return Protocol::HTTP::Response[200, Rpc.octet_headers, body]
        end

        response_value = service.public_send(method, request_value, request)
        Protocol::HTTP::Response[200, Rpc.octet_headers, [Rpc.dump(response_value)]]
      rescue Unauthenticated => e
        Protocol::HTTP::Response[401, OCTET_HEADERS, [e.message.to_s]]
      rescue StandardError => e
        # Unexpected raises return 500 (`Rpc::Error`, NOT retried) so an
        # unforeseen bug on one node cannot get amplified by client-side retries
        # into a stampede against an already-struggling cluster. 503 is reserved
        # for transport-level unavailability (timeouts, connection failures —
        # produced by `with_rpc_errors`, not by `route`) which IS retried via
        # `WorkflowRpc::NodeUnavailable`.
        Protocol::HTTP::Response[500, OCTET_HEADERS, [e.message.to_s]]
      end
    end

    class Service
      #: (node_id: String, store: Store, worker_pool: String, workflow_handlers: Hash[String, Object], transient_handler: (Proc | Method)?, node_directory: NodeDirectory, authorize: (Proc | Method)?, awaken_batch: (Proc | Method)?, evict_lease: (Proc | Method)?, deliver_message: (Proc | Method)?, ?stream_handler: (Proc | Method)?, ?verify_deliver_message_owner: bool) -> void
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
        stream_handler: nil,
        verify_deliver_message_owner: true
      )
        @node_id = node_id
        @store = store
        @worker_pool = worker_pool
        @workflow_handlers = workflow_handlers
        @transient_handler = transient_handler
        @stream_handler = stream_handler
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
        raise
      rescue StandardError => e
        remote_error_response(e)
      end

      # Returns a streaming response body for a `call_transient_stream` request.
      # The body is filled by a child reactor task running the stream handler;
      # `route` wraps it in a 200 response and the server pumps frames to the
      # consumer as they are written. Authorization is checked synchronously so a
      # failure becomes a 401 (rather than a half-open stream). Application
      # errors raised by the handler are encoded as a terminal error frame.
      #: (Messages::TransientRequest, Object) -> Protocol::HTTP::Body::Writable
      def open_stream(request, call)
        authorize!(call)
        body = Protocol::HTTP::Body::Writable.new(nil, queue: Thread::SizedQueue.new(STREAM_BUFFER))
        Async { write_stream(request, body) }
        body
      end

      private

      #: (Messages::TransientRequest, Protocol::HTTP::Body::Writable) -> void
      def write_stream(request, body)
        raise WorkflowRpc::UnknownCommand, "no streaming RPC handler registered" unless @stream_handler

        args = load_request_args(request, context: "CallTransientStream #{request["method"]} args")
        @stream_handler.call(request:, args:, writer: StreamWriter.new(body))
        body.close_write
      rescue StandardError => e
        finish_stream_with_error(body, e)
      end

      # Encodes a terminal error frame and ends the stream. If the body is
      # already closed the consumer cancelled (reset the stream), so there is
      # nothing to deliver and any write would raise — bail out quietly.
      #: (Protocol::HTTP::Body::Writable, Exception) -> void
      def finish_stream_with_error(body, error)
        return if body.closed?

        frame = Messages::StreamFrame.new(
          kind: :error,
          error: Messages::RemoteError.new(
            klass: error.class.name.to_s,
            message: error.message.to_s,
            backtrace: error.backtrace || [],
          ),
        )
        body.write(FrameCodec.frame(Rpc.dump(frame)))
        body.close_write
      rescue Protocol::HTTP::Body::Writable::Closed, IOError
        nil
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
          "payload" => Rpc.load(request.args) || {},
        }
        with_store do |store|
          WorkflowRpc::Handler.new(
            store:,
            node_id: @node_id,
            handlers: @workflow_handlers,
          ).call(payload)
        end
      end

      # Routes a transient query through a dedicated AR connection when the store
      # exposes one (PR #77). Reactor-shared connections corrupt under concurrent
      # fan-out (Trilogy yields mid-query); the dedicated lane keeps query bodies
      # isolated from the surrounding RPC reactor.
      #: () { (Store) -> Object? } -> Object?
      def with_store(&block)
        if @store.respond_to?(:with_dedicated_connection)
          @store.with_dedicated_connection(&block)
        else
          block.call(@store)
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

      #: (Messages::TargetRequest) -> bool
      def stale_workflow_message?(request)
        return false unless request.target_kind == "workflow"

        lease = @store.current_workflow_lease(request.target_id, worker_pool: request.worker_pool)
        !lease || lease.fetch("worker_id") != @node_id
      end

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
