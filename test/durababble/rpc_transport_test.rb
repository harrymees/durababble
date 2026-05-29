# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "async"
require "async/queue"
require "async/http/endpoint"
require "async/grpc"
require "timeout"

class DurababbleRpcTransportTest < DurababbleTestCase
  TestTransientResponse = Struct.new(:result, :ok, :err, :moved, keyword_init: true)
  TestRemoteError = Struct.new(:klass, :message, keyword_init: true)

  # Injected via `grpc_client:` to exercise transport error translation without
  # a live server.
  class FailingGrpcClient
    def initialize(error)
      @error = error
    end

    def stub(_interface, _service_name)
      self
    end

    def public_send(_method, _request, **_options)
      raise @error
    end

    def close; end
  end

  class RecordingGrpcClient
    attr_reader :calls

    def initialize(response)
      @response = response
      @calls = []
    end

    def stub(_interface, _service_name)
      self
    end

    def public_send(method, request, **options)
      @calls << [method, request, options]
      @response
    end
  end

  def setup
    @durababble_backend = durababble_store_backends.first
    @durababble_schema = "#{@durababble_backend.default_schema_prefix}_rpc_transport_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    @durababble_store = Durababble::Store.connect(database_url:, schema:)
    @durababble_store.migrate!
    @workflow_id = store.enqueue_workflow(name: "rpc-transport-test", input: {})
  end

  def teardown
    Durababble::Rpc.shutdown_grpc_clients!
    @durababble_store&.drop_schema!
    @durababble_store&.close
    @durababble_store = nil
    @durababble_schema = nil
    @durababble_backend = nil
    @workflow_id = nil
  end

  test "rpc messages are grpc-compatible envelopes around Paquito payloads" do
    request = Durababble::Rpc::Messages::AwakenBatchRequest.new(worker_pool: "default", workflow_ids: ["wf-1"])
    decoded = Durababble::Rpc::Messages::AwakenBatchRequest.decode(request.encode)

    assert_equal("default", decoded.worker_pool)
    assert_equal(["wf-1"], decoded.workflow_ids)
    assert_raises(TypeError) do
      Durababble::Rpc::Messages::AwakenBatchRequest.decode(Durababble::Rpc::Messages::AwakenBatchResponse.new.encode)
    end
  end

  test "serves the full four-method RPC contract over localhost" do
    events = []
    with_rpc_server(
      node_id: "node-a",
      store:,
      workflow_handlers: { "status" => ->(payload) { { "seen" => payload.fetch("value") } } },
      awaken_batch: ->(**event) { events << [:awaken_batch, event] },
      evict_lease: ->(**event) { events << [:evict_lease, event] },
      deliver_message: ->(**event) { events << [:deliver_message, event] },
    ) do |server|
      claim_as("node-a")
      events = []
      client = Durababble::Rpc::Client.new(address: server.address)

      assert_equal(true, client.awaken_batch(worker_pool: "default", workflow_ids: [workflow_id, "wf-2"]))
      assert_equal(true, client.evict_lease(
        worker_pool: "default",
        target_kind: "workflow",
        target_id: workflow_id,
      ))
      assert_equal(true, client.deliver_message(
        worker_pool: "default",
        target_kind: "object",
        target_class: "Counter",
        target_id: "counter-1",
      ))
      assert_equal(
        { "seen" => 7 },
        client.call_transient(
          worker_pool: "default",
          workflow_id:,
          method: "status",
          args: { "value" => 7 },
        ),
      )
      assert_equal(
        [
          [:awaken_batch, { worker_pool: "default", workflow_ids: [workflow_id, "wf-2"] }],
          [:evict_lease, { worker_pool: "default", target_kind: "workflow", target_class: "", target_id: workflow_id }],
          [:deliver_message, { worker_pool: "default", target_kind: "object", target_class: "Counter", target_id: "counter-1" }],
        ],
        events,
      )
    end
  end

  test "routes workflow RPC through real clients and reroutes when the lease moves" do
    store = self.store
    assertions_ran = false
    claim_as("node-a")
    directory = Durababble::Rpc::NodeDirectory.new
    with_rpc_server(
      node_id: "node-b",
      store:,
      node_directory: directory,
      workflow_handlers: { "status" => ->(_payload) { { "owner" => "node-b" } } },
    ) do |node_b|
      directory.register(node_id: "node-b", rpc_address: node_b.address)
      with_rpc_server(
        node_id: "node-a",
        store:,
        node_directory: directory,
        workflow_handlers: {
          "status" => lambda do |_payload|
            move_lease_to("node-b")
            { "should_not" => "escape" }
          end,
        },
      ) do |node_a|
        directory.register(node_id: "node-a", rpc_address: node_a.address)
        router = Durababble::WorkflowRpc::Router.new(
          store:,
          rpc_clients: {
            "node-a" => Durababble::Rpc::WorkflowClient.new(address: node_a.address),
            "node-b" => Durababble::Rpc::WorkflowClient.new(address: node_b.address),
          },
          retry_on_stale: true,
        )

        assert_equal({ "owner" => "node-b" }, router.request(workflow_id:, command: "status", payload: {}))
        assertions_ran = true
      end
    end
    assert_equal(true, assertions_ran)
  end

  test "rejects workflow RPCs addressed to a previous worker incarnation at a recycled address" do
    store = self.store
    ran = false
    with_rpc_server(
      node_id: nil,
      store:,
      workflow_handlers: { "status" => ->(_payload) { ran = true } },
    ) do |server|
      old_identity = "previous-worker@#{server.address}"
      store.claim_workflow(workflow_id:, worker_id: old_identity, lease_seconds: 30)
      client = Durababble::Rpc::Client.new(address: server.address)

      assert_match(/\A[0-9a-f]{12}@#{Regexp.escape(server.address)}\z/, server.node_id)
      assert_equal(server.address, Durababble::WorkerIdentity.address_for(old_identity))
      assert_raises(Durababble::WorkflowRpc::StaleLease) do
        client.call_transient(
          worker_pool: "default",
          workflow_id:,
          method: "status",
          args: {},
          expected_worker_id: old_identity,
        )
      end
      assert_equal(false, ran)
    end
  end

  test "acknowledges workflow message wakeups without work when the lease moved away" do
    store = self.store
    claim_as("node-b")
    events = []
    with_rpc_server(
      node_id: "node-a",
      store:,
      deliver_message: ->(**event) { events << event },
    ) do |server|
      client = Durababble::Rpc::Client.new(address: server.address)

      assert_equal(true, client.deliver_message(
        worker_pool: "default",
        target_kind: "workflow",
        target_id: workflow_id,
      ))
      assert_empty(events)
    end
  end

  test "returns typed workflow RPC errors over the wire" do
    store = self.store
    claim_as("node-a")
    with_rpc_server(node_id: "node-a", store:, workflow_handlers: {}) do |server|
      client = Durababble::Rpc::Client.new(address: server.address)

      assert_raises_matching(Durababble::WorkflowRpc::UnknownCommand, /missing/) do
        client.call_transient(worker_pool: "default", workflow_id:, method: "missing", args: {})
      end

      complete_workflow
      assert_raises_matching(Durababble::WorkflowRpc::WorkflowNotRunning, /completed/) do
        client.call_transient(worker_pool: "default", workflow_id:, method: "missing", args: {})
      end

      terminal_workflows = [
        ["canceled", "rpc-transport-canceled", ->(id) { store.cancel_workflow(id, reason: "user canceled") }],
        ["failed", "rpc-transport-failed", ->(id) { store.fail_workflow(id, error: "fatal") }],
      ]
      terminal_workflows.each do |status, name, finish|
        id = store.enqueue_workflow(name:, input: {})
        finish.call(id)
        error = assert_raises(Durababble::WorkflowRpc::WorkflowNotRunning) do
          client.call_transient(worker_pool: "default", workflow_id: id, method: "missing", args: {})
        end
        refute_instance_of(Durababble::WorkflowRpc::NoActiveLease, error)
        assert_match(/#{status}/, error.message)
      end
    end
  end

  test "maps no active lease and unavailable nodes to typed routing failures" do
    store = self.store
    address = nil
    with_rpc_server(node_id: "node-a", store:, workflow_handlers: {}) do |server|
      address = server.address
      client = Durababble::Rpc::Client.new(address:)

      assert_raises_matching(Durababble::WorkflowRpc::NoActiveLease, /not running/) do
        client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
      end
    end

    client = Durababble::Rpc::Client.new(address:, timeout: 0.1)
    assert_raises(Durababble::WorkflowRpc::NodeUnavailable) do
      client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
    end
  end

  test "maps timeouts and transport failures to typed node-unavailable routing errors" do
    [
      Async::TimeoutError.new("deadline exceeded"),
      Errno::ECONNREFUSED.new("connection reset"),
      SocketError.new("getaddrinfo: nodename nor servname provided"),
    ].each do |error|
      client = Durababble::Rpc::Client.new(address: "node-a", grpc_client: FailingGrpcClient.new(error))

      assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /#{Regexp.escape(error.message)}/) do
        client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
      end
    end
  end

  test "uses local timeout without sending grpc timeout metadata" do
    grpc_client = RecordingGrpcClient.new(
      Durababble::Rpc::Messages::TransientResponse.new(ok: Durababble::Rpc.dump({ "ok" => true })),
    )
    client = Durababble::Rpc::Client.new(address: "node-a", timeout: 1.5, grpc_client:)

    assert_equal({ "ok" => true }, client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {}))

    assert_equal(1, grpc_client.calls.length)
    method, _request, options = grpc_client.calls.first
    assert_equal(:call_transient, method)
    refute_includes(options, :timeout)

    assert_raises(ArgumentError) do
      Durababble::Rpc::Client.new(address: "node-a", http_client: Object.new)
    end
  end

  test "maps grpc statuses to typed transport errors but re-raises unexpected errors" do
    unavailable_client = Durababble::Rpc::Client.new(
      address: "node-a",
      grpc_client: FailingGrpcClient.new(Protocol::GRPC::Unavailable.new("peer unavailable")),
    )
    assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /peer unavailable/) do
      unavailable_client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
    end

    deadline_client = Durababble::Rpc::Client.new(
      address: "node-a",
      grpc_client: FailingGrpcClient.new(Protocol::GRPC::DeadlineExceeded.new("deadline exceeded")),
    )
    assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /deadline exceeded/) do
      deadline_client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
    end

    unauthenticated_client = Durababble::Rpc::Client.new(
      address: "node-a",
      grpc_client: FailingGrpcClient.new(Protocol::GRPC::Unauthenticated.new("bad peer")),
    )
    assert_raises_matching(Durababble::Rpc::Unauthenticated, /bad peer/) do
      unauthenticated_client.awaken_batch(worker_pool: "default", workflow_ids: [])
    end

    internal_client = Durababble::Rpc::Client.new(
      address: "node-a",
      grpc_client: FailingGrpcClient.new(Protocol::GRPC::Internal.new("handler exploded")),
    )
    error = assert_raises(Durababble::Rpc::Error) do
      internal_client.awaken_batch(worker_pool: "default", workflow_ids: [])
    end
    refute_kind_of(Durababble::Rpc::Unavailable, error)
    assert_match(/handler exploded/, error.message)

    # A non-transport StandardError is not a connectivity failure: it must
    # propagate unchanged rather than be masked as node-unavailable.
    surprising_client = Durababble::Rpc::Client.new(
      address: "node-a",
      grpc_client: FailingGrpcClient.new(ArgumentError.new("unexpected")),
    )
    assert_raises_matching(ArgumentError, /unexpected/) do
      surprising_client.awaken_batch(worker_pool: "default", workflow_ids: [])
    end
  end

  test "keeps server lifecycle and workflow client command validation idempotent" do
    store = self.store
    server = Durababble::Rpc::Server.new(node_id: "node-a", store:, port: 0)

    assert_raises_matching(Durababble::ConfigurationError, /inside a running Async reactor/) do
      server.start
    end

    Async do |task|
      assert_same(server, server.start)
      assert_same(server, server.start)
      assert_same(server, server.start_async(parent: task))
      assert_match(/\A127\.0\.0\.1:\d+\z/, server.address)
      assert_equal(true, Durababble::Rpc::Client.new(address: server.address).awaken_batch(worker_pool: "default", workflow_ids: []))

      client = Durababble::Rpc::WorkflowClient.new(address: server.address)
      assert_raises_matching(Durababble::WorkflowRpc::UnknownCommand, /not_workflow_rpc/) do
        client.request("not_workflow_rpc", {})
      end
    ensure
      server.stop
    end.wait
  ensure
    server&.stop
  end

  test "stop is safe before start and idempotent after a start/stop cycle" do
    store = self.store
    server = Durababble::Rpc::Server.new(node_id: "node-a", store:, port: 0)

    # Never started: the `&.` guards in `stop` must make this a no-op, not raise.
    Timeout.timeout(5) { server.stop }
    assert_nil(server.port)

    Async do
      server.start
      assert_match(/\A127\.0\.0\.1:\d+\z/, server.address)
      Timeout.timeout(5) { server.stop }
      # The Async accept task and bound socket are torn down and the port is cleared.
      assert_nil(server.port)
      # A second stop after teardown is still safe.
      Timeout.timeout(5) { server.stop }
    end.wait
  ensure
    server&.stop
  end

  test "stop can be invoked from a non-reactor control thread" do
    store = self.store
    server = Durababble::Rpc::Server.new(node_id: "node-a", store:, port: 0)
    started = Queue.new

    reactor_thread = Thread.new do
      Thread.current.report_on_exception = false
      Async do |task|
        server.start_async(parent: task)
        started << server.address
        server.instance_variable_get(:@task).wait
      ensure
        server.stop
      end.wait
    end

    address = started.pop
    assert_match(/\A127\.0\.0\.1:\d+\z/, address)
    assert_equal(true, Durababble::Rpc::Client.new(address:).awaken_batch(worker_pool: "default", workflow_ids: []))

    Timeout.timeout(5) { server.stop }
    Timeout.timeout(5) { reactor_thread.join }
    assert_equal(false, reactor_thread.alive?)
    assert_nil(server.port)
  ensure
    server&.stop
    if reactor_thread&.alive?
      reactor_thread.kill
      reactor_thread.join
    end
  end

  test "nested rpc server helper waits for child assertions" do
    with_rpc_server(node_id: "node-a", store:, workflow_handlers: {}) do
      error = assert_raises(Minitest::Assertion) do
        with_rpc_server(node_id: "node-b", store:, workflow_handlers: {}) do
          flunk("nested failure must propagate through the helper")
        end
      end
      assert_match(/nested failure/, error.message)
    end
  end

  test "rejects unauthorized peers before running handlers" do
    store = self.store
    ran = false
    with_rpc_server(
      node_id: "node-a",
      store:,
      workflow_handlers: { "status" => ->(_payload) { ran = true } },
      authorize: ->(_call) { false },
    ) do |server|
      client = Durababble::Rpc::Client.new(address: server.address)

      assert_raises_matching(Durababble::Rpc::Unauthenticated, /not authorized/) do
        client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
      end
      assert_equal(false, ran)
    end
  end

  test "supports non-workflow transient handlers over the same transient method" do
    store = self.store
    with_rpc_server(
      node_id: "node-a",
      store:,
      transient_handler: ->(request:, args:) { { "method" => request["method"], "args" => args } },
    ) do |server|
      client = Durababble::Rpc::Client.new(address: server.address)

      assert_equal(
        { "method" => "balance", "args" => { "object" => "acct-1" } },
        client.call_transient(
          worker_pool: "default",
          class_name: "Account",
          durable_object_id: "acct-1",
          method: "balance",
          args: { "object" => "acct-1" },
        ),
      )
    end
  end

  test "enforces RPC argument byte limits before sending or dispatching" do
    args = { "body" => "x" * 64 }
    size = Durababble::Rpc::SERIALIZER.dump(args).bytesize
    calls = []
    with_rpc_server(
      node_id: "node-a",
      store:,
      transient_handler: lambda do |request:, args:|
        calls << [request["method"], args]
        { "ok" => true }
      end,
    ) do |server|
      client = Durababble::Rpc::Client.new(address: server.address)

      with_payload_limit(:rpc_argument, size + 1) do
        assert_equal(
          { "ok" => true },
          client.call_transient(worker_pool: "default", method: "status", args:),
        )
      end
      assert_equal([["status", args]], calls)

      with_payload_limit(:rpc_argument, size) do
        assert_equal(
          { "ok" => true },
          client.call_transient(worker_pool: "default", method: "status", args:),
        )
      end
      assert_equal([["status", args], ["status", args]], calls)

      error = with_payload_limit(:rpc_argument, size - 1) do
        assert_raises(Durababble::PayloadTooLarge) do
          client.call_transient(worker_pool: "default", method: "status", args:)
        end
      end
      assert_equal(:rpc_argument, error.surface)
      assert_match(/CallTransient status args/, error.message)
      assert_equal(2, calls.length)

      # Prove the receiving node re-checks the limit even when a peer skips the
      # client-side guard: invoke the raw gRPC method with a pre-serialized,
      # oversized TransientRequest, bypassing Client#call_transient's Rpc.dump
      # enforcement. The server raises PayloadTooLarge, which surfaces as an
      # `err` frame rather than running the handler.
      raw_args = Durababble::Rpc::SERIALIZER.dump(args)
      raw_request = Durababble::Rpc::Messages::TransientRequest.new(worker_pool: "default", method: "status", args: raw_args)
      response = with_payload_limit(:rpc_argument, size - 1) do
        raw_grpc_call_transient(server.address, raw_request)
      end
      assert_equal("Durababble::PayloadTooLarge", response.err.klass)
      assert_equal(2, calls.length)
    end
  end

  test "decodes transport payloads and typed transient response branches" do
    assert_nil Durababble::Rpc.load(nil)
    assert_nil Durababble::Rpc.load("")
    assert_equal({ "ok" => true }, Durababble::Rpc.load(Durababble::Rpc.dump({ "ok" => true })))
    assert_nil Durababble::Rpc::Client.decode_transient_response(TestTransientResponse.new(result: :unknown))
    assert_raises(Durababble::Rpc::RemoteError) do
      Durababble::Rpc::Client.decode_transient_response(
        TestTransientResponse.new(result: :err, err: TestRemoteError.new(klass: "UnknownRemote", message: "bad")),
      )
    end
  end

  test "reports the populated oneof on transient responses" do
    messages = Durababble::Rpc::Messages
    assert_equal(:ok, messages::TransientResponse.new(ok: "x").result)
    assert_equal(:err, messages::TransientResponse.new(err: messages::RemoteError.new(klass: "E")).result)
    assert_equal(:not_running, messages::TransientResponse.new(not_running: true).result)
    assert_equal(:moved, messages::TransientResponse.new(moved: messages::LeaseMoved.new(new_node_id: "n")).result)
    # No field populated mirrors the former protobuf oneof reporting nothing set.
    assert_nil(messages::TransientResponse.new.result)
  end

  test "reports unknown custom transient methods as remote errors" do
    store = self.store
    service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store:,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: nil,
      node_directory: Durababble::Rpc::NodeDirectory.new,
      authorize: nil,
      awaken_batch: nil,
      evict_lease: nil,
      deliver_message: nil,
    )

    response = service.call_transient(
      Durababble::Rpc::Messages::TransientRequest.new(worker_pool: "default", method: "balance", args: Durababble::Rpc.dump({})),
      :call,
    )

    assert_equal("Durababble::WorkflowRpc::UnknownCommand", response.err.klass)
    assert_match(/unknown transient RPC method balance/, response.err.message)
  end

  test "delivers workflow message wakeups when this node owns the workflow lease" do
    store = self.store
    claim_as("node-a")
    delivered = []
    service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store:,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: nil,
      node_directory: Durababble::Rpc::NodeDirectory.new,
      authorize: nil,
      awaken_batch: nil,
      evict_lease: nil,
      deliver_message: ->(**kwargs) { delivered << kwargs },
    )

    service.deliver_message(
      Durababble::Rpc::Messages::DeliverMessageRequest.new(
        worker_pool: "default",
        target_kind: "workflow",
        target_class: "",
        target_id: workflow_id,
      ),
      :call,
    )

    assert_equal(
      [{ worker_pool: "default", target_kind: "workflow", target_class: "", target_id: workflow_id }],
      delivered,
    )
  end

  test "drops target RPCs addressed to a previous worker incarnation" do
    store = self.store
    delivered = []
    service = Durababble::Rpc::Service.new(
      node_id: "fresh-worker@127.0.0.1:50051",
      store:,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: nil,
      node_directory: Durababble::Rpc::NodeDirectory.new,
      authorize: nil,
      awaken_batch: nil,
      evict_lease: ->(**kwargs) { delivered << [:evict, kwargs] },
      deliver_message: ->(**kwargs) { delivered << [:deliver, kwargs] },
    )

    service.evict_lease(
      Durababble::Rpc::Messages::EvictLeaseRequest.new(
        worker_pool: "default",
        target_kind: "workflow",
        target_class: "",
        target_id: workflow_id,
        expected_worker_id: "old-worker@127.0.0.1:50051",
      ),
      :call,
    )
    service.deliver_message(
      Durababble::Rpc::Messages::DeliverMessageRequest.new(
        worker_pool: "default",
        target_kind: "workflow",
        target_class: "",
        target_id: workflow_id,
        expected_worker_id: "old-worker@127.0.0.1:50051",
      ),
      :call,
    )

    assert_empty delivered
  end

  test "drops stale workflow deliveries and returns local transient responses" do
    store = self.store
    delivered = []
    service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store:,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: ->(request:, args:) { ["custom", request["method"], args] },
      node_directory: Durababble::Rpc::NodeDirectory.new("node-b" => "127.0.0.1:6000"),
      authorize: ->(_call) { true },
      awaken_batch: ->(**kwargs) { delivered << [:awaken, kwargs] },
      evict_lease: ->(**kwargs) { delivered << [:evict, kwargs] },
      deliver_message: ->(**kwargs) { delivered << [:deliver, kwargs] },
    )

    service.awaken_batch(Durababble::Rpc::Messages::AwakenBatchRequest.new(worker_pool: "default", workflow_ids: [workflow_id]), :call)
    service.evict_lease(Durababble::Rpc::Messages::EvictLeaseRequest.new(worker_pool: "default", target_kind: "workflow", target_class: "", target_id: workflow_id), :call)
    service.deliver_message(Durababble::Rpc::Messages::DeliverMessageRequest.new(worker_pool: "default", target_kind: "workflow", target_class: "", target_id: workflow_id), :call)
    assert_equal [:awaken, :evict], delivered.map(&:first)

    response = service.call_transient(
      Durababble::Rpc::Messages::TransientRequest.new(worker_pool: "default", method: "ping", args: Durababble::Rpc.dump({ "x" => 1 })),
      :call,
    )
    assert_equal ["custom", "ping", { "x" => 1 }], Durababble::Rpc.load(response.ok)
  end

  test "rejects unauthorized service calls before dispatch" do
    store = self.store
    unauthorized = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store:,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: nil,
      node_directory: Durababble::Rpc::NodeDirectory.new,
      authorize: ->(_call) { false },
      awaken_batch: nil,
      evict_lease: nil,
      deliver_message: nil,
    )
    assert_raises(Durababble::Rpc::Unauthenticated) do
      unauthorized.awaken_batch(Durababble::Rpc::Messages::AwakenBatchRequest.new(worker_pool: "default", workflow_ids: []), :call)
    end
  end

  test "returns moved and local workflow transient errors from service dispatch" do
    store = self.store
    claim_as("node-b")
    moved_service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store:,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: nil,
      node_directory: Durababble::Rpc::NodeDirectory.new("node-b" => "127.0.0.1:6000"),
      authorize: nil,
      awaken_batch: nil,
      evict_lease: nil,
      deliver_message: nil,
    )
    moved = moved_service.call_transient(
      Durababble::Rpc::Messages::TransientRequest.new(worker_pool: "default", workflow_id: workflow_id, method: "status", args: Durababble::Rpc.dump({})),
      :call,
    )
    assert_equal "node-b", moved.moved.new_node_id

    move_lease_to("node-a")
    same_node_service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store:,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: nil,
      node_directory: Durababble::Rpc::NodeDirectory.new,
      authorize: nil,
      awaken_batch: nil,
      evict_lease: nil,
      deliver_message: nil,
    )
    remote_error = same_node_service.call_transient(
      Durababble::Rpc::Messages::TransientRequest.new(worker_pool: "default", workflow_id: workflow_id, method: "status", args: Durababble::Rpc.dump({})),
      :call,
    )
    assert_equal "Durababble::WorkflowRpc::UnknownCommand", remote_error.err.klass
  end

  test "answers non-grpc requests with 404 and grpc handler raises as Rpc::Error without retry" do
    store = self.store
    with_rpc_server(
      node_id: "node-a",
      store:,
      awaken_batch: ->(**_event) { raise "handler exploded" },
    ) do |server|
      status, = raw_http_post(server.address, "/durababble/v1/does_not_exist", "not grpc")
      assert_equal(404, status)

      # Unexpected handler raises become Rpc::Error (NOT Rpc::Unavailable /
      # NodeUnavailable), so the router will not retry.
      client = Durababble::Rpc::Client.new(address: server.address)
      error = assert_raises(Durababble::Rpc::Error) do
        client.awaken_batch(worker_pool: "default", workflow_ids: [])
      end
      refute_kind_of(Durababble::Rpc::Unavailable, error)
      assert_match(/handler exploded/, error.message)
    end
  end

  test "reroutes workflow RPC after local deadline while original owner changes" do
    store = self.store
    claim_as("node-a")
    original_started = Async::Queue.new
    original_release = Async::Queue.new
    assertions_ran = false

    with_rpc_server(
      node_id: "node-b",
      store:,
      workflow_handlers: { "status" => ->(_payload) { { "owner" => "node-b" } } },
    ) do |node_b|
      with_rpc_server(
        node_id: "node-a",
        store:,
        workflow_handlers: {
          "status" => lambda do |_payload|
            move_lease_to("node-b")
            original_started << true
            original_release.pop
            { "owner" => "node-a" }
          end,
        },
      ) do |node_a|
        router = Durababble::WorkflowRpc::Router.new(
          store:,
          rpc_clients: {
            "node-a" => Durababble::Rpc::WorkflowClient.new(address: node_a.address, timeout: 0.1),
            "node-b" => Durababble::Rpc::WorkflowClient.new(address: node_b.address, timeout: 1.0),
          },
          retry_on_stale: true,
        )

        response = Timeout.timeout(5) do
          router.request(workflow_id:, command: "status", payload: {})
        end
        Timeout.timeout(5) { original_started.pop }
        assert_equal({ "owner" => "node-b" }, response)
        assertions_ran = true
      ensure
        original_release << true
      end
    end
    assert_equal(true, assertions_ran)
  end

  test "grpc deadline exceeded remains retryable when peer supplied a deadline" do
    store = self.store
    claim_as("node-a")
    release = Async::Queue.new
    assertions_ran = false

    with_rpc_server(
      node_id: "node-b",
      store:,
      workflow_handlers: { "status" => ->(_payload) { { "owner" => "node-b" } } },
    ) do |node_b|
      with_rpc_server(
        node_id: "node-a",
        store:,
        workflow_handlers: {
          "status" => lambda do |_payload|
            move_lease_to("node-b")
            release.pop
            { "owner" => "node-a" }
          end,
        },
      ) do |node_a|
        raw_request = Durababble::Rpc::Messages::TransientRequest.new(
          worker_pool: "default",
          workflow_id:,
          method: "status",
          args: Durababble::Rpc.dump({}),
          expected_worker_id: "node-a",
        )

        assert_raises(Protocol::GRPC::DeadlineExceeded) do
          raw_grpc_call_transient(node_a.address, raw_request, timeout: 0.05)
        end

        router = Durababble::WorkflowRpc::Router.new(
          store:,
          rpc_clients: {
            "node-a" => Durababble::Rpc::WorkflowClient.new(address: node_a.address, timeout: 0.1),
            "node-b" => Durababble::Rpc::WorkflowClient.new(address: node_b.address, timeout: 1.0),
          },
          retry_on_stale: true,
        )
        assert_equal({ "owner" => "node-b" }, router.request(workflow_id:, command: "status", payload: {}))
        assertions_ran = true
      ensure
        release << true
      end
    end
    assert_equal(true, assertions_ran)
  end

  test "maps other grpc statuses to non-retryable client errors" do
    not_found = Durababble::Rpc::Client.new(address: "node-a", grpc_client: FailingGrpcClient.new(Protocol::GRPC::NotFound.new("missing")))
    error = assert_raises(Durababble::Rpc::Error) do
      not_found.awaken_batch(worker_pool: "default", workflow_ids: [])
    end
    refute_kind_of(Durababble::Rpc::Unavailable, error)
    assert_match(/missing/, error.message)
  end

  test "caches grpc clients per address and lets the caller shut them down" do
    store = self.store
    claim_as("node-a")
    with_rpc_server(
      node_id: "node-a",
      store:,
      workflow_handlers: { "status" => ->(_payload) { { "ok" => true } } },
    ) do |server|
      address = server.address

      # No cache to start with.
      Durababble::Rpc.shutdown_grpc_clients!
      refute(Durababble::Rpc.grpc_client_cached?(address))

      # First call populates the per-thread cache; subsequent constructors of
      # Rpc::Client wrappers reuse the same cached Async::GRPC::Client across
      # back-to-back calls.
      client_one = Durababble::Rpc::Client.new(address:)
      assert_equal({ "ok" => true }, client_one.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {}))
      assert(Durababble::Rpc.grpc_client_cached?(address))
      cached = Durababble::Rpc.grpc_client_for(address)

      client_two = Durababble::Rpc::Client.new(address:)
      assert_equal({ "ok" => true }, client_two.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {}))
      assert_same(cached, Durababble::Rpc.grpc_client_for(address), "second wrapper must hit the same cached gRPC client")

      # Different scheme/address is a separate cache entry.
      scheme_client = Durababble::Rpc::Client.new(address: "http://#{address}")
      assert_equal({ "ok" => true }, scheme_client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {}))
      assert(Durababble::Rpc.grpc_client_cached?("http://#{address}"))
      refute_same(cached, Durababble::Rpc.grpc_client_for("http://#{address}"))

      # Explicit shutdown is idempotent and clears the cache.
      Durababble::Rpc.shutdown_grpc_clients!
      refute(Durababble::Rpc.grpc_client_cached?(address))
      Durababble::Rpc.shutdown_grpc_clients! # second call is a no-op
    end
  end

  test "accepts addresses with an explicit scheme" do
    store = self.store
    claim_as("node-a")
    with_rpc_server(
      node_id: "node-a",
      store:,
      workflow_handlers: { "status" => ->(_payload) { { "ok" => true } } },
    ) do |server|
      client = Durababble::Rpc::Client.new(address: "http://#{server.address}")

      assert_equal(
        { "ok" => true },
        client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {}),
      )
    end
  end

  private

  attr_reader :workflow_id

  def with_rpc_server(**kwargs)
    server = nil
    runner = lambda do |task|
      server = Durababble::Rpc::Server.new(**kwargs, port: 0)
      server.start_async(parent: task)
      yield server
    ensure
      server&.stop
    end

    if (task = Async::Task.current?)
      runner.call(task)
    else
      Async(&runner)
    end
  end

  def database_url
    backend_descriptor.database_url
  end

  def claim_as(worker_id, lease_seconds: 60)
    store.claim_workflow(workflow_id:, worker_id:, lease_seconds:)
  end

  def move_lease_to(worker_id, lease_seconds: 60)
    current_owner = store.workflow(workflow_id)["locked_by"]
    store.release_worker_leases!(worker_id: current_owner) if current_owner
    claim_as(worker_id, lease_seconds:)
  end

  def complete_workflow
    store.complete_workflow(workflow_id, result: {})
  end

  # Invokes the gRPC service directly with a pre-built TransientRequest,
  # bypassing Client#call_transient and its client-side byte-limit guard.
  def raw_grpc_call_transient(address, request, timeout: nil)
    endpoint = Async::HTTP::Endpoint.parse("http://#{address}", protocol: Async::HTTP::Protocol::HTTP2)
    Sync do
      client = Async::HTTP::Client.new(endpoint, protocol: Async::HTTP::Protocol::HTTP2)
      begin
        grpc_client = Async::GRPC::Client.new(client)
        grpc_client.stub(Durababble::Rpc::Interface, Durababble::Rpc::SERVICE_NAME).call_transient(request, timeout:)
      ensure
        client.close
      end
    end
  end

  # Raw non-gRPC POST to exercise the dispatcher fallback path.
  def raw_http_post(address, path, body)
    endpoint = Async::HTTP::Endpoint.parse("http://#{address}", protocol: Async::HTTP::Protocol::HTTP2)
    Sync do
      client = Async::HTTP::Client.new(endpoint, protocol: Async::HTTP::Protocol::HTTP2)
      begin
        response = client.post(path, { "content-type" => "text/plain" }, [body])
        [response.status, response.read]
      ensure
        client.close
      end
    end
  end

  def with_payload_limit(surface, value)
    configured = Durababble.instance_variable_defined?(:@payload_limits)
    previous = Durababble.instance_variable_get(:@payload_limits) if configured
    Durababble.payload_limits = { surface => value }
    yield
  ensure
    if configured
      Durababble.instance_variable_set(:@payload_limits, previous)
    elsif Durababble.instance_variable_defined?(:@payload_limits)
      Durababble.remove_instance_variable(:@payload_limits)
    end
  end
end
