# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "timeout"

class DurababbleRpcTransportTest < DurababbleTestCase
  TestTransientResponse = Struct.new(:result, :ok, :err, :moved, keyword_init: true)
  TestRemoteError = Struct.new(:klass, :message, keyword_init: true)

  # Injected via `http_client:` to exercise the transport's error translation
  # without a live server: its `#post` raises the transport-level failure.
  class FailingHttpClient
    def initialize(error)
      @error = error
    end

    def post(_path, _headers, _body)
      raise @error
    end

    def close; end
  end

  StubHttpResponse = Struct.new(:status, :body) do
    def read = body
  end

  # Injected via `http_client:` to drive `Client#handle_response` status
  # branches (and `error_message`'s empty-body default) without a live server.
  class StubHttpClient
    def initialize(status:, body:)
      @status = status
      @body = body
    end

    def post(_path, _headers, _body)
      StubHttpResponse.new(@status, @body)
    end

    def close; end
  end

  def setup
    @durababble_backend = durababble_store_backends.first
    @durababble_schema = "#{@durababble_backend.default_schema_prefix}_rpc_transport_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    @durababble_store = Durababble::Store.connect(database_url:, schema:)
    @durababble_store.migrate!
    @workflow_id = store.enqueue_workflow(name: "rpc-transport-test", input: {})
  end

  def teardown
    Durababble::Rpc.shutdown_http_clients!
    @durababble_store&.drop_schema!
    @durababble_store&.close
    @durababble_store = nil
    @durababble_schema = nil
    @durababble_backend = nil
    @workflow_id = nil
  end

  test "serves the full four-method RPC contract over localhost" do
    store = self.store
    claim_as("node-a")
    events = []
    server = start_rpc_server(
      node_id: "node-a",
      store:,
      workflow_handlers: { "status" => ->(payload) { { "seen" => payload.fetch("value") } } },
      awaken_batch: ->(**event) { events << [:awaken_batch, event] },
      evict_lease: ->(**event) { events << [:evict_lease, event] },
      deliver_message: ->(**event) { events << [:deliver_message, event] },
    )
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
  ensure
    server&.stop
  end

  test "routes workflow RPC through real clients and reroutes when the lease moves" do
    store = self.store
    claim_as("node-a")
    directory = Durababble::Rpc::NodeDirectory.new
    node_b = start_rpc_server(
      node_id: "node-b",
      store:,
      node_directory: directory,
      workflow_handlers: { "status" => ->(_payload) { { "owner" => "node-b" } } },
    )
    directory.register(node_id: "node-b", rpc_address: node_b.address)
    node_a = start_rpc_server(
      node_id: "node-a",
      store:,
      node_directory: directory,
      workflow_handlers: {
        "status" => lambda do |_payload|
          move_lease_to("node-b")
          { "should_not" => "escape" }
        end,
      },
    )
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
  ensure
    node_a&.stop
    node_b&.stop
  end

  test "rejects workflow RPCs addressed to a previous worker incarnation at a recycled address" do
    store = self.store
    ran = false
    server = start_rpc_server(
      node_id: nil,
      store:,
      workflow_handlers: { "status" => ->(_payload) { ran = true } },
    )
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
  ensure
    server&.stop
  end

  test "acknowledges workflow message wakeups without work when the lease moved away" do
    store = self.store
    claim_as("node-b")
    events = []
    server = start_rpc_server(
      node_id: "node-a",
      store:,
      deliver_message: ->(**event) { events << event },
    )
    client = Durababble::Rpc::Client.new(address: server.address)

    assert_equal(true, client.deliver_message(
      worker_pool: "default",
      target_kind: "workflow",
      target_id: workflow_id,
    ))
    assert_empty(events)
  ensure
    server&.stop
  end

  test "returns typed workflow RPC errors over the wire" do
    store = self.store
    claim_as("node-a")
    server = start_rpc_server(node_id: "node-a", store:, workflow_handlers: {})
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
  ensure
    server&.stop
  end

  test "maps no active lease and unavailable nodes to typed routing failures" do
    store = self.store
    server = start_rpc_server(node_id: "node-a", store:, workflow_handlers: {})
    address = server.address
    client = Durababble::Rpc::Client.new(address:)

    assert_raises_matching(Durababble::WorkflowRpc::NoActiveLease, /not running/) do
      client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
    end

    server.stop
    server = nil
    client = Durababble::Rpc::Client.new(address:, timeout: 0.1)
    assert_raises(Durababble::WorkflowRpc::NodeUnavailable) do
      client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
    end
  ensure
    server&.stop
  end

  test "maps timeouts and transport failures to typed node-unavailable routing errors" do
    [
      Async::TimeoutError.new("deadline exceeded"),
      Errno::ECONNREFUSED.new("connection reset"),
      SocketError.new("getaddrinfo: nodename nor servname provided"),
    ].each do |error|
      client = Durababble::Rpc::Client.new(address: "node-a", http_client: FailingHttpClient.new(error))

      assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /#{Regexp.escape(error.message)}/) do
        client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
      end
    end
  end

  test "treats HTTP/2 protocol errors as unavailable but re-raises unexpected errors" do
    http2_client = Durababble::Rpc::Client.new(
      address: "node-a",
      http_client: FailingHttpClient.new(Protocol::HTTP2::Error.new("stream reset")),
    )
    assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /stream reset/) do
      http2_client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
    end

    # A non-transport StandardError is not a connectivity failure: it must
    # propagate unchanged rather than be masked as node-unavailable.
    surprising_client = Durababble::Rpc::Client.new(
      address: "node-a",
      http_client: FailingHttpClient.new(ArgumentError.new("unexpected")),
    )
    assert_raises_matching(ArgumentError, /unexpected/) do
      surprising_client.awaken_batch(worker_pool: "default", workflow_ids: [])
    end
  end

  test "keeps server lifecycle and workflow client command validation idempotent" do
    store = self.store
    server = Durababble::Rpc::Server.new(node_id: "node-a", store:, port: 0, pool_size: 2)

    assert_same(server, server.start)
    assert_same(server, server.start)
    assert_match(/\A127\.0\.0\.1:\d+\z/, server.address)
    assert_equal(true, Durababble::Rpc::Client.new(address: server.address).awaken_batch(worker_pool: "default", workflow_ids: []))

    client = Durababble::Rpc::WorkflowClient.new(address: server.address)
    assert_raises_matching(Durababble::WorkflowRpc::UnknownCommand, /not_workflow_rpc/) do
      client.request("not_workflow_rpc", {})
    end
  ensure
    server&.stop
  end

  test "stop is safe before start and idempotent after a start/stop cycle" do
    store = self.store
    server = Durababble::Rpc::Server.new(node_id: "node-a", store:, port: 0)

    # Never started: the `&.` guards in `stop` must make this a no-op, not raise.
    Timeout.timeout(5) { server.stop }
    assert_nil(server.port)

    server.start
    assert_match(/\A127\.0\.0\.1:\d+\z/, server.address)
    Timeout.timeout(5) { server.stop }
    # The reactor thread and bound socket are torn down and the port is cleared.
    assert_nil(server.port)
    # A second stop after teardown is still safe.
    Timeout.timeout(5) { server.stop }
  ensure
    server&.stop
  end

  test "rejects unauthorized peers before running handlers" do
    store = self.store
    ran = false
    server = start_rpc_server(
      node_id: "node-a",
      store:,
      workflow_handlers: { "status" => ->(_payload) { ran = true } },
      authorize: ->(_call) { false },
    )
    client = Durababble::Rpc::Client.new(address: server.address)

    assert_raises_matching(Durababble::Rpc::Unauthenticated, /not authorized/) do
      client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
    end
    assert_equal(false, ran)
  ensure
    server&.stop
  end

  test "supports non-workflow transient handlers over the same transient method" do
    store = self.store
    server = start_rpc_server(
      node_id: "node-a",
      store:,
      transient_handler: ->(request:, args:) { { "method" => request["method"], "args" => args } },
    )
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
  ensure
    server&.stop
  end

  test "enforces RPC argument byte limits before sending or dispatching" do
    args = { "body" => "x" * 64 }
    size = Durababble::Rpc::SERIALIZER.dump(args).bytesize
    calls = []
    server = start_rpc_server(
      node_id: "node-a",
      store:,
      transient_handler: lambda do |request:, args:|
        calls << [request["method"], args]
        { "ok" => true }
      end,
    )
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
    # client-side guard: POST a pre-serialized, oversized TransientRequest
    # straight at the server (bypassing Client#call_transient's Rpc.dump
    # enforcement). The server raises PayloadTooLarge, which surfaces as an
    # `err` frame rather than running the handler.
    raw_args = Durababble::Rpc::SERIALIZER.dump(args)
    raw_request = Durababble::Rpc::Messages::TransientRequest.new(worker_pool: "default", method: "status", args: raw_args)
    response = with_payload_limit(:rpc_argument, size - 1) do
      post_raw_transient(server.address, raw_request)
    end
    assert_equal("Durababble::PayloadTooLarge", response.err.klass)
    assert_equal(2, calls.length)
  ensure
    server&.stop
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

  test "answers 404 for unknown paths and 500 (Rpc::Error, NOT retried) when a handler raises" do
    store = self.store
    server = start_rpc_server(
      node_id: "node-a",
      store:,
      awaken_batch: ->(**_event) { raise "handler exploded" },
    )

    status, = raw_rpc_post(server.address, "/durababble/v1/does_not_exist", { "anything" => true })
    assert_equal(404, status)

    # The raw POST proves the wire-level status code (500, not 503).
    raise_status, raise_body = raw_rpc_post(server.address, "/durababble/v1/awaken_batch", Durababble::Rpc::Messages::AwakenBatchRequest.new(worker_pool: "default"))
    assert_equal(500, raise_status)
    assert_match(/handler exploded/, raise_body)

    # And the client-side mapping: unexpected handler raises become Rpc::Error
    # (NOT Rpc::Unavailable / NodeUnavailable), so the router will not retry.
    client = Durababble::Rpc::Client.new(address: server.address)
    error = assert_raises(Durababble::Rpc::Error) do
      client.awaken_batch(worker_pool: "default", workflow_ids: [])
    end
    refute_kind_of(Durababble::Rpc::Unavailable, error)
    assert_match(/handler exploded/, error.message)
  ensure
    server&.stop
  end

  test "maps unexpected status codes and empty error bodies to typed client errors" do
    not_found = Durababble::Rpc::Client.new(address: "node-a", http_client: StubHttpClient.new(status: 404, body: ""))
    error = assert_raises(Durababble::Rpc::Error) do
      not_found.awaken_batch(worker_pool: "default", workflow_ids: [])
    end
    assert_match(/status 404/, error.message)

    unauthorized = Durababble::Rpc::Client.new(address: "node-a", http_client: StubHttpClient.new(status: 401, body: ""))
    assert_raises_matching(Durababble::Rpc::Unauthenticated, /not authorized/) do
      unauthorized.awaken_batch(worker_pool: "default", workflow_ids: [])
    end

    unavailable = Durababble::Rpc::Client.new(address: "node-a", http_client: StubHttpClient.new(status: 503, body: ""))
    assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /unavailable/) do
      unavailable.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
    end

    # 500 surfaces as Rpc::Error (not Unavailable), so call_transient does NOT
    # rewrite it as NodeUnavailable — that's the no-retry contract.
    exploded = Durababble::Rpc::Client.new(address: "node-a", http_client: StubHttpClient.new(status: 500, body: ""))
    error = assert_raises(Durababble::Rpc::Error) do
      exploded.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {})
    end
    refute_kind_of(Durababble::Rpc::Unavailable, error)
    assert_match(/handler raised on the peer/, error.message)
  end

  test "caches HTTP/2 clients per address and lets the caller shut them down" do
    store = self.store
    claim_as("node-a")
    server = start_rpc_server(
      node_id: "node-a",
      store:,
      workflow_handlers: { "status" => ->(_payload) { { "ok" => true } } },
    )
    address = server.address

    # No cache to start with.
    Durababble::Rpc.shutdown_http_clients!
    refute(Durababble::Rpc.http_client_cached?(address))

    # First call populates the per-thread cache; subsequent constructors of
    # Rpc::Client wrappers reuse the same cached Async::HTTP::Client across
    # back-to-back calls.
    client_one = Durababble::Rpc::Client.new(address:)
    assert_equal({ "ok" => true }, client_one.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {}))
    assert(Durababble::Rpc.http_client_cached?(address))
    cached = Durababble::Rpc.http_client_for(address)

    client_two = Durababble::Rpc::Client.new(address:)
    assert_equal({ "ok" => true }, client_two.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {}))
    assert_same(cached, Durababble::Rpc.http_client_for(address), "second wrapper must hit the same cached HTTP/2 client")

    # Different scheme/address is a separate cache entry.
    scheme_client = Durababble::Rpc::Client.new(address: "http://#{address}")
    assert_equal({ "ok" => true }, scheme_client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {}))
    assert(Durababble::Rpc.http_client_cached?("http://#{address}"))
    refute_same(cached, Durababble::Rpc.http_client_for("http://#{address}"))

    # Explicit shutdown is idempotent and clears the cache.
    Durababble::Rpc.shutdown_http_clients!
    refute(Durababble::Rpc.http_client_cached?(address))
    Durababble::Rpc.shutdown_http_clients! # second call is a no-op
  ensure
    server&.stop
  end

  test "accepts addresses with an explicit scheme" do
    store = self.store
    claim_as("node-a")
    server = start_rpc_server(
      node_id: "node-a",
      store:,
      workflow_handlers: { "status" => ->(_payload) { { "ok" => true } } },
    )
    client = Durababble::Rpc::Client.new(address: "http://#{server.address}")

    assert_equal(
      { "ok" => true },
      client.call_transient(worker_pool: "default", workflow_id:, method: "status", args: {}),
    )
  ensure
    server&.stop
  end

  private

  attr_reader :workflow_id

  def start_rpc_server(**kwargs)
    Durababble::Rpc::Server.new(**kwargs, port: 0, pool_size: 2).start
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

  # POSTs an already-serialized request straight at the server's call_transient
  # path over async-http, bypassing Client#call_transient (and its client-side
  # byte-limit guard). Returns the decoded TransientResponse value object.
  def post_raw_transient(address, request)
    _status, body = raw_rpc_post(address, Durababble::Rpc::PATHS.fetch(:call_transient), request)
    Durababble::Rpc.load(body)
  end

  # Raw async-http POST of a Paquito-dumped value to an arbitrary path. Returns
  # [status, raw_body]; lets tests reach the server's routing/error paths
  # (e.g. unknown paths) that Client never exercises.
  def raw_rpc_post(address, path, value)
    endpoint = Async::HTTP::Endpoint.parse("http://#{address}")
    body = Durababble::Rpc.dump(value)
    Sync do
      client = Async::HTTP::Client.new(endpoint, protocol: Async::HTTP::Protocol::HTTP2)
      begin
        response = client.post(path, Durababble::Rpc::OCTET_HEADERS, [body])
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
