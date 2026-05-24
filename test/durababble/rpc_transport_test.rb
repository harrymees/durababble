# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleRpcTransportTest < DurababbleTestCase
  FakeTransientResponse = Struct.new(:result, :ok, :err, :moved, keyword_init: true)
  FakeRemoteError = Struct.new(:klass, :message, keyword_init: true)

  class RpcTransportFakeStore
    attr_reader :workflow_id

    def initialize(row)
      @row = row
      @workflow_id = "wf-1"
    end

    def workflow(id)
      raise KeyError, id unless id == workflow_id

      @row
    end

    def current_workflow_lease(workflow_id)
      row = workflow(workflow_id)
      return unless row.fetch("status") == "running"
      return unless row.fetch("locked_by")
      return if Time.parse(row.fetch("locked_until")) < Time.now

      { "workflow_id" => workflow_id, "worker_id" => row.fetch("locked_by"), "locked_until" => row.fetch("locked_until") }
    end

    def transition(row)
      @row = row
    end
  end

  class FailingGrpcStub
    def initialize(error)
      @error = error
    end

    def call_transient(_request, deadline:)
      raise @error
    end
  end

  test "serves the full four-method gRPC contract over localhost" do
    store = RpcTransportFakeStore.new(running("node-a"))
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

    assert_equal(true, client.awaken_batch(worker_pool: "default", workflow_ids: ["wf-1", "wf-2"]))
    assert_equal(true, client.evict_lease(
      worker_pool: "default",
      target_kind: "workflow",
      target_id: "wf-1",
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
        workflow_id: "wf-1",
        method: "status",
        args: { "value" => 7 },
      ),
    )
    assert_equal(
      [
        [:awaken_batch, { worker_pool: "default", workflow_ids: ["wf-1", "wf-2"] }],
        [:evict_lease, { worker_pool: "default", target_kind: "workflow", target_class: "", target_id: "wf-1" }],
        [:deliver_message, { worker_pool: "default", target_kind: "object", target_class: "Counter", target_id: "counter-1" }],
      ],
      events,
    )
  ensure
    server&.stop
  end

  test "routes workflow RPC through real gRPC clients and reroutes when the lease moves" do
    store = RpcTransportFakeStore.new(running("node-a"))
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
          store.transition(running("node-b"))
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

    assert_equal({ "owner" => "node-b" }, router.request(workflow_id: "wf-1", command: "status", payload: {}))
  ensure
    node_a&.stop
    node_b&.stop
  end

  test "acknowledges workflow message wakeups without work when the lease moved away" do
    store = RpcTransportFakeStore.new(running("node-b"))
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
      target_id: "wf-1",
    ))
    assert_empty(events)
  ensure
    server&.stop
  end

  test "returns typed workflow RPC errors over gRPC" do
    store = RpcTransportFakeStore.new(running("node-a"))
    server = start_rpc_server(node_id: "node-a", store:, workflow_handlers: {})
    client = Durababble::Rpc::Client.new(address: server.address)

    assert_raises_matching(Durababble::WorkflowRpc::UnknownCommand, /missing/) do
      client.call_transient(worker_pool: "default", workflow_id: "wf-1", method: "missing", args: {})
    end

    store.transition(completed)
    assert_raises_matching(Durababble::WorkflowRpc::WorkflowNotRunning, /completed/) do
      client.call_transient(worker_pool: "default", workflow_id: "wf-1", method: "missing", args: {})
    end
  ensure
    server&.stop
  end

  test "maps no active lease and unavailable nodes to typed routing failures" do
    store = RpcTransportFakeStore.new(pending)
    server = start_rpc_server(node_id: "node-a", store:, workflow_handlers: {})
    client = Durababble::Rpc::Client.new(address: server.address, timeout: 0.1)

    assert_raises_matching(Durababble::WorkflowRpc::NoActiveLease, /not running/) do
      client.call_transient(worker_pool: "default", workflow_id: "wf-1", method: "status", args: {})
    end

    server.stop
    server = nil
    assert_raises(Durababble::WorkflowRpc::NodeUnavailable) do
      client.call_transient(worker_pool: "default", workflow_id: "wf-1", method: "status", args: {})
    end
  ensure
    server&.stop
  end

  test "maps gRPC deadline and transport failures to typed node-unavailable routing errors" do
    [
      GRPC::DeadlineExceeded.new("deadline exceeded"),
      GRPC::Unavailable.new("connection reset"),
    ].each do |error|
      client = Durababble::Rpc::Client.new(address: "node-a", stub: FailingGrpcStub.new(error))

      assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /#{Regexp.escape(error.details)}/) do
        client.call_transient(worker_pool: "default", workflow_id: "wf-1", method: "status", args: {})
      end
    end
  end

  test "rejects unauthorized gRPC peers before running handlers" do
    ran = false
    server = start_rpc_server(
      node_id: "node-a",
      store: RpcTransportFakeStore.new(running("node-a")),
      workflow_handlers: { "status" => ->(_payload) { ran = true } },
      authorize: ->(_call) { false },
    )
    client = Durababble::Rpc::Client.new(address: server.address)

    assert_raises_matching(Durababble::Rpc::Unauthenticated, /not authorized/) do
      client.call_transient(worker_pool: "default", workflow_id: "wf-1", method: "status", args: {})
    end
    assert_equal(false, ran)
  ensure
    server&.stop
  end

  test "supports non-workflow transient handlers over the same gRPC method" do
    server = start_rpc_server(
      node_id: "node-a",
      store: RpcTransportFakeStore.new(running("node-a")),
      transient_handler: ->(request:, args:) { { "method" => request["method"], "args" => args } },
    )
    client = Durababble::Rpc::Client.new(address: server.address)

    assert_equal(
      { "method" => "balance", "args" => { "object" => "acct-1" } },
      client.call_transient(
        worker_pool: "default",
        class_name: "Account",
        object_id: "acct-1",
        method: "balance",
        args: { "object" => "acct-1" },
      ),
    )
  ensure
    server&.stop
  end

  test "decodes transport payloads and typed transient response branches" do
    assert_nil Durababble::Rpc.load(nil)
    assert_nil Durababble::Rpc.load("")
    assert_equal({ "ok" => true }, Durababble::Rpc.load(Durababble::Rpc.dump({ "ok" => true })))
    assert_nil Durababble::Rpc::Client.decode_transient_response(FakeTransientResponse.new(result: :unknown))
    assert_raises(Durababble::RpcClient::RemoteError) do
      Durababble::Rpc::Client.decode_transient_response(
        FakeTransientResponse.new(result: :err, err: FakeRemoteError.new(klass: "UnknownRemote", message: "bad")),
      )
    end
  end

  test "drops stale workflow deliveries and returns local transient responses" do
    stale_store = Object.new
    def stale_store.current_workflow_lease(_id) = nil

    delivered = []
    service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store: stale_store,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: ->(request:, args:) { ["custom", request["method"], args] },
      node_directory: Durababble::Rpc::NodeDirectory.new("node-b" => "127.0.0.1:6000"),
      authorize: ->(_call) { true },
      awaken_batch: ->(**kwargs) { delivered << [:awaken, kwargs] },
      evict_lease: ->(**kwargs) { delivered << [:evict, kwargs] },
      deliver_message: ->(**kwargs) { delivered << [:deliver, kwargs] },
    )

    service.awaken_batch(Durababble::Rpc::Proto::AwakenBatchRequest.new(worker_pool: "default", workflow_ids: ["wf"]), :call)
    service.evict_lease(Durababble::Rpc::Proto::EvictLeaseRequest.new(worker_pool: "default", target_kind: "workflow", target_class: "", target_id: "wf"), :call)
    service.deliver_message(Durababble::Rpc::Proto::DeliverMessageRequest.new(worker_pool: "default", target_kind: "workflow", target_class: "", target_id: "wf"), :call)
    assert_equal [:awaken, :evict], delivered.map(&:first)

    response = service.call_transient(
      Durababble::Rpc::Proto::TransientRequest.new(worker_pool: "default", method: "ping", args: Durababble::Rpc.dump({ "x" => 1 })),
      :call,
    )
    assert_equal ["custom", "ping", { "x" => 1 }], Durababble::Rpc.load(response.ok)
  end

  test "rejects unauthorized service calls before dispatch" do
    stale_store = Object.new
    def stale_store.current_workflow_lease(_id) = nil

    unauthorized = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store: stale_store,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: nil,
      node_directory: Durababble::Rpc::NodeDirectory.new,
      authorize: ->(_call) { false },
      awaken_batch: nil,
      evict_lease: nil,
      deliver_message: nil,
    )
    assert_raises(GRPC::Unauthenticated) do
      unauthorized.awaken_batch(Durababble::Rpc::Proto::AwakenBatchRequest.new(worker_pool: "default", workflow_ids: []), :call)
    end
  end

  test "returns moved and local workflow transient errors from service dispatch" do
    moved_store = Object.new
    def moved_store.current_workflow_lease(_id) = { "worker_id" => "node-b" }

    moved_service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store: moved_store,
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
      Durababble::Rpc::Proto::TransientRequest.new(worker_pool: "default", workflow_id: "wf", method: "status", args: Durababble::Rpc.dump({})),
      :call,
    )
    assert_equal "node-b", moved.moved.new_node_id

    same_node_store = Object.new
    def same_node_store.current_workflow_lease(_id) = { "worker_id" => "node-a" }

    same_node_service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store: same_node_store,
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
      Durababble::Rpc::Proto::TransientRequest.new(worker_pool: "default", workflow_id: "wf", method: "status", args: Durababble::Rpc.dump({})),
      :call,
    )
    assert_equal "Durababble::WorkflowRpc::UnknownCommand", remote_error.err.klass
  end

  private

  def start_rpc_server(**kwargs)
    Durababble::Rpc::Server.new(**kwargs, port: 0, pool_size: 2).start
  end

  def running(owner, seconds: 60)
    {
      "id" => "wf-1",
      "status" => "running",
      "locked_by" => owner,
      "locked_until" => (Time.now + seconds).utc.iso8601(6),
    }
  end

  def pending
    { "id" => "wf-1", "status" => "pending", "locked_by" => nil, "locked_until" => nil }
  end

  def completed
    { "id" => "wf-1", "status" => "completed", "locked_by" => nil, "locked_until" => nil }
  end
end
