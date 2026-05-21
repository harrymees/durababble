# frozen_string_literal: true

require "spec_helper"

RSpec.describe Durababble::WorkflowRpc do
  class WorkflowRpcFakeStore
    attr_reader :workflow_id, :claims

    def initialize(rows)
      @rows = rows
      @claims = []
      @workflow_id = "wf-1"
    end

    def workflow(id)
      raise KeyError, id unless id == workflow_id

      @rows.last
    end

    def current_workflow_lease(workflow_id)
      row = workflow(workflow_id)
      return nil unless row.fetch("status") == "running"
      return nil unless row.fetch("locked_by")
      return nil if Time.parse(row.fetch("locked_until")) < Time.now

      { "workflow_id" => workflow_id, "worker_id" => row.fetch("locked_by"), "locked_until" => row.fetch("locked_until") }
    end

    def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
      @claims << { workflow_id:, worker_id:, lease_seconds: }
      row = workflow(workflow_id)
      return nil unless row.fetch("status") == "pending" || row.fetch("status") == "failed" || (row.fetch("status") == "running" && Time.parse(row.fetch("locked_until")) < Time.now)

      transition({ "id" => workflow_id, "status" => "running", "locked_by" => worker_id, "locked_until" => (Time.now + lease_seconds).utc.iso8601(6) })
      workflow(workflow_id)
    end

    def transition(row)
      @rows << row
    end
  end

  class WorkflowRpcFakeClient
    attr_reader :requests

    def initialize(handler)
      @handler = handler
      @requests = []
    end

    def request(command, payload)
      @requests << [command, payload]
      @handler.call(payload)
    end
  end

  class WorkflowRpcRemoteErrorClient
    attr_reader :requests

    def initialize(error_message, &on_request)
      @error_message = error_message
      @on_request = on_request
      @requests = []
    end

    def request(command, payload)
      @requests << [command, payload]
      @on_request&.call
      raise Durababble::RpcClient::RemoteError, @error_message
    end
  end

  def running(owner, seconds: 60)
    { "id" => "wf-1", "status" => "running", "locked_by" => owner, "locked_until" => (Time.now + seconds).utc.iso8601(6) }
  end

  def expired(owner)
    { "id" => "wf-1", "status" => "running", "locked_by" => owner, "locked_until" => (Time.now - 1).utc.iso8601(6) }
  end

  def pending
    { "id" => "wf-1", "status" => "pending", "locked_by" => nil, "locked_until" => nil }
  end

  def completed
    { "id" => "wf-1", "status" => "completed", "locked_by" => nil, "locked_until" => nil }
  end

  def waiting
    { "id" => "wf-1", "status" => "waiting", "locked_by" => nil, "locked_until" => nil }
  end

  it "routes workflow RPCs to the current lease holder and the receiver validates ownership" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    handler = described_class::Handler.new(store:, node_id: "worker-a", handlers: {
      "status" => ->(payload) { { "seen" => payload.fetch("value") } }
    })
    client = WorkflowRpcFakeClient.new(handler)
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => client })

    expect(router.request(workflow_id: "wf-1", command: "status", payload: { "value" => 1 })).to eq("seen" => 1)
    expect(client.requests.length).to eq(1)
  end

  it "rejects a stale in-flight RPC when the target lost the workflow lease before receive" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    handler = described_class::Handler.new(store:, node_id: "worker-a", handlers: {
      "status" => ->(_payload) { { "should_not" => "run" } }
    })
    stale_client = WorkflowRpcFakeClient.new(lambda do |payload|
      store.transition(running("worker-b"))
      handler.call(payload)
    end)
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => stale_client })

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::WorkflowRpc::StaleLease, /worker-a no longer owns/)
  end

  it "rejects a stale in-flight RPC when it reaches a different node than the routed lease holder" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    handler = described_class::Handler.new(store:, node_id: "worker-b", handlers: {
      "status" => ->(_payload) { { "should_not" => "run" } }
    })
    wrong_node_client = WorkflowRpcFakeClient.new(handler)
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => wrong_node_client })

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::WorkflowRpc::StaleLease, /expected worker-a, but reached worker-b/)
  end

  it "rejects unknown workflow RPC commands before invoking a handler" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    handler = described_class::Handler.new(store:, node_id: "worker-a", handlers: {})
    client = WorkflowRpcFakeClient.new(handler)
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => client })

    expect { router.request(workflow_id: "wf-1", command: "missing", payload: {}) }.to raise_error(Durababble::WorkflowRpc::UnknownCommand, /unknown workflow RPC command missing/)
  end

  it "rejects an RPC whose workflow is lost after the handler runs" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    handler = described_class::Handler.new(store:, node_id: "worker-a", handlers: {
      "finish" => lambda do |_payload|
        store.transition(completed)
        { "should_not" => "escape" }
      end
    })
    client = WorkflowRpcFakeClient.new(handler)
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => client })

    expect { router.request(workflow_id: "wf-1", command: "finish", payload: {}) }.to raise_error(Durababble::WorkflowRpc::WorkflowNotRunning, /completed/)
  end

  it "does not internally start workflows that are waiting rather than runnable" do
    store = WorkflowRpcFakeStore.new([waiting])
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => WorkflowRpcFakeClient.new(->(_payload) { nil }) }, retry_on_stale: true)

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::WorkflowRpc::WorkflowNotRunning, /waiting/)
    expect(store.claims).to be_empty
  end

  it "raises no-active-lease without starting when retry-on-stale is disabled" do
    store = WorkflowRpcFakeStore.new([pending])
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => WorkflowRpcFakeClient.new(->(_payload) { nil }) })

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::WorkflowRpc::NoActiveLease, /no active lease/)
    expect(store.claims).to be_empty
  end

  it "stops retrying if stale ownership keeps changing underneath the RPC" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    stale_clients = {}
    %w[worker-a worker-b worker-c worker-d].each_cons(2) do |from, to|
      handler = described_class::Handler.new(store:, node_id: from, handlers: { "status" => ->(_payload) { { "bad" => true } } })
      stale_clients[from] = WorkflowRpcFakeClient.new(lambda do |payload|
        store.transition(running(to))
        handler.call(payload)
      end)
    end
    stale_clients["worker-d"] = WorkflowRpcFakeClient.new(lambda do |payload|
      store.transition(running("worker-e"))
      described_class::Handler.new(store:, node_id: "worker-d", handlers: { "status" => ->(_payload) { { "bad" => true } } }).call(payload)
    end)
    router = described_class::Router.new(store:, rpc_clients: stale_clients, retry_on_stale: true)

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::WorkflowRpc::StaleLease)
  end

  it "can refresh and retry once when an in-flight RPC discovers a new lease holder" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    stale_handler = described_class::Handler.new(store:, node_id: "worker-a", handlers: { "status" => ->(_payload) { { "bad" => true } } })
    fresh_handler = described_class::Handler.new(store:, node_id: "worker-b", handlers: { "status" => ->(_payload) { { "owner" => "worker-b" } } })
    stale_client = WorkflowRpcFakeClient.new(lambda do |payload|
      store.transition(running("worker-b"))
      stale_handler.call(payload)
    end)
    fresh_client = WorkflowRpcFakeClient.new(fresh_handler)
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => stale_client, "worker-b" => fresh_client }, retry_on_stale: true)

    expect(router.request(workflow_id: "wf-1", command: "status", payload: {})).to eq("owner" => "worker-b")
    expect(stale_client.requests.length).to eq(1)
    expect(fresh_client.requests.length).to eq(1)
  end

  it "maps remote stale errors back to workflow RPC errors so process-backed clients retry" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    stale_client = WorkflowRpcRemoteErrorClient.new("Durababble::WorkflowRpc::StaleLease: worker-a no longer owns workflow wf-1") do
      store.transition(running("worker-b"))
    end
    fresh_handler = described_class::Handler.new(store:, node_id: "worker-b", handlers: { "status" => ->(_payload) { { "owner" => "worker-b" } } })
    fresh_client = WorkflowRpcFakeClient.new(fresh_handler)
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => stale_client, "worker-b" => fresh_client }, retry_on_stale: true)

    expect(router.request(workflow_id: "wf-1", command: "status", payload: {})).to eq("owner" => "worker-b")
    expect(stale_client.requests.length).to eq(1)
    expect(fresh_client.requests.length).to eq(1)
  end

  it "maps remote terminal workflow errors without retrying them" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    remote_client = WorkflowRpcRemoteErrorClient.new("Durababble::WorkflowRpc::WorkflowNotRunning: workflow wf-1 is completed")
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => remote_client }, retry_on_stale: true)

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::WorkflowRpc::WorkflowNotRunning, /completed/)
    expect(remote_client.requests.length).to eq(1)
  end

  it "maps remote no-active-lease errors back to typed routing errors" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    remote_client = WorkflowRpcRemoteErrorClient.new("WorkflowRpc::NoActiveLease: workflow wf-1 has no active lease")
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => remote_client }, retry_on_stale: false)

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::WorkflowRpc::NoActiveLease, /no active lease/)
  end

  it "maps remote node-unavailable errors back to typed routing errors" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    remote_client = WorkflowRpcRemoteErrorClient.new("NodeUnavailable: worker-a unavailable")
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => remote_client }, retry_on_stale: false)

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::WorkflowRpc::NodeUnavailable, /unavailable/)
  end

  it "maps remote unknown-command errors back to typed handler errors" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    remote_client = WorkflowRpcRemoteErrorClient.new("UnknownCommand: unknown workflow RPC command missing")
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => remote_client }, retry_on_stale: false)

    expect { router.request(workflow_id: "wf-1", command: "missing", payload: {}) }.to raise_error(Durababble::WorkflowRpc::UnknownCommand, /missing/)
  end

  it "preserves unrecognized remote errors instead of inventing routing semantics" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    remote_client = WorkflowRpcRemoteErrorClient.new("SomeOtherError: boom")
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => remote_client }, retry_on_stale: false)

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::RpcClient::RemoteError, /SomeOtherError/)
  end

  it "does not retry a stale in-flight RPC after the workflow has shut down" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    handler = described_class::Handler.new(store:, node_id: "worker-a", handlers: { "status" => ->(_payload) { { "bad" => true } } })
    client = WorkflowRpcFakeClient.new(lambda do |payload|
      store.transition(completed)
      handler.call(payload)
    end)
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => client }, retry_on_stale: true)

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::WorkflowRpc::WorkflowNotRunning)
    expect(client.requests.length).to eq(1)
  end

  it "refreshes after stale rejection and reports an unavailable new active owner" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    handler = described_class::Handler.new(store:, node_id: "worker-a", handlers: { "status" => ->(_payload) { { "bad" => true } } })
    stale_client = WorkflowRpcFakeClient.new(lambda do |payload|
      store.transition(running("worker-b"))
      handler.call(payload)
    end)
    router = described_class::Router.new(store:, rpc_clients: { "worker-a" => stale_client }, retry_on_stale: true)

    expect { router.request(workflow_id: "wf-1", command: "status", payload: {}) }.to raise_error(Durababble::WorkflowRpc::NodeUnavailable)
    expect(stale_client.requests.length).to eq(1)
  end

  it "refreshes after stale rejection and starts the workflow internally before rerouting" do
    store = WorkflowRpcFakeStore.new([running("worker-a")])
    stale_handler = described_class::Handler.new(store:, node_id: "worker-a", handlers: { "status" => ->(_payload) { { "bad" => true } } })
    stale_client = WorkflowRpcFakeClient.new(lambda do |payload|
      store.transition(pending)
      stale_handler.call(payload)
    end)
    restarted_handler = described_class::Handler.new(store:, node_id: "worker-b", handlers: { "status" => ->(_payload) { { "owner" => "worker-b", "started" => true } } })
    restarted_client = WorkflowRpcFakeClient.new(restarted_handler)
    router = described_class::Router.new(
      store:,
      rpc_clients: { "worker-a" => stale_client, "worker-b" => restarted_client },
      retry_on_stale: true,
      start_workflow: described_class::LeaseStarter.new(store:, worker_ids: ["worker-b"], lease_seconds: 30)
    )

    expect(router.request(workflow_id: "wf-1", command: "status", payload: {})).to eq("owner" => "worker-b", "started" => true)
    expect(stale_client.requests.length).to eq(1)
    expect(restarted_client.requests.length).to eq(1)
    expect(store.claims).to include(hash_including(worker_id: "worker-b", lease_seconds: 30))
  end

  it "starts expired leases with the first worker that can claim and awaits the active owner" do
    store = WorkflowRpcFakeStore.new([expired("worker-a")])
    starter = described_class::LeaseStarter.new(store:, worker_ids: ["worker-b", "worker-c"], lease_seconds: 9)

    lease = starter.call(workflow_id: "wf-1")

    expect(lease).to include("worker_id" => "worker-b")
    expect(store.claims).to eq([{ workflow_id: "wf-1", worker_id: "worker-b", lease_seconds: 9 }])
  end

  it "awaits an externally started lease when no configured worker wins the claim" do
    store = WorkflowRpcFakeStore.new([pending])
    sleeps = []
    starter = described_class::LeaseStarter.new(
      store:,
      worker_ids: [],
      await_attempts: 3,
      await_sleep: lambda do |attempt|
        sleeps << attempt
        store.transition(running("external-worker")) if attempt.zero?
      end
    )

    expect(starter.call(workflow_id: "wf-1")).to include("worker_id" => "external-worker")
    expect(sleeps).to eq([0])
  end

  it "awaits an externally started lease when configured workers cannot claim" do
    store = WorkflowRpcFakeStore.new([waiting])
    sleeps = []
    starter = described_class::LeaseStarter.new(
      store:,
      worker_ids: ["worker-b"],
      await_attempts: 2,
      await_sleep: lambda do |attempt|
        sleeps << attempt
        store.transition(running("external-worker"))
      end
    )

    expect(starter.call(workflow_id: "wf-1")).to include("worker_id" => "external-worker")
    expect(store.claims).to eq([{ workflow_id: "wf-1", worker_id: "worker-b", lease_seconds: 60 }])
    expect(sleeps).to eq([0])
  end

  it "raises no-active-lease if internal start cannot establish an owner" do
    store = WorkflowRpcFakeStore.new([pending])
    starter = described_class::LeaseStarter.new(store:, worker_ids: [], await_attempts: 2)

    expect { starter.call(workflow_id: "wf-1") }.to raise_error(Durababble::WorkflowRpc::NoActiveLease, /could not be started/)
  end
end
