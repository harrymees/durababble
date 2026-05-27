# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowRpcTest < DurababbleTestCase
  class InProcessWorkflowRpcClient
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
      raise Durababble::Rpc::RemoteError, @error_message
    end
  end

  def setup
    @durababble_backend = durababble_store_backends.first
    @durababble_schema = "#{@durababble_backend.default_schema_prefix}_workflow_rpc_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    @durababble_store = Durababble::Store.connect(database_url:, schema:)
    @durababble_store.migrate!
    @workflow_id = store.enqueue_workflow(name: "workflow-rpc-test", input: {})
  end

  def teardown
    @durababble_store&.drop_schema!
    @durababble_store&.close
    @durababble_store = nil
    @durababble_schema = nil
    @durababble_backend = nil
    @workflow_id = nil
  end

  test "routes workflow RPCs to the current lease holder and the receiver validates ownership" do
    store = self.store
    claim_as("worker-a")
    handler = Durababble::WorkflowRpc::Handler.new(store:, node_id: "worker-a", handlers: {
      "status" => ->(payload) { { "seen" => payload.fetch("value") } },
    })
    client = InProcessWorkflowRpcClient.new(handler)
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => client })

    assert_equal({ "seen" => 1 }, router.request(workflow_id:, command: "status", payload: { "value" => 1 }))
    assert_equal 1, client.requests.length
  end

  test "passes the active lease worker pool to route client factories" do
    store = self.store
    pooled_workflow_id = store.enqueue_workflow(name: "workflow-rpc-test", input: {}, worker_pool: "priority")
    store.claim_workflow(workflow_id: pooled_workflow_id, worker_id: "worker-a", lease_seconds: 60, worker_pool: "priority")
    handler = Durababble::WorkflowRpc::Handler.new(store:, node_id: "worker-a", handlers: {
      "status" => ->(_payload) { { "ok" => true } },
    })
    client = InProcessWorkflowRpcClient.new(handler)
    routed = []
    router = Durababble::WorkflowRpc::Router.new(
      store:,
      rpc_client_factory: lambda do |worker_id, worker_pool:|
        routed << [worker_id, worker_pool]
        client
      end,
    )

    assert_equal({ "ok" => true }, router.request(workflow_id: pooled_workflow_id, command: "status", payload: {}))
    assert_equal [["worker-a", "priority"]], routed
  end

  test "rejects a stale in-flight RPC when the target lost the workflow lease before receive" do
    store = self.store
    claim_as("worker-a")
    handler = Durababble::WorkflowRpc::Handler.new(store:, node_id: "worker-a", handlers: {
      "status" => ->(_payload) { { "should_not" => "run" } },
    })
    stale_client = InProcessWorkflowRpcClient.new(lambda do |payload|
      move_lease_to("worker-b")
      handler.call(payload)
    end)
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => stale_client })

    assert_raises_matching(Durababble::WorkflowRpc::StaleLease, /worker-a no longer owns/) do
      router.request(workflow_id:, command: "status", payload: {})
    end
  end

  test "rejects a stale in-flight RPC when it reaches a different node than the routed lease holder" do
    store = self.store
    claim_as("worker-a")
    handler = Durababble::WorkflowRpc::Handler.new(store:, node_id: "worker-b", handlers: {
      "status" => ->(_payload) { { "should_not" => "run" } },
    })
    wrong_node_client = InProcessWorkflowRpcClient.new(handler)
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => wrong_node_client })

    assert_raises_matching(Durababble::WorkflowRpc::StaleLease, /expected worker-a, but reached worker-b/) do
      router.request(workflow_id:, command: "status", payload: {})
    end
  end

  test "rejects unknown workflow RPC commands before invoking a handler" do
    store = self.store
    claim_as("worker-a")
    handler = Durababble::WorkflowRpc::Handler.new(store:, node_id: "worker-a", handlers: {})
    client = InProcessWorkflowRpcClient.new(handler)
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => client })

    assert_raises_matching(Durababble::WorkflowRpc::UnknownCommand, /unknown workflow RPC command missing/) do
      router.request(workflow_id:, command: "missing", payload: {})
    end
  end

  test "rejects an RPC whose workflow is lost after the handler runs" do
    store = self.store
    claim_as("worker-a")
    handler = Durababble::WorkflowRpc::Handler.new(store:, node_id: "worker-a", handlers: {
      "finish" => lambda do |_payload|
        complete_workflow
        { "should_not" => "escape" }
      end,
    })
    client = InProcessWorkflowRpcClient.new(handler)
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => client })

    assert_raises_matching(Durababble::WorkflowRpc::WorkflowNotRunning, /completed/) do
      router.request(workflow_id:, command: "finish", payload: {})
    end
  end

  test "does not internally start workflows that are waiting rather than runnable" do
    store = self.store
    record_waiting_workflow
    router = Durababble::WorkflowRpc::Router.new(
      store:,
      rpc_clients: { "worker-a" => InProcessWorkflowRpcClient.new(->(_payload) { nil }) },
      retry_on_stale: true,
    )

    assert_raises_matching(Durababble::WorkflowRpc::WorkflowNotRunning, /waiting/) do
      router.request(workflow_id:, command: "status", payload: {})
    end
    assert_nil store.current_workflow_lease(workflow_id)
    assert_equal "waiting", store.workflow(workflow_id).fetch("status")
  end

  test "raises no-active-lease without starting when retry-on-stale is disabled" do
    store = self.store
    router = Durababble::WorkflowRpc::Router.new(
      store:,
      rpc_clients: { "worker-a" => InProcessWorkflowRpcClient.new(->(_payload) { nil }) },
    )

    assert_raises_matching(Durababble::WorkflowRpc::NoActiveLease, /no active lease/) do
      router.request(workflow_id:, command: "status", payload: {})
    end
    assert_nil store.current_workflow_lease(workflow_id)
    assert_equal "pending", store.workflow(workflow_id).fetch("status")
  end

  test "stops retrying if stale ownership keeps changing underneath the RPC" do
    store = self.store
    claim_as("worker-a")
    stale_clients = {}
    ["worker-a", "worker-b", "worker-c", "worker-d"].each_cons(2) do |from, to|
      handler = Durababble::WorkflowRpc::Handler.new(
        store:,
        node_id: from,
        handlers: { "status" => ->(_payload) { { "bad" => true } } },
      )
      stale_clients[from] = InProcessWorkflowRpcClient.new(lambda do |payload|
        move_lease_to(to)
        handler.call(payload)
      end)
    end
    stale_clients["worker-d"] = InProcessWorkflowRpcClient.new(lambda do |payload|
      move_lease_to("worker-e")
      Durababble::WorkflowRpc::Handler.new(
        store:,
        node_id: "worker-d",
        handlers: { "status" => ->(_payload) { { "bad" => true } } },
      ).call(payload)
    end)
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: stale_clients, retry_on_stale: true)

    assert_raises(Durababble::WorkflowRpc::StaleLease) do
      router.request(workflow_id:, command: "status", payload: {})
    end
  end

  test "can refresh and retry once when an in-flight RPC discovers a new lease holder" do
    store = self.store
    claim_as("worker-a")
    stale_handler = Durababble::WorkflowRpc::Handler.new(
      store:,
      node_id: "worker-a",
      handlers: { "status" => ->(_payload) { { "bad" => true } } },
    )
    fresh_handler = Durababble::WorkflowRpc::Handler.new(
      store:,
      node_id: "worker-b",
      handlers: { "status" => ->(_payload) { { "owner" => "worker-b" } } },
    )
    stale_client = InProcessWorkflowRpcClient.new(lambda do |payload|
      move_lease_to("worker-b")
      stale_handler.call(payload)
    end)
    fresh_client = InProcessWorkflowRpcClient.new(fresh_handler)
    router = Durababble::WorkflowRpc::Router.new(
      store:,
      rpc_clients: { "worker-a" => stale_client, "worker-b" => fresh_client },
      retry_on_stale: true,
    )

    assert_equal({ "owner" => "worker-b" }, router.request(workflow_id:, command: "status", payload: {}))
    assert_equal 1, stale_client.requests.length
    assert_equal 1, fresh_client.requests.length
  end

  test "retries a transient node-unavailable transport failure against the fresh active owner" do
    store = self.store
    claim_as("worker-a")
    handler = Durababble::WorkflowRpc::Handler.new(
      store:,
      node_id: "worker-a",
      handlers: { "status" => ->(_payload) { { "owner" => "worker-a" } } },
    )
    attempts = 0
    client = InProcessWorkflowRpcClient.new(lambda do |payload|
      attempts += 1
      raise Durababble::WorkflowRpc::NodeUnavailable, "worker-a timeout" if attempts == 1

      handler.call(payload)
    end)
    router = Durababble::WorkflowRpc::Router.new(
      store:,
      rpc_clients: { "worker-a" => client },
      retry_on_stale: true,
    )

    assert_equal({ "owner" => "worker-a" }, router.request(workflow_id:, command: "status", payload: {}))
    assert_equal 2, client.requests.length
  end

  test "reroutes after a transport failure when the failed owner loses its lease" do
    store = self.store
    claim_as("worker-a")
    unavailable_client = InProcessWorkflowRpcClient.new(lambda do |_payload|
      move_lease_to("worker-b")
      raise Durababble::WorkflowRpc::NodeUnavailable, "worker-a connection reset"
    end)
    fresh_handler = Durababble::WorkflowRpc::Handler.new(
      store:,
      node_id: "worker-b",
      handlers: { "status" => ->(_payload) { { "owner" => "worker-b" } } },
    )
    fresh_client = InProcessWorkflowRpcClient.new(fresh_handler)
    router = Durababble::WorkflowRpc::Router.new(
      store:,
      rpc_clients: { "worker-a" => unavailable_client, "worker-b" => fresh_client },
      retry_on_stale: true,
    )

    assert_equal({ "owner" => "worker-b" }, router.request(workflow_id:, command: "status", payload: {}))
    assert_equal 1, unavailable_client.requests.length
    assert_equal 1, fresh_client.requests.length
  end

  test "stops retrying persistent node-unavailable transport failures" do
    store = self.store
    claim_as("worker-a")
    unavailable_client = InProcessWorkflowRpcClient.new(lambda do |_payload|
      raise Durababble::WorkflowRpc::NodeUnavailable, "worker-a unavailable"
    end)
    router = Durababble::WorkflowRpc::Router.new(
      store:,
      rpc_clients: { "worker-a" => unavailable_client },
      retry_on_stale: true,
    )

    assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /unavailable/) do
      router.request(workflow_id:, command: "status", payload: {})
    end
    assert_equal 4, unavailable_client.requests.length
  end

  test "maps remote stale errors back to workflow RPC errors so process-backed clients retry" do
    store = self.store
    claim_as("worker-a")
    stale_client = WorkflowRpcRemoteErrorClient.new("Durababble::WorkflowRpc::StaleLease: worker-a no longer owns workflow #{workflow_id}") do
      move_lease_to("worker-b")
    end
    fresh_handler = Durababble::WorkflowRpc::Handler.new(
      store:,
      node_id: "worker-b",
      handlers: { "status" => ->(_payload) { { "owner" => "worker-b" } } },
    )
    fresh_client = InProcessWorkflowRpcClient.new(fresh_handler)
    router = Durababble::WorkflowRpc::Router.new(
      store:,
      rpc_clients: { "worker-a" => stale_client, "worker-b" => fresh_client },
      retry_on_stale: true,
    )

    assert_equal({ "owner" => "worker-b" }, router.request(workflow_id:, command: "status", payload: {}))
    assert_equal 1, stale_client.requests.length
    assert_equal 1, fresh_client.requests.length
  end

  test "maps remote terminal workflow errors without retrying them" do
    store = self.store
    claim_as("worker-a")
    remote_client = WorkflowRpcRemoteErrorClient.new("Durababble::WorkflowRpc::WorkflowNotRunning: workflow #{workflow_id} is completed")
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => remote_client }, retry_on_stale: true)

    assert_raises_matching(Durababble::WorkflowRpc::WorkflowNotRunning, /completed/) do
      router.request(workflow_id:, command: "status", payload: {})
    end
    assert_equal 1, remote_client.requests.length
  end

  test "maps remote no-active-lease errors back to typed routing errors" do
    store = self.store
    claim_as("worker-a")
    remote_client = WorkflowRpcRemoteErrorClient.new("WorkflowRpc::NoActiveLease: workflow #{workflow_id} has no active lease")
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => remote_client }, retry_on_stale: false)

    assert_raises_matching(Durababble::WorkflowRpc::NoActiveLease, /no active lease/) do
      router.request(workflow_id:, command: "status", payload: {})
    end
  end

  test "maps remote node-unavailable errors back to typed routing errors" do
    store = self.store
    claim_as("worker-a")
    remote_client = WorkflowRpcRemoteErrorClient.new("NodeUnavailable: worker-a unavailable")
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => remote_client }, retry_on_stale: false)

    assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /unavailable/) do
      router.request(workflow_id:, command: "status", payload: {})
    end
  end

  test "maps remote unknown-command errors back to typed handler errors" do
    store = self.store
    claim_as("worker-a")
    remote_client = WorkflowRpcRemoteErrorClient.new("UnknownCommand: unknown workflow RPC command missing")
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => remote_client }, retry_on_stale: false)

    assert_raises_matching(Durababble::WorkflowRpc::UnknownCommand, /missing/) do
      router.request(workflow_id:, command: "missing", payload: {})
    end
  end

  test "preserves unrecognized remote errors instead of inventing routing semantics" do
    store = self.store
    claim_as("worker-a")
    remote_client = WorkflowRpcRemoteErrorClient.new("SomeOtherError: boom")
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => remote_client }, retry_on_stale: false)

    assert_raises_matching(Durababble::Rpc::RemoteError, /SomeOtherError/) do
      router.request(workflow_id:, command: "status", payload: {})
    end
  end

  test "does not retry a stale in-flight RPC after the workflow has shut down" do
    store = self.store
    claim_as("worker-a")
    handler = Durababble::WorkflowRpc::Handler.new(
      store:,
      node_id: "worker-a",
      handlers: { "status" => ->(_payload) { { "bad" => true } } },
    )
    client = InProcessWorkflowRpcClient.new(lambda do |payload|
      complete_workflow
      handler.call(payload)
    end)
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => client }, retry_on_stale: true)

    assert_raises(Durababble::WorkflowRpc::WorkflowNotRunning) do
      router.request(workflow_id:, command: "status", payload: {})
    end
    assert_equal 1, client.requests.length
  end

  test "refreshes after stale rejection and reports an unavailable new active owner" do
    store = self.store
    claim_as("worker-a")
    handler = Durababble::WorkflowRpc::Handler.new(
      store:,
      node_id: "worker-a",
      handlers: { "status" => ->(_payload) { { "bad" => true } } },
    )
    stale_client = InProcessWorkflowRpcClient.new(lambda do |payload|
      move_lease_to("worker-b")
      handler.call(payload)
    end)
    router = Durababble::WorkflowRpc::Router.new(store:, rpc_clients: { "worker-a" => stale_client }, retry_on_stale: true)

    assert_raises(Durababble::WorkflowRpc::NodeUnavailable) do
      router.request(workflow_id:, command: "status", payload: {})
    end
    assert_equal 1, stale_client.requests.length
  end

  test "refreshes after stale rejection and starts the workflow internally before rerouting" do
    store = self.store
    claim_as("worker-a")
    stale_handler = Durababble::WorkflowRpc::Handler.new(
      store:,
      node_id: "worker-a",
      handlers: { "status" => ->(_payload) { { "bad" => true } } },
    )
    stale_client = InProcessWorkflowRpcClient.new(lambda do |payload|
      release_current_lease
      stale_handler.call(payload)
    end)
    restarted_handler = Durababble::WorkflowRpc::Handler.new(
      store:,
      node_id: "worker-b",
      handlers: { "status" => ->(_payload) { { "owner" => "worker-b", "started" => true } } },
    )
    restarted_client = InProcessWorkflowRpcClient.new(restarted_handler)
    router = Durababble::WorkflowRpc::Router.new(
      store:,
      rpc_clients: { "worker-a" => stale_client, "worker-b" => restarted_client },
      retry_on_stale: true,
      start_workflow: Durababble::WorkflowRpc::LeaseStarter.new(store:, worker_ids: ["worker-b"], lease_seconds: 30),
    )

    assert_equal(
      { "owner" => "worker-b", "started" => true },
      router.request(workflow_id:, command: "status", payload: {}),
    )
    assert_equal 1, stale_client.requests.length
    assert_equal 1, restarted_client.requests.length
    assert_hash_includes store.current_workflow_lease(workflow_id), "worker_id" => "worker-b"
  end

  test "starts expired leases with the first worker that can claim and awaits the active owner" do
    store = self.store
    claim_as("worker-a", lease_seconds: -1)
    starter = Durababble::WorkflowRpc::LeaseStarter.new(store:, worker_ids: ["worker-b", "worker-c"], lease_seconds: 9)

    lease = starter.call(workflow_id:)

    assert_hash_includes lease, "worker_id" => "worker-b"
    assert_hash_includes store.current_workflow_lease(workflow_id), "worker_id" => "worker-b"
  end

  test "starts non-default pool workflows from the persisted workflow worker pool" do
    store = self.store
    pooled_workflow_id = store.enqueue_workflow(name: "workflow-rpc-test", input: {}, worker_pool: "priority")
    starter = Durababble::WorkflowRpc::LeaseStarter.new(store:, worker_ids: ["worker-b"], lease_seconds: 9)

    lease = starter.call(workflow_id: pooled_workflow_id)

    assert_hash_includes lease, "worker_id" => "worker-b", "worker_pool" => "priority"
    assert_hash_includes store.current_workflow_lease(pooled_workflow_id), "worker_id" => "worker-b", "worker_pool" => "priority"
  end

  test "awaits an externally started lease when no configured worker wins the claim" do
    store = self.store
    sleeps = []
    starter = Durababble::WorkflowRpc::LeaseStarter.new(
      store:,
      worker_ids: [],
      await_attempts: 3,
      await_sleep: lambda do |attempt|
        sleeps << attempt
        move_lease_to("external-worker") if attempt.zero?
      end,
    )

    assert_hash_includes starter.call(workflow_id:), "worker_id" => "external-worker"
    assert_equal [0], sleeps
  end

  test "awaits an externally started lease when configured workers cannot claim" do
    store = self.store
    record_waiting_workflow
    sleeps = []
    starter = Durababble::WorkflowRpc::LeaseStarter.new(
      store:,
      worker_ids: ["worker-b"],
      await_attempts: 2,
      await_sleep: lambda do |attempt|
        sleeps << attempt
        signal_wait_and_claim_as("external-worker")
      end,
    )

    assert_hash_includes starter.call(workflow_id:), "worker_id" => "external-worker"
    assert_hash_includes store.current_workflow_lease(workflow_id), "worker_id" => "external-worker"
    assert_equal [0], sleeps
  end

  test "raises no-active-lease if internal start cannot establish an owner" do
    store = self.store
    starter = Durababble::WorkflowRpc::LeaseStarter.new(store:, worker_ids: [], await_attempts: 2)

    assert_raises_matching(Durababble::WorkflowRpc::NoActiveLease, /could not be started/) do
      starter.call(workflow_id:)
    end
  end

  private

  attr_reader :workflow_id

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

  def release_current_lease
    current_owner = store.workflow(workflow_id)["locked_by"]
    store.release_worker_leases!(worker_id: current_owner) if current_owner
  end

  def record_waiting_workflow
    claim_as("waiting-owner")
    store.record_step_scheduled(workflow_id:, command_id: 0, name: "wait")
    store.record_step_started(workflow_id:, command_id: 0, name: "wait")
    store.record_wait(
      workflow_id:,
      command_id: 0,
      name: "wait",
      wait_request: Durababble::WaitRequest.new(kind: "timer", wake_at: Time.now + 3600, event_key: nil, context: {}),
    )
  end

  def signal_wait_and_claim_as(worker_id)
    store.wake_due_timers(now: Time.now + 7200)
    claim_as(worker_id)
  end
end
