# typed: false
# frozen_string_literal: true

require "async"
require "timeout"
require_relative "../test_helper"

# End-to-end coverage for the streaming-result consumer entrypoints: opening a
# stream off a `DurableObjectRef` / `WorkflowRef` and the routing that picks a
# local snapshot vs. the lease-holding worker. The remote paths run a real
# `Rpc::Server` whose handler is a `StreamDispatcher`, exercising the full
# WorkflowRef -> rpc_client_factory -> Client -> Server -> dispatcher -> producer
# round-trip over localhost gRPC.
class DurababbleStreamConsumerTest < DurababbleTestCase
  def setup
    @durababble_backend = durababble_store_backends.first
    @durababble_schema = "#{@durababble_backend.default_schema_prefix}_stream_consumer_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    @durababble_store = Durababble::Store.connect(database_url:, schema:)
    @durababble_store.migrate!
    @servers = []
  end

  def teardown
    @servers.each(&:stop)
    @servers = nil
    @durababble_store&.drop_schema!
    @durababble_store&.close
    @durababble_store = nil
    @durababble_schema = nil
    @durababble_backend = nil
  end

  test "a durable-object stream runs against a local state snapshot" do
    object_class = Class.new(Durababble::DurableObject) do
      object_type "snapshot_log"
      expose_stream :entries
      def entries(&block)
        Array(current_state).each { |entry| block.call(entry) }
      end
    end
    store.save_object_state(object_type: "snapshot_log", object_id: "log-1", state: ["x", "y", "z"])

    stream = object_class.at("log-1", store:).entries

    assert_equal ["x", "y", "z"], stream.to_a
  end

  test "a workflow stream with no active lease runs against a local snapshot" do
    workflow_class = Class.new(Durababble::Workflow) do
      workflow_name "local_progress_wf"
      expose_stream :history
      def history(&block)
        [{ "v" => 1 }, { "v" => 2 }].each { |entry| block.call(entry) }
      end

      def execute(_input) = nil
    end
    workflow_id = store.enqueue_workflow(name: "local_progress_wf", input: {})

    stream = workflow_class.handle(workflow_id, store:).history

    assert_equal [{ "v" => 1 }, { "v" => 2 }], stream.to_a
  end

  test "a workflow stream routes to the worker that holds the lease" do
    workflow_class = Class.new(Durababble::Workflow) do
      workflow_name "remote_progress_wf"
      expose_stream :progress
      def progress(&block)
        [{ "step" => 1 }, { "step" => 2 }, { "step" => 3 }].each { |entry| block.call(entry) }
      end

      def execute(_input) = nil
    end
    workflow_id = store.enqueue_workflow(name: "remote_progress_wf", input: {})
    Async do
      server = start_dispatch_server(workflows: [workflow_class])
      store.claim_workflow(workflow_id:, worker_id: server.node_id, lease_seconds: 60)

      stream = workflow_class.handle(workflow_id, store:).progress

      assert_equal([{ "step" => 1 }, { "step" => 2 }, { "step" => 3 }], stream.to_a)
    end.wait
  end

  test "a workflow stream ends with StaleLease when the lease moves mid-stream" do
    workflow_class = Class.new(Durababble::Workflow) do
      workflow_name "lease_loss_wf"
      expose_stream :progress
      def progress(&block)
        loop do
          block.call({ "tick" => true })
          Kernel.sleep(0.02)
        end
      end

      def execute(_input) = nil
    end
    workflow_id = store.enqueue_workflow(name: "lease_loss_wf", input: {})
    Async do
      server = start_dispatch_server(workflows: [workflow_class])
      store.claim_workflow(workflow_id:, worker_id: server.node_id, lease_seconds: 60)

      stream = workflow_class.handle(workflow_id, store:).progress

      # `read` is incremental, so the first read, the lease move, and the drain all
      # run within one reactor scope (the producer task survives between pulls).
      assert_raises(Durababble::WorkflowRpc::StaleLease) do
        Timeout.timeout(5) do
          assert_equal({ "tick" => true }, stream.read)
          move_workflow_lease(workflow_id, to: "node-other")
          loop { stream.read }
        end
      end
    end.wait
  end

  private

  # Starts a live RPC server whose stream handler dispatches to a
  # `StreamDispatcher` bound to its own (post-start) node_id. In the in-process
  # default topology `node_id == address`, so claiming a workflow with
  # `worker_id: server.node_id` makes `rpc_client_factory` resolve back to it.
  def start_dispatch_server(workflows: [], objects: [])
    dispatcher = nil
    server = Durababble::Rpc::Server.new(
      node_id: nil,
      store:,
      port: 0,
      stream_handler: ->(request:, args:, writer:) { dispatcher.call(request:, args:, writer:) },
    )
    task = Async::Task.current? || raise("start_dispatch_server requires an active Async task")
    server.start_async(parent: task)
    dispatcher = Durababble::StreamDispatcher.new(store:, workflows:, objects:, node_id: server.node_id)
    @servers << server
    server
  end

  def move_workflow_lease(workflow_id, to:)
    current_owner = store.workflow(workflow_id)["locked_by"]
    store.release_worker_leases!(worker_id: current_owner) if current_owner
    store.claim_workflow(workflow_id:, worker_id: to, lease_seconds: 60)
  end

  def database_url
    backend_descriptor.database_url
  end
end
