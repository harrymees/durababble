# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "thread"

class DurababbleWorkerLifecycleTest < DurababbleTestCase
  class RuntimeBranchStore
    attr_reader :closed, :released

    def initialize(tick_results: [])
      @tick_results = tick_results.dup
      @closed = false
      @released = []
    end

    def migrate!; end

    def close
      @closed = true
    end

    def release_worker_leases!(worker_id:)
      @released << worker_id
    end

    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil)
      result = @tick_results.shift
      raise result if result.is_a?(Exception)

      result
    end

    def claim_target_activation(worker_id:, lease_seconds:, target_kinds:, target_types:)
      nil
    end
  end

  class ForcedUnavailableDeliveryClient
    def deliver_message(**)
      raise Durababble::Rpc::Unavailable, "forced delivery failure"
    end
  end

  def setup
    @durababble_backend = durababble_store_backends.first
    @durababble_schema = "#{@durababble_backend.default_schema_prefix}_worker_lifecycle_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    @durababble_store = Durababble::Store.connect(database_url:, schema:)
    @durababble_store.migrate!
  end

  def teardown
    @runtime_store&.close if defined?(@runtime_store) && @runtime_store
    @durababble_store&.drop_schema!
    @durababble_store&.close
    @runtime_store = nil
    @durababble_store = nil
    @durababble_schema = nil
    @durababble_backend = nil
  end

  test "requires either a store or database url" do
    assert_raises_matching(ArgumentError, /store: or database_url/) do
      Durababble::WorkerRuntime.new(workflows: {}, worker_pool: "default")
    end
  end

  test "requires rpc address configuration" do
    store = RuntimeBranchStore.new
    assert_raises_matching(ArgumentError, /rpc_host/) do
      Durababble::WorkerRuntime.new(store:, workflows: {}, worker_pool: "default", rpc_host: nil)
    end
    assert_raises_matching(ArgumentError, /rpc_port/) do
      Durababble::WorkerRuntime.new(store:, workflows: {}, worker_pool: "default", rpc_port: nil)
    end
  end

  test "is idempotent when started more than once and stopped more than once" do
    store = RuntimeBranchStore.new
    runtime = Durababble::WorkerRuntime.new(
      store:,
      workflows: {},
      worker_pool: "default",
      poll_interval: 0.01,
      migrate: false,
    )

    assert_same runtime, runtime.start
    assert_equal runtime.rpc_address, runtime.worker_id
    first_thread = runtime.wait(timeout: 0.05)
    assert(first_thread.is_a?(Thread) || first_thread.nil?, "expected wait to return a thread or nil")
    assert_same runtime, runtime.start
    assert_equal :stopped, runtime.shutdown(timeout: 1)
    assert_equal :stopped, runtime.shutdown(timeout: 1)
    assert_nil runtime.wait

    runtime.close
    assert_equal false, store.closed
  end

  test "records lease conflicts from the polling loop without releasing leases" do
    store = RuntimeBranchStore.new(tick_results: [Durababble::LeaseConflict.new("moved")])
    runtime = Durababble::WorkerRuntime.new(
      store:,
      workflows: {},
      worker_pool: "default",
      worker_id: "runtime-branch",
      poll_interval: 0.01,
      migrate: false,
    )

    runtime.start
    runtime.wait(timeout: 1)

    assert_kind_of Durababble::LeaseConflict, runtime.last_error
    assert_equal :stopped, runtime.shutdown(timeout: 1)
    assert_empty store.released
  end

  test "records unexpected polling errors and closes owned stores" do
    branch_store = RuntimeBranchStore.new(tick_results: [RuntimeError.new("boom")])
    Durababble::Store.expects(:connect).returns(branch_store)
    runtime = Durababble::WorkerRuntime.new(
      database_url: "postgresql://example.invalid/db",
      workflows: {},
      worker_pool: "default",
      poll_interval: 0.01,
      migrate: false,
    )

    runtime.start
    runtime.wait(timeout: 1)
    runtime.close

    assert_kind_of RuntimeError, runtime.last_error
    assert_equal true, branch_store.closed
  end

  test "starts a background worker and gracefully completes in-flight work during shutdown" do
    completed = Queue.new
    workflow = durababble_test_workflow("runtime-graceful") do
      test_step("finish") do |ctx|
        completed << true
        ctx.merge("done" => true)
      end
    end
    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.name => workflow },
      worker_pool: "default",
      worker_id: "runtime-a",
      poll_interval: 0.01,
      migrate: false,
    )
    runtime.start
    assert_equal true, completed.pop

    assert_equal :stopped, runtime.shutdown(timeout: 1)

    assert_equal false, runtime.running?
    assert_hash_includes(
      store.workflow(workflow_id),
      "status" => "completed",
      "result" => { "done" => true },
      "locked_by" => nil,
      "locked_until" => nil,
    )
    assert_equal ["completed"], store.steps_for(workflow_id).map { |step| step.fetch("status") }
  end

  test "stops claiming work on shutdown timeout, revokes owned leases, and lets another worker retry incomplete steps" do
    entered = Queue.new
    release_zombie = Queue.new
    attempts = Queue.new
    workflow = durababble_test_workflow("runtime-timeout") do
      test_step("maybe-stuck") do |ctx|
        attempt = attempts.length + 1
        attempts << attempt
        entered << attempt
        release_zombie.pop if attempt == 1
        ctx.merge("attempt" => attempt)
      end
    end
    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.name => workflow },
      worker_pool: "default",
      worker_id: "runtime-timeout",
      poll_interval: 0.01,
      lease_seconds: 60,
      migrate: false,
    )
    runtime.start
    assert_equal 1, entered.pop

    assert_equal :timeout, runtime.shutdown(timeout: 0.05)

    revoked = store.workflow(workflow_id)
    assert_hash_includes revoked, "status" => "pending", "locked_by" => nil, "locked_until" => nil
    assert_equal "running", store.steps_for(workflow_id).first.fetch("status")

    release_zombie << true
    runtime.wait(timeout: 1)

    recovery = Durababble::Worker.new(
      store:,
      workflows: { workflow.name => workflow },
      worker_id: "recovery",
      lease_seconds: 60,
      migrate: false,
    )
    assert_equal 1, recovery.run_until_idle(max_ticks: 2)

    row = store.workflow(workflow_id)
    assert_hash_includes row, "status" => "completed", "result" => { "attempt" => 2 }
    assert_equal ["failed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
  end

  test "shutdown timeout revokes object inbox and activation leases for immediate recovery" do
    entered = Queue.new
    release_zombie = Queue.new
    blocking_object = Class.new(Durababble::DurableObject) do
      object_type "runtime_timeout_object"

      define_method(:record) do
        entered << command_context.attempt_number
        release_zombie.pop if command_context.attempt_number == 1
        update_state({ "attempt" => command_context.attempt_number })
      end
      expose_command :record, retry: { maximum_attempts: 2, schedule: [0] }
    end

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: {},
      objects: [blocking_object],
      worker_pool: "default",
      poll_interval: 0.01,
      lease_seconds: 60,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    runtime.start
    message_id = blocking_object.tell("object-1", :record, store:)
    assert_equal(1, entered.pop)
    assert_hash_includes(store.inbox_message(message_id), "status" => "running", "locked_by" => runtime.worker_id)
    assert_hash_includes(
      store.target_activation(target_kind: "object", target_type: blocking_object.object_type, target_id: "object-1"),
      "status" => "running",
      "locked_by" => runtime.worker_id,
    )

    assert_equal(:timeout, runtime.shutdown(timeout: 0.05))
    assert_hash_includes(store.inbox_message(message_id), "status" => "pending", "locked_by" => nil, "locked_until" => nil)
    assert_hash_includes(
      store.target_activation(target_kind: "object", target_type: blocking_object.object_type, target_id: "object-1"),
      "status" => "pending",
      "locked_by" => nil,
      "locked_until" => nil,
    )

    release_zombie << true
    runtime.wait(timeout: 1)

    recovery = Durababble::Worker.new(
      store:,
      workflows: {},
      objects: [blocking_object],
      worker_id: "object-recovery",
      lease_seconds: 60,
      migrate: false,
    )
    assert_equal(1, recovery.run_until_idle(max_ticks: 2))
    assert_hash_includes(store.inbox_message(message_id), "status" => "completed", "result" => { "attempt" => 2 })
    assert_equal({ "attempt" => 2 }, store.object_state(object_type: blocking_object.object_type, object_id: "object-1"))
  ensure
    release_zombie << true if runtime&.running?
    runtime&.shutdown(timeout: 1)
  end

  test "only claims workflow names served by this runtime's worker pool" do
    pool_workflow = durababble_test_workflow("pool-a-work") do
      test_step("finish") { |ctx| ctx.merge("pool" => "a") }
    end
    other_workflow = durababble_test_workflow("pool-b-work") do
      test_step("finish") { |ctx| ctx.merge("pool" => "b") }
    end
    pool_workflow_id = store.enqueue_workflow(name: pool_workflow.name, input: {})
    other_workflow_id = store.enqueue_workflow(name: other_workflow.name, input: {})

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { pool_workflow.name => pool_workflow },
      worker_pool: "pool-a",
      worker_id: "pool-a-1",
      poll_interval: 0.01,
      migrate: false,
    )
    runtime.start
    eventually(timeout: 2) { assert_equal "completed", store.workflow(pool_workflow_id).fetch("status") }
    assert_equal :stopped, runtime.shutdown(timeout: 1)

    assert_hash_includes store.workflow(pool_workflow_id), "status" => "completed"
    assert_hash_includes store.workflow(other_workflow_id), "status" => "pending", "locked_by" => nil
  end

  test "wakes the active leaseholder runtime through DeliverMessage instead of waiting for the next poll" do
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "runtime-rpc-command"

      expose_command def approve(reason:)
        { "approved_by" => reason }
      end
    end
    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.workflow_name => workflow },
      worker_pool: "default",
      poll_interval: 10,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    runtime.start
    assert_equal(runtime.rpc_address, runtime.worker_id)

    workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})
    store.claim_workflow(workflow_id:, worker_id: runtime.rpc_address, lease_seconds: 30)
    assert_hash_includes(
      store.workflow(workflow_id),
      "status" => "running",
      "locked_by" => runtime.rpc_address,
    )

    caller_store = Durababble::Store.connect(database_url:, schema:)
    started_at = Time.now
    result = workflow.ref(workflow_id, store: caller_store).approve(reason: "operator")
    elapsed = Time.now - started_at

    assert_equal({ "approved_by" => "operator" }, result)
    assert_operator(elapsed, :<, 3)
    assert_nil(
      store.target_activation(
        target_kind: "workflow",
        target_type: workflow.workflow_name,
        target_id: workflow_id,
      ),
    )
  ensure
    caller_store&.close
    runtime&.shutdown(timeout: 1)
  end

  test "address-routed DeliverMessage wakes a non-default pool runtime" do
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "runtime-rpc-pool-command"

      expose_command def approve(reason:)
        { "approved_by" => reason }
      end
    end
    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.workflow_name => workflow },
      worker_pool: "pool-a",
      poll_interval: 10,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    runtime.start

    workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})
    store.claim_workflow(workflow_id:, worker_id: runtime.rpc_address, lease_seconds: 30)
    message_id = store.enqueue_workflow_command(
      workflow_id:,
      workflow_name: workflow.workflow_name,
      method_name: "approve",
      payload: { "method" => "approve", "args" => [], "kwargs" => { reason: "operator" } },
    )

    Durababble::Rpc::Client.new(address: runtime.rpc_address).deliver_message(
      worker_pool: "default",
      target_kind: "workflow",
      target_class: workflow.workflow_name,
      target_id: workflow_id,
    )

    eventually(timeout: 1) do
      assert_hash_includes(
        store.inbox_message(message_id),
        "status" => "completed",
        "result" => { "approved_by" => "operator" },
      )
    end
  ensure
    runtime&.shutdown(timeout: 1)
  end

  test "activation fallback forwards a failed command wake to the active leaseholder" do
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "runtime-rpc-forward-command"

      expose_command def approve(reason:)
        { "approved_by" => reason }
      end
    end
    runtime_a = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.workflow_name => workflow },
      worker_pool: "pool-a",
      poll_interval: 10,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    runtime_b_store = Durababble::Store.connect(database_url:, schema:)
    runtime_b = Durababble::WorkerRuntime.new(
      store: runtime_b_store,
      workflows: { workflow.workflow_name => workflow },
      worker_pool: "pool-a",
      poll_interval: 0.01,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    runtime_a.start
    runtime_b.start

    workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})
    store.claim_workflow(workflow_id:, worker_id: runtime_a.rpc_address, lease_seconds: 30)
    caller_store = Durababble::Store.connect(database_url:, schema:)
    caller_store.rpc_client_factory = ->(_address) { ForcedUnavailableDeliveryClient.new }
    def caller_store.wait_for_inbox_message(message_id, poll_interval: 0.01, timeout: 3)
      super
    end

    started_at = Time.now
    result = workflow.ref(workflow_id, store: caller_store).approve(reason: "operator")
    elapsed = Time.now - started_at

    assert_equal({ "approved_by" => "operator" }, result)
    assert_operator(elapsed, :<, 3)
    assert_nil(
      store.target_activation(
        target_kind: "workflow",
        target_type: workflow.workflow_name,
        target_id: workflow_id,
      ),
    )
  ensure
    caller_store&.close
    runtime_b&.shutdown(timeout: 1)
    runtime_b_store&.close
    runtime_a&.shutdown(timeout: 1)
  end

  private

  def database_url
    backend_descriptor.database_url
  end

  def runtime_store
    @runtime_store ||= Durababble::Store.connect(database_url:, schema:)
  end

  def eventually(timeout:)
    deadline = Time.now + timeout
    loop do
      yield
      return
    rescue Minitest::Assertion
      raise if Time.now >= deadline

      sleep(0.01)
    end
  end
end
