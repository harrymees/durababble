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
  end

  def setup
    @durababble_backend = durababble_store_backends.first
    @durababble_schema = "#{@durababble_backend.default_schema_prefix}_worker_lifecycle_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    @durababble_store = Durababble::Store.connect(database_url:, schema:)
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
    store.migrate!
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
    store.migrate!
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

  test "only claims workflow names served by this runtime's worker pool" do
    store.migrate!
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
