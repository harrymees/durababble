# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "open3"
require "rbconfig"
require "thread"

class DurababbleHardeningTest < DurababbleTestCase
  def setup
    @durababble_backend = durababble_store_backends.first
    @durababble_schema = "#{@durababble_backend.default_schema_prefix}_hardening_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    @durababble_store = Durababble::Store.connect(database_url:, schema:)
  end

  def teardown
    @durababble_store&.drop_schema!
    @durababble_store&.close
    @durababble_store = nil
    @durababble_schema = nil
    @durababble_backend = nil
  end

  test "claims each workflow exactly once across concurrent workers using separate connections" do
    store.migrate!
    ids = 20.times.map { |i| store.enqueue_workflow(name: "counter", input: { "count" => i }) }

    # [DURABABBLE-LEASE-1] Concurrent pollers cannot obtain duplicate live ownership.
    claimed = run_threads(10) do |index, local|
      worker_claims = []
      loop do
        claim = local.claim_runnable_workflow(worker_id: "worker-#{index}", lease_seconds: 60)
        break unless claim

        worker_claims << claim.fetch("id")
      end
      worker_claims
    end.flatten

    assert_equal ids.sort, claimed.sort
    assert_equal ids.length, claimed.uniq.length
  end

  test "enforces lease ownership before resuming a workflow" do
    store.migrate!
    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 1 })
    claimed = store.claim_runnable_workflow(worker_id: "owner", lease_seconds: 60)
    assert_equal workflow_id, claimed.fetch("id")

    assert_raises(Durababble::LeaseConflict) do
      Durababble::Engine.new(store:, worker_id: "intruder").resume(counter_workflow, workflow_id:)
    end

    # [DURABABBLE-LEASE-4] A non-owner cannot commit workflow or step results.
    run = Durababble::Engine.new(store:, worker_id: "owner").resume(counter_workflow, workflow_id:)
    assert_equal "completed", run.status
  end

  test "does not rewrite an unexpired workflow lease already owned by the same worker" do
    store.migrate!
    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 1 })
    first_claim = store.claim_runnable_workflow(worker_id: "owner", lease_seconds: 60)

    second_claim = store.claim_workflow(workflow_id:, worker_id: "owner", lease_seconds: 3_600)

    assert_equal workflow_id, second_claim.fetch("id")
    assert_equal first_claim.fetch("locked_until"), second_claim.fetch("locked_until")
  end

  test "does not leave a stale running attempt when retrying a step that crashed after start" do
    store.migrate!
    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 2 })
    workflow = counter_workflow

    assert_raises(Durababble::InjectedCrash) do
      Durababble::Engine.new(store:, worker_id: "crasher", crash_after: :step_started).resume(workflow, workflow_id:)
    end

    store.steal_expired_leases!(now: Time.now + 61)
    run = Durababble::Engine.new(store:, worker_id: "recover").resume(workflow, workflow_id:)

    # [DURABABBLE-STEP-2] Stale running attempts are closed before appending the retry attempt.
    assert_equal "completed", run.status
    assert_equal ["failed", "completed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
  end

  test "preserves scalar step results and text columns that look parseable" do
    store.migrate!
    workflow = durababble_test_workflow("123") do
      test_step("false") { |_ctx| false }
    end

    run = Durababble::Engine.new(store:).run(workflow, input: { "start" => true })

    assert_equal "completed", run.status
    assert_equal false, run.result
    workflow_row = store.workflow(run.id)
    assert_equal "123", workflow_row.fetch("name")
    assert_equal "false", store.steps_for(run.id).first.fetch("name")
    assert_equal false, store.step_attempts_for(run.id).first.fetch("result")
  end

  test "runs a fenced side effect only once under concurrent callers" do
    store.migrate!
    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 1 })
    calls = Queue.new

    # [DURABABBLE-FENCE-1] The unique fence row serializes concurrent side-effect callers.
    results = run_threads(8) do |_index, local|
      local.with_fence(workflow_id:, key: "charge:concurrent", poll_interval: 0.01, timeout: 5) do
        calls << true
        sleep(0.2)
        { "charge_id" => "ch_once" }
      end
    end

    assert(results.all? { |result| result == { "charge_id" => "ch_once" } }, "expected all results to reuse the first fence result")
    assert_equal 1, calls.length
  end

  test "claims an outbox message once concurrently and reclaims it after lease expiry before ack" do
    store.migrate!
    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 1 })
    outbox_id = store.enqueue_outbox(workflow_id:, topic: "notify", payload: { "n" => 1 }, key: "notify:once")

    # [DURABABBLE-OUTBOX-1] One sender owns the outbox lease until ack or lease expiry.
    claims = run_threads(8) do |index, local|
      local.claim_outbox(worker_id: "sender-#{index}", lease_seconds: 1)&.fetch("id")
    end.compact
    assert_equal [outbox_id], claims

    sleep 1.2
    reclaimed = new_store.claim_outbox(worker_id: "recover", lease_seconds: 60)
    assert_equal outbox_id, reclaimed.fetch("id")
    store.ack_outbox(outbox_id, worker_id: "recover")
    assert_nil store.claim_outbox(worker_id: "late", lease_seconds: 60)
  end

  test "signals a waiting event once under concurrent signalers" do
    store.migrate!
    workflow = durababble_test_workflow("waiting") do
      test_step("wait") { |ctx| Durababble.wait_event("approval:#{ctx.fetch("id")}", ctx) }
      test_step("done") { |ctx| ctx.merge("done" => true) }
    end
    workflow_id = store.enqueue_workflow(name: "waiting", input: { "id" => "x" })
    Durababble::Engine.new(store:, worker_id: "worker").resume(workflow, workflow_id:)

    # [DURABABBLE-WAIT-1] Concurrent signalers race through the same pending wait row.
    signaled = run_threads(8) do |index, local|
      local.signal_event("approval:x", payload: { "signaler" => index })
    end
    assert_equal 1, signaled.sum
    assert_equal "completed", Durababble::Engine.new(store:, worker_id: "worker").resume(workflow, workflow_id:).status
    assert_equal ["completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
  end

  test "marks waiting step attempts completed when the wait is satisfied" do
    store.migrate!
    workflow = durababble_test_workflow("waiting_attempt") do
      test_step("wait") { |ctx| Durababble.wait_event("event:#{ctx.fetch("id")}", ctx) }
      test_step("done") { |ctx| ctx.merge("done" => true) }
    end
    workflow_id = store.enqueue_workflow(name: "waiting_attempt", input: { "id" => "attempt" })
    Durababble::Engine.new(store:, worker_id: "worker").resume(workflow, workflow_id:)
    assert_equal "waiting", store.step_attempts_for(workflow_id).first.fetch("status")

    store.signal_event("event:attempt", payload: { "ok" => true })
    Durababble::Engine.new(store:, worker_id: "worker").resume(workflow, workflow_id:)

    assert_equal ["completed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
  end

  test "recovers after a subprocess crashes at durable crash points" do
    store.migrate!
    script = File.expand_path("../support/crash_runner.rb", __dir__)

    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 4 })
    _stdout, _stderr, status = Open3.capture3(
      {
        "DURABABBLE_DATABASE_URL" => database_url,
        "DURABABBLE_SCHEMA" => schema,
        "DURABABBLE_WORKFLOW_ID" => workflow_id,
        "DURABABBLE_CRASH_AFTER" => "step_completed",
      },
      RbConfig.ruby,
      "-Ilib",
      script,
      chdir: File.expand_path("../..", __dir__),
    )
    refute_predicate status, :success?
    store.steal_expired_leases!(now: Time.now + 61)

    events = []
    run = Durababble::Engine.new(store:, worker_id: "recover").resume(counter_workflow(events:), workflow_id:)
    assert_equal({ "count" => 10 }, run.result)
    assert_equal ["double"], events
  end

  private

  def database_url
    backend_descriptor.database_url
  end

  def new_store
    Durababble::Store.connect(database_url:, schema:)
  end

  def counter_workflow(events: nil)
    durababble_test_workflow("counter") do
      test_step("increment") do |ctx|
        events << "increment" if events
        { "count" => ctx.fetch("count") + 1 }
      end
      test_step("double") do |ctx|
        events << "double" if events
        { "count" => ctx.fetch("count") * 2 }
      end
    end
  end

  def run_threads(count)
    barrier = Queue.new
    ready = Queue.new
    threads = count.times.map do |index|
      Thread.new do
        local = new_store
        ready << true
        barrier.pop
        yield(index, local)
      ensure
        local&.close
      end
    end
    count.times { ready.pop }
    count.times { barrier << true }
    threads.map(&:value)
  end
end
