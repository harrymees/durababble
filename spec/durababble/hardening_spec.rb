# frozen_string_literal: true

require "open3"
require "spec_helper"
require "thread"
require "timeout"

RSpec.describe "Durababble hardened durability and concurrency", :integration do
  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_hardening_test_#{Process.pid}_#{object_id.abs}" }
  let(:store) { Durababble::Store.connect(database_url:, schema:) }

  after do
    store&.drop_schema!
    store&.close
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

  def run_threads(count, &block)
    barrier = Queue.new
    ready = Queue.new
    threads = count.times.map do |index|
      Thread.new do
        local = new_store
        ready << true
        barrier.pop
        block.call(index, local)
      ensure
        local&.close
      end
    end
    count.times { ready.pop }
    count.times { barrier << true }
    threads.map(&:value)
  end

  it "claims each workflow exactly once across concurrent workers using separate connections" do
    store.migrate!
    ids = 20.times.map { |i| store.enqueue_workflow(name: "counter", input: { "count" => i }) }

    claimed = run_threads(10) do |index, local|
      worker_claims = []
      loop do
        claim = local.claim_runnable_workflow(worker_id: "worker-#{index}", lease_seconds: 60)
        break unless claim
        worker_claims << claim.fetch("id")
      end
      worker_claims
    end.flatten

    expect(claimed.sort).to eq(ids.sort)
    expect(claimed.uniq.length).to eq(ids.length)
  end

  it "enforces lease ownership before resuming a workflow" do
    store.migrate!
    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 1 })
    claimed = store.claim_runnable_workflow(worker_id: "owner", lease_seconds: 60)
    expect(claimed.fetch("id")).to eq(workflow_id)

    expect do
      Durababble::Engine.new(store:, worker_id: "intruder").resume(counter_workflow, workflow_id:)
    end.to raise_error(Durababble::LeaseConflict)

    run = Durababble::Engine.new(store:, worker_id: "owner").resume(counter_workflow, workflow_id:)
    expect(run.status).to eq("completed")
  end

  it "does not rewrite an unexpired workflow lease already owned by the same worker" do
    store.migrate!
    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 1 })
    first_claim = store.claim_runnable_workflow(worker_id: "owner", lease_seconds: 60)

    second_claim = store.claim_workflow(workflow_id:, worker_id: "owner", lease_seconds: 3_600)

    expect(second_claim.fetch("id")).to eq(workflow_id)
    expect(second_claim.fetch("locked_until")).to eq(first_claim.fetch("locked_until"))
  end

  it "does not leave a stale running attempt when retrying a step that crashed after start" do
    store.migrate!
    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 2 })
    workflow = counter_workflow

    expect do
      Durababble::Engine.new(store:, worker_id: "crasher", crash_after: :step_started).resume(workflow, workflow_id:)
    end.to raise_error(Durababble::InjectedCrash)

    store.steal_expired_leases!(now: Time.now + 61)
    run = Durababble::Engine.new(store:, worker_id: "recover").resume(workflow, workflow_id:)

    expect(run.status).to eq("completed")
    expect(store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }).to eq(%w[failed completed completed])
  end

  it "preserves scalar step results and text columns that look parseable" do
    store.migrate!
    workflow = durababble_test_workflow("123") do
      test_step("false") { |_ctx| false }
    end

    run = Durababble::Engine.new(store:).run(workflow, input: { "start" => true })

    expect(run.status).to eq("completed")
    expect(run.result).to eq(false)
    workflow_row = store.workflow(run.id)
    expect(workflow_row.fetch("name")).to eq("123")
    expect(store.steps_for(run.id).first.fetch("name")).to eq("false")
    expect(store.step_attempts_for(run.id).first.fetch("result")).to eq(false)
  end

  it "runs a fenced side effect only once under concurrent callers" do
    store.migrate!
    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 1 })
    calls = Queue.new

    results = run_threads(8) do |_index, local|
      local.with_fence(workflow_id:, key: "charge:concurrent", poll_interval: 0.01, timeout: 5) do
        calls << true
        sleep 0.2
        { "charge_id" => "ch_once" }
      end
    end

    expect(results).to all(eq({ "charge_id" => "ch_once" }))
    expect(calls.length).to eq(1)
  end

  it "claims an outbox message once concurrently and reclaims it after lease expiry before ack" do
    store.migrate!
    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 1 })
    outbox_id = store.enqueue_outbox(workflow_id:, topic: "notify", payload: { "n" => 1 }, key: "notify:once")

    claims = run_threads(8) do |index, local|
      local.claim_outbox(worker_id: "sender-#{index}", lease_seconds: 1)&.fetch("id")
    end.compact
    expect(claims).to eq([outbox_id])

    sleep 1.2
    reclaimed = new_store.claim_outbox(worker_id: "recover", lease_seconds: 60)
    expect(reclaimed.fetch("id")).to eq(outbox_id)
    store.ack_outbox(outbox_id, worker_id: "recover")
    expect(store.claim_outbox(worker_id: "late", lease_seconds: 60)).to be_nil
  end

  it "signals a waiting event once under concurrent signalers" do
    store.migrate!
    workflow = durababble_test_workflow("waiting") do
      test_step("wait") { |ctx| Durababble.wait_event("approval:#{ctx.fetch("id")}", ctx) }
      test_step("done") { |ctx| ctx.merge("done" => true) }
    end
    workflow_id = store.enqueue_workflow(name: "waiting", input: { "id" => "x" })
    Durababble::Engine.new(store:, worker_id: "worker").resume(workflow, workflow_id:)

    signaled = run_threads(8) do |index, local|
      local.signal_event("approval:x", payload: { "signaler" => index })
    end
    expect(signaled.sum).to eq(1)
    expect(Durababble::Engine.new(store:, worker_id: "worker").resume(workflow, workflow_id:).status).to eq("completed")
    expect(store.waits_for(workflow_id).map { |w| w.fetch("status") }).to eq(["completed"])
  end

  it "marks waiting step attempts completed when the wait is satisfied" do
    store.migrate!
    workflow = durababble_test_workflow("waiting_attempt") do
      test_step("wait") { |ctx| Durababble.wait_event("event:#{ctx.fetch("id")}", ctx) }
      test_step("done") { |ctx| ctx.merge("done" => true) }
    end
    workflow_id = store.enqueue_workflow(name: "waiting_attempt", input: { "id" => "attempt" })
    Durababble::Engine.new(store:, worker_id: "worker").resume(workflow, workflow_id:)
    expect(store.step_attempts_for(workflow_id).first.fetch("status")).to eq("waiting")

    store.signal_event("event:attempt", payload: { "ok" => true })
    Durababble::Engine.new(store:, worker_id: "worker").resume(workflow, workflow_id:)

    expect(store.step_attempts_for(workflow_id).map { |a| a.fetch("status") }).to eq(%w[completed completed])
  end

  it "recovers after a subprocess crashes at durable crash points" do
    store.migrate!
    script = File.expand_path("../support/crash_runner.rb", __dir__)

    workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 4 })
    _stdout, _stderr, status = Open3.capture3(
      { "DURABABBLE_DATABASE_URL" => database_url, "DURABABBLE_SCHEMA" => schema, "DURABABBLE_WORKFLOW_ID" => workflow_id, "DURABABBLE_CRASH_AFTER" => "step_completed" },
      "mise", "exec", "--", "ruby", script,
      chdir: File.expand_path("../..", __dir__)
    )
    expect(status).not_to be_success
    store.steal_expired_leases!(now: Time.now + 61)

    events = []
    run = Durababble::Engine.new(store:, worker_id: "recover").resume(counter_workflow(events:), workflow_id:)
    expect(run.result).to eq({ "count" => 10 })
    expect(events).to eq(["double"])
  end
end
