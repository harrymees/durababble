# frozen_string_literal: true

require "spec_helper"
require "securerandom"
require "thread"

RSpec.describe Durababble::WorkerRuntime, :integration do
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

  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_worker_lifecycle_test_#{Process.pid}_#{SecureRandom.hex(4)}" }
  let(:store) { @store ||= Durababble::Store.connect(database_url:, schema:) }
  let(:runtime_store) { @runtime_store ||= Durababble::Store.connect(database_url:, schema:) }

  after do
    @runtime_store&.close if instance_variable_defined?(:@runtime_store)
    if instance_variable_defined?(:@store)
      @store.drop_schema!
      @store.close
    end
  end

  it "requires either a store or database url" do
    expect do
      described_class.new(workflows: {}, worker_pool: "default")
    end.to raise_error(ArgumentError, /store: or database_url/)
  end

  it "is idempotent when started more than once and stopped more than once" do
    runtime = described_class.new(store: RuntimeBranchStore.new, workflows: {}, worker_pool: "default", poll_interval: 0.01, migrate: false)

    expect(runtime.start).to equal(runtime)
    first_thread = runtime.wait(timeout: 0.05)
    expect(first_thread).to be_a(Thread).or be_nil
    expect(runtime.start).to equal(runtime)
    expect(runtime.shutdown(timeout: 1)).to eq(:stopped)
    expect(runtime.shutdown(timeout: 1)).to eq(:stopped)
    expect(runtime.wait).to be_nil
  end

  it "records lease conflicts from the polling loop without releasing leases" do
    store = RuntimeBranchStore.new(tick_results: [Durababble::LeaseConflict.new("moved")])
    runtime = described_class.new(store:, workflows: {}, worker_pool: "default", worker_id: "runtime-branch", poll_interval: 0.01, migrate: false)

    runtime.start
    runtime.wait(timeout: 1)

    expect(runtime.last_error).to be_a(Durababble::LeaseConflict)
    expect(runtime.shutdown(timeout: 1)).to eq(:stopped)
    expect(store.released).to be_empty
  end

  it "records unexpected polling errors and closes owned stores" do
    store = RuntimeBranchStore.new(tick_results: [RuntimeError.new("boom")])
    allow(Durababble::Store).to receive(:connect).and_return(store)
    runtime = described_class.new(database_url: "postgresql://example.invalid/db", workflows: {}, worker_pool: "default", poll_interval: 0.01, migrate: false)

    runtime.start
    runtime.wait(timeout: 1)
    runtime.close

    expect(runtime.last_error).to be_a(RuntimeError)
    expect(store.closed).to eq(true)
  end

  it "starts a background worker and gracefully completes in-flight work during shutdown" do
    store.migrate!
    completed = Queue.new
    workflow = durababble_test_workflow("runtime-graceful") do
      test_step("finish") do |ctx|
        completed << true
        ctx.merge("done" => true)
      end
    end
    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

    runtime = described_class.new(store: runtime_store, workflows: { workflow.name => workflow }, worker_pool: "default", worker_id: "runtime-a", poll_interval: 0.01, migrate: false)
    runtime.start
    expect(completed.pop).to eq(true)

    expect(runtime.shutdown(timeout: 1)).to eq(:stopped)

    expect(runtime).not_to be_running
    expect(store.workflow(workflow_id)).to include("status" => "completed", "result" => { "done" => true }, "locked_by" => nil, "locked_until" => nil)
    expect(store.steps_for(workflow_id).map { |step| step.fetch("status") }).to eq(["completed"])
  end

  it "stops claiming work on shutdown timeout, revokes owned leases, and lets another worker retry incomplete steps" do
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

    runtime = described_class.new(store: runtime_store, workflows: { workflow.name => workflow }, worker_pool: "default", worker_id: "runtime-timeout", poll_interval: 0.01, lease_seconds: 60, migrate: false)
    runtime.start
    expect(entered.pop).to eq(1)

    expect(runtime.shutdown(timeout: 0.05)).to eq(:timeout)

    revoked = store.workflow(workflow_id)
    expect(revoked).to include("status" => "pending", "locked_by" => nil, "locked_until" => nil)
    expect(store.steps_for(workflow_id).first.fetch("status")).to eq("running")

    release_zombie << true
    runtime.wait(timeout: 1)

    recovery = Durababble::Worker.new(store:, workflows: { workflow.name => workflow }, worker_id: "recovery", lease_seconds: 60, migrate: false)
    expect(recovery.run_until_idle(max_ticks: 2)).to eq(1)

    row = store.workflow(workflow_id)
    expect(row).to include("status" => "completed", "result" => { "attempt" => 2 })
    expect(store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }).to eq(%w[failed completed])
  end

  it "only claims workflow names served by this runtime's worker pool" do
    store.migrate!
    pool_workflow = durababble_test_workflow("pool-a-work") do
      test_step("finish") { |ctx| ctx.merge("pool" => "a") }
    end
    other_workflow = durababble_test_workflow("pool-b-work") do
      test_step("finish") { |ctx| ctx.merge("pool" => "b") }
    end
    pool_workflow_id = store.enqueue_workflow(name: pool_workflow.name, input: {})
    other_workflow_id = store.enqueue_workflow(name: other_workflow.name, input: {})

    runtime = described_class.new(store: runtime_store, workflows: { pool_workflow.name => pool_workflow }, worker_pool: "pool-a", worker_id: "pool-a-1", poll_interval: 0.01, migrate: false)
    runtime.start
    eventually(timeout: 2) { expect(store.workflow(pool_workflow_id).fetch("status")).to eq("completed") }
    expect(runtime.shutdown(timeout: 1)).to eq(:stopped)

    expect(store.workflow(pool_workflow_id)).to include("status" => "completed")
    expect(store.workflow(other_workflow_id)).to include("status" => "pending", "locked_by" => nil)
  end

  def eventually(timeout:)
    deadline = Time.now + timeout
    loop do
      yield
      return
    rescue RSpec::Expectations::ExpectationNotMetError
      raise if Time.now >= deadline

      sleep 0.01
    end
  end
end
