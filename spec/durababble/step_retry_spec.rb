# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe "Durababble step retry policies", :integration do
  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_step_retry_test_#{Process.pid}_#{SecureRandom.hex(4)}" }
  let(:store) { Durababble::Store.connect(database_url:, schema:) }

  after do
    store&.drop_schema!
    store&.close
  end

  it "retries a failed step according to a Ruby-ified Temporal exponential policy" do
    store.migrate!
    attempts = 0
    workflow = durababble_test_workflow("retry-exponential") do
      test_step "flaky",
           retry_policy: {
             initial_interval: 2,
             backoff_coefficient: 3,
             maximum_interval: 5,
             maximum_attempts: 4
           } do |ctx|
        attempts += 1
        raise "boom #{attempts}" if attempts < 4

        ctx.merge("attempts" => attempts)
      end
    end

    worker = Durababble::Worker.new(store:, workflows: { workflow.name => workflow }, worker_id: "retry-worker", migrate: false)
    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

    expect(worker.tick).to eq(:worked)
    expect(store.workflow(workflow_id)).to include("status" => "pending", "locked_by" => nil)
    expect(store.workflow(workflow_id).fetch("next_run_at")).not_to be_nil
    expect(worker.tick).to eq(:idle)

    store.make_workflow_due!(workflow_id, now: Time.now + 3)
    expect(worker.tick).to eq(:worked)
    store.make_workflow_due!(workflow_id, now: Time.now + 7)
    expect(worker.tick).to eq(:worked)
    store.make_workflow_due!(workflow_id, now: Time.now + 10)
    expect(worker.tick).to eq(:worked)

    expect(store.workflow(workflow_id)).to include("status" => "completed", "result" => { "attempts" => 4 })
    expect(store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }).to eq(%w[failed failed failed completed])
  end

  it "persists retry schedule across lease expiry and process restart" do
    store.migrate!
    attempts = 0
    workflow = durababble_test_workflow("retry-restart") do
      test_step "flaky", retry_policy: { initial_interval: 5, maximum_attempts: 2 } do |ctx|
        attempts += 1
        raise "temporary" if attempts == 1

        ctx.merge("recovered" => true)
      end
    end
    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

    first_worker = Durababble::Worker.new(store:, workflows: { workflow.name => workflow }, worker_id: "first", lease_seconds: 60, migrate: false)
    expect(first_worker.tick).to eq(:worked)
    scheduled = store.workflow(workflow_id)
    expect(scheduled).to include("status" => "pending", "locked_by" => nil)
    expect(scheduled.fetch("next_run_at")).not_to be_nil

    restarted_store = Durababble::Store.connect(database_url:, schema:)
    begin
      restarted_worker = Durababble::Worker.new(store: restarted_store, workflows: { workflow.name => workflow }, worker_id: "second", lease_seconds: 60, migrate: false)
      expect(restarted_worker.tick).to eq(:idle)
      restarted_store.make_workflow_due!(workflow_id, now: Time.now + 6)
      expect(restarted_worker.tick).to eq(:worked)
    ensure
      restarted_store.close
    end

    expect(store.workflow(workflow_id)).to include("status" => "completed", "result" => { "recovered" => true })
  end

  it "bubbles the final failure to the workflow when attempts are exhausted" do
    store.migrate!
    workflow = durababble_test_workflow("retry-exhausted") do
      test_step "always-fails", retry_policy: { initial_interval: 1, maximum_attempts: 2 } do |_ctx|
        raise ArgumentError, "still bad"
      end
    end
    worker = Durababble::Worker.new(store:, workflows: { workflow.name => workflow }, worker_id: "retry-worker", migrate: false)
    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

    expect(worker.tick).to eq(:worked)
    store.make_workflow_due!(workflow_id, now: Time.now + 2)
    expect(worker.tick).to eq(:worked)

    failed = store.workflow(workflow_id)
    expect(failed.fetch("status")).to eq("failed")
    expect(failed.fetch("error")).to include("ArgumentError: still bad")
    expect(failed.fetch("next_run_at")).to be_nil
    expect(store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }).to eq(%w[failed failed])
  end

  it "does not retry non-retryable errors" do
    store.migrate!
    workflow = durababble_test_workflow("retry-nonretryable") do
      test_step "bad-input", retry_policy: { maximum_attempts: 5, non_retryable_errors: [ArgumentError] } do |_ctx|
        raise ArgumentError, "invalid"
      end
    end
    worker = Durababble::Worker.new(store:, workflows: { workflow.name => workflow }, worker_id: "retry-worker", migrate: false)
    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

    expect(worker.tick).to eq(:worked)

    failed = store.workflow(workflow_id)
    expect(failed.fetch("status")).to eq("failed")
    expect(store.step_attempts_for(workflow_id).length).to eq(1)
  end

  it "supports an explicit retry schedule array before falling back to capped exponential backoff" do
    policy = Durababble::RetryPolicy.new(initial_interval: 10, backoff_coefficient: 2, maximum_interval: 30, maximum_attempts: 5, schedule: [1, 4])

    expect((1..4).map { |attempt| policy.delay_for_attempt(attempt) }).to eq([1, 4, 30, 30])
  end

  it "normalizes retry policy edge cases" do
    existing = Durababble::RetryPolicy.new(maximum_attempts: nil, initial_interval: "2")

    expect(Durababble::RetryPolicy.from(existing)).to equal(existing)
    expect(Durababble::RetryPolicy.from(nil).retryable?(RuntimeError.new("boom"), attempt_number: 1)).to eq(false)
    expect(existing.retryable?(RuntimeError.new("boom"), attempt_number: 10_000)).to eq(true)
    expect(existing.delay_for_attempt(2)).to eq(4.0)
    expect { Durababble::RetryPolicy.new(initial_interval: Object.new) }.to raise_error(ArgumentError)
  end
end
