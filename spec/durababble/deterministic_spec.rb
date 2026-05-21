# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Durababble deterministic simulation harness" do
  SAFETY_MATRIX_SCENARIOS = {
    "workflows are durable before execution" => %w[workflow_durable_before_claim],
    "runnable work is claimable by one worker at a time" => %w[multi_worker_counter],
    "resume honors lease ownership" => %w[lease_conflict],
    "active leases can be heartbeated" => %w[heartbeat_extension],
    "expired leases can be recovered" => %w[lease_expiry],
    "completed steps are not re-executed on resume" => %w[completed_step_skip_after_crash],
    "incomplete steps are retried" => %w[incomplete_step_retry_after_crash],
    "step attempts are append-only" => %w[attempt_history_append_only],
    "waiting attempts complete when signaled" => %w[concurrent_signal_once],
    "timer waits survive process exit" => %w[timer_and_partition],
    "event waits survive process exit" => %w[concurrent_signal_once],
    "signaled waits resume with payload" => %w[waits_fences_and_outbox concurrent_signal_once],
    "concurrent signalers wake a wait once" => %w[concurrent_signal_once],
    "side effects can be fenced by key" => %w[fenced_side_effect_once waits_fences_and_outbox],
    "outbox delivery is durable and leased" => %w[outbox_lease_expiry waits_fences_and_outbox],
    "multi-row state transitions remain coherent under recovery" => %w[completed_step_skip_after_crash incomplete_step_retry_after_crash chaos]
  }.freeze

  CRASH_MATRIX_SCENARIOS = {
    "after enqueue before claim" => %w[crash_after_enqueue],
    "after lease claim before step start" => %w[crash_after_lease_claim],
    "after step start before step completion" => %w[crash_after_step_started],
    "after step completion before workflow completion" => %w[crash_after_step_completed],
    "while waiting for an event" => %w[crash_while_waiting_event],
    "after outbox insert before delivery" => %w[crash_after_outbox_insert],
    "after outbox claim before ack" => %w[crash_after_outbox_claim]
  }.freeze

  def expect_scenarios_hold(matrix, seeds: 1..100)
    matrix.each do |condition, scenarios|
      scenarios.each do |scenario|
        failures = Durababble::Deterministic.search(scenario, seeds:)
        expect(failures).to be_empty, "#{condition.inspect} failed in scenario #{scenario.inspect}: #{failures.inspect}"
      end
    end
  end

  it "proves full determinism by replaying the same scenario twice for the same seed" do
    first = Durababble::Deterministic.prove("multi_worker_counter", seed: 12_345)
    second = Durababble::Deterministic.prove("multi_worker_counter", seed: 12_345)

    expect(first.digest).to eq(second.digest)
    expect(first.trace).to eq(second.trace)
    expect(first.violations).to be_empty
    expect(first.summary.fetch(:completed_workflows)).to eq(8)
    expect(first.trace.scan("workflow_claimed").length).to eq(8)
  end

  it "uses the seed to control delivery order and failure schedule" do
    first = Durababble::Deterministic.prove("multi_worker_counter", seed: 1)
    second = Durababble::Deterministic.prove("multi_worker_counter", seed: 2)

    expect(first.digest).not_to eq(second.digest)
    expect(first.violations).to be_empty
    expect(second.violations).to be_empty
  end

  it "simulates clients, worker nodes, virtual networking, and a fully simulated Yugabyte store" do
    result = Durababble::Deterministic.prove("waits_fences_and_outbox", seed: 99)

    expect(result.violations).to be_empty
    expect(result.summary.fetch(:completed_workflows)).to be >= 4
    expect(result.summary.fetch(:side_effects)).to eq(1)
    expect(result.summary.fetch(:processed_outbox)).to eq(1)
    expect(result.trace).to include("network.send")
    expect(result.trace).to include("virtual_yugabyte")
  end

  it "reclaims expired workflow leases deterministically" do
    result = Durababble::Deterministic.prove("lease_expiry", seed: 7)

    expect(result.violations).to be_empty
    expect(result.summary.fetch(:completed_workflows)).to eq(1)
    expect(result.trace).to include("steal_expired")
  end

  it "reclaims expired outbox leases deterministically" do
    result = Durababble::Deterministic.prove("outbox_lease_expiry", seed: 8)

    expect(result.violations).to be_empty
    expect(result.summary.fetch(:processed_outbox)).to eq(1)
    expect(result.trace).to include("outbox_claimed")
  end

  it "models timer waits, partitions, and healing without real time or sockets" do
    result = Durababble::Deterministic.prove("timer_and_partition", seed: 101)

    expect(result.violations).to be_empty
    expect(result.summary.fetch(:completed_workflows)).to eq(1)
    expect(result.trace).to include("network.drop")
    expect(result.trace).to include("heal")
    expect(result.trace).to include("wait_completed")
  end

  it "reports deterministic invariant violations for intentionally broken scenarios" do
    failures = Durababble::Deterministic.search("bug_duplicate_completion", seeds: 1..2)

    expect(failures.length).to eq(2)
    expect(failures.first.last.join).to include("running attempt")
  end

  it "proves every safety matrix condition across many deterministic seeds" do
    expect_scenarios_hold(SAFETY_MATRIX_SCENARIOS, seeds: 1..100)
  end

  it "proves every crash matrix recovery condition across many deterministic seeds" do
    expect_scenarios_hold(CRASH_MATRIX_SCENARIOS, seeds: 1..100)
  end

  it "searches a range of deterministic chaos seeds for invariant violations" do
    failures = Durababble::Deterministic.search("chaos", seeds: 1..100)

    expect(failures).to be_empty
  end

  it "exposes deterministic utility edge cases directly" do
    rng = Durababble::Deterministic::Rng.new(1)
    expect { rng.int(0) }.to raise_error(ArgumentError)

    trace = Durababble::Deterministic::Trace.new
    trace.event(0, "tester", "nested", z: [2, 1], a: { "b" => 2, "a" => 1 })
    expect(trace.to_s).to eq('t=000000 actor=tester event=nested a={a:1,b:2} z=[2,1]')

    scheduler = Durababble::Deterministic::Scheduler.new(seed: 1, trace:)
    scheduler.schedule(actor: "loop", delay: 0, name: "again") { scheduler.schedule(actor: "loop", delay: 0, name: "again") {} }
    expect { scheduler.run(max_events: 1) }.to raise_error(/exceeded/)
  end
end
