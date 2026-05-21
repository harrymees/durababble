# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Durababble deterministic simulation harness" do
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

  it "searches a range of deterministic chaos seeds for invariant violations" do
    failures = Durababble::Deterministic.search("chaos", seeds: 1..40)

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
