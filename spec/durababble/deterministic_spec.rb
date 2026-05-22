# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Durababble deterministic simulation harness" do
  SAFETY_MATRIX_SCENARIOS = {
    "workflows are durable before execution" => %w[workflow_durable_before_claim],
    "runnable work is claimable by one worker at a time" => %w[multi_worker_counter],
    "resume honors lease ownership" => %w[lease_conflict],
    "active leases can be heartbeated" => %w[heartbeat_extension],
    "step heartbeats persist opaque cursors for retry" => %w[step_heartbeat_cursor_recovery],
    "step retry policies schedule durable retries across restarts" => %w[step_retry_policy_recovery],
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
    "outbox delivery is durable and leased" => %w[outbox_lease_expiry waits_fences_and_outbox store_fault_after_outbox_enqueue],
    "store write faults leave durable state recoverable" => %w[store_fault_after_step_completed store_fault_after_wait_recorded store_fault_after_outbox_enqueue],
    "duplicate network delivery does not duplicate durable effects" => %w[duplicate_delivery_signal_and_outbox],
    "multi-row state transitions remain coherent under recovery" => %w[completed_step_skip_after_crash incomplete_step_retry_after_crash chaos],
    "internal RPC clients surface timeout, connection, EOF, and remote failures" => %w[rpc_fault_injection],
    "lease-routed workflow RPCs reject stale holders and recover after mid-flight lease changes" => %w[workflow_rpc_lease_change workflow_rpc_shutdown_midflight workflow_rpc_no_active_owner_recovery]
  }.freeze

  CRASH_MATRIX_SCENARIOS = {
    "after enqueue before claim" => %w[crash_after_enqueue],
    "after lease claim before step start" => %w[crash_after_lease_claim],
    "after step start before step completion" => %w[crash_after_step_started],
    "after step heartbeat before step completion" => %w[step_heartbeat_cursor_recovery],
    "after step failure before retry due time" => %w[step_retry_policy_recovery],
    "after step completion before workflow completion" => %w[crash_after_step_completed],
    "while waiting for an event" => %w[crash_while_waiting_event],
    "after outbox insert before delivery" => %w[crash_after_outbox_insert],
    "after outbox claim before ack" => %w[crash_after_outbox_claim],
    "during lease-routed workflow RPC" => %w[workflow_rpc_lease_change workflow_rpc_shutdown_midflight workflow_rpc_no_active_owner_recovery]
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

  it "models internal RPC timeout, connection error, EOF, and remote error faults" do
    result = Durababble::Deterministic.prove("rpc_fault_injection", seed: 42)

    expect(result.violations).to be_empty
    expect(result.trace).to include("rpc.timeout")
    expect(result.trace).to include("rpc.connection_error")
    expect(result.trace).to include("rpc.eof")
    expect(result.trace).to include("rpc.remote_error")
    expect(result.trace).to include("rpc.reconnect")
    expect(result.trace).to include("rpc.success")
  end

  it "models lease-routed workflow RPCs across happy, mid-flight lease change, and shutdown paths" do
    changed = Durababble::Deterministic.prove("workflow_rpc_lease_change", seed: 42)
    shutdown = Durababble::Deterministic.prove("workflow_rpc_shutdown_midflight", seed: 43)
    no_active = Durababble::Deterministic.prove("workflow_rpc_no_active_owner_recovery", seed: 44)

    expect(changed.violations).to be_empty
    expect(changed.trace).to include("workflow_rpc.lookup")
    expect(changed.trace).to include("workflow_rpc.stale_rejected")
    expect(changed.trace).to include("workflow_rpc.retry_success")
    expect(shutdown.violations).to be_empty
    expect(shutdown.trace).to include("workflow_rpc.shutdown_rejected")
    expect(shutdown.trace).not_to include("workflow_rpc.unowned_handler_ran")
    expect(no_active.violations).to be_empty
    expect(no_active.trace).to include("workflow_rpc.no_active_holder_rejected")
    expect(no_active.trace).to include("workflow_rpc.internal_start_retry_success")
    expect(no_active.summary.fetch(:completed_workflows)).to eq(0)
  end

  it "models step heartbeat cursor recovery after a crashed invocation" do
    result = Durababble::Deterministic.prove("step_heartbeat_cursor_recovery", seed: 45)

    expect(result.violations).to be_empty
    expect(result.trace).to include("step_heartbeat")
    expect(result.trace).to include("step_heartbeat_crash")
    expect(result.trace).to include("step_heartbeat_resumed")
    expect(result.summary.fetch(:completed_workflows)).to eq(1)
  end

  it "models configured step retry schedules across process restarts" do
    result = Durababble::Deterministic.prove("step_retry_policy_recovery", seed: 46)

    expect(result.violations).to be_empty
    expect(result.trace).to include("workflow_retry_scheduled")
    expect(result.trace).to include("step_retry_not_due")
    expect(result.trace.scan("step_retry_attempt").length).to eq(3)
    expect(result.summary.fetch(:completed_workflows)).to eq(1)
  end

  it "models durable store faults after writes and recovers from the persisted state" do
    step = Durababble::Deterministic.prove("store_fault_after_step_completed", seed: 47)
    wait = Durababble::Deterministic.prove("store_fault_after_wait_recorded", seed: 48)
    outbox = Durababble::Deterministic.prove("store_fault_after_outbox_enqueue", seed: 49)

    expect(step.violations).to be_empty
    expect(step.trace).to include("fault.injected")
    expect(step.summary.fetch(:completed_workflows)).to eq(1)
    expect(wait.violations).to be_empty
    expect(wait.trace).to include("wait_recorded")
    expect(wait.summary.fetch(:completed_workflows)).to eq(1)
    expect(outbox.violations).to be_empty
    expect(outbox.summary.fetch(:processed_outbox)).to eq(1)
  end

  it "models duplicate network delivery without duplicate wait or outbox effects" do
    result = Durababble::Deterministic.prove("duplicate_delivery_signal_and_outbox", seed: 50)

    expect(result.violations).to be_empty
    expect(result.trace).to include("network.duplicate")
    expect(result.trace.scan("wait_completed").length).to eq(1)
    expect(result.summary.fetch(:processed_outbox)).to eq(1)
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
