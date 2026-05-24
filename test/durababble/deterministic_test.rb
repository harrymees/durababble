# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleDeterministicTest < DurababbleTestCase
  FUZZ_SCENARIOS = [
    "workflow_durable_before_claim",
    "multi_worker_counter",
    "lease_conflict",
    "heartbeat_extension",
    "zombie_workflow_heartbeat_after_expiry",
    "step_heartbeat_cursor_recovery",
    "step_retry_policy_recovery",
    "lease_expiry",
    "completed_step_skip_after_crash",
    "incomplete_step_retry_after_crash",
    "attempt_history_append_only",
    "concurrent_signal_once",
    "timer_and_partition",
    "stale_wait_signal_terminal_workflow",
    "waits_fences_and_outbox",
    "fenced_side_effect_once",
    "outbox_lease_expiry",
    "store_fault_after_step_completed",
    "store_fault_after_wait_recorded",
    "store_fault_after_outbox_enqueue",
    "duplicate_delivery_signal_and_outbox",
    "workflow_rpc_owner_state_matrix",
    "cooperative_cancellation_cleanup",
    "grpc_workflow_rpc_response_matrix",
    "grpc_workflow_rpc_transport_fault_matrix",
    "grpc_workflow_rpc_transport_fault_reroute",
    "chaos",
  ].freeze

  CONTRACT_SCENARIOS = [
    "rpc_fault_injection",
    "grpc_service_contract",
    "grpc_wakeup_fault_matrix",
  ].freeze

  def assert_scenarios_hold(scenarios, seeds:)
    scenarios.each do |scenario|
      failures = Durababble::Deterministic.search(scenario, seeds:)
      assert_empty(failures, "scenario #{scenario.inspect} failed: #{failures.inspect}")
    end
  end

  test "proves full determinism by replaying the same scenario twice for the same seed" do
    first = Durababble::Deterministic.prove("multi_worker_counter", seed: 12_345)
    second = Durababble::Deterministic.prove("multi_worker_counter", seed: 12_345)

    assert_equal second.digest, first.digest
    assert_equal second.trace, first.trace
    assert_empty first.violations
    assert_equal 8, first.summary.fetch(:completed_workflows)
    assert_equal 8, first.trace.scan("workflow_claimed").length
  end

  test "uses the seed to control delivery order and failure schedule" do
    first = Durababble::Deterministic.prove("multi_worker_counter", seed: 1)
    second = Durababble::Deterministic.prove("multi_worker_counter", seed: 2)

    refute_equal second.digest, first.digest
    assert_empty first.violations
    assert_empty second.violations
  end

  test "simulates clients, worker nodes, virtual networking, and a fully simulated Yugabyte store" do
    result = Durababble::Deterministic.prove("waits_fences_and_outbox", seed: 99)

    assert_empty result.violations
    assert_operator result.summary.fetch(:completed_workflows), :>=, 4
    assert_equal 1, result.summary.fetch(:side_effects)
    assert_equal 1, result.summary.fetch(:processed_outbox)
    assert_includes result.trace, "network.send"
    assert_includes result.trace, "virtual_yugabyte"
  end

  test "reclaims expired workflow leases deterministically" do
    result = Durababble::Deterministic.prove("lease_expiry", seed: 7)

    assert_empty result.violations
    assert_equal 1, result.summary.fetch(:completed_workflows)
    assert_includes result.trace, "steal_expired"
  end

  test "reclaims expired outbox leases deterministically" do
    result = Durababble::Deterministic.prove("outbox_lease_expiry", seed: 8)

    assert_empty result.violations
    assert_equal 1, result.summary.fetch(:processed_outbox)
    assert_includes result.trace, "outbox_claimed"
  end

  test "models timer waits, partitions, and healing without real time or sockets" do
    result = Durababble::Deterministic.prove("timer_and_partition", seed: 101)

    assert_empty result.violations
    assert_equal 1, result.summary.fetch(:completed_workflows)
    assert_includes result.trace, "network.drop"
    assert_includes result.trace, "heal"
    assert_includes result.trace, "wait_completed"
  end

  test "models internal RPC timeout, connection error, EOF, and remote error faults" do
    result = Durababble::Deterministic.prove("rpc_fault_injection", seed: 42)

    assert_empty result.violations
    assert_includes result.trace, "rpc.timeout"
    assert_includes result.trace, "rpc.connection_error"
    assert_includes result.trace, "rpc.eof"
    assert_includes result.trace, "rpc.remote_error"
    assert_includes result.trace, "rpc.reconnect"
    assert_includes result.trace, "rpc.success"
  end

  test "models lease-routed workflow RPCs across happy, mid-flight lease change, and shutdown paths" do
    result = Durababble::Deterministic.prove("workflow_rpc_owner_state_matrix", seed: 42)

    assert_empty result.violations
    assert_includes result.trace, "workflow_rpc.lookup"
    assert_includes result.trace, "workflow_rpc.stale_rejected"
    assert_includes result.trace, "workflow_rpc.retry_success"
    assert_includes result.trace, "workflow_rpc.shutdown_rejected"
    refute_includes result.trace, "workflow_rpc.unowned_handler_ran"
    assert_includes result.trace, "workflow_rpc.no_active_holder_rejected"
    assert_includes result.trace, "workflow_rpc.internal_start_retry_success"
    assert_equal 1, result.summary.fetch(:completed_workflows)
  end

  test "models gRPC workflow RPC transport response variants" do
    contract = Durababble::Deterministic.prove("grpc_service_contract", seed: 55)
    response = Durababble::Deterministic.prove("grpc_workflow_rpc_response_matrix", seed: 53)
    transport_faults = Durababble::Deterministic.prove("grpc_workflow_rpc_transport_fault_matrix", seed: 56)
    reroute_faults = Durababble::Deterministic.prove("grpc_workflow_rpc_transport_fault_reroute", seed: 57)
    wakeup_faults = Durababble::Deterministic.prove("grpc_wakeup_fault_matrix", seed: 58)

    assert_empty contract.violations
    assert_includes contract.trace, "grpc.awaken_batch"
    assert_includes contract.trace, "grpc.evict_lease"
    assert_includes contract.trace, "grpc.deliver_message"
    assert_includes contract.trace, "grpc.call_transient_ok"
    assert_includes contract.trace, "grpc.call_object_transient_ok"
    assert_includes contract.trace, "grpc.deliver_message_stale_ack"
    assert_empty response.violations
    assert_includes response.trace, "grpc.call_transient"
    assert_includes response.trace, "grpc.lease_moved"
    assert_includes response.trace, "grpc.decode_moved"
    assert_includes response.trace, "grpc.retry_success"
    assert_includes response.trace, "grpc.unavailable"
    assert_includes response.trace, "grpc.node_unavailable_observed"
    assert_includes response.trace, "grpc.not_running"
    assert_includes response.trace, "grpc.not_running_observed"
    assert_empty transport_faults.violations
    assert_includes transport_faults.trace, "grpc.timeout"
    assert_includes transport_faults.trace, "grpc.deadline_exceeded"
    assert_includes transport_faults.trace, "grpc.rst"
    assert_includes transport_faults.trace, "grpc.eof"
    assert_includes transport_faults.trace, "grpc.response_timeout"
    assert_includes transport_faults.trace, "grpc.duplicate_response"
    assert_empty reroute_faults.violations
    assert_includes reroute_faults.trace, "grpc.transport_reroute_success"
    assert_empty wakeup_faults.violations
    assert_includes wakeup_faults.trace, "grpc.drop"
    assert_includes wakeup_faults.trace, "grpc.duplicate"
    assert_includes wakeup_faults.trace, "grpc.wakeup_fault_observed"
  end

  test "models step heartbeat cursor recovery after a crashed invocation" do
    result = Durababble::Deterministic.prove("step_heartbeat_cursor_recovery", seed: 45)

    assert_empty result.violations
    assert_includes result.trace, "step_heartbeat"
    assert_includes result.trace, "step_heartbeat_crash"
    assert_includes result.trace, "step_heartbeat_resumed"
    assert_equal 1, result.summary.fetch(:completed_workflows)
  end

  test "models configured step retry schedules across process restarts" do
    result = Durababble::Deterministic.prove("step_retry_policy_recovery", seed: 46)

    assert_empty result.violations
    assert_includes result.trace, "workflow_retry_scheduled"
    assert_includes result.trace, "step_retry_not_due"
    assert_equal 3, result.trace.scan("step_retry_attempt").length
    assert_equal 1, result.summary.fetch(:completed_workflows)
  end

  test "models cooperative cancellation cleanup" do
    result = Durababble::Deterministic.prove("cooperative_cancellation_cleanup", seed: 59)

    assert_empty result.violations
    assert_includes result.trace, "workflow_cancel_requested"
    assert_includes result.trace, "workflow_cancel_delivered"
    assert_includes result.trace, "step_heartbeat"
    assert_includes result.trace, "advance by=5"
    assert_includes result.trace, "cleanup_ran"
    assert_equal 1, result.summary.fetch(:canceled_workflows)
  end

  test "models durable store faults after writes and recovers from the persisted state" do
    step = Durababble::Deterministic.prove("store_fault_after_step_completed", seed: 47)
    wait = Durababble::Deterministic.prove("store_fault_after_wait_recorded", seed: 48)
    outbox = Durababble::Deterministic.prove("store_fault_after_outbox_enqueue", seed: 49)

    assert_empty step.violations
    assert_includes step.trace, "fault.injected"
    assert_equal 1, step.summary.fetch(:completed_workflows)
    assert_empty wait.violations
    assert_includes wait.trace, "wait_recorded"
    assert_equal 1, wait.summary.fetch(:completed_workflows)
    assert_empty outbox.violations
    assert_equal 1, outbox.summary.fetch(:processed_outbox)
  end

  test "models duplicate network delivery without duplicate wait or outbox effects" do
    result = Durababble::Deterministic.prove("duplicate_delivery_signal_and_outbox", seed: 50)

    assert_empty result.violations
    assert_includes result.trace, "network.duplicate"
    assert_equal 1, result.trace.scan("wait_completed").length
    assert_equal 1, result.summary.fetch(:processed_outbox)
  end

  test "rejects zombie heartbeats and stale wait signals in the virtual store" do
    zombie = Durababble::Deterministic.prove("zombie_workflow_heartbeat_after_expiry", seed: 51)
    stale_wait = Durababble::Deterministic.prove("stale_wait_signal_terminal_workflow", seed: 52)

    assert_empty zombie.violations
    assert_includes zombie.trace, "zombie_heartbeat_rejected"
    assert_empty stale_wait.violations
    assert_includes stale_wait.trace, "stale_wait_ignored"
    assert_equal 1, stale_wait.summary.fetch(:completed_workflows)
  end

  test "reports deterministic invariant violations for intentionally broken scenarios" do
    failures = Durababble::Deterministic.search("bug_duplicate_completion", seeds: 1..2)

    assert_equal 2, failures.length
    assert_includes failures.first.last.join, "running attempt"
    assert_includes failures.first.last.join, "live step"
  end

  test "reports deterministic store shape invariant violations" do
    failures = Durababble::Deterministic.search("bug_invalid_store_shape", seeds: 1..1)

    assert_equal 1, failures.length
    messages = failures.first.last.join("\n")
    assert_includes messages, "running workflow"
    assert_includes messages, "unknown status"
    assert_includes messages, "partial lease"
    assert_includes messages, "still locked"
    assert_includes messages, "inconsistent identity"
    assert_includes messages, "duplicate completed step positions"
    assert_includes messages, "no attempt history"
    assert_includes messages, "multiple live attempts"
    assert_includes messages, "references missing step"
    assert_includes messages, "references missing workflow"
    assert_includes messages, "non-completed step"
    assert_includes messages, "processing outbox"
  end

  test "fuzzes each unique scenario target across many deterministic seeds" do
    assert_scenarios_hold(FUZZ_SCENARIOS, seeds: 1..100)
  end

  test "runs deterministic contract scenarios once" do
    assert_scenarios_hold(CONTRACT_SCENARIOS, seeds: [1])
  end

  test "exposes deterministic utility edge cases directly" do
    rng = Durababble::Deterministic::Rng.new(1)
    assert_raises(ArgumentError) { rng.int(0) }

    trace = Durababble::Deterministic::Trace.new
    trace.event(0, "tester", "nested", z: [2, 1], a: { "b" => 2, "a" => 1 })
    assert_equal "t=000000 actor=tester event=nested a={a:1,b:2} z=[2,1]", trace.to_s

    scheduler = Durababble::Deterministic::Scheduler.new(seed: 1, trace:)
    scheduler.schedule(actor: "loop", delay: 0, name: "again") do
      scheduler.schedule(actor: "loop", delay: 0, name: "again") {}
    end
    assert_raises_matching(RuntimeError, /exceeded/) { scheduler.run(max_events: 1) }
  end
end
