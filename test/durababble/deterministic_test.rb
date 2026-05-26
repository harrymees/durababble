# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

require_relative "../support/deterministic"

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
    "concurrent_timer_wake_once",
    "timer_and_partition",
    "stale_wait_timer_terminal_workflow",
    "waits_fences_and_outbox",
    "fenced_side_effect_once",
    "fence_holder_crash_and_reclaim",
    "step_failure_crash_matrix",
    "outbox_lease_expiry",
    "store_fault_after_step_completed",
    "store_fault_after_wait_recorded",
    "store_fault_after_outbox_enqueue",
    "duplicate_delivery_timer_and_outbox",
    "workflow_rpc_owner_state_matrix",
    "cooperative_cancellation_cleanup",
    "workflow_command_async_delivery",
    "workflow_command_delivery_crash_recovery",
    "workflow_command_delivery_crash_matrix",
    "workflow_command_claim_window_crash_matrix",
    "workflow_command_retry_then_complete",
    "workflow_command_terminal_failure",
    "workflow_command_delivery_to_terminal_workflow",
    "object_command_failure_exhaustion",
    "object_command_claim_contention",
    "object_command_crash_fuzz",
    "object_command_state_crash_fuzz",
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

  # The full fuzz sweep (every FUZZ_SCENARIOS target × these seeds) dominates the
  # DST suite's wall time. Locally it runs in full; CI sharding sets DST_FUZZ_SEEDS
  # (e.g. "1..50") so parallel jobs each cover a slice of the seed space without
  # dropping any scenario. The default keeps the complete 1..100 sweep for anyone
  # running `rake dst` directly.
  DEFAULT_FUZZ_SEEDS = (1..100)

  def fuzz_seeds
    spec = ENV.fetch("DST_FUZZ_SEEDS", nil)
    return DEFAULT_FUZZ_SEEDS if spec.nil? || spec.empty?

    first, last = spec.split("..", 2).map { |part| Integer(part.strip) }
    raise ArgumentError, "invalid DST_FUZZ_SEEDS=#{spec.inspect}" if last.nil?

    first..last
  end

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
    assert_includes result.trace, "store"
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

  test "deterministic store keeps command history coherent for deferred waits and cancellation" do
    trace = Durababble::Deterministic::Trace.new
    scheduler = Durababble::Deterministic::Scheduler.new(seed: 61, trace:)
    store = Durababble::Deterministic::DeterministicSqliteStore.build(scheduler:)

    workflow_id = store.enqueue_workflow(name: "virtual-history", input: {})
    store.claim_workflow(workflow_id:, worker_id: "worker-a", lease_seconds: 10)
    store.record_step_scheduled(workflow_id:, command_id: 0, name: "sleep", args: [10])
    store.record_step_started(workflow_id:, command_id: 0, name: "sleep")
    store.record_wait(
      workflow_id:,
      command_id: 0,
      name: "sleep",
      wait_request: Durababble.wait_until(store.current_time + 10, { "started" => true }),
      suspend_workflow: false,
    )

    assert_equal "running", store.workflow(workflow_id).fetch("status")
    assert_equal 1, store.wake_due_timers(now: scheduler.time + 11)
    assert_equal "running", store.workflow(workflow_id).fetch("status")

    store.record_step_scheduled(workflow_id:, command_id: 1, name: "cancelable", args: ["work"])
    store.record_step_started(workflow_id:, command_id: 1, name: "cancelable")
    store.record_step_canceled(workflow_id:, position: 1, error: "workflow cancellation requested")

    assert_equal(
      ["step_scheduled", "step_started", "step_waiting", "step_completed", "step_scheduled", "step_started", "step_canceled"],
      store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") },
    )
    assert_equal ["completed", "canceled"], store.steps_for(workflow_id).map { |step| step.fetch("status") }

    pending_id = store.enqueue_workflow(name: "cancel-pending", input: {})
    first = store.request_workflow_cancellation(workflow_id: pending_id, reason: "first")
    second = store.request_workflow_cancellation(workflow_id: pending_id, reason: "second")
    store.mark_workflow_cancellation_delivered(workflow_id: pending_id)
    store.mark_workflow_cancellation_delivered(workflow_id: "missing-cancel")

    assert_equal "canceling", first.fetch("status")
    assert_equal "canceling", second.fetch("status")
    assert_equal "first", store.workflow_cancellation(pending_id).fetch("reason")
    refute_nil store.workflow_cancellation(pending_id).fetch("delivered_at")
    assert_equal "running", store.claim_workflow(workflow_id: pending_id, worker_id: "worker-b", lease_seconds: 10).fetch("status")

    retry_id = store.enqueue_workflow(name: "cancel-retry", input: {})
    store.claim_workflow(workflow_id: retry_id, worker_id: "worker-c", lease_seconds: 10)
    store.request_workflow_cancellation(workflow_id: retry_id, reason: "retry canceled")
    store.schedule_workflow_retry(workflow_id: retry_id, worker_id: "worker-c", run_at: scheduler.time + 30)
    assert_equal "canceling", store.workflow(retry_id).fetch("status")

    expired_id = store.enqueue_workflow(name: "cancel-expired", input: {})
    store.claim_workflow(workflow_id: expired_id, worker_id: "worker-d", lease_seconds: 1)
    store.request_workflow_cancellation(workflow_id: expired_id, reason: "expired canceled")
    scheduler.advance(2)
    assert_equal 1, store.steal_expired_leases!
    assert_equal "canceling", store.workflow(expired_id).fetch("status")

    completed_id = store.enqueue_workflow(name: "cancel-terminal", input: {})
    store.complete_workflow(completed_id, result: { "done" => true })
    assert_equal "completed", store.request_workflow_cancellation(workflow_id: completed_id, reason: "too late").fetch("status")
    assert_nil store.workflow_cancellation(completed_id)
  end

  test "deterministic store no-op lease and queue paths stay inert" do
    trace = Durababble::Deterministic::Trace.new
    scheduler = Durababble::Deterministic::Scheduler.new(seed: 62, trace:)
    store = Durababble::Deterministic::DeterministicSqliteStore.build(scheduler:)

    workflow_id = store.enqueue_workflow(name: "virtual-noops", input: {})

    assert_nil store.heartbeat_step(workflow_id:, command_id: 0, worker_id: "worker-a", lease_seconds: 10, cursor: "ignored")
    assert_nil store.step_heartbeat_cursor(workflow_id:, command_id: 0)
    assert(store.suspend_workflow(workflow_id:))
    assert_nil store.claim_outbox(worker_id: "sender", lease_seconds: 10)

    outbox_id = store.enqueue_outbox(workflow_id:, topic: "topic", payload: {}, key: "key")
    assert_equal 0, store.ack_outbox(outbox_id, worker_id: "wrong-sender").affected_rows
    assert_equal "pending", store.outbox_message(outbox_id).fetch("status")

    store.claim_workflow(workflow_id:, worker_id: "worker-a", lease_seconds: 10)

    assert_nil store.heartbeat_step(workflow_id:, command_id: 99, worker_id: "worker-a", lease_seconds: 10, cursor: "missing")
    assert_nil store.schedule_workflow_retry(workflow_id:, worker_id: "worker-b", run_at: scheduler.time + 10)
    refute store.suspend_workflow(workflow_id:, worker_id: "worker-b")
    assert(store.suspend_workflow(workflow_id:, worker_id: "worker-a"))
    assert_equal "pending", store.workflow(workflow_id).fetch("status")
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
    result = Durababble::Deterministic.prove("duplicate_delivery_timer_and_outbox", seed: 50)

    assert_empty result.violations
    assert_includes result.trace, "network.duplicate"
    assert_equal 1, result.trace.scan("wait_completed").length
    assert_equal 1, result.summary.fetch(:processed_outbox)
  end

  test "rejects zombie heartbeats and stale timer waits in the virtual store" do
    zombie = Durababble::Deterministic.prove("zombie_workflow_heartbeat_after_expiry", seed: 51)
    stale_wait = Durababble::Deterministic.prove("stale_wait_timer_terminal_workflow", seed: 52)

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

  test "flags a stuck fence whose expired lease was never reclaimed" do
    failures = Durababble::Deterministic.search("bug_stuck_fence", seeds: 1..1)

    assert_equal 1, failures.length
    assert_includes failures.first.last.join("\n"), "stuck fence"
  end

  test "flags a stuck outbox message whose expired lease was never reclaimed" do
    failures = Durababble::Deterministic.search("bug_stuck_outbox", seeds: 1..1)

    assert_equal 1, failures.length
    assert_includes failures.first.last.join("\n"), "stuck outbox"
  end

  test "flags a stuck inbox message whose expired lease was never reclaimed" do
    failures = Durababble::Deterministic.search("bug_stuck_inbox", seeds: 1..1)

    assert_equal 1, failures.length
    assert_includes failures.first.last.join("\n"), "stuck inbox"
  end

  test "flags a stuck target activation whose expired lease was never reclaimed" do
    failures = Durababble::Deterministic.search("bug_stuck_activation", seeds: 1..1)

    assert_equal 1, failures.length
    assert_includes failures.first.last.join("\n"), "stuck target activation"
  end

  test "flags an activatable inbox head with no target activation as a lost wakeup" do
    failures = Durababble::Deterministic.search("bug_lost_wakeup", seeds: 1..1)

    assert_equal 1, failures.length
    assert_includes failures.first.last.join("\n"), "lost wakeup"
  end

  test "flags a target activation over an empty mailbox as an orphaned wakeup" do
    failures = Durababble::Deterministic.search("bug_orphaned_activation", seeds: 1..1)

    assert_equal 1, failures.length
    assert_includes failures.first.last.join("\n"), "orphaned wakeup"
  end

  test "delivers async workflow commands exactly once and retires the wakeup row" do
    result = Durababble::Deterministic.prove("workflow_command_async_delivery", seed: 7)

    assert_empty result.violations
  end

  test "reclaims a crashed delivery worker's leases and delivers each command exactly once" do
    result = Durababble::Deterministic.prove("workflow_command_delivery_crash_recovery", seed: 7)

    assert_empty result.violations
    assert_includes result.trace, "delivery_worker_crashed"
  end

  test "resumes command delivery after an injected post-commit crash without losing or re-delivering" do
    result = Durababble::Deterministic.prove("workflow_command_delivery_crash_matrix", seed: 7)

    assert_empty result.violations
    assert_includes result.trace, "fault.injected"
    assert_includes result.trace, "delivery_crashed_after_commit"
  end

  test "dead-letters a terminally-failed command while completing the rest" do
    result = Durababble::Deterministic.prove("workflow_command_terminal_failure", seed: 7)

    assert_empty result.violations
  end

  test "re-delivers and completes a command exactly once after a transient retry" do
    result = Durababble::Deterministic.prove("workflow_command_retry_then_complete", seed: 7)

    assert_empty result.violations
  end

  test "dead-letters an object command once it exhausts its attempts" do
    result = Durababble::Deterministic.prove("object_command_failure_exhaustion", seed: 7)

    assert_empty result.violations
  end

  test "blocks a second worker from stealing a live object-command lease" do
    result = Durababble::Deterministic.prove("object_command_claim_contention", seed: 7)

    assert_empty result.violations
    assert_includes result.trace, "b_blocked_by_lease"
    refute_includes result.trace, "b_stole_live_lease"
  end

  test "drives an object command to a consistent terminal state through store crashes" do
    result = Durababble::Deterministic.prove("object_command_crash_fuzz", seed: 7)

    assert_empty result.violations
    assert_includes result.trace, "object_command_crashed"
  end

  test "applies durable object state exactly once per command through store crashes" do
    result = Durababble::Deterministic.prove("object_command_state_crash_fuzz", seed: 7)

    assert_empty result.violations
    assert_includes result.trace, "object_state_crashed"
  end

  test "dead-letters queued commands when their workflow terminated before delivery" do
    result = Durababble::Deterministic.prove("workflow_command_delivery_to_terminal_workflow", seed: 7)

    assert_empty result.violations
  end

  test "reclaims durably-leased rows after an injected crash inside a claim window" do
    result = Durababble::Deterministic.prove("workflow_command_claim_window_crash_matrix", seed: 7)

    assert_empty result.violations
    assert_includes result.trace, "fault.injected"
    assert_includes result.trace, "claim_window_crashed"
  end

  test "reclaims a fence abandoned by a crashed holder and runs the effect exactly once" do
    result = Durababble::Deterministic.prove("fence_holder_crash_and_reclaim", seed: 7)

    assert_empty result.violations
    assert_equal 1, result.summary.fetch(:side_effects)
    assert_includes result.trace, "crashed_holding_fence"
    assert_includes result.trace, "fence_reclaimed"
    assert_includes result.trace, "fence_completed"
  end

  test "recovers atomic step-failure outcomes after a crash at :step_failed_recorded" do
    result = Durababble::Deterministic.prove("step_failure_crash_matrix", seed: 11)

    assert_empty result.violations
    assert_includes result.trace, "crashed_after_retry_scheduled"
    assert_includes result.trace, "crashed_after_terminal_failure"
    # Retry path recovered with no lease-stealing reaper involved.
    refute_includes result.trace, "retry_recover_blocked"
  end

  test "flags abandoned-but-runnable workflows under an expect_settled scenario" do
    failures = Durababble::Deterministic.search("bug_abandoned_runnable_workflow", seeds: 1..1)

    assert_equal 1, failures.length
    assert_includes failures.first.last.join("\n"), "pending and runnable"
  end

  test "flags unmet exactly-once effect expectations" do
    failures = Durababble::Deterministic.search("bug_unmet_effect_expectation", seeds: 1..1)

    assert_equal 1, failures.length
    messages = failures.first.last.join("\n")
    assert_includes messages, "expected 1 side effect"
    assert_includes messages, "expected 1 processed outbox"
  end

  test "fuzzes each unique scenario target across many deterministic seeds" do
    assert_scenarios_hold(FUZZ_SCENARIOS, seeds: fuzz_seeds)
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
