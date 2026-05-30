# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module ScenarioSets
      FUZZ_SCENARIOS = [
        "workflow_durable_before_claim",
        "multi_worker_counter",
        "lease_conflict",
        "lease_expiry",
        "concurrent_timer_wake_once",
        "object_wake_survives_worker_crash",
        "timer_and_partition",
        "waits_fences_and_outbox",
        "fenced_side_effect_once",
        "random_invalid_operation_sequence",
        "mailbox_network_faults",
        "generated_workflow_shape_fuzz",
        "step_failure_crash_matrix",
        "duplicate_delivery_timer_and_outbox",
        "cancellation_cleanup_crash_fuzz",
        "cancellation_during_suspend_race",
        "wait_condition_timer_command_race",
        "record_step_canceled_crash_fuzz",
        "workflow_termination_dependents_crash_fuzz",
        "timer_wakeup_batch_crash_fuzz",
        "workflow_command_delivery_crash_matrix",
        "workflow_command_claim_window_crash_matrix",
        "workflow_command_terminal_failure_crash_fuzz",
        "object_command_crash_fuzz",
        "object_command_state_crash_fuzz",
        "object_command_retry_then_apply_crash_fuzz",
        "object_command_activation_driven_drain",
        "object_command_idempotent_enqueue",
        "object_command_multi_target_isolation",
        "worker_tick_dispatch_fuzz",
        "rpc_workflow_rpc_transport_fault_matrix",
        "random_operation_sequence",
        "generated_workflow_rpc_interleaving_fuzz",
        "chaos",
      ].freeze

      REGRESSION_SCENARIOS = [
        "heartbeat_extension",
        "zombie_workflow_heartbeat_after_expiry",
        "step_heartbeat_cursor_recovery",
        "step_retry_policy_recovery",
        "completed_step_skip_after_crash",
        "incomplete_step_retry_after_crash",
        "attempt_history_append_only",
        "multiple_named_object_wakes",
        "stale_wait_timer_terminal_workflow",
        "fence_holder_crash_and_reclaim",
        "outbox_lease_expiry",
        "store_fault_after_step_completed",
        "store_fault_after_wait_recorded",
        "store_fault_after_outbox_enqueue",
        "cooperative_cancellation_cleanup",
        "parallel_branch_failure_orphans_step",
        "parallel_wait_with_retrying_sibling",
        "wait_condition_command_wakeup",
        "wait_condition_sequential_command_wakeups",
        "stolen_lease_write_rejection",
        "workflow_command_async_delivery",
        "workflow_command_delivery_crash_recovery",
        "workflow_command_retry_then_complete",
        "workflow_command_terminal_failure",
        "workflow_command_delivery_to_terminal_workflow",
        "object_command_failure_exhaustion",
        "object_command_claim_contention",
      ].freeze

      CONTRACT_SCENARIOS = [
        "rpc_fault_injection",
        "rpc_service_contract",
        "rpc_wakeup_fault_matrix",
        "workflow_rpc_owner_state_matrix",
        "rpc_workflow_rpc_response_matrix",
        "rpc_workflow_rpc_transport_fault_reroute",
      ].freeze

      CLEAN_SCENARIOS = (FUZZ_SCENARIOS + REGRESSION_SCENARIOS + CONTRACT_SCENARIOS).freeze
      DEFAULT_FUZZ_SEEDS = (1..100)
      EXPLORATION_PROBE_SEEDS = [1, 2, 3].freeze
    end
  end
end
