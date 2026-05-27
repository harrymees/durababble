# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_invalid_store_shape(seed)
        run(seed, "bug_invalid_store_shape") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id)
          base = h.store.workflow(id)

          # Impossible shapes injected via the store's test-only overlay (the real
          # schema's NOT NULL / FK constraints would reject these), so the harness
          # invariant checkers see the same corrupt state they must flag.
          h.store.inject_workflow(base.merge(
            "id" => "bad-status-workflow",
            "status" => "mystery",
            "locked_by" => nil,
            "locked_until" => nil,
          ))
          h.store.inject_workflow(base.merge(
            "id" => "partial-lease-workflow",
            "status" => "pending",
            "locked_by" => "worker-a",
            "locked_until" => nil,
          ))
          h.store.inject_workflow(base.merge(
            "id" => "locked-waiting-workflow",
            "status" => "waiting",
            "locked_by" => "stale",
            "locked_until" => h.scheduler.time + 10,
          ))
          h.store.inject_workflow(base.merge(
            "id" => "terminal-live-step-workflow",
            "status" => "completed",
            "locked_by" => nil,
            "locked_until" => nil,
          ))
          h.store.inject_step({
            "workflow_id" => "missing-workflow",
            "position" => 0,
            "name" => "missing_owner",
            "status" => "completed",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "missing-workflow-attempt",
            "workflow_id" => "missing-workflow",
            "position" => 0,
            "name" => "missing_owner",
            "status" => "completed",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_step({
            "workflow_id" => id,
            "position" => 0,
            "name" => "orphaned_step",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_step({
            "workflow_id" => "other-workflow",
            "position" => 4,
            "name" => "bad_step",
            "status" => "mystery",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
            "__group_id" => id,
            "__position_key" => 3,
          })
          h.store.inject_attempt({
            "id" => "mismatched-step-attempt",
            "workflow_id" => id,
            "position" => 3,
            "name" => "other_name",
            "status" => "completed",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_step({
            "workflow_id" => id,
            "position" => 9,
            "name" => "duplicate_completed_a",
            "status" => "completed",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
            "__position_key" => 4,
          })
          h.store.inject_step({
            "workflow_id" => id,
            "position" => 9,
            "name" => "duplicate_completed_b",
            "status" => "completed",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
            "__position_key" => 5,
          })
          h.store.inject_step({
            "workflow_id" => id,
            "position" => 8,
            "name" => "multi_live",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "live-attempt-a",
            "workflow_id" => id,
            "position" => 8,
            "name" => "multi_live",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "live-attempt-b",
            "workflow_id" => id,
            "position" => 8,
            "name" => "multi_live",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_step({
            "workflow_id" => "terminal-live-step-workflow",
            "position" => 0,
            "name" => "still_running",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "terminal-live-attempt",
            "workflow_id" => "terminal-live-step-workflow",
            "position" => 0,
            "name" => "still_running",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "bad-attempt",
            "workflow_id" => id,
            "position" => 1,
            "name" => "missing_step",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "bad-status-attempt",
            "workflow_id" => "other-workflow",
            "position" => 99,
            "name" => "bad_status",
            "status" => "mystery",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_wait({
            "id" => "bad-wait",
            "workflow_id" => id,
            "position" => 2,
            "kind" => "event",
            "event_key" => "missing-step",
            "wake_at" => nil,
            "context" => {},
            "payload" => nil,
            "status" => "completed",
          })
          h.store.inject_wait({
            "id" => "bad-status-wait",
            "workflow_id" => id,
            "position" => 0,
            "kind" => "event",
            "event_key" => "bad-status",
            "wake_at" => nil,
            "context" => {},
            "payload" => nil,
            "status" => "mystery",
          })
          h.store.inject_wait({
            "id" => "completed-running-step-wait",
            "workflow_id" => id,
            "position" => 0,
            "kind" => "event",
            "event_key" => "running-step",
            "wake_at" => nil,
            "context" => {},
            "payload" => nil,
            "status" => "completed",
          })
          h.store.inject_wait({
            "id" => "missing-workflow-wait",
            "workflow_id" => "missing-workflow",
            "position" => 9,
            "kind" => "event",
            "event_key" => "missing-workflow",
            "wake_at" => nil,
            "context" => {},
            "payload" => nil,
            "status" => "pending",
          })
          h.store.inject_outbox({
            "id" => "bad-outbox",
            "workflow_id" => "missing-workflow",
            "topic" => "email",
            "payload" => {},
            "key" => "bad-outbox",
            "status" => "processing",
            "locked_by" => nil,
            "locked_until" => nil,
          })
          h.store.inject_outbox({
            "id" => "bad-status-outbox",
            "workflow_id" => id,
            "topic" => "email",
            "payload" => {},
            "key" => "bad-status-outbox",
            "status" => "mystery",
            "locked_by" => nil,
            "locked_until" => nil,
          })
        end
      end
    end
  end
end
