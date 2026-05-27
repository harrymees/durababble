# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_command_target_identity_isolation(seed)
        run(seed, "workflow_command_target_identity_isolation") do |h|
          h.store.enqueue_workflow(name: "counter", input: {}, id: "wf-a")
          h.store.enqueue_workflow(name: "counter", input: {}, id: "wf-b")
          first_id = h.store.enqueue_workflow_command(
            workflow_id: "wf-a",
            workflow_name: "counter",
            method_name: "approve",
            payload: { "method_name" => "approve", "args" => [], "kwargs" => {} },
            idempotency_key: "cmd-a-1",
          )
          second_id = h.store.enqueue_workflow_command(
            workflow_id: "wf-a",
            workflow_name: "counter",
            method_name: "reject",
            payload: { "method_name" => "reject", "args" => [], "kwargs" => {} },
            idempotency_key: "cmd-a-2",
          )

          h.store.claim_target_activation(
            worker_id: "command-worker",
            lease_seconds: 30,
            target_kinds: ["workflow"],
            target_types: ["counter"],
          )
          h.store.claim_inbox_messages(
            target_kind: "workflow",
            target_type: "counter",
            target_id: "wf-a",
            worker_id: "command-worker",
            lease_seconds: 30,
            limit: 2,
          )

          wrong_complete = h.store.complete_workflow_command(
            message_id: first_id,
            workflow_id: "wf-b",
            result: { "ok" => true },
            worker_id: "command-worker",
          )
          wrong_fail = h.store.fail_workflow_command(
            message_id: second_id,
            workflow_id: "wf-b",
            error: "wrong target",
            worker_id: "command-worker",
          )
          first = h.store.inbox_message(first_id)
          second = h.store.inbox_message(second_id)
          wf_b_history = h.store.workflow_history_for("wf-b")
          h.scheduler.trace.event(
            h.scheduler.time,
            "command-worker",
            "wrong_workflow_command_target",
            completed: !!wrong_complete,
            failed: !!wrong_fail,
            first_status: first&.fetch("status"),
            second_status: second&.fetch("status"),
            wf_b_history: wf_b_history.length,
          )

          h.check("workflow command completion must match the command target workflow") do
            wrong_complete.nil? && first&.fetch("status") == "running" && first&.fetch("locked_by") == "command-worker"
          end
          h.check("workflow command failure must match the command target workflow") do
            wrong_fail.nil? && second&.fetch("status") == "running" && second&.fetch("locked_by") == "command-worker"
          end
          h.check("wrong target workflow history is unchanged") do
            wf_b_history.empty?
          end
        end
      end
    end
  end
end
