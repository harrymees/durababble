# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_command_delivery_crash_recovery(seed)
        run(seed, "workflow_command_delivery_crash_recovery") do |h|
          # A delivery worker crashes mid-flight: it claims the wakeup row
          # (activation -> running, leased) and the head inbox message (-> running,
          # leased) but completes neither. Both leases must expire and be reclaimed
          # by a recovery worker that drains the mailbox, delivering every command
          # exactly once (the partially-claimed head must be delivered by recovery,
          # neither lost nor double-delivered). Exercises the reclaim primitives
          # (claim_target_activation taking over an expired activation;
          # claim_inbox_messages re-claiming an expired in-flight message) that the
          # happy-path workflow_command_async_delivery scenario never reaches.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 1 + h.scheduler.rng.int(3) # 1..3 commands
          command_ids = []
          command_count.times do |i|
            command_ids << h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end

          # The crashed worker grabs the activation and the head message under a
          # short lease, then does nothing else (simulating a crash before the
          # complete writes). Scheduled early so both leases expire before recovery.
          h.scheduler.schedule(actor: "crashed-worker", delay: 1, name: "claim_then_crash") do
            h.store.claim_target_activation(
              worker_id: "crashed-worker",
              lease_seconds: 5,
              target_kinds: ["workflow"],
              target_types: ["counter"],
            )
            h.store.claim_inbox_messages(
              target_kind: "workflow",
              target_type: "counter",
              target_id: workflow_id,
              worker_id: "crashed-worker",
              lease_seconds: 5,
              limit: 1,
            )
            h.scheduler.trace.event(h.scheduler.time, "crashed-worker", "delivery_worker_crashed", id: workflow_id)
          end

          delivered = []
          h.scheduler.schedule(actor: "recovery-worker", delay: 20, name: "drain_after_recovery") do
            (command_count + 3).times do
              activation = h.store.claim_target_activation(
                worker_id: "recovery-worker",
                lease_seconds: 30,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "recovery-worker",
                lease_seconds: 30,
                limit: 1,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id:,
                  result: { "ok" => message.fetch("id") },
                  worker_id: "recovery-worker",
                )
                delivered << message.fetch("id")
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "recovery-worker",
              )
            end
          end

          h.scheduler.schedule(actor: "finisher", delay: 40, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("crashed delivery worker was observed") do
            h.scheduler.trace.to_s.include?("delivery_worker_crashed")
          end
          h.check("every command delivered exactly once despite the crash") do
            delivered.sort == command_ids.sort && delivered.length == delivered.uniq.length
          end
          h.check("wakeup row fully reconciled away after recovery drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end
    end
  end
end
