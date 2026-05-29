# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_command_claim_window_crash_matrix(seed)
        run(seed, "workflow_command_claim_window_crash_matrix") do |h|
          # Crash *injection* in the two claim windows of the drain loop, where
          # the lease has been durably taken but no command has been delivered.
          # Unlike the complete_workflow_command crash (whose reconcile clears
          # the lease, letting recovery start immediately), a crash here leaves
          # the activation (window A) or the activation + inbox head (window B)
          # leased to the dead worker. Recovery therefore cannot proceed until
          # the lease expires, then reclaims via claim_target_activation / the
          # expired-running inbox path and delivers every command exactly
          # once. This exercises the post-commit fault hooks on the claim
          # methods, systematically covering the inter-transaction gaps the
          # complete-side crash matrix does not.
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

          # Seed picks the window: 0 => crash holding only the activation lease;
          # 1 => crash holding the activation + inbox head leases.
          window = h.scheduler.rng.int(2)
          crash_op = window.zero? ? :claim_target_activation : :claim_inbox_messages
          h.store.fault_plan.fail_after(crash_op, message: "crash in #{crash_op} claim window")

          drain = lambda do |worker_id, lease_seconds|
            # [DURABABBLE-LEASE-4] Inbox command commits need the workflow lease;
            # mimic production where each delivery worker holds the workflow lease.
            h.store.mark_workflow_running(workflow_id, worker_id:, lease_seconds:)
            (command_count + 3).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds:,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id:,
                lease_seconds:,
                limit: 1,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id:,
                  result: { "ok" => message.fetch("id") },
                  worker_id:,
                  event_index: h.next_event_index(workflow_id),
                )
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id:,
              )
            end
          end

          # Faulty worker leases under a SHORT lease, then InjectedCrash fires
          # from the claim hook before any command completes. The lease stays
          # held until it expires.
          h.scheduler.schedule(actor: "faulty-worker", delay: h.scheduler.rng.int(4), name: "claim_then_crash") do
            drain.call("faulty-worker", 5)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "faulty-worker", "claim_window_crashed", id: workflow_id, op: crash_op.to_s)
          end

          # Recovery runs well past the 5-tick lease so the held lease has
          # expired and is reclaimable.
          h.scheduler.schedule(actor: "recovery-worker", delay: 20, name: "drain_after_expiry") do
            drain.call("recovery-worker", 30)
          end

          h.scheduler.schedule(actor: "finisher", delay: 40, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("a claim-window crash was observed") do
            h.scheduler.trace.to_s.include?("claim_window_crashed")
          end
          h.check("every command is completed exactly once in the store") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("wakeup row fully reconciled away after recovery drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end
    end
  end
end
