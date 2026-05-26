# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_command_claim_contention(seed)
        run(seed, "object_command_claim_contention") do |h|
          # The command-mailbox lease has never been contended: every existing
          # concurrency scenario (lease_conflict, lease_expiry, multi_worker)
          # races on the *workflow* claim path. Here worker A claims an object
          # command under a short lease and stalls (never completes). While that
          # lease is live, worker B must be BLOCKED from claiming the same head
          # (claim_object_command -> inbox_row_claimable? false for a leased
          # running row) — proving mutual exclusion. After A's lease expires, B
          # reclaims via the expired-running path and completes the command
          # exactly once, advancing the durable counter by exactly one. This
          # pins lease mutual-exclusion + expiry-reclaim + exactly-once for the
          # inbox/command path the delivery loop depends on.
          object_type = "counter-object"
          object_id = "obj-#{seed}"

          command_id = h.store.enqueue_object_command(
            object_type:,
            object_id:,
            method_name: "bump",
            args: [seed],
            kwargs: {},
            max_attempts: 50,
          )

          # A takes a short (8-tick) lease at t=0 and never completes — a worker
          # that grabbed the command then stalled.
          h.scheduler.schedule(actor: "worker-a", delay: 0, name: "claim_and_stall") do
            claimed = h.store.claim_object_command(command_id:, worker_id: "worker-a", lease_seconds: 8)
            h.scheduler.trace.event(h.scheduler.time, "worker-a", claimed ? "a_claimed" : "a_claim_failed", id: command_id)
          end

          # B probes while A's lease is live (t=3). It must NOT be able to claim.
          h.scheduler.schedule(actor: "worker-b", delay: 3, name: "probe_during_live_lease") do
            claimed = h.store.claim_object_command(command_id:, worker_id: "worker-b", lease_seconds: 8)
            if claimed
              h.scheduler.trace.event(h.scheduler.time, "worker-b", "b_stole_live_lease", id: command_id)
            else
              h.scheduler.trace.event(h.scheduler.time, "worker-b", "b_blocked_by_lease", id: command_id)
            end
          end

          # After A's lease expires (t>=8), B reclaims and completes exactly once.
          h.scheduler.schedule(actor: "worker-b", delay: 12, name: "reclaim_after_expiry") do
            claimed = h.store.claim_object_command(command_id:, worker_id: "worker-b", lease_seconds: 30)
            next unless claimed

            current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
            h.store.complete_object_command(
              command_id:,
              result: { "ok" => command_id },
              object_type:,
              object_id:,
              state: { "n" => current.fetch("n") + 1 },
            )
            h.scheduler.trace.event(h.scheduler.time, "worker-b", "b_completed_after_reclaim", id: command_id)
          end

          h.check("B was blocked while A held a live lease") do
            trace = h.scheduler.trace.to_s
            trace.include?("a_claimed") && trace.include?("b_blocked_by_lease") && !trace.include?("b_stole_live_lease")
          end
          h.check("the command completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }
            completed.length == 1 && completed.first.fetch("id") == command_id
          end
          h.check("the durable counter advanced by exactly one") do
            state = h.store.object_state(object_type:, object_id:)
            state && state.fetch("n") == 1
          end
          h.check("wakeup row reconciled away after the command completed") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end
    end
  end
end
