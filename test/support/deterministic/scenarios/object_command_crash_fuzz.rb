# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_command_crash_fuzz(seed)
        run(seed, "object_command_crash_fuzz") do |h|
          # Point the generic crash-after-write fuzz (enable_write_crashes!) at
          # the durable-object command failure loop, which it has never driven
          # (chaos only crashes the workflow-resume path). Each claim/fail step
          # is its own transaction; fail_object_command commits
          # fail_inbox_message + reconcile_target_activation together, so a
          # :mid_transaction crash must roll BOTH back, while an :after_commit
          # crash leaves a durable, consistent state. Because attempts increment
          # at claim (mark_inbox_row_running), a crash after a committed claim
          # but before the fail "burns" an attempt: the command may dead-letter
          # with attempts >= max_attempts after fewer real failures, and attempts
          # can even exceed max_attempts across reclaim waves. The invariant that
          # must survive every crash window is liveness: the command reaches a
          # consistent terminal (dead_lettered) state with its activation
          # reconciled away, and the harness store invariants
          # (activation<->inbox consistency, no stuck activation) hold.
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          max_attempts = 2 + h.scheduler.rng.int(2) # 2..3

          command_id = h.store.enqueue_object_command(
            object_type:,
            object_id:,
            method_name: "bump",
            args: [seed],
            kwargs: {},
            max_attempts:,
          )

          h.store.enable_write_crashes!(percent: 25)

          # Crashing worker waves. Each holds a short (8-tick) lease and rescues
          # InjectedCrash (a modelled process death). Waves are spaced wider than
          # the lease so an abandoned, expired-running inbox row is reclaimable by
          # the next wave via inbox_row_claimable?.
          drain = lambda do |worker_id|
            (max_attempts + 5).times do
              claimed = h.store.claim_object_command(command_id:, worker_id:, lease_seconds: 8)
              break if claimed.nil?

              h.store.fail_object_command(command_id:, error: "boom", worker_id:, terminal: false)
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "obj-worker-#{w}", delay: 5 + w * 12, name: "fail_with_crashes") do
              h.store.crashable { drain.call("obj-worker-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "obj-worker-#{w}", "object_command_crashed", id: command_id)
            end
          end

          # Reaper expires any abandoned workflow/fence leases between waves.
          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 10 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          # Disarm crashes, then one final guaranteed-clean drain so liveness is
          # not at the mercy of every wave happening to crash before exhaustion.
          h.scheduler.schedule(actor: "settler", delay: 70, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 80, name: "final_drain") do
            drain.call("closer")
          end

          h.check("the command eventually dead-letters with attempts >= max_attempts") do
            message = h.store.inbox_message(command_id)
            message && message.fetch("status") == "dead_lettered" && message.fetch("attempts").to_i >= max_attempts
          end
          h.check("wakeup row reconciled away once the command is terminal") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end
    end
  end
end
