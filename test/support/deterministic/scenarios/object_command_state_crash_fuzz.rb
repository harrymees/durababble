# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_command_state_crash_fuzz(seed)
        run(seed, "object_command_state_crash_fuzz") do |h|
          # Exercises the durable-object STATE persistence path under crash
          # fuzz, which no scenario reached: complete_object_command commits
          # save_object_state + complete_inbox_message + reconcile in ONE
          # transaction. A worker reads the object's counter, increments it, and
          # completes the command with the new state. Commands for one object
          # are strict-FIFO (claim_object_command only takes the inbox head), so
          # they apply in order, one at a time — no concurrent read-modify-write.
          # The exactly-once invariant that must hold across every crash window:
          # the final persisted counter equals the number of commands (a
          # :mid_transaction crash rolls back the save AND the completion, so the
          # command re-delivers and re-applies once; an :after_commit crash
          # leaves both durable, so it is never re-applied). Splitting that
          # transaction would either strand state (completion without save) or
          # double-count (save without completion) — both caught here.
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          command_count = 2 + h.scheduler.rng.int(3) # 2..4

          command_ids = command_count.times.map do |i|
            h.store.enqueue_object_command(
              object_type:,
              object_id:,
              method_name: "bump",
              args: [i],
              kwargs: {},
              max_attempts: 50, # never exhaust; we only complete, never fail
            )
          end

          h.store.enable_write_crashes!(percent: 20)

          # Claim the head (only the head is claimable), read-modify-write the
          # counter, and complete. Iterating ids in enqueue order naturally hits
          # the current head first.
          apply = lambda do |worker_id|
            (command_count * 2 + 5).times do
              progressed = false
              command_ids.each do |command_id|
                claimed = h.store.claim_object_command(command_id:, worker_id:, lease_seconds: 8)
                next if claimed.nil?

                current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
                h.store.complete_object_command(
                  command_id:,
                  result: { "ok" => command_id },
                  object_type:,
                  object_id:,
                  state: { "n" => current.fetch("n") + 1 },
                )
                progressed = true
              end
              break unless progressed
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "state-worker-#{w}", delay: 5 + w * 12, name: "apply_with_crashes") do
              h.store.crashable { apply.call("state-worker-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "state-worker-#{w}", "object_state_crashed", id: object_id)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 10 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 70, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 80, name: "final_apply") do
            apply.call("closer")
          end

          h.check("every command completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("the persisted counter equals the command count (applied exactly once each)") do
            state = h.store.object_state(object_type:, object_id:)
            state && state.fetch("n") == command_count
          end
          h.check("wakeup row reconciled away once every command is applied") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end
    end
  end
end
