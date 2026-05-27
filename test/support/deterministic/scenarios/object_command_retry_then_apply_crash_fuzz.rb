# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_command_retry_then_apply_crash_fuzz(seed)
        run(seed, "object_command_retry_then_apply_crash_fuzz") do |h|
          # Exercises retry_object_command under crash fuzz combined with state
          # mutation -- a path NO scenario reached. object_command_state_crash_fuzz
          # only ever completes; object_command_failure_exhaustion uses
          # fail_object_command (no state, no crashes). Here each command suffers
          # one or more TRANSIENT failures (retry_object_command, which records the
          # error + re-arms but must NOT touch object state) before eventually
          # completing with state n+1. The exactly-once invariant under every crash
          # window: the final counter equals the command count -- a transient
          # failure must never apply state, and a command that retries N times then
          # completes once must apply its bump exactly once. If retry leaked into
          # the state-writing path, or completion double-applied across a re-claim,
          # the counter would drift; if retry failed to re-arm/clear its lease, a
          # command would strand and the counter would fall short. The fenced
          # retry+completion paths (worker_id passed to both) are driven here.
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          command_count = 2 + h.scheduler.rng.int(3) # 2..4

          thresholds = {}
          command_ids = command_count.times.map do |i|
            id = h.store.enqueue_object_command(
              object_type:,
              object_id:,
              method_name: "bump",
              args: [i],
              kwargs: {},
              max_attempts: 100, # never exhaust; transient failures retry, never dead-letter
            )
            # Each command fails transiently 1..2 times before it is allowed to
            # complete. The decision is keyed on the persisted attempt count, so it
            # is crash-consistent: a rolled-back retry does not advance attempts.
            thresholds[id] = 1 + h.scheduler.rng.int(2)
            id
          end

          h.store.enable_write_crashes!(percent: 20)

          apply = lambda do |worker_id|
            (command_count * 8 + 10).times do
              progressed = false
              command_ids.each do |command_id|
                claimed = h.store.claim_object_command(command_id:, worker_id:, lease_seconds: 8)
                next if claimed.nil?

                attempts = claimed.fetch("attempts").to_i
                if attempts <= thresholds.fetch(command_id)
                  # Transient failure: re-arm for immediate retry. Must not touch
                  # object state. ready_at == current_time keeps it claimable now.
                  h.store.retry_object_command(
                    command_id:,
                    error: "transient #{attempts}",
                    worker_id:,
                    ready_at: h.store.current_time,
                  )
                else
                  current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
                  h.store.complete_object_command(
                    command_id:,
                    result: { "ok" => command_id },
                    object_type:,
                    object_id:,
                    state: { "n" => current.fetch("n") + 1 },
                    worker_id:,
                  )
                end
                progressed = true
              end
              break unless progressed
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "retry-worker-#{w}", delay: 5 + w * 11, name: "apply_with_retries") do
              h.store.crashable { apply.call("retry-worker-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "retry-worker-#{w}", "object_retry_crashed", id: object_id)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 10 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 80, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 90, name: "final_apply") do
            apply.call("closer")
          end

          h.check("every command completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("every completed command recorded at least one transient retry") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            messages.all? { |message| message.fetch("attempts").to_i >= 2 }
          end
          h.check("the persisted counter equals the command count (state applied exactly once despite retries)") do
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
