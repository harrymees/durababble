# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_command_idempotent_enqueue(seed)
        run(seed, "object_command_idempotent_enqueue") do |h|
          # enqueue_inbox_message dedups on idempotency_key: a repeated enqueue
          # of the same key+shape returns the existing message id and only
          # re-arms the wakeup row if that message is still activatable. No DST
          # scenario exercised this client-facing exactly-once guarantee. The
          # invariant pinned here: however many times the same command is
          # (re-)enqueued — before delivery, racing with it, or as a LATE
          # duplicate after the command has already completed — it is delivered
          # and applied exactly once. The late-duplicate case is the sharp edge:
          # the existing message is `completed` (not activatable) so the dedup
          # branch must NOT re-arm the activation, otherwise a worker would be
          # woken to re-run an already-applied command. Enqueues run under crash
          # fuzz with retry, so a crash mid/after the enqueue transaction (the
          # exact client-retry-with-same-key situation) must still collapse to
          # one message.
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          key = "idem-#{seed}"
          enqueue_count = 3 + h.scheduler.rng.int(3) # 3..5 duplicate enqueues

          enqueue = lambda do
            h.store.enqueue_object_command(
              object_type:,
              object_id:,
              method_name: "bump",
              args: [seed], # identical shape every time => dedups
              kwargs: {},
              idempotency_key: key,
              max_attempts: 50,
            )
          end

          # First enqueue is guaranteed (no crashes yet) so there is always work.
          enqueue.call
          h.store.enable_write_crashes!(percent: 20)

          # Duplicate enqueues scattered across the timeline, each retried if the
          # write crashes — the realistic "client retried with the same key"
          # path. Some land before delivery, some after the command completes.
          enqueue_count.times do |i|
            h.scheduler.schedule(actor: "enqueuer-#{i}", delay: 2 + i * 9, name: "dup_enqueue") do
              h.store.crashable { enqueue.call }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "enqueuer-#{i}", "enqueue_crashed", id: object_id)
              h.store.enqueue_object_command(
                object_type:,
                object_id:,
                method_name: "bump",
                args: [seed],
                kwargs: {},
                idempotency_key: key,
                max_attempts: 50,
              )
            end
          end

          # A single delivery worker claims the head and applies it exactly once.
          h.scheduler.schedule(actor: "deliverer", delay: 6, name: "deliver_once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            head = messages.min_by { |message| message.fetch("sequence") }
            next if head.nil?

            claimed = h.store.claim_object_command(command_id: head.fetch("id"), worker_id: "deliverer", lease_seconds: 30)
            next unless claimed

            current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
            h.store.complete_object_command(
              command_id: head.fetch("id"),
              result: { "ok" => head.fetch("id") },
              object_type:,
              object_id:,
              state: { "n" => current.fetch("n") + 1 },
              worker_id: "deliverer",
            )
          end

          # Disarm crashes and re-run a guaranteed delivery so a wrongly re-armed
          # late duplicate (re-execution) would advance the counter past one and
          # trip the exactly-once check, rather than silently passing.
          h.scheduler.schedule(actor: "settler", delay: 70, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 80, name: "final_deliver") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            head = messages.select { |message| ["pending", "failed", "running"].include?(message.fetch("status")) }
              .min_by { |message| message.fetch("sequence") }
            next if head.nil?

            claimed = h.store.claim_object_command(command_id: head.fetch("id"), worker_id: "closer", lease_seconds: 30)
            next unless claimed

            current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
            h.store.complete_object_command(
              command_id: head.fetch("id"),
              result: { "ok" => head.fetch("id") },
              object_type:,
              object_id:,
              state: { "n" => current.fetch("n") + 1 },
              worker_id: "closer",
            )
          end

          h.check("repeated idempotent enqueues collapsed to exactly one message") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            messages.length == 1
          end
          h.check("the command was applied exactly once despite duplicate enqueues") do
            state = h.store.object_state(object_type:, object_id:)
            state && state.fetch("n") == 1
          end
          h.check("a late duplicate of the completed command did not re-arm a wakeup") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end
    end
  end
end
