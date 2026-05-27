# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_command_multi_target_isolation(seed)
        run(seed, "object_command_multi_target_isolation") do |h|
          # Every object-command scenario so far drove a SINGLE target, so a
          # reconcile/head lookup that ignored target_id (keyed only by
          # worker_pool/target_kind/target_type) would pass them all — there is
          # only one target to confuse. Here three distinct objects share a
          # worker_pool and object_type, with interleaved enqueues, drained by
          # the activation loop under crash fuzz. The activation a worker claims
          # carries a target_id; it must drain ONLY that object's mailbox and
          # reconcile ONLY that object's wakeup row. A target_id-blind reconcile
          # would delete a sibling's still-pending activation (lost wakeup) or
          # apply a command to the wrong object's state. The invariant: each
          # object's commands are delivered exactly once to that object and its
          # counter equals its own command count — no cross-contamination.
          object_type = "counter-object"
          objects = ["a", "b", "c"].map { |suffix| "obj-#{suffix}-#{seed}" }
          command_ids = objects.to_h { |object_id| [object_id, []] }

          # Interleave enqueues round-robin so the three mailboxes are live
          # concurrently rather than drained one target at a time.
          (2 * objects.length + h.scheduler.rng.int(3)).times do |i|
            object_id = objects[i % objects.length]
            command_ids[object_id] << h.store.enqueue_object_command(
              object_type:,
              object_id:,
              method_name: "bump",
              args: [i],
              kwargs: {},
              max_attempts: 50,
            )
          end

          h.store.enable_write_crashes!(percent: 20)

          # Activation-driven drain over ALL objects of this type: claim whatever
          # target the wakeup table offers, then act strictly on that target_id.
          drain = lambda do |worker_id|
            (command_ids.values.sum(&:length) * 2 + 8).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds: 8,
                target_kinds: ["object"],
                target_types: [object_type],
              )
              break if activation.nil?

              target_id = activation.fetch("target_id")
              messages = h.store.claim_inbox_messages(
                target_kind: "object",
                target_type: object_type,
                target_id:,
                worker_id:,
                lease_seconds: 8,
                limit: 1,
              )
              messages.each do |message|
                current = h.store.object_state(object_type:, object_id: target_id) || { "n" => 0 }
                h.store.complete_object_command(
                  command_id: message.fetch("id"),
                  result: { "ok" => message.fetch("id") },
                  object_type:,
                  object_id: target_id,
                  state: { "n" => current.fetch("n") + 1 },
                  worker_id:,
                )
              end
              h.store.complete_target_activation(
                target_kind: "object",
                target_type: object_type,
                target_id:,
                worker_id:,
              )
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "obj-deliverer-#{w}", delay: 5 + w * 11, name: "drain_with_crashes") do
              h.store.crashable { drain.call("obj-deliverer-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "obj-deliverer-#{w}", "multi_target_crashed", seed:)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 9 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 90, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 100, name: "final_drain") do
            drain.call("closer")
          end

          objects.each do |object_id|
            h.check("#{object_id}: every command delivered exactly once to its own mailbox") do
              messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
              completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
              completed.sort == command_ids.fetch(object_id).sort && completed.length == completed.uniq.length
            end
            h.check("#{object_id}: counter equals its own command count (no cross-contamination)") do
              state = h.store.object_state(object_type:, object_id:)
              state && state.fetch("n") == command_ids.fetch(object_id).length
            end
          end
          h.check("no wakeup rows survive once every mailbox drains") do
            h.store.all_target_activations.none? { |activation| objects.include?(activation.fetch("target_id")) }
          end
        end
      end
    end
  end
end
