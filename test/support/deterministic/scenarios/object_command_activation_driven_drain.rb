# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_command_activation_driven_drain(seed)
        run(seed, "object_command_activation_driven_drain") do |h|
          # The durable-object command path has only ever been driven by
          # claim_object_command(command_id:) directly; the realistic #69
          # delivery loop — claim_target_activation(target_kinds: ["object"]) ->
          # claim_inbox_messages(limit: 1) -> complete_object_command ->
          # complete_target_activation — has never run for objects (only
          # workflow commands exercise claim_target_activation). With multiple
          # commands queued to one object this also pins the activation
          # HEAD-HANDOFF: after the head completes, reconcile must keep the
          # wakeup row alive for the next pending head and retire it only when
          # the mailbox empties. A handoff bug (retiring the activation while
          # pending work remains) strands the tail, and an activation-driven
          # worker — which only touches the mailbox when an activation says
          # there is work — stops, leaving an undelivered command that the
          # lost-wakeup checker flags. Run under crash fuzz so every window in
          # the multi-transaction loop is exercised.
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
              max_attempts: 50, # never exhaust; the loop only completes
            )
          end

          h.store.enable_write_crashes!(percent: 20)

          # Activation-driven drain: only act when an activation says there is
          # work. This is what makes a wrongly-retired wakeup row fatal (the
          # tail strands) rather than masked by blindly iterating ids.
          drain = lambda do |worker_id|
            (command_count * 2 + 6).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds: 8,
                target_kinds: ["object"],
                target_types: [object_type],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "object",
                target_type: object_type,
                target_id: object_id,
                worker_id:,
                lease_seconds: 8,
                limit: 1,
              )
              messages.each do |message|
                current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
                h.store.complete_object_command(
                  command_id: message.fetch("id"),
                  result: { "ok" => message.fetch("id") },
                  object_type:,
                  object_id:,
                  state: { "n" => current.fetch("n") + 1 },
                  worker_id:,
                )
              end
              h.store.complete_target_activation(
                target_kind: "object",
                target_type: object_type,
                target_id: object_id,
                worker_id:,
              )
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "obj-deliverer-#{w}", delay: 5 + w * 12, name: "drain_with_crashes") do
              h.store.crashable { drain.call("obj-deliverer-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "obj-deliverer-#{w}", "object_delivery_crashed", id: object_id)
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
          h.scheduler.schedule(actor: "closer", delay: 90, name: "final_drain") do
            drain.call("closer")
          end

          h.check("every command delivered exactly once via the activation loop") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("the durable counter equals the command count (applied exactly once each)") do
            state = h.store.object_state(object_type:, object_id:)
            state && state.fetch("n") == command_count
          end
          h.check("wakeup row reconciled away once the mailbox drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end
    end
  end
end
