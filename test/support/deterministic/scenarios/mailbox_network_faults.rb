# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def mailbox_network_faults(seed)
        run(seed, "mailbox_network_faults") do |h|
          h.expect_settled!
          h.network.duplicate_percent = 100
          h.workflows["counter"] = counter_workflow

          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => 0 }, id: "network-workflow")
          workflow_command_count = 2 + h.scheduler.rng.int(3)
          workflow_command_ids = workflow_command_count.times.map do |i|
            h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end

          object_type = "network-counter-object"
          object_id = "network-object"
          object_command_count = 2 + h.scheduler.rng.int(3)
          object_command_ids = object_command_count.times.map do |i|
            h.store.enqueue_object_command(
              object_type:,
              object_id:,
              method_name: "bump",
              args: [i],
              kwargs: {},
              max_attempts: 50,
            )
          end

          drain_workflow = lambda do |worker_id|
            activation = h.store.claim_target_activation(
              worker_id:,
              lease_seconds: 20,
              target_kinds: ["workflow"],
              target_types: ["counter"],
            )
            unless activation
              h.scheduler.trace.event(h.scheduler.time, worker_id, "mailbox_workflow_empty")
              next
            end

            messages = h.store.claim_inbox_messages(
              target_kind: "workflow",
              target_type: "counter",
              target_id: workflow_id,
              worker_id:,
              lease_seconds: 20,
              limit: 1,
            )
            messages.each do |message|
              h.store.complete_workflow_command(
                message_id: message.fetch("id"),
                workflow_id:,
                result: { "ok" => message.fetch("id") },
                worker_id:,
              )
              h.scheduler.trace.event(h.scheduler.time, worker_id, "mailbox_workflow_delivered", message_id: message.fetch("id"))
            end
            h.store.complete_target_activation(target_kind: "workflow", target_type: "counter", target_id: workflow_id, worker_id:)
          end

          drain_object = lambda do |worker_id|
            activation = h.store.claim_target_activation(
              worker_id:,
              lease_seconds: 20,
              target_kinds: ["object"],
              target_types: [object_type],
            )
            unless activation
              h.scheduler.trace.event(h.scheduler.time, worker_id, "mailbox_object_empty")
              next
            end

            messages = h.store.claim_inbox_messages(
              target_kind: "object",
              target_type: object_type,
              target_id: object_id,
              worker_id:,
              lease_seconds: 20,
              limit: 1,
            )
            messages.each do |message|
              state = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
              h.store.complete_object_command(
                command_id: message.fetch("id"),
                result: { "ok" => message.fetch("id") },
                object_type:,
                object_id:,
                state: { "n" => state.fetch("n") + 1 },
                worker_id:,
              )
              h.scheduler.trace.event(h.scheduler.time, worker_id, "mailbox_object_delivered", message_id: message.fetch("id"))
            end
            h.store.complete_target_activation(target_kind: "object", target_type: object_type, target_id: object_id, worker_id:)
          end

          send_delivery = lambda do |kind, index, drop:, &block|
            source = "#{kind}-sender-#{index}"
            target = "#{kind}-mailbox"
            h.scheduler.schedule(actor: source, delay: h.scheduler.rng.int(35), name: "send_#{kind}_delivery") do
              h.network.partition(source, target) if drop
              h.network.send(source:, target:, type: "#{kind}_delivery") { block.call("#{kind}-worker-#{index}") }
              h.network.heal(source, target) if drop
            end
          end

          (workflow_command_count * 2 + 5).times do |i|
            drop = i < 2 || h.scheduler.rng.chance(20)
            send_delivery.call("workflow", i, drop:, &drain_workflow)
          end
          (object_command_count * 2 + 5).times do |i|
            drop = i < 2 || h.scheduler.rng.chance(20)
            send_delivery.call("object", i, drop:, &drain_object)
          end

          final_drain = lambda do |worker_id|
            (workflow_command_count + 3).times { drain_workflow.call("#{worker_id}-workflow") }
            (object_command_count + 3).times { drain_object.call("#{worker_id}-object") }
          end

          h.scheduler.schedule(actor: "network-settler", delay: 80, name: "final_mailbox_drain") do
            final_drain.call("network-settler")
          end
          h.scheduler.schedule(actor: "finisher", delay: 100, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 20)
            h.store.complete_workflow(workflow_id, result: { "count" => workflow_command_count }, worker_id: "finisher")
          end

          h.check("network modeled dropped and duplicate mailbox deliveries") do
            trace = h.scheduler.trace.to_s
            trace.include?("network.drop") && trace.include?("network.duplicate")
          end
          h.check("workflow mailbox commands completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == workflow_command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("object mailbox commands completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == object_command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("object state reflects each delivered command once") do
            state = h.store.object_state(object_type:, object_id:)
            state && state.fetch("n") == object_command_count
          end
        end
      end
    end
  end
end
