# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_command_delivery_to_terminal_workflow(seed)
        run(seed, "workflow_command_delivery_to_terminal_workflow") do |h|
          # A workflow terminates while commands are still queued in its inbox
          # (they were enqueued before it terminated). Delivery must dead-letter
          # every pending command rather than execute it against a terminal
          # workflow, and the wakeup row must be reconciled away. This exercises
          # the terminal branch of complete_workflow_command +
          # reconcile_target_activation (dead_letter_terminal_workflow_inbox +
          # delete activation), which the happy-path delivery scenarios — which
          # always finish the workflow AFTER draining — never reach.
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

          # Terminate the workflow before anything drains the mailbox.
          h.scheduler.schedule(actor: "finisher", delay: 1, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.scheduler.schedule(actor: "command-worker", delay: 10, name: "drain_commands") do
            (command_count + 3).times do
              activation = h.store.claim_target_activation(
                worker_id: "command-worker",
                lease_seconds: 30,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
                lease_seconds: 30,
                limit: 1,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id:,
                  result: { "ok" => message.fetch("id") },
                  worker_id: "command-worker",
                  event_index: h.next_event_index(workflow_id),
                )
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
              )
            end
          end

          h.check("no command was completed against the terminal workflow") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            messages.none? { |message| message.fetch("status") == "completed" }
          end
          h.check("every queued command was dead-lettered, not lost") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            dead = messages.select { |message| message.fetch("status") == "dead_lettered" }.map { |message| message.fetch("id") }
            dead.sort == command_ids.sort
          end
          h.check("wakeup row reconciled away after the terminal mailbox drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end
    end
  end
end
