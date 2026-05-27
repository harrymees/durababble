# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_command_retry_then_complete(seed)
        run(seed, "workflow_command_retry_then_complete") do |h|
          # Each command's first delivery attempt fails transiently
          # (retry_object_command -> message back to pending, immediately
          # re-ready) and its second attempt completes. Exercises the inbox
          # retry state machine (retry_inbox_message + reconcile-to-pending,
          # which clears the activation lease and re-arms it) and proves every
          # command is re-delivered and ultimately completed exactly once, with
          # none lost or double-completed. The happy-path/crash delivery
          # scenarios never fail a command, so this path was previously
          # unexercised by DST.
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

          retried = Hash.new(0)
          completed = []
          h.scheduler.schedule(actor: "command-worker", delay: 5, name: "drain_with_retries") do
            # [DURABABBLE-LEASE-4] Inbox command commits need the workflow lease; mimic production.
            h.store.mark_workflow_running(workflow_id, worker_id: "command-worker", lease_seconds: 30)
            # Bounded at two passes per command (retry + complete) plus slack so
            # a stuck head fails a check rather than spinning the virtual clock.
            (command_count * 2 + 3).times do
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
                id = message.fetch("id")
                if retried[id].zero?
                  retried[id] += 1
                  # ready_at = now means the retried head is immediately
                  # re-deliverable in this same drain pass.
                  h.store.retry_object_command(
                    command_id: id,
                    error: "transient delivery failure",
                    worker_id: "command-worker",
                    ready_at: h.store.current_time,
                  )
                else
                  h.store.complete_workflow_command(
                    message_id: id,
                    workflow_id:,
                    result: { "ok" => id },
                    worker_id: "command-worker",
                  )
                  completed << id
                end
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
              )
            end
          end

          h.scheduler.schedule(actor: "finisher", delay: 20, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("every command was retried exactly once before completing") do
            command_ids.all? { |id| retried[id] == 1 }
          end
          h.check("every command completed exactly once after its retry") do
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("inbox shows each command completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            done = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            done.sort == command_ids.sort
          end
          h.check("wakeup row reconciled away after the mailbox drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end
    end
  end
end
