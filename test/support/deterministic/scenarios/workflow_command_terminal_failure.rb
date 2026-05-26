# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_command_terminal_failure(seed)
        run(seed, "workflow_command_terminal_failure") do |h|
          # The final queued command fails terminally (fail_workflow_command ->
          # dead_lettered, workflow still alive), while the earlier commands
          # complete normally. Exercises the alive-workflow dead-letter branch of
          # fail_workflow_command (history append + dead_letter_inbox_message +
          # reconcile), which is unit-test-only and otherwise unreached by DST.
          # Failing the LAST command means there is no tail behind the
          # dead-lettered head, so the documented "dead-lettered head wedges the
          # FIFO" behaviour (harness verify_activation_inbox_consistency!) leaves
          # nothing stranded: the activation is correctly reconciled away.
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

          doomed = command_ids.last
          completed = []
          dead_lettered = []
          h.scheduler.schedule(actor: "command-worker", delay: 5, name: "drain_with_terminal_failure") do
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
                id = message.fetch("id")
                if id == doomed
                  h.store.fail_workflow_command(message_id: id, workflow_id:, error: "terminal command failure", worker_id: "command-worker")
                  dead_lettered << id
                else
                  h.store.complete_workflow_command(message_id: id, workflow_id:, result: { "ok" => id }, worker_id: "command-worker")
                  completed << id
                end
              end
              h.store.complete_target_activation(target_kind: "workflow", target_type: "counter", target_id: workflow_id, worker_id: "command-worker")
            end
          end

          h.scheduler.schedule(actor: "finisher", delay: 20, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("the doomed command was dead-lettered, not completed") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            doomed_row = messages.find { |message| message.fetch("id") == doomed }
            doomed_row && doomed_row.fetch("status") == "dead_lettered" && !completed.include?(doomed)
          end
          h.check("every non-doomed command completed exactly once") do
            expected = command_ids[0...-1].sort
            completed.sort == expected && completed.length == completed.uniq.length
          end
          h.check("wakeup row reconciled away after the terminal-failure drain") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end
    end
  end
end
