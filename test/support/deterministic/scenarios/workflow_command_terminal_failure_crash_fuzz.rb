# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_command_terminal_failure_crash_fuzz(seed)
        run(seed, "workflow_command_terminal_failure_crash_fuzz") do |h|
          # Crash-fuzzes the alive-workflow branch of fail_workflow_command, which
          # writes THREE rows in one transaction: append_workflow_history
          # (workflow_command_failed) + dead_letter_inbox_message + reconcile. Only
          # workflow_command_terminal_failure reached this branch and it ran with no
          # crashes. This is the same multi-write-atomicity class as the original
          # step-failure bug: a :mid_transaction crash must roll back ALL three
          # (history not appended, message still running, activation not reconciled),
          # so a recovery worker re-claims and re-fails -- the command must end
          # dead-lettered exactly once AND the workflow_command_failed history entry
          # must appear exactly once. If the history append were not atomic with the
          # dead-letter, a crash between them would leave a dangling history row and
          # re-delivery would append a second -> duplicate. The last command is the
          # doomed one (no tail behind the dead-lettered head, so nothing strands).
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 2 + h.scheduler.rng.int(3) # 2..4 commands
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

          h.store.enable_write_crashes!(percent: 20)

          drain = lambda do |worker_id|
            (command_count * 3 + 5).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds: 8,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id:,
                lease_seconds: 8,
                limit: 1,
              )
              messages.each do |message|
                id = message.fetch("id")
                if id == doomed
                  h.store.fail_workflow_command(message_id: id, workflow_id:, error: "terminal command failure", worker_id:)
                else
                  h.store.complete_workflow_command(message_id: id, workflow_id:, result: { "ok" => id }, worker_id:)
                end
              end
              h.store.complete_target_activation(target_kind: "workflow", target_type: "counter", target_id: workflow_id, worker_id:)
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "fail-worker-#{w}", delay: 5 + w * 11, name: "drain_with_crashes") do
              h.store.crashable { drain.call("fail-worker-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "fail-worker-#{w}", "terminal_failure_crashed", id: workflow_id)
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
          h.scheduler.schedule(actor: "finisher", delay: 130, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("the doomed command is dead-lettered") do
            message = h.store.inbox_message(doomed)
            message && message.fetch("status") == "dead_lettered"
          end
          h.check("the workflow_command_failed history entry appears exactly once (atomic with dead-letter)") do
            failed = h.store.workflow_history_for(workflow_id).select do |entry|
              entry.fetch("kind") == "workflow_command_failed" && entry["attempt_id"] == doomed
            end
            failed.length == 1
          end
          h.check("every non-doomed command completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids[0...-1].sort && completed.length == completed.uniq.length
          end
          h.check("wakeup row reconciled away after the terminal-failure drain") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end
    end
  end
end
