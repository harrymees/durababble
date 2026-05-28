# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_command_delivery_crash_matrix(seed)
        run(seed, "workflow_command_delivery_crash_matrix") do |h|
          # Crash *injection* (not simulation) inside the multi-command drain: a
          # worker delivers one command durably, then InjectedCrash fires from the
          # store's complete_workflow_command hook (post-commit) before it can move
          # to the next command or release its activation. A recovery worker must
          # resume the remaining mailbox, delivering every command exactly once
          # (the committed one must NOT be re-delivered; the rest must NOT be lost).
          # Exactly-once is judged from store state, since the crashing call's
          # durable delivery never returns to the in-Ruby tracker.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 2 + h.scheduler.rng.int(3) # 2..4 commands, so work remains after the crash
          command_ids = []
          command_count.times do |i|
            command_ids << h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end

          # Crash once, after the first durable command completion.
          h.store.fault_plan.fail_after(:complete_workflow_command, message: "crash after durable command delivery")

          drain = lambda do |worker_id|
            # [DURABABBLE-LEASE-4] Inbox command commits need the workflow lease;
            # mimic production where each delivery worker holds the workflow lease.
            h.store.mark_workflow_running(workflow_id, worker_id:, lease_seconds: 30)
            (command_count + 3).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds: 30,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id:,
                lease_seconds: 30,
                limit: 1,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id:,
                  result: { "ok" => message.fetch("id") },
                  worker_id:,
                  event_index: h.next_event_index(workflow_id),
                )
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id:,
              )
            end
          end

          h.scheduler.schedule(actor: "faulty-worker", delay: h.scheduler.rng.int(4), name: "deliver_then_crash") do
            drain.call("faulty-worker")
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "faulty-worker", "delivery_crashed_after_commit", id: workflow_id)
          end

          # The crashing reconcile cleared the activation lease (set pending), so a
          # recovery worker can pick up immediately without waiting for expiry.
          h.scheduler.schedule(actor: "recovery-worker", delay: 15, name: "drain_after_crash") do
            drain.call("recovery-worker")
          end

          h.scheduler.schedule(actor: "finisher", delay: 30, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("a command delivery crashed after committing") do
            h.scheduler.trace.to_s.include?("delivery_crashed_after_commit")
          end
          h.check("every command is completed exactly once in the store") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("wakeup row fully reconciled away after recovery drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end
    end
  end
end
