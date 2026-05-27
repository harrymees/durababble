# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_command_async_delivery(seed)
        run(seed, "workflow_command_async_delivery") do |h|
          # Drives #69's async command-delivery path live: enqueue_workflow_command
          # (writes the inbox message and upserts the wakeup row in one txn) ->
          # claim_target_activation -> claim_inbox_messages(limit: 1) ->
          # complete_workflow_command -> complete_target_activation, looping until
          # the mailbox drains and reconcile retires the wakeup row. This is the
          # only scenario that exercises the multi-transaction worker-drain loop, so
          # the activation-invariant and lost-wakeup consistency checkers run
          # against real reconcile behaviour rather than only hand-injected
          # fixtures. Each enqueued command must be delivered exactly once and the
          # wakeup row must be fully reconciled away once the mailbox is empty.
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

          delivered = []
          h.scheduler.schedule(actor: "command-worker", delay: 5, name: "drain_commands") do
            # [DURABABBLE-LEASE-4] Inbox command commits need the workflow lease;
            # mimic production where the workflow worker holds the lease while
            # draining its inbox.
            h.store.mark_workflow_running(workflow_id, worker_id: "command-worker", lease_seconds: 30)
            # Bounded so a buggy re-arm (activation never retired) fails the
            # exactly-once check rather than spinning the virtual clock forever.
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
                )
                delivered << message.fetch("id")
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
              )
            end
          end

          # Retire the workflow after the mailbox drains so the liveness checker
          # sees a terminal target rather than an abandoned-but-runnable workflow.
          h.scheduler.schedule(actor: "finisher", delay: 20, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("every enqueued command delivered exactly once") do
            delivered.sort == command_ids.sort && delivered.length == delivered.uniq.length
          end
          h.check("wakeup row fully reconciled away after the mailbox drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end
    end
  end
end
