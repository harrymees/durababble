# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def random_invalid_operation_sequence(seed)
        run(seed, "random_invalid_operation_sequence") do |h|
          h.expect_settled!
          h.workflows["counter"] = counter_workflow
          h.workflows["invalid-waiting"] = workflow_class("invalid-waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(ctx.fetch("wake_at"), ctx.merge("woken" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end

          terminal_id = h.store.enqueue_workflow(name: "counter", input: { "count" => 0 }, id: "invalid-terminal")
          terminal_command_count = 2 + h.scheduler.rng.int(3)
          terminal_command_ids = terminal_command_count.times.map do |i|
            h.store.enqueue_workflow_command(
              workflow_id: terminal_id,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end
          h.store.complete_workflow(terminal_id, result: { "done" => true })

          waiting_id = h.store.enqueue_workflow(
            name: "invalid-waiting",
            input: { "wake_at" => 70 },
            id: "invalid-waiting",
          )

          stale_id = h.store.enqueue_workflow(name: "counter", input: { "count" => 1 }, id: "invalid-stale")
          h.store.claim_workflow(workflow_id: stale_id, worker_id: "stale-owner", lease_seconds: 5)
          h.store.record_step_scheduled(workflow_id: stale_id, command_id: 0, name: "increment", worker_id: "stale-owner")
          h.store.record_step_started(workflow_id: stale_id, command_id: 0, name: "increment", worker_id: "stale-owner")

          object_type = "invalid-counter-object"
          object_id = "invalid-object"
          object_key = "invalid-object-command"
          object_command_id = h.store.enqueue_object_command(
            object_type:,
            object_id:,
            method_name: "bump",
            args: [1],
            kwargs: {},
            idempotency_key: object_key,
            max_attempts: 20,
          )

          outbox_id = h.store.enqueue_outbox(
            workflow_id: terminal_id,
            topic: "invalid",
            payload: { "kind" => "wrong-ack" },
            key: "invalid-outbox",
          )

          safe = lambda do |actor, operation, &block|
            h.scheduler.trace.event(h.scheduler.time, actor, "invalid_operation", operation:)
            block.call
          rescue StandardError => e
            h.scheduler.trace.event(h.scheduler.time, actor, "invalid_operation_rejected", operation:, error: "#{e.class}: #{e.message}")
          end

          request_missing_cancel = lambda do |actor|
            row = h.store.request_workflow_cancellation(workflow_id: "missing-workflow", reason: "missing cancel")
            h.scheduler.trace.event(h.scheduler.time, actor, "missing_cancel_ignored") if row.nil?
          end
          request_missing_terminate = lambda do |actor|
            row = h.store.request_workflow_termination(workflow_id: "missing-workflow", reason: "missing terminate")
            h.scheduler.trace.event(h.scheduler.time, actor, "missing_terminate_ignored") if row.nil?
          end

          stale_writes = [
            ["record_step_completed", -> { h.store.record_step_completed(workflow_id: stale_id, command_id: 0, result: { "stale" => true }, worker_id: "stale-owner") }],
            ["record_step_failed", -> { h.store.record_step_failed(workflow_id: stale_id, command_id: 0, error: "stale failure", worker_id: "stale-owner") }],
            ["record_step_canceled", -> { h.store.record_step_canceled(workflow_id: stale_id, command_id: 0, error: "stale cancel", worker_id: "stale-owner") }],
            ["complete_workflow", -> { h.store.complete_workflow(stale_id, result: { "stale" => true }, worker_id: "stale-owner") }],
            ["fail_workflow", -> { h.store.fail_workflow(stale_id, error: "stale fail", worker_id: "stale-owner") }],
            ["cancel_workflow", -> { h.store.cancel_workflow(stale_id, reason: "stale cancel", worker_id: "stale-owner") }],
          ].freeze

          reject_stale_write = lambda do |actor, name, operation|
            operation.call
            h.scheduler.trace.event(h.scheduler.time, actor, "stale_accepted", operation: name)
          rescue LeaseConflict, Error => e
            h.scheduler.trace.event(h.scheduler.time, actor, "stale_write_rejected", operation: name, error: "#{e.class}: #{e.message}")
          end

          drain_terminal_commands = lambda do |worker_id|
            (terminal_command_count + 4).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds: 20,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: terminal_id,
                worker_id:,
                lease_seconds: 20,
                limit: 1,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id: terminal_id,
                  result: { "unexpected" => message.fetch("id") },
                  worker_id:,
                )
              end
              h.store.complete_target_activation(target_kind: "workflow", target_type: "counter", target_id: terminal_id, worker_id:)
            end
          end

          apply_object_command = lambda do |worker_id|
            claimed = h.store.claim_object_command(command_id: object_command_id, worker_id:, lease_seconds: 20)
            next if claimed.nil?

            state = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
            h.store.complete_object_command(
              command_id: object_command_id,
              result: { "ok" => true },
              object_type:,
              object_id:,
              state: { "n" => state.fetch("n") + 1 },
              worker_id:,
            )
          end

          wrong_ack = lambda do |actor|
            claimed = h.store.claim_outbox(worker_id: "outbox-owner", lease_seconds: 20)
            if claimed
              h.store.ack_outbox(claimed.fetch("id"), worker_id: "wrong-owner")
              h.scheduler.trace.event(h.scheduler.time, actor, "wrong_outbox_ack_ignored")
            end
          end

          guaranteed = [
            ["cancel_missing", request_missing_cancel],
            ["terminate_missing", request_missing_terminate],
            ["duplicate_workflow_id", ->(_actor) { h.store.enqueue_workflow(name: "counter", input: {}, id: terminal_id) }],
            ["early_timer_wake", ->(_actor) { h.store.wake_due_timers(now: h.store.current_time + 10) }],
            ["wrong_outbox_ack", wrong_ack],
          ]
          guaranteed.each_with_index do |(operation, block), index|
            h.scheduler.schedule(actor: "invalid-guaranteed-#{index}", delay: 1 + index, name: operation) do
              safe.call("invalid-guaranteed-#{index}", operation) { block.call("invalid-guaranteed-#{index}") }
            end
          end

          h.scheduler.schedule(actor: "waiting-worker", delay: 6, name: "park_waiting") do
            Durababble::Engine.new(store: h.store, worker_id: "waiting-worker").resume(
              h.workflows.fetch("invalid-waiting"),
              workflow_id: waiting_id,
            )
          end
          h.scheduler.schedule(actor: "reaper", delay: 10, name: "steal_expired") { h.store.steal_expired_leases! }
          h.scheduler.schedule(actor: "new-owner", delay: 12, name: "claim_stale_workflow") do
            h.store.claim_workflow(workflow_id: stale_id, worker_id: "new-owner", lease_seconds: 60)
          end

          36.times do |i|
            operation = [
              "cancel_missing",
              "terminate_missing",
              "cancel_terminal",
              "terminate_terminal",
              "duplicate_object_enqueue",
              "drain_terminal_commands",
              "apply_object_command",
              "stale_write",
              "wake_timers",
            ][h.scheduler.rng.int(9)]
            actor = "invalid-op-#{i}"
            h.scheduler.schedule(actor:, delay: 15 + h.scheduler.rng.int(80), name: operation) do
              safe.call(actor, operation) do
                case operation
                when "cancel_missing"
                  request_missing_cancel.call(actor)
                when "terminate_missing"
                  request_missing_terminate.call(actor)
                when "cancel_terminal"
                  h.store.request_workflow_cancellation(workflow_id: terminal_id, reason: "too late")
                when "terminate_terminal"
                  h.store.request_workflow_termination(workflow_id: terminal_id, reason: "too late")
                when "duplicate_object_enqueue"
                  h.store.enqueue_object_command(
                    object_type:,
                    object_id:,
                    method_name: "bump",
                    args: [1],
                    kwargs: {},
                    idempotency_key: object_key,
                    max_attempts: 20,
                  )
                when "drain_terminal_commands"
                  drain_terminal_commands.call(actor)
                when "apply_object_command"
                  apply_object_command.call(actor)
                when "stale_write"
                  name, block = stale_writes[h.scheduler.rng.int(stale_writes.length)]
                  reject_stale_write.call(actor, name, block)
                when "wake_timers"
                  h.store.wake_due_timers(now: h.store.current_time + 120)
                end
              end
            end
          end

          h.scheduler.schedule(actor: "timer", delay: 80, name: "wake_waiting") do
            h.store.wake_due_timers(now: h.store.current_time + 120)
          end
          h.scheduler.schedule(actor: "waiting-worker", delay: 90, name: "finish_waiting") do
            Durababble::Engine.new(store: h.store, worker_id: "waiting-worker").resume(
              h.workflows.fetch("invalid-waiting"),
              workflow_id: waiting_id,
            )
          end
          h.scheduler.schedule(actor: "terminal-drainer", delay: 100, name: "final_terminal_drain") do
            drain_terminal_commands.call("terminal-drainer")
          end
          h.scheduler.schedule(actor: "object-drainer", delay: 105, name: "final_object_drain") do
            apply_object_command.call("object-drainer")
          end
          h.scheduler.schedule(actor: "sender", delay: 110, name: "final_outbox_ack") do
            message = h.store.claim_outbox(worker_id: "sender", lease_seconds: 20)
            h.store.ack_outbox(message.fetch("id"), worker_id: "sender") if message
          end
          h.scheduler.schedule(actor: "new-owner", delay: 115, name: "finish_stale_workflow") do
            h.store.claim_workflow(workflow_id: stale_id, worker_id: "new-owner", lease_seconds: 60)
            h.store.record_step_completed(workflow_id: stale_id, command_id: 0, result: { "count" => 2 }, worker_id: "new-owner")
            h.store.complete_workflow(stale_id, result: { "count" => 2 }, worker_id: "new-owner")
          end

          h.check("missing workflow operations were safely rejected") do
            lines = h.scheduler.trace.lines
            lines.any? { |line| line.include?("invalid_operation_rejected") && line.include?('operation="cancel_missing"') } &&
              lines.any? { |line| line.include?("invalid_operation_rejected") && line.include?('operation="terminate_missing"') }
          end
          h.check("stale owner writes were rejected") do
            trace = h.scheduler.trace.to_s
            trace.include?("stale_write_rejected") && !trace.include?("stale_accepted")
          end
          h.check("terminal workflow commands were dead-lettered, not delivered") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: terminal_id)
            dead = messages.select { |message| message.fetch("status") == "dead_lettered" }.map { |message| message.fetch("id") }
            dead.sort == terminal_command_ids.sort
          end
          h.check("duplicate object command enqueue applied once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            state = h.store.object_state(object_type:, object_id:)
            messages.one? && messages.first.fetch("status") == "completed" && state && state.fetch("n") == 1
          end
          h.check("wrong outbox ack did not lose the message") do
            h.store.outbox_message(outbox_id).fetch("status") == "processed"
          end
        end
      end
    end
  end
end
