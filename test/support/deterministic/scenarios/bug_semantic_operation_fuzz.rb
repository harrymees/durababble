# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_semantic_operation_fuzz(seed)
        run(seed, "bug_semantic_operation_fuzz") do |h|
          h.monitor_transitions!
          h.workflows["semantic-counter"] = counter_workflow

          monitor_row = h.store.workflow(
            h.store.enqueue_workflow(name: "semantic-counter", input: { "count" => seed }, id: "monitor-base"),
          )
          terminal_monitor = monitor_row.merge(
            "id" => "monitor-terminal",
            "status" => WorkflowStatus::COMPLETED,
            "result" => { "ok" => true },
            "error" => nil,
            "cancel_reason" => nil,
            "locked_by" => nil,
            "locked_until" => nil,
            "next_run_at" => nil,
          )
          backoff_monitor = monitor_row.merge(
            "id" => "monitor-backoff",
            "status" => WorkflowStatus::FAILED,
            "result" => nil,
            "error" => "retry later",
            "cancel_reason" => nil,
            "locked_by" => nil,
            "locked_until" => nil,
            "next_run_at" => h.store.current_time + 100,
          )
          h.store.inject_workflow(terminal_monitor)
          h.store.inject_workflow(backoff_monitor)
          h.store.inject_outbox({
            "id" => "monitor-outbox",
            "workflow_id" => "monitor-terminal",
            "topic" => "monitor",
            "payload" => { "seed" => seed },
            "key" => "monitor-outbox",
            "status" => OutboxStatus::PENDING,
            "locked_by" => nil,
            "locked_until" => nil,
            "processed_at" => nil,
          })
          h.store.inject_target_activation({
            "worker_pool" => "default",
            "target_kind" => "object",
            "target_type" => "monitor-object",
            "target_id" => "activation",
            "status" => "running",
            "ready_at" => h.store.current_time,
            "created_at" => h.store.current_time,
            "locked_by" => "monitor-activation",
            "locked_until" => h.store.current_time + 1,
          })
          command_monitor_id = h.store.enqueue_workflow(name: "semantic-counter", input: { "count" => seed }, id: "monitor-command")
          h.store.inject_inbox({
            "id" => "monitor-command-message",
            "worker_pool" => "default",
            "target_kind" => "workflow",
            "target_type" => "semantic-counter",
            "target_id" => command_monitor_id,
            "method_name" => "poke",
            "payload" => { "method" => "poke", "args" => [], "kwargs" => {} },
            "status" => InboxStatus::RUNNING,
            "locked_by" => "monitor-command",
            "locked_until" => h.store.current_time + 100,
            "sequence" => 1,
            "ready_at" => h.store.current_time,
            "attempts" => 1,
            "max_attempts" => 1,
          })
          h.store.inject_target_activation({
            "worker_pool" => "default",
            "target_kind" => "workflow",
            "target_type" => "semantic-counter",
            "target_id" => command_monitor_id,
            "status" => "running",
            "ready_at" => h.store.current_time,
            "created_at" => h.store.current_time,
            "locked_by" => "monitor-command",
            "locked_until" => h.store.current_time + 100,
          })

          safe = lambda do |actor, operation, &block|
            h.scheduler.trace.event(h.scheduler.time, actor, "semantic_operation", operation:)
            block.call
            h.scheduler.trace.event(h.scheduler.time, actor, "semantic_operation_accepted", operation:)
          rescue Error, FenceTimeout, KeyError, LeaseConflict, WorkflowAlreadyExists => e
            h.scheduler.trace.event(h.scheduler.time, actor, "semantic_operation_rejected", operation:, error: "#{e.class}: #{e.message}")
          end

          terminal_id = h.store.enqueue_workflow(name: "semantic-counter", input: { "count" => seed }, id: "semantic-terminal")
          h.store.complete_workflow(terminal_id, result: { "terminal" => true })

          backoff_id = h.store.enqueue_workflow(name: "semantic-counter", input: { "count" => seed }, id: "semantic-backoff")
          h.store.claim_workflow(workflow_id: backoff_id, worker_id: "backoff-owner", lease_seconds: 30)
          h.store.record_step_scheduled(workflow_id: backoff_id, command_id: 0, name: "increment", worker_id: "backoff-owner")
          h.store.record_step_started(workflow_id: backoff_id, command_id: 0, name: "increment", worker_id: "backoff-owner")
          h.store.record_step_failed_and_schedule_retry(
            workflow_id: backoff_id,
            command_id: 0,
            error: "transient backoff",
            worker_id: "backoff-owner",
            run_at: h.store.current_time + 100,
          )

          outbox_workflow_id = h.store.enqueue_workflow(name: "semantic-counter", input: { "count" => seed }, id: "semantic-outbox")
          outbox_id = h.store.enqueue_outbox(
            workflow_id: outbox_workflow_id,
            topic: "semantic",
            payload: { "seed" => seed },
            key: "semantic-outbox",
          )
          h.store.claim_outbox(worker_id: "stale-sender", lease_seconds: 5)

          object_command_id = h.store.enqueue_object_command(
            object_type: "semantic-object",
            object_id: "activation",
            method_name: "bump",
            args: [seed],
            kwargs: {},
            idempotency_key: "semantic-activation",
            max_attempts: 5,
          )
          claimed_activation = h.store.claim_target_activation(
            worker_id: "stale-activation",
            lease_seconds: 5,
            target_kinds: ["object"],
            target_types: ["semantic-object"],
          )

          command_workflow_id = h.store.enqueue_workflow(name: "semantic-counter", input: { "count" => seed }, id: "semantic-command")
          command_message_id = h.store.enqueue_workflow_command(
            workflow_id: command_workflow_id,
            workflow_name: "semantic-counter",
            method_name: "bump",
            payload: { "method" => "bump", "args" => [], "kwargs" => {} },
          )
          h.store.claim_target_activation(
            worker_id: "no-lease-command",
            lease_seconds: 20,
            target_kinds: ["workflow"],
            target_types: ["semantic-counter"],
          )
          h.store.claim_inbox_messages(
            target_kind: "workflow",
            target_type: "semantic-counter",
            target_id: command_workflow_id,
            worker_id: "no-lease-command",
            lease_seconds: 20,
            limit: 1,
          )

          h.scheduler.schedule(actor: "monitor-terminal", delay: 1, name: "monitor_terminal_mutation") do
            h.store.inject_workflow(terminal_monitor.merge(
              "status" => WorkflowStatus::RUNNING,
              "locked_by" => "monitor-terminal",
              "locked_until" => h.store.current_time + 30,
            ))
          end

          h.scheduler.schedule(actor: "monitor-backoff", delay: 2, name: "monitor_backoff_early_claim") do
            h.store.inject_workflow(backoff_monitor.merge(
              "status" => WorkflowStatus::RUNNING,
              "locked_by" => "monitor-backoff",
              "locked_until" => h.store.current_time + 30,
            ))
          end

          h.scheduler.schedule(actor: "monitor-outbox", delay: 3, name: "monitor_outbox_without_lease") do
            h.store.inject_outbox({
              "id" => "monitor-outbox",
              "workflow_id" => "monitor-terminal",
              "topic" => "monitor",
              "payload" => { "seed" => seed },
              "key" => "monitor-outbox",
              "status" => OutboxStatus::PROCESSED,
              "locked_by" => nil,
              "locked_until" => nil,
              "processed_at" => h.store.current_time,
            })
          end

          h.scheduler.schedule(actor: "monitor-activation", delay: 4, name: "monitor_activation_completed_after_expiry") do
            h.store.inject_target_activation({
              "worker_pool" => "default",
              "target_kind" => "object",
              "target_type" => "monitor-object",
              "target_id" => "activation",
              "status" => "pending",
              "ready_at" => h.store.current_time,
              "created_at" => h.store.current_time,
              "locked_by" => nil,
              "locked_until" => nil,
            })
          end

          h.scheduler.schedule(actor: "monitor-command", delay: 5, name: "monitor_command_history_without_workflow_lease") do
            h.store.send(
              :append_workflow_history_without_transaction,
              workflow_id: command_monitor_id,
              kind: "workflow_command_completed",
              name: "poke",
              attempt_id: "monitor-command-message",
              payload: { "message_id" => "monitor-command-message", "result" => { "ok" => true } },
            )
          end

          h.scheduler.schedule(actor: "terminal-resurrector", delay: 1 + h.scheduler.rng.int(3), name: "mark_terminal_running") do
            safe.call("terminal-resurrector", "mark_terminal_running") do
              h.store.mark_workflow_running(terminal_id, worker_id: "terminal-resurrector", lease_seconds: 30)
            end
          end

          h.scheduler.schedule(actor: "early-activation", delay: 4 + h.scheduler.rng.int(4), name: "claim_backoff_for_activation") do
            safe.call("early-activation", "claim_backoff_for_activation") do
              h.store.claim_workflow_for_activation(workflow_id: backoff_id, worker_id: "early-activation", lease_seconds: 30)
            end
          end

          h.scheduler.schedule(actor: "stale-sender", delay: 10 + h.scheduler.rng.int(3), name: "stale_outbox_ack") do
            safe.call("stale-sender", "stale_outbox_ack") do
              h.store.ack_outbox(outbox_id, worker_id: "stale-sender")
            end
          end

          h.scheduler.schedule(actor: "outbox-recovery", delay: 24, name: "recover_outbox") do
            safe.call("outbox-recovery", "recover_outbox") do
              message = h.store.claim_outbox(worker_id: "outbox-recovery", lease_seconds: 20)
              h.store.ack_outbox(message.fetch("id"), worker_id: "outbox-recovery") if message
            end
          end

          h.scheduler.schedule(actor: "stale-activation", delay: 11 + h.scheduler.rng.int(3), name: "stale_activation_complete") do
            safe.call("stale-activation", "stale_activation_complete") do
              h.store.complete_target_activation(
                target_kind: "object",
                target_type: "semantic-object",
                target_id: "activation",
                worker_id: "stale-activation",
              )
            end
          end

          h.scheduler.schedule(actor: "activation-recovery", delay: 25, name: "recover_activation") do
            safe.call("activation-recovery", "recover_activation") do
              h.store.claim_target_activation(
                worker_id: "activation-recovery",
                lease_seconds: 20,
                target_kinds: ["object"],
                target_types: ["semantic-object"],
              )
              claimed = h.store.claim_object_command(command_id: object_command_id, worker_id: "activation-recovery", lease_seconds: 20)
              if claimed
                h.store.complete_object_command(
                  command_id: object_command_id,
                  result: { "ok" => true },
                  object_type: "semantic-object",
                  object_id: "activation",
                  state: { "n" => 1 },
                  worker_id: "activation-recovery",
                )
              end
            end
          end

          h.scheduler.schedule(actor: "no-lease-command", delay: 12 + h.scheduler.rng.int(3), name: "workflow_command_without_workflow_lease") do
            safe.call("no-lease-command", "workflow_command_without_workflow_lease") do
              h.store.complete_workflow_command(
                message_id: command_message_id,
                workflow_id: command_workflow_id,
                result: { "unexpected" => true },
                worker_id: "no-lease-command",
              )
            end
          end

          h.scheduler.schedule(actor: "command-recovery", delay: 45, name: "recover_workflow_command") do
            safe.call("command-recovery", "recover_workflow_command") do
              activation = h.store.claim_target_activation(
                worker_id: "command-recovery",
                lease_seconds: 20,
                target_kinds: ["workflow"],
                target_types: ["semantic-counter"],
              )
              if activation && activation.fetch("target_id") == command_workflow_id
                h.store.claim_workflow_for_activation(workflow_id: command_workflow_id, worker_id: "command-recovery", lease_seconds: 20)
                h.store.claim_inbox_messages(
                  target_kind: "workflow",
                  target_type: "semantic-counter",
                  target_id: command_workflow_id,
                  worker_id: "command-recovery",
                  lease_seconds: 20,
                  limit: 1,
                )
                h.store.complete_workflow_command(
                  message_id: command_message_id,
                  workflow_id: command_workflow_id,
                  result: { "ok" => true },
                  worker_id: "command-recovery",
                )
                h.store.complete_target_activation(
                  target_kind: "workflow",
                  target_type: "semantic-counter",
                  target_id: command_workflow_id,
                  worker_id: "command-recovery",
                )
              end
            end
          end

          12.times do |i|
            h.scheduler.schedule(actor: "semantic-random-#{i}", delay: 16 + h.scheduler.rng.int(35), name: "random_semantic_operation") do
              operation = [
                "cancel_terminal",
                "claim_backoff_for_activation",
                "stale_outbox_ack",
                "stale_activation_complete",
                "workflow_command_without_workflow_lease",
              ][h.scheduler.rng.int(5)]
              safe.call("semantic-random-#{i}", operation) do
                case operation
                when "cancel_terminal"
                  h.store.request_workflow_cancellation(workflow_id: terminal_id, reason: "late cancel")
                when "claim_backoff_for_activation"
                  h.store.claim_workflow_for_activation(workflow_id: backoff_id, worker_id: "semantic-random-#{i}", lease_seconds: 10)
                when "stale_outbox_ack"
                  h.store.ack_outbox(outbox_id, worker_id: "stale-sender")
                when "stale_activation_complete"
                  h.store.complete_target_activation(target_kind: "object", target_type: "semantic-object", target_id: "activation", worker_id: "stale-activation")
                when "workflow_command_without_workflow_lease"
                  h.store.complete_workflow_command(message_id: command_message_id, workflow_id: command_workflow_id, result: { "random" => i }, worker_id: "no-lease-command")
                end
              end
            end
          end

          h.check("semantic fuzz exercised the broad operation mix") do
            trace = h.scheduler.trace.to_s
            ["mark_terminal_running", "claim_backoff_for_activation", "stale_outbox_ack", "stale_activation_complete", "workflow_command_without_workflow_lease"].all? do |operation|
              trace.include?(operation)
            end
          end
          h.check("stale activation setup claimed an activation lease") do
            !claimed_activation.nil?
          end
          h.check("workflow command writes without the workflow lease did not append history") do
            h.store.workflow_history_for(command_workflow_id).none? { |event| event.fetch("kind") == "workflow_command_completed" }
          end
        end
      end
    end
  end
end
