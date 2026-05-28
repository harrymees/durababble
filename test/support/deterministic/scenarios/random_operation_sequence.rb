# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def random_operation_sequence(seed)
        run(seed, "random_operation_sequence") do |h|
          h.workflows["counter"] = counter_workflow
          h.workflows["random-waiting"] = workflow_class("random-waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(ctx.fetch("wake_at"), ctx.merge("woken" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end

          workflow_ids = []
          object_commands = []
          object_type = "random-counter-object"
          object_id = "object-#{seed}"
          operations = [
            "enqueue_counter",
            "enqueue_waiting",
            "request_cancellation",
            "request_termination",
            "wake_timers",
            "steal_expired",
            "enqueue_outbox",
            "process_outbox",
            "run_fence",
            "enqueue_object_command",
            "drain_object_command",
          ].freeze

          pick = lambda do |items|
            next nil if items.empty?

            items[h.scheduler.rng.int(items.length)]
          end
          resume_random_workflow = lambda do |actor, workflow_id|
            row = h.store.workflow(workflow_id)
            return if ["completed", "failed", "canceled", "terminated"].include?(row.fetch("status"))

            resume_workflow_once(h, actor:, workflow: h.workflows.fetch(row.fetch("name")), workflow_id:)
          end

          safe = lambda do |actor, operation, &body|
            h.scheduler.trace.event(h.scheduler.time, actor, "random_operation", operation:)
            body.call
          rescue WorkflowAlreadyExists, Error, KeyError, LeaseConflict, CommandTimeout, FenceTimeout => e
            h.scheduler.trace.event(h.scheduler.time, actor, "random_operation_rejected", operation:, error: "#{e.class}: #{e.message}")
          end

          48.times do |i|
            operation = operations[h.scheduler.rng.int(operations.length)]
            delay = h.scheduler.rng.int(260)
            actor = "random-op-#{i}"
            h.scheduler.schedule(actor:, delay:, name: operation) do
              safe.call(actor, operation) do
                case operation
                when "enqueue_counter"
                  workflow_id = "random-counter-#{seed}-#{i}"
                  workflow_ids << h.store.enqueue_workflow(name: "counter", input: { "count" => i }, id: workflow_id)
                when "enqueue_waiting"
                  workflow_id = "random-waiting-#{seed}-#{i}"
                  workflow_ids << h.store.enqueue_workflow(name: "random-waiting", input: { "id" => workflow_id, "wake_at" => h.store.current_time + 40 + h.scheduler.rng.int(80) }, id: workflow_id)
                when "request_cancellation"
                  workflow_id = pick.call(workflow_ids)
                  h.store.request_workflow_cancellation(workflow_id:, reason: "random cancel #{i}") if workflow_id
                when "request_termination"
                  workflow_id = pick.call(workflow_ids)
                  h.store.request_workflow_termination(workflow_id:, reason: "random terminate #{i}") if workflow_id
                when "wake_timers"
                  workflow_id = pick.call(workflow_ids)
                  resume_random_workflow.call(actor, workflow_id) if workflow_id
                when "steal_expired"
                  h.store.steal_expired_leases!(now: h.scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
                when "enqueue_outbox"
                  workflow_id = pick.call(workflow_ids)
                  if workflow_id
                    h.store.enqueue_outbox(workflow_id:, topic: "random", payload: { "i" => i, "seed" => seed }, key: "random-outbox-#{seed}-#{i}")
                  end
                when "process_outbox"
                  message = h.store.claim_outbox(worker_id: actor, lease_seconds: 20)
                  h.store.ack_outbox(message.fetch("id"), worker_id: actor) if message
                when "run_fence"
                  workflow_id = pick.call(workflow_ids)
                  h.store.with_fence(workflow_id:, key: "random-fence-#{i}") { { "i" => i } } if workflow_id
                when "enqueue_object_command"
                  object_commands << h.store.enqueue_object_command(
                    object_type:,
                    object_id:,
                    method_name: "bump",
                    args: [i],
                    kwargs: {},
                    idempotency_key: "random-object-#{seed}-#{i}",
                    max_attempts: 100,
                  )
                when "drain_object_command"
                  command_id = pick.call(object_commands)
                  claimed = h.store.claim_object_command(command_id:, worker_id: actor, lease_seconds: 20) if command_id
                  if claimed
                    if h.scheduler.rng.chance(25)
                      h.store.fail_object_command(command_id:, error: "random failure #{i}", worker_id: actor)
                    else
                      state = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
                      h.store.complete_object_command(
                        command_id:,
                        result: { "ok" => i },
                        object_type:,
                        object_id:,
                        state: { "n" => state.fetch("n") + 1 },
                        worker_id: actor,
                      )
                    end
                  end
                end
              end
            end
          end

          h.add_workers(["random-worker-a", "random-worker-b", "random-worker-c"], ticks: 32, crash_percent: 12)
          8.times do |i|
            h.scheduler.schedule(actor: "random-reaper", delay: 70 + i * 35, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
            end
            h.scheduler.schedule(actor: "random-timer", delay: 85 + i * 30, name: "wake_due_timers") do
              workflow_ids.each { |workflow_id| resume_random_workflow.call("random-timer", workflow_id) }
            end
          end

          h.check("random sequence executed multiple operation kinds") do
            operations.count { |operation| h.scheduler.trace.to_s.include?("operation=#{operation.inspect}") } >= 4
          end
        end
      end
    end
  end
end
