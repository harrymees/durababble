# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def worker_tick_dispatch_fuzz(seed)
        run(seed, "worker_tick_dispatch_fuzz") do |h|
          h.expect_settled!
          h.workflows["worker-counter"] = workflow_class("worker-counter") do
            test_step("increment") { |ctx| ctx.merge("count" => ctx.fetch("count") + 1) }
          end

          object_type = "worker-counter-object"
          object_class = durable_object_class(object_type) do
            define_method(:initialize_state) { { "count" => 0, "commands" => [] } }
            define_method(:bump) do |amount|
              update_state(
                current_state.merge(
                  "count" => current_state.fetch("count") + amount,
                  "commands" => current_state.fetch("commands") + [command_context.command_id],
                ),
              )
              current_state.fetch("count")
            end
            expose_command(:bump)
          end

          workflow_count = 2 + h.scheduler.rng.int(3)
          object_command_count = 2 + h.scheduler.rng.int(3)
          workflow_ids = workflow_count.times.map do |index|
            h.store.enqueue_workflow(
              name: "worker-counter",
              input: { "id" => "worker-workflow-#{seed}-#{index}", "count" => index },
              id: "worker-workflow-#{seed}-#{index}",
            )
          end
          object_command_ids = object_command_count.times.map do |index|
            h.store.enqueue_object_command(
              object_type:,
              object_id: "worker-object-#{seed}",
              method_name: "bump",
              args: [index + 1],
              kwargs: {},
              idempotency_key: "worker-object-#{seed}-#{index}",
              max_attempts: 20,
            )
          end

          workers = Array.new(3) do |index|
            Durababble::Worker.new(
              store: h.store,
              workflows: [h.workflows.fetch("worker-counter")],
              objects: [object_class],
              worker_id: "real-worker-#{index}",
              lease_seconds: 15,
              migrate: false,
            )
          end

          worker_tick_count = 18 + h.scheduler.rng.int(8)
          worker_tick_count.times do |tick|
            worker = workers[h.scheduler.rng.int(workers.length)]
            h.scheduler.schedule(actor: "real-worker-tick-#{tick}", delay: h.scheduler.rng.int(90), name: "worker_tick") do
              result = worker.tick
              h.scheduler.trace.event(h.scheduler.time, "real-worker", "real_worker_tick", result:, tick:)
            rescue WorkflowSuspended, StepRetryScheduled, LeaseConflict => e
              h.scheduler.trace.event(h.scheduler.time, "real-worker", "real_worker_tick_yield", error: e.class.name, tick:)
            end
          end

          4.times do |index|
            h.scheduler.schedule(actor: "real-worker-reaper-#{index}", delay: 20 + index * 20, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 20)
            end
          end
          h.scheduler.schedule(actor: "real-worker-settler", delay: 120, name: "run_until_idle") do
            workers.each { |worker| worker.run_until_idle(max_ticks: workflow_count + object_command_count + 5) }
          end

          h.check("real worker tick completed every workflow") do
            workflow_ids.all? { |workflow_id| h.store.workflow(workflow_id).fetch("status") == "completed" }
          end
          h.check("real worker tick drained every object command exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: "worker-object-#{seed}")
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == object_command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("real worker tick applied the durable object state exactly once per command") do
            state = h.store.object_state(object_type:, object_id: "worker-object-#{seed}")
            state &&
              state.fetch("count") == (1..object_command_count).sum &&
              state.fetch("commands").sort == object_command_ids.sort
          end
          h.check("real worker tick path exercised workflow and object dispatch") do
            trace = h.scheduler.trace.to_s
            trace.include?("real_worker_tick") &&
              trace.include?("result=:worked") &&
              trace.include?("workflow_claimed") &&
              h.store.object_state(object_type:, object_id: "worker-object-#{seed}")
          end
        end
      end
    end
  end
end
