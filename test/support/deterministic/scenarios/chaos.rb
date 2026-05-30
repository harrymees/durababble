# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def chaos(seed)
        run(seed, "chaos") do |h|
          h.workflows["counter"] = counter_workflow
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(h.store.current_time + 80, ctx.merge("woken" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          object_type = "chaos-counter-object"
          object_id = "chaos-object-#{seed}"
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

          12.times do |i|
            name = h.scheduler.rng.chance(25) ? "waiting" : "counter"
            input = name == "waiting" ? { "id" => "w#{i}" } : { "count" => i }
            h.network.send(source: "client-#{i}", target: "db", type: "enqueue") { h.store.enqueue_workflow(name:, input:) }
            h.scheduler.schedule(actor: "timer-#{i}", delay: 80 + h.scheduler.rng.int(200), name: "timer_tick") do
              h.scheduler.trace.event(h.scheduler.time, "timer-#{i}", "timer_tick")
            end
          end
          command_amounts = {}
          (4 + h.scheduler.rng.int(3)).times do |i|
            h.network.send(source: "object-client-#{i}", target: "db", type: "enqueue_object_command") do
              command_id = h.store.enqueue_object_command(
                object_type:,
                object_id:,
                method_name: "bump",
                args: [i + 1],
                kwargs: {},
                idempotency_key: "chaos-object-#{seed}-#{i}",
                max_attempts: 20,
              )
              command_amounts[command_id] = i + 1
            end
          end
          # Crash workers mid-resume between durable writes (not just whole-tick
          # skips), so every inter-write window is exercised under chaos. The
          # reaper + repeated ticks must still drive every workflow and target
          # activation to a crash-consistent state.
          h.store.enable_write_crashes!(percent: 20)
          h.add_workers(["worker-a", "worker-b", "worker-c", "worker-d"], ticks: 30, crash_percent: 15)

          real_workers = Array.new(2) do |i|
            Worker.new(
              store: h.store,
              workflows: h.workflows.values,
              objects: [object_class],
              worker_id: "chaos-real-worker-#{i}",
              lease_seconds: 20,
              migrate: false,
            )
          end
          20.times do |tick|
            worker = real_workers[h.scheduler.rng.int(real_workers.length)]
            h.scheduler.schedule(actor: "chaos-real-worker-#{tick}", delay: 10 + h.scheduler.rng.int(320), name: "real_worker_tick") do
              result = worker.tick
              h.scheduler.trace.event(h.scheduler.time, "chaos-real-worker", "real_worker_tick", result:, tick:)
            rescue WorkflowSuspended, StepRetryScheduled, LeaseConflict => e
              h.scheduler.trace.event(h.scheduler.time, "chaos-real-worker", "real_worker_tick_yield", error: e.class.name, tick:)
            rescue InjectedCrash => e
              h.scheduler.trace.event(h.scheduler.time, "chaos-real-worker", "real_worker_tick_crashed", error: e.message, tick:)
            end
          end
          8.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 60 + i * 50, name: "steal_expired") { h.store.steal_expired_leases!(now: h.scheduler.time + 61) }
          end
          h.scheduler.schedule(actor: "chaos-settler", delay: 420, name: "run_real_workers_until_idle") do
            h.store.enable_write_crashes!(percent: 0)
            real_workers.each { |worker| worker.run_until_idle(max_ticks: [command_amounts.length * 2, 16].max) }
          end

          h.check("chaos production worker ticks ran real work") do
            trace = h.scheduler.trace.to_s
            trace.include?("real_worker_tick") && trace.include?("result=:worked")
          end
          h.check("chaos enqueued object commands for worker dispatch") do
            command_amounts.any?
          end
          h.check("chaos object commands drained exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_amounts.keys.sort && completed.length == completed.uniq.length
          end
          h.check("chaos object state matches completed commands") do
            state = h.store.object_state(object_type:, object_id:)
            state &&
              state.fetch("count") == command_amounts.values.sum &&
              state.fetch("commands").sort == command_amounts.keys.sort
          end
        end
      end
    end
  end
end
