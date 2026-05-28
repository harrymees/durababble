# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def timer_and_partition(seed)
        run(seed, "timer_and_partition") do |h|
          h.workflows["timer"] = workflow_class("timer") do
            test_step("sleep") { |ctx| Durababble.wait_until(ctx.fetch("wake_at"), ctx) }
            test_step("finish") { |ctx| ctx.merge("timer_done" => true) }
          end
          h.network.partition("partitioned-client", "db")
          h.network.send(source: "partitioned-client", target: "db", type: "enqueue") { h.store.enqueue_workflow(name: "timer", input: { "wake_at" => 50 }) }
          h.scheduler.schedule(actor: "network", delay: 10, name: "heal") { h.network.heal("partitioned-client", "db") }
          h.scheduler.schedule(actor: "healed-client", delay: 12, name: "enqueue") { h.store.enqueue_workflow(name: "timer", input: { "wake_at" => 50 }) }
          h.add_workers(["worker-a", "worker-b"], ticks: 15)
          h.scheduler.schedule(actor: "timer", delay: 55, name: "timer_due") do
            h.scheduler.trace.event(h.scheduler.time, "timer", "timer_due")
          end
        end
      end
    end
  end
end
