# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def stale_wait_timer_terminal_workflow(seed)
        run(seed, "stale_wait_timer_terminal_workflow") do |h|
          h.workflows["waiting"] = workflow = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(ctx.fetch("wake_at"), ctx.merge("woken" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          id = h.store.enqueue_workflow(name: "waiting", input: { "seed" => seed, "wake_at" => 10 })
          h.scheduler.schedule(actor: "parker", delay: 1, name: "park") do
            resume_workflow_once(h, actor: "parker", workflow:, workflow_id: id)
          end
          h.scheduler.schedule(actor: "timer-worker", delay: 12, name: "timer_resume") do
            resume_workflow_once(h, actor: "timer-worker", workflow:, workflow_id: id)
          end
          h.scheduler.schedule(actor: "timer", delay: 20 + h.scheduler.rng.int(5), name: "stale_timer") do
            woken = h.store.wake_due_timers(now: h.store.current_time + 11)
            event = woken.zero? ? "stale_wait_ignored" : "stale_wait_completed"
            h.scheduler.trace.event(h.scheduler.time, "timer", event, workflow_id: id, woken:)
          end
          h.check("stale wait timer was ignored") { h.scheduler.trace.to_s.include?("stale_wait_ignored") }
          h.check("terminal workflow remained completed") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end
    end
  end
end
