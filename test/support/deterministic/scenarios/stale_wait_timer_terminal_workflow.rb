# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def stale_wait_timer_terminal_workflow(seed)
        run(seed, "stale_wait_timer_terminal_workflow") do |h|
          id = h.store.create_workflow(name: "waiting", input: { "seed" => seed })
          h.store.record_step_started(workflow_id: id, position: 0, name: "wait")
          h.store.record_wait(
            workflow_id: id,
            position: 0,
            name: "wait",
            wait_request: Durababble.wait_until(h.store.current_time + 10, { "seed" => seed }),
          )
          h.store.wake_due_timers(now: h.store.current_time + 11)
          h.store.complete_workflow(id, result: { "done" => true })
          h.scheduler.schedule(actor: "timer", delay: h.scheduler.rng.int(5), name: "stale_timer") do
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
