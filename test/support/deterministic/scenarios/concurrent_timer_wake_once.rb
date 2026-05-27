# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def concurrent_timer_wake_once(seed)
        run(seed, "concurrent_timer_wake_once") do |h|
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(h.store.current_time + 20, ctx.merge("woken" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          id = h.store.enqueue_workflow(name: "waiting", input: { "id" => "sig" })
          h.scheduler.schedule(actor: "worker", delay: 1, name: "park") { Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("waiting"), workflow_id: id) }
          5.times do |i|
            h.scheduler.schedule(actor: "timer-#{i}", delay: 20 + h.scheduler.rng.int(5), name: "wake_due_timers") { h.store.wake_due_timers(now: h.store.current_time + 100) }
          end
          h.scheduler.schedule(actor: "worker", delay: 40, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("waiting"), workflow_id: id) }
          h.check("wait completed once") { h.scheduler.trace.to_s.scan("wait_completed").length == 1 }
          h.check("workflow completed after timer wake") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end
    end
  end
end
