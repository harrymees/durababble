# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def store_fault_after_wait_recorded(seed)
        run(seed, "store_fault_after_wait_recorded") do |h|
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(h.store.current_time + 20, ctx.merge("ok" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          h.store.fault_plan.fail_after(:record_wait, message: "lost connection after durable wait write")
          id = h.store.enqueue_workflow(name: "waiting", input: { "id" => seed.to_s })
          h.scheduler.schedule(
            actor: "faulty-worker",
            delay: h.scheduler.rng.int(5),
            name: "fault_after_wait_recorded",
          ) do
            Durababble::Engine.new(
              store: h.store,
              worker_id: "faulty-worker",
              lease_seconds: 10,
            ).resume(h.workflows.fetch("waiting"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "faulty-worker", "store_fault_observed", workflow_id: id)
          end
          h.scheduler.schedule(actor: "recover", delay: 25, name: "resume") do
            resume_workflow_once(h, actor: "recover", workflow: h.workflows.fetch("waiting"), workflow_id: id)
          end
          h.check("fault was injected after wait write") { h.scheduler.trace.to_s.include?("fault.injected") }
          h.check("workflow completed after recovering from durable wait write") do
            h.store.workflow(id).fetch("status") == "completed"
          end
        end
      end
    end
  end
end
