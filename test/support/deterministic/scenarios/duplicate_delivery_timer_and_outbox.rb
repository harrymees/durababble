# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def duplicate_delivery_timer_and_outbox(seed)
        run(seed, "duplicate_delivery_timer_and_outbox") do |h|
          h.network.duplicate_percent = 100
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(h.store.current_time + 10, ctx.merge("ok" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          workflow_id = h.store.enqueue_workflow(name: "waiting", input: { "id" => seed.to_s })
          h.scheduler.schedule(actor: "worker", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "worker").resume(
              h.workflows.fetch("waiting"),
              workflow_id:,
            )
          end
          h.network.send(source: "client-timer", target: "db", type: "timer", payload: {}) do
            resume_workflow_once(h, actor: "client-timer", workflow: h.workflows.fetch("waiting"), workflow_id:)
          end
          h.network.send(source: "producer", target: "db", type: "outbox", payload: {}) do
            h.store.enqueue_outbox(
              workflow_id:,
              topic: "email",
              payload: { "seed" => seed },
              key: "dup-email:#{seed}",
            )
          end
          h.scheduler.schedule(actor: "sender", delay: 25, name: "send") do
            message = h.store.claim_outbox(worker_id: "sender", lease_seconds: 10)
            h.store.ack_outbox(message.fetch("id"), worker_id: "sender") if message
          end
          h.scheduler.schedule(actor: "worker", delay: 30, name: "resume") do
            resume_workflow_once(h, actor: "worker", workflow: h.workflows.fetch("waiting"), workflow_id:)
          end
          h.check("duplicate network delivery occurred") { h.scheduler.trace.to_s.include?("network.duplicate") }
          h.check("wait completed once despite duplicate timer delivery") do
            h.store.workflow_history_for(workflow_id).count { |event| event.fetch("kind") == "step_completed" && event.fetch("command_id") == 0 } == 1
          end
          h.check("outbox message was idempotent despite duplicate producer delivery") do
            h.store.summary.fetch(:processed_outbox) == 1
          end
          h.check("workflow completed after duplicate timer delivery") do
            h.store.workflow(workflow_id).fetch("status") == "completed"
          end
        end
      end
    end
  end
end
