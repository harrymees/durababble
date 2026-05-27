# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def step_retry_policy_recovery(seed)
        run(seed, "step_retry_policy_recovery") do |h|
          attempts = 0
          h.workflows["retry"] = workflow_class("retry") do
            test_step("flaky", retry_policy: { initial_interval: 10, backoff_coefficient: 2, maximum_interval: 15, maximum_attempts: 3 }) do |ctx|
              attempts += 1
              h.scheduler.trace.event(h.scheduler.time, "worker", "step_retry_attempt", attempt: attempts)
              raise "transient #{attempts}" if attempts < 3

              ctx.merge("attempts" => attempts)
            end
          end
          id = h.store.enqueue_workflow(name: "retry", input: {})
          h.scheduler.schedule(actor: "worker-a", delay: 1 + h.scheduler.rng.int(3), name: "first_attempt") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a").resume(h.workflows.fetch("retry"), workflow_id: id)
          end
          h.scheduler.schedule(actor: "worker-b", delay: 8, name: "restart_before_due") do
            claimed = h.store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: Engine::DEFAULT_LEASE_SECONDS)
            h.scheduler.trace.event(h.scheduler.time, "worker-b", "step_retry_not_due") unless claimed
          end
          h.scheduler.schedule(actor: "worker-b", delay: 20, name: "second_attempt_after_restart") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-b").resume(h.workflows.fetch("retry"), workflow_id: id)
          end
          h.scheduler.schedule(actor: "worker-c", delay: 36, name: "final_attempt_after_restart") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-c").resume(h.workflows.fetch("retry"), workflow_id: id)
          end
          h.check("retry waited for due time") { h.scheduler.trace.to_s.include?("step_retry_not_due") }
          h.check("workflow completed after durable retries") { h.store.workflow(id).fetch("status") == "completed" }
          h.check("attempt history records retries") { h.store.step_attempts_for(id).map { |a| a.fetch("status") } == ["failed", "failed", "completed"] }
        end
      end
    end
  end
end
