# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def attempt_history_append_only(seed)
        run(seed, "attempt_history_append_only") do |h|
          h.workflows["flaky"] = workflow_class("flaky") do
            test_step("fail", retry_policy: { schedule: [0, 0], maximum_attempts: 3 }) { |_ctx| raise "boom" }
          end
          id = h.store.enqueue_workflow(name: "flaky", input: { "seed" => seed })
          3.times do |i|
            h.scheduler.schedule(actor: "worker-#{i}", delay: i * 20, name: "attempt") do
              h.store.make_workflow_due!(id, now: h.scheduler.time) if i.positive?
              Durababble::Engine.new(store: h.store, worker_id: "worker-#{i}").resume(h.workflows.fetch("flaky"), workflow_id: id)
            end
          end
          h.check("each retry appended an attempt") { h.store.step_attempts_for(id).length == 3 }
          h.check("attempts are failed terminal records") { h.store.step_attempts_for(id).all? { |a| a.fetch("status") == "failed" } }
        end
      end
    end
  end
end
