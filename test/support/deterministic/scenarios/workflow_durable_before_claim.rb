# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_durable_before_claim(seed)
        run(seed, "workflow_durable_before_claim") do |h|
          h.workflows["counter"] = counter_workflow
          h.scheduler.schedule(actor: "client", delay: h.scheduler.rng.int(20), name: "enqueue_then_crash") do
            h.store.enqueue_workflow(name: "counter", input: { "count" => 5 })
          end
          h.add_workers(["worker-a", "worker-b"], ticks: 12)
          h.check("pending workflow eventually completed") { h.store.summary.fetch(:completed_workflows) == 1 }
        end
      end
    end
  end
end
