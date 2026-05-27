# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def lease_expiry(seed)
        run(seed, "lease_expiry") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => 3 })
          h.store.claim_workflow(workflow_id: id, worker_id: "crashed-worker", lease_seconds: 10)
          h.scheduler.schedule(actor: "reaper", delay: 20, name: "steal_expired") { h.store.steal_expired_leases! }
          h.add_workers(["replacement-worker"], ticks: 5)
        end
      end
    end
  end
end
