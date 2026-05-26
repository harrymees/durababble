# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def zombie_workflow_heartbeat_after_expiry(seed)
        run(seed, "zombie_workflow_heartbeat_after_expiry") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: id, worker_id: "zombie", lease_seconds: 10)
          h.scheduler.schedule(actor: "zombie", delay: 20, name: "heartbeat_after_expiry") do
            h.store.heartbeat(workflow_id: id, worker_id: "zombie", lease_seconds: 60)
            if h.store.workflow_owned?(workflow_id: id, worker_id: "zombie")
              h.scheduler.trace.event(h.scheduler.time, "zombie", "zombie_heartbeat_renewed", workflow_id: id)
            else
              h.scheduler.trace.event(h.scheduler.time, "zombie", "zombie_heartbeat_rejected", workflow_id: id)
            end
          end
          h.check("expired heartbeat was rejected") { h.scheduler.trace.to_s.include?("zombie_heartbeat_rejected") }
          h.check("zombie did not regain ownership") { !h.store.workflow_owned?(workflow_id: id, worker_id: "zombie") }
        end
      end
    end
  end
end
