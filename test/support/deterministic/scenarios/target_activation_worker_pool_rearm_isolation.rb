# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def target_activation_worker_pool_rearm_isolation(seed)
        run(seed, "target_activation_worker_pool_rearm_isolation") do |h|
          h.store.enqueue_inbox_message(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            message_kind: "tell",
            method_name: "write",
            payload: { "method_name" => "write", "args" => [], "kwargs" => {} },
            idempotency_key: "pool-a-message",
          )

          activation = h.store.claim_target_activation(
            worker_pool: "pool-a",
            worker_id: "shared-worker",
            lease_seconds: 30,
            target_kinds: ["object"],
            target_types: ["counter"],
          )

          h.store.rearm_target_activation(
            worker_pool: "pool-b",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            ready_at: h.scheduler.time,
          )

          after_wrong_rearm = h.store.target_activation(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
          )
          stolen_claim = h.store.claim_target_activation(
            worker_pool: "pool-a",
            worker_id: "other-worker",
            lease_seconds: 30,
            target_kinds: ["object"],
            target_types: ["counter"],
            now: h.scheduler.time,
          )
          h.scheduler.trace.event(
            h.scheduler.time,
            "pool-b",
            "wrong_pool_rearm",
            status: after_wrong_rearm&.fetch("status"),
            locked_by: after_wrong_rearm&.fetch("locked_by"),
            stolen: !!stolen_claim,
          )

          h.check("wrong worker pool cannot rearm another pool activation") do
            activation &&
              after_wrong_rearm&.fetch("status") == "running" &&
              after_wrong_rearm&.fetch("locked_by") == "shared-worker"
          end
          h.check("wrong worker pool cannot make the activation reclaimable") do
            stolen_claim.nil?
          end
        end
      end
    end
  end
end
