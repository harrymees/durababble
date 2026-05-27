# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def target_activation_worker_pool_completion_isolation(seed)
        run(seed, "target_activation_worker_pool_completion_isolation") do |h|
          message_id = h.store.enqueue_inbox_message(
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
          visible_to_wrong_pool = h.store.target_activation(
            worker_pool: "pool-b",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
          )
          wrong_pool_complete = h.store.complete_target_activation(
            worker_pool: "pool-b",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            worker_id: "shared-worker",
          )
          after_wrong_complete = h.store.target_activation(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
          )
          h.scheduler.trace.event(
            h.scheduler.time,
            "pool-b",
            "wrong_pool_complete",
            visible: !!visible_to_wrong_pool,
            completed: !!wrong_pool_complete,
            still_active: !!after_wrong_complete,
          )

          right_pool_claim = h.store.claim_inbox_messages(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            worker_id: "shared-worker",
            lease_seconds: 30,
            limit: 1,
          )

          h.check("wrong worker pool cannot read another pool activation") do
            visible_to_wrong_pool.nil?
          end
          h.check("wrong worker pool cannot complete another pool activation") do
            wrong_pool_complete.nil? && after_wrong_complete&.fetch("locked_by") == "shared-worker"
          end
          h.check("right worker pool can still claim the inbox head") do
            activation && right_pool_claim.map { |row| row.fetch("id") } == [message_id]
          end
        end
      end
    end
  end
end
