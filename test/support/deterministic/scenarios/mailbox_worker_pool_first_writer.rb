# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def mailbox_worker_pool_first_writer(seed)
        run(seed, "mailbox_worker_pool_first_writer") do |h|
          payload_a = { "method_name" => "write", "args" => ["a"], "kwargs" => {} }
          first_id = h.store.enqueue_inbox_message(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            message_kind: "tell",
            method_name: "write",
            payload: payload_a,
            idempotency_key: "pool-a-message",
          )

          payload_b = { "method_name" => "write", "args" => ["b"], "kwargs" => {} }
          second_id = h.store.enqueue_inbox_message(
            worker_pool: "pool-b",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            message_kind: "tell",
            method_name: "write",
            payload: payload_b,
            idempotency_key: "pool-b-message",
          )

          rows = h.store.inbox_messages_for(target_kind: "object", target_type: "counter", target_id: "shared-object")
          h.scheduler.trace.event(h.scheduler.time, "mailbox", "persisted_pools", pools: rows.map { |row| row.fetch("worker_pool") })

          wrong_pool_activation = h.store.claim_target_activation(
            worker_pool: "pool-b",
            worker_id: "pool-b-worker",
            lease_seconds: 30,
            target_kinds: ["object"],
            target_types: ["counter"],
          )
          right_pool_activation = h.store.claim_target_activation(
            worker_pool: "pool-a",
            worker_id: "pool-a-worker",
            lease_seconds: 30,
            target_kinds: ["object"],
            target_types: ["counter"],
          )
          right_pool_claim = h.store.claim_inbox_messages(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            worker_id: "pool-a-worker",
            lease_seconds: 30,
            limit: 10,
          )
          h.scheduler.trace.event(h.scheduler.time, "pool-a", "right_pool_claim", claimed: right_pool_claim.map { |row| row.fetch("id") })

          h.check("second inbox message stayed on the first materialized pool") do
            rows.map { |row| row.fetch("worker_pool") } == ["pool-a", "pool-a"]
          end
          h.check("wrong worker pool has no activation for the target") do
            wrong_pool_activation.nil?
          end
          h.check("first worker pool claimed both inbox messages") do
            right_pool_activation && right_pool_claim.map { |row| row.fetch("id") } == [first_id, second_id]
          end
        end
      end
    end
  end
end
