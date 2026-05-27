# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def mailbox_worker_pool_isolation(seed)
        run(seed, "mailbox_worker_pool_isolation") do |h|
          payload = { "method_name" => "write", "args" => [], "kwargs" => {} }
          message_id = h.store.enqueue_inbox_message(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            message_kind: "tell",
            method_name: "write",
            payload:,
            idempotency_key: "pool-a-message",
          )

          wrong_pool_claim = h.store.claim_inbox_messages(
            worker_pool: "pool-b",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            worker_id: "wrong-pool-worker",
            lease_seconds: 30,
            limit: 1,
          )
          h.scheduler.trace.event(h.scheduler.time, "pool-b", "wrong_pool_claim", claimed: wrong_pool_claim.map { |row| row.fetch("id") })

          right_pool_claim = h.store.claim_inbox_messages(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            worker_id: "pool-a-worker",
            lease_seconds: 30,
            limit: 1,
          )
          h.scheduler.trace.event(h.scheduler.time, "pool-a", "right_pool_claim", claimed: right_pool_claim.map { |row| row.fetch("id") })

          h.check("wrong worker pool could not claim the inbox head") do
            wrong_pool_claim.empty?
          end
          h.check("right worker pool claimed the original message") do
            right_pool_claim.map { |row| row.fetch("id") } == [message_id]
          end
        end
      end
    end
  end
end
