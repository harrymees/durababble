# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def inbox_messages_for_worker_pool_isolation(seed)
        run(seed, "inbox_messages_for_worker_pool_isolation") do |h|
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

          wrong_pool = h.store.inbox_messages_for(
            worker_pool: "pool-b",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
          )
          right_pool = h.store.inbox_messages_for(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
          )
          all_pools = h.store.inbox_messages_for(
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
          )

          h.scheduler.trace.event(
            h.scheduler.time,
            "mailbox",
            "pool_filtered_inbox_inspection",
            wrong_pool: wrong_pool.map { |row| row.fetch("id") },
            right_pool: right_pool.map { |row| row.fetch("id") },
            all_pools: all_pools.map { |row| row.fetch("id") },
          )

          h.check("wrong worker pool cannot inspect another pool's inbox messages") do
            wrong_pool.empty?
          end
          h.check("right worker pool can inspect its inbox messages") do
            right_pool.map { |row| row.fetch("id") } == [message_id]
          end
          h.check("unscoped inspection still returns all target inbox messages") do
            all_pools.map { |row| row.fetch("id") } == [message_id]
          end
        end
      end
    end
  end
end
