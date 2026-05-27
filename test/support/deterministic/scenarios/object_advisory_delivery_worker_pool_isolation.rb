# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_advisory_delivery_worker_pool_isolation(seed)
        run(seed, "object_advisory_delivery_worker_pool_isolation") do |h|
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

          claimed = h.store.claim_inbox_messages(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            worker_id: "pool-a-worker@127.0.0.1:12345",
            lease_seconds: 30,
            limit: 1,
          )

          deliveries = []
          client = Object.new
          client.define_singleton_method(:deliver_message) do |**kwargs|
            deliveries << kwargs
            true
          end
          factory = ->(_address) { client }

          wrong_pool_delivered = h.store.deliver_target_message(
            worker_pool: "pool-b",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            client_factory: factory,
          )
          right_pool_delivered = h.store.deliver_target_message(
            worker_pool: "pool-a",
            target_kind: "object",
            target_type: "counter",
            target_id: "shared-object",
            client_factory: factory,
          )

          h.scheduler.trace.event(
            h.scheduler.time,
            "mailbox",
            "object_delivery_pool_scope",
            claimed: claimed.map { |row| row.fetch("id") },
            wrong_pool_delivered:,
            right_pool_delivered:,
            deliveries: deliveries.map { |delivery| delivery.fetch(:worker_pool) },
          )

          h.check("object advisory delivery ignores a lease from another worker pool") do
            !wrong_pool_delivered && deliveries.count { |delivery| delivery.fetch(:worker_pool) == "pool-b" }.zero?
          end
          h.check("object advisory delivery still reaches the active pool lease") do
            right_pool_delivered && deliveries.map { |delivery| delivery.fetch(:worker_pool) } == ["pool-a"]
          end
          h.check("object delivery used the claimed inbox lease") do
            claimed.map { |row| row.fetch("id") } == [message_id]
          end
        end
      end
    end
  end
end
