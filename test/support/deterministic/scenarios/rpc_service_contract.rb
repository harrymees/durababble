# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def rpc_service_contract(seed)
        run(seed, "rpc_service_contract") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: id, worker_id: "worker-a", lease_seconds: 10)
          events = []
          service = Durababble::Rpc::Service.new(
            node_id: "worker-a",
            store: h.store,
            worker_pool: "default",
            workflow_handlers: { "status" => ->(payload) { { "node" => "worker-a", "seed" => payload.fetch("seed") } } },
            transient_handler: ->(request:, args:) { { "method" => request["method"], "args" => args } },
            node_directory: Durababble::Rpc::NodeDirectory.new("worker-b" => "virtual://worker-b"),
            authorize: nil,
            awaken_batch: ->(**event) { events << [:awaken_batch, event] },
            evict_lease: ->(**event) { events << [:evict_lease, event] },
            deliver_message: ->(**event) { events << [:deliver_message, event] },
          )

          h.scheduler.schedule(actor: "caller", delay: 1, name: "rpc_awaken_batch") do
            service.awaken_batch(
              Durababble::Rpc::Messages::AwakenBatchRequest.new(worker_pool: "default", workflow_ids: [id]),
              nil,
            )
            h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.awaken_batch")
          end
          h.scheduler.schedule(actor: "caller", delay: 2, name: "rpc_evict_lease") do
            service.evict_lease(
              Durababble::Rpc::Messages::EvictLeaseRequest.new(
                worker_pool: "default",
                target_kind: "workflow",
                target_id: id,
              ),
              nil,
            )
            h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.evict_lease")
          end
          h.scheduler.schedule(actor: "caller", delay: 3, name: "rpc_deliver_message") do
            service.deliver_message(
              Durababble::Rpc::Messages::DeliverMessageRequest.new(
                worker_pool: "default",
                target_kind: "workflow",
                target_id: id,
              ),
              nil,
            )
            h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.deliver_message")
          end
          h.scheduler.schedule(actor: "caller", delay: 4, name: "rpc_call_transient") do
            response = service.call_transient(
              Durababble::Rpc::Messages::TransientRequest.new(
                worker_pool: "default",
                workflow_id: id,
                method: "status",
                args: Durababble::Rpc.dump({ "seed" => seed }),
              ),
              nil,
            )
            decoded = Durababble::Rpc::Client.decode_transient_response(response)
            h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.call_transient_ok", decoded:)
          end
          h.scheduler.schedule(actor: "caller", delay: 5, name: "rpc_call_object_transient") do
            response = service.call_transient(
              Durababble::Rpc::Messages::TransientRequest.new(
                worker_pool: "default",
                class_name: "Counter",
                durable_object_id: "counter-1",
                method: "balance",
                args: Durababble::Rpc.dump({ "seed" => seed }),
              ),
              nil,
            )
            decoded = Durababble::Rpc::Client.decode_transient_response(response)
            h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.call_object_transient_ok", decoded:)
          end
          h.scheduler.schedule(actor: "caller", delay: 6, name: "rpc_stale_deliver_message") do
            before = events.length
            h.store.steal_expired_leases!(now: 11)
            h.store.claim_workflow(workflow_id: id, worker_id: "worker-b", lease_seconds: 60)
            service.deliver_message(
              Durababble::Rpc::Messages::DeliverMessageRequest.new(
                worker_pool: "default",
                target_kind: "workflow",
                target_id: id,
              ),
              nil,
            )
            h.scheduler.trace.event(
              h.scheduler.time,
              "rpc",
              events.length == before ? "rpc.deliver_message_stale_ack" : "rpc.deliver_message_stale_work",
            )
          end
          h.check("AwakenBatch was served") { h.scheduler.trace.to_s.include?("rpc.awaken_batch") }
          h.check("EvictLease was served") { h.scheduler.trace.to_s.include?("rpc.evict_lease") }
          h.check("DeliverMessage was served for the active owner") { h.scheduler.trace.to_s.include?("rpc.deliver_message") }
          h.check("CallTransient decoded a workflow response") { h.scheduler.trace.to_s.include?("rpc.call_transient_ok") }
          h.check("CallTransient decoded an object response") do
            h.scheduler.trace.to_s.include?("rpc.call_object_transient_ok")
          end
          h.check("stale DeliverMessage returned without doing work") do
            h.scheduler.trace.to_s.include?("rpc.deliver_message_stale_ack")
          end
        end
      end
    end
  end
end
