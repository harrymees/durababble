# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def rpc_workflow_rpc_response_matrix(seed)
        run(seed, "rpc_workflow_rpc_response_matrix") do |h|
          moved_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: moved_id, worker_id: "worker-a", lease_seconds: 10)
          worker_a = rpc_workflow_rpc_client(h, "worker-a") do |payload|
            h.store.steal_expired_leases!(now: 11)
            h.store.claim_workflow(workflow_id: moved_id, worker_id: "worker-b", lease_seconds: 60)
            workflow_rpc_handler(h, "worker-a").call(payload)
          end
          worker_b = rpc_workflow_rpc_client(h, "worker-b") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.retry_success", target: "worker-b")
            workflow_rpc_handler(h, "worker-b").call(payload)
          end
          moved_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-a" => worker_a, "worker-b" => worker_b },
            retry_on_stale: true,
          )
          h.scheduler.schedule(actor: "caller", delay: 5, name: "rpc_workflow_rpc") do
            moved_router.request(workflow_id: moved_id, command: "status", payload: { "request" => seed })
          end

          unavailable_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 1 })
          h.store.claim_workflow(workflow_id: unavailable_id, worker_id: "worker-a", lease_seconds: 60)
          unavailable_client = Object.new
          unavailable_client.define_singleton_method(:request) do |command, _payload|
            raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

            h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.unavailable", target: "worker-a")
            raise Durababble::WorkflowRpc::NodeUnavailable, "worker-a unavailable"
          end
          unavailable_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-a" => unavailable_client },
            retry_on_stale: false,
          )
          h.scheduler.schedule(actor: "caller", delay: 1, name: "rpc_unavailable") do
            unavailable_router.request(workflow_id: unavailable_id, command: "status", payload: {})
          rescue Durababble::WorkflowRpc::NodeUnavailable
            h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.node_unavailable_observed")
          end

          not_running_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 2 })
          worker_b = rpc_workflow_rpc_client(h, "worker-b")
          h.scheduler.schedule(actor: "caller", delay: 15, name: "rpc_not_running") do
            worker_b.request("workflow_rpc", {
              "workflow_id" => not_running_id,
              "command" => "status",
              "payload" => {},
            })
          rescue Durababble::WorkflowRpc::NoActiveLease
            h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.not_running_observed")
          end
          h.check("RPC CallTransient was used") { h.scheduler.trace.to_s.include?("rpc.call_transient") }
          h.check("RPC moved response was emitted") { h.scheduler.trace.to_s.include?("rpc.lease_moved") }
          h.check("RPC moved response decoded as stale lease") { h.scheduler.trace.to_s.include?("rpc.decode_moved") }
          h.check("RPC retry reached the new owner") { h.scheduler.trace.to_s.include?("rpc.retry_success") }
          h.check("RPC unavailable surfaced as node unavailable") do
            h.scheduler.trace.to_s.include?("rpc.node_unavailable_observed")
          end
          h.check("RPC not_running response decoded as no active lease") do
            h.scheduler.trace.to_s.include?("rpc.not_running_observed")
          end
        end
      end
    end
  end
end
