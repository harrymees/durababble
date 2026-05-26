# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def grpc_workflow_rpc_transport_fault_matrix(seed)
        run(seed, "grpc_workflow_rpc_transport_fault_matrix") do |h|
          faults = [
            "timeout",
            "deadline_exceeded",
            "connection_reset",
            "eof",
            "unavailable",
            "response_timeout",
            "duplicate_response",
          ].rotate(h.scheduler.rng.int(7))
          handler_calls = Hash.new(0)

          faults.each_with_index do |fault, index|
            id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + index })
            node_id = "worker-#{index}"
            h.store.claim_workflow(workflow_id: id, worker_id: node_id, lease_seconds: 60)
            client = grpc_workflow_rpc_client(h, node_id, faults: [fault]) do |payload|
              handler_calls[fault] += 1
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.handler", fault:, target: node_id)
              workflow_rpc_handler(h, node_id).call(payload)
            end
            router = Durababble::WorkflowRpc::Router.new(
              store: h.store,
              rpc_clients: { node_id => client },
              retry_on_stale: true,
            )
            h.scheduler.schedule(actor: "caller", delay: index * 4, name: "grpc_fault:#{fault}") do
              router.request(workflow_id: id, command: "status", payload: { "fault" => fault })
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.transport_retry_success", fault:)
            end
          end

          h.check("timeout transport fault was injected") { h.scheduler.trace.to_s.include?("grpc.timeout") }
          h.check("deadline transport fault was injected") { h.scheduler.trace.to_s.include?("grpc.deadline_exceeded") }
          h.check("RST transport fault was injected") { h.scheduler.trace.to_s.include?("grpc.rst") }
          h.check("EOF transport fault was injected") { h.scheduler.trace.to_s.include?("grpc.eof") }
          h.check("unavailable transport fault was injected") { h.scheduler.trace.to_s.include?("grpc.unavailable") }
          h.check("response-timeout fault was injected after handler execution") do
            h.scheduler.trace.to_s.include?("grpc.response_timeout")
          end
          h.check("duplicate response delivery was modeled") { h.scheduler.trace.to_s.include?("grpc.duplicate_response") }
          h.check("each transport fault retried to success") do
            h.scheduler.trace.to_s.scan("grpc.transport_retry_success").length == faults.length
          end
          h.check("lost response can duplicate a transient handler invocation") do
            handler_calls["response_timeout"] == 2
          end
          h.check("explicit duplicate response can duplicate a transient handler invocation") do
            handler_calls["duplicate_response"] == 2
          end
        end
      end
    end
  end
end
