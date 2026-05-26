# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def grpc_wakeup_fault_matrix(seed)
        run(seed, "grpc_wakeup_fault_matrix") do |h|
          h.workflows["counter"] = counter_workflow
          active_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          recovery_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 1 })
          h.store.claim_workflow(workflow_id: active_id, worker_id: "worker-a", lease_seconds: 60)
          events = []
          service = Durababble::Rpc::Service.new(
            node_id: "worker-a",
            store: h.store,
            worker_pool: "default",
            workflow_handlers: {},
            transient_handler: nil,
            node_directory: Durababble::Rpc::NodeDirectory.new,
            authorize: nil,
            awaken_batch: ->(**event) { events << [:awaken_batch, event] },
            evict_lease: ->(**event) { events << [:evict_lease, event] },
            deliver_message: ->(**event) { events << [:deliver_message, event] },
          )
          operations = [
            ["awaken_batch", "drop"],
            ["awaken_batch", "duplicate"],
            ["deliver_message", "timeout"],
            ["deliver_message", "connection_reset"],
            ["deliver_message", "duplicate"],
            ["evict_lease", "eof"],
            ["evict_lease", "unavailable"],
          ].rotate(h.scheduler.rng.int(7))

          operations.each_with_index do |(method_name, fault), index|
            h.scheduler.schedule(actor: "caller", delay: index * 3, name: "grpc_wakeup:#{method_name}:#{fault}") do
              grpc_faulty_unary(h, method_name, target: "worker-a", fault:) do
                call_grpc_service_method(service, method_name, workflow_id: active_id)
              end
            rescue Durababble::WorkflowRpc::NodeUnavailable
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.wakeup_fault_observed", method: method_name, fault:)
            end
          end

          h.add_workers(["polling-worker"], ticks: 8)
          h.scheduler.schedule(actor: "reaper", delay: 80, name: "release_active_for_recovery") do
            h.store.steal_expired_leases!(now: h.scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
          end
          h.check("wakeup drop was injected") { h.scheduler.trace.to_s.include?("grpc.drop") }
          h.check("wakeup duplicate was injected") { h.scheduler.trace.to_s.include?("grpc.duplicate") }
          h.check("wakeup timeout was observed") { h.scheduler.trace.to_s.include?("grpc.wakeup_fault_observed") }
          h.check("polling completed workflow despite wakeup transport faults") do
            h.store.workflow(recovery_id).fetch("status") == "completed"
          end
          h.check("duplicate wakeups did not create durable duplicate effects") do
            events.count { |event, _| event == :awaken_batch } <= 2 &&
              events.count { |event, _| event == :deliver_message } <= 2
          end
        end
      end
    end
  end
end
