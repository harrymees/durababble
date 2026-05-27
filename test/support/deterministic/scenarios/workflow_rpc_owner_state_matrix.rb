# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_rpc_owner_state_matrix(seed)
        run(seed, "workflow_rpc_owner_state_matrix") do |h|
          moved_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: moved_id, worker_id: "worker-a", lease_seconds: 10)
          moved_worker_a = workflow_rpc_client(h, "worker-a") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.lookup", target: "worker-a")
            h.store.steal_expired_leases!(now: 11)
            h.store.claim_workflow(workflow_id: moved_id, worker_id: "worker-b", lease_seconds: 60)
            handler = workflow_rpc_handler(h, "worker-a")
            handler.call(payload)
          rescue Durababble::WorkflowRpc::StaleLease
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.stale_rejected", stale: "worker-a")
            raise
          end
          moved_worker_b = workflow_rpc_client(h, "worker-b") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.retry_success", target: "worker-b")
            workflow_rpc_handler(h, "worker-b").call(payload)
          end
          moved_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-a" => moved_worker_a, "worker-b" => moved_worker_b },
            retry_on_stale: true,
          )
          h.scheduler.schedule(actor: "caller", delay: 5, name: "workflow_rpc_moved") do
            moved_router.request(workflow_id: moved_id, command: "status", payload: { "request" => seed })
          end

          h.workflows["counter"] = counter_workflow
          no_active_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 1 })
          h.store.claim_workflow(workflow_id: no_active_id, worker_id: "worker-c", lease_seconds: 30)
          no_active_worker_c = workflow_rpc_client(h, "worker-c") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.lookup", target: "worker-a")
            h.store.steal_expired_leases!(now: 31)
            workflow_rpc_handler(h, "worker-c").call(payload)
          rescue Durababble::WorkflowRpc::NoActiveLease
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.no_active_holder_rejected", stale: "worker-c")
            raise
          end
          restarted_worker_d = workflow_rpc_client(h, "worker-d") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.internal_start_retry_success", target: "worker-d")
            workflow_rpc_handler(h, "worker-d").call(payload)
          end
          starter = Durababble::WorkflowRpc::LeaseStarter.new(store: h.store, worker_ids: ["worker-d"], lease_seconds: 60)
          no_active_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-c" => no_active_worker_c, "worker-d" => restarted_worker_d },
            retry_on_stale: true,
            start_workflow: starter,
          )
          h.scheduler.schedule(actor: "caller", delay: 15, name: "workflow_rpc_no_active") do
            no_active_router.request(workflow_id: no_active_id, command: "status", payload: { "request" => seed })
          end

          shutdown_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 2 })
          h.store.claim_workflow(workflow_id: shutdown_id, worker_id: "worker-e", lease_seconds: 60)
          shutdown_worker = workflow_rpc_client(h, "worker-e") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.lookup", target: "worker-e")
            h.store.complete_workflow(shutdown_id, result: { "shutdown" => true })
            workflow_rpc_handler(h, "worker-e") do
              h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.unowned_handler_ran")
              { "bad" => true }
            end.call(payload)
          rescue Durababble::WorkflowRpc::WorkflowNotRunning
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.shutdown_rejected", stale: "worker-e")
            raise
          end
          shutdown_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-e" => shutdown_worker },
            retry_on_stale: true,
          )
          h.scheduler.schedule(actor: "caller", delay: 25, name: "workflow_rpc_shutdown") do
            shutdown_router.request(workflow_id: shutdown_id, command: "status", payload: {})
          rescue Durababble::WorkflowRpc::WorkflowNotRunning
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.no_retry_after_shutdown")
          end

          h.check("workflow lease moved to worker-b") { h.store.workflow(moved_id).fetch("locked_by") == "worker-b" }
          h.check("stale holder rejected") { h.scheduler.trace.to_s.include?("workflow_rpc.stale_rejected") }
          h.check("retry reached new holder") { h.scheduler.trace.to_s.include?("workflow_rpc.retry_success") }
          h.check("stale no-active RPC rejected") { h.scheduler.trace.to_s.include?("workflow_rpc.no_active_holder_rejected") }
          h.check("workflow was started internally") { h.store.workflow(no_active_id).fetch("locked_by") == "worker-d" }
          h.check("RPC retried after internal start") { h.scheduler.trace.to_s.include?("workflow_rpc.internal_start_retry_success") }
          h.check("shutdown stale RPC rejected") { h.scheduler.trace.to_s.include?("workflow_rpc.shutdown_rejected") }
          h.check("unowned handler did not run") { !h.scheduler.trace.to_s.include?("workflow_rpc.unowned_handler_ran") }
        end
      end
    end
  end
end
