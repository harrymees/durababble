# typed: true
# frozen_string_literal: true

require "digest"

require_relative "harness"

module Durababble
  module Deterministic
    module Scenarios
      extend self
      include Kernel

      #: (untyped) -> untyped
      def fetch(name)
        method(name).to_proc
      rescue NameError
        raise ArgumentError, "unknown deterministic scenario: #{name}"
      end

      #: (untyped) -> untyped
      def multi_worker_counter(seed)
        run(seed, "multi_worker_counter") do |h|
          workflow = counter_workflow
          h.workflows["counter"] = workflow
          8.times do |i|
            h.network.send(source: "client-#{i}", target: "db", type: "enqueue") do
              h.store.enqueue_workflow(name: "counter", input: { "count" => i })
            end
          end
          h.add_workers(["worker-a", "worker-b", "worker-c"], ticks: 18)
        end
      end

      #: (untyped) -> untyped
      def waits_fences_and_outbox(seed)
        run(seed, "waits_fences_and_outbox") do |h|
          h.workflows["counter"] = counter_workflow
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(h.store.current_time + 60, ctx.merge("approved" => true)) }
            test_step("finish") { |ctx| ctx.merge("finished" => true) }
          end

          ids = []
          3.times { |i| ids << h.store.enqueue_workflow(name: "counter", input: { "count" => i }) }
          h.store.enqueue_workflow(name: "waiting", input: { "id" => "req" })
          h.add_workers(["worker-a", "worker-b"], ticks: 15)
          h.scheduler.schedule(actor: "client-timer", delay: 120, name: "wake_due_timers") { h.store.wake_due_timers }
          h.scheduler.schedule(actor: "client-fence", delay: 40, name: "fence") do
            h.store.with_fence(workflow_id: ids.first, key: "charge") { { "charge" => "ok" } }
            h.store.with_fence(workflow_id: ids.first, key: "charge") { { "charge" => "duplicate" } }
          end
          h.scheduler.schedule(actor: "client-outbox", delay: 70, name: "outbox") do
            outbox = h.store.enqueue_outbox(workflow_id: ids.first, topic: "email", payload: { "to" => "x" }, key: "email")
            message = h.store.claim_outbox(worker_id: "sender", lease_seconds: 20)
            h.store.ack_outbox(outbox, worker_id: message.fetch("locked_by"))
          end
        end
      end

      #: (untyped) -> untyped
      def lease_expiry(seed)
        run(seed, "lease_expiry") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => 3 })
          h.store.claim_workflow(workflow_id: id, worker_id: "crashed-worker", lease_seconds: 10)
          h.scheduler.schedule(actor: "reaper", delay: 20, name: "steal_expired") { h.store.steal_expired_leases! }
          h.add_workers(["replacement-worker"], ticks: 5)
        end
      end

      #: (untyped) -> untyped
      def outbox_lease_expiry(seed)
        run(seed, "outbox_lease_expiry") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => 1 })
          outbox = h.store.enqueue_outbox(workflow_id: id, topic: "email", payload: { "to" => "x" }, key: "email")
          h.store.claim_outbox(worker_id: "crashed-sender", lease_seconds: 10)
          h.scheduler.schedule(actor: "sender-b", delay: 20, name: "recover_outbox") do
            message = h.store.claim_outbox(worker_id: "sender-b", lease_seconds: 10)
            h.store.ack_outbox(outbox, worker_id: message.fetch("locked_by"))
          end
        end
      end

      #: (untyped) -> untyped
      def timer_and_partition(seed)
        run(seed, "timer_and_partition") do |h|
          h.workflows["timer"] = workflow_class("timer") do
            test_step("sleep") { |ctx| Durababble.wait_until(ctx.fetch("wake_at"), ctx) }
            test_step("finish") { |ctx| ctx.merge("timer_done" => true) }
          end
          h.network.partition("partitioned-client", "db")
          h.network.send(source: "partitioned-client", target: "db", type: "enqueue") { h.store.enqueue_workflow(name: "timer", input: { "wake_at" => 50 }) }
          h.scheduler.schedule(actor: "network", delay: 10, name: "heal") { h.network.heal("partitioned-client", "db") }
          h.scheduler.schedule(actor: "healed-client", delay: 12, name: "enqueue") { h.store.enqueue_workflow(name: "timer", input: { "wake_at" => 50 }) }
          h.add_workers(["worker-a", "worker-b"], ticks: 15)
          h.scheduler.schedule(actor: "timer", delay: 55, name: "wake_due_timers") { h.store.wake_due_timers }
        end
      end

      #: (untyped) -> untyped
      def bug_duplicate_completion(seed)
        run(seed, "bug_duplicate_completion") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id, worker_id: "bug", lease_seconds: 10)
          h.store.record_step_started(workflow_id: id, position: 0, name: "broken")
          h.store.complete_workflow(id, result: { "count" => seed })
        end
      end

      #: (untyped) -> untyped
      def bug_invalid_store_shape(seed)
        run(seed, "bug_invalid_store_shape") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id)
          base = h.store.workflow(id)

          # Impossible shapes injected via the store's test-only overlay (the real
          # schema's NOT NULL / FK constraints would reject these), so the harness
          # invariant checkers see the same corrupt state they must flag.
          h.store.inject_workflow(base.merge(
            "id" => "bad-status-workflow",
            "status" => "mystery",
            "locked_by" => nil,
            "locked_until" => nil,
          ))
          h.store.inject_workflow(base.merge(
            "id" => "partial-lease-workflow",
            "status" => "pending",
            "locked_by" => "worker-a",
            "locked_until" => nil,
          ))
          h.store.inject_workflow(base.merge(
            "id" => "locked-waiting-workflow",
            "status" => "waiting",
            "locked_by" => "stale",
            "locked_until" => h.scheduler.time + 10,
          ))
          h.store.inject_workflow(base.merge(
            "id" => "terminal-live-step-workflow",
            "status" => "completed",
            "locked_by" => nil,
            "locked_until" => nil,
          ))
          h.store.inject_step({
            "workflow_id" => "missing-workflow",
            "position" => 0,
            "name" => "missing_owner",
            "status" => "completed",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "missing-workflow-attempt",
            "workflow_id" => "missing-workflow",
            "position" => 0,
            "name" => "missing_owner",
            "status" => "completed",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_step({
            "workflow_id" => id,
            "position" => 0,
            "name" => "orphaned_step",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_step({
            "workflow_id" => "other-workflow",
            "position" => 4,
            "name" => "bad_step",
            "status" => "mystery",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
            "__group_id" => id,
            "__position_key" => 3,
          })
          h.store.inject_attempt({
            "id" => "mismatched-step-attempt",
            "workflow_id" => id,
            "position" => 3,
            "name" => "other_name",
            "status" => "completed",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_step({
            "workflow_id" => id,
            "position" => 9,
            "name" => "duplicate_completed_a",
            "status" => "completed",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
            "__position_key" => 4,
          })
          h.store.inject_step({
            "workflow_id" => id,
            "position" => 9,
            "name" => "duplicate_completed_b",
            "status" => "completed",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
            "__position_key" => 5,
          })
          h.store.inject_step({
            "workflow_id" => id,
            "position" => 8,
            "name" => "multi_live",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "live-attempt-a",
            "workflow_id" => id,
            "position" => 8,
            "name" => "multi_live",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "live-attempt-b",
            "workflow_id" => id,
            "position" => 8,
            "name" => "multi_live",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_step({
            "workflow_id" => "terminal-live-step-workflow",
            "position" => 0,
            "name" => "still_running",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "terminal-live-attempt",
            "workflow_id" => "terminal-live-step-workflow",
            "position" => 0,
            "name" => "still_running",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "bad-attempt",
            "workflow_id" => id,
            "position" => 1,
            "name" => "missing_step",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "bad-status-attempt",
            "workflow_id" => "other-workflow",
            "position" => 99,
            "name" => "bad_status",
            "status" => "mystery",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_wait({
            "id" => "bad-wait",
            "workflow_id" => id,
            "position" => 2,
            "kind" => "event",
            "event_key" => "missing-step",
            "wake_at" => nil,
            "context" => {},
            "payload" => nil,
            "status" => "completed",
          })
          h.store.inject_wait({
            "id" => "bad-status-wait",
            "workflow_id" => id,
            "position" => 0,
            "kind" => "event",
            "event_key" => "bad-status",
            "wake_at" => nil,
            "context" => {},
            "payload" => nil,
            "status" => "mystery",
          })
          h.store.inject_wait({
            "id" => "completed-running-step-wait",
            "workflow_id" => id,
            "position" => 0,
            "kind" => "event",
            "event_key" => "running-step",
            "wake_at" => nil,
            "context" => {},
            "payload" => nil,
            "status" => "completed",
          })
          h.store.inject_wait({
            "id" => "missing-workflow-wait",
            "workflow_id" => "missing-workflow",
            "position" => 9,
            "kind" => "event",
            "event_key" => "missing-workflow",
            "wake_at" => nil,
            "context" => {},
            "payload" => nil,
            "status" => "pending",
          })
          h.store.inject_outbox({
            "id" => "bad-outbox",
            "workflow_id" => "missing-workflow",
            "topic" => "email",
            "payload" => {},
            "key" => "bad-outbox",
            "status" => "processing",
            "locked_by" => nil,
            "locked_until" => nil,
          })
          h.store.inject_outbox({
            "id" => "bad-status-outbox",
            "workflow_id" => id,
            "topic" => "email",
            "payload" => {},
            "key" => "bad-status-outbox",
            "status" => "mystery",
            "locked_by" => nil,
            "locked_until" => nil,
          })
        end
      end

      #: (untyped) -> untyped
      def bug_stuck_fence(seed)
        run(seed, "bug_stuck_fence") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id)
          # A fence left running by a crashed holder, its lease already expired and
          # never reclaimed — the stuck-fence checker must flag it.
          h.store.inject_fence({
            "workflow_id" => id,
            "key" => "charge",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "locked_by" => "crashed-worker",
            "locked_until" => h.scheduler.time - 1,
          })
        end
      end

      #: (untyped) -> untyped
      def bug_stuck_outbox(seed)
        run(seed, "bug_stuck_outbox") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id)
          # An outbox message left `processing` by a crashed holder, its lease
          # already expired and never reclaimed — the stuck-outbox checker must
          # flag it (symmetric to the stuck-fence fixture).
          h.store.inject_outbox({
            "id" => "stuck-outbox",
            "workflow_id" => id,
            "topic" => "email",
            "payload" => {},
            "key" => "stuck-outbox",
            "status" => "processing",
            "locked_by" => "crashed-worker",
            "locked_until" => h.scheduler.time - 1,
          })
        end
      end

      #: (untyped) -> untyped
      def bug_abandoned_runnable_workflow(seed)
        run(seed, "bug_abandoned_runnable_workflow") do |h|
          h.expect_settled!
          h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          # No workers ever run: the workflow is left pending and immediately
          # runnable, which the liveness checker must flag.
        end
      end

      #: (untyped) -> untyped
      def bug_unmet_effect_expectation(seed)
        run(seed, "bug_unmet_effect_expectation") do |h|
          h.expect_side_effects(1)
          h.expect_processed_outbox(1)
          h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          # No fence is ever acquired and no outbox message is processed, so the
          # declared exactly-once expectations are violated.
        end
      end

      #: (untyped) -> untyped
      def rpc_fault_injection(seed)
        run(seed, "rpc_fault_injection") do |h|
          outcomes = ["success", "timeout", "connection_error", "eof", "remote_error", "idle_disconnect_reconnect"]
          outcomes.rotate(h.scheduler.rng.int(outcomes.length)).each_with_index do |outcome, index|
            h.scheduler.schedule(actor: "rpc-client", delay: index * 3, name: "rpc:#{outcome}") do
              h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.request", id: index, outcome:)
              case outcome
              when "success"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.success", id: index)
              when "timeout"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.timeout", id: index)
              when "connection_error"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.connection_error", id: index)
              when "eof"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.eof", id: index)
              when "remote_error"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.remote_error", id: index)
              when "idle_disconnect_reconnect"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.idle_disconnect", id: index)
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.reconnect", id: index)
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.success", id: index)
              end
            end
          end
          h.check("success path observed") { h.scheduler.trace.to_s.include?("rpc.success") }
          h.check("timeout fault observed") { h.scheduler.trace.to_s.include?("rpc.timeout") }
          h.check("connection fault observed") { h.scheduler.trace.to_s.include?("rpc.connection_error") }
          h.check("eof fault observed") { h.scheduler.trace.to_s.include?("rpc.eof") }
          h.check("remote error observed") { h.scheduler.trace.to_s.include?("rpc.remote_error") }
          h.check("idle disconnect recovery observed") { h.scheduler.trace.to_s.include?("rpc.reconnect") }
        end
      end

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

      #: (untyped) -> untyped
      def grpc_workflow_rpc_response_matrix(seed)
        run(seed, "grpc_workflow_rpc_response_matrix") do |h|
          moved_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: moved_id, worker_id: "worker-a", lease_seconds: 10)
          worker_a = grpc_workflow_rpc_client(h, "worker-a") do |payload|
            h.store.steal_expired_leases!(now: 11)
            h.store.claim_workflow(workflow_id: moved_id, worker_id: "worker-b", lease_seconds: 60)
            workflow_rpc_handler(h, "worker-a").call(payload)
          end
          worker_b = grpc_workflow_rpc_client(h, "worker-b") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.retry_success", target: "worker-b")
            workflow_rpc_handler(h, "worker-b").call(payload)
          end
          moved_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-a" => worker_a, "worker-b" => worker_b },
            retry_on_stale: true,
          )
          h.scheduler.schedule(actor: "caller", delay: 5, name: "grpc_workflow_rpc") do
            moved_router.request(workflow_id: moved_id, command: "status", payload: { "request" => seed })
          end

          unavailable_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 1 })
          h.store.claim_workflow(workflow_id: unavailable_id, worker_id: "worker-a", lease_seconds: 60)
          unavailable_client = Object.new
          unavailable_client.define_singleton_method(:request) do |command, _payload|
            raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.unavailable", target: "worker-a")
            raise Durababble::WorkflowRpc::NodeUnavailable, "worker-a unavailable"
          end
          unavailable_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-a" => unavailable_client },
            retry_on_stale: false,
          )
          h.scheduler.schedule(actor: "caller", delay: 1, name: "grpc_unavailable") do
            unavailable_router.request(workflow_id: unavailable_id, command: "status", payload: {})
          rescue Durababble::WorkflowRpc::NodeUnavailable
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.node_unavailable_observed")
          end

          not_running_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 2 })
          worker_b = grpc_workflow_rpc_client(h, "worker-b")
          h.scheduler.schedule(actor: "caller", delay: 15, name: "grpc_not_running") do
            worker_b.request("workflow_rpc", {
              "workflow_id" => not_running_id,
              "command" => "status",
              "payload" => {},
            })
          rescue Durababble::WorkflowRpc::NoActiveLease
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.not_running_observed")
          end
          h.check("gRPC CallTransient was used") { h.scheduler.trace.to_s.include?("grpc.call_transient") }
          h.check("gRPC moved response was emitted") { h.scheduler.trace.to_s.include?("grpc.lease_moved") }
          h.check("gRPC moved response decoded as stale lease") { h.scheduler.trace.to_s.include?("grpc.decode_moved") }
          h.check("gRPC retry reached the new owner") { h.scheduler.trace.to_s.include?("grpc.retry_success") }
          h.check("gRPC unavailable surfaced as node unavailable") do
            h.scheduler.trace.to_s.include?("grpc.node_unavailable_observed")
          end
          h.check("gRPC not_running response decoded as no active lease") do
            h.scheduler.trace.to_s.include?("grpc.not_running_observed")
          end
        end
      end

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

      #: (untyped) -> untyped
      def grpc_workflow_rpc_transport_fault_reroute(seed)
        run(seed, "grpc_workflow_rpc_transport_fault_reroute") do |h|
          faults = ["timeout", "deadline_exceeded", "connection_reset", "eof", "unavailable"]

          faults.each_with_index do |fault, index|
            id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + index })
            old_owner = "old-owner-#{index}"
            new_owner = "new-owner-#{index}"
            h.store.claim_workflow(workflow_id: id, worker_id: old_owner, lease_seconds: 10)
            old_client = Object.new
            transport_fault = method(:grpc_transport_fault!)
            old_client.define_singleton_method(:request) do |command, _payload|
              raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

              h.store.mark_workflow_running(id, worker_id: new_owner, lease_seconds: 60)
              transport_fault.call(h, fault, target: old_owner)
            end
            new_client = grpc_workflow_rpc_client(h, new_owner) do |payload|
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.transport_reroute_success", fault:)
              workflow_rpc_handler(h, new_owner).call(payload)
            end
            router = Durababble::WorkflowRpc::Router.new(
              store: h.store,
              rpc_clients: { old_owner => old_client, new_owner => new_client },
              retry_on_stale: true,
            )
            h.scheduler.schedule(actor: "caller", delay: index * 4, name: "grpc_reroute:#{fault}") do
              router.request(workflow_id: id, command: "status", payload: { "fault" => fault })
            end
          end

          h.check("transport failures caused fresh lease lookups and reroutes") do
            h.scheduler.trace.to_s.scan("grpc.transport_reroute_success").length == faults.length
          end
          h.check("reroute matrix included timeout") { h.scheduler.trace.to_s.include?("fault=\"timeout\"") }
          h.check("reroute matrix included RST") { h.scheduler.trace.to_s.include?("fault=\"connection_reset\"") }
          h.check("reroute matrix included EOF") { h.scheduler.trace.to_s.include?("fault=\"eof\"") }
        end
      end

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

      #: (untyped) -> untyped
      def grpc_service_contract(seed)
        run(seed, "grpc_service_contract") do |h|
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

          h.scheduler.schedule(actor: "caller", delay: 1, name: "grpc_awaken_batch") do
            service.awaken_batch(
              Durababble::Rpc::Proto::AwakenBatchRequest.new(worker_pool: "default", workflow_ids: [id]),
              nil,
            )
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.awaken_batch")
          end
          h.scheduler.schedule(actor: "caller", delay: 2, name: "grpc_evict_lease") do
            service.evict_lease(
              Durababble::Rpc::Proto::EvictLeaseRequest.new(
                worker_pool: "default",
                target_kind: "workflow",
                target_id: id,
              ),
              nil,
            )
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.evict_lease")
          end
          h.scheduler.schedule(actor: "caller", delay: 3, name: "grpc_deliver_message") do
            service.deliver_message(
              Durababble::Rpc::Proto::DeliverMessageRequest.new(
                worker_pool: "default",
                target_kind: "workflow",
                target_id: id,
              ),
              nil,
            )
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.deliver_message")
          end
          h.scheduler.schedule(actor: "caller", delay: 4, name: "grpc_call_transient") do
            response = service.call_transient(
              Durababble::Rpc::Proto::TransientRequest.new(
                worker_pool: "default",
                workflow_id: id,
                method: "status",
                args: Durababble::Rpc.dump({ "seed" => seed }),
              ),
              nil,
            )
            decoded = Durababble::Rpc::Client.decode_transient_response(response)
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.call_transient_ok", decoded:)
          end
          h.scheduler.schedule(actor: "caller", delay: 5, name: "grpc_call_object_transient") do
            response = service.call_transient(
              Durababble::Rpc::Proto::TransientRequest.new(
                worker_pool: "default",
                class_name: "Counter",
                object_id: "counter-1",
                method: "balance",
                args: Durababble::Rpc.dump({ "seed" => seed }),
              ),
              nil,
            )
            decoded = Durababble::Rpc::Client.decode_transient_response(response)
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.call_object_transient_ok", decoded:)
          end
          h.scheduler.schedule(actor: "caller", delay: 6, name: "grpc_stale_deliver_message") do
            before = events.length
            h.store.steal_expired_leases!(now: 11)
            h.store.claim_workflow(workflow_id: id, worker_id: "worker-b", lease_seconds: 60)
            service.deliver_message(
              Durababble::Rpc::Proto::DeliverMessageRequest.new(
                worker_pool: "default",
                target_kind: "workflow",
                target_id: id,
              ),
              nil,
            )
            h.scheduler.trace.event(
              h.scheduler.time,
              "grpc",
              events.length == before ? "grpc.deliver_message_stale_ack" : "grpc.deliver_message_stale_work",
            )
          end
          h.check("AwakenBatch was served") { h.scheduler.trace.to_s.include?("grpc.awaken_batch") }
          h.check("EvictLease was served") { h.scheduler.trace.to_s.include?("grpc.evict_lease") }
          h.check("DeliverMessage was served for the active owner") { h.scheduler.trace.to_s.include?("grpc.deliver_message") }
          h.check("CallTransient decoded a workflow response") { h.scheduler.trace.to_s.include?("grpc.call_transient_ok") }
          h.check("CallTransient decoded an object response") do
            h.scheduler.trace.to_s.include?("grpc.call_object_transient_ok")
          end
          h.check("stale DeliverMessage returned without doing work") do
            h.scheduler.trace.to_s.include?("grpc.deliver_message_stale_ack")
          end
        end
      end

      #: (untyped, untyped) { (?) -> untyped } -> untyped
      def workflow_rpc_client(_h, _node_id, &block)
        Object.new.tap do |client|
          client.define_singleton_method(:request) do |command, payload|
            raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

            block.call(payload)
          end
        end
      end

      #: (untyped, untyped, ?faults: untyped) { (?) -> untyped } -> untyped
      def grpc_workflow_rpc_client(h, node_id, faults: [], &block)
        fault_queue = faults.dup
        transport_fault = method(:grpc_transport_fault!)
        workflow_response = method(:grpc_workflow_rpc_response)
        remote_error_response = method(:grpc_remote_error_response)
        handler_for = method(:workflow_rpc_handler)
        Object.new.tap do |client|
          client.define_singleton_method(:request) do |command, payload|
            raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.call_transient", target: node_id)
            server_payload = {
              "workflow_id" => payload.fetch("workflow_id"),
              "expected_worker_id" => node_id,
              "command" => payload.fetch("command"),
              "payload" => payload.fetch("payload", {}),
            }
            fault = fault_queue.shift
            transport_fault.call(h, fault, target: node_id)
            response_context = {
              h:,
              node_id:,
              workflow_id: payload.fetch("workflow_id"),
              handler_for:,
              remote_error_response:,
              handler_block: block,
            }
            response = workflow_response.call(response_context, server_payload)
            if fault == "duplicate_response"
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.duplicate_response", target: node_id)
              workflow_response.call(response_context, server_payload)
            end
            if fault == "response_timeout"
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.response_timeout", target: node_id)
              raise Durababble::WorkflowRpc::NodeUnavailable, "#{node_id} response timed out"
            end

            Durababble::Rpc::Client.decode_transient_response(response)
          rescue Durababble::WorkflowRpc::StaleLease
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.decode_moved", target: node_id)
            raise
          rescue Durababble::WorkflowRpc::NoActiveLease
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.decode_not_running", target: node_id)
            raise
          end
        end
      end

      #: (untyped, untyped) -> untyped
      def grpc_workflow_rpc_response(context, payload)
        h = context.fetch(:h)
        node_id = context.fetch(:node_id)
        workflow_id = context.fetch(:workflow_id)
        handler_block = context.fetch(:handler_block)
        handler_for = context.fetch(:handler_for)
        remote_error_response = context.fetch(:remote_error_response)

        result = handler_block ? handler_block.call(payload) : handler_for.call(h, node_id).call(payload)
        Durababble::Rpc::Proto::TransientResponse.new(ok: Durababble::Rpc.dump(result))
      rescue Durababble::WorkflowRpc::NoActiveLease
        h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.not_running", target: node_id)
        Durababble::Rpc::Proto::TransientResponse.new(not_running: true)
      rescue Durababble::WorkflowRpc::StaleLease => e
        lease = h.store.current_workflow_lease(workflow_id)
        if lease && lease.fetch("worker_id") != node_id
          h.scheduler.trace.event(
            h.scheduler.time,
            "grpc",
            "grpc.lease_moved",
            from: node_id,
            to: lease.fetch("worker_id"),
          )
          Durababble::Rpc::Proto::TransientResponse.new(
            moved: Durababble::Rpc::Proto::LeaseMoved.new(
              new_node_id: lease.fetch("worker_id"),
              new_rpc_address: "virtual://#{lease.fetch("worker_id")}",
            ),
          )
        else
          remote_error_response.call(e)
        end
      rescue StandardError => e
        remote_error_response.call(e)
      end

      #: (untyped, untyped, target: untyped) -> untyped
      def grpc_transport_fault!(h, fault, target:)
        case fault
        when nil, "success", "response_timeout", "duplicate_response"
          nil
        when "timeout"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.timeout", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} timed out"
        when "deadline_exceeded"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.deadline_exceeded", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} deadline exceeded"
        when "connection_reset"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.rst", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} reset the stream"
        when "eof"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.eof", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} closed the stream"
        when "unavailable"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.unavailable", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} unavailable"
        else
          raise ArgumentError, "unknown gRPC fault #{fault}"
        end
      end

      #: (untyped, untyped, target: untyped, fault: untyped) { (?) -> untyped } -> untyped
      def grpc_faulty_unary(h, method_name, target:, fault:, &block)
        h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.#{method_name}.request", target:, fault:)
        case fault
        when "drop"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.drop", method: method_name, target:)
          :dropped
        when "duplicate"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.duplicate", method: method_name, target:)
          block.call
          block.call
          :ok
        else
          grpc_transport_fault!(h, fault, target:)
          block.call
          :ok
        end
      end

      #: (untyped, untyped, workflow_id: untyped) -> untyped
      def call_grpc_service_method(service, method_name, workflow_id:)
        case method_name
        when "awaken_batch"
          service.awaken_batch(
            Durababble::Rpc::Proto::AwakenBatchRequest.new(
              worker_pool: "default",
              workflow_ids: [workflow_id],
            ),
            nil,
          )
        when "deliver_message"
          service.deliver_message(
            Durababble::Rpc::Proto::DeliverMessageRequest.new(
              worker_pool: "default",
              target_kind: "workflow",
              target_id: workflow_id,
            ),
            nil,
          )
        when "evict_lease"
          service.evict_lease(
            Durababble::Rpc::Proto::EvictLeaseRequest.new(
              worker_pool: "default",
              target_kind: "workflow",
              target_id: workflow_id,
            ),
            nil,
          )
        else
          raise ArgumentError, "unknown gRPC service method #{method_name}"
        end
      end

      #: (untyped) -> untyped
      def grpc_remote_error_response(error)
        Durababble::Rpc::Proto::TransientResponse.new(
          err: Durababble::Rpc::Proto::RemoteError.new(
            klass: error.class.name,
            message: error.message,
            backtrace: error.backtrace || [],
          ),
        )
      end

      #: (untyped, untyped) { (?) -> untyped } -> untyped
      def workflow_rpc_handler(h, node_id, &handler_block)
        Durababble::WorkflowRpc::Handler.new(store: h.store, node_id:, handlers: {
          "status" => handler_block || ->(payload) { { "node" => node_id, "payload" => payload } },
        })
      end

      #: (untyped) -> untyped
      def workflow_durable_before_claim(seed)
        run(seed, "workflow_durable_before_claim") do |h|
          h.workflows["counter"] = counter_workflow
          h.scheduler.schedule(actor: "client", delay: h.scheduler.rng.int(20), name: "enqueue_then_crash") do
            h.store.enqueue_workflow(name: "counter", input: { "count" => 5 })
          end
          h.add_workers(["worker-a", "worker-b"], ticks: 12)
          h.check("pending workflow eventually completed") { h.store.summary.fetch(:completed_workflows) == 1 }
        end
      end

      #: (untyped) -> untyped
      def lease_conflict(seed)
        run(seed, "lease_conflict") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => 1 })
          h.store.claim_workflow(workflow_id: id, worker_id: "owner", lease_seconds: 60)
          h.scheduler.schedule(actor: "intruder", delay: h.scheduler.rng.int(20), name: "resume_without_lease") do
            Durababble::Engine.new(store: h.store, worker_id: "intruder").resume(h.workflows.fetch("counter"), workflow_id: id)
          rescue LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "intruder", "lease_conflict_observed", workflow_id: id)
          end
          h.scheduler.schedule(actor: "owner", delay: 30 + h.scheduler.rng.int(10), name: "owner_resume") do
            Durababble::Engine.new(store: h.store, worker_id: "owner").resume(h.workflows.fetch("counter"), workflow_id: id)
          end
          h.check("lease conflict observed") { h.scheduler.trace.to_s.include?("lease_conflict_observed") }
          h.check("owner completed workflow") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      #: (untyped) -> untyped
      def heartbeat_extension(seed)
        run(seed, "heartbeat_extension") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => 2 })
          h.store.claim_workflow(workflow_id: id, worker_id: "owner", lease_seconds: 20)
          h.scheduler.schedule(actor: "owner", delay: 15 + h.scheduler.rng.int(5), name: "heartbeat") { h.store.heartbeat(workflow_id: id, worker_id: "owner", lease_seconds: 80) }
          h.scheduler.schedule(actor: "reaper", delay: 30, name: "steal_before_original_expiry") { h.store.steal_expired_leases! }
          h.scheduler.schedule(actor: "owner", delay: 35, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "owner").resume(h.workflows.fetch("counter"), workflow_id: id) }
          h.check("heartbeat prevented premature steal") { h.store.workflow(id).fetch("status") == "completed" }
          h.check("no lease steal occurred") { !h.scheduler.trace.to_s.include?("steal_expired") }
        end
      end

      #: (untyped) -> untyped
      def zombie_workflow_heartbeat_after_expiry(seed)
        run(seed, "zombie_workflow_heartbeat_after_expiry") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: id, worker_id: "zombie", lease_seconds: 10)
          h.scheduler.schedule(actor: "zombie", delay: 20, name: "heartbeat_after_expiry") do
            h.store.heartbeat(workflow_id: id, worker_id: "zombie", lease_seconds: 60)
            if h.store.workflow_owned?(workflow_id: id, worker_id: "zombie")
              h.scheduler.trace.event(h.scheduler.time, "zombie", "zombie_heartbeat_renewed", workflow_id: id)
            else
              h.scheduler.trace.event(h.scheduler.time, "zombie", "zombie_heartbeat_rejected", workflow_id: id)
            end
          end
          h.check("expired heartbeat was rejected") { h.scheduler.trace.to_s.include?("zombie_heartbeat_rejected") }
          h.check("zombie did not regain ownership") { !h.store.workflow_owned?(workflow_id: id, worker_id: "zombie") }
        end
      end

      #: (untyped) -> untyped
      def stale_wait_timer_terminal_workflow(seed)
        run(seed, "stale_wait_timer_terminal_workflow") do |h|
          id = h.store.create_workflow(name: "waiting", input: { "seed" => seed })
          h.store.record_step_started(workflow_id: id, position: 0, name: "wait")
          h.store.record_wait(
            workflow_id: id,
            position: 0,
            name: "wait",
            wait_request: Durababble.wait_until(h.store.current_time + 10, { "seed" => seed }),
          )
          h.store.wake_due_timers(now: h.store.current_time + 11)
          h.store.complete_workflow(id, result: { "done" => true })
          h.scheduler.schedule(actor: "timer", delay: h.scheduler.rng.int(5), name: "stale_timer") do
            woken = h.store.wake_due_timers(now: h.store.current_time + 11)
            event = woken.zero? ? "stale_wait_ignored" : "stale_wait_completed"
            h.scheduler.trace.event(h.scheduler.time, "timer", event, workflow_id: id, woken:)
          end
          h.check("stale wait timer was ignored") { h.scheduler.trace.to_s.include?("stale_wait_ignored") }
          h.check("terminal workflow remained completed") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      #: (untyped) -> untyped
      def step_heartbeat_cursor_recovery(seed)
        run(seed, "step_heartbeat_cursor_recovery") do |h|
          attempts = []
          h.workflows["cursor"] = workflow_class("cursor") do
            test_step("download") do |_ctx, heartbeat|
              attempts << heartbeat.cursor
              if attempts.length == 1
                heartbeat.record({ "offset" => seed })
                raise InjectedCrash, "crash after step heartbeat"
              end

              h.scheduler.trace.event(h.scheduler.time, "worker", "step_heartbeat_resumed", cursor: heartbeat.cursor)
              { "resumed_from" => heartbeat.cursor.fetch("offset") }
            end
          end
          id = h.store.enqueue_workflow(name: "cursor", input: {})
          h.scheduler.schedule(actor: "crashing-worker", delay: h.scheduler.rng.int(5), name: "heartbeat_then_crash") do
            Durababble::Engine.new(store: h.store, worker_id: "crashing-worker", lease_seconds: 10).resume(h.workflows.fetch("cursor"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "crashing-worker", "step_heartbeat_crash", workflow_id: id)
          end
          h.scheduler.schedule(actor: "reaper", delay: 20, name: "steal") { h.store.steal_expired_leases! }
          h.scheduler.schedule(actor: "recover", delay: 25, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "recover").resume(h.workflows.fetch("cursor"), workflow_id: id) }
          h.check("cursor was provided on retry") { attempts == [nil, { "offset" => seed }] }
          h.check("workflow completed from cursor") { h.store.workflow(id).fetch("result") == { "resumed_from" => seed } }
        end
      end

      #: (untyped) -> untyped
      def step_retry_policy_recovery(seed)
        run(seed, "step_retry_policy_recovery") do |h|
          attempts = 0
          h.workflows["retry"] = workflow_class("retry") do
            test_step("flaky", retry_policy: { initial_interval: 10, backoff_coefficient: 2, maximum_interval: 15, maximum_attempts: 3 }) do |ctx|
              attempts += 1
              h.scheduler.trace.event(h.scheduler.time, "worker", "step_retry_attempt", attempt: attempts)
              raise "transient #{attempts}" if attempts < 3

              ctx.merge("attempts" => attempts)
            end
          end
          id = h.store.enqueue_workflow(name: "retry", input: {})
          h.scheduler.schedule(actor: "worker-a", delay: 1 + h.scheduler.rng.int(3), name: "first_attempt") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a").resume(h.workflows.fetch("retry"), workflow_id: id)
          end
          h.scheduler.schedule(actor: "worker-b", delay: 8, name: "restart_before_due") do
            claimed = h.store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: Engine::DEFAULT_LEASE_SECONDS)
            h.scheduler.trace.event(h.scheduler.time, "worker-b", "step_retry_not_due") unless claimed
          end
          h.scheduler.schedule(actor: "worker-b", delay: 20, name: "second_attempt_after_restart") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-b").resume(h.workflows.fetch("retry"), workflow_id: id)
          end
          h.scheduler.schedule(actor: "worker-c", delay: 36, name: "final_attempt_after_restart") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-c").resume(h.workflows.fetch("retry"), workflow_id: id)
          end
          h.check("retry waited for due time") { h.scheduler.trace.to_s.include?("step_retry_not_due") }
          h.check("workflow completed after durable retries") { h.store.workflow(id).fetch("status") == "completed" }
          h.check("attempt history records retries") { h.store.step_attempts_for(id).map { |a| a.fetch("status") } == ["failed", "failed", "completed"] }
        end
      end

      #: (untyped) -> untyped
      def cooperative_cancellation_cleanup(seed)
        run(seed, "cooperative_cancellation_cleanup") do |h|
          cleanup_runs = 0
          cleanup_lease_observations = []
          workflow_id_for_cleanup = nil
          h.workflows["cancelable"] = workflow = Class.new(Durababble::Workflow) do
            workflow_name "cancelable"

            define_method(:execute) do |input|
              instance = self #: as untyped
              instance.wait_for_timer(input)
              { "done" => true }
            rescue Durababble::CancellationError => e
              instance.cleanup(input.merge("reason" => e.reason))
            end

            define_method(:wait_for_timer) do |input|
              Durababble.wait_until(h.store.current_time + 60, input)
            end
            step :wait_for_timer

            define_method(:cleanup) do |input|
              instance = self #: as untyped
              cleanup_runs += 1
              before = h.store.workflow(workflow_id_for_cleanup)
              h.scheduler.advance(5)
              instance.step_context.heartbeat.record({ "phase" => "cleanup", "run" => cleanup_runs })
              after = h.store.workflow(workflow_id_for_cleanup)
              cleanup_lease_observations << {
                before_locked_by: before.fetch("locked_by"),
                before_locked_until: before.fetch("locked_until"),
                after_locked_by: after.fetch("locked_by"),
                after_locked_until: after.fetch("locked_until"),
              }
              h.scheduler.trace.event(h.scheduler.time, "worker", "cleanup_ran", count: cleanup_runs, reason: input.fetch("reason"))
              { "cleaned" => true, "reason" => input.fetch("reason") }
            end
            step :cleanup
          end

          id = h.store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => seed.to_s })
          workflow_id_for_cleanup = id
          h.scheduler.schedule(actor: "worker-a", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a").resume(workflow, workflow_id: id)
          end
          h.scheduler.schedule(actor: "client", delay: 5, name: "cancel") do
            workflow.handle(id, store: h.store).cancel(reason: "stop #{seed}")
          end
          h.scheduler.schedule(actor: "worker-b", delay: 10, name: "cleanup") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-b", lease_seconds: 20).resume(workflow, workflow_id: id)
          end
          h.scheduler.schedule(actor: "client-timer", delay: 70, name: "late_timer") do
            woken = h.store.wake_due_timers
            h.scheduler.trace.event(h.scheduler.time, "client-timer", "late_timer", woken:)
          end
          h.check("workflow canceled after cleanup") { h.store.workflow(id).fetch("status") == "canceled" }
          h.check("cleanup ran once") { cleanup_runs == 1 }
          h.check("cleanup heartbeat kept ownership") do
            cleanup_lease_observations.any? do |observation|
              observation.fetch(:before_locked_by) == "worker-b" &&
                observation.fetch(:after_locked_by) == "worker-b" &&
                observation.fetch(:after_locked_until) > observation.fetch(:before_locked_until)
            end
          end
          h.check("cleanup heartbeat persisted") { h.store.steps_for(id).any? { |step| step.fetch("name") == "cleanup" && step.fetch("heartbeat_cursor") == { "phase" => "cleanup", "run" => 1 } } }
          h.check("late timer ignored") { h.scheduler.trace.to_s.include?("late_timer woken=0") }
          h.check("waiting attempt canceled") { h.store.step_attempts_for(id).any? { |attempt| attempt.fetch("status") == "canceled" } }
        end
      end

      #: (untyped) -> untyped
      def store_fault_after_step_completed(seed)
        run(seed, "store_fault_after_step_completed") do |h|
          h.workflows["counter"] = counter_workflow
          h.store.fault_plan.fail_after(:record_step_completed, message: "lost connection after durable step write")
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.scheduler.schedule(
            actor: "faulty-worker",
            delay: h.scheduler.rng.int(5),
            name: "fault_after_step_completed",
          ) do
            Durababble::Engine.new(
              store: h.store,
              worker_id: "faulty-worker",
              lease_seconds: 10,
            ).resume(h.workflows.fetch("counter"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "faulty-worker", "store_fault_observed", workflow_id: id)
          end
          h.scheduler.schedule(actor: "reaper", delay: 20, name: "steal") { h.store.steal_expired_leases! }
          h.scheduler.schedule(actor: "recover", delay: 25, name: "resume") do
            Durababble::Engine.new(store: h.store, worker_id: "recover").resume(
              h.workflows.fetch("counter"),
              workflow_id: id,
            )
          end
          h.check("fault was injected after step write") { h.scheduler.trace.to_s.include?("fault.injected") }
          h.check("completed step was not marked failed after store fault") do
            !h.store.step_attempts_for(id).map { |attempt| attempt.fetch("status") }.include?("failed")
          end
          h.check("workflow completed after recovering from durable step write") do
            h.store.workflow(id).fetch("status") == "completed"
          end
        end
      end

      #: (untyped) -> untyped
      def store_fault_after_wait_recorded(seed)
        run(seed, "store_fault_after_wait_recorded") do |h|
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(h.store.current_time + 20, ctx.merge("ok" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          h.store.fault_plan.fail_after(:record_wait, message: "lost connection after durable wait write")
          id = h.store.enqueue_workflow(name: "waiting", input: { "id" => seed.to_s })
          h.scheduler.schedule(
            actor: "faulty-worker",
            delay: h.scheduler.rng.int(5),
            name: "fault_after_wait_recorded",
          ) do
            Durababble::Engine.new(
              store: h.store,
              worker_id: "faulty-worker",
              lease_seconds: 10,
            ).resume(h.workflows.fetch("waiting"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "faulty-worker", "store_fault_observed", workflow_id: id)
          end
          h.scheduler.schedule(actor: "timer", delay: 20, name: "wake_due_timers") do
            h.store.wake_due_timers(now: h.store.current_time + 100)
          end
          h.scheduler.schedule(actor: "recover", delay: 25, name: "resume") do
            Durababble::Engine.new(store: h.store, worker_id: "recover").resume(
              h.workflows.fetch("waiting"),
              workflow_id: id,
            )
          end
          h.check("fault was injected after wait write") { h.scheduler.trace.to_s.include?("fault.injected") }
          h.check("workflow completed after recovering from durable wait write") do
            h.store.workflow(id).fetch("status") == "completed"
          end
        end
      end

      #: (untyped) -> untyped
      def store_fault_after_outbox_enqueue(seed)
        run(seed, "store_fault_after_outbox_enqueue") do |h|
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.fault_plan.fail_after(:enqueue_outbox, message: "lost connection after durable outbox write")
          outbox_id = nil #: untyped
          h.scheduler.schedule(
            actor: "producer",
            delay: h.scheduler.rng.int(5),
            name: "fault_after_outbox_enqueue",
          ) do
            outbox_id = h.store.enqueue_outbox(
              workflow_id:,
              topic: "email",
              payload: { "seed" => seed },
              key: "email:#{seed}",
            )
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "producer", "store_fault_observed", workflow_id:)
            outbox_id = h.store.enqueue_outbox(
              workflow_id:,
              topic: "email",
              payload: { "seed" => seed, "retry" => true },
              key: "email:#{seed}",
            )
          end
          h.scheduler.schedule(actor: "sender", delay: 20, name: "send") do
            message = h.store.claim_outbox(worker_id: "sender", lease_seconds: 10)
            h.store.ack_outbox(message.fetch("id"), worker_id: "sender") if message
          end
          h.check("fault was injected after outbox write") { h.scheduler.trace.to_s.include?("fault.injected") }
          h.check("retry reused the original outbox message") do
            h.store.outbox_message(outbox_id).fetch("payload") == { "seed" => seed }
          end
          h.check("outbox processed once after enqueue fault") { h.store.summary.fetch(:processed_outbox) == 1 }
        end
      end

      #: (untyped) -> untyped
      def duplicate_delivery_timer_and_outbox(seed)
        run(seed, "duplicate_delivery_timer_and_outbox") do |h|
          h.network.duplicate_percent = 100
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(h.store.current_time + 10, ctx.merge("ok" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          workflow_id = h.store.enqueue_workflow(name: "waiting", input: { "id" => seed.to_s })
          h.scheduler.schedule(actor: "worker", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "worker").resume(
              h.workflows.fetch("waiting"),
              workflow_id:,
            )
          end
          h.network.send(source: "client-timer", target: "db", type: "timer", payload: {}) do
            h.store.wake_due_timers(now: h.store.current_time + 100)
          end
          h.network.send(source: "producer", target: "db", type: "outbox", payload: {}) do
            h.store.enqueue_outbox(
              workflow_id:,
              topic: "email",
              payload: { "seed" => seed },
              key: "dup-email:#{seed}",
            )
          end
          h.scheduler.schedule(actor: "sender", delay: 25, name: "send") do
            message = h.store.claim_outbox(worker_id: "sender", lease_seconds: 10)
            h.store.ack_outbox(message.fetch("id"), worker_id: "sender") if message
          end
          h.scheduler.schedule(actor: "worker", delay: 30, name: "resume") do
            Durababble::Engine.new(store: h.store, worker_id: "worker").resume(
              h.workflows.fetch("waiting"),
              workflow_id:,
            )
          end
          h.check("duplicate network delivery occurred") { h.scheduler.trace.to_s.include?("network.duplicate") }
          h.check("wait completed once despite duplicate timer delivery") do
            h.scheduler.trace.to_s.scan("wait_completed").length == 1
          end
          h.check("outbox message was idempotent despite duplicate producer delivery") do
            h.store.summary.fetch(:processed_outbox) == 1
          end
          h.check("workflow completed after duplicate timer delivery") do
            h.store.workflow(workflow_id).fetch("status") == "completed"
          end
        end
      end

      #: (untyped) -> untyped
      def completed_step_skip_after_crash(seed)
        run(seed, "completed_step_skip_after_crash") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.scheduler.schedule(actor: "crashing-worker", delay: h.scheduler.rng.int(5), name: "crash_after_step_completed") do
            Durababble::Engine.new(store: h.store, worker_id: "crashing-worker", crash_after: :step_completed).resume(h.workflows.fetch("counter"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "crashing-worker", "crashed_after_step_completed", workflow_id: id)
          end
          h.scheduler.schedule(actor: "reaper", delay: 70, name: "steal") { h.store.steal_expired_leases!(now: h.scheduler.time + 61) }
          h.scheduler.schedule(actor: "recover", delay: 80, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "recover").resume(h.workflows.fetch("counter"), workflow_id: id) }
          h.check("completed step was not re-started") { h.scheduler.trace.to_s.scan("step_started").length == 2 }
          h.check("workflow completed after recovery") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      #: (untyped) -> untyped
      def incomplete_step_retry_after_crash(seed)
        run(seed, "incomplete_step_retry_after_crash") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.scheduler.schedule(actor: "crashing-worker", delay: h.scheduler.rng.int(5), name: "crash_after_step_started") do
            Durababble::Engine.new(store: h.store, worker_id: "crashing-worker", crash_after: :step_started).resume(h.workflows.fetch("counter"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "crashing-worker", "crashed_after_step_started", workflow_id: id)
          end
          h.scheduler.schedule(actor: "reaper", delay: 70, name: "steal") { h.store.steal_expired_leases!(now: h.scheduler.time + 61) }
          h.scheduler.schedule(actor: "recover", delay: 80, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "recover").resume(h.workflows.fetch("counter"), workflow_id: id) }
          h.check("incomplete step was retried") { h.store.step_attempts_for(id).map { |attempt| attempt.fetch("status") } == ["failed", "completed", "completed"] }
          h.check("workflow completed after retry") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      #: (untyped) -> untyped
      def attempt_history_append_only(seed)
        run(seed, "attempt_history_append_only") do |h|
          h.workflows["flaky"] = workflow_class("flaky") do
            test_step("fail", retry_policy: { schedule: [0, 0], maximum_attempts: 3 }) { |_ctx| raise "boom" }
          end
          id = h.store.enqueue_workflow(name: "flaky", input: { "seed" => seed })
          3.times do |i|
            h.scheduler.schedule(actor: "worker-#{i}", delay: i * 20, name: "attempt") do
              h.store.make_workflow_due!(id, now: h.scheduler.time) if i.positive?
              Durababble::Engine.new(store: h.store, worker_id: "worker-#{i}").resume(h.workflows.fetch("flaky"), workflow_id: id)
            end
          end
          h.check("each retry appended an attempt") { h.store.step_attempts_for(id).length == 3 }
          h.check("attempts are failed terminal records") { h.store.step_attempts_for(id).all? { |a| a.fetch("status") == "failed" } }
        end
      end

      #: (untyped) -> untyped
      def concurrent_timer_wake_once(seed)
        run(seed, "concurrent_timer_wake_once") do |h|
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(h.store.current_time + 20, ctx.merge("woken" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          id = h.store.enqueue_workflow(name: "waiting", input: { "id" => "sig" })
          h.scheduler.schedule(actor: "worker", delay: 1, name: "park") { Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("waiting"), workflow_id: id) }
          5.times do |i|
            h.scheduler.schedule(actor: "timer-#{i}", delay: 20 + h.scheduler.rng.int(5), name: "wake_due_timers") { h.store.wake_due_timers(now: h.store.current_time + 100) }
          end
          h.scheduler.schedule(actor: "worker", delay: 40, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("waiting"), workflow_id: id) }
          h.check("wait completed once") { h.scheduler.trace.to_s.scan("wait_completed").length == 1 }
          h.check("workflow completed after timer wake") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      #: (untyped) -> untyped
      def fenced_side_effect_once(seed)
        run(seed, "fenced_side_effect_once") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          5.times do |i|
            h.scheduler.schedule(actor: "caller-#{i}", delay: h.scheduler.rng.int(5), name: "fence") do
              h.store.with_fence(workflow_id: id, key: "charge") { { "winner" => i } }
            rescue FenceTimeout
              h.scheduler.trace.event(h.scheduler.time, "caller-#{i}", "fence_waited")
            end
          end
          h.check("side effect ran once") { h.store.summary.fetch(:side_effects) == 1 }
        end
      end

      # Pins bug 2: a worker crashes while holding a fence (the row is left
      # `running`), and a second worker must reclaim the expired lease and run the
      # side effect exactly once. Reverting `claim_expired_fence` strands the
      # fence: the reclaimer times out, side_effects stays 0, and the stuck-fence
      # checker fires — so this scenario goes red.
      #: (untyped) -> untyped
      def fence_holder_crash_and_reclaim(seed)
        run(seed, "fence_holder_crash_and_reclaim") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.fault_plan.fail_after(:fence_acquired, message: "crash holding fence")

          h.scheduler.schedule(actor: "holder", delay: 5, name: "fence") do
            h.store.with_fence(workflow_id: id, key: "charge") { { "winner" => "holder" } }
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "holder", "crashed_holding_fence")
          end

          # Runs well after the 10-tick fence lease has expired, so the abandoned
          # `running` fence is reclaimable.
          h.scheduler.schedule(actor: "reclaimer", delay: 40, name: "fence") do
            h.store.with_fence(workflow_id: id, key: "charge") { { "winner" => "reclaimer" } }
          rescue FenceTimeout
            h.scheduler.trace.event(h.scheduler.time, "reclaimer", "fence_timed_out")
          end

          h.check("side effect ran exactly once") { h.store.summary.fetch(:side_effects) == 1 }
          h.check("fence reclaimed to completion") do
            h.store.all_fences.any? { |fence| fence.fetch("key") == "charge" && fence.fetch("status") == "completed" }
          end
        end
      end

      # Pins bug 1 (step-failure outcome must be atomic + correctly terminal).
      # Two crash flows, both crashing at :step_failed_recorded — the window the
      # fix opened *after* the durable failure write:
      #
      #   * retry path: the failed attempt and the retry scheduling land in one
      #     transaction, so after the crash the workflow is already `pending` and
      #     unleased. A plain resume reclaims and finishes it *without* any
      #     lease-stealing reaper. Revert the atomic write (drop the retry
      #     scheduling) and the workflow is stranded `running`/leased: the
      #     no-steal recovery can't claim it and "workflow completed" goes red.
      #   * exhausted path: the final failure is recorded as terminal history, so
      #     replay re-raises the recorded error instead of re-running the step.
      #     The side effect therefore runs exactly once across crash + recovery.
      #     Drop the terminal marking and recovery re-runs the step — the side
      #     effect fires twice and the count check goes red.
      #: (untyped) -> untyped
      def step_failure_crash_matrix(seed)
        run(seed, "step_failure_crash_matrix") do |h|
          retry_side_effects = 0
          retry_attempts = 0
          h.workflows["retry_crash"] = workflow_class("retry_crash") do
            test_step("charge", retry_policy: { initial_interval: 10, backoff_coefficient: 1, maximum_interval: 10, maximum_attempts: 3 }) do |ctx|
              retry_attempts += 1
              retry_side_effects += 1
              raise "transient charge failure #{retry_attempts}" if retry_attempts < 2

              ctx.merge("charged" => true)
            end
          end

          exhausted_side_effects = 0
          h.workflows["exhausted_crash"] = workflow_class("exhausted_crash") do
            test_step("charge_once", retry_policy: { maximum_attempts: 1 }) do |_ctx|
              exhausted_side_effects += 1
              raise "permanent charge failure"
            end
          end

          retry_id = h.store.enqueue_workflow(name: "retry_crash", input: { "seed" => seed })
          exhausted_id = h.store.enqueue_workflow(name: "exhausted_crash", input: { "seed" => seed })

          # --- retry path: crash right after the atomic failed+retry write ---
          h.scheduler.schedule(actor: "worker-a", delay: 1 + h.scheduler.rng.int(3), name: "retry_attempt_then_crash") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a", crash_after: :step_failed_recorded)
              .resume(h.workflows.fetch("retry_crash"), workflow_id: retry_id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "worker-a", "crashed_after_retry_scheduled", workflow_id: retry_id)
          end
          # No reaper: recovery relies solely on the workflow having been left
          # claimable (pending, unleased) by the atomic retry write.
          h.scheduler.schedule(actor: "worker-b", delay: 20, name: "retry_recover") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-b").resume(h.workflows.fetch("retry_crash"), workflow_id: retry_id)
          rescue LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "worker-b", "retry_recover_blocked", workflow_id: retry_id)
          end

          # --- exhausted path: crash right after the terminal failure write ---
          h.scheduler.schedule(actor: "worker-c", delay: 1 + h.scheduler.rng.int(3), name: "exhausted_attempt_then_crash") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-c", crash_after: :step_failed_recorded)
              .resume(h.workflows.fetch("exhausted_crash"), workflow_id: exhausted_id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "worker-c", "crashed_after_terminal_failure", workflow_id: exhausted_id)
          end
          # The engine never finalized the workflow (it crashed first), so the row
          # is still `running`/leased; steal the expired lease, then replay must
          # re-raise the recorded terminal failure rather than re-run the step.
          h.scheduler.schedule(actor: "reaper", delay: 70, name: "steal") { h.store.steal_expired_leases!(now: h.scheduler.time + 61) }
          h.scheduler.schedule(actor: "worker-d", delay: 80, name: "exhausted_recover") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-d").resume(h.workflows.fetch("exhausted_crash"), workflow_id: exhausted_id)
          rescue Durababble::Error
            h.scheduler.trace.event(h.scheduler.time, "worker-d", "exhausted_recover_failed", workflow_id: exhausted_id)
          end

          h.check("retry workflow completed after crash without a steal") { h.store.workflow(retry_id).fetch("status") == "completed" }
          h.check("retry side effect ran once per attempt") { retry_side_effects == 2 && retry_attempts == 2 }
          h.check("exhausted workflow failed after crash") { h.store.workflow(exhausted_id).fetch("status") == "failed" }
          h.check("exhausted step ran exactly once across recovery") { exhausted_side_effects == 1 }
          h.check("exhausted step not re-run on replay") { h.store.step_attempts_for(exhausted_id).one? }
        end
      end

      #: (untyped) -> untyped
      def chaos(seed)
        run(seed, "chaos") do |h|
          h.workflows["counter"] = counter_workflow
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_until(h.store.current_time + 80, ctx.merge("woken" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end

          12.times do |i|
            name = h.scheduler.rng.chance(25) ? "waiting" : "counter"
            input = name == "waiting" ? { "id" => "w#{i}" } : { "count" => i }
            h.network.send(source: "client-#{i}", target: "db", type: "enqueue") { h.store.enqueue_workflow(name:, input:) }
            h.scheduler.schedule(actor: "timer-#{i}", delay: 80 + h.scheduler.rng.int(200), name: "wake_due_timers") { h.store.wake_due_timers(now: h.store.current_time + 100) }
          end
          # Crash workers mid-resume between durable writes (not just whole-tick
          # skips), so every inter-write window is exercised under chaos. The
          # reaper + repeated ticks must still drive every workflow to a
          # crash-consistent state.
          h.store.enable_write_crashes!(percent: 20)
          h.add_workers(["worker-a", "worker-b", "worker-c", "worker-d"], ticks: 30, crash_percent: 15)
          8.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 60 + i * 50, name: "steal_expired") { h.store.steal_expired_leases!(now: h.scheduler.time + 61) }
          end
        end
      end

      #: () -> untyped
      def counter_workflow
        workflow_class("counter") do
          test_step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
          test_step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
        end
      end

      #: (untyped, ?retry_policy: untyped) ?{ (untyped, ?untyped) -> untyped } -> untyped
      def test_step(name, retry_policy: nil, &block)
        nil
      end

      #: (untyped) ?{ (?) -> untyped } -> untyped
      def workflow_class(name, &definition)
        workflow = Class.new(Durababble::Workflow)
        workflow.workflow_name(name)
        workflow.define_method(:execute) do |input|
          instance = self #: as untyped
          instance.class.step_order.reduce(input) { |ctx, method_name| instance.public_send(method_name, ctx) }
        end
        workflow.define_singleton_method(:test_step) do |step_name, retry_policy: nil, &block|
          workflow_class = self #: as untyped
          workflow_class.define_method(step_name) do |ctx|
            instance = self #: as untyped
            if block.arity >= 2
              block.call(ctx, instance.step_context.heartbeat)
            else
              block.call(ctx)
            end
          end
          workflow_class.step(step_name, retry: retry_policy)
        end
        workflow.class_eval(&definition) if definition
        workflow
      end

      #: (untyped, untyped) { (untyped) -> untyped } -> untyped
      def run(seed, scenario, &block)
        trace = Trace.new
        scheduler = Scheduler.new(seed:, trace:)
        network = VirtualNetwork.new(scheduler:, drop_percent: scenario == "chaos" ? 5 : 0)
        store = DeterministicSqliteStore.build(scheduler:)
        begin
          store.with_deterministic_uuids do
            harness = Harness.new(scenario:, seed:, scheduler:, network:, store:)
            trace.event(0, "dst", "begin", scenario:, seed:)
            block.call(harness)
            scheduler.run
            harness.verify!
            trace.event(scheduler.time, "dst", "end", scenario:, seed:)
            trace_s = trace.to_s
            Result.new(scenario:, seed:, trace: trace_s, digest: Digest::SHA256.hexdigest(trace_s), violations: harness.violations, summary: store.summary)
          end
        ensure
          store.close
        end
      end
    end
  end
end
