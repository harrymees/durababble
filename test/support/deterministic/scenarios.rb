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
      def bug_stuck_inbox(seed)
        run(seed, "bug_stuck_inbox") do |h|
          # An inbox message claimed (`running`) by a crashed worker, its lease
          # already expired and never reclaimed — the stuck-inbox checker must
          # flag it (symmetric to the stuck-fence/stuck-outbox fixtures). The
          # inbox is target-oriented, so no workflow row is required.
          h.store.inject_inbox({
            "id" => "stuck-inbox",
            "status" => "running",
            "locked_by" => "crashed-worker",
            "locked_until" => h.scheduler.time - 1,
          })
        end
      end

      #: (untyped) -> untyped
      def bug_stuck_activation(seed)
        run(seed, "bug_stuck_activation") do |h|
          # A target_activation (the #69 wakeup row) claimed (`running`) by a
          # crashed worker, its lease already expired and never reclaimed by
          # `claim_expired_target_activation` — the stuck-activation checker must
          # flag it (completes the lease-reclaim quartet alongside
          # fence/outbox/inbox). Composite-keyed, so no workflow row is required.
          h.store.inject_target_activation({
            "worker_pool" => "default",
            "target_kind" => "workflow",
            "target_type" => "counter",
            "target_id" => "stuck-#{seed}",
            "status" => "running",
            "ready_at" => h.scheduler.time - 5,
            "locked_by" => "crashed-worker",
            "locked_until" => h.scheduler.time - 1,
          })
        end
      end

      #: (untyped) -> untyped
      def bug_lost_wakeup(seed)
        run(seed, "bug_lost_wakeup") do |h|
          # An activatable (pending) inbox head for a target with NO matching
          # target_activations row: reconcile is supposed to keep a wakeup row
          # alive while a non-dead-lettered head exists, so the absence means no
          # worker will ever be woken to drain this mailbox. The consistency
          # checker must flag it. We inject only the inbox message (no activation).
          h.store.inject_inbox({
            "id" => "lost-#{seed}",
            "worker_pool" => "default",
            "target_kind" => "workflow",
            "target_type" => "counter",
            "target_id" => "lost-#{seed}",
            "sequence" => 0,
            "status" => "pending",
          })
        end
      end

      #: (untyped) -> untyped
      def bug_orphaned_activation(seed)
        run(seed, "bug_orphaned_activation") do |h|
          # The reverse of bug_lost_wakeup: a target_activations wakeup row whose
          # mailbox has NO activatable inbox head (here: no inbox messages at
          # all). reconcile_target_activation is supposed to delete the wakeup
          # row once the head is completed/absent, so a surviving activation
          # means a reconcile was skipped (e.g. a crash between the inbox write
          # and the reconcile) and never repaired — a worker will be woken to
          # drain a mailbox with nothing claimable. The activation-inbox
          # consistency checker must flag it. The lease is unexpired so the
          # stuck-activation checker does NOT fire; only the orphan check should.
          h.store.inject_target_activation({
            "worker_pool" => "default",
            "target_kind" => "workflow",
            "target_type" => "counter",
            "target_id" => "orphan-#{seed}",
            "status" => "pending",
            "ready_at" => h.scheduler.time,
            "locked_by" => nil,
            "locked_until" => nil,
          })
        end
      end

      #: (untyped) -> untyped
      def workflow_command_async_delivery(seed)
        run(seed, "workflow_command_async_delivery") do |h|
          # Drives #69's async command-delivery path live: enqueue_workflow_command
          # (writes the inbox message and upserts the wakeup row in one txn) ->
          # claim_target_activation -> claim_inbox_messages(limit: 1) ->
          # complete_workflow_command -> complete_target_activation, looping until
          # the mailbox drains and reconcile retires the wakeup row. This is the
          # only scenario that exercises the multi-transaction worker-drain loop, so
          # the activation-invariant and lost-wakeup consistency checkers run
          # against real reconcile behaviour rather than only hand-injected
          # fixtures. Each enqueued command must be delivered exactly once and the
          # wakeup row must be fully reconciled away once the mailbox is empty.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 1 + h.scheduler.rng.int(3) # 1..3 commands
          command_ids = []
          command_count.times do |i|
            command_ids << h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end

          delivered = []
          h.scheduler.schedule(actor: "command-worker", delay: 5, name: "drain_commands") do
            # Bounded so a buggy re-arm (activation never retired) fails the
            # exactly-once check rather than spinning the virtual clock forever.
            (command_count + 3).times do
              activation = h.store.claim_target_activation(
                worker_id: "command-worker",
                lease_seconds: 30,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
                lease_seconds: 30,
                limit: 1,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id:,
                  result: { "ok" => message.fetch("id") },
                  worker_id: "command-worker",
                )
                delivered << message.fetch("id")
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
              )
            end
          end

          # Retire the workflow after the mailbox drains so the liveness checker
          # sees a terminal target rather than an abandoned-but-runnable workflow.
          h.scheduler.schedule(actor: "finisher", delay: 20, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("every enqueued command delivered exactly once") do
            delivered.sort == command_ids.sort && delivered.length == delivered.uniq.length
          end
          h.check("wakeup row fully reconciled away after the mailbox drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end

      #: (untyped) -> untyped
      def workflow_command_delivery_crash_recovery(seed)
        run(seed, "workflow_command_delivery_crash_recovery") do |h|
          # A delivery worker crashes mid-flight: it claims the wakeup row
          # (activation -> running, leased) and the head inbox message (-> running,
          # leased) but completes neither. Both leases must expire and be reclaimed
          # by a recovery worker that drains the mailbox, delivering every command
          # exactly once (the partially-claimed head must be delivered by recovery,
          # neither lost nor double-delivered). Exercises the reclaim primitives
          # (claim_target_activation taking over an expired activation;
          # claim_inbox_messages re-claiming an expired in-flight message) that the
          # happy-path workflow_command_async_delivery scenario never reaches.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 1 + h.scheduler.rng.int(3) # 1..3 commands
          command_ids = []
          command_count.times do |i|
            command_ids << h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end

          # The crashed worker grabs the activation and the head message under a
          # short lease, then does nothing else (simulating a crash before the
          # complete writes). Scheduled early so both leases expire before recovery.
          h.scheduler.schedule(actor: "crashed-worker", delay: 1, name: "claim_then_crash") do
            h.store.claim_target_activation(
              worker_id: "crashed-worker",
              lease_seconds: 5,
              target_kinds: ["workflow"],
              target_types: ["counter"],
            )
            h.store.claim_inbox_messages(
              target_kind: "workflow",
              target_type: "counter",
              target_id: workflow_id,
              worker_id: "crashed-worker",
              lease_seconds: 5,
              limit: 1,
            )
            h.scheduler.trace.event(h.scheduler.time, "crashed-worker", "delivery_worker_crashed", id: workflow_id)
          end

          delivered = []
          h.scheduler.schedule(actor: "recovery-worker", delay: 20, name: "drain_after_recovery") do
            (command_count + 3).times do
              activation = h.store.claim_target_activation(
                worker_id: "recovery-worker",
                lease_seconds: 30,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "recovery-worker",
                lease_seconds: 30,
                limit: 1,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id:,
                  result: { "ok" => message.fetch("id") },
                  worker_id: "recovery-worker",
                )
                delivered << message.fetch("id")
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "recovery-worker",
              )
            end
          end

          h.scheduler.schedule(actor: "finisher", delay: 40, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("crashed delivery worker was observed") do
            h.scheduler.trace.to_s.include?("delivery_worker_crashed")
          end
          h.check("every command delivered exactly once despite the crash") do
            delivered.sort == command_ids.sort && delivered.length == delivered.uniq.length
          end
          h.check("wakeup row fully reconciled away after recovery drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end

      #: (untyped) -> untyped
      def workflow_command_delivery_crash_matrix(seed)
        run(seed, "workflow_command_delivery_crash_matrix") do |h|
          # Crash *injection* (not simulation) inside the multi-command drain: a
          # worker delivers one command durably, then InjectedCrash fires from the
          # store's complete_workflow_command hook (post-commit) before it can move
          # to the next command or release its activation. A recovery worker must
          # resume the remaining mailbox, delivering every command exactly once
          # (the committed one must NOT be re-delivered; the rest must NOT be lost).
          # Exactly-once is judged from store state, since the crashing call's
          # durable delivery never returns to the in-Ruby tracker.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 2 + h.scheduler.rng.int(3) # 2..4 commands, so work remains after the crash
          command_ids = []
          command_count.times do |i|
            command_ids << h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end

          # Crash once, after the first durable command completion.
          h.store.fault_plan.fail_after(:complete_workflow_command, message: "crash after durable command delivery")

          drain = lambda do |worker_id|
            (command_count + 3).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds: 30,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id:,
                lease_seconds: 30,
                limit: 1,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id:,
                  result: { "ok" => message.fetch("id") },
                  worker_id:,
                )
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id:,
              )
            end
          end

          h.scheduler.schedule(actor: "faulty-worker", delay: h.scheduler.rng.int(4), name: "deliver_then_crash") do
            drain.call("faulty-worker")
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "faulty-worker", "delivery_crashed_after_commit", id: workflow_id)
          end

          # The crashing reconcile cleared the activation lease (set pending), so a
          # recovery worker can pick up immediately without waiting for expiry.
          h.scheduler.schedule(actor: "recovery-worker", delay: 15, name: "drain_after_crash") do
            drain.call("recovery-worker")
          end

          h.scheduler.schedule(actor: "finisher", delay: 30, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("a command delivery crashed after committing") do
            h.scheduler.trace.to_s.include?("delivery_crashed_after_commit")
          end
          h.check("every command is completed exactly once in the store") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("wakeup row fully reconciled away after recovery drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end

      #: (untyped) -> untyped
      def workflow_command_claim_window_crash_matrix(seed)
        run(seed, "workflow_command_claim_window_crash_matrix") do |h|
          # Crash *injection* in the two claim windows of the drain loop, where
          # the lease has been durably taken but no command has been delivered.
          # Unlike the complete_workflow_command crash (whose reconcile clears
          # the lease, letting recovery start immediately), a crash here leaves
          # the activation (window A) or the activation + inbox head (window B)
          # leased to the dead worker. Recovery therefore cannot proceed until
          # the lease expires, then reclaims via claim_expired_target_activation
          # / the expired-running inbox path and delivers every command exactly
          # once. This exercises the post-commit fault hooks on the claim
          # methods, systematically covering the inter-transaction gaps the
          # complete-side crash matrix does not.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 1 + h.scheduler.rng.int(3) # 1..3 commands
          command_ids = []
          command_count.times do |i|
            command_ids << h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end

          # Seed picks the window: 0 => crash holding only the activation lease;
          # 1 => crash holding the activation + inbox head leases.
          window = h.scheduler.rng.int(2)
          crash_op = window.zero? ? :claim_target_activation : :claim_inbox_messages
          h.store.fault_plan.fail_after(crash_op, message: "crash in #{crash_op} claim window")

          drain = lambda do |worker_id, lease_seconds|
            (command_count + 3).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds:,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id:,
                lease_seconds:,
                limit: 1,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id:,
                  result: { "ok" => message.fetch("id") },
                  worker_id:,
                )
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id:,
              )
            end
          end

          # Faulty worker leases under a SHORT lease, then InjectedCrash fires
          # from the claim hook before any command completes. The lease stays
          # held until it expires.
          h.scheduler.schedule(actor: "faulty-worker", delay: h.scheduler.rng.int(4), name: "claim_then_crash") do
            drain.call("faulty-worker", 5)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "faulty-worker", "claim_window_crashed", id: workflow_id, op: crash_op.to_s)
          end

          # Recovery runs well past the 5-tick lease so the held lease has
          # expired and is reclaimable.
          h.scheduler.schedule(actor: "recovery-worker", delay: 20, name: "drain_after_expiry") do
            drain.call("recovery-worker", 30)
          end

          h.scheduler.schedule(actor: "finisher", delay: 40, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("a claim-window crash was observed") do
            h.scheduler.trace.to_s.include?("claim_window_crashed")
          end
          h.check("every command is completed exactly once in the store") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("wakeup row fully reconciled away after recovery drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end

      #: (untyped) -> untyped
      def object_command_failure_exhaustion(seed)
        run(seed, "object_command_failure_exhaustion") do |h|
          # A durable-object command fails repeatedly. Each non-terminal failure
          # (fail_object_command -> fail_inbox_message) marks the message
          # 'failed' (still activatable, so reconcile re-arms the wakeup row and
          # it is re-delivered) until attempts reach max_attempts, at which point
          # the CASE in fail_inbox_message auto-dead-letters it. Exercises the
          # exhaustion-aware failure path + the 'failed' re-delivery edge of the
          # inbox state machine, plus the object-command claim path — all
          # previously unreached by DST (which only drove workflow commands).
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          max_attempts = 2 + h.scheduler.rng.int(2) # 2..3 attempts before exhaustion

          command_id = h.store.enqueue_object_command(
            object_type:,
            object_id:,
            method_name: "bump",
            args: [seed],
            kwargs: {},
            max_attempts:,
          )

          failures = 0
          h.scheduler.schedule(actor: "object-worker", delay: 5, name: "fail_until_exhausted") do
            # Bounded above max_attempts so a missing dead-letter spins the check
            # red rather than the virtual clock forever.
            (max_attempts + 3).times do
              claimed = h.store.claim_object_command(command_id:, worker_id: "object-worker", lease_seconds: 30)
              break if claimed.nil?

              h.store.fail_object_command(command_id:, error: "boom #{failures}", worker_id: "object-worker", terminal: false)
              failures += 1
            end
          end

          h.check("the command failed exactly max_attempts times before exhaustion") do
            failures == max_attempts
          end
          h.check("the exhausted command is dead-lettered with attempts == max_attempts") do
            message = h.store.inbox_message(command_id)
            message && message.fetch("status") == "dead_lettered" && message.fetch("attempts").to_i == max_attempts
          end
          h.check("wakeup row reconciled away after the command is exhausted") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end

      #: (untyped) -> untyped
      def object_command_claim_contention(seed)
        run(seed, "object_command_claim_contention") do |h|
          # The command-mailbox lease has never been contended: every existing
          # concurrency scenario (lease_conflict, lease_expiry, multi_worker)
          # races on the *workflow* claim path. Here worker A claims an object
          # command under a short lease and stalls (never completes). While that
          # lease is live, worker B must be BLOCKED from claiming the same head
          # (claim_object_command -> inbox_row_claimable? false for a leased
          # running row) — proving mutual exclusion. After A's lease expires, B
          # reclaims via the expired-running path and completes the command
          # exactly once, advancing the durable counter by exactly one. This
          # pins lease mutual-exclusion + expiry-reclaim + exactly-once for the
          # inbox/command path the delivery loop depends on.
          object_type = "counter-object"
          object_id = "obj-#{seed}"

          command_id = h.store.enqueue_object_command(
            object_type:,
            object_id:,
            method_name: "bump",
            args: [seed],
            kwargs: {},
            max_attempts: 50,
          )

          # A takes a short (8-tick) lease at t=0 and never completes — a worker
          # that grabbed the command then stalled.
          h.scheduler.schedule(actor: "worker-a", delay: 0, name: "claim_and_stall") do
            claimed = h.store.claim_object_command(command_id:, worker_id: "worker-a", lease_seconds: 8)
            h.scheduler.trace.event(h.scheduler.time, "worker-a", claimed ? "a_claimed" : "a_claim_failed", id: command_id)
          end

          # B probes while A's lease is live (t=3). It must NOT be able to claim.
          h.scheduler.schedule(actor: "worker-b", delay: 3, name: "probe_during_live_lease") do
            claimed = h.store.claim_object_command(command_id:, worker_id: "worker-b", lease_seconds: 8)
            if claimed
              h.scheduler.trace.event(h.scheduler.time, "worker-b", "b_stole_live_lease", id: command_id)
            else
              h.scheduler.trace.event(h.scheduler.time, "worker-b", "b_blocked_by_lease", id: command_id)
            end
          end

          # After A's lease expires (t>=8), B reclaims and completes exactly once.
          h.scheduler.schedule(actor: "worker-b", delay: 12, name: "reclaim_after_expiry") do
            claimed = h.store.claim_object_command(command_id:, worker_id: "worker-b", lease_seconds: 30)
            next unless claimed

            current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
            h.store.complete_object_command(
              command_id:,
              result: { "ok" => command_id },
              object_type:,
              object_id:,
              state: { "n" => current.fetch("n") + 1 },
            )
            h.scheduler.trace.event(h.scheduler.time, "worker-b", "b_completed_after_reclaim", id: command_id)
          end

          h.check("B was blocked while A held a live lease") do
            trace = h.scheduler.trace.to_s
            trace.include?("a_claimed") && trace.include?("b_blocked_by_lease") && !trace.include?("b_stole_live_lease")
          end
          h.check("the command completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }
            completed.length == 1 && completed.first.fetch("id") == command_id
          end
          h.check("the durable counter advanced by exactly one") do
            state = h.store.object_state(object_type:, object_id:)
            state && state.fetch("n") == 1
          end
          h.check("wakeup row reconciled away after the command completed") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end

      #: (untyped) -> untyped
      def object_command_crash_fuzz(seed)
        run(seed, "object_command_crash_fuzz") do |h|
          # Point the generic crash-after-write fuzz (enable_write_crashes!) at
          # the durable-object command failure loop, which it has never driven
          # (chaos only crashes the workflow-resume path). Each claim/fail step
          # is its own transaction; fail_object_command commits
          # fail_inbox_message + reconcile_target_activation together, so a
          # :mid_transaction crash must roll BOTH back, while an :after_commit
          # crash leaves a durable, consistent state. Because attempts increment
          # at claim (mark_inbox_row_running), a crash after a committed claim
          # but before the fail "burns" an attempt: the command may dead-letter
          # with attempts >= max_attempts after fewer real failures, and attempts
          # can even exceed max_attempts across reclaim waves. The invariant that
          # must survive every crash window is liveness: the command reaches a
          # consistent terminal (dead_lettered) state with its activation
          # reconciled away, and the harness store invariants
          # (activation<->inbox consistency, no stuck activation) hold.
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          max_attempts = 2 + h.scheduler.rng.int(2) # 2..3

          command_id = h.store.enqueue_object_command(
            object_type:,
            object_id:,
            method_name: "bump",
            args: [seed],
            kwargs: {},
            max_attempts:,
          )

          h.store.enable_write_crashes!(percent: 25)

          # Crashing worker waves. Each holds a short (8-tick) lease and rescues
          # InjectedCrash (a modelled process death). Waves are spaced wider than
          # the lease so an abandoned, expired-running inbox row is reclaimable by
          # the next wave via inbox_row_claimable?.
          drain = lambda do |worker_id|
            (max_attempts + 5).times do
              claimed = h.store.claim_object_command(command_id:, worker_id:, lease_seconds: 8)
              break if claimed.nil?

              h.store.fail_object_command(command_id:, error: "boom", worker_id:, terminal: false)
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "obj-worker-#{w}", delay: 5 + w * 12, name: "fail_with_crashes") do
              h.store.crashable { drain.call("obj-worker-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "obj-worker-#{w}", "object_command_crashed", id: command_id)
            end
          end

          # Reaper expires any abandoned workflow/fence leases between waves.
          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 10 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          # Disarm crashes, then one final guaranteed-clean drain so liveness is
          # not at the mercy of every wave happening to crash before exhaustion.
          h.scheduler.schedule(actor: "settler", delay: 70, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 80, name: "final_drain") do
            drain.call("closer")
          end

          h.check("the command eventually dead-letters with attempts >= max_attempts") do
            message = h.store.inbox_message(command_id)
            message && message.fetch("status") == "dead_lettered" && message.fetch("attempts").to_i >= max_attempts
          end
          h.check("wakeup row reconciled away once the command is terminal") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end

      #: (untyped) -> untyped
      def object_command_state_crash_fuzz(seed)
        run(seed, "object_command_state_crash_fuzz") do |h|
          # Exercises the durable-object STATE persistence path under crash
          # fuzz, which no scenario reached: complete_object_command commits
          # save_object_state + complete_inbox_message + reconcile in ONE
          # transaction. A worker reads the object's counter, increments it, and
          # completes the command with the new state. Commands for one object
          # are strict-FIFO (claim_object_command only takes the inbox head), so
          # they apply in order, one at a time — no concurrent read-modify-write.
          # The exactly-once invariant that must hold across every crash window:
          # the final persisted counter equals the number of commands (a
          # :mid_transaction crash rolls back the save AND the completion, so the
          # command re-delivers and re-applies once; an :after_commit crash
          # leaves both durable, so it is never re-applied). Splitting that
          # transaction would either strand state (completion without save) or
          # double-count (save without completion) — both caught here.
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          command_count = 2 + h.scheduler.rng.int(3) # 2..4

          command_ids = command_count.times.map do |i|
            h.store.enqueue_object_command(
              object_type:,
              object_id:,
              method_name: "bump",
              args: [i],
              kwargs: {},
              max_attempts: 50, # never exhaust; we only complete, never fail
            )
          end

          h.store.enable_write_crashes!(percent: 20)

          # Claim the head (only the head is claimable), read-modify-write the
          # counter, and complete. Iterating ids in enqueue order naturally hits
          # the current head first.
          apply = lambda do |worker_id|
            (command_count * 2 + 5).times do
              progressed = false
              command_ids.each do |command_id|
                claimed = h.store.claim_object_command(command_id:, worker_id:, lease_seconds: 8)
                next if claimed.nil?

                current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
                h.store.complete_object_command(
                  command_id:,
                  result: { "ok" => command_id },
                  object_type:,
                  object_id:,
                  state: { "n" => current.fetch("n") + 1 },
                )
                progressed = true
              end
              break unless progressed
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "state-worker-#{w}", delay: 5 + w * 12, name: "apply_with_crashes") do
              h.store.crashable { apply.call("state-worker-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "state-worker-#{w}", "object_state_crashed", id: object_id)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 10 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 70, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 80, name: "final_apply") do
            apply.call("closer")
          end

          h.check("every command completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("the persisted counter equals the command count (applied exactly once each)") do
            state = h.store.object_state(object_type:, object_id:)
            state && state.fetch("n") == command_count
          end
          h.check("wakeup row reconciled away once every command is applied") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end

      #: (untyped) -> untyped
      def object_command_retry_then_apply_crash_fuzz(seed)
        run(seed, "object_command_retry_then_apply_crash_fuzz") do |h|
          # Exercises retry_object_command under crash fuzz combined with state
          # mutation -- a path NO scenario reached. object_command_state_crash_fuzz
          # only ever completes; object_command_failure_exhaustion uses
          # fail_object_command (no state, no crashes). Here each command suffers
          # one or more TRANSIENT failures (retry_object_command, which records the
          # error + re-arms but must NOT touch object state) before eventually
          # completing with state n+1. The exactly-once invariant under every crash
          # window: the final counter equals the command count -- a transient
          # failure must never apply state, and a command that retries N times then
          # completes once must apply its bump exactly once. If retry leaked into
          # the state-writing path, or completion double-applied across a re-claim,
          # the counter would drift; if retry failed to re-arm/clear its lease, a
          # command would strand and the counter would fall short. The fenced
          # retry+completion paths (worker_id passed to both) are driven here.
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          command_count = 2 + h.scheduler.rng.int(3) # 2..4

          thresholds = {}
          command_ids = command_count.times.map do |i|
            id = h.store.enqueue_object_command(
              object_type:,
              object_id:,
              method_name: "bump",
              args: [i],
              kwargs: {},
              max_attempts: 100, # never exhaust; transient failures retry, never dead-letter
            )
            # Each command fails transiently 1..2 times before it is allowed to
            # complete. The decision is keyed on the persisted attempt count, so it
            # is crash-consistent: a rolled-back retry does not advance attempts.
            thresholds[id] = 1 + h.scheduler.rng.int(2)
            id
          end

          h.store.enable_write_crashes!(percent: 20)

          apply = lambda do |worker_id|
            (command_count * 8 + 10).times do
              progressed = false
              command_ids.each do |command_id|
                claimed = h.store.claim_object_command(command_id:, worker_id:, lease_seconds: 8)
                next if claimed.nil?

                attempts = claimed.fetch("attempts").to_i
                if attempts <= thresholds.fetch(command_id)
                  # Transient failure: re-arm for immediate retry. Must not touch
                  # object state. ready_at == current_time keeps it claimable now.
                  h.store.retry_object_command(
                    command_id:,
                    error: "transient #{attempts}",
                    worker_id:,
                    ready_at: h.store.current_time,
                  )
                else
                  current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
                  h.store.complete_object_command(
                    command_id:,
                    result: { "ok" => command_id },
                    object_type:,
                    object_id:,
                    state: { "n" => current.fetch("n") + 1 },
                    worker_id:,
                  )
                end
                progressed = true
              end
              break unless progressed
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "retry-worker-#{w}", delay: 5 + w * 11, name: "apply_with_retries") do
              h.store.crashable { apply.call("retry-worker-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "retry-worker-#{w}", "object_retry_crashed", id: object_id)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 10 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 80, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 90, name: "final_apply") do
            apply.call("closer")
          end

          h.check("every command completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("every completed command recorded at least one transient retry") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            messages.all? { |message| message.fetch("attempts").to_i >= 2 }
          end
          h.check("the persisted counter equals the command count (state applied exactly once despite retries)") do
            state = h.store.object_state(object_type:, object_id:)
            state && state.fetch("n") == command_count
          end
          h.check("wakeup row reconciled away once every command is applied") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end

      #: (untyped) -> untyped
      def object_command_activation_driven_drain(seed)
        run(seed, "object_command_activation_driven_drain") do |h|
          # The durable-object command path has only ever been driven by
          # claim_object_command(command_id:) directly; the realistic #69
          # delivery loop — claim_target_activation(target_kinds: ["object"]) ->
          # claim_inbox_messages(limit: 1) -> complete_object_command ->
          # complete_target_activation — has never run for objects (only
          # workflow commands exercise claim_target_activation). With multiple
          # commands queued to one object this also pins the activation
          # HEAD-HANDOFF: after the head completes, reconcile must keep the
          # wakeup row alive for the next pending head and retire it only when
          # the mailbox empties. A handoff bug (retiring the activation while
          # pending work remains) strands the tail, and an activation-driven
          # worker — which only touches the mailbox when an activation says
          # there is work — stops, leaving an undelivered command that the
          # lost-wakeup checker flags. Run under crash fuzz so every window in
          # the multi-transaction loop is exercised.
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          command_count = 2 + h.scheduler.rng.int(3) # 2..4

          command_ids = command_count.times.map do |i|
            h.store.enqueue_object_command(
              object_type:,
              object_id:,
              method_name: "bump",
              args: [i],
              kwargs: {},
              max_attempts: 50, # never exhaust; the loop only completes
            )
          end

          h.store.enable_write_crashes!(percent: 20)

          # Activation-driven drain: only act when an activation says there is
          # work. This is what makes a wrongly-retired wakeup row fatal (the
          # tail strands) rather than masked by blindly iterating ids.
          drain = lambda do |worker_id|
            (command_count * 2 + 6).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds: 8,
                target_kinds: ["object"],
                target_types: [object_type],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "object",
                target_type: object_type,
                target_id: object_id,
                worker_id:,
                lease_seconds: 8,
                limit: 1,
              )
              messages.each do |message|
                current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
                h.store.complete_object_command(
                  command_id: message.fetch("id"),
                  result: { "ok" => message.fetch("id") },
                  object_type:,
                  object_id:,
                  state: { "n" => current.fetch("n") + 1 },
                  worker_id:,
                )
              end
              h.store.complete_target_activation(
                target_kind: "object",
                target_type: object_type,
                target_id: object_id,
                worker_id:,
              )
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "obj-deliverer-#{w}", delay: 5 + w * 12, name: "drain_with_crashes") do
              h.store.crashable { drain.call("obj-deliverer-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "obj-deliverer-#{w}", "object_delivery_crashed", id: object_id)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 10 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 80, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 90, name: "final_drain") do
            drain.call("closer")
          end

          h.check("every command delivered exactly once via the activation loop") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("the durable counter equals the command count (applied exactly once each)") do
            state = h.store.object_state(object_type:, object_id:)
            state && state.fetch("n") == command_count
          end
          h.check("wakeup row reconciled away once the mailbox drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end

      #: (untyped) -> untyped
      def object_command_multi_target_isolation(seed)
        run(seed, "object_command_multi_target_isolation") do |h|
          # Every object-command scenario so far drove a SINGLE target, so a
          # reconcile/head lookup that ignored target_id (keyed only by
          # worker_pool/target_kind/target_type) would pass them all — there is
          # only one target to confuse. Here three distinct objects share a
          # worker_pool and object_type, with interleaved enqueues, drained by
          # the activation loop under crash fuzz. The activation a worker claims
          # carries a target_id; it must drain ONLY that object's mailbox and
          # reconcile ONLY that object's wakeup row. A target_id-blind reconcile
          # would delete a sibling's still-pending activation (lost wakeup) or
          # apply a command to the wrong object's state. The invariant: each
          # object's commands are delivered exactly once to that object and its
          # counter equals its own command count — no cross-contamination.
          object_type = "counter-object"
          objects = ["a", "b", "c"].map { |suffix| "obj-#{suffix}-#{seed}" }
          command_ids = objects.to_h { |object_id| [object_id, []] }

          # Interleave enqueues round-robin so the three mailboxes are live
          # concurrently rather than drained one target at a time.
          (2 * objects.length + h.scheduler.rng.int(3)).times do |i|
            object_id = objects[i % objects.length]
            command_ids[object_id] << h.store.enqueue_object_command(
              object_type:,
              object_id:,
              method_name: "bump",
              args: [i],
              kwargs: {},
              max_attempts: 50,
            )
          end

          h.store.enable_write_crashes!(percent: 20)

          # Activation-driven drain over ALL objects of this type: claim whatever
          # target the wakeup table offers, then act strictly on that target_id.
          drain = lambda do |worker_id|
            (command_ids.values.sum(&:length) * 2 + 8).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds: 8,
                target_kinds: ["object"],
                target_types: [object_type],
              )
              break if activation.nil?

              target_id = activation.fetch("target_id")
              messages = h.store.claim_inbox_messages(
                target_kind: "object",
                target_type: object_type,
                target_id:,
                worker_id:,
                lease_seconds: 8,
                limit: 1,
              )
              messages.each do |message|
                current = h.store.object_state(object_type:, object_id: target_id) || { "n" => 0 }
                h.store.complete_object_command(
                  command_id: message.fetch("id"),
                  result: { "ok" => message.fetch("id") },
                  object_type:,
                  object_id: target_id,
                  state: { "n" => current.fetch("n") + 1 },
                  worker_id:,
                )
              end
              h.store.complete_target_activation(
                target_kind: "object",
                target_type: object_type,
                target_id:,
                worker_id:,
              )
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "obj-deliverer-#{w}", delay: 5 + w * 11, name: "drain_with_crashes") do
              h.store.crashable { drain.call("obj-deliverer-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "obj-deliverer-#{w}", "multi_target_crashed", seed:)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 9 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 90, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 100, name: "final_drain") do
            drain.call("closer")
          end

          objects.each do |object_id|
            h.check("#{object_id}: every command delivered exactly once to its own mailbox") do
              messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
              completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
              completed.sort == command_ids.fetch(object_id).sort && completed.length == completed.uniq.length
            end
            h.check("#{object_id}: counter equals its own command count (no cross-contamination)") do
              state = h.store.object_state(object_type:, object_id:)
              state && state.fetch("n") == command_ids.fetch(object_id).length
            end
          end
          h.check("no wakeup rows survive once every mailbox drains") do
            h.store.all_target_activations.none? { |activation| objects.include?(activation.fetch("target_id")) }
          end
        end
      end

      #: (untyped) -> untyped
      def object_command_idempotent_enqueue(seed)
        run(seed, "object_command_idempotent_enqueue") do |h|
          # enqueue_inbox_message dedups on idempotency_key: a repeated enqueue
          # of the same key+shape returns the existing message id and only
          # re-arms the wakeup row if that message is still activatable. No DST
          # scenario exercised this client-facing exactly-once guarantee. The
          # invariant pinned here: however many times the same command is
          # (re-)enqueued — before delivery, racing with it, or as a LATE
          # duplicate after the command has already completed — it is delivered
          # and applied exactly once. The late-duplicate case is the sharp edge:
          # the existing message is `completed` (not activatable) so the dedup
          # branch must NOT re-arm the activation, otherwise a worker would be
          # woken to re-run an already-applied command. Enqueues run under crash
          # fuzz with retry, so a crash mid/after the enqueue transaction (the
          # exact client-retry-with-same-key situation) must still collapse to
          # one message.
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          key = "idem-#{seed}"
          enqueue_count = 3 + h.scheduler.rng.int(3) # 3..5 duplicate enqueues

          enqueue = lambda do
            h.store.enqueue_object_command(
              object_type:,
              object_id:,
              method_name: "bump",
              args: [seed], # identical shape every time => dedups
              kwargs: {},
              idempotency_key: key,
              max_attempts: 50,
            )
          end

          # First enqueue is guaranteed (no crashes yet) so there is always work.
          enqueue.call
          h.store.enable_write_crashes!(percent: 20)

          # Duplicate enqueues scattered across the timeline, each retried if the
          # write crashes — the realistic "client retried with the same key"
          # path. Some land before delivery, some after the command completes.
          enqueue_count.times do |i|
            h.scheduler.schedule(actor: "enqueuer-#{i}", delay: 2 + i * 9, name: "dup_enqueue") do
              h.store.crashable { enqueue.call }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "enqueuer-#{i}", "enqueue_crashed", id: object_id)
              h.store.enqueue_object_command(
                object_type:,
                object_id:,
                method_name: "bump",
                args: [seed],
                kwargs: {},
                idempotency_key: key,
                max_attempts: 50,
              )
            end
          end

          # A single delivery worker claims the head and applies it exactly once.
          h.scheduler.schedule(actor: "deliverer", delay: 6, name: "deliver_once") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            head = messages.min_by { |message| message.fetch("sequence") }
            next if head.nil?

            claimed = h.store.claim_object_command(command_id: head.fetch("id"), worker_id: "deliverer", lease_seconds: 30)
            next unless claimed

            current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
            h.store.complete_object_command(
              command_id: head.fetch("id"),
              result: { "ok" => head.fetch("id") },
              object_type:,
              object_id:,
              state: { "n" => current.fetch("n") + 1 },
              worker_id: "deliverer",
            )
          end

          # Disarm crashes and re-run a guaranteed delivery so a wrongly re-armed
          # late duplicate (re-execution) would advance the counter past one and
          # trip the exactly-once check, rather than silently passing.
          h.scheduler.schedule(actor: "settler", delay: 70, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 80, name: "final_deliver") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            head = messages.select { |message| ["pending", "failed", "running"].include?(message.fetch("status")) }
              .min_by { |message| message.fetch("sequence") }
            next if head.nil?

            claimed = h.store.claim_object_command(command_id: head.fetch("id"), worker_id: "closer", lease_seconds: 30)
            next unless claimed

            current = h.store.object_state(object_type:, object_id:) || { "n" => 0 }
            h.store.complete_object_command(
              command_id: head.fetch("id"),
              result: { "ok" => head.fetch("id") },
              object_type:,
              object_id:,
              state: { "n" => current.fetch("n") + 1 },
              worker_id: "closer",
            )
          end

          h.check("repeated idempotent enqueues collapsed to exactly one message") do
            messages = h.store.inbox_messages_for(target_kind: "object", target_type: object_type, target_id: object_id)
            messages.length == 1
          end
          h.check("the command was applied exactly once despite duplicate enqueues") do
            state = h.store.object_state(object_type:, object_id:)
            state && state.fetch("n") == 1
          end
          h.check("a late duplicate of the completed command did not re-arm a wakeup") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end

      #: (untyped) -> untyped
      def workflow_command_terminal_failure(seed)
        run(seed, "workflow_command_terminal_failure") do |h|
          # The final queued command fails terminally (fail_workflow_command ->
          # dead_lettered, workflow still alive), while the earlier commands
          # complete normally. Exercises the alive-workflow dead-letter branch of
          # fail_workflow_command (history append + dead_letter_inbox_message +
          # reconcile), which is unit-test-only and otherwise unreached by DST.
          # Failing the LAST command means there is no tail behind the
          # dead-lettered head, so the documented "dead-lettered head wedges the
          # FIFO" behaviour (harness verify_activation_inbox_consistency!) leaves
          # nothing stranded: the activation is correctly reconciled away.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 1 + h.scheduler.rng.int(3) # 1..3 commands
          command_ids = []
          command_count.times do |i|
            command_ids << h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end

          doomed = command_ids.last
          completed = []
          dead_lettered = []
          h.scheduler.schedule(actor: "command-worker", delay: 5, name: "drain_with_terminal_failure") do
            (command_count + 3).times do
              activation = h.store.claim_target_activation(
                worker_id: "command-worker",
                lease_seconds: 30,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
                lease_seconds: 30,
                limit: 1,
              )
              messages.each do |message|
                id = message.fetch("id")
                if id == doomed
                  h.store.fail_workflow_command(message_id: id, workflow_id:, error: "terminal command failure", worker_id: "command-worker")
                  dead_lettered << id
                else
                  h.store.complete_workflow_command(message_id: id, workflow_id:, result: { "ok" => id }, worker_id: "command-worker")
                  completed << id
                end
              end
              h.store.complete_target_activation(target_kind: "workflow", target_type: "counter", target_id: workflow_id, worker_id: "command-worker")
            end
          end

          h.scheduler.schedule(actor: "finisher", delay: 20, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("the doomed command was dead-lettered, not completed") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            doomed_row = messages.find { |message| message.fetch("id") == doomed }
            doomed_row && doomed_row.fetch("status") == "dead_lettered" && !completed.include?(doomed)
          end
          h.check("every non-doomed command completed exactly once") do
            expected = command_ids[0...-1].sort
            completed.sort == expected && completed.length == completed.uniq.length
          end
          h.check("wakeup row reconciled away after the terminal-failure drain") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end

      #: (untyped) -> untyped
      def workflow_command_terminal_failure_crash_fuzz(seed)
        run(seed, "workflow_command_terminal_failure_crash_fuzz") do |h|
          # Crash-fuzzes the alive-workflow branch of fail_workflow_command, which
          # writes THREE rows in one transaction: append_workflow_history
          # (workflow_command_failed) + dead_letter_inbox_message + reconcile. Only
          # workflow_command_terminal_failure reached this branch and it ran with no
          # crashes. This is the same multi-write-atomicity class as the original
          # step-failure bug: a :mid_transaction crash must roll back ALL three
          # (history not appended, message still running, activation not reconciled),
          # so a recovery worker re-claims and re-fails -- the command must end
          # dead-lettered exactly once AND the workflow_command_failed history entry
          # must appear exactly once. If the history append were not atomic with the
          # dead-letter, a crash between them would leave a dangling history row and
          # re-delivery would append a second -> duplicate. The last command is the
          # doomed one (no tail behind the dead-lettered head, so nothing strands).
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 2 + h.scheduler.rng.int(3) # 2..4 commands
          command_ids = []
          command_count.times do |i|
            command_ids << h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end
          doomed = command_ids.last

          h.store.enable_write_crashes!(percent: 20)

          drain = lambda do |worker_id|
            (command_count * 3 + 5).times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds: 8,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id:,
                lease_seconds: 8,
                limit: 1,
              )
              messages.each do |message|
                id = message.fetch("id")
                if id == doomed
                  h.store.fail_workflow_command(message_id: id, workflow_id:, error: "terminal command failure", worker_id:)
                else
                  h.store.complete_workflow_command(message_id: id, workflow_id:, result: { "ok" => id }, worker_id:)
                end
              end
              h.store.complete_target_activation(target_kind: "workflow", target_type: "counter", target_id: workflow_id, worker_id:)
            end
          end

          4.times do |w|
            h.scheduler.schedule(actor: "fail-worker-#{w}", delay: 5 + w * 11, name: "drain_with_crashes") do
              h.store.crashable { drain.call("fail-worker-#{w}") }
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "fail-worker-#{w}", "terminal_failure_crashed", id: workflow_id)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 10 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 80, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 90, name: "final_drain") do
            drain.call("closer")
          end
          h.scheduler.schedule(actor: "finisher", delay: 130, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("the doomed command is dead-lettered") do
            message = h.store.inbox_message(doomed)
            message && message.fetch("status") == "dead_lettered"
          end
          h.check("the workflow_command_failed history entry appears exactly once (atomic with dead-letter)") do
            failed = h.store.workflow_history_for(workflow_id).select do |entry|
              entry.fetch("kind") == "workflow_command_failed" && entry["attempt_id"] == doomed
            end
            failed.length == 1
          end
          h.check("every non-doomed command completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            completed = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            completed.sort == command_ids[0...-1].sort && completed.length == completed.uniq.length
          end
          h.check("wakeup row reconciled away after the terminal-failure drain") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end

      #: (untyped) -> untyped
      def workflow_command_retry_then_complete(seed)
        run(seed, "workflow_command_retry_then_complete") do |h|
          # Each command's first delivery attempt fails transiently
          # (retry_object_command -> message back to pending, immediately
          # re-ready) and its second attempt completes. Exercises the inbox
          # retry state machine (retry_inbox_message + reconcile-to-pending,
          # which clears the activation lease and re-arms it) and proves every
          # command is re-delivered and ultimately completed exactly once, with
          # none lost or double-completed. The happy-path/crash delivery
          # scenarios never fail a command, so this path was previously
          # unexercised by DST.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 1 + h.scheduler.rng.int(3) # 1..3 commands
          command_ids = []
          command_count.times do |i|
            command_ids << h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end

          retried = Hash.new(0)
          completed = []
          h.scheduler.schedule(actor: "command-worker", delay: 5, name: "drain_with_retries") do
            # Bounded at two passes per command (retry + complete) plus slack so
            # a stuck head fails a check rather than spinning the virtual clock.
            (command_count * 2 + 3).times do
              activation = h.store.claim_target_activation(
                worker_id: "command-worker",
                lease_seconds: 30,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
                lease_seconds: 30,
                limit: 1,
              )
              messages.each do |message|
                id = message.fetch("id")
                if retried[id].zero?
                  retried[id] += 1
                  # ready_at = now means the retried head is immediately
                  # re-deliverable in this same drain pass.
                  h.store.retry_object_command(
                    command_id: id,
                    error: "transient delivery failure",
                    worker_id: "command-worker",
                    ready_at: h.store.current_time,
                  )
                else
                  h.store.complete_workflow_command(
                    message_id: id,
                    workflow_id:,
                    result: { "ok" => id },
                    worker_id: "command-worker",
                  )
                  completed << id
                end
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
              )
            end
          end

          h.scheduler.schedule(actor: "finisher", delay: 20, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.check("every command was retried exactly once before completing") do
            command_ids.all? { |id| retried[id] == 1 }
          end
          h.check("every command completed exactly once after its retry") do
            completed.sort == command_ids.sort && completed.length == completed.uniq.length
          end
          h.check("inbox shows each command completed exactly once") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            done = messages.select { |message| message.fetch("status") == "completed" }.map { |message| message.fetch("id") }
            done.sort == command_ids.sort
          end
          h.check("wakeup row reconciled away after the mailbox drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
        end
      end

      #: (untyped) -> untyped
      def workflow_command_delivery_to_terminal_workflow(seed)
        run(seed, "workflow_command_delivery_to_terminal_workflow") do |h|
          # A workflow terminates while commands are still queued in its inbox
          # (they were enqueued before it terminated). Delivery must dead-letter
          # every pending command rather than execute it against a terminal
          # workflow, and the wakeup row must be reconciled away. This exercises
          # the terminal branch of complete_workflow_command +
          # reconcile_target_activation (dead_letter_terminal_workflow_inbox +
          # delete activation), which the happy-path delivery scenarios — which
          # always finish the workflow AFTER draining — never reach.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })

          command_count = 1 + h.scheduler.rng.int(3) # 1..3 commands
          command_ids = []
          command_count.times do |i|
            command_ids << h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name: "counter",
              method_name: "bump",
              payload: { "method" => "bump", "args" => [i], "kwargs" => {} },
            )
          end

          # Terminate the workflow before anything drains the mailbox.
          h.scheduler.schedule(actor: "finisher", delay: 1, name: "complete_workflow") do
            h.store.mark_workflow_running(workflow_id, worker_id: "finisher", lease_seconds: 30)
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "finisher")
          end

          h.scheduler.schedule(actor: "command-worker", delay: 10, name: "drain_commands") do
            (command_count + 3).times do
              activation = h.store.claim_target_activation(
                worker_id: "command-worker",
                lease_seconds: 30,
                target_kinds: ["workflow"],
                target_types: ["counter"],
              )
              break if activation.nil?

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
                lease_seconds: 30,
                limit: 1,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id:,
                  result: { "ok" => message.fetch("id") },
                  worker_id: "command-worker",
                )
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "counter",
                target_id: workflow_id,
                worker_id: "command-worker",
              )
            end
          end

          h.check("no command was completed against the terminal workflow") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            messages.none? { |message| message.fetch("status") == "completed" }
          end
          h.check("every queued command was dead-lettered, not lost") do
            messages = h.store.inbox_messages_for(target_kind: "workflow", target_type: "counter", target_id: workflow_id)
            dead = messages.select { |message| message.fetch("status") == "dead_lettered" }.map { |message| message.fetch("id") }
            dead.sort == command_ids.sort
          end
          h.check("wakeup row reconciled away after the terminal mailbox drains") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == workflow_id }
          end
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
      def cancellation_cleanup_crash_fuzz(seed)
        run(seed, "cancellation_cleanup_crash_fuzz") do |h|
          # Crash-fuzzes request_workflow_cancellation, which the existing
          # cooperative_cancellation_cleanup scenario never exercises under
          # crashes. On the FIRST cancellation request it runs a multi-write
          # transaction: set cancel_requested_at + cancel_pending_waits_for_workflow
          # (which itself cancels the pending wait, the waiting step, AND the
          # waiting step attempt -- three writes) + mark_canceling. Every write is
          # gated on first_request (cancel_requested_at IS NULL), so atomicity is
          # load-bearing in a way the original step-failure bug was: if a crash
          # committed cancel_requested_at but NOT the wait/step/attempt
          # cancellations, the idempotent re-request would see first_request=false
          # and SKIP them -- stranding a pending wait, waiting step, and waiting
          # attempt forever. A :mid_transaction crash must roll the whole thing
          # back so a later request redoes it cleanly.
          #
          # The workflow waits on a timer, gets canceled, then runs a cleanup step
          # and lands terminal `canceled`. Crashing clients race the cancel request
          # and crashing workers race the resume/cleanup; reapers reclaim expired
          # leases; a crash-free tail (guaranteed cancel + closer) drives it to a
          # terminal state so progress is assured regardless of which crashes fired.
          cleanup_runs = 0
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
              instance.step_context.heartbeat.record({ "phase" => "cleanup", "run" => cleanup_runs })
              { "cleaned" => true, "reason" => input.fetch("reason") }
            end
            step :cleanup
          end

          id = h.store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => seed.to_s })

          h.store.enable_write_crashes!(percent: 20)

          # Park the workflow on its wait timer first (crash-free) so cancellation
          # always races a genuinely-waiting workflow.
          h.scheduler.schedule(actor: "parker", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "parker").resume(workflow, workflow_id: id)
          end

          # Several clients request cancellation concurrently under crashes; the
          # request must be idempotent and atomic.
          3.times do |c|
            h.scheduler.schedule(actor: "canceler-#{c}", delay: 5 + c * 7, name: "request_cancel") do
              h.store.crashable do
                workflow.handle(id, store: h.store).cancel(reason: "stop #{seed}")
              end
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "canceler-#{c}", "cancel_request_crashed", id:)
            end
          end

          # Crashing workers race to resume, deliver the cancellation, and run
          # cleanup.
          4.times do |w|
            h.scheduler.schedule(actor: "cancel-worker-#{w}", delay: 9 + w * 9, name: "resume_with_crashes") do
              h.store.crashable do
                Durababble::Engine.new(store: h.store, worker_id: "cancel-worker-#{w}", lease_seconds: 12).resume(workflow, workflow_id: id)
              end
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "cancel-worker-#{w}", "cancellation_crashed", id:)
            rescue Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "cancel-worker-#{w}", "cancellation_lease_conflict", id:)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 12 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 100, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          # Crash-free guaranteed cancel: ensures the request lands durably even if
          # every crashing canceler above rolled back. Idempotent -- if an earlier
          # request already committed, first_request is false and this is a no-op;
          # if a non-atomic crash stranded the wait/step/attempt, this re-request
          # would (wrongly, under a buggy split) skip them, and the invariants below
          # catch it.
          h.scheduler.schedule(actor: "guaranteed-canceler", delay: 105, name: "ensure_cancel") do
            workflow.handle(id, store: h.store).cancel(reason: "stop #{seed}")
          end
          # Crash-free closer: free any stranded lease, then resume to a terminal
          # state. Two passes in case the first only delivers cancellation and the
          # second runs cleanup. Tolerate a LeaseConflict (final invariant catches
          # a workflow that never reached terminal).
          [110, 125].each do |delay|
            h.scheduler.schedule(actor: "closer", delay:, name: "final_resume") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
              Durababble::Engine.new(store: h.store, worker_id: "closer", lease_seconds: 30).resume(workflow, workflow_id: id)
            rescue Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "closer", "closer_lease_conflict", id:)
            end
          end

          h.check("workflow lands terminal canceled") do
            h.store.workflow(id).fetch("status") == "canceled"
          end
          h.check("cleanup step recorded completed exactly once") do
            cleanup_steps = h.store.steps_for(id).select { |step| step.fetch("name") == "cleanup" }
            cleanup_steps.length == 1 && cleanup_steps.first.fetch("status") == "completed"
          end
          h.check("no pending wait stranded by cancellation (request_workflow_cancellation atomic)") do
            h.store.all_waits.values.none? do |wait|
              wait.fetch("workflow_id") == id && wait.fetch("status") == "pending"
            end
          end
          h.check("no waiting step stranded by cancellation") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
          h.check("no waiting step attempt stranded by cancellation") do
            h.store.step_attempts_for(id).none? { |attempt| attempt.fetch("status") == "waiting" }
          end
          h.check("no wakeup row survives the cancellation drain") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == id }
          end
        end
      end

      #: (untyped) -> untyped
      def cancellation_during_suspend_race(seed)
        run(seed, "cancellation_during_suspend_race") do |h|
          # A cancellation request that lands while the workflow is RUNNING -- after
          # it has been claimed but BEFORE its step records a wait -- is a different
          # race from cooperative_cancellation_cleanup, which parks the workflow
          # first so cancellation always sees a genuinely-waiting workflow (and its
          # first-request cleanup, cancel_pending_waits_for_workflow, terminalizes
          # the live wait/step/attempt). Here request_workflow_cancellation runs
          # while status is 'running', so that cleanup finds NOTHING to cancel --
          # the wait/step/attempt do not exist yet. The running step then records
          # its wait, and suspend_workflow's CASE flips the workflow to 'canceling'
          # (the branch that exists precisely for this race). Finalization via
          # cancel_workflow was a bare status UPDATE that never terminalized the
          # now-orphaned waiting step / pending wait / waiting attempt -> a terminal
          # 'canceled' workflow carrying a LIVE waiting step (caught by the harness's
          # always-on "canceled workflow has live step" invariant). The fix
          # terminalizes live waits/steps/attempts inside cancel_workflow's own
          # transaction. Teeth: drop that cleanup -> the invariants below go red.
          cancel_delivered = false
          workflow = workflow_class("suspender") do
            test_step("wait") do |ctx|
              # Model an external cancellation RPC arriving mid-step, before suspend.
              unless cancel_delivered
                cancel_delivered = true
                h.store.request_workflow_cancellation(workflow_id: ctx.fetch("id"), reason: "stop #{seed}")
              end
              Durababble.wait_until(h.store.current_time + 60, ctx)
            end
          end
          h.workflows["suspender"] = workflow
          id = "suspend-race-#{seed}"
          h.store.enqueue_workflow(name: "suspender", input: { "id" => id }, id:)

          # Worker A claims (cancel not yet requested), runs the step, the cancel
          # lands mid-step, and the step suspends -> workflow goes 'canceling' with a
          # freshly recorded waiting step + pending wait.
          h.scheduler.schedule(actor: "worker-a", delay: 1 + h.scheduler.rng.int(3), name: "run_and_suspend") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a", lease_seconds: 30).resume(workflow, workflow_id: id)
          rescue Durababble::LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "worker-a", "suspend_lease_conflict", id:)
          end

          # A finalizer claims the canceling workflow and drives it to terminal.
          [20, 35].each do |delay|
            h.scheduler.schedule(actor: "finalizer", delay:, name: "finalize_cancel") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
              Durababble::Engine.new(store: h.store, worker_id: "finalizer", lease_seconds: 30).resume(workflow, workflow_id: id)
            rescue Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "finalizer", "finalize_lease_conflict", id:)
            end
          end

          h.check("workflow lands terminal canceled") do
            h.store.workflow(id).fetch("status") == "canceled"
          end
          h.check("no waiting step stranded on the canceled workflow") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
          h.check("no pending wait stranded on the canceled workflow") do
            h.store.all_waits.values.none? { |wait| wait.fetch("workflow_id") == id && wait.fetch("status") == "pending" }
          end
          h.check("no waiting step attempt stranded on the canceled workflow") do
            h.store.step_attempts_for(id).none? { |attempt| attempt.fetch("status") == "waiting" }
          end
        end
      end

      #: (untyped) -> untyped
      def parallel_branch_failure_orphans_step(seed)
        run(seed, "parallel_branch_failure_orphans_step") do |h|
          # A workflow scatters two parallel branches (raw Async, the same shape
          # async_workflow_test exercises): one branch parks on a wait (recording a
          # pending wait + a `waiting` step + a waiting attempt), the sibling raises
          # a terminal failure. The engine surfaces the sibling failure and calls
          # fail_workflow -> the workflow lands `failed`. Unlike request_workflow_
          # termination (which cascades terminate_workflow_dependents) and the now-
          # fixed cancel_workflow (which repeats the wait/step/attempt cleanup),
          # fail_workflow was a bare status UPDATE: it left the parked branch's
          # `waiting` step / pending wait / waiting attempt stranded on a terminal
          # workflow forever. The harness's always-on verify_step_invariants! flags
          # "failed workflow <id> has live step" (a waiting step counts as live);
          # the explicit checks below pin the same clean-terminal contract the
          # other terminal paths already satisfy. Teeth: drop fail_workflow's
          # dependent cleanup -> these go red every seed.
          workflow = workflow_class("parallel-failer") do
            test_step("wait_branch") { |ctx| Durababble.wait_until(h.store.current_time + 3600, ctx) }
            test_step("fail_branch", retry_policy: { maximum_attempts: 1 }) { |ctx| raise "boom #{ctx.fetch("id")}" }
            define_method(:execute) do |input|
              instance = self #: as untyped
              Async do |task|
                errors = []
                [
                  task.async { instance.wait_branch(input) },
                  task.async { instance.fail_branch(input) },
                ].each do |child|
                  child.wait
                rescue StandardError => e
                  errors << e
                end
                fatal = errors.find { |candidate| !candidate.is_a?(Durababble::WorkflowSuspended) } || errors.first
                raise fatal if fatal
              end.wait
            end
          end
          h.workflows["parallel-failer"] = workflow
          id = "parallel-fail-#{seed}"
          h.store.enqueue_workflow(name: "parallel-failer", input: { "id" => id }, id:)

          h.scheduler.schedule(actor: "worker-a", delay: 1 + h.scheduler.rng.int(3), name: "run_branches") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a", lease_seconds: 30).resume(workflow, workflow_id: id)
          rescue Durababble::LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "worker-a", "resume_lease_conflict", id:)
          end

          # A timer fires well after the failure; it must not resurrect the terminal
          # workflow, and the wait it touches must already be terminalized.
          h.scheduler.schedule(actor: "timer", delay: 50, name: "wake_due_timers") do
            h.store.wake_due_timers(now: h.store.current_time + 4000)
          end

          h.check("workflow lands terminal failed") do
            h.store.workflow(id).fetch("status") == "failed"
          end
          h.check("no waiting step stranded on the failed workflow") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
          h.check("no pending wait stranded on the failed workflow") do
            h.store.all_waits.values.none? { |wait| wait.fetch("workflow_id") == id && wait.fetch("status") == "pending" }
          end
          h.check("no waiting step attempt stranded on the failed workflow") do
            h.store.step_attempts_for(id).none? { |attempt| attempt.fetch("status") == "waiting" }
          end
        end
      end

      #: (untyped) -> untyped
      def record_step_canceled_crash_fuzz(seed)
        run(seed, "record_step_canceled_crash_fuzz") do |h|
          # Crash-fuzzes record_step_canceled, the path that cancels an ACTIVELY
          # RUNNING step (a waiting step is canceled by cancel_waiting_steps inside
          # the cancellation request instead, and never reaches here -- which is
          # why cancellation_cleanup_crash_fuzz does not exercise it). It is a
          # THREE-write transaction -- cancel_step (step->canceled) +
          # update_latest_attempt(status=canceled) + append_workflow_history
          # (step_canceled) -- the same atomicity class as the original
          # step-failure bug. A :mid_transaction crash must roll back all three so
          # a retry redoes them cleanly; if append_workflow_history were split out,
          # a crash between the attempt update and the history append would leave
          # the attempt canceled with no history row, and the replay-style "skip if
          # already canceled" guard below would never re-add it -> a step_canceled
          # entry permanently lost. Conversely a history-without-attempt split
          # would let the guard re-run and append a duplicate. The invariant pins
          # exactly one history entry against exactly one canceled attempt.
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id:, worker_id: "owner", lease_seconds: 1000)
          h.store.record_step_scheduled(workflow_id:, command_id: 0, name: "work", args: [])
          h.store.record_step_started(workflow_id:, command_id: 0, name: "work")

          h.store.enable_write_crashes!(percent: 25)

          already_canceled = lambda do
            h.store.step_attempts_for(workflow_id).any? { |attempt| attempt.fetch("status") == "canceled" }
          end

          # Several crashing workers race to cancel the running step. The guard
          # mirrors the engine's replay behaviour: a worker only records the
          # cancellation if no canceled attempt is durably present yet. With the
          # atomic transaction a crash rolls the whole thing back, so the guard
          # still reads "not canceled" and a later worker retries cleanly.
          5.times do |w|
            h.scheduler.schedule(actor: "cancel-worker-#{w}", delay: 3 + w * 7, name: "record_cancel_with_crashes") do
              h.store.crashable do
                next if already_canceled.call

                h.store.record_step_canceled(workflow_id:, command_id: 0, error: "workflow cancellation requested")
              end
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "cancel-worker-#{w}", "record_cancel_crashed", id: workflow_id)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 60, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 70, name: "ensure_canceled") do
            h.store.record_step_canceled(workflow_id:, command_id: 0, error: "workflow cancellation requested") unless already_canceled.call
          end

          h.check("the running step is recorded canceled") do
            step = h.store.steps_for(workflow_id).find { |s| s.fetch("name") == "work" }
            step && step.fetch("status") == "canceled"
          end
          h.check("exactly one step attempt is canceled") do
            h.store.step_attempts_for(workflow_id).one? { |attempt| attempt.fetch("status") == "canceled" }
          end
          h.check("step_canceled history entry recorded exactly once (record_step_canceled atomic)") do
            h.store.workflow_history_for(workflow_id).one? { |entry| entry.fetch("kind") == "step_canceled" }
          end
          h.check("no running step attempt stranded") do
            h.store.step_attempts_for(workflow_id).none? { |attempt| attempt.fetch("status") == "running" }
          end
        end
      end

      #: (untyped) -> untyped
      def workflow_termination_dependents_crash_fuzz(seed)
        run(seed, "workflow_termination_dependents_crash_fuzz") do |h|
          # Crash-fuzzes request_workflow_termination, which no existing scenario
          # exercises under crashes. It is a TERMINAL-GATED multi-write
          # transaction: lock the workflow row, return early if already terminal,
          # otherwise terminate_workflow (status->terminated) +
          # terminate_workflow_dependents (a FIVE-write cascade -- waits, steps,
          # step attempts, inbox, target activations) + append_workflow_history.
          # The early-return gate is `terminal?(status)`, so atomicity is
          # load-bearing in the same way the cancellation request's
          # first_request gate is: if a crash committed the status->terminated
          # write but NOT the dependent cascade, the idempotent re-request would
          # see the workflow already terminal and SKIP the cascade -- stranding a
          # pending wait, waiting step, waiting attempt, and wakeup row forever.
          # A :mid_transaction crash must roll the whole thing back (status AND
          # any cascade writes) so a later request redoes it cleanly. The teeth:
          # move terminate_workflow out of the transaction so it commits before
          # the cascade -> a crash strands the dependents and the gate skips the
          # redo -> the stranding invariants below go red.
          #
          # The workflow parks on a wait timer first (crash-free) so termination
          # always races a genuinely-waiting workflow with live dependents.
          # Crashing clients race the terminate request; reapers reclaim expired
          # leases; a crash-free guaranteed terminator at the tail drives it to a
          # terminal state so progress is assured regardless of which crashes
          # fired.
          h.workflows["terminable"] = workflow = Class.new(Durababble::Workflow) do
            workflow_name "terminable"

            define_method(:execute) do |input|
              instance = self #: as untyped
              instance.wait_for_timer(input)
              { "done" => true }
            end

            define_method(:wait_for_timer) do |input|
              Durababble.wait_until(h.store.current_time + 60, input)
            end
            step :wait_for_timer
          end

          id = h.store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => seed.to_s })

          # Park the workflow on its wait timer first (crash-free) so termination
          # always races a genuinely-waiting workflow with a pending wait,
          # waiting step, and waiting step attempt to strand.
          h.scheduler.schedule(actor: "parker", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "parker").resume(workflow, workflow_id: id)
          end

          h.scheduler.schedule(actor: "enable-crashes", delay: 3, name: "enable_crashes") do
            h.store.enable_write_crashes!(percent: 25)
          end

          # Several clients request termination concurrently under crashes; the
          # request must be idempotent and atomic.
          4.times do |c|
            h.scheduler.schedule(actor: "terminator-#{c}", delay: 5 + c * 6, name: "request_terminate") do
              h.store.crashable do
                workflow.handle(id, store: h.store).terminate(reason: "halt #{seed}")
              end
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "terminator-#{c}", "terminate_request_crashed", id:)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 8 + i * 9, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 80, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          # Crash-free guaranteed terminate: ensures the request lands durably even
          # if every crashing terminator above rolled back. Idempotent -- if an
          # earlier request already committed, the workflow is terminal and this is
          # a no-op; if a non-atomic crash stranded the dependents, this re-request
          # would (wrongly, under a buggy split) skip them, and the invariants
          # below catch it.
          h.scheduler.schedule(actor: "guaranteed-terminator", delay: 85, name: "ensure_terminate") do
            workflow.handle(id, store: h.store).terminate(reason: "halt #{seed}")
          end

          h.check("workflow lands terminal terminated") do
            h.store.workflow(id).fetch("status") == "terminated"
          end
          h.check("no pending wait stranded by termination (request_workflow_termination atomic)") do
            h.store.all_waits.values.none? do |wait|
              wait.fetch("workflow_id") == id && wait.fetch("status") == "pending"
            end
          end
          h.check("no waiting step stranded by termination") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
          h.check("no waiting step attempt stranded by termination") do
            h.store.step_attempts_for(id).none? { |attempt| attempt.fetch("status") == "waiting" }
          end
          h.check("no wakeup row survives the termination drain") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == id }
          end
          h.check("workflow_terminated history entry recorded exactly once") do
            h.store.workflow_history_for(id).one? { |entry| entry.fetch("kind") == "workflow_terminated" }
          end
        end
      end

      #: (untyped) -> untyped
      def stolen_lease_write_rejection(seed)
        run(seed, "stolen_lease_write_rejection") do |h|
          # Pins the store-level split-brain guard: once a workflow lease is
          # stolen (the prior owner's lease expired and a reaper handed it to a
          # new worker), EVERY durable write the stale prior owner attempts must
          # be rejected with LeaseConflict. This is a different bug class from the
          # crash-atomicity scenarios -- it is a concurrency/ownership invariant.
          # The existing workflow_rpc_owner_state_matrix only exercises the
          # RPC-routing staleness checks (StaleLease / NoActiveLease /
          # WorkflowNotRunning); it never drives the store write guards
          # (assert_workflow_lease_for_update! for step writes,
          # require_fenced_workflow_update! for workflow completion/fail/cancel)
          # through a real steal. If either guard regressed to a no-op, the stale
          # worker could complete/fail/cancel a step or the whole workflow that a
          # new owner is actively running -> double side effects / split brain.
          # The teeth: make assert_workflow_lease_for_update! a no-op -> the stale
          # step writes are accepted -> "no stale write was accepted" goes red.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id:, worker_id: "stale-owner", lease_seconds: 10)
          h.store.record_step_scheduled(workflow_id:, command_id: 0, name: "work", args: [])
          h.store.record_step_started(workflow_id:, command_id: 0, name: "work")

          # The lease expires; a reaper reclaims it and a new worker takes over.
          h.scheduler.schedule(actor: "reaper", delay: 20, name: "steal_expired") do
            h.store.steal_expired_leases!(now: h.scheduler.time + 1)
          end
          h.scheduler.schedule(actor: "new-owner", delay: 22, name: "claim") do
            h.store.claim_workflow(workflow_id:, worker_id: "new-owner", lease_seconds: 200)
          end

          # The stale prior owner wakes up after the steal and tries to flush its
          # work. Each durable write carries worker_id: "stale-owner" and must be
          # rejected. Any write that is ACCEPTED is a split-brain bug.
          stale_writes = {
            "record_step_completed" => -> { h.store.record_step_completed(workflow_id:, command_id: 0, result: { "stale" => true }, worker_id: "stale-owner") },
            "record_step_failed" => -> { h.store.record_step_failed(workflow_id:, command_id: 0, error: "stale failure", worker_id: "stale-owner") },
            "record_step_canceled" => -> { h.store.record_step_canceled(workflow_id:, command_id: 0, error: "stale cancel", worker_id: "stale-owner") },
            "complete_workflow" => -> { h.store.complete_workflow(workflow_id, result: { "stale" => true }, worker_id: "stale-owner") },
            "fail_workflow" => -> { h.store.fail_workflow(workflow_id, error: "stale failure", worker_id: "stale-owner") },
            "cancel_workflow" => -> { h.store.cancel_workflow(workflow_id, reason: "stale cancel", worker_id: "stale-owner") },
          }
          stale_writes.each_with_index do |(op, write), i|
            h.scheduler.schedule(actor: "stale-owner", delay: 24 + i, name: "stale_#{op}") do
              write.call
              h.scheduler.trace.event(h.scheduler.time, "stale-owner", "stale_accepted", op:)
            rescue Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "stale-owner", "stale_rejected", op:)
            end
          end

          # The legitimate new owner finishes the step and workflow.
          h.scheduler.schedule(actor: "new-owner", delay: 40, name: "finish") do
            h.store.record_step_completed(workflow_id:, command_id: 0, result: { "ok" => true }, worker_id: "new-owner")
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "new-owner")
          end

          h.check("no stale write was accepted (split-brain guard holds)") do
            !h.scheduler.trace.to_s.include?("stale_accepted")
          end
          h.check("every stale write was rejected with LeaseConflict") do
            stale_writes.keys.all? { |op| h.scheduler.trace.to_s.include?("stale_rejected op=#{op.inspect}") }
          end
          # complete_workflow is fence-guarded (require_fenced_workflow_update!),
          # so a `completed` status proves the new owner -- not the stale owner --
          # drove it to terminal.
          h.check("workflow completed by the new owner (fenced completion)") do
            h.store.workflow(workflow_id).fetch("status") == "completed"
          end
          h.check("step has exactly one completed attempt") do
            h.store.step_attempts_for(workflow_id).one? { |attempt| attempt.fetch("status") == "completed" }
          end
          h.check("no step attempt was failed or canceled by the stale owner") do
            h.store.step_attempts_for(workflow_id).none? { |attempt| ["failed", "canceled"].include?(attempt.fetch("status")) }
          end
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
      def timer_wakeup_batch_crash_fuzz(seed)
        run(seed, "timer_wakeup_batch_crash_fuzz") do |h|
          # Crash-fuzzes complete_timer_waits, the batched timer-wakeup
          # transaction that concurrent_timer_wake_once exercises only WITHOUT
          # crashes. For each due wait in the batch it runs complete_wait +
          # record_step_completed_without_transaction (itself 3 writes), then a
          # single mark_waits_workflows_pending that flips every woken workflow
          # from `waiting` back to `pending`. All of it is ONE transaction, so the
          # trailing pending-flip is load-bearing: if it were split into a
          # separate commit, a crash after the waits and step records committed
          # but before the flip would leave workflows in `waiting` with a
          # `completed` wait -- stranded forever, since a waiting workflow is never
          # claimed by a worker. A :mid_transaction crash must roll the whole
          # batch back so a later wake_due_timers redoes it cleanly. Several
          # workflows nap concurrently so the batch carries multiple waits at once
          # (the flip is a single multi-row UPDATE); crashing wakers race
          # wake_due_timers, then a crash-free guaranteed waker + finishers drive
          # everything terminal. Teeth: no-op mark_waits_workflows_pending -> every
          # workflow strands in `waiting` with a completed wait -> red.
          h.workflows["napper"] = workflow = workflow_class("napper") do
            test_step("nap") { |ctx| Durababble.wait_until(h.store.current_time + 30, ctx.merge("woken" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end

          ids = Array.new(4) do |i|
            h.store.enqueue_workflow(name: "napper", input: { "id" => "#{seed}-#{i}" })
          end

          # Park each workflow on its nap timer (crash-free) so the wake batch
          # always has several genuinely-waiting workflows to flip at once.
          ids.each_with_index do |id, i|
            h.scheduler.schedule(actor: "parker-#{i}", delay: 1 + i, name: "park") do
              Durababble::Engine.new(store: h.store, worker_id: "parker-#{i}").resume(workflow, workflow_id: id)
            end
          end

          h.scheduler.schedule(actor: "enable-crashes", delay: 10, name: "enable_crashes") do
            h.store.enable_write_crashes!(percent: 30)
          end

          # Crashing wakers race the batched wake. A :mid_transaction crash must
          # roll the entire batch back; a later waker redoes it cleanly.
          5.times do |w|
            h.scheduler.schedule(actor: "waker-#{w}", delay: 40 + w * 5, name: "wake_with_crashes") do
              h.store.crashable do
                h.store.wake_due_timers(now: h.scheduler.time)
              end
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "waker-#{w}", "wake_crashed")
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 80, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          # Crash-free guaranteed wake: every still-due wait completes durably even
          # if every crashing waker rolled back.
          h.scheduler.schedule(actor: "guaranteed-waker", delay: 85, name: "ensure_wake") do
            h.store.wake_due_timers(now: h.scheduler.time)
          end
          # Crash-free finishers resume each woken workflow to terminal.
          ids.each_with_index do |id, i|
            h.scheduler.schedule(actor: "finisher-#{i}", delay: 90 + i, name: "finish") do
              Durababble::Engine.new(store: h.store, worker_id: "finisher-#{i}").resume(workflow, workflow_id: id)
            rescue Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "finisher-#{i}", "finish_lease_conflict", id:)
            end
          end

          h.check("no workflow stranded waiting with a completed wait (complete_timer_waits atomic)") do
            ids.none? do |id|
              h.store.workflow(id).fetch("status") == "waiting" &&
                h.store.all_waits.values.any? { |wait| wait.fetch("workflow_id") == id && wait.fetch("status") == "completed" }
            end
          end
          h.check("every nap wait completed exactly once") do
            ids.all? do |id|
              h.store.all_waits.values.one? { |wait| wait.fetch("workflow_id") == id && wait.fetch("status") == "completed" }
            end
          end
          h.check("no workflow left waiting after the wake drain") do
            ids.none? { |id| h.store.workflow(id).fetch("status") == "waiting" }
          end
          h.check("nap step recorded completed exactly once per workflow") do
            ids.all? do |id|
              h.store.steps_for(id).one? { |step| step.fetch("name") == "nap" && step.fetch("status") == "completed" }
            end
          end
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
