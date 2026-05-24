# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleStoreBackendConformanceTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "migrates, enqueues, claims, completes, and decodes serialized workflow state with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        store.migrate!

        workflow_id = store.enqueue_workflow(name: "conformance", input: { "count" => 1 })
        claimed = store.claim_runnable_workflow(worker_id: "worker-a", lease_seconds: 30)

        assert_hash_includes(
          claimed,
          "id" => workflow_id,
          "name" => "conformance",
          "status" => "running",
          "input" => { "count" => 1 },
          "locked_by" => "worker-a",
        )

        store.record_step_scheduled(workflow_id:, command_id: 0, name: "increment", args: [{ "count" => 1 }])
        store.record_step_started(workflow_id:, command_id: 0, name: "increment")
        store.record_step_completed(workflow_id:, command_id: 0, result: { "count" => 2 })
        store.complete_workflow(workflow_id, result: { "count" => 2 })

        assert_hash_includes store.workflow(workflow_id), "status" => "completed", "result" => { "count" => 2 }
        assert_hash_includes store.steps_for(workflow_id).first, "command_id" => 0, "status" => "completed", "result" => { "count" => 2 }
        assert_equal(
          ["step_scheduled", "step_started", "step_completed"],
          store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") },
        )
        assert_equal [{ "name" => "increment", "args" => [{ "count" => 1 }], "kwargs" => {} }, nil, { "count" => 2 }], store.workflow_history_for(workflow_id).map { |event| event["payload"] }
      end
    end

    test "persists, claims, decodes, and acknowledges outbox messages with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(name: "outbox-owner", input: {})

        first_id = store.enqueue_outbox(workflow_id:, topic: "events", payload: { "event" => 1 }, key: "events:1")
        duplicate_id = store.enqueue_outbox(workflow_id:, topic: "events", payload: { "event" => "ignored" }, key: "events:1")

        assert_equal first_id, duplicate_id

        claimed = store.claim_outbox(worker_id: "sender-a", lease_seconds: 30)
        assert_hash_includes(
          claimed,
          "id" => first_id,
          "workflow_id" => workflow_id,
          "topic" => "events",
          "payload" => { "event" => 1 },
          "status" => "processing",
          "locked_by" => "sender-a",
        )

        store.ack_outbox(first_id, worker_id: "sender-b")
        assert_hash_includes store.outbox_message(first_id), "status" => "processing"

        store.ack_outbox(first_id, worker_id: "sender-a")
        assert_hash_includes store.outbox_message(first_id), "status" => "processed", "locked_by" => "sender-a"
      end
    end

    test "persists waits and wakes event waiters once with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        store.migrate!
        workflow_id = store.create_workflow(name: "waiter", input: { "start" => true })
        wait_id = store.record_wait(
          workflow_id:,
          position: 0,
          name: "approval",
          wait_request: Durababble.wait_event("approval:#{workflow_id}", { "before" => true }),
        )

        assert_hash_includes store.workflow(workflow_id), "status" => "waiting"
        assert_hash_includes(
          store.waits_for(workflow_id).first,
          "id" => wait_id,
          "status" => "pending",
          "context" => { "before" => true },
        )

        assert_equal 1, store.signal_event("approval:#{workflow_id}", payload: { "approved" => true })
        assert_equal 0, store.signal_event("approval:#{workflow_id}", payload: { "approved" => false })

        assert_hash_includes store.workflow(workflow_id), "status" => "pending"
        assert_hash_includes(
          store.steps_for(workflow_id).first,
          "status" => "completed",
          "result" => { "before" => true, "approved" => true },
        )
        assert_hash_includes(
          store.waits_for(workflow_id).first,
          "status" => "completed",
          "payload" => { "approved" => true },
        )
      end
    end

    test "persists waits and wakes due timers once with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        store.migrate!
        workflow_id = store.create_workflow(name: "timer", input: {})
        wait_id = store.record_wait(
          workflow_id:,
          position: 0,
          name: "sleep",
          wait_request: Durababble.wait_until(Time.utc(2026, 1, 1, 0, 0, 0), { "timer" => true }),
        )

        assert_equal 0, store.wake_due_timers(now: Time.utc(2025, 12, 31, 23, 59, 59))
        assert_hash_includes store.waits_for(workflow_id).first, "id" => wait_id, "status" => "pending"

        assert_equal 1, store.wake_due_timers(now: Time.utc(2026, 1, 1, 0, 0, 1))
        assert_equal 0, store.wake_due_timers(now: Time.utc(2026, 1, 1, 0, 0, 2))
        assert_hash_includes store.workflow(workflow_id), "status" => "pending"
        assert_hash_includes store.steps_for(workflow_id).first, "status" => "completed", "result" => { "timer" => true }
      end
    end

    test "deduplicates fenced work and replays completed or failed results with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        store.migrate!
        workflow_id = store.create_workflow(name: "fenced", input: {})
        calls = 0

        first = store.with_fence(workflow_id:, key: "charge:1", poll_interval: 0.001, timeout: 1) do
          calls += 1
          { "charged" => true }
        end
        second = store.with_fence(workflow_id:, key: "charge:1", poll_interval: 0.001, timeout: 1) do
          calls += 1
          { "charged" => false }
        end

        assert_equal({ "charged" => true }, first)
        assert_equal({ "charged" => true }, second)
        assert_equal 1, calls

        assert_raises_matching(RuntimeError, /processor down/) do
          store.with_fence(workflow_id:, key: "charge:fails", poll_interval: 0.001, timeout: 1) do
            raise "processor down"
          end
        end

        assert_raises_matching(Durababble::Error, /processor down/) do
          store.with_fence(workflow_id:, key: "charge:fails", poll_interval: 0.001, timeout: 1) do
            raise "should not run"
          end
        end
      end
    end

    test "persists durable object state and command lifecycle payloads with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        store.migrate!

        assert_nil store.object_state(object_type: "counter", object_id: "abc")
        assert_equal({ "count" => 1 }, store.save_object_state(object_type: "counter", object_id: "abc", state: { "count" => 1 }))
        assert_equal({ "count" => 1 }, store.object_state(object_type: "counter", object_id: "abc"))

        command_id = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "increment",
          args: [1],
          kwargs: { "by" => 2 },
        )
        assert_hash_includes(
          store.inbox_message(command_id),
          "id" => command_id,
          "target_kind" => "object",
          "target_type" => "counter",
          "target_id" => "abc",
          "message_kind" => "ask",
          "method_name" => "increment",
          "status" => "pending",
        )
        assert_equal 1, store.inbox_message(command_id).fetch("sequence").to_i
        assert_equal 0, store.inbox_message(command_id).fetch("attempts").to_i
        claimed = store.claim_object_command(command_id:, worker_id: "object-worker", lease_seconds: 30)
        assert_hash_includes(
          claimed,
          "id" => command_id,
          "object_type" => "counter",
          "object_id" => "abc",
          "method_name" => "increment",
          "args" => [1],
          "kwargs" => { "by" => 2 },
          "status" => "running",
          "locked_by" => "object-worker",
        )

        store.complete_object_command(command_id:, result: { "count" => 3 })
        assert_hash_includes store.inbox_message(command_id), "status" => "completed", "result" => { "count" => 3 }
        assert_nil store.claim_object_command(command_id:, worker_id: "object-worker", lease_seconds: 30)

        fenced_command_id = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "increment",
          args: [],
          kwargs: {},
        )
        assert_hash_includes(
          store.claim_object_command(command_id: fenced_command_id, worker_id: "object-owner", lease_seconds: 30),
          "locked_by" => "object-owner",
        )
        intruder = store.complete_object_command(
          command_id: fenced_command_id,
          result: { "count" => 999 },
          worker_id: "intruder",
        )
        assert(intruder.nil? || intruder.cmd_tuples.to_i.zero?)
        owner = store.complete_object_command(
          command_id: fenced_command_id,
          result: { "count" => 4 },
          worker_id: "object-owner",
        )
        assert_equal 1, owner.cmd_tuples
      end
    end

    test "allocates inbox sequences transactionally and drains only a contiguous ready prefix with #{backend.name}" do
      with_durababble_store(backend, "inbox_sequence") do |store|
        store.migrate!
        blocked = store.enqueue_inbox_message(
          target_kind: "object",
          target_type: "counter",
          target_id: "blocked",
          message_kind: "wake",
          payload: { "wake" => 1 },
          ready_at: Time.now + 60,
        )
        ready = store.enqueue_inbox_message(
          target_kind: "object",
          target_type: "counter",
          target_id: "blocked",
          message_kind: "tell",
          payload: { "tell" => 2 },
        )

        assert_equal [1, 2], store.inbox_messages_for(target_kind: "object", target_type: "counter", target_id: "blocked").map { |message| message.fetch("sequence").to_i }
        assert_equal [], store.claim_inbox_messages(target_kind: "object", target_type: "counter", target_id: "blocked", worker_id: "worker-a", lease_seconds: 30, limit: 2)
        assert_hash_includes store.inbox_message(blocked), "status" => "pending"
        assert_hash_includes store.inbox_message(ready), "status" => "pending"

        future = Time.now + 120
        due = store.claim_inbox_messages(target_kind: "object", target_type: "counter", target_id: "blocked", worker_id: "worker-a", lease_seconds: 30, limit: 2, now: future)
        assert_equal [blocked, ready], due.map { |message| message.fetch("id") }
        assert_equal ["running", "running"], due.map { |message| message.fetch("status") }
      end
    end

    test "deduplicates inbox enqueues and rejects idempotency shape conflicts with #{backend.name}" do
      with_durababble_store(backend, "inbox_idempotency") do |store|
        store.migrate!
        first = store.enqueue_inbox_message(
          target_kind: "workflow",
          target_type: "approval",
          target_id: "wf-1",
          message_kind: "workflow_signal",
          payload: { "approved" => true },
          idempotency_key: "signal:approval:wf-1",
        )
        duplicate = store.enqueue_inbox_message(
          target_kind: "workflow",
          target_type: "approval",
          target_id: "wf-1",
          message_kind: "workflow_signal",
          payload: { "approved" => true },
          idempotency_key: "signal:approval:wf-1",
        )

        assert_equal first, duplicate
        assert_equal [1], store.inbox_messages_for(target_kind: "workflow", target_type: "approval", target_id: "wf-1").map { |message| message.fetch("sequence").to_i }

        assert_raises(Durababble::IdempotencyKeyConflict) do
          store.enqueue_inbox_message(
            target_kind: "workflow",
            target_type: "approval",
            target_id: "wf-1",
            message_kind: "workflow_signal",
            payload: { "approved" => false },
            idempotency_key: "signal:approval:wf-1",
          )
        end
      end
    end

    test "keeps committed inbox rows claimable after caller crash with #{backend.name}" do
      with_durababble_store(backend, "inbox_crash_after_commit") do |store|
        store.migrate!
        message_id = store.enqueue_inbox_message(
          target_kind: "workflow",
          target_type: "approval",
          target_id: "wf-2",
          message_kind: "workflow_command",
          method_name: "approve",
          payload: { "reason" => "ok" },
        )

        recovered = Durababble::Store.connect(database_url: backend.database_url, schema:)
        begin
          assert_hash_includes(recovered.inbox_message(message_id), "status" => "pending")
          assert_equal(1, recovered.inbox_message(message_id).fetch("sequence").to_i)
          claimed = recovered.claim_inbox_messages(target_kind: "workflow", target_type: "approval", target_id: "wf-2", worker_id: "workflow-owner", lease_seconds: 30)
          assert_equal([message_id], claimed.map { |message| message.fetch("id") })
        ensure
          recovered.close
        end
      end
    end

    test "allocates unique contiguous mailbox sequences under concurrent enqueue with #{backend.name}" do
      with_durababble_store(backend, "inbox_concurrent") do |store|
        store.migrate!
        errors = Queue.new
        threads = 6.times.map do |index|
          Thread.new do
            local = Durababble::Store.connect(database_url: backend.database_url, schema:)
            begin
              local.enqueue_inbox_message(
                target_kind: "object",
                target_type: "counter",
                target_id: "concurrent",
                message_kind: "tell",
                payload: { "index" => index },
              )
            rescue StandardError => e
              errors << e
            ensure
              local&.close
            end
          end
        end
        threads.each(&:join)
        raise errors.pop unless errors.empty?

        sequences = store.inbox_messages_for(target_kind: "object", target_type: "counter", target_id: "concurrent").map { |message| message.fetch("sequence").to_i }.sort
        assert_equal [1, 2, 3, 4, 5, 6], sequences
      end
    end

    test "supports lease, heartbeat, retry, failure, and release lifecycle operations with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(name: "lifecycle", input: { "start" => true })

        assert_nil store.claim_runnable_workflow(worker_id: "nobody", lease_seconds: 30, workflow_names: [])
        assert_raises_matching(KeyError, /missing-workflow/) { store.workflow("missing-workflow") }
        assert_nil store.step_heartbeat_cursor(workflow_id:, position: 99)

        assert_hash_includes(
          store.claim_workflow(workflow_id:, worker_id: "worker-a", lease_seconds: 30),
          "id" => workflow_id,
          "status" => "running",
          "locked_by" => "worker-a",
        )
        assert_equal true, store.workflow_owned?(workflow_id:, worker_id: "worker-a")
        assert_equal false, store.workflow_owned?(workflow_id:, worker_id: "worker-b")
        assert_hash_includes store.current_workflow_lease(workflow_id), "workflow_id" => workflow_id, "worker_id" => "worker-a"
        assert_equal 1, store.heartbeat(workflow_id:, worker_id: "worker-a", lease_seconds: 30).cmd_tuples

        store.record_step_started(workflow_id:, position: 0, name: "heartbeat")
        assert_nil store.heartbeat_step(
          workflow_id:,
          position: 99,
          worker_id: "worker-a",
          lease_seconds: 30,
          cursor: { "offset" => 99 },
        )
        refute_nil store.heartbeat_step(
          workflow_id:,
          position: 0,
          worker_id: "worker-a",
          lease_seconds: 30,
          cursor: { "offset" => 10 },
        )
        assert_equal({ "offset" => 10 }, store.step_heartbeat_cursor(workflow_id:, position: 0))
        store.record_step_failed(workflow_id:, position: 0, error: "boom")
        assert_hash_includes store.steps_for(workflow_id).first, "status" => "failed", "error" => "boom"

        run_at = Time.now + 60
        store.schedule_workflow_retry(workflow_id:, worker_id: "worker-a", run_at:)
        assert_nil store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 30)
        store.make_workflow_due!(workflow_id, now: Time.now)
        assert_hash_includes store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 30), "id" => workflow_id, "locked_by" => "worker-b"

        assert_hash_includes store.release_worker_leases!(worker_id: "worker-b"), "workflows" => 1
        assert_hash_includes store.claim_workflow(workflow_id:, worker_id: "worker-c", lease_seconds: 30), "locked_by" => "worker-c"
        store.fail_workflow(workflow_id, error: "fatal")
        assert_hash_includes store.workflow(workflow_id), "status" => "failed", "error" => "fatal"
        assert_nil store.claim_runnable_workflow(worker_id: "worker-d", lease_seconds: 30)
        assert_nil store.claim_workflow(workflow_id:, worker_id: "worker-d", lease_seconds: 30)
        assert_equal 0, store.steal_expired_leases!(now: Time.now)
      end
    end

    test "rejects heartbeat attempts after a workflow lease expires with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(name: "expired-heartbeat", input: {})

        assert_hash_includes(
          store.claim_workflow(workflow_id:, worker_id: "zombie", lease_seconds: -1),
          "locked_by" => "zombie",
        )
        assert_equal false, store.workflow_owned?(workflow_id:, worker_id: "zombie")
        assert_equal 0, store.heartbeat(workflow_id:, worker_id: "zombie", lease_seconds: 30).cmd_tuples
        assert_equal false, store.workflow_owned?(workflow_id:, worker_id: "zombie")
      end
    end

    test "reclaims expired durable object command leases with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        store.migrate!
        command_id = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "increment",
          args: [],
          kwargs: {},
        )

        assert_hash_includes(
          store.claim_object_command(command_id:, worker_id: "crashed-object-worker", lease_seconds: -1),
          "locked_by" => "crashed-object-worker",
        )
        assert_hash_includes(
          store.claim_object_command(command_id:, worker_id: "recovery-object-worker", lease_seconds: 30),
          "id" => command_id,
          "status" => "running",
          "locked_by" => "recovery-object-worker",
        )
      end
    end

    test "does not leave MySQL transactions open after no-op claim paths with #{backend.name}" do
      skip("only the MySQL backend exposes transaction metadata") unless backend.mysql?

      with_durababble_store(backend, "conformance") do |store|
        store.migrate!
        assert_nil store.claim_runnable_workflow(worker_id: "idle-worker", lease_seconds: 30)
        assert_equal 0, mysql_transaction_depth

        workflow_id = store.enqueue_workflow(name: "active", input: {})
        assert_hash_includes store.claim_workflow(workflow_id:, worker_id: "owner", lease_seconds: 30), "locked_by" => "owner"
        assert_nil store.claim_workflow(workflow_id:, worker_id: "intruder", lease_seconds: 30)
        assert_equal 0, mysql_transaction_depth

        outbox_id = store.enqueue_outbox(workflow_id:, topic: "events", payload: {}, key: "events:active")
        assert_hash_includes store.claim_outbox(worker_id: "sender", lease_seconds: 30), "id" => outbox_id
        assert_nil store.claim_outbox(worker_id: "other", lease_seconds: 30)
        assert_equal 0, mysql_transaction_depth

        command_id = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "increment",
          args: [],
          kwargs: {},
        )
        assert_hash_includes store.claim_object_command(command_id:, worker_id: "object-worker", lease_seconds: 30), "id" => command_id
        assert_nil store.claim_object_command(command_id:, worker_id: "other-object-worker", lease_seconds: 30)
        assert_equal 0, mysql_transaction_depth
      end
    end
  end

  def mysql_transaction_depth
    row = begin
      store.send(:execute_params, "SELECT @@in_transaction AS in_tx", []).first
    rescue StandardError => e
      raise unless e.message.include?("Unknown system variable 'in_transaction'")

      store.send(
        :execute_params,
        "SELECT COUNT(*) AS in_tx FROM information_schema.innodb_trx WHERE trx_mysql_thread_id = CONNECTION_ID()",
        [],
      ).first
    end
    row.fetch("in_tx").to_i
  end
end
