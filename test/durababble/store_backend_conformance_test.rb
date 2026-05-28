# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleStoreBackendConformanceTest < DurababbleTestCase
  class AdvisoryDeliveryClient
    attr_reader :deliveries

    def initialize
      @deliveries = []
    end

    def deliver_message(**kwargs)
      @deliveries << kwargs
      true
    end
  end

  durababble_conformance_store_backends.each do |backend|
    test "migrates, enqueues, claims, completes, and decodes serialized workflow state with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
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

    test "step completion and failure APIs return inserted workflow history indexes with #{backend.name}" do
      with_durababble_store(backend, "history_indexes") do |store|
        completed_workflow_id = store.create_workflow(name: "history-completed", input: {})
        store.record_step_started(workflow_id: completed_workflow_id, command_id: 0, name: "step")

        completed_index = store.record_step_completed(workflow_id: completed_workflow_id, command_id: 0, result: { "ok" => true })

        assert_equal 1, completed_index
        assert_equal [0, 1], store.workflow_history_for(completed_workflow_id).map { |event| event.fetch("event_index") }

        failed_workflow_id = store.create_workflow(name: "history-failed", input: {})
        store.record_step_started(workflow_id: failed_workflow_id, command_id: 0, name: "step")

        failed_index = store.record_step_failed(workflow_id: failed_workflow_id, command_id: 0, error: "boom")

        assert_equal 1, failed_index
        assert_equal [0, 1], store.workflow_history_for(failed_workflow_id).map { |event| event.fetch("event_index") }
      end
    end

    test "enqueues explicit workflow ids and rejects duplicate ids without side effects with #{backend.name}" do
      with_durababble_store(backend, "explicit_workflow_id") do |store|
        workflow_id = "wf-explicit-#{SecureRandom.hex(4)}"

        assert_equal workflow_id, store.enqueue_workflow(name: "explicit-id", input: { "count" => 1 }, id: workflow_id)
        assert_hash_includes(
          store.workflow(workflow_id),
          "id" => workflow_id,
          "name" => "explicit-id",
          "status" => "pending",
          "input" => { "count" => 1 },
        )

        error = assert_raises(Durababble::WorkflowAlreadyExists) do
          store.enqueue_workflow(name: "explicit-id", input: { "count" => 2 }, id: workflow_id)
        end
        assert_match(/workflow #{Regexp.escape(workflow_id)} already exists/, error.message)

        assert_hash_includes store.workflow(workflow_id), "input" => { "count" => 1 }, "status" => "pending"
        assert_equal [], store.workflow_history_for(workflow_id)
        assert_equal [], store.steps_for(workflow_id)
        assert_equal [], store.wait_snapshots_for(workflow_id)
        assert_equal [], store.inbox_messages_for(target_kind: "workflow", target_type: "explicit-id", target_id: workflow_id)
        assert_nil store.target_activation(target_kind: "workflow", target_type: "explicit-id", target_id: workflow_id)
      end
    end

    test "completed workflows still reject duplicate explicit workflow ids with #{backend.name}" do
      with_durababble_store(backend, "explicit_workflow_id_completed") do |store|
        workflow_id = "wf-completed-#{SecureRandom.hex(4)}"

        assert_equal workflow_id, store.enqueue_workflow(name: "explicit-completed", input: { "count" => 1 }, id: workflow_id)
        store.complete_workflow(workflow_id, result: { "count" => 2 })
        assert_hash_includes store.workflow(workflow_id), "status" => "completed", "result" => { "count" => 2 }

        error = assert_raises(Durababble::WorkflowAlreadyExists) do
          store.enqueue_workflow(name: "explicit-completed", input: { "count" => 3 }, id: workflow_id)
        end
        assert_match(/workflow #{Regexp.escape(workflow_id)} already exists/, error.message)

        assert_hash_includes store.workflow(workflow_id), "input" => { "count" => 1 }, "status" => "completed", "result" => { "count" => 2 }
        assert_equal [], store.wait_snapshots_for(workflow_id)
        assert_equal [], store.inbox_messages_for(target_kind: "workflow", target_type: "explicit-completed", target_id: workflow_id)
        assert_nil store.target_activation(target_kind: "workflow", target_type: "explicit-completed", target_id: workflow_id)
      end
    end

    test "concurrent duplicate explicit workflow id enqueues create one workflow row with #{backend.name}" do
      skip("in-memory SQLite is a single serialized connection; this test spins up concurrent independent Store.connect handles") if backend.sqlite?

      with_durababble_store(backend, "explicit_workflow_id_race") do |store|
        workflow_id = "wf-race-#{SecureRandom.hex(4)}"
        queue = Queue.new
        thread_count = 8
        mutex = Mutex.new
        condition = ConditionVariable.new
        ready = 0
        release = false

        threads = thread_count.times.map do |index|
          Thread.new do
            thread_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
            begin
              mutex.synchronize do
                ready += 1
                condition.broadcast
                condition.wait(mutex) until release
              end
              queue << [:ok, thread_store.enqueue_workflow(name: "explicit-race", input: { "winner" => index }, id: workflow_id)]
            rescue Durababble::WorkflowAlreadyExists => e
              queue << [:duplicate, e.message]
            rescue StandardError => e
              queue << [:error, e]
            ensure
              thread_store.close
            end
          end
        end

        mutex.synchronize do
          condition.wait(mutex) until ready == thread_count
          release = true
          condition.broadcast
        end

        results = thread_count.times.map { queue.pop }
        threads.each(&:join)

        errors = results.select { |status, _value| status == :error }.map(&:last)
        raise errors.first unless errors.empty?

        assert_equal 1, results.count { |status, _value| status == :ok }
        assert_equal thread_count - 1, results.count { |status, _value| status == :duplicate }
        assert_equal [workflow_id], results.select { |status, _value| status == :ok }.map(&:last)
        assert_hash_includes store.workflow(workflow_id), "id" => workflow_id, "name" => "explicit-race", "status" => "pending"
        assert_equal [], store.workflow_history_for(workflow_id)
        assert_equal [], store.steps_for(workflow_id)
        assert_equal [], store.wait_snapshots_for(workflow_id)
        assert_equal [], store.inbox_messages_for(target_kind: "workflow", target_type: "explicit-race", target_id: workflow_id)
        assert_nil store.target_activation(target_kind: "workflow", target_type: "explicit-race", target_id: workflow_id)
      end
    end

    test "sets and preserves step started_at when a scheduled step starts with #{backend.name}" do
      with_durababble_store(backend, "step_start_metadata") do |store|
        workflow_id = store.create_workflow(name: "step-start-metadata", input: {})

        store.record_step_scheduled(workflow_id:, command_id: 0, name: "existing_step")
        scheduled = store.steps_for(workflow_id).first
        assert_hash_includes scheduled, "status" => "scheduled", "started_at" => nil

        store.record_step_started(workflow_id:, command_id: 0, name: "existing_step")
        running = store.steps_for(workflow_id).first
        assert_hash_includes running, "status" => "running", "error" => nil
        refute_nil running.fetch("started_at")
        first_started_at = running.fetch("started_at")

        sleep 0.01
        store.record_step_started(workflow_id:, command_id: 0, name: "existing_step")
        restarted = store.steps_for(workflow_id).first
        assert_hash_includes restarted, "status" => "running", "error" => nil
        assert_equal first_started_at, restarted.fetch("started_at")
      end
    end

    test "atomically records failed retry attempts with retry backoff with #{backend.name}" do
      with_durababble_store(backend, "step_retry_atomicity") do |store|
        workflow_id = store.enqueue_workflow(name: "atomic-retry", input: {})
        store.claim_workflow(workflow_id:, worker_id: "worker-a", lease_seconds: 30)
        store.record_step_scheduled(workflow_id:, command_id: 0, name: "retryable", worker_id: "worker-a")
        store.record_step_started(workflow_id:, command_id: 0, name: "retryable", worker_id: "worker-a")

        assert_raises(Durababble::LeaseConflict) do
          store.record_step_failed_and_schedule_retry(
            workflow_id:,
            command_id: 0,
            error: "RuntimeError: wrong owner",
            worker_id: "worker-b",
            run_at: Time.now + 60,
          )
        end
        assert_hash_includes store.steps_for(workflow_id).first, "status" => "running"
        assert_equal ["step_scheduled", "step_started"], store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }

        run_at = Time.now + 60
        store.record_step_failed_and_schedule_retry(
          workflow_id:,
          command_id: 0,
          error: "RuntimeError: retry me",
          worker_id: "worker-a",
          run_at:,
        )

        assert_hash_includes store.steps_for(workflow_id).first, "status" => "failed", "error" => "RuntimeError: retry me"
        retry_row = store.workflow(workflow_id)
        assert_hash_includes retry_row, "status" => "pending", "locked_by" => nil
        refute_nil retry_row.fetch("next_run_at")
        retry_history = store.workflow_history_for(workflow_id)
        assert_equal ["step_scheduled", "step_started", "step_failed"], retry_history.map { |event| event.fetch("kind") }
        assert_equal({ "retrying" => true }, retry_history.last.fetch("payload"))
        assert_nil store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 30)
        store.make_workflow_due!(workflow_id, now: Time.now)
        assert_hash_includes store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 30), "id" => workflow_id, "locked_by" => "worker-b"
      end
    end

    test "persists, claims, decodes, and acknowledges outbox messages with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
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

    test "persists waits and makes due timer workflows claimable with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        workflow_id = store.create_workflow(name: "timer", input: {})
        due_at = Time.now + 3600
        wait_id = store.record_wait(
          workflow_id:,
          position: 0,
          name: "sleep",
          wait_request: Durababble.wait_until(due_at, { "timer" => true }),
        )

        assert_equal 0, store.wake_due_timers(now: due_at - 1)
        assert_hash_includes store.wait_snapshots_for(workflow_id).first, "id" => wait_id, "status" => "pending"

        make_workflow_timer_due(store, workflow_id, at: due_at)
        claimed = store.claim_runnable_workflow(worker_id: "timer-worker", lease_seconds: 60, workflow_names: ["timer"])
        assert_equal workflow_id, claimed.fetch("id")
        assert_hash_includes store.workflow(workflow_id), "status" => "running", "locked_by" => "timer-worker"
        assert_hash_includes store.steps_for(workflow_id).first, "status" => "waiting", "result" => { "timer" => true }
      end
    end

    test "claims due timer workflows across bounded worker polls with #{backend.name}" do
      with_durababble_store(backend, "timer_batches") do |store|
        first_workflow = store.create_workflow(name: "timer-batch", input: {})
        second_workflow = store.create_workflow(name: "timer-batch", input: {})
        due_at = Time.now + 3600

        [first_workflow, second_workflow].each_with_index do |workflow_id, index|
          store.record_wait(
            workflow_id:,
            position: 0,
            name: "sleep",
            wait_request: Durababble.wait_until(due_at, { "timer" => index }),
          )
          make_workflow_timer_due(store, workflow_id, at: due_at)
        end

        first_claim = store.claim_runnable_workflow(worker_id: "timer-worker-a", lease_seconds: 60, workflow_names: ["timer-batch"])
        second_claim = store.claim_runnable_workflow(worker_id: "timer-worker-b", lease_seconds: 60, workflow_names: ["timer-batch"])
        assert_equal [first_workflow, second_workflow].sort, [first_claim.fetch("id"), second_claim.fetch("id")].sort
        assert_nil store.claim_runnable_workflow(worker_id: "timer-worker-c", lease_seconds: 60, workflow_names: ["timer-batch"])
        assert_equal ["pending"], store.wait_snapshots_for(first_workflow).map { |wait| wait.fetch("status") }
        assert_equal ["pending"], store.wait_snapshots_for(second_workflow).map { |wait| wait.fetch("status") }
      end
    end

    test "counts step attempts for one command with #{backend.name}" do
      with_durababble_store(backend, "attempt_count") do |store|
        workflow_id = store.create_workflow(name: "attempt-count", input: {})

        3.times { store.record_step_started(workflow_id:, command_id: 0, name: "flaky") }
        2.times { store.record_step_started(workflow_id:, command_id: 1, name: "other") }

        assert_equal 3, store.step_attempt_count_for(workflow_id:, command_id: 0)
        assert_equal 2, store.step_attempt_count_for(workflow_id:, position: 1)
      end
    end

    test "deduplicates fenced work and replays completed or failed results with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
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

    test "reclaims expired running fences with #{backend.name}" do
      with_durababble_store(backend, "fence_reclaim") do |store|
        workflow_id = store.create_workflow(name: "stale-fence", input: {})
        fence_key = "charge:stale"
        calls = 0

        store.send(:execute_store_query, :insert_fence, [workflow_id, fence_key, "abandoned-worker", -1])

        result = store.with_fence(workflow_id:, key: fence_key, poll_interval: 0.001, timeout: 1) do
          calls += 1
          { "charged_by" => "reclaimer" }
        end

        assert_equal({ "charged_by" => "reclaimer" }, result)
        assert_equal 1, calls
        replayed = store.with_fence(workflow_id:, key: fence_key, poll_interval: 0.001, timeout: 1) do
          raise "completed fence should replay without running the block"
        end
        assert_equal result, replayed
      end
    end

    test "persists durable object state and command lifecycle payloads with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
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
        store.release_object_lease(object_type: "counter", object_id: "abc", worker_id: "object-worker")

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
        assert(intruder.nil? || intruder.affected_rows.to_i.zero?)
        owner = store.complete_object_command(
          command_id: fenced_command_id,
          result: { "count" => 4 },
          worker_id: "object-owner",
        )
        assert_equal 1, owner.affected_rows
      end
    end

    test "does not complete object commands for a different object with #{backend.name}" do
      with_durababble_store(backend, "object_command_target_identity") do |store|
        command_id = store.enqueue_object_command(
          object_type: "counter",
          object_id: "object-a",
          method_name: "write",
          args: [],
          kwargs: {},
        )
        assert_hash_includes(
          store.claim_object_command(command_id:, worker_id: "object-worker", lease_seconds: 30),
          "locked_by" => "object-worker",
        )

        assert_nil store.complete_object_command(
          command_id:,
          result: { "ok" => true },
          object_type: "counter",
          object_id: "object-b",
          state: { "value" => "wrong-object" },
          worker_id: "object-worker",
        )
        assert_hash_includes store.inbox_message(command_id), "status" => "running", "locked_by" => "object-worker"
        assert_nil store.object_state(object_type: "counter", object_id: "object-b")
      end
    end

    test "does not claim an earlier object command when asked for a later one with #{backend.name}" do
      with_durababble_store(backend, "object_command_fifo_claim") do |store|
        first = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "increment",
          args: [1],
          kwargs: {},
        )
        second = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "increment",
          args: [2],
          kwargs: {},
        )

        assert_nil store.claim_object_command(command_id: second, worker_id: "object-worker", lease_seconds: 30)
        assert_hash_includes store.inbox_message(first), "status" => "pending", "locked_by" => nil
        assert_hash_includes store.inbox_message(second), "status" => "pending", "locked_by" => nil

        assert_hash_includes(
          store.claim_object_command(command_id: first, worker_id: "object-worker", lease_seconds: 30),
          "id" => first,
          "status" => "running",
          "locked_by" => "object-worker",
        )
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
        assert_nil store.current_object_lease("counter", "blocked")

        future = Time.now + 120
        due = store.claim_inbox_messages(target_kind: "object", target_type: "counter", target_id: "blocked", worker_id: "worker-a", lease_seconds: 30, limit: 2, now: future)
        assert_equal [blocked, ready], due.map { |message| message.fetch("id") }
        assert_equal ["running", "running"], due.map { |message| message.fetch("status") }
      end
    end

    test "coalesces inbox messages into one target activation with #{backend.name}" do
      with_durababble_store(backend, "target_activation") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(name: "approval", input: {})
        first = store.enqueue_inbox_message(
          target_kind: "workflow",
          target_type: "approval",
          target_id: workflow_id,
          message_kind: "workflow_command",
          method_name: "approve",
          payload: { "method" => "approve", "args" => [], "kwargs" => { reason: "first" } },
        )
        second = store.enqueue_inbox_message(
          target_kind: "workflow",
          target_type: "approval",
          target_id: workflow_id,
          message_kind: "workflow_command",
          method_name: "approve",
          payload: { "method" => "approve", "args" => [], "kwargs" => { reason: "second" } },
        )

        activation = store.claim_target_activation(worker_id: "activation-worker", lease_seconds: 30, target_kinds: ["workflow"], target_types: ["approval"])
        assert_hash_includes(
          activation,
          "target_kind" => "workflow",
          "target_type" => "approval",
          "target_id" => workflow_id,
          "status" => "running",
          "locked_by" => "activation-worker",
        )
        assert_nil store.claim_target_activation(worker_id: "other", lease_seconds: 30, target_kinds: ["workflow"], target_types: ["approval"])

        assert_hash_includes(
          store.claim_workflow_for_activation(workflow_id:, worker_id: "activation-worker", lease_seconds: 30),
          "id" => workflow_id,
          "locked_by" => "activation-worker",
        )
        claimed = store.claim_inbox_messages(target_kind: "workflow", target_type: "approval", target_id: workflow_id, worker_id: "activation-worker", lease_seconds: 30, limit: 1)
        assert_equal [first], claimed.map { |message| message.fetch("id") }
        store.complete_workflow_command(message_id: first, workflow_id:, result: { "ok" => 1 }, worker_id: "activation-worker")
        assert store.suspend_workflow(workflow_id:, worker_id: "activation-worker")
        store.complete_target_activation(
          target_kind: "workflow",
          target_type: "approval",
          target_id: workflow_id,
          worker_id: "activation-worker",
        )

        rearmed = store.claim_target_activation(worker_id: "activation-worker-2", lease_seconds: 30, target_kinds: ["workflow"], target_types: ["approval"])
        assert_hash_includes rearmed, "target_id" => workflow_id, "locked_by" => "activation-worker-2"
        assert_hash_includes(
          store.claim_workflow_for_activation(workflow_id:, worker_id: "activation-worker-2", lease_seconds: 30),
          "id" => workflow_id,
          "locked_by" => "activation-worker-2",
        )
        remaining = store.claim_inbox_messages(target_kind: "workflow", target_type: "approval", target_id: workflow_id, worker_id: "activation-worker-2", lease_seconds: 30, limit: 1)
        assert_equal [second], remaining.map { |message| message.fetch("id") }
      end
    end

    test "workflow inbox command writes require a live workflow lease with #{backend.name}" do
      with_durababble_store(backend, "workflow_inbox_lease_fence") do |store|
        store.migrate!

        [
          [
            "completion",
            lambda do |workflow_id, message_id|
              store.complete_workflow_command(
                message_id:,
                workflow_id:,
                result: { "ok" => true },
                worker_id: "workflow-owner",
              )
            end,
          ],
          [
            "failure",
            lambda do |workflow_id, message_id|
              store.fail_workflow_command(
                message_id:,
                workflow_id:,
                error: "boom",
                worker_id: "workflow-owner",
              )
            end,
          ],
        ].each do |operation, write_command|
          workflow_id = store.enqueue_workflow(name: "approval", input: { "operation" => operation })
          message_id = store.enqueue_workflow_command(
            workflow_id:,
            workflow_name: "approval",
            method_name: "approve",
            payload: { "method" => "approve", "args" => [], "kwargs" => { reason: operation } },
          )

          assert_hash_includes(
            store.claim_target_activation(
              worker_id: "workflow-owner",
              lease_seconds: 300,
              target_kinds: ["workflow"],
              target_types: ["approval"],
            ),
            "target_id" => workflow_id,
            "locked_by" => "workflow-owner",
          )
          assert_hash_includes(
            store.claim_workflow_for_activation(workflow_id:, worker_id: "workflow-owner", lease_seconds: 30),
            "id" => workflow_id,
            "locked_by" => "workflow-owner",
          )
          claimed_messages = store.claim_inbox_messages(
            target_kind: "workflow",
            target_type: "approval",
            target_id: workflow_id,
            worker_id: "workflow-owner",
            lease_seconds: 300,
          )
          assert_equal(
            [message_id],
            claimed_messages.map { |message| message.fetch("id") },
          )

          assert_equal 1, store.steal_expired_leases!(now: Time.now + 31)
          assert_raises(Durababble::LeaseConflict) { write_command.call(workflow_id, message_id) }
          assert_equal [], store.workflow_history_for(workflow_id)
          assert_hash_includes store.inbox_message(message_id), "status" => "running", "locked_by" => "workflow-owner"
        end
      end
    end

    test "workflow-origin child starts require a live parent workflow lease with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_parent_lease_fence") do |store|
        workflow_id = store.enqueue_workflow(name: "parent", input: {})
        assert_hash_includes(
          store.claim_workflow(workflow_id:, worker_id: "stale-parent", lease_seconds: 1),
          "locked_by" => "stale-parent",
        )
        sleep(1.1)

        assert_raises_matching(Durababble::LeaseConflict, /lease expired or moved/) do
          store.start_child_workflow(
            origin_kind: "workflow",
            parent_workflow_id: workflow_id,
            parent_command_id: 0,
            parent_worker_id: "stale-parent",
            child_workflow_name: "child",
            child_workflow_id: "stale-parent-child",
            input: { "value" => "blocked" },
            worker_pool: "default",
            cancellation_policy: "request_cancel",
          )
        end
        assert_raises(KeyError) { store.workflow("stale-parent-child") }

        assert_hash_includes(
          store.claim_workflow(workflow_id:, worker_id: "current-parent", lease_seconds: 30),
          "locked_by" => "current-parent",
        )
        child = store.start_child_workflow(
          origin_kind: "workflow",
          parent_workflow_id: workflow_id,
          parent_command_id: 0,
          parent_worker_id: "current-parent",
          child_workflow_name: "child",
          child_workflow_id: "current-parent-child",
          input: { "value" => "allowed" },
          worker_pool: "default",
          cancellation_policy: "request_cancel",
        )
        assert_hash_includes child, "child_workflow_id" => "current-parent-child", "parent_workflow_id" => workflow_id
      end
    end

    test "does not complete workflow commands for a different workflow with #{backend.name}" do
      with_durababble_store(backend, "workflow_command_target_identity") do |store|
        store.enqueue_workflow(name: "approval", input: {}, id: "wf-a")
        store.enqueue_workflow(name: "approval", input: {}, id: "wf-b")
        first = store.enqueue_workflow_command(
          workflow_id: "wf-a",
          workflow_name: "approval",
          method_name: "approve",
          payload: { "method_name" => "approve", "args" => [], "kwargs" => {} },
          idempotency_key: "cmd-a-1",
        )
        second = store.enqueue_workflow_command(
          workflow_id: "wf-a",
          workflow_name: "approval",
          method_name: "reject",
          payload: { "method_name" => "reject", "args" => [], "kwargs" => {} },
          idempotency_key: "cmd-a-2",
        )

        store.claim_target_activation(worker_id: "command-worker", lease_seconds: 30, target_kinds: ["workflow"], target_types: ["approval"])
        claimed = store.claim_inbox_messages(target_kind: "workflow", target_type: "approval", target_id: "wf-a", worker_id: "command-worker", lease_seconds: 30, limit: 2)
        assert_equal [first, second], claimed.map { |message| message.fetch("id") }

        assert_nil store.complete_workflow_command(message_id: first, workflow_id: "wf-b", result: { "ok" => true }, worker_id: "command-worker")
        assert_hash_includes store.inbox_message(first), "status" => "running", "locked_by" => "command-worker"
        assert_nil store.fail_workflow_command(message_id: second, workflow_id: "wf-b", error: "wrong target", worker_id: "command-worker")
        assert_hash_includes store.inbox_message(second), "status" => "running", "locked_by" => "command-worker"
        assert_empty store.workflow_history_for("wf-a")
        assert_empty store.workflow_history_for("wf-b")
      end
    end

    test "does not enqueue workflow commands for a different workflow name with #{backend.name}" do
      with_durababble_store(backend, "workflow_command_target_name") do |store|
        workflow_id = store.enqueue_workflow(name: "approval", input: {})

        error = assert_raises(Durababble::Error) do
          store.enqueue_workflow_command(
            workflow_id:,
            workflow_name: "other-approval",
            method_name: "approve",
            payload: { "method" => "approve", "args" => [], "kwargs" => {} },
            idempotency_key: "wrong-name",
          )
        end
        assert_match(/workflow #{Regexp.escape(workflow_id)} is approval, not other-approval/, error.message)
        assert_equal [], store.inbox_messages_for(target_kind: "workflow", target_type: "other-approval", target_id: workflow_id)
        assert_nil store.target_activation(target_kind: "workflow", target_type: "other-approval", target_id: workflow_id)
        assert_equal [], store.inbox_messages_for(target_kind: "workflow", target_type: "approval", target_id: workflow_id)
      end
    end

    test "scopes workflow claims and command activations by persisted worker pool with #{backend.name}" do
      with_durababble_store(backend, "workflow_pool_routing") do |store|
        pool_a_workflow = store.enqueue_workflow(name: "shared-workflow", input: { "pool" => "a" }, worker_pool: "pool-a")
        pool_b_workflow = store.enqueue_workflow(name: "shared-workflow", input: { "pool" => "b" }, worker_pool: "pool-b")

        assert_nil store.claim_runnable_workflow(
          worker_id: "default-worker",
          lease_seconds: 30,
          workflow_names: ["shared-workflow"],
          worker_pool: "default",
        )

        claimed_a = store.claim_runnable_workflow(
          worker_id: "pool-a-worker",
          lease_seconds: 30,
          workflow_names: ["shared-workflow"],
          worker_pool: "pool-a",
        )
        assert_hash_includes claimed_a, "id" => pool_a_workflow, "worker_pool" => "pool-a", "locked_by" => "pool-a-worker"

        claimed_b = store.claim_runnable_workflow(
          worker_id: "pool-b-worker",
          lease_seconds: 30,
          workflow_names: ["shared-workflow"],
          worker_pool: "pool-b",
        )
        assert_hash_includes claimed_b, "id" => pool_b_workflow, "worker_pool" => "pool-b", "locked_by" => "pool-b-worker"

        command_id = store.enqueue_workflow_command(
          workflow_id: pool_a_workflow,
          workflow_name: "shared-workflow",
          method_name: "approve",
          payload: { "method" => "approve", "args" => [], "kwargs" => {} },
        )
        assert_hash_includes store.inbox_message(command_id), "worker_pool" => "pool-a"
        assert_nil store.claim_target_activation(worker_id: "wrong-pool", lease_seconds: 30, target_kinds: ["workflow"], target_types: ["shared-workflow"], worker_pool: "pool-b")
        activation = store.claim_target_activation(worker_id: "right-pool", lease_seconds: 30, target_kinds: ["workflow"], target_types: ["shared-workflow"], worker_pool: "pool-a")
        assert_hash_includes activation, "worker_pool" => "pool-a", "target_id" => pool_a_workflow
      end
    end

    test "treats object state, mailbox sequence, and inbox idempotency as globally unique regardless of worker pool with #{backend.name}" do
      with_durababble_store(backend, "object_global_identity") do |store|
        store.save_object_state(worker_pool: "pool-a", object_type: "counter", object_id: "same", state: { "pool" => "a" })
        store.save_object_state(worker_pool: "pool-b", object_type: "counter", object_id: "same", state: { "pool" => "b" })

        # Object identity is global: the same (object_type, object_id) is one row regardless of pool,
        # so the second write updates the same record (last write wins) rather than creating a sibling.
        assert_equal({ "pool" => "b" }, store.object_state(object_type: "counter", object_id: "same"))

        payload = { "method_name" => "write", "args" => ["x"], "kwargs" => {} }
        pool_a_message = store.enqueue_inbox_message(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          message_kind: "tell",
          method_name: "write",
          payload:,
          idempotency_key: "natural-key",
        )
        pool_b_message = store.enqueue_inbox_message(
          worker_pool: "pool-b",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          message_kind: "tell",
          method_name: "write",
          payload:,
          idempotency_key: "natural-key",
        )

        # Idempotency is global too: the same natural key on the same target dedupes across pools,
        # leaving a single mailbox sequence entry.
        assert_equal pool_a_message, pool_b_message
        assert_equal [1], store.inbox_messages_for(target_kind: "object", target_type: "counter", target_id: "same").map { |message| message.fetch("sequence").to_i }

        # worker_pool survives as routing metadata: set by the first writer and used to filter claims.
        assert_nil store.claim_target_activation(worker_id: "wrong-pool", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"], worker_pool: "pool-b")
        activation = store.claim_target_activation(worker_id: "pool-a-worker", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"], worker_pool: "pool-a")
        assert_hash_includes activation, "worker_pool" => "pool-a", "target_id" => "same"
      end
    end

    test "does not claim inbox messages across worker pools with #{backend.name}" do
      with_durababble_store(backend, "inbox_worker_pool_isolation") do |store|
        payload = { "method_name" => "write", "args" => [], "kwargs" => {} }
        message_id = store.enqueue_inbox_message(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          message_kind: "tell",
          method_name: "write",
          payload:,
          idempotency_key: "pool-a-message",
        )

        wrong_pool = store.claim_inbox_messages(
          worker_pool: "pool-b",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          worker_id: "pool-b-worker",
          lease_seconds: 30,
          limit: 1,
        )
        assert_empty wrong_pool
        assert_hash_includes store.inbox_message(message_id), "status" => "pending", "locked_by" => nil

        right_pool = store.claim_inbox_messages(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          worker_id: "pool-a-worker",
          lease_seconds: 30,
          limit: 1,
        )
        assert_equal [message_id], right_pool.map { |message| message.fetch("id") }
      end
    end

    test "filters inbox message inspection by worker pool with #{backend.name}" do
      with_durababble_store(backend, "inbox_messages_for_worker_pool_isolation") do |store|
        message_id = store.enqueue_inbox_message(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          message_kind: "tell",
          method_name: "write",
          payload: { "method_name" => "write", "args" => [], "kwargs" => {} },
          idempotency_key: "pool-a-message",
        )

        wrong_pool = store.inbox_messages_for(
          worker_pool: "pool-b",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
        )
        right_pool = store.inbox_messages_for(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
        )
        all_pools = store.inbox_messages_for(
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
        )

        assert_empty wrong_pool
        assert_equal [message_id], right_pool.map { |message| message.fetch("id") }
        assert_equal [message_id], all_pools.map { |message| message.fetch("id") }
      end
    end

    test "keeps object inbox messages on the first materialized worker pool with #{backend.name}" do
      with_durababble_store(backend, "object_inbox_first_pool") do |store|
        first_id = store.enqueue_inbox_message(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          message_kind: "tell",
          method_name: "write",
          payload: { "method_name" => "write", "args" => ["a"], "kwargs" => {} },
          idempotency_key: "pool-a-message",
        )
        second_id = store.enqueue_inbox_message(
          worker_pool: "pool-b",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          message_kind: "tell",
          method_name: "write",
          payload: { "method_name" => "write", "args" => ["b"], "kwargs" => {} },
          idempotency_key: "pool-b-message",
        )

        messages = store.inbox_messages_for(target_kind: "object", target_type: "counter", target_id: "same")
        assert_equal ["pool-a", "pool-a"], messages.map { |message| message.fetch("worker_pool") }
        assert_nil store.claim_target_activation(worker_id: "wrong-pool", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"], worker_pool: "pool-b")

        activation = store.claim_target_activation(worker_id: "pool-a-worker", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"], worker_pool: "pool-a")
        assert_hash_includes activation, "worker_pool" => "pool-a", "target_id" => "same"
        claimed = store.claim_inbox_messages(worker_pool: "pool-a", target_kind: "object", target_type: "counter", target_id: "same", worker_id: "pool-a-worker", lease_seconds: 30, limit: 10)
        assert_equal [first_id, second_id], claimed.map { |message| message.fetch("id") }
      end
    end

    test "does not complete target activations across worker pools with #{backend.name}" do
      with_durababble_store(backend, "target_activation_completion_pool") do |store|
        message_id = store.enqueue_inbox_message(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          message_kind: "tell",
          method_name: "write",
          payload: { "method_name" => "write", "args" => [], "kwargs" => {} },
          idempotency_key: "pool-a-message",
        )

        activation = store.claim_target_activation(worker_id: "shared-worker", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"], worker_pool: "pool-a")
        assert_hash_includes activation, "worker_pool" => "pool-a", "target_id" => "same", "locked_by" => "shared-worker"
        assert_nil store.target_activation(worker_pool: "pool-b", target_kind: "object", target_type: "counter", target_id: "same")

        assert_nil store.complete_target_activation(worker_pool: "pool-b", target_kind: "object", target_type: "counter", target_id: "same", worker_id: "shared-worker")
        assert_hash_includes store.target_activation(worker_pool: "pool-a", target_kind: "object", target_type: "counter", target_id: "same"), "status" => "running", "locked_by" => "shared-worker"

        claimed = store.claim_inbox_messages(worker_pool: "pool-a", target_kind: "object", target_type: "counter", target_id: "same", worker_id: "shared-worker", lease_seconds: 30, limit: 1)
        assert_equal [message_id], claimed.map { |message| message.fetch("id") }
      end
    end

    test "does not rearm target activations across worker pools with #{backend.name}" do
      with_durababble_store(backend, "target_activation_rearm_pool") do |store|
        store.enqueue_inbox_message(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          message_kind: "tell",
          method_name: "write",
          payload: { "method_name" => "write", "args" => [], "kwargs" => {} },
          idempotency_key: "pool-a-message",
        )

        activation = store.claim_target_activation(worker_id: "shared-worker", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"], worker_pool: "pool-a")
        assert_hash_includes activation, "worker_pool" => "pool-a", "target_id" => "same", "status" => "running", "locked_by" => "shared-worker"

        store.rearm_target_activation(worker_pool: "pool-b", target_kind: "object", target_type: "counter", target_id: "same", ready_at: Time.now)

        assert_hash_includes store.target_activation(worker_pool: "pool-a", target_kind: "object", target_type: "counter", target_id: "same"), "status" => "running", "locked_by" => "shared-worker"
        assert_nil store.claim_target_activation(worker_id: "other-worker", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"], worker_pool: "pool-a")
      end
    end

    test "does not claim future target activations until ready with #{backend.name}" do
      with_durababble_store(backend, "target_activation_future") do |store|
        store.migrate!
        ready_at = Time.now + 60
        store.enqueue_inbox_message(
          target_kind: "object",
          target_type: "counter",
          target_id: "future",
          message_kind: "wake",
          payload: { "wake" => true },
          ready_at:,
        )

        assert_nil store.claim_target_activation(worker_id: "early", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"], now: Time.now)
        activation = store.claim_target_activation(worker_id: "late", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"], now: ready_at + 1)
        assert_hash_includes activation, "target_kind" => "object", "target_type" => "counter", "target_id" => "future", "locked_by" => "late"
      end
    end

    test "expired target activation owners cannot complete activations with #{backend.name}" do
      with_durababble_store(backend, "target_activation_completion_lease") do |store|
        store.migrate!
        store.enqueue_object_command(
          object_type: "counter",
          object_id: "stale-activation",
          method_name: "increment",
          args: [],
          kwargs: {},
        )

        assert_hash_includes(
          store.claim_target_activation(
            worker_id: "activation-owner",
            lease_seconds: 30,
            target_kinds: ["object"],
            target_types: ["counter"],
          ),
          "target_id" => "stale-activation",
          "locked_by" => "activation-owner",
        )
        expire_target_activation!(
          store,
          backend,
          target_kind: "object",
          target_type: "counter",
          target_id: "stale-activation",
        )

        assert_nil store.complete_target_activation(
          target_kind: "object",
          target_type: "counter",
          target_id: "stale-activation",
          worker_id: "activation-owner",
        )
        assert_hash_includes(
          store.target_activation(target_kind: "object", target_type: "counter", target_id: "stale-activation"),
          "status" => "running",
          "locked_by" => "activation-owner",
        )
        assert_hash_includes(
          store.claim_target_activation(
            worker_id: "activation-reclaimer",
            lease_seconds: 30,
            target_kinds: ["object"],
            target_types: ["counter"],
          ),
          "target_id" => "stale-activation",
          "locked_by" => "activation-reclaimer",
        )
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

        same_key_other_target = store.enqueue_inbox_message(
          target_kind: "workflow",
          target_type: "approval",
          target_id: "wf-2",
          message_kind: "workflow_signal",
          payload: { "approved" => true },
          idempotency_key: "signal:approval:wf-1",
        )

        refute_equal first, same_key_other_target
        assert_equal [1], store.inbox_messages_for(target_kind: "workflow", target_type: "approval", target_id: "wf-2").map { |message| message.fetch("sequence").to_i }

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

    test "atomically enqueues workflow commands and rejects terminal workflows with #{backend.name}" do
      with_durababble_store(backend, "workflow_command_enqueue") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(name: "approval", input: {})
        first = store.enqueue_workflow_command(
          workflow_id:,
          workflow_name: "approval",
          method_name: "approve",
          payload: { "method" => "approve", "args" => [], "kwargs" => { reason: "first" } },
          idempotency_key: "approve:1",
        )
        duplicate = store.enqueue_workflow_command(
          workflow_id:,
          workflow_name: "approval",
          method_name: "approve",
          payload: { "method" => "approve", "args" => [], "kwargs" => { reason: "first" } },
          idempotency_key: "approve:1",
        )

        assert_equal first, duplicate
        assert_raises(Durababble::IdempotencyKeyConflict) do
          store.enqueue_workflow_command(
            workflow_id:,
            workflow_name: "approval",
            method_name: "approve",
            payload: { "method" => "approve", "args" => [], "kwargs" => { reason: "different" } },
            idempotency_key: "approve:1",
          )
        end

        store.complete_workflow(workflow_id, result: { "done" => true })
        assert_raises_matching(Durababble::Error, /terminal/) do
          store.enqueue_workflow_command(
            workflow_id:,
            workflow_name: "approval",
            method_name: "approve",
            payload: { "method" => "approve", "args" => [], "kwargs" => { reason: "late" } },
            idempotency_key: "approve:2",
          )
        end
        assert_equal [first], store.inbox_messages_for(target_kind: "workflow", target_type: "approval", target_id: workflow_id).map { |message| message.fetch("id") }
      end
    end

    test "advisory-delivers committed workflow messages to the active lease address with #{backend.name}" do
      with_durababble_store(backend, "workflow_advisory_delivery") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(name: "approval", input: {})
        worker_id = "worker-1@127.0.0.1:12345"
        store.claim_workflow(workflow_id:, worker_id:, lease_seconds: 30)

        client = AdvisoryDeliveryClient.new
        delivered = store.deliver_target_message(
          target_kind: "workflow",
          target_type: "approval",
          target_id: workflow_id,
          client_factory: lambda do |address|
            assert_equal("127.0.0.1:12345", address)
            client
          end,
        )

        assert_equal(true, delivered)
        assert_equal(
          [
            {
              worker_pool: "default",
              target_kind: "workflow",
              target_class: "approval",
              target_id: workflow_id,
              expected_worker_id: worker_id,
            },
          ],
          client.deliveries,
        )

        store.suspend_workflow(workflow_id:, worker_id:)
        assert_equal false, store.deliver_target_message(
          target_kind: "workflow",
          target_type: "approval",
          target_id: workflow_id,
          client_factory: lambda do |_address|
            raise "should not build a client without a live lease"
          end,
        )
      end
    end

    test "advisory-delivers object messages through the active lease worker pool with #{backend.name}" do
      with_durababble_store(backend, "object_advisory_delivery_worker_pool") do |store|
        message_id = store.enqueue_inbox_message(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          message_kind: "tell",
          method_name: "write",
          payload: { "method_name" => "write", "args" => [], "kwargs" => {} },
          idempotency_key: "pool-a-message",
        )
        worker_id = "pool-a-worker@127.0.0.1:34567"
        claimed = store.claim_inbox_messages(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          worker_id:,
          lease_seconds: 30,
          limit: 1,
        )
        assert_equal [message_id], claimed.map { |message| message.fetch("id") }

        wrong_client = AdvisoryDeliveryClient.new
        delivered = store.deliver_target_message(
          worker_pool: "pool-b",
          target_kind: "object",
          target_type: "counter",
          target_id: "same",
          client_factory: lambda do |address|
            assert_equal("127.0.0.1:34567", address)
            wrong_client
          end,
        )
        assert_equal true, delivered
        assert_equal(
          [
            {
              worker_pool: "pool-a",
              target_kind: "object",
              target_class: "counter",
              target_id: "same",
              expected_worker_id: worker_id,
            },
          ],
          wrong_client.deliveries,
        )
      end
    end

    test "keeps committed inbox rows claimable after caller crash with #{backend.name}" do
      skip("in-memory SQLite is a single serialized connection; a second Store.connect handle is a separate database") if backend.sqlite?

      with_durababble_store(backend, "inbox_crash_after_commit") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(name: "approval", input: {})
        message_id = store.enqueue_inbox_message(
          target_kind: "workflow",
          target_type: "approval",
          target_id: workflow_id,
          message_kind: "workflow_command",
          method_name: "approve",
          payload: { "reason" => "ok" },
        )

        recovered = Durababble::Store.connect(database_url: backend.database_url, schema:)
        begin
          assert_hash_includes(recovered.inbox_message(message_id), "status" => "pending")
          assert_equal(1, recovered.inbox_message(message_id).fetch("sequence").to_i)
          assert_hash_includes(
            recovered.claim_workflow_for_activation(workflow_id:, worker_id: "workflow-owner", lease_seconds: 30),
            "id" => workflow_id,
            "locked_by" => "workflow-owner",
          )
          claimed = recovered.claim_inbox_messages(target_kind: "workflow", target_type: "approval", target_id: workflow_id, worker_id: "workflow-owner", lease_seconds: 30)
          assert_equal([message_id], claimed.map { |message| message.fetch("id") })
        ensure
          recovered.close
        end
      end
    end

    test "allocates unique contiguous mailbox sequences under concurrent enqueue with #{backend.name}" do
      skip("in-memory SQLite is a single serialized connection; this test spins up concurrent independent connections") if backend.sqlite?

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

    test "skips excluded workflow ids when claiming runnable work with #{backend.name}" do
      with_durababble_store(backend, "workflow_claim_exclusion") do |store|
        first = store.enqueue_workflow(name: "exclude-workflow", input: { "id" => "first" })
        second = store.enqueue_workflow(name: "exclude-workflow", input: { "id" => "second" })

        claimed = store.claim_runnable_workflow(
          worker_id: "worker-a",
          lease_seconds: 30,
          workflow_names: ["exclude-workflow"],
          excluding_workflow_ids: [first],
        )
        assert_hash_includes claimed, "id" => second, "locked_by" => "worker-a"
        assert_hash_includes store.workflow(first), "status" => "pending", "locked_by" => nil
        assert_nil store.claim_runnable_workflow(
          worker_id: "worker-b",
          lease_seconds: 30,
          workflow_names: ["exclude-workflow"],
          excluding_workflow_ids: [first],
        )
        assert_hash_includes(
          store.claim_runnable_workflow(worker_id: "worker-c", lease_seconds: 30, workflow_names: ["exclude-workflow"]),
          "id" => first,
          "locked_by" => "worker-c",
        )
      end
    end

    test "supports lease, heartbeat, retry, failure, and release lifecycle operations with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
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
        assert_equal 1, store.heartbeat(workflow_id:, worker_id: "worker-a", lease_seconds: 30).affected_rows

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

        object_command_id = store.enqueue_object_command(
          object_type: "counter",
          object_id: "release",
          method_name: "increment",
          args: [],
          kwargs: {},
        )
        assert_hash_includes(
          store.claim_target_activation(worker_id: "object-worker", lease_seconds: 60, target_kinds: ["object"], target_types: ["counter"]),
          "target_kind" => "object",
          "target_type" => "counter",
          "target_id" => "release",
          "locked_by" => "object-worker",
        )
        assert_hash_includes(
          store.claim_object_command(command_id: object_command_id, worker_id: "object-worker", lease_seconds: 60),
          "status" => "running",
          "locked_by" => "object-worker",
        )

        released = store.release_worker_leases!(worker_id: "object-worker")
        assert_hash_includes released, "inbox" => 1, "target_activations" => 1
        assert_hash_includes store.inbox_message(object_command_id), "status" => "pending", "locked_by" => nil, "locked_until" => nil
        assert_hash_includes(
          store.claim_target_activation(worker_id: "object-recovery", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"]),
          "target_id" => "release",
          "locked_by" => "object-recovery",
        )
        assert_hash_includes(
          store.claim_object_command(command_id: object_command_id, worker_id: "object-recovery", lease_seconds: 30),
          "status" => "running",
          "locked_by" => "object-recovery",
        )

        store.fail_workflow(workflow_id, error: "fatal")
        assert_hash_includes store.workflow(workflow_id), "status" => "failed", "error" => "fatal"
        assert_nil store.claim_runnable_workflow(worker_id: "worker-d", lease_seconds: 30)
        assert_nil store.claim_workflow(workflow_id:, worker_id: "worker-d", lease_seconds: 30)
        assert_equal 0, store.steal_expired_leases!(now: Time.now)
      end
    end

    test "same-owner workflow claims refresh live leases with #{backend.name}" do
      with_durababble_store(backend, "claim_lease_refresh") do |store|
        {
          claim_workflow: "targeted-claim-refresh",
          claim_workflow_for_activation: "activation-claim-refresh",
        }.each do |claim_method, workflow_name|
          workflow_id = store.enqueue_workflow(name: workflow_name, input: {})

          first = store.public_send(claim_method, workflow_id:, worker_id: "owner", lease_seconds: 5)
          before = workflow_lease_time(first.fetch("locked_until"))

          refreshed = store.public_send(claim_method, workflow_id:, worker_id: "owner", lease_seconds: 60)
          after = workflow_lease_time(refreshed.fetch("locked_until"))

          assert_operator after, :>, before + 30, "#{claim_method} should extend the live lease held by the same worker"
          assert_hash_includes store.workflow(workflow_id), "status" => "running", "locked_by" => "owner"
          assert_nil store.public_send(claim_method, workflow_id:, worker_id: "intruder", lease_seconds: 60)
        end
      end
    end

    test "mysql targeted workflow claims skip locked rows under contention with #{backend.name}" do
      skip("MySQL-specific SKIP LOCKED behavior") unless backend.mysql?

      with_durababble_store(backend, "targeted_claim_contention") do |store|
        workflow_ids = {
          claim_workflow: store.enqueue_workflow(name: "targeted-contention", input: {}),
          claim_workflow_for_activation: store.enqueue_workflow(name: "activation-contention", input: {}),
        }
        holder = Durababble::Store.connect(database_url: backend.database_url, schema:)
        contender = Durababble::Store.connect(database_url: backend.database_url, schema:)

        begin
          contender.send(:execute, "SET SESSION innodb_lock_wait_timeout = 1")

          workflow_ids.each do |claim_method, workflow_id|
            holder.send(:transaction) do
              holder.send(:execute_store_query, :lock_workflow_for_update, [workflow_id])

              started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
              assert_nil(contender.public_send(claim_method, workflow_id:, worker_id: "contender", lease_seconds: 30))
              elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at

              assert_operator(elapsed, :<, 1.0, "#{claim_method} should skip a row locked by another transaction")
            end

            assert_hash_includes(
              contender.public_send(claim_method, workflow_id:, worker_id: "contender", lease_seconds: 30),
              "id" => workflow_id,
              "locked_by" => "contender",
            )
          end
        ensure
          holder&.close
          contender&.close
        end
      end
    end

    test "rejects heartbeat attempts after a workflow lease expires with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        workflow_id = store.enqueue_workflow(name: "expired-heartbeat", input: {})

        assert_hash_includes(
          store.claim_workflow(workflow_id:, worker_id: "zombie", lease_seconds: 1),
          "locked_by" => "zombie",
        )
        sleep(1.1)
        assert_equal false, store.workflow_owned?(workflow_id:, worker_id: "zombie")
        assert_equal 0, store.heartbeat(workflow_id:, worker_id: "zombie", lease_seconds: 30).affected_rows
        assert_equal false, store.workflow_owned?(workflow_id:, worker_id: "zombie")
      end
    end

    test "rejects non-positive workflow outbox activation and inbox leases with #{backend.name}" do
      with_durababble_store(backend, "lease_validation") do |store|
        workflow_id = store.enqueue_workflow(name: "lease-validation", input: {})
        store.enqueue_outbox(workflow_id:, topic: "events", payload: {}, key: "lease-validation")
        command_id = store.enqueue_inbox_message(
          target_kind: "object",
          target_type: "counter",
          target_id: "lease-validation",
          message_kind: "ask",
          method_name: "increment",
          payload: { "method_name" => "increment", "args" => [], "kwargs" => {} },
        )

        [
          -> { store.create_workflow(name: "invalid-create", input: {}, worker_id: "owner", lease_seconds: 0) },
          -> { store.mark_workflow_running(workflow_id, worker_id: "owner", lease_seconds: 0) },
          -> { store.claim_runnable_workflow(worker_id: "owner", lease_seconds: 0) },
          -> { store.claim_workflow(workflow_id:, worker_id: "owner", lease_seconds: 0) },
          -> { store.claim_workflow_for_activation(workflow_id:, worker_id: "owner", lease_seconds: 0) },
          -> { store.heartbeat(workflow_id:, worker_id: "owner", lease_seconds: 0) },
          -> { store.claim_target_activation(worker_id: "owner", lease_seconds: 0) },
          -> { store.claim_outbox(worker_id: "owner", lease_seconds: 0) },
          -> { store.claim_inbox_messages(target_kind: "object", target_type: "counter", target_id: "lease-validation", worker_id: "owner", lease_seconds: 0) },
          -> { store.claim_next_workflow_command(worker_pool: "default", workflow_name: "lease-validation", workflow_id:, worker_id: "owner", lease_seconds: 0) },
          -> { store.claim_object_command(command_id:, worker_id: "owner", lease_seconds: 0) },
          -> { store.heartbeat_step(workflow_id:, worker_id: "owner", lease_seconds: 0, cursor: {}) },
        ].each do |operation|
          assert_raises_matching(ArgumentError, /lease_seconds must be a positive Numeric/, &operation)
        end
      end
    end

    test "heartbeat_step treats the fenced lease renewal as authoritative for #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        workflow_id = store.enqueue_workflow(name: "heartbeat-fence", input: {})
        store.claim_workflow(workflow_id:, worker_id: "owner", lease_seconds: 30)
        store.record_step_started(workflow_id:, position: 0, name: "fenced")

        # The genuine owner renews successfully while it holds the lease.
        refute_nil store.heartbeat_step(workflow_id:, position: 0, worker_id: "owner", lease_seconds: 30, cursor: { "offset" => 1 })

        # Simulate losing the lease in the window between an ownership read and the
        # fenced renewal UPDATE: the conditional UPDATE matches zero rows. The renewal
        # result must be authoritative, so heartbeat_step must report failure (nil)
        # rather than trusting a stale ownership read or an unscoped locked_until lookup.
        original = store.method(:execute_store_query)
        store.define_singleton_method(:execute_store_query) do |id, params = [], **locals|
          if id == :heartbeat_step_workflow
            ActiveRecord::Result.empty(affected_rows: 0)
          else
            original.call(id, params, **locals)
          end
        end

        assert_nil store.heartbeat_step(workflow_id:, position: 0, worker_id: "owner", lease_seconds: 30, cursor: { "offset" => 2 })
      end
    end

    test "fences terminal workflow status updates with the active workflow lease for #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        completion_id = store.enqueue_workflow(name: "fenced-complete", input: {})
        store.claim_workflow(workflow_id: completion_id, worker_id: "owner", lease_seconds: 30)
        assert_raises(Durababble::LeaseConflict) do
          store.complete_workflow(completion_id, result: { "done" => true }, worker_id: "intruder")
        end
        assert_hash_includes store.workflow(completion_id), "status" => "running", "locked_by" => "owner", "result" => nil
        store.complete_workflow(completion_id, result: { "done" => true }, worker_id: "owner")
        assert_hash_includes store.workflow(completion_id), "status" => "completed", "locked_by" => nil, "result" => { "done" => true }

        failure_id = store.enqueue_workflow(name: "fenced-fail", input: {})
        store.claim_workflow(workflow_id: failure_id, worker_id: "owner", lease_seconds: 1)
        sleep(1.1)
        assert_raises(Durababble::LeaseConflict) do
          store.fail_workflow(failure_id, error: "late failure", worker_id: "owner")
        end
        assert_hash_includes store.workflow(failure_id), "status" => "running", "locked_by" => "owner", "error" => nil

        cancel_id = store.enqueue_workflow(name: "fenced-cancel", input: {})
        store.claim_workflow(workflow_id: cancel_id, worker_id: "owner", lease_seconds: 30)
        assert_raises(Durababble::LeaseConflict) do
          store.cancel_workflow(cancel_id, reason: "wrong owner", worker_id: "intruder")
        end
        assert_hash_includes store.workflow(cancel_id), "status" => "running", "locked_by" => "owner", "error" => nil
        store.cancel_workflow(cancel_id, reason: "owner cancel", result: { "cleanup" => true }, worker_id: "owner")
        assert_hash_includes store.workflow(cancel_id), "status" => "canceled", "locked_by" => nil, "error" => "owner cancel", "result" => { "cleanup" => true }
      end
    end

    test "unfenced workflow status writes do not mutate terminal rows with #{backend.name}" do
      with_durababble_store(backend, "terminal_immutability") do |store|
        completed_id = store.enqueue_workflow(name: "terminal-completed", input: {})
        store.complete_workflow(completed_id, result: { "done" => true })

        assert_raises(Durababble::Error) do
          store.complete_workflow(completed_id, result: { "done" => false })
        end
        store.cancel_workflow(completed_id, reason: "too late")
        store.fail_workflow(completed_id, error: "too late")
        store.mark_workflow_running(completed_id, worker_id: "owner", lease_seconds: 30)
        assert_hash_includes(
          store.workflow(completed_id),
          "status" => "completed",
          "result" => { "done" => true },
          "error" => nil,
          "locked_by" => nil,
        )

        canceled_id = store.enqueue_workflow(name: "terminal-canceled", input: {})
        store.cancel_workflow(canceled_id, reason: "operator stop", result: { "cleanup" => true })

        assert_raises(Durababble::Error) do
          store.complete_workflow(canceled_id, result: { "done" => true })
        end
        store.cancel_workflow(canceled_id, reason: "second cancel")
        store.fail_workflow(canceled_id, error: "too late")
        store.mark_workflow_running(canceled_id)
        assert_hash_includes(
          store.workflow(canceled_id),
          "status" => "canceled",
          "result" => { "cleanup" => true },
          "error" => "operator stop",
          "locked_by" => nil,
        )

        failed_id = store.enqueue_workflow(name: "terminal-failed", input: {})
        store.fail_workflow(failed_id, error: "fatal")

        assert_raises(Durababble::Error) do
          store.complete_workflow(failed_id, result: { "done" => true })
        end
        store.cancel_workflow(failed_id, reason: "too late")
        store.fail_workflow(failed_id, error: "different")
        store.mark_workflow_running(failed_id, worker_id: "owner", lease_seconds: 30)
        assert_hash_includes(
          store.workflow(failed_id),
          "status" => "failed",
          "error" => "fatal",
          "next_run_at" => nil,
          "locked_by" => nil,
        )
      end
    end

    test "terminal workflow writes do not leave incomplete durable work with #{backend.name}" do
      with_durababble_store(backend, "terminal_work_cleanup") do |store|
        completion_id = store.create_workflow(name: "reject-incomplete-complete", input: {})
        store.record_step_scheduled(workflow_id: completion_id, command_id: 0, name: "not_done")
        assert_raises(Durababble::Error) do
          store.complete_workflow(completion_id, result: { "done" => true })
        end
        assert_hash_includes store.workflow(completion_id), "status" => "running", "result" => nil
        assert_equal ["scheduled"], store.steps_for(completion_id).map { |step| step.fetch("status") }

        pending_wait_id = store.create_workflow(name: "reject-pending-wait-complete", input: {})
        store.record_wait(
          workflow_id: pending_wait_id,
          command_id: 0,
          name: "waiting",
          wait_request: Durababble.wait_until(Time.now + 3600, { "waiting" => true }),
        )
        store.record_step_completed(workflow_id: pending_wait_id, command_id: 0, result: { "finished" => true })
        assert_raises(Durababble::Error) do
          store.complete_workflow(pending_wait_id, result: { "done" => true })
        end
        assert_hash_includes store.workflow(pending_wait_id), "status" => "waiting", "result" => nil
        assert_equal ["completed"], store.wait_snapshots_for(pending_wait_id).map { |wait| wait.fetch("status") }

        cancel_id = store.create_workflow(name: "cancel-incomplete-work", input: {})
        store.record_step_scheduled(workflow_id: cancel_id, command_id: 0, name: "scheduled")
        store.record_step_started(workflow_id: cancel_id, command_id: 1, name: "running")
        store.record_wait(
          workflow_id: cancel_id,
          command_id: 2,
          name: "waiting",
          wait_request: Durababble.wait_until(Time.now + 3600, { "waiting" => true }),
        )

        store.cancel_workflow(cancel_id, reason: "operator stop")
        assert_hash_includes store.workflow(cancel_id), "status" => "canceled", "error" => "operator stop"
        assert_equal ["canceled", "canceled", "canceled"], store.steps_for(cancel_id).map { |step| step.fetch("status") }
        assert_equal ["canceled"], store.step_attempts_for(cancel_id).map { |attempt| attempt.fetch("status") }
        assert_equal ["canceled"], store.wait_snapshots_for(cancel_id).map { |wait| wait.fetch("status") }
      end
    end

    test "reclaims expired durable object command leases with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        command_id = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "increment",
          args: [],
          kwargs: {},
        )

        assert_hash_includes(
          store.claim_object_command(command_id:, worker_id: "crashed-object-worker", lease_seconds: 30),
          "locked_by" => "crashed-object-worker",
        )
        expire_inbox_message_lease!(store, backend, command_id)
        expire_object_lease!(store, backend, "counter", "abc")
        assert_hash_includes(
          store.claim_object_command(command_id:, worker_id: "recovery-object-worker", lease_seconds: 30),
          "id" => command_id,
          "status" => "running",
          "locked_by" => "recovery-object-worker",
        )
      end
    end

    test "fences stale durable object command failure and retry writes with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        stale_failure = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "increment",
          args: [],
          kwargs: {},
        )
        assert_hash_includes(
          store.claim_object_command(command_id: stale_failure, worker_id: "expired-object-worker", lease_seconds: 30),
          "locked_by" => "expired-object-worker",
        )
        expire_object_lease!(store, backend, "counter", "abc")
        assert_raises(Durababble::LeaseConflict) do
          store.fail_object_command(command_id: stale_failure, error: "stale failure", worker_id: "expired-object-worker", terminal: true)
        end
        assert_hash_includes store.inbox_message(stale_failure), "status" => "running", "locked_by" => "expired-object-worker", "error" => nil
        expire_inbox_message_lease!(store, backend, stale_failure)
        assert_hash_includes(
          store.claim_object_command(command_id: stale_failure, worker_id: "recovery-object-worker", lease_seconds: 30),
          "locked_by" => "recovery-object-worker",
        )

        stale_retry = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc-retry",
          method_name: "increment",
          args: [],
          kwargs: {},
        )
        assert_hash_includes(
          store.claim_object_command(command_id: stale_retry, worker_id: "expired-object-worker", lease_seconds: 30),
          "locked_by" => "expired-object-worker",
        )
        expire_object_lease!(store, backend, "counter", "abc-retry")
        assert_raises(Durababble::LeaseConflict) do
          store.retry_object_command(command_id: stale_retry, error: "stale retry", worker_id: "expired-object-worker", ready_at: Time.now + 60)
        end
        assert_hash_includes store.inbox_message(stale_retry), "status" => "running", "locked_by" => "expired-object-worker", "error" => nil
        expire_inbox_message_lease!(store, backend, stale_retry)
        assert_hash_includes(
          store.claim_object_command(command_id: stale_retry, worker_id: "recovery-object-worker", lease_seconds: 30),
          "locked_by" => "recovery-object-worker",
        )
      end
    end

    test "object-origin child starts require a live object command lease with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_object_command_lease_fence") do |store|
        command_id = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "start_child",
          args: [],
          kwargs: {},
        )
        assert_hash_includes(
          store.claim_object_command(command_id:, worker_id: "stale-object-worker", lease_seconds: 1),
          "locked_by" => "stale-object-worker",
        )
        sleep(1.1)

        assert_raises_matching(Durababble::LeaseConflict, /lease expired or moved/) do
          store.start_child_workflow(
            origin_kind: "object",
            parent_object_type: "counter",
            parent_object_id: "abc",
            parent_object_command_id: command_id,
            parent_object_worker_id: "stale-object-worker",
            child_workflow_name: "child",
            child_workflow_id: "stale-object-child",
            input: { "value" => "blocked" },
            worker_pool: "default",
            cancellation_policy: "abandon",
          )
        end
        assert_raises(KeyError) { store.workflow("stale-object-child") }

        assert_hash_includes(
          store.claim_object_command(command_id:, worker_id: "current-object-worker", lease_seconds: 30),
          "locked_by" => "current-object-worker",
        )
        child = store.start_child_workflow(
          origin_kind: "object",
          parent_object_type: "counter",
          parent_object_id: "abc",
          parent_object_command_id: command_id,
          parent_object_worker_id: "current-object-worker",
          child_workflow_name: "child",
          child_workflow_id: "current-object-child",
          input: { "value" => "allowed" },
          worker_pool: "default",
          cancellation_policy: "abandon",
        )
        assert_hash_includes child, "child_workflow_id" => "current-object-child", "parent_object_command_id" => command_id
      end
    end

    test "fences stale durable object command completion writes with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        command_id = store.enqueue_object_command(
          object_type: "counter",
          object_id: "stale-complete",
          method_name: "increment",
          args: [],
          kwargs: {},
        )
        assert_hash_includes(
          store.claim_object_command(command_id:, worker_id: "expired-object-worker", lease_seconds: 30),
          "locked_by" => "expired-object-worker",
        )

        expire_object_lease!(store, backend, "counter", "stale-complete")
        assert_raises(Durababble::LeaseConflict) do
          store.complete_object_command(
            command_id:,
            result: { "count" => 1 },
            object_type: "counter",
            object_id: "stale-complete",
            state: { "count" => 1 },
            worker_id: "expired-object-worker",
          )
        end
        assert_hash_includes store.inbox_message(command_id), "status" => "running", "locked_by" => "expired-object-worker", "result" => nil
        assert_nil store.object_state(object_type: "counter", object_id: "stale-complete")

        expire_inbox_message_lease!(store, backend, command_id)
        assert_hash_includes(
          store.claim_object_command(command_id:, worker_id: "recovery-object-worker", lease_seconds: 30),
          "locked_by" => "recovery-object-worker",
        )
      end
    end

    test "does not leave MySQL transactions open after no-op claim paths with #{backend.name}" do
      skip("only the MySQL backend exposes transaction metadata") unless backend.mysql?

      with_durababble_store(backend, "conformance") do |store|
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

    test "claims, renews, releases, and surfaces the unified object lease with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        # claim takes worker_pool as routing metadata; renew/release key only on
        # (object_type, object_id) since object identity is global.
        claim_key = { worker_pool: "default", object_type: "counter", object_id: "abc" }
        key = { object_type: "counter", object_id: "abc" }
        assert_nil store.current_object_lease("counter", "abc")

        first = store.claim_object_lease(**claim_key, worker_id: "owner-a", lease_seconds: 30)
        assert_equal "owner-a", first.fetch("worker_id")
        assert_hash_includes(
          store.current_object_lease("counter", "abc"),
          "worker_id" => "owner-a",
        )

        # Re-claim by the same worker refreshes the lease (no conflict).
        same = store.claim_object_lease(**claim_key, worker_id: "owner-a", lease_seconds: 30)
        assert_equal "owner-a", same.fetch("worker_id")

        # A different worker is rejected while a live lease holds the row, and the
        # rejection surfaces as nil so callers branch on win/loss without having
        # to compare worker ids. The existing holder is not exposed through this
        # path; consumers read current_object_lease for routing.
        assert_nil store.claim_object_lease(**claim_key, worker_id: "intruder", lease_seconds: 30)
        assert_hash_includes(
          store.current_object_lease("counter", "abc"),
          "worker_id" => "owner-a",
        )

        # Negative or zero lease_seconds are rejected at the public boundary: a
        # non-positive lease would be expired the moment it was written, which
        # only confuses the takeover path. Tests that need to seed an expired
        # row must do so explicitly (see expire_object_lease! below).
        assert_raises(ArgumentError) do
          store.claim_object_lease(**claim_key, worker_id: "intruder", lease_seconds: -1)
        end
        assert_raises(ArgumentError) do
          store.claim_object_lease(**claim_key, worker_id: "intruder", lease_seconds: 0)
        end

        assert_equal true, store.renew_object_lease(**key, worker_id: "owner-a", lease_seconds: 30)
        # A non-owner cannot renew.
        assert_equal false, store.renew_object_lease(**key, worker_id: "intruder", lease_seconds: 30)

        # A non-owner cannot release.
        assert_equal false, store.release_object_lease(**key, worker_id: "intruder")
        assert_hash_includes(store.current_object_lease("counter", "abc"), "worker_id" => "owner-a")

        # The owner releases cleanly.
        assert_equal true, store.release_object_lease(**key, worker_id: "owner-a")
        assert_nil store.current_object_lease("counter", "abc")

        # After release, renew on an empty row returns false.
        assert_equal false, store.renew_object_lease(**key, worker_id: "owner-a", lease_seconds: 30)
      end
    end

    test "preserves fractional object lease durations with #{backend.name}" do
      with_durababble_store(backend, "fractional_object_lease") do |store|
        claim_key = { worker_pool: "default", object_type: "counter", object_id: "fractional" }

        assert_equal "owner-a", store.claim_object_lease(**claim_key, worker_id: "owner-a", lease_seconds: 0.75).fetch("worker_id")
        lease = store.current_object_lease("counter", "fractional")

        refute_nil lease
        assert_hash_includes(lease, "worker_id" => "owner-a")
        assert_operator lease_seconds_remaining(store, backend, lease.fetch("locked_until")), :>, 0.1
        assert_nil store.claim_object_lease(**claim_key, worker_id: "intruder", lease_seconds: 30)
      end
    end

    test "takes over an expired object lease and reports it via release_worker_leases / steal with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        claim_key = { worker_pool: "default", object_type: "counter", object_id: "expired" }

        # Claim normally, then explicitly backdate `locked_until` to simulate a
        # crashed owner. This separates the seeding step from the takeover under
        # test: the previous shortcut (`lease_seconds: -1`) blurred the two and
        # depended on a since-removed permissive claim path.
        store.claim_object_lease(**claim_key, worker_id: "crashed-owner", lease_seconds: 30)
        expire_object_lease!(store, backend, "counter", "expired")
        assert_nil store.current_object_lease("counter", "expired")

        # A new owner can take over because the existing row is expired.
        new_holder = store.claim_object_lease(**claim_key, worker_id: "new-owner", lease_seconds: 30)
        assert_equal "new-owner", new_holder.fetch("worker_id")
        assert_hash_includes(store.current_object_lease("counter", "expired"), "worker_id" => "new-owner")

        # release_worker_leases! clears the new owner's holds across both tables.
        counts = store.release_worker_leases!(worker_id: "new-owner")
        assert_operator counts.fetch("durable_objects").to_i, :>=, 1
        assert_nil store.current_object_lease("counter", "expired")

        # steal_expired_leases! sweeps stale rows. Seed an expired row again and
        # confirm the sweep clears it.
        store.claim_object_lease(**claim_key, worker_id: "another-crashed", lease_seconds: 30)
        expire_object_lease!(store, backend, "counter", "expired")
        stolen = store.steal_expired_leases!(now: Time.now)
        assert_operator stolen, :>=, 1
        # The next claim succeeds because the stale row was cleared.
        fresh = store.claim_object_lease(**claim_key, worker_id: "post-steal", lease_seconds: 30)
        assert_equal "post-steal", fresh.fetch("worker_id")
      end
    end

    test "gates claim_inbox_messages for objects on the unified lease with #{backend.name}" do
      with_durababble_store(backend, "conformance") do |store|
        store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "increment",
          args: [],
          kwargs: {},
        )

        # A non-owner is rejected: the claim returns no rows because the lease
        # acquisition inside the transaction did not award them ownership.
        store.claim_object_lease(worker_pool: "default", object_type: "counter", object_id: "abc", worker_id: "owner", lease_seconds: 30)
        rejected = store.claim_inbox_messages(target_kind: "object", target_type: "counter", target_id: "abc", worker_id: "intruder", lease_seconds: 30)
        assert_empty rejected

        # The owner can claim normally.
        claimed = store.claim_inbox_messages(target_kind: "object", target_type: "counter", target_id: "abc", worker_id: "owner", lease_seconds: 30)
        assert_equal 1, claimed.size
      end
    end

    test "does not claim an object lease until pool-local inbox work is claimable with #{backend.name}" do
      with_durababble_store(backend, "object_lease_requires_claimable_work") do |store|
        now = Time.now
        ready_at = now + 60
        message_id = store.enqueue_inbox_message(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "deferred",
          message_kind: "tell",
          method_name: "write",
          payload: { "method_name" => "write", "args" => [], "kwargs" => {} },
          ready_at:,
        )

        early = store.claim_inbox_messages(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "deferred",
          worker_id: "early-worker",
          lease_seconds: 30,
          limit: 1,
          now:,
        )
        assert_empty early
        assert_nil store.current_object_lease("counter", "deferred")

        claimed = store.claim_inbox_messages(
          worker_pool: "pool-a",
          target_kind: "object",
          target_type: "counter",
          target_id: "deferred",
          worker_id: "ready-worker",
          lease_seconds: 30,
          limit: 1,
          now: ready_at + 1,
        )
        assert_equal [message_id], claimed.map { |message| message.fetch("id") }
        assert_hash_includes(
          store.current_object_lease("counter", "deferred"),
          "worker_id" => "ready-worker",
          "worker_pool" => "pool-a",
        )
      end
    end

    test "awards the object lease to exactly one of many concurrent claimers with #{backend.name}" do
      # The atomic-claim primitive's contract is "exactly one wins" — Ruby-level
      # serialization through a single connection only proves the WHERE clause
      # rejects the second claim in sequence. To pin down the SQL-engine-level
      # race we fan out N independent stores (separate connections) and have
      # them all attempt to claim the same fresh row at once. The DB has to
      # serialize the writes; this test asserts the loser branch returns nil
      # rather than blowing up on a unique-key violation, deadlock, or partial
      # update. SQLite skips: the conformance harness uses a single in-memory
      # database with serialized access, which doesn't model the contention.
      skip("in-memory SQLite serializes connections; this test needs real concurrency") if backend.sqlite?

      with_durababble_store(backend, "object_lease_race") do |store|
        store.migrate!
        claim_key = { worker_pool: "default", object_type: "counter", object_id: "race" }
        queue = Queue.new
        thread_count = 8
        mutex = Mutex.new
        condition = ConditionVariable.new
        ready = 0
        release = false

        threads = thread_count.times.map do |index|
          Thread.new do
            thread_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
            begin
              mutex.synchronize do
                ready += 1
                condition.broadcast
                condition.wait(mutex) until release
              end
              holder = thread_store.claim_object_lease(**claim_key, worker_id: "owner-#{index}", lease_seconds: 30)
              queue << (holder ? [:won, "owner-#{index}", holder] : [:lost, "owner-#{index}"])
            rescue StandardError => e
              queue << [:error, "owner-#{index}", e]
            ensure
              thread_store.close
            end
          end
        end

        mutex.synchronize do
          condition.wait(mutex) until ready == thread_count
          release = true
          condition.broadcast
        end

        results = thread_count.times.map { queue.pop }
        threads.each(&:join)

        errors = results.select { |status, *_| status == :error }
        raise errors.first.last unless errors.empty?

        winners = results.select { |status, *_| status == :won }
        losers = results.select { |status, *_| status == :lost }
        assert_equal 1, winners.size, "expected exactly one winner, got #{winners.map { |_, id, _| id }.inspect}"
        assert_equal thread_count - 1, losers.size

        # The lease the database actually holds matches the unique winner.
        winning_worker = winners.first[1]
        live = store.current_object_lease("counter", "race")
        assert_hash_includes live, "worker_id" => winning_worker

        # And the winner's claim row reports the same worker — i.e., the row
        # the DB awarded matches what the caller was told it won.
        winning_row = winners.first[2]
        assert_equal winning_worker, winning_row.fetch("worker_id")
      end
    end

    test "release after steal is a safe no-op for the original holder with #{backend.name}" do
      # The hot-path scenario: worker A claims, A's process stalls past the
      # lease deadline, worker B steals the row and starts processing, and
      # then A's `ensure`-block release finally fires. A's release must NOT
      # clear B's hold — the conditional `WHERE locked_by = worker_id` in the
      # release SQL is what makes this safe, and a regression here would
      # silently strand a real owner. (`drain_object_inbox` relies on this in
      # its ensure clause, as does any stream lease releaser layered on top.)
      with_durababble_store(backend, "object_lease_release_after_steal") do |store|
        claim_key = { worker_pool: "default", object_type: "counter", object_id: "stale" }
        key = { object_type: "counter", object_id: "stale" }

        # A holds the lease, then the row is backdated to simulate A's process
        # stalling. We don't actually wait `lease_seconds` — `expire_object_lease!`
        # rewrites locked_until directly, mirroring how `steal_expired_leases!`
        # would observe a crashed owner.
        store.claim_object_lease(**claim_key, worker_id: "owner-a", lease_seconds: 30)
        expire_object_lease!(store, backend, "counter", "stale")

        # B takes over by claiming the now-expired row. After the takeover, B
        # is the only legitimate holder; current_object_lease reflects that.
        assert_equal "owner-b", store.claim_object_lease(**claim_key, worker_id: "owner-b", lease_seconds: 30).fetch("worker_id")
        assert_hash_includes(store.current_object_lease("counter", "stale"), "worker_id" => "owner-b")

        # A's release fires late. The `WHERE locked_by = worker_id` guard
        # means it returns false (nothing to release for A) and does not
        # touch B's hold.
        assert_equal false, store.release_object_lease(**key, worker_id: "owner-a")
        assert_hash_includes(store.current_object_lease("counter", "stale"), "worker_id" => "owner-b")

        # B can still renew and release normally — the row is intact.
        assert_equal true, store.renew_object_lease(**key, worker_id: "owner-b", lease_seconds: 30)
        assert_equal true, store.release_object_lease(**key, worker_id: "owner-b")
        assert_nil store.current_object_lease("counter", "stale")

        # And A's release remains idempotent on the now-empty row.
        assert_equal false, store.release_object_lease(**key, worker_id: "owner-a")
      end
    end
  end

  def expire_inbox_message_lease!(store, backend, message_id)
    table = store.send(:table, "inbox")
    if backend.postgres?
      store.send(
        :execute_params,
        "UPDATE #{table} SET locked_until = now() - interval '1 hour' WHERE id = $1",
        [message_id],
      )
    elsif backend.sqlite?
      expired_at = ((Time.now.to_r - 3600) * 1_000_000).to_i
      store.send(
        :execute_params,
        "UPDATE #{table} SET locked_until = ? WHERE id = ?",
        [expired_at, message_id],
      )
    else
      expired_at = (Time.now - 3600).strftime("%Y-%m-%d %H:%M:%S.%6N")
      store.send(
        :execute_params,
        "UPDATE #{table} SET locked_until = ? WHERE id = ?",
        [expired_at, message_id],
      )
    end
  end

  def expire_target_activation!(store, backend, target_kind:, target_type:, target_id:)
    table = store.send(:table, "target_activations")
    if backend.sqlite?
      # SqliteStore stores locked_until as an integer microsecond clock; subtract
      # 1 hour worth of integer ticks from dura_now() so the comparison matches
      # the integer clock the translated NOW(6) reads.
      store.send(
        :execute_params,
        <<~SQL,
          UPDATE #{table}
          SET locked_until = dura_now() - (3600 * #{store.send(:seconds_scale)})
          WHERE target_kind = ? AND target_type = ? AND target_id = ?
        SQL
        [target_kind, target_type, target_id],
      )
    elsif backend.mysql?
      expired_at = (Time.now - 3600).strftime("%Y-%m-%d %H:%M:%S.%6N")
      store.send(
        :execute_params,
        <<~SQL,
          UPDATE #{table}
          SET locked_until = ?
          WHERE target_kind = ? AND target_type = ? AND target_id = ?
        SQL
        [expired_at, target_kind, target_type, target_id],
      )
    else
      store.send(
        :execute_params,
        <<~SQL,
          UPDATE #{table}
          SET locked_until = now() - interval '1 hour'
          WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
        SQL
        [target_kind, target_type, target_id],
      )
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

  def lease_seconds_remaining(store, backend, locked_until)
    if backend.sqlite?
      return (locked_until.to_f - store.current_time.to_f) / store.send(:seconds_scale)
    end

    locked_until = locked_until.is_a?(Time) ? locked_until : Time.parse(locked_until.to_s)
    locked_until.to_f - Time.now.to_f
  end

  def workflow_lease_time(value)
    return value.to_time if value.respond_to?(:to_time)
    return Time.at(value.to_r / 1_000_000) if value.is_a?(Integer)

    Time.parse(value.to_s)
  end

  # Backdate a row's locked_until to simulate a crashed owner without relying
  # on a permissive `claim_object_lease(lease_seconds: -1)` shortcut, which the
  # public API now rejects. Each backend stores `locked_until` in its own
  # native form: Postgres takes a literal interval; MySQL uses a DATETIME(6)
  # string; SQLite stores an integer microsecond epoch (dura_now()'s clock),
  # so we have to hand it the integer that comparison against dura_now() will
  # treat as "in the past".
  def expire_object_lease!(store, backend, object_type, object_id)
    table = store.send(:table, "durable_objects")
    if backend.postgres?
      store.send(
        :execute_params,
        "UPDATE #{table} SET locked_until = now() - interval '1 hour' WHERE object_type = $1 AND object_id = $2",
        [object_type, object_id],
      )
    elsif backend.sqlite?
      expired_at = ((Time.now.to_r - 3600) * 1_000_000).to_i
      store.send(
        :execute_params,
        "UPDATE #{table} SET locked_until = ? WHERE object_type = ? AND object_id = ?",
        [expired_at, object_type, object_id],
      )
    else
      expired_at = (Time.now - 3600).strftime("%Y-%m-%d %H:%M:%S.%6N")
      store.send(
        :execute_params,
        "UPDATE #{table} SET locked_until = ? WHERE object_type = ? AND object_id = ?",
        [expired_at, object_type, object_id],
      )
    end
  end
end
