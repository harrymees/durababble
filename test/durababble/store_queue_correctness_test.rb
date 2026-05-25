# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleStoreQueueCorrectnessTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "claims the oldest runnable workflow across pending, failed, and expired running queues with #{backend.name}" do
      with_durababble_store(backend, "queue_correctness") do |store|
        pending_newer = enqueue_workflow_at("pending-newer", status: "pending", created_at: Time.now - 60)
        failed_middle = enqueue_workflow_at(
          "failed-middle",
          status: "failed",
          created_at: Time.now - 120,
          next_run_at: Time.now - 90,
        )
        expired_oldest = enqueue_workflow_at(
          "expired-oldest",
          status: "running",
          created_at: Time.now - 180,
          locked_by: "dead",
          locked_until: Time.now - 30,
        )
        active_oldest = enqueue_workflow_at(
          "active-oldest",
          status: "running",
          created_at: Time.now - 240,
          locked_by: "live",
          locked_until: Time.now + 300,
        )

        first = store.claim_runnable_workflow(worker_id: "worker-a", lease_seconds: 60)
        second = store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 60)
        third = store.claim_runnable_workflow(worker_id: "worker-c", lease_seconds: 60)
        fourth = store.claim_runnable_workflow(worker_id: "worker-d", lease_seconds: 60)

        assert_equal expired_oldest, first.fetch("id")
        assert_equal failed_middle, second.fetch("id")
        assert_equal pending_newer, third.fetch("id")
        assert_nil fourth

        assert_hash_includes store.workflow(active_oldest), "status" => "running", "locked_by" => "live"
        assert_hash_includes store.workflow(expired_oldest), "status" => "running", "locked_by" => "worker-a"
      end
    end

    test "does not hide the oldest expired workflow behind a newer more-expired lease with #{backend.name}" do
      with_durababble_store(backend, "queue_correctness") do |store|
        expired_oldest = enqueue_workflow_at(
          "expired-oldest",
          status: "running",
          created_at: Time.now - 300,
          locked_by: "dead-a",
          locked_until: Time.now - 10,
        )
        pending_middle = enqueue_workflow_at("pending-middle", status: "pending", created_at: Time.now - 100)
        expired_newer_more_expired = enqueue_workflow_at(
          "expired-newer",
          status: "running",
          created_at: Time.now - 50,
          locked_by: "dead-b",
          locked_until: Time.now - 100,
        )

        first = store.claim_runnable_workflow(worker_id: "worker-a", lease_seconds: 60)
        second = store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 60)
        third = store.claim_runnable_workflow(worker_id: "worker-c", lease_seconds: 60)

        assert_equal expired_oldest, first.fetch("id")
        assert_equal pending_middle, second.fetch("id")
        assert_equal expired_newer_more_expired, third.fetch("id")
      end
    end

    test "does not steal an unexpired workflow lease from another worker via direct claim with #{backend.name}" do
      with_durababble_store(backend, "queue_correctness") do |store|
        workflow_id = enqueue_workflow_at(
          "active",
          status: "running",
          created_at: Time.now - 60,
          locked_by: "owner",
          locked_until: Time.now + 300,
        )

        stolen = store.claim_workflow(workflow_id:, worker_id: "intruder", lease_seconds: 60)

        assert_nil stolen
        assert_hash_includes store.workflow(workflow_id), "status" => "running", "locked_by" => "owner"
      end
    end

    test "claims the oldest available outbox message across pending and expired processing queues with #{backend.name}" do
      with_durababble_store(backend, "queue_correctness") do |store|
        workflow_id = store.enqueue_workflow(name: "outbox-owner", input: {})
        active_processing = enqueue_outbox_at(
          workflow_id:,
          id: "active-processing",
          key: "outbox:active",
          status: "processing",
          created_at: Time.now - 240,
          locked_by: "sender",
          locked_until: Time.now + 300,
        )
        expired_processing = enqueue_outbox_at(
          workflow_id:,
          id: "expired-processing",
          key: "outbox:expired",
          status: "processing",
          created_at: Time.now - 180,
          locked_by: "dead-sender",
          locked_until: Time.now - 30,
        )
        pending_newer = enqueue_outbox_at(
          workflow_id:,
          id: "pending-newer",
          key: "outbox:pending",
          status: "pending",
          created_at: Time.now - 60,
        )

        first = store.claim_outbox(worker_id: "sender-a", lease_seconds: 60)
        second = store.claim_outbox(worker_id: "sender-b", lease_seconds: 60)
        third = store.claim_outbox(worker_id: "sender-c", lease_seconds: 60)

        assert_equal expired_processing, first.fetch("id")
        assert_equal pending_newer, second.fetch("id")
        assert_nil third

        assert_hash_includes store.outbox_message(active_processing), "status" => "processing", "locked_by" => "sender"
      end
    end

    test "does not hide the oldest expired outbox message behind a newer more-expired lease with #{backend.name}" do
      with_durababble_store(backend, "queue_correctness") do |store|
        workflow_id = store.enqueue_workflow(name: "outbox-owner", input: {})
        expired_oldest = enqueue_outbox_at(
          workflow_id:,
          id: "expired-oldest",
          key: "outbox:expired-oldest",
          status: "processing",
          created_at: Time.now - 300,
          locked_by: "dead-a",
          locked_until: Time.now - 10,
        )
        pending_middle = enqueue_outbox_at(
          workflow_id:,
          id: "outbox-pending-middle",
          key: "outbox:pending-middle",
          status: "pending",
          created_at: Time.now - 100,
        )
        expired_newer_more_expired = enqueue_outbox_at(
          workflow_id:,
          id: "expired-newer",
          key: "outbox:expired-newer",
          status: "processing",
          created_at: Time.now - 50,
          locked_by: "dead-b",
          locked_until: Time.now - 100,
        )

        first = store.claim_outbox(worker_id: "sender-a", lease_seconds: 60)
        second = store.claim_outbox(worker_id: "sender-b", lease_seconds: 60)
        third = store.claim_outbox(worker_id: "sender-c", lease_seconds: 60)

        assert_equal expired_oldest, first.fetch("id")
        assert_equal pending_middle, second.fetch("id")
        assert_equal expired_newer_more_expired, third.fetch("id")
      end
    end

    test "only acknowledges outbox messages for the worker that owns the lease with #{backend.name}" do
      with_durababble_store(backend, "queue_correctness") do |store|
        workflow_id = store.enqueue_workflow(name: "outbox-owner", input: {})
        outbox_id = store.enqueue_outbox(workflow_id:, topic: "events", payload: { "ok" => true }, key: "outbox:ack-owner")
        claimed = store.claim_outbox(worker_id: "owner", lease_seconds: 60)
        assert_equal outbox_id, claimed.fetch("id")

        store.ack_outbox(outbox_id, worker_id: "intruder")
        assert_hash_includes store.outbox_message(outbox_id), "status" => "processing", "locked_by" => "owner"

        store.ack_outbox(outbox_id, worker_id: "owner")
        assert_hash_includes store.outbox_message(outbox_id), "status" => "processed", "locked_by" => "owner"
      end
    end

    test "does not requeue a terminal workflow when a stale wait is signaled with #{backend.name}" do
      with_durababble_store(backend, "queue_correctness") do |store|
        workflow_id = store.create_workflow(name: "stale-wait", input: {})
        store.record_wait(
          workflow_id:,
          position: 0,
          name: "wait_for_event",
          wait_request: Durababble.wait_event("approval:stale", { "before" => true }),
        )
        store.complete_workflow(workflow_id, result: { "done" => true })

        assert_equal 0, store.signal_event("approval:stale", payload: { "approved" => true })
        assert_hash_includes store.workflow(workflow_id), "status" => "completed", "result" => { "done" => true }
      end
    end
  end

  private

  def enqueue_workflow_at(label, status:, created_at:, locked_by: nil, locked_until: nil, next_run_at: nil)
    id = store.enqueue_workflow(name: label, input: { "label" => label })
    if backend_descriptor.mysql?
      store.send(:execute_params, <<~SQL, [status, locked_by, timestamp_or_nil(locked_until), timestamp_or_nil(next_run_at), timestamp(created_at), timestamp(created_at), id])
        UPDATE #{table("workflows")}
        SET status = ?, locked_by = ?, locked_until = ?, next_run_at = ?, created_at = ?, updated_at = ?
        WHERE id = ?
      SQL
    else
      store.send(:execute_params, <<~SQL, [id, status, locked_by, timestamp_or_nil(locked_until), timestamp(created_at), timestamp_or_nil(next_run_at)])
        UPDATE #{table("workflows")}
        SET status = $2, locked_by = $3, locked_until = $4::timestamptz, created_at = $5::timestamptz, updated_at = $5::timestamptz, next_run_at = $6::timestamptz
        WHERE id = $1
      SQL
    end
    id
  end

  def enqueue_outbox_at(workflow_id:, id:, key:, status:, created_at:, locked_by: nil, locked_until: nil)
    payload = store.send(:dump_serialized, { "id" => id })
    if backend_descriptor.mysql?
      store.send(
        :execute_params,
        <<~SQL,
          INSERT INTO #{table("outbox")} (id, workflow_id, topic, payload, `key`, status, locked_by, locked_until, created_at)
          VALUES (?, ?, 'events', ?, ?, ?, ?, ?, ?)
        SQL
        [id, workflow_id, payload, key, status, locked_by, timestamp_or_nil(locked_until), timestamp(created_at)],
      )
    else
      store.send(
        :execute_params,
        <<~SQL,
          INSERT INTO #{table("outbox")} (id, workflow_id, topic, payload, key, status, locked_by, locked_until, created_at)
          VALUES ($1, $2, 'events', $8::bytea, $3, $4, $5, $6::timestamptz, $7::timestamptz)
        SQL
        [id, workflow_id, key, status, locked_by, timestamp_or_nil(locked_until), timestamp(created_at), payload],
      )
    end
    id
  end

  def table(name)
    store.send(:table, name)
  end

  def timestamp(time)
    backend_descriptor.mysql? ? time.utc.strftime("%Y-%m-%d %H:%M:%S.%6N") : time.utc.iso8601(6)
  end

  def timestamp_or_nil(time)
    time ? timestamp(time) : nil
  end
end
