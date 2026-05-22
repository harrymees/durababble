# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Durababble store backend conformance", :integration do
  durababble_store_backends.each do |backend|
    context "with #{backend.name}" do
      let(:schema) { "#{backend.default_schema_prefix}_conformance_#{Process.pid}_#{object_id.abs}" }
      let(:store) { Durababble::Store.connect(database_url: backend.database_url, schema:) }

      after do
        store&.drop_schema!
        store&.close
      end

      it "migrates, enqueues, claims, completes, and decodes serialized workflow state" do
        store.migrate!

        workflow_id = store.enqueue_workflow(name: "conformance", input: { "count" => 1 })
        claimed = store.claim_runnable_workflow(worker_id: "worker-a", lease_seconds: 30)

        expect(claimed).to include(
          "id" => workflow_id,
          "name" => "conformance",
          "status" => "running",
          "input" => { "count" => 1 },
          "locked_by" => "worker-a"
        )

        store.record_step_started(workflow_id:, position: 0, name: "increment")
        store.record_step_completed(workflow_id:, position: 0, result: { "count" => 2 })
        store.complete_workflow(workflow_id, result: { "count" => 2 })

        expect(store.workflow(workflow_id)).to include(
          "status" => "completed",
          "result" => { "count" => 2 }
        )
        expect(store.steps_for(workflow_id).first).to include(
          "status" => "completed",
          "result" => { "count" => 2 }
        )
      end

      it "persists, claims, decodes, and acknowledges outbox messages" do
        store.migrate!
        workflow_id = store.enqueue_workflow(name: "outbox-owner", input: {})

        first_id = store.enqueue_outbox(workflow_id:, topic: "events", payload: { "event" => 1 }, key: "events:1")
        duplicate_id = store.enqueue_outbox(workflow_id:, topic: "events", payload: { "event" => "ignored" }, key: "events:1")

        expect(duplicate_id).to eq(first_id)

        claimed = store.claim_outbox(worker_id: "sender-a", lease_seconds: 30)
        expect(claimed).to include(
          "id" => first_id,
          "workflow_id" => workflow_id,
          "topic" => "events",
          "payload" => { "event" => 1 },
          "status" => "processing",
          "locked_by" => "sender-a"
        )

        store.ack_outbox(first_id, worker_id: "sender-b")
        expect(store.outbox_message(first_id)).to include("status" => "processing")

        store.ack_outbox(first_id, worker_id: "sender-a")
        expect(store.outbox_message(first_id)).to include("status" => "processed", "locked_by" => "sender-a")
      end

      it "persists waits and wakes event waiters once" do
        store.migrate!
        workflow_id = store.create_workflow(name: "waiter", input: { "start" => true })
        wait_id = store.record_wait(
          workflow_id:,
          position: 0,
          name: "approval",
          wait_request: Durababble.wait_event("approval:#{workflow_id}", { "before" => true })
        )

        expect(store.workflow(workflow_id)).to include("status" => "waiting")
        expect(store.waits_for(workflow_id).first).to include(
          "id" => wait_id,
          "status" => "pending",
          "context" => { "before" => true }
        )

        expect(store.signal_event("approval:#{workflow_id}", payload: { "approved" => true })).to eq(1)
        expect(store.signal_event("approval:#{workflow_id}", payload: { "approved" => false })).to eq(0)

        expect(store.workflow(workflow_id)).to include("status" => "pending")
        expect(store.steps_for(workflow_id).first).to include(
          "status" => "completed",
          "result" => { "before" => true, "approved" => true }
        )
        expect(store.waits_for(workflow_id).first).to include(
          "status" => "completed",
          "payload" => { "approved" => true }
        )
      end

      it "persists waits and wakes due timers once" do
        store.migrate!
        workflow_id = store.create_workflow(name: "timer", input: {})
        wait_id = store.record_wait(
          workflow_id:,
          position: 0,
          name: "sleep",
          wait_request: Durababble.wait_until(Time.utc(2026, 1, 1, 0, 0, 0), { "timer" => true })
        )

        expect(store.wake_due_timers(now: Time.utc(2025, 12, 31, 23, 59, 59))).to eq(0)
        expect(store.waits_for(workflow_id).first).to include("id" => wait_id, "status" => "pending")

        expect(store.wake_due_timers(now: Time.utc(2026, 1, 1, 0, 0, 1))).to eq(1)
        expect(store.wake_due_timers(now: Time.utc(2026, 1, 1, 0, 0, 2))).to eq(0)
        expect(store.workflow(workflow_id)).to include("status" => "pending")
        expect(store.steps_for(workflow_id).first).to include("status" => "completed", "result" => { "timer" => true })
      end

      it "deduplicates fenced work and replays the first completed result" do
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

        expect(first).to eq({ "charged" => true })
        expect(second).to eq({ "charged" => true })
        expect(calls).to eq(1)
      end

      it "persists durable object state and command lifecycle payloads" do
        store.migrate!

        expect(store.object_state(object_type: "counter", object_id: "abc")).to be_nil
        expect(store.save_object_state(object_type: "counter", object_id: "abc", state: { "count" => 1 })).to eq({ "count" => 1 })
        expect(store.object_state(object_type: "counter", object_id: "abc")).to eq({ "count" => 1 })

        command_id = store.enqueue_object_command(
          object_type: "counter",
          object_id: "abc",
          method_name: "increment",
          args: [1],
          kwargs: { "by" => 2 }
        )
        claimed = store.claim_object_command(command_id:, worker_id: "object-worker", lease_seconds: 30)
        expect(claimed).to include(
          "id" => command_id,
          "object_type" => "counter",
          "object_id" => "abc",
          "method_name" => "increment",
          "args" => [1],
          "kwargs" => { "by" => 2 },
          "status" => "running",
          "locked_by" => "object-worker"
        )

        store.complete_object_command(command_id:, result: { "count" => 3 })
        expect(store.claim_object_command(command_id:, worker_id: "object-worker", lease_seconds: 30)).to be_nil
      end

      it "supports lease, heartbeat, retry, failure, and release lifecycle operations" do
        store.migrate!
        workflow_id = store.enqueue_workflow(name: "lifecycle", input: { "start" => true })

        expect(store.claim_workflow(workflow_id:, worker_id: "worker-a", lease_seconds: 30)).to include(
          "id" => workflow_id,
          "status" => "running",
          "locked_by" => "worker-a"
        )
        expect(store.workflow_owned?(workflow_id:, worker_id: "worker-a")).to be(true)
        expect(store.workflow_owned?(workflow_id:, worker_id: "worker-b")).to be(false)
        expect(store.current_workflow_lease(workflow_id)).to include("workflow_id" => workflow_id, "worker_id" => "worker-a")
        expect(store.heartbeat(workflow_id:, worker_id: "worker-a", lease_seconds: 30).cmd_tuples).to eq(1)

        store.record_step_started(workflow_id:, position: 0, name: "heartbeat")
        expect(store.heartbeat_step(workflow_id:, position: 0, worker_id: "worker-a", lease_seconds: 30, cursor: { "offset" => 10 })).not_to be_nil
        expect(store.step_heartbeat_cursor(workflow_id:, position: 0)).to eq({ "offset" => 10 })
        store.record_step_failed(workflow_id:, position: 0, error: "boom")
        expect(store.steps_for(workflow_id).first).to include("status" => "failed", "error" => "boom")

        run_at = Time.now + 60
        store.schedule_workflow_retry(workflow_id:, worker_id: "worker-a", run_at:)
        expect(store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 30)).to be_nil
        store.make_workflow_due!(workflow_id, now: Time.now)
        expect(store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 30)).to include("id" => workflow_id, "locked_by" => "worker-b")

        expect(store.release_worker_leases!(worker_id: "worker-b")).to include("workflows" => 1)
        expect(store.claim_workflow(workflow_id:, worker_id: "worker-c", lease_seconds: 30)).to include("locked_by" => "worker-c")
        store.fail_workflow(workflow_id, error: "fatal")
        expect(store.workflow(workflow_id)).to include("status" => "failed", "error" => "fatal")
        expect(store.steal_expired_leases!(now: Time.now)).to eq(0)
      end
    end
  end
end
