# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleHeartbeatTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "extends the workflow lease and stores an opaque cursor during a running step with #{backend.name}" do
      with_durababble_store(backend, "heartbeat_test") do |store|
        test_store = store
        parse_lease_time = ->(value) { value.is_a?(Time) ? value : Time.parse(value) }
        observed = {}
        workflow = durababble_test_workflow("heartbeat-extension") do
          test_step("long-step") do |ctx, heartbeat|
            observed[:cursor_before] = heartbeat.cursor
            before = parse_lease_time.call(test_store.workflow(ctx.fetch("workflow_id")).fetch("locked_until"))
            heartbeat.record({ "offset" => 10 })
            after = parse_lease_time.call(test_store.workflow(ctx.fetch("workflow_id")).fetch("locked_until"))
            observed[:extended] = after > before
            { "done" => true }
          end
        end

        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})
        update_workflow_input(workflow_id, { "workflow_id" => workflow_id })

        run = described_engine(lease_seconds: 3_600).resume(workflow, workflow_id:)

        assert_equal "completed", run.status
        assert_equal({ "done" => true }, run.result)
        assert_hash_includes observed, cursor_before: nil, extended: true
        assert_equal({ "offset" => 10 }, store.steps_for(workflow_id).first.fetch("heartbeat_cursor"))
      end
    end

    test "passes the last heartbeat cursor into the next step invocation after lease expiry recovery with #{backend.name}" do
      with_durababble_store(backend, "heartbeat_test") do |store|
        attempts = []
        workflow = durababble_test_workflow("heartbeat-cursor-resume") do
          test_step("download") do |_ctx, heartbeat|
            attempts << heartbeat.cursor
            if attempts.length == 1
              heartbeat.record({ "page" => 42 })
              raise Durababble::InjectedCrash, "crash after heartbeat"
            end
            { "resumed_from" => heartbeat.cursor.fetch("page") }
          end
        end

        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})
        assert_raises(Durababble::InjectedCrash) do
          described_engine(lease_seconds: 1).resume(workflow, workflow_id:)
        end

        store.steal_expired_leases!(now: Time.now + 2)
        recovered = described_engine(worker_id: "recover", lease_seconds: 60).resume(workflow, workflow_id:)

        assert_equal "completed", recovered.status
        assert_equal({ "resumed_from" => 42 }, recovered.result)
        assert_equal [nil, { "page" => 42 }], attempts
      end
    end

    test "rejects a zombie heartbeat after the worker misses its lease deadline with #{backend.name}" do
      with_durababble_store(backend, "heartbeat_test") do |store|
        test_store = store
        expire_lease = ->(workflow_id) { expire_workflow_lease(workflow_id, test_store) }
        workflow = durababble_test_workflow("zombie-heartbeat") do
          test_step("work") do |ctx, heartbeat|
            expire_lease.call(ctx.fetch("workflow_id"))
            heartbeat.record({ "too_late" => true })
            { "should_not" => "complete" }
          end
        end

        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})
        update_workflow_input(workflow_id, { "workflow_id" => workflow_id })

        assert_raises_matching(Durababble::LeaseConflict, /expired or moved/) do
          described_engine(worker_id: "zombie", lease_seconds: 1).resume(workflow, workflow_id:)
        end

        row = store.workflow(workflow_id)
        assert_hash_includes row, "status" => "running", "locked_by" => "zombie"
        assert_operator parse_time(row.fetch("locked_until")), :<, Time.now
        assert_nil store.steps_for(workflow_id).first.fetch("heartbeat_cursor")
      end
    end
  end

  private

  def described_engine(worker_id: "owner", lease_seconds: 60)
    Durababble::Engine.new(store:, worker_id:, lease_seconds:)
  end

  def update_workflow_input(workflow_id, input)
    payload = store.send(:dump_serialized, input)
    if backend_descriptor.mysql?
      store.send(:execute_params, "UPDATE #{table("workflows")} SET input = ? WHERE id = ?", [payload, workflow_id])
    else
      store.send(:execute_params, "UPDATE #{table("workflows")} SET input = $2::bytea WHERE id = $1", [workflow_id, payload])
    end
  end

  def expire_workflow_lease(workflow_id, target_store = store)
    if backend_descriptor.mysql?
      expired_at = (Time.now - 3600).strftime("%Y-%m-%d %H:%M:%S.%6N")
      target_store.send(
        :execute_params,
        "UPDATE #{table("workflows")} SET locked_until = ? WHERE id = ?",
        [expired_at, workflow_id],
      )
    else
      target_store.send(
        :execute_params,
        "UPDATE #{table("workflows")} SET locked_until = now() - interval '1 hour' WHERE id = $1",
        [workflow_id],
      )
    end
  end

  def table(name)
    store.send(:table, name)
  end

  def parse_time(value)
    value.is_a?(Time) ? value : Time.parse(value)
  end
end
