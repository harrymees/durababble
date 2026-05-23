# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleHatchetInspiredTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "rejects replay when completed step history no longer matches workflow code with #{backend.name}" do
      with_durababble_store(backend, "hatchet_inspired") do |store|
        store.migrate!
        first_version = Class.new(Durababble::Workflow) do
          workflow_name "shape-check"

          def execute(input)
            old_step(input)
          end

          step def old_step(input)
            input.merge("from" => "old")
          end
        end
        second_version = Class.new(Durababble::Workflow) do
          workflow_name "shape-check"

          def execute(input)
            new_step(input)
          end

          step def new_step(input)
            input.merge("from" => "new")
          end
        end
        workflow_id = store.enqueue_workflow(name: first_version.workflow_name, input: { "n" => 1 })

        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(
            store:,
            worker_id: "first-version",
            crash_after: :step_completed,
            migrate: false,
          ).resume(first_version, workflow_id:)
        end
        store.steal_expired_leases!(now: Time.now + 120)

        run = Durababble::Engine.new(
          store:,
          worker_id: "second-version",
          migrate: false,
        ).resume(second_version, workflow_id:)

        assert_equal "failed", run.status
        assert_match(/NonDeterminismError/, run.error)
        assert_match(/new_step/, run.error)
        assert_match(/old_step/, run.error)
        assert_hash_includes store.steps_for(workflow_id).first, "name" => "old_step", "status" => "completed"
        assert_equal ["completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "runs a durable timer wait followed by an event wait with #{backend.name}" do
      with_durababble_store(backend, "hatchet_inspired") do |store|
        store.migrate!
        wake_at = Time.utc(2026, 1, 1, 0, 0, 0)
        workflow = durababble_test_workflow("sleep-then-event") do
          test_step("sleep") do |ctx|
            Durababble.wait_until(wake_at, ctx.merge("slept" => true))
          end

          test_step("wait_for_event") do |ctx|
            Durababble.wait_event("approval:#{ctx.fetch("id")}", ctx)
          end

          test_step("finish") do |ctx|
            ctx.merge("done" => true)
          end
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "wait-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.name, input: { "id" => "hatchet" })

        assert_equal :worked, worker.tick
        assert_hash_includes store.workflow(workflow_id), "status" => "waiting"
        assert_hash_includes store.waits_for(workflow_id).first, "kind" => "timer", "status" => "pending"

        assert_equal 0, store.wake_due_timers(now: Time.utc(2025, 12, 31, 23, 59, 59))
        assert_equal 1, store.wake_due_timers(now: Time.utc(2026, 1, 1, 0, 0, 1))
        assert_equal :worked, worker.tick
        assert_hash_includes store.workflow(workflow_id), "status" => "waiting"
        assert_equal ["completed", "pending"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }

        assert_equal 1, store.signal_event("approval:hatchet", payload: { "approved" => true })
        assert_equal :worked, worker.tick

        assert_hash_includes(
          store.workflow(workflow_id),
          "status" => "completed",
          "result" => { "id" => "hatchet", "slept" => true, "approved" => true, "done" => true },
        )
        assert_equal ["completed", "completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
        assert_equal(
          ["completed", "completed", "completed"],
          store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") },
        )
      end
    end
  end
end
