# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleHatchetInspiredTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "rejects replay when completed step history no longer matches workflow code with #{backend.name}" do
      with_durababble_store(backend, "hatchet_inspired") do |store|
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
        assert_match(/different durable command shape/, run.error)
        assert_hash_includes store.steps_for(workflow_id).first, "name" => "old_step", "status" => "completed"
        assert_equal ["completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "rejects replay when current workflow stops before completed history is consumed with #{backend.name}" do
      with_durababble_store(backend, "hatchet_inspired") do |store|
        first_version = Class.new(Durababble::Workflow) do
          workflow_name "suffix-shape-check"

          def execute(input)
            wait_for_release(first_step(input))
          end

          step def first_step(input)
            input.merge("first" => true)
          end

          step def wait_for_release(input)
            Durababble.wait_until(Time.now + 3600, input.merge("waiting" => true, "released" => true))
          end
        end
        second_version = Class.new(Durababble::Workflow) do
          workflow_name "suffix-shape-check"

          def execute(input)
            first_step(input)
          end

          step def first_step(input)
            input.merge("first" => "new")
          end
        end
        workflow_id = store.enqueue_workflow(name: first_version.workflow_name, input: { "id" => "suffix" })

        waiting = Durababble::Engine.new(store:, worker_id: "first-version", migrate: false).resume(first_version, workflow_id:)
        assert_equal "waiting", waiting.status
        assert_equal 1, store.wake_due_timers(now: Time.now + 3601)

        run = Durababble::Engine.new(store:, worker_id: "second-version", migrate: false).resume(second_version, workflow_id:)

        assert_equal "failed", run.status
        assert_match(/NonDeterminismError/, run.error)
        assert_match(/without consuming durable command history/, run.error)
        assert_match(/1:wait_for_release/, run.error)
        assert_equal(
          [["0", "first_step", "completed"], ["1", "wait_for_release", "completed"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("position").to_s, step.fetch("name"), step.fetch("status")] },
        )
      end
    end

    test "rejects replay when completed steps are reordered by new workflow code with #{backend.name}" do
      with_durababble_store(backend, "hatchet_inspired") do |store|
        first_version = Class.new(Durababble::Workflow) do
          workflow_name "order-shape-check"

          def execute(input)
            second(first(input))
          end

          step def first(input)
            input.merge("first" => true)
          end

          step def second(input)
            input.merge("second" => true)
          end
        end
        reordered_version = Class.new(Durababble::Workflow) do
          workflow_name "order-shape-check"

          def execute(input)
            first(second(input))
          end

          step def second(input)
            input.merge("second" => "new")
          end

          step def first(input)
            input.merge("first" => "new")
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
          worker_id: "reordered-version",
          migrate: false,
        ).resume(reordered_version, workflow_id:)

        assert_equal "failed", run.status
        assert_match(/NonDeterminismError/, run.error)
        assert_match(/second/, run.error)
        assert_match(/different durable command shape/, run.error)
      end
    end

    test "runs sequential durable timer waits with #{backend.name}" do
      with_durababble_store(backend, "hatchet_inspired") do |store|
        wake_at = Time.utc(2026, 1, 1, 0, 0, 0)
        workflow = durababble_test_workflow("sleep-then-sleep") do
          test_step("sleep") do |ctx|
            Durababble.wait_until(wake_at, ctx.merge("slept" => true))
          end

          test_step("sleep_again") do |ctx|
            Durababble.wait_until(Time.now + 3600, ctx.merge("approved" => true))
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

        assert_equal 1, store.wake_due_timers(now: Time.now + 3601)
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

    test "reuses an idempotent outbox child effect across a retried step with #{backend.name}" do
      with_durababble_store(backend, "hatchet_inspired") do |store|
        attempts = 0
        outbox_ids = []
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "outbox-child-cache"

          define_method(:execute) do |input|
            enqueue_child(input)
          end

          define_method(:enqueue_child) do |input|
            attempts += 1
            outbox_id = store.enqueue_outbox(
              workflow_id: step_context.workflow_id,
              topic: "child-work",
              payload: { "attempt" => attempts },
              key: "child:#{input.fetch("id")}",
            )
            outbox_ids << outbox_id
            raise "transient after child enqueue" if attempts == 1

            input.merge("outbox_id" => outbox_id, "attempts" => attempts)
          end

          step :enqueue_child, retry: { schedule: [0], maximum_attempts: 2 }
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "outbox-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.name, input: { "id" => "child-1" })

        assert_equal :worked, worker.tick
        assert_equal "pending", store.workflow(workflow_id).fetch("status")
        store.make_workflow_due!(workflow_id, now: Time.now + 1)
        assert_equal :worked, worker.tick

        outbox_id = outbox_ids.first
        assert_equal [outbox_id], outbox_ids.uniq
        assert_hash_includes store.workflow(workflow_id), "status" => "completed", "result" => { "id" => "child-1", "outbox_id" => outbox_id, "attempts" => 2 }
        assert_hash_includes store.outbox_message(outbox_id), "payload" => { "attempt" => 1 }, "status" => "pending"
        assert_equal outbox_id, store.claim_outbox(worker_id: "sender", lease_seconds: 60).fetch("id")
        assert_nil store.claim_outbox(worker_id: "other", lease_seconds: 60)
      end
    end
  end
end
