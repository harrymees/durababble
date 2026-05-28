# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleDurableWaitRecoveryTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "does not recreate a timer wait after crashing immediately after persistence with #{backend.name}" do
      with_durababble_store(backend, "durable_wait_recovery") do |store|
        wake_at = Time.now + 3600
        workflow = durababble_test_workflow("durable-timer-checkpoint") do
          test_step("sleep") do |ctx|
            Durababble.wait_until(wake_at, ctx.merge("slept" => true))
          end

          test_step("finish") do |ctx|
            ctx.merge("done" => true)
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.name, input: { "id" => "timer-crash" })

        # Inspired by Absurd's sleep/checkpoint regression tests: a durable sleep
        # checkpoint must survive a crash without scheduling another copy.
        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(
            store:,
            worker_id: "crasher",
            crash_after: :wait_recorded,
          ).resume(workflow, workflow_id:)
        end
        assert_hash_includes store.workflow(workflow_id), "status" => "waiting", "locked_by" => nil
        assert_equal ["pending"], store.wait_snapshots_for(workflow_id).map { |wait| wait.fetch("status") }

        run = resume_waiting_workflow(store, workflow, workflow_id, worker_id: "recover")

        assert_equal "completed", run.status
        assert_equal({ "id" => "timer-crash", "slept" => true, "done" => true }, run.result)
        assert_equal 1, store.wait_snapshots_for(workflow_id).length
        assert_equal ["completed"], store.wait_snapshots_for(workflow_id).map { |wait| wait.fetch("status") }
        assert_equal ["completed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "keeps repeated durable waits from the same step method distinct by position with #{backend.name}" do
      with_durababble_store(backend, "durable_wait_recovery") do |store|
        first_wake = Time.now + 3600
        second_wake = Time.now + 7200
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "durable-repeated-waits"

          define_method(:execute) do |input|
            first = pause_until(input.merge("phase" => "first"), first_wake)
            second = pause_until(first.merge("phase" => "second"), second_wake)
            second.merge("done" => true)
          end

          define_method(:pause_until) do |ctx, wake_at|
            Durababble.wait_until(wake_at, ctx)
          end

          step :pause_until
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "repeat-wait-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.name, input: { "id" => "repeat" })

        # Absurd has explicit tests for repeated step/sleep names. Durababble's
        # equivalent contract is method/order step identity, so the same method
        # may appear more than once as separate durable positions.
        assert_equal :worked, worker.tick
        assert_equal(
          [["0", "pause_until", "waiting"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("position").to_s, step.fetch("name"), step.fetch("status")] },
        )

        make_workflow_timer_due(store, workflow_id, at: first_wake)
        assert_equal :worked, with_store_current_time(store, first_wake + 1) { worker.tick }
        assert_equal(
          [["0", "pause_until", "completed"], ["1", "pause_until", "waiting"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("position").to_s, step.fetch("name"), step.fetch("status")] },
        )

        make_workflow_timer_due(store, workflow_id, at: second_wake)
        assert_equal :worked, with_store_current_time(store, second_wake + 1) { worker.tick }

        assert_hash_includes(
          store.workflow(workflow_id),
          "status" => "completed",
          "result" => { "id" => "repeat", "phase" => "second", "done" => true },
        )
        assert_equal(
          [["0", "pause_until", "completed"], ["1", "pause_until", "completed"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("position").to_s, step.fetch("name"), step.fetch("status")] },
        )
        assert_equal ["completed", "completed"], store.wait_snapshots_for(workflow_id).map { |wait| wait.fetch("status") }
        assert_equal ["completed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end
  end
end
