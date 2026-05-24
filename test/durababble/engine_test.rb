# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleEngineTest < DurababbleTestCase
  test "allows lease-free assertions and requested injected crash points" do
    no_lease_store = Object.new
    engine = Durababble::Engine.new(store: no_lease_store, migrate: false)
    assert_nil engine.send(:assert_workflow_lease!, "wf")

    crashy_engine = Durababble::Engine.new(store: no_lease_store, migrate: false, crash_after: :workflow_completed)
    assert_raises(Durababble::InjectedCrash) { crashy_engine.send(:crash!, :workflow_completed) }
  end

  durababble_store_backends.each do |backend|
    test "runs a workflow once and records durable step outputs with #{backend.name}" do
      with_durababble_store(backend, "engine_test") do |store|
        workflow = durababble_test_workflow("counter") do
          test_step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
          test_step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
        end

        engine = Durababble::Engine.new(store:)
        run = engine.run(workflow, input: { "count" => 2 })

        assert_equal "completed", run.status
        assert_equal({ "count" => 6 }, run.result)
        assert_equal(
          [
            ["increment", "completed"],
            ["double", "completed"],
          ],
          store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )
      end
    end

    test "can resume a due retry without rerunning completed steps with #{backend.name}" do
      with_durababble_store(backend, "engine_test") do |store|
        first_step_runs = 0
        attempts = 0
        workflow = durababble_test_workflow("flaky") do
          test_step("first") do |ctx|
            first_step_runs += 1
            { "count" => ctx.fetch("count") + 1 }
          end
          test_step("flaky", retry_policy: { initial_interval: 1, maximum_attempts: 2 }) do |ctx|
            attempts += 1
            raise "boom" if attempts == 1

            { "count" => ctx.fetch("count") + 10 }
          end
        end

        engine = Durababble::Engine.new(store:)
        scheduled = engine.run(workflow, input: { "count" => 1 })
        assert_equal "pending", scheduled.status
        refute_nil store.workflow(scheduled.id).fetch("next_run_at")

        store.make_workflow_due!(scheduled.id, now: Time.now + 2)
        resumed = engine.resume(workflow, workflow_id: scheduled.id)

        assert_equal "completed", resumed.status
        assert_equal({ "count" => 12 }, resumed.result)
        assert_equal 1, first_step_runs
        assert_equal 2, attempts
        assert_equal ["completed", "completed"], store.steps_for(resumed.id).map { |step| step.fetch("status") }
      end
    end

    test "replays a large completed step prefix without rerunning side effects with #{backend.name}" do
      with_durababble_store(backend, "engine_test") do |store|
        side_effect_count = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "large-history-replay"

          define_method(:execute) do |input|
            ctx = input
            input.fetch("iterations").times do
              ctx = accumulate(ctx)
            end
            finish(wait_event("large-history:#{ctx.fetch("id")}", ctx))
          end

          define_method(:accumulate) do |ctx|
            side_effect_count += 1
            ctx.merge("count" => ctx.fetch("count") + 1)
          end

          define_method(:finish) do |ctx|
            ctx.merge("finished" => true)
          end

          step :accumulate
          step :finish
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "large-history-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: workflow.name,
          input: { "id" => "history", "count" => 0, "iterations" => 75 },
        )

        assert_equal :worked, worker.tick
        assert_equal "waiting", store.workflow(workflow_id).fetch("status")
        assert_equal 75, side_effect_count
        assert_equal 75, store.steps_for(workflow_id).length
        assert_equal(
          75,
          store.steps_for(workflow_id).count do |step|
            step.fetch("name") == "accumulate" && step.fetch("status") == "completed"
          end,
        )

        assert_equal 1, store.signal_event("large-history:history", payload: { "released" => true })
        assert_equal :worked, worker.tick

        assert_hash_includes(
          store.workflow(workflow_id),
          "status" => "completed",
          "result" => {
            "id" => "history",
            "count" => 75,
            "iterations" => 75,
            "released" => true,
            "finished" => true,
          },
        )
        assert_equal 75, side_effect_count
        assert_equal 76, store.steps_for(workflow_id).length
        assert_equal ["completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }.uniq
      end
    end
  end
end
