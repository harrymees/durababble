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
        store.migrate!
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
        store.migrate!
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
  end
end
