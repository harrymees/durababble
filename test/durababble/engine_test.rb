# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleEngineTest < DurababbleTestCase
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

    test "can resume a previously failed workflow without rerunning completed steps with #{backend.name}" do
      with_durababble_store(backend, "engine_test") do |store|
        store.migrate!
        attempts = 0
        workflow = durababble_test_workflow("flaky") do
          test_step("first") { |ctx| { "count" => ctx.fetch("count") + 1 } }
          test_step("flaky") do |ctx|
            attempts += 1
            raise "boom" if attempts == 1

            { "count" => ctx.fetch("count") + 10 }
          end
        end

        engine = Durababble::Engine.new(store:)
        failed = engine.run(workflow, input: { "count" => 1 })
        assert_equal "failed", failed.status

        resumed = engine.resume(workflow, workflow_id: failed.id)

        assert_equal "completed", resumed.status
        assert_equal({ "count" => 12 }, resumed.result)
        assert_equal 2, attempts
        assert_equal ["completed", "completed"], store.steps_for(resumed.id).map { |step| step.fetch("status") }
      end
    end
  end
end
