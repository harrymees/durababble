# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababblePublicWorkflowApiTest < DurababbleTestCase
  class PublicApiCounterWorkflow < Durababble::Workflow
    def execute(input)
      incremented = increment(input)
      double(incremented)
    end

    step def increment(input)
      input.merge("count" => input.fetch("count") + 1, "key" => step_context.idempotency_key)
    end

    step def double(input)
      input.merge("count" => input.fetch("count") * 2)
    end
  end

  test "runs simple-looking workflow code and records method-derived durable steps" do
    backend = durababble_store_backends.first
    with_durababble_store(backend, "public_workflow_api") do |store|
      store.migrate!

      run = Durababble::Engine.new(store:).run(PublicApiCounterWorkflow, input: { "count" => 2 })

      assert_equal "completed", run.status
      assert_equal 6, run.result.fetch("count")
      assert_equal "durababble:v1:workflow:#{run.id}:step:0", run.result.fetch("key")
      assert_equal(
        [
          ["increment", "completed"],
          ["double", "completed"],
        ],
        store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] },
      )
    end
  end
end
