# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababblePublicWorkflowApiTest < DurababbleTestCase
  ImportedIncrementStep = Durababble.step("external_increment") do |input|
    input.merge(
      "count" => input.fetch("count") + 1,
      "key" => step_context.idempotency_key,
      "module_key" => Durababble.step_context.idempotency_key,
    )
  end

  ImportedDoubleStep = Durababble.step("external_double") do |input|
    input.merge("count" => input.fetch("count") * 2)
  end

  ImportedReplayCounts = Hash.new(0)
  ImportedReplayStep = Durababble.step("external_replay_once") do |input|
    id = input.fetch("id")
    ImportedReplayCounts[id] += 1
    input.merge("runs" => ImportedReplayCounts.fetch(id))
  end

  ImportedRetryAttempts = Hash.new(0)
  ImportedFlakyStep = Durababble.step(
    "external_flaky",
    retry: { initial_interval: 1, maximum_attempts: 2 },
  ) do |input|
    id = input.fetch("id")
    ImportedRetryAttempts[id] += 1
    raise "temporary #{id}" if ImportedRetryAttempts.fetch(id) == 1

    input.merge("attempts" => ImportedRetryAttempts.fetch(id))
  end

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

  class PublicApiImportedStepsWorkflow < Durababble::Workflow
    def execute(input)
      ImportedDoubleStep.call(ImportedIncrementStep.call(input))
    end
  end

  class PublicApiImportedReplayWorkflow < Durababble::Workflow
    def execute(input)
      ImportedDoubleStep.call(ImportedReplayStep.call(input))
    end
  end

  class PublicApiImportedRetryWorkflow < Durababble::Workflow
    def execute(input)
      ImportedFlakyStep.call(input)
    end
  end

  test "runs simple-looking workflow code and records method-derived durable steps" do
    backend = durababble_store_backends.first
    with_durababble_store(backend, "public_workflow_api") do |store|
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
      assert_respond_to PublicApiCounterWorkflow.handle(run.id, store:), :terminate
    end
  end

  test "runs imported reusable step constants through durable step execution" do
    backend = durababble_store_backends.first
    with_durababble_store(backend, "public_workflow_imported_steps") do |store|
      run = Durababble::Engine.new(store:).run(PublicApiImportedStepsWorkflow, input: { "count" => 2 })

      assert_equal "completed", run.status
      assert_equal 6, run.result.fetch("count")
      assert_equal "durababble:v1:workflow:#{run.id}:step:0", run.result.fetch("key")
      assert_equal run.result.fetch("key"), run.result.fetch("module_key")
      assert_equal(
        [
          ["external_increment", "completed"],
          ["external_double", "completed"],
        ],
        store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] },
      )
    end
  end

  test "replays completed imported step constants without rerunning the body" do
    backend = durababble_store_backends.first
    with_durababble_store(backend, "public_workflow_imported_replay") do |store|
      workflow_id = store.enqueue_workflow(
        name: PublicApiImportedReplayWorkflow.workflow_name,
        input: { "id" => "external-replay", "count" => 2 },
      )
      ImportedReplayCounts.clear

      assert_raises(Durababble::InjectedCrash) do
        Durababble::Engine.new(store:, worker_id: "crashy", crash_after: :step_completed)
          .resume(PublicApiImportedReplayWorkflow, workflow_id:)
      end

      assert_equal 1, store.steal_expired_leases!(now: Time.now + 61)
      recovered = Durababble::Engine.new(store:, worker_id: "recovery").resume(PublicApiImportedReplayWorkflow, workflow_id:)

      assert_equal "completed", recovered.status
      assert_equal 4, recovered.result.fetch("count")
      assert_equal 1, recovered.result.fetch("runs")
      assert_equal 1, ImportedReplayCounts.fetch("external-replay")
      assert_equal(
        [
          ["external_replay_once", "completed"],
          ["external_double", "completed"],
        ],
        store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] },
      )
    end
  end

  test "uses retry metadata from imported step constants" do
    backend = durababble_store_backends.first
    with_durababble_store(backend, "public_workflow_imported_retry") do |store|
      workflow_id = store.enqueue_workflow(
        name: PublicApiImportedRetryWorkflow.workflow_name,
        input: { "id" => "external-retry" },
      )
      ImportedRetryAttempts.clear
      worker = Durababble::Worker.new(
        store:,
        workflows: { PublicApiImportedRetryWorkflow.workflow_name => PublicApiImportedRetryWorkflow },
        worker_id: "imported-retry-worker",
        migrate: false,
      )

      assert_equal :worked, worker.tick
      assert_hash_includes store.workflow(workflow_id), "status" => "pending", "locked_by" => nil
      refute_nil store.workflow(workflow_id).fetch("next_run_at")

      store.make_workflow_due!(workflow_id, now: Time.now + 2)
      assert_equal :worked, worker.tick

      assert_hash_includes store.workflow(workflow_id), "status" => "completed", "result" => { "id" => "external-retry", "attempts" => 2 }
      assert_equal ["failed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
    end
  end
end
