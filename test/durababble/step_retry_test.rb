# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleStepRetryTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "retries a failed step according to a Ruby-ified Temporal exponential policy with #{backend.name}" do
      with_durababble_store(backend, "step_retry_test") do |store|
        attempts = 0
        workflow = durababble_test_workflow("retry-exponential") do
          test_step(
            "flaky",
            retry_policy: {
              initial_interval: 2,
              backoff_coefficient: 3,
              maximum_interval: 5,
              maximum_attempts: 4,
            },
          ) do |ctx|
            attempts += 1
            raise "boom #{attempts}" if attempts < 4

            ctx.merge("attempts" => attempts)
          end
        end

        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "retry-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

        assert_equal :worked, worker.tick
        assert_hash_includes store.workflow(workflow_id), "status" => "pending", "locked_by" => nil
        refute_nil store.workflow(workflow_id).fetch("next_run_at")
        assert_equal :idle, worker.tick

        store.make_workflow_due!(workflow_id, now: Time.now + 3)
        assert_equal :worked, worker.tick
        store.make_workflow_due!(workflow_id, now: Time.now + 7)
        assert_equal :worked, worker.tick
        store.make_workflow_due!(workflow_id, now: Time.now + 10)
        assert_equal :worked, worker.tick

        assert_hash_includes store.workflow(workflow_id), "status" => "completed", "result" => { "attempts" => 4 }
        assert_equal ["failed", "failed", "failed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "persists retry schedule across lease expiry and process restart with #{backend.name}" do
      with_durababble_store(backend, "step_retry_test") do |store|
        attempts = 0
        workflow = durababble_test_workflow("retry-restart") do
          test_step("flaky", retry_policy: { initial_interval: 5, maximum_attempts: 2 }) do |ctx|
            attempts += 1
            raise "temporary" if attempts == 1

            ctx.merge("recovered" => true)
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

        first_worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "first",
          lease_seconds: 60,
          migrate: false,
        )
        assert_equal :worked, first_worker.tick
        scheduled = store.workflow(workflow_id)
        assert_hash_includes scheduled, "status" => "pending", "locked_by" => nil
        refute_nil scheduled.fetch("next_run_at")

        restarted_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
        begin
          restarted_worker = Durababble::Worker.new(
            store: restarted_store,
            workflows: { workflow.name => workflow },
            worker_id: "second",
            lease_seconds: 60,
            migrate: false,
          )
          assert_equal(:idle, restarted_worker.tick)
          restarted_store.make_workflow_due!(workflow_id, now: Time.now + 6)
          assert_equal(:worked, restarted_worker.tick)
        ensure
          restarted_store.close
        end

        assert_hash_includes store.workflow(workflow_id), "status" => "completed", "result" => { "recovered" => true }
      end
    end

    test "bubbles the final failure to the workflow when attempts are exhausted with #{backend.name}" do
      with_durababble_store(backend, "step_retry_test") do |store|
        workflow = durababble_test_workflow("retry-exhausted") do
          test_step("always-fails", retry_policy: { initial_interval: 1, maximum_attempts: 2 }) do |_ctx|
            raise ArgumentError, "still bad"
          end
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "retry-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

        assert_equal :worked, worker.tick
        store.make_workflow_due!(workflow_id, now: Time.now + 2)
        assert_equal :worked, worker.tick

        failed = store.workflow(workflow_id)
        assert_equal "failed", failed.fetch("status")
        assert_includes failed.fetch("error"), "ArgumentError: still bad"
        assert_nil failed.fetch("next_run_at")
        assert_equal ["failed", "failed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "does not retry non-retryable errors with #{backend.name}" do
      with_durababble_store(backend, "step_retry_test") do |store|
        workflow = durababble_test_workflow("retry-nonretryable") do
          test_step("bad-input", retry_policy: { maximum_attempts: 5, non_retryable_errors: [ArgumentError] }) do |_ctx|
            raise ArgumentError, "invalid"
          end
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "retry-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

        assert_equal :worked, worker.tick

        failed = store.workflow(workflow_id)
        assert_equal "failed", failed.fetch("status")
        assert_equal 1, store.step_attempts_for(workflow_id).length
      end
    end
  end

  test "supports an explicit retry schedule array before falling back to capped exponential backoff" do
    policy = Durababble::RetryPolicy.new(
      initial_interval: 10,
      backoff_coefficient: 2,
      maximum_interval: 30,
      maximum_attempts: 5,
      schedule: [1, 4],
    )

    assert_equal [1, 4, 30, 30], (1..4).map { |attempt| policy.delay_for_attempt(attempt) }
  end

  test "normalizes retry policy edge cases" do
    existing = Durababble::RetryPolicy.new(maximum_attempts: nil, initial_interval: "2")

    assert_same existing, Durababble::RetryPolicy.from(existing)
    assert_equal false, Durababble::RetryPolicy.from(nil).retryable?(RuntimeError.new("boom"), attempt_number: 1)
    assert_equal true, existing.retryable?(RuntimeError.new("boom"), attempt_number: 10_000)
    assert_equal 4.0, existing.delay_for_attempt(2)
    assert_raises(ArgumentError) { Durababble::RetryPolicy.new(initial_interval: Object.new) }
  end
end
