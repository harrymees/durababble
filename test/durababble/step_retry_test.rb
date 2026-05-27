# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleStepRetryTest < DurababbleTestCase
  class ReplaySemanticStepError < StandardError; end

  class ReplayKeywordStepError < StandardError
    def initialize(required:)
      super(required)
    end
  end

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

    test "persists failed step retry scheduling before crash recovery with #{backend.name}" do
      with_durababble_store(backend, "step_retry_failure_crash") do |store|
        attempts = 0
        workflow = durababble_test_workflow("retry-failure-crash") do
          test_step("flaky", retry_policy: { initial_interval: 60, maximum_attempts: 2 }) do |ctx|
            attempts += 1
            raise "temporary" if attempts == 1

            ctx.merge("attempts" => attempts)
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(
            store:,
            worker_id: "crashy-retry",
            crash_after: :step_failed_recorded,
          ).resume(workflow, workflow_id:)
        end

        scheduled = store.workflow(workflow_id)
        assert_hash_includes scheduled, "status" => "pending", "locked_by" => nil
        refute_nil scheduled.fetch("next_run_at")
        assert_nil store.claim_runnable_workflow(worker_id: "too-early", lease_seconds: 30)

        store.make_workflow_due!(workflow_id, now: Time.now + 61)
        recovered = Durababble::Engine.new(store:, worker_id: "recovery").resume(workflow, workflow_id:)

        assert_equal "completed", recovered.status
        assert_equal({ "attempts" => 2 }, recovered.result)
        assert_equal ["failed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
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

    test "replays a persisted final step failure after crash recovery with #{backend.name}" do
      with_durababble_store(backend, "step_retry_final_failure_crash") do |store|
        attempts = 0
        workflow = durababble_test_workflow("retry-final-failure-crash") do
          test_step("always-fails", retry_policy: { maximum_attempts: 1 }) do |_ctx|
            attempts += 1
            raise ArgumentError, "still bad"
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(
            store:,
            worker_id: "crashy-final",
            crash_after: :step_failed_recorded,
          ).resume(workflow, workflow_id:)
        end

        crashed = store.workflow(workflow_id)
        assert_equal "running", crashed.fetch("status")
        assert_equal "crashy-final", crashed.fetch("locked_by")
        assert_equal ["failed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
        assert_nil store.claim_runnable_workflow(worker_id: "late-worker", lease_seconds: 30)

        assert_equal 1, store.steal_expired_leases!(now: Time.now + 61)
        recovered = Durababble::Engine.new(store:, worker_id: "recovery").resume(workflow, workflow_id:)

        assert_equal "failed", recovered.status
        assert_includes recovered.error, "ArgumentError: still bad"
        assert_equal 1, attempts
        assert_nil store.workflow(workflow_id).fetch("locked_by")
        assert_equal ["failed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "replays terminal step failures with the original exception semantics after crash recovery with #{backend.name}" do
      with_durababble_store(backend, "step_retry_terminal_failure_semantics") do |store|
        attempts = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "terminal-failure-semantics"

          define_method(:execute) do |input|
            fail_once(input)
          rescue ReplaySemanticStepError => e
            {
              "rescued" => true,
              "error_class" => e.class.name,
              "error_message" => e.message,
            }
          end

          define_method(:fail_once) do |_input|
            attempts += 1
            raise ReplaySemanticStepError, "semantic boom"
          end
          step :fail_once, retry: { maximum_attempts: 1 }
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})

        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(
            store:,
            worker_id: "semantic-crash",
            crash_after: :step_failed_recorded,
          ).resume(workflow, workflow_id:)
        end
        assert_equal 1, attempts

        assert_equal 1, store.steal_expired_leases!(now: Time.now + 61)
        recovered = Durababble::Engine.new(store:, worker_id: "semantic-recovery").resume(workflow, workflow_id:)

        assert_equal "completed", recovered.status
        assert_equal(
          {
            "rescued" => true,
            "error_class" => "DurababbleStepRetryTest::ReplaySemanticStepError",
            "error_message" => "semantic boom",
          },
          recovered.result,
        )
        assert_equal 1, attempts
        assert_equal ["failed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
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

  test "reconstructs terminal step failure replay errors from persisted payloads" do
    error = step_failure_error_from_replay(
      "error" => "DurababbleStepRetryTest::ReplaySemanticStepError: payload boom",
      "payload" => {
        "terminal" => true,
        "error_class" => "DurababbleStepRetryTest::ReplaySemanticStepError",
        "error_message" => "payload boom",
      },
    )

    assert_instance_of(ReplaySemanticStepError, error)
    assert_equal("payload boom", error.message)
  end

  test "reconstructs terminal step failure replay errors from legacy formatted errors" do
    error = step_failure_error_from_replay(
      "error" => "DurababbleStepRetryTest::ReplaySemanticStepError: legacy boom",
      "payload" => { "terminal" => true },
    )

    assert_instance_of(ReplaySemanticStepError, error)
    assert_equal("legacy boom", error.message)
  end

  test "falls back safely when terminal step failure replay cannot rebuild the original error" do
    plain = step_failure_error_from_replay("error" => "plain boom")
    assert_instance_of(Durababble::Error, plain)
    assert_equal("plain boom", plain.message)

    empty_class = step_failure_error_from_replay(
      "error" => "empty class",
      "payload" => {
        "terminal" => true,
        "error_class" => "",
        "error_message" => "empty",
      },
    )
    assert_instance_of(Durababble::Error, empty_class)
    assert_equal("empty class", empty_class.message)

    non_error_class = step_failure_error_from_replay(
      "error" => "String: not an error",
      "payload" => {
        "terminal" => true,
        "error_class" => "String",
        "error_message" => "not an error",
      },
    )
    assert_instance_of(Durababble::Error, non_error_class)
    assert_equal("String: not an error", non_error_class.message)

    missing_class = step_failure_error_from_replay(
      "error" => "MissingReplayError: missing",
      "payload" => {
        "terminal" => true,
        "error_class" => "MissingReplayError",
        "error_message" => "missing",
      },
    )
    assert_instance_of(Durababble::Error, missing_class)
    assert_equal("MissingReplayError: missing", missing_class.message)

    keyword_only = step_failure_error_from_replay(
      "error" => "DurababbleStepRetryTest::ReplayKeywordStepError: cannot",
      "payload" => {
        "terminal" => true,
        "error_class" => "DurababbleStepRetryTest::ReplayKeywordStepError",
        "error_message" => "cannot",
      },
    )
    assert_instance_of(Durababble::Error, keyword_only)
    assert_equal("DurababbleStepRetryTest::ReplayKeywordStepError: cannot", keyword_only.message)
  end

  test "normalizes retry policy edge cases" do
    existing = Durababble::RetryPolicy.new(maximum_attempts: nil, initial_interval: "2")

    assert_same existing, Durababble::RetryPolicy.from(existing)
    assert_equal false, Durababble::RetryPolicy.from(nil).retryable?(RuntimeError.new("boom"), attempt_number: 1)
    assert_equal true, existing.retryable?(RuntimeError.new("boom"), attempt_number: 10_000)
    assert_equal 4.0, existing.delay_for_attempt(2)
    assert_equal false, Durababble::RetryPolicy.new(maximum_attempts: 2, non_retryable_errors: ["RuntimeError"]).retryable?(RuntimeError.new("boom"), attempt_number: 1)
    assert_raises(ArgumentError) { Durababble::RetryPolicy.new(initial_interval: Object.new) }
  end

  private

  def step_failure_error_from_replay(event)
    Durababble::WorkflowExecution.allocate.send(:step_failure_error_from, event)
  end
end
