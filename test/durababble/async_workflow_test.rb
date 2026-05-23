# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleAsyncWorkflowTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "fan out and fan in async steps in deterministic future order after out-of-order completion with #{backend.name}" do
      with_durababble_store(backend, "async_workflow_test") do |store|
        store.migrate!
        release_slow = Queue.new
        completions = Queue.new
        contexts = Queue.new
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async_order"

          define_method(:execute) do |input|
            slow_future = async { slow(input) }
            fast_future = async { fast(input) }

            { "results" => await_all([slow_future, fast_future]) }
          end

          step define_method(:slow) { |input|
            contexts << ["slow", step_context.step_index, step_context.idempotency_key]
            release_slow.pop
            input.merge("step" => "slow")
          }

          step define_method(:fast) { |input|
            contexts << ["fast", step_context.step_index, step_context.idempotency_key]
            completions << "fast"
            release_slow << true
            input.merge("step" => "fast")
          }
        end

        run = Durababble::Engine.new(store:).run(workflow, input: { "id" => 1 })

        assert_equal "completed", run.status
        assert_equal "fast", completions.pop
        assert_equal(
          [{ "id" => 1, "step" => "slow" }, { "id" => 1, "step" => "fast" }],
          run.result.fetch("results"),
        )
        assert_equal(
          [["slow", "completed"], ["fast", "completed"]],
          store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )
        observed_contexts = 2.times.map { contexts.pop }.sort_by { |name, _index, _key| name }
        assert_equal ["fast", 1, "durababble:v1:workflow:#{run.id}:step:1"], observed_contexts.fetch(0)
        assert_equal ["slow", 0, "durababble:v1:workflow:#{run.id}:step:0"], observed_contexts.fetch(1)
      end
    end

    test "persists completed sibling when one async step fails with #{backend.name}" do
      with_durababble_store(backend, "async_workflow_test") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async_partial_failure"

          def execute(input)
            await_all([
              async { ok(input) },
              async { boom(input) },
            ])
          end

          step def ok(input)
            input.merge("ok" => true)
          end

          step def boom(_input)
            raise ArgumentError, "bad async branch"
          end
        end

        run = Durababble::Engine.new(store:).run(workflow, input: {})

        assert_equal "failed", run.status
        assert_includes run.error, "ArgumentError: bad async branch"
        assert_equal(
          [["ok", "completed"], ["boom", "failed"]],
          store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )
      end
    end

    test "retries only the failed async step and skips completed siblings with #{backend.name}" do
      with_durababble_store(backend, "async_workflow_test") do |store|
        store.migrate!
        ok_calls = 0
        flaky_calls = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async_retry"

          define_method(:execute) do |input|
            values = await_all([
              async { ok(input) },
              async { flaky(input) },
            ])
            { "values" => values }
          end

          step define_method(:ok) { |input|
            ok_calls += 1
            input.merge("ok_calls" => ok_calls)
          }

          step retry: { initial_interval: 1, maximum_attempts: 2 }
          define_method(:flaky) do |input|
            flaky_calls += 1
            raise "retry me" if flaky_calls == 1

            input.merge("flaky_calls" => flaky_calls)
          end
        end

        worker = Durababble::Worker.new(store:, workflows: { workflow.name => workflow }, worker_id: "async-worker", migrate: false)
        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

        assert_equal :worked, worker.tick
        assert_hash_includes store.workflow(workflow_id), "status" => "pending", "locked_by" => nil
        assert_equal 1, ok_calls
        assert_equal 1, flaky_calls

        store.make_workflow_due!(workflow_id, now: Time.now + 2)
        assert_equal :worked, worker.tick

        completed = store.workflow(workflow_id)
        assert_hash_includes completed, "status" => "completed"
        assert_equal [{ "ok_calls" => 1 }, { "flaky_calls" => 2 }], completed.fetch("result").fetch("values")
        assert_equal 1, ok_calls
        assert_equal 2, flaky_calls
        assert_equal ["completed", "failed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "does not retry non-retryable async step failures with #{backend.name}" do
      with_durababble_store(backend, "async_workflow_test") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async_nonretryable"

          def execute(input)
            await_all([async { bad(input) }])
          end

          step retry: { maximum_attempts: 5, non_retryable_errors: [ArgumentError] }
          def bad(_input)
            raise ArgumentError, "invalid"
          end
        end

        run = Durababble::Engine.new(store:).run(workflow, input: {})

        assert_equal "failed", run.status
        assert_includes run.error, "ArgumentError: invalid"
        assert_equal 1, store.step_attempts_for(run.id).length
      end
    end

    test "persists cancellation for pending async work with #{backend.name}" do
      with_durababble_store(backend, "async_workflow_test") do |store|
        store.migrate!
        canceled_ran = false
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async_cancel"

          define_method(:execute) do |input|
            canceled = async { never(input) }
            canceled_result = canceled.cancel
            kept = async { ok(input) }

            { "canceled" => canceled_result, "values" => await_all(kept) }
          end

          step define_method(:never) { |_input|
            canceled_ran = true
            { "never" => true }
          }

          step def ok(input)
            input.merge("ok" => true)
          end
        end

        run = Durababble::Engine.new(store:).run(workflow, input: {})

        assert_equal "completed", run.status
        assert_equal({ "canceled" => true, "values" => [{ "ok" => true }] }, run.result)
        assert_equal false, canceled_ran
        assert_equal(
          [["async", "canceled"], ["ok", "completed"]],
          store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )
      end
    end

    test "fails workflows that start async work without awaiting it with #{backend.name}" do
      with_durababble_store(backend, "async_workflow_test") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async_unawaited"

          def execute(input)
            async { ok(input) }.start
            { "ignored" => true }
          end

          step def ok(input)
            input.merge("ok" => true)
          end
        end

        run = Durababble::Engine.new(store:).run(workflow, input: {})

        assert_equal "failed", run.status
        assert_includes run.error, "Durababble::AsyncBoundaryError"
        assert_includes run.error, "unawaited async step positions: 0"
        assert_equal(
          [["ok", "completed"]],
          store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )
      end
    end

    test "rejects async blocks that do not reach one durable step with #{backend.name}" do
      with_durababble_store(backend, "async_workflow_test") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async_missing_boundary"

          def execute(_input)
            await_all(async { "not durable" })
          end
        end

        run = Durababble::Engine.new(store:).run(workflow, input: {})

        assert_equal "failed", run.status
        assert_includes run.error, "Durababble::AsyncBoundaryError"
        assert_includes run.error, "must call exactly one durable workflow step"
        assert_empty store.steps_for(run.id)
      end
    end

    test "resumes async fan-out after crashing once a sibling completed with #{backend.name}" do
      with_durababble_store(backend, "async_workflow_test") do |store|
        store.migrate!
        slow_calls = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async_crash_resume"

          define_method(:execute) do |input|
            await_all([
              async { fast(input) },
              async { slow(input) },
            ])
          end

          step def fast(input)
            input.merge("fast" => true)
          end

          step define_method(:slow) { |input|
            slow_calls += 1
            input.merge("slow_calls" => slow_calls)
          }
        end
        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(store:, worker_id: "crasher", lease_seconds: 60, crash_after: :step_completed, migrate: false).resume(workflow, workflow_id:)
        end
        store.steal_expired_leases!(now: Time.now + 61)

        recovered = Durababble::Engine.new(store:, worker_id: "recovery", migrate: false).resume(workflow, workflow_id:)

        assert_equal "completed", recovered.status
        assert_equal [{ "fast" => true }, { "slow_calls" => 1 }], recovered.result
        assert_equal(
          [["fast", "completed"], ["slow", "completed"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )
      end
    end

    test "mixes async steps with durable waits and resumes deterministically with #{backend.name}" do
      with_durababble_store(backend, "async_workflow_test") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async_wait"

          def execute(input)
            values = await_all([
              async { compute(input) },
              async { wait_for_event(input) },
            ])
            { "values" => values }
          end

          step def compute(input)
            input.merge("computed" => true)
          end

          step def wait_for_event(input)
            wait_event("async:#{input.fetch("id")}", { "waiting" => input.fetch("id") })
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.name, input: { "id" => "wf-1" })

        waiting = Durababble::Engine.new(store:, migrate: false).resume(workflow, workflow_id:)
        assert_equal "waiting", waiting.status
        assert_equal(
          [["compute", "completed"], ["wait_for_event", "waiting"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )

        assert_equal 1, store.signal_event("async:wf-1", payload: { "event" => "done" })
        completed = Durababble::Engine.new(store:, migrate: false).resume(workflow, workflow_id:)

        assert_equal "completed", completed.status
        assert_equal(
          [{ "id" => "wf-1", "computed" => true }, { "waiting" => "wf-1", "event" => "done" }],
          completed.result.fetch("values"),
        )
      end
    end

    test "rejects stale async step completion after lease release with #{backend.name}" do
      with_durababble_store(backend, "async_workflow_test") do |store|
        store.migrate!
        started = Queue.new
        release = Queue.new
        attempts = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async_stale_lease"

          define_method(:execute) do |input|
            await_all([async { blocked(input) }])
          end

          step define_method(:blocked) { |input|
            attempts += 1
            if attempts == 1
              started << true
              release.pop
            end
            input.merge("done" => true)
          }
        end
        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

        error_queue = Queue.new
        thread = Thread.new do
          Durababble::Engine.new(store:, worker_id: "zombie", lease_seconds: 60, migrate: false).resume(workflow, workflow_id:)
        rescue StandardError => e
          error_queue << e
        end

        started.pop
        release_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
        begin
          release_store.release_worker_leases!(worker_id: "zombie")
        ensure
          release_store.close
        end
        release << true
        thread.join

        assert_instance_of Durababble::LeaseConflict, error_queue.pop
        assert_equal "pending", store.workflow(workflow_id).fetch("status")
        assert_equal [["blocked", "running"]], store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] }

        recovered = Durababble::Engine.new(store:, worker_id: "owner", migrate: false).resume(workflow, workflow_id:)
        assert_equal "completed", recovered.status
        assert_equal [{ "done" => true }], recovered.result
      end
    end
  end
end
