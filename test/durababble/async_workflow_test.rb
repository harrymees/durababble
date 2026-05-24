# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

require "timeout"

class DurababbleAsyncWorkflowTest < DurababbleTestCase
  test "command futures ignore duplicate terminal resolutions" do
    resolved = Durababble::CommandFuture.new(0)
    resolved.resolve("first")
    resolved.resolve("second")

    assert_equal "first", resolved.value

    rejected = Durababble::CommandFuture.new(1)
    rejected.reject(RuntimeError.new("first"))
    rejected.reject(RuntimeError.new("second"))

    error = assert_raises(RuntimeError) { rejected.value }
    assert_equal "first", error.message
  end

  durababble_store_backends.each do |backend|
    test "scatter gather fanout schedules every branch before completions with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async-fanout"

          def execute(ids)
            await_all(ids.map { |id| async { fetch(id) } })
          end

          def fetch(id)
            sleep((4 - id) * 0.005)
            { "id" => id, "key" => step_context.idempotency_key }
          end
          step :fetch
        end

        run = Durababble::Engine.new(store:, worker_id: "fanout-worker").run(workflow, input: [1, 2, 3])

        assert_equal "completed", run.status
        assert_equal [1, 2, 3], run.result.map { |row| row.fetch("id") }
        assert_equal(
          ["durababble:v1:workflow:#{run.id}:step:0", "durababble:v1:workflow:#{run.id}:step:1", "durababble:v1:workflow:#{run.id}:step:2"],
          run.result.map { |row| row.fetch("key") }.sort,
        )

        history = store.workflow_history_for(run.id)
        scheduled = history.select { |event| event.fetch("kind") == "step_scheduled" }
        completed = history.select { |event| event.fetch("kind") == "step_completed" }
        assert_equal [0, 1, 2], scheduled.map { |event| event.fetch("command_id").to_i }
        assert_equal [[1], [2], [3]], scheduled.map { |event| event.fetch("payload").fetch("args") }.sort_by(&:first)
        assert_operator scheduled.last.fetch("event_index").to_i, :<, completed.first.fetch("event_index").to_i
      end
    end

    test "raw Async scatter gather fanout schedules every branch before completions with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "raw-async-fanout"

          def execute(ids)
            Async do |task|
              ids.map { |id| task.async { fetch(id) } }.map(&:wait)
            end.wait
          end

          def fetch(id)
            sleep((4 - id) * 0.005)
            { "id" => id, "key" => step_context.idempotency_key }
          end
          step :fetch
        end

        run = Durababble::Engine.new(store:, worker_id: "raw-fanout-worker").run(workflow, input: [1, 2, 3])

        assert_equal "completed", run.status
        assert_equal [1, 2, 3], run.result.map { |row| row.fetch("id") }
        assert_equal(
          ["durababble:v1:workflow:#{run.id}:step:0", "durababble:v1:workflow:#{run.id}:step:1", "durababble:v1:workflow:#{run.id}:step:2"],
          run.result.map { |row| row.fetch("key") }.sort,
        )

        history = store.workflow_history_for(run.id)
        scheduled = history.select { |event| event.fetch("kind") == "step_scheduled" }
        completed = history.select { |event| event.fetch("kind") == "step_completed" }
        assert_equal [0, 1, 2], scheduled.map { |event| event.fetch("command_id").to_i }
        assert_equal [[1], [2], [3]], scheduled.map { |event| event.fetch("payload").fetch("args") }.sort_by(&:first)
        assert_operator scheduled.last.fetch("event_index").to_i, :<, completed.first.fetch("event_index").to_i
      end
    end

    test "continuation fanout replays dependent command order from completion history with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = continuation_workflow("continuation-replay")
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: [1, 2])
        store.claim_workflow(workflow_id:, worker_id: "history-seeder", lease_seconds: 60)
        retry_shape = default_retry_shape
        store.record_step_scheduled(workflow_id:, command_id: 0, name: "fetch_profile", args: [1], metadata: { "retry" => retry_shape })
        store.record_step_started(workflow_id:, command_id: 0, name: "fetch_profile")
        store.record_step_scheduled(workflow_id:, command_id: 1, name: "fetch_profile", args: [2], metadata: { "retry" => retry_shape })
        store.record_step_started(workflow_id:, command_id: 1, name: "fetch_profile")
        store.record_step_completed(workflow_id:, command_id: 1, result: { "id" => 2 })
        store.record_step_completed(workflow_id:, command_id: 0, result: { "id" => 1 })

        run = Durababble::Engine.new(store:, worker_id: "history-seeder").resume(workflow, workflow_id:)

        assert_equal "completed", run.status
        assert_equal [1, 2], run.result.map { |row| row.fetch("id") }
        score_schedules = store.workflow_history_for(workflow_id)
          .select { |event| event.fetch("kind") == "step_scheduled" && event.fetch("name") == "score_profile" }
        assert_equal [2, 1], score_schedules.map { |event| event.fetch("payload").fetch("args").first.fetch("id") }
      end
    end

    test "raw Async continuation fanout replays dependent command order from completion history with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = raw_continuation_workflow("raw-continuation-replay")
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: [1, 2])
        store.claim_workflow(workflow_id:, worker_id: "raw-history-seeder", lease_seconds: 60)
        retry_shape = default_retry_shape
        store.record_step_scheduled(workflow_id:, command_id: 0, name: "fetch_profile", args: [1], metadata: { "retry" => retry_shape })
        store.record_step_started(workflow_id:, command_id: 0, name: "fetch_profile")
        store.record_step_scheduled(workflow_id:, command_id: 1, name: "fetch_profile", args: [2], metadata: { "retry" => retry_shape })
        store.record_step_started(workflow_id:, command_id: 1, name: "fetch_profile")
        store.record_step_completed(workflow_id:, command_id: 1, result: { "id" => 2 })
        store.record_step_completed(workflow_id:, command_id: 0, result: { "id" => 1 })

        run = Durababble::Engine.new(store:, worker_id: "raw-history-seeder").resume(workflow, workflow_id:)

        assert_equal "completed", run.status
        assert_equal [1, 2], run.result.map { |row| row.fetch("id") }
        score_schedules = store.workflow_history_for(workflow_id)
          .select { |event| event.fetch("kind") == "step_scheduled" && event.fetch("name") == "score_profile" }
        assert_equal [2, 1], score_schedules.map { |event| event.fetch("payload").fetch("args").first.fetch("id") }
      end
    end

    test "scheduled command shape is replay checked before a step starts with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        first_version = Class.new(Durababble::Workflow) do
          workflow_name "scheduled-shape"

          def execute(_input)
            fetch(1)
          end

          def fetch(id) = { "id" => id }
          step :fetch
        end

        second_version = Class.new(Durababble::Workflow) do
          workflow_name "scheduled-shape"

          def execute(_input)
            fetch(2)
          end

          def fetch(id) = { "id" => id }
          step :fetch
        end

        workflow_id = store.enqueue_workflow(name: first_version.workflow_name, input: {})
        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(store:, worker_id: "crasher", crash_after: :step_scheduled).resume(first_version, workflow_id:)
        end
        assert_equal ["step_scheduled"], store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }

        store.steal_expired_leases!(now: Time.now + 61)
        run = Durababble::Engine.new(store:, worker_id: "replay-worker").resume(second_version, workflow_id:)

        assert_equal "failed", run.status
        assert_match(/NonDeterminismError/, run.error)
        assert_equal ["scheduled"], store.steps_for(workflow_id).map { |step| step.fetch("status") }
      end
    end

    test "raw Async tasks propagate workflow context to durable steps with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "raw-async-supported"

          def execute(input)
            Async { fetch(input) }.wait
          end

          def fetch(input)
            input.merge("key" => step_context.idempotency_key)
          end
          step :fetch
        end

        run = Durababble::Engine.new(store:, worker_id: "raw-worker").run(workflow, input: { "ok" => true })

        assert_equal "completed", run.status
        assert_equal true, run.result.fetch("ok")
        assert_equal "durababble:v1:workflow:#{run.id}:step:0", run.result.fetch("key")
        assert_equal ["step_scheduled", "step_started", "step_completed"], store.workflow_history_for(run.id).map { |event| event.fetch("kind") }
      end
    end

    test "Sync and raw Async propagate workflow context to durable steps with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "sync-raw-async-supported"

          def execute(input)
            Sync do
              Async { fetch(input) }.wait
            end
          end

          def fetch(input)
            input.merge("nested" => true)
          end
          step :fetch
        end

        run = Durababble::Engine.new(store:, worker_id: "sync-raw-worker").run(workflow, input: { "ok" => true })

        assert_equal "completed", run.status
        assert_equal({ "ok" => true, "nested" => true }, run.result)
        assert_equal ["completed"], store.steps_for(run.id).map { |step| step.fetch("status") }
      end
    end

    test "Async::Task#async propagates workflow context to durable steps with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "task-async-supported"

          def execute(input)
            Async do |task|
              task.async { fetch(input.fetch("left")) }.wait +
                task.async { fetch(input.fetch("right")) }.wait
            end.wait
          end

          def fetch(value)
            value * 2
          end
          step :fetch
        end

        run = Durababble::Engine.new(store:, worker_id: "task-raw-worker").run(workflow, input: { "left" => 3, "right" => 4 })

        assert_equal "completed", run.status
        assert_equal 14, run.result
        assert_equal [0, 1], store.workflow_history_for(run.id)
          .select { |event| event.fetch("kind") == "step_scheduled" }
          .map { |event| event.fetch("command_id").to_i }
      end
    end

    test "transient raw Async tasks do not inherit workflow context with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "transient-raw-async-rejected"

          def execute(input)
            outcome = Async do |task|
              task.async(transient: true, finished: false) do
                fetch(input)
              rescue StandardError => e
                e
              end.wait
            end.wait
            raise outcome if outcome.is_a?(StandardError)

            outcome
          end

          def fetch(input) = input
          step :fetch
        end

        run = Durababble::Engine.new(store:, worker_id: "raw-worker").run(workflow, input: { "ok" => true })

        assert_equal "failed", run.status
        assert_match(/Durababble-managed workflow task/, run.error)
        assert_empty store.workflow_history_for(run.id)
      end
    end

    test "transient raw Async descendants do not regain workflow context with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "transient-descendant-raw-async-rejected"

          def execute(input)
            outcome = Async do |task|
              task.async(transient: true, finished: false) do
                Async do
                  fetch(input)
                rescue StandardError => e
                  e
                end.wait
              end.wait
            end.wait
            raise outcome if outcome.is_a?(StandardError)

            outcome
          end

          def fetch(input) = input
          step :fetch
        end

        run = Durababble::Engine.new(store:, worker_id: "raw-worker").run(workflow, input: { "ok" => true })

        assert_equal "failed", run.status
        assert_match(/Durababble-managed workflow task/, run.error)
        assert_empty store.workflow_history_for(run.id)
      end
    end

    test "await_all observes every task failure before raising the first failure with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "await-all-failures"

          def execute(ids)
            await_all(ids.map { |id| async { fail_step(id) } })
          end

          def fail_step(id)
            raise "boom #{id}"
          end
          step :fail_step
        end

        run = Durababble::Engine.new(store:, worker_id: "failure-worker").run(workflow, input: [1, 2])

        assert_equal "failed", run.status
        assert_match(/boom 1/, run.error)
        assert_equal ["failed", "failed"], store.step_attempts_for(run.id).map { |attempt| attempt.fetch("status") }
        assert_equal [0, 1], store.workflow_history_for(run.id)
          .select { |event| event.fetch("kind") == "step_failed" }
          .map { |event| event.fetch("command_id").to_i }
      end
    end

    test "await_all does not let workflow suspension mask a sibling step failure with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "await-all-suspension-failure"

          def execute(input)
            await_all(
              async { wait_for_release(input.fetch("id")) },
              async { fail_sibling(input.fetch("id")) },
            )
          end

          def wait_for_release(id)
            wait_event("masked-release:#{id}", { "id" => id })
          end
          step :wait_for_release

          def fail_sibling(id)
            sleep(0.01)
            raise "boom #{id}"
          end
          step :fail_sibling
        end

        run = Durababble::Engine.new(store:, worker_id: "failure-worker").run(workflow, input: { "id" => "masked" })

        assert_equal "failed", run.status
        assert_match(/boom masked/, run.error)
        assert_equal [["wait_for_release", "waiting"], ["fail_sibling", "failed"]], store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] }
      end
    end

    test "suspended branch does not prevent already scheduled siblings from completing with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "parallel-wait-sibling"

          def execute(input)
            wait_task = async { wait_for_release(input.fetch("id")) }
            work_task = async { persist_sibling(input.fetch("id")) }
            await_all(wait_task, work_task)
          end

          def wait_for_release(id)
            wait_event("release:#{id}", { "id" => id })
          end
          step :wait_for_release

          def persist_sibling(id)
            sleep(0.01)
            { "sibling" => id }
          end
          step :persist_sibling
        end

        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "w1" })
        suspended = Durababble::Engine.new(store:, worker_id: "wait-worker").resume(workflow, workflow_id:)

        assert_equal "waiting", suspended.status
        assert_equal(
          [["wait_for_release", "waiting"], ["persist_sibling", "completed"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )

        assert_equal 1, store.signal_event("release:w1", payload: { "released" => true })
        completed = Durababble::Engine.new(store:, worker_id: "resume-worker").resume(workflow, workflow_id:)

        assert_equal "completed", completed.status
        assert_equal [{ "id" => "w1", "released" => true }, { "sibling" => "w1" }], completed.result
        assert_equal(
          ["step_scheduled", "step_started", "step_waiting", "step_scheduled", "step_started", "step_completed", "step_completed"],
          store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") },
        )
      end
    end

    test "raw Async branch suspension does not prevent already scheduled siblings from completing with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "raw-parallel-wait-sibling"

          def execute(input)
            Async do |task|
              wait_task = task.async { wait_for_release(input.fetch("id")) }
              work_task = task.async { persist_sibling(input.fetch("id")) }
              [wait_task.wait, work_task.wait]
            end.wait
          end

          def wait_for_release(id)
            wait_event("raw-release:#{id}", { "id" => id })
          end
          step :wait_for_release

          def persist_sibling(id)
            sleep(0.01)
            { "sibling" => id }
          end
          step :persist_sibling
        end

        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "raw-w1" })
        suspended = Durababble::Engine.new(store:, worker_id: "raw-wait-worker").resume(workflow, workflow_id:)

        assert_equal "waiting", suspended.status
        assert_equal(
          [["wait_for_release", "waiting"], ["persist_sibling", "completed"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )

        assert_equal 1, store.signal_event("raw-release:raw-w1", payload: { "released" => true })
        completed = Durababble::Engine.new(store:, worker_id: "raw-resume-worker").resume(workflow, workflow_id:)

        assert_equal "completed", completed.status
        assert_equal [{ "id" => "raw-w1", "released" => true }, { "sibling" => "raw-w1" }], completed.result
      end
    end

    test "suspended async branch does not release the lease before root work completes with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "parallel-wait-root-sibling"

          def execute(input)
            wait_task = async { wait_for_release(input.fetch("id")) }
            sibling = persist_sibling(input.fetch("id"))
            [await(wait_task), sibling]
          end

          def wait_for_release(id)
            wait_event("root-release:#{id}", { "id" => id })
          end
          step :wait_for_release

          def persist_sibling(id)
            sleep(0.01)
            { "sibling" => id }
          end
          step :persist_sibling
        end

        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "w2" })
        suspended = Durababble::Engine.new(store:, worker_id: "wait-worker").resume(workflow, workflow_id:)

        assert_equal "waiting", suspended.status
        assert_equal(
          [["persist_sibling", "completed"], ["wait_for_release", "waiting"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] }.sort_by(&:first),
        )

        assert_equal 1, store.signal_event("root-release:w2", payload: { "released" => true })
        completed = Durababble::Engine.new(store:, worker_id: "resume-worker").resume(workflow, workflow_id:)

        assert_equal "completed", completed.status
        assert_equal [{ "id" => "w2", "released" => true }, { "sibling" => "w2" }], completed.result
      end
    end

    test "event delivered during deferred suspension is not lost with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!
        signal_counts = []

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "parallel-wait-signal-window"

          define_method(:execute) do |input|
            wait_task = async { wait_for_release(input.fetch("id")) }
            signal_task = async { signal_release(input.fetch("id")) }
            await_all(wait_task, signal_task)
          end

          def wait_for_release(id)
            wait_event("window-release:#{id}", { "id" => id })
          end
          step :wait_for_release

          define_method(:signal_release) do |id|
            sleep(0.01)
            count = store.signal_event("window-release:#{id}", payload: { "released" => true })
            signal_counts << count
            { "signals" => count }
          end
          step :signal_release
        end

        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "window" })
        first = Durababble::Engine.new(store:, worker_id: "window-worker").resume(workflow, workflow_id:)

        assert_equal [1], signal_counts
        assert_equal "pending", first.status

        completed = Durababble::Engine.new(store:, worker_id: "window-resume").resume(workflow, workflow_id:)

        assert_equal "completed", completed.status
        assert_equal [{ "id" => "window", "released" => true }, { "signals" => 1 }], completed.result
      end
    end

    test "replay fails instead of hanging when history resolves an unscheduled command first with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        store.migrate!

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "completion-before-unscheduled-command"

          def execute(_input)
            [fetch_profile(1), fetch_profile(2)]
          end

          def fetch_profile(id)
            { "id" => id }
          end
          step :fetch_profile
        end

        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})
        store.claim_workflow(workflow_id:, worker_id: "history-seeder", lease_seconds: 60)
        retry_shape = default_retry_shape
        store.record_step_scheduled(workflow_id:, command_id: 0, name: "fetch_profile", args: [1], metadata: { "retry" => retry_shape })
        store.record_step_started(workflow_id:, command_id: 0, name: "fetch_profile")
        store.record_step_scheduled(workflow_id:, command_id: 1, name: "fetch_profile", args: [2], metadata: { "retry" => retry_shape })
        store.record_step_started(workflow_id:, command_id: 1, name: "fetch_profile")
        store.record_step_completed(workflow_id:, command_id: 1, result: { "id" => 2 })
        store.record_step_completed(workflow_id:, command_id: 0, result: { "id" => 1 })

        run = Timeout.timeout(1) do
          Durababble::Engine.new(store:, worker_id: "history-seeder").resume(workflow, workflow_id:)
        end

        assert_equal "failed", run.status
        assert_match(/NonDeterminismError/, run.error)
        assert_match(/resolved command 1 before command 0/, run.error)
      end
    end
  end

  def continuation_workflow(name)
    Class.new(Durababble::Workflow) do
      workflow_name name

      def execute(ids)
        await_all(ids.map do |id|
          async do
            profile = fetch_profile(id)
            score_profile(profile)
          end
        end)
      end

      def fetch_profile(id)
        { "id" => id }
      end
      step :fetch_profile

      def score_profile(profile)
        profile
      end
      step :score_profile
    end
  end

  def raw_continuation_workflow(name)
    Class.new(Durababble::Workflow) do
      workflow_name name

      def execute(ids)
        Async do |task|
          ids.map do |id|
            task.async do
              profile = fetch_profile(id)
              score_profile(profile)
            end
          end.map(&:wait)
        end.wait
      end

      def fetch_profile(id)
        { "id" => id }
      end
      step :fetch_profile

      def score_profile(profile)
        profile
      end
      step :score_profile
    end
  end

  def default_retry_shape
    retry_policy = Durababble::RetryPolicy.from(nil)
    {
      "initial_interval" => retry_policy.initial_interval,
      "backoff_coefficient" => retry_policy.backoff_coefficient,
      "maximum_interval" => retry_policy.maximum_interval,
      "maximum_attempts" => retry_policy.maximum_attempts,
      "schedule" => retry_policy.schedule,
      "non_retryable_errors" => retry_policy.non_retryable_errors.map(&:to_s),
    }
  end
end
