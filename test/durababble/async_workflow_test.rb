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

  test "workflow replay treats non-retrying step failures as terminal resolutions" do
    failed = {
      "kind" => "step_failed",
      "command_id" => 0,
      "event_index" => 1,
      "error" => "RuntimeError: boom",
    }
    retrying = failed.merge("payload" => { "retrying" => true })

    assert Durababble::WorkflowReplayHistory.new([failed]).terminal_recorded?(0)
    refute Durababble::WorkflowReplayHistory.new([retrying]).terminal_recorded?(0)
  end

  durababble_store_backends.each do |backend|
    test "raw Async scatter gather fanout schedules every branch before completions with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        release = release_gate([3, 2, 1], expected_count: 3)
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "raw-async-fanout"

          def execute(ids)
            Async do |task|
              ids.map { |id| task.async { fetch(id) } }.map(&:wait)
            end.wait
          end

          define_method(:fetch) do |id|
            release.call(id)
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
        scheduled_command_ids = scheduled.map { |event| event.fetch("command_id").to_i }
        completed_ids = completed.map { |event| event.fetch("payload").fetch("id") }
        assert_equal [0, 1, 2], scheduled_command_ids
        assert_equal [[1], [2], [3]], scheduled.map { |event| event.fetch("payload").fetch("args") }.sort_by(&:first)
        assert_operator scheduled.last.fetch("event_index").to_i, :<, completed.first.fetch("event_index").to_i
        assert_equal [1, 2, 3], completed_ids.sort
      end
    end

    test "raw Async reports a later branch failure after out-of-order branch completions with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        release = release_gate([3, 1, 2], expected_count: 3)
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "raw-async-fanout-late-failure"

          def execute(ids)
            Async do |task|
              ids.map { |id| task.async { maybe_fetch(id) } }.map(&:wait)
            end.wait
          end

          define_method(:maybe_fetch) do |id|
            release.call(id)
            raise "boom #{id}" if id == 2

            { "id" => id }
          end
          step :maybe_fetch
        end

        run = Durababble::Engine.new(store:, worker_id: "raw-late-failure-worker").run(workflow, input: [1, 2, 3])

        assert_equal "failed", run.status
        assert_match(/boom 2/, run.error)

        history = store.workflow_history_for(run.id)
        scheduled = history.select { |event| event.fetch("kind") == "step_scheduled" }
        terminals = history.select { |event| ["step_completed", "step_failed"].include?(event.fetch("kind")) }
        scheduled_command_ids = scheduled.map { |event| event.fetch("command_id").to_i }
        input_by_command_id = scheduled.to_h do |event|
          [event.fetch("command_id").to_i, event.fetch("payload").fetch("args").first]
        end
        terminal_inputs = terminals.map do |event|
          [event.fetch("kind"), input_by_command_id.fetch(event.fetch("command_id").to_i)]
        end

        assert_equal [0, 1, 2], scheduled_command_ids
        assert_operator scheduled.last.fetch("event_index").to_i, :<, terminals.first.fetch("event_index").to_i
        assert_equal(
          [["step_completed", 1], ["step_failed", 2], ["step_completed", 3]],
          terminal_inputs.sort_by(&:last),
        )
        assert_equal(
          ["completed", "completed", "failed"],
          store.steps_for(run.id).map { |step| step.fetch("status") }.sort,
        )
      end
    end

    test "raw Async continuation fanout replays dependent command order from completion history with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
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
        assert_equal ["canceled"], store.steps_for(workflow_id).map { |step| step.fetch("status") }
        assert_empty store.step_attempts_for(workflow_id)
      end
    end

    test "caught step failures replay from history without rerunning the failed step with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        failed_runs = 0
        cleanup_runs = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "caught-step-failure-replay"

          define_method(:execute) do |_input|
            risky_step
          rescue StandardError => e
            cleanup_after_failure(e.class.name)
          end

          define_method(:risky_step) do
            failed_runs += 1
            raise "should not rerun"
          end
          step :risky_step

          define_method(:cleanup_after_failure) do |error_class|
            cleanup_runs += 1
            { "handled" => error_class }
          end
          step :cleanup_after_failure
        end

        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})
        store.claim_workflow(workflow_id:, worker_id: "failure-history-seeder", lease_seconds: 60)
        store.record_step_scheduled(workflow_id:, command_id: 0, name: "risky_step", metadata: { "retry" => default_retry_shape })
        store.record_step_started(workflow_id:, command_id: 0, name: "risky_step")
        store.record_step_failed(workflow_id:, command_id: 0, error: "RuntimeError: persisted failure")

        run = Durababble::Engine.new(store:, worker_id: "failure-history-seeder").resume(workflow, workflow_id:)

        assert_equal "completed", run.status
        assert_equal({ "handled" => "Durababble::Error" }, run.result)
        assert_equal 0, failed_runs
        assert_equal 1, cleanup_runs
      end
    end

    test "raw Async tasks propagate workflow context to durable steps with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
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

    test "step context propagates to raw Async children inside step bodies with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "step-context-raw-async-child"

          def execute(input)
            parent_context = capture_context(input)
            child_context = capture_context_in_child(input)
            [parent_context, child_context]
          end

          def capture_context(input)
            {
              "input" => input,
              "key" => step_context.idempotency_key,
              "index" => step_context.step_index,
            }
          end
          step :capture_context

          def capture_context_in_child(input)
            Async do
              {
                "input" => input,
                "key" => step_context.idempotency_key,
                "index" => step_context.step_index,
              }
            end.wait
          end
          step :capture_context_in_child
        end

        run = Durababble::Engine.new(store:, worker_id: "step-context-worker").run(workflow, input: { "ok" => true })

        assert_equal "completed", run.status
        assert_equal(
          [
            { "input" => { "ok" => true }, "key" => "durababble:v1:workflow:#{run.id}:step:0", "index" => 0 },
            { "input" => { "ok" => true }, "key" => "durababble:v1:workflow:#{run.id}:step:1", "index" => 1 },
          ],
          run.result,
        )
      end
    end

    test "durable steps remain unavailable in raw Async children inside step bodies with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "step-child-durable-step-rejected"

          def execute(input)
            outer_step(input)
          end

          def outer_step(input)
            Async do
              inner_step(input)
            rescue StandardError => e
              e.message
            end.wait
          end
          step :outer_step

          def inner_step(input)
            input
          end
          step :inner_step
        end

        run = Durababble::Engine.new(store:, worker_id: "step-context-worker").run(workflow, input: { "ok" => true })

        assert_equal "completed", run.status
        assert_match(/Durababble-managed workflow task/, run.result)
        assert_equal ["outer_step"], store.steps_for(run.id).map { |step| step.fetch("name") }
      end
    end

    test "Sync and raw Async propagate workflow context to durable steps with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
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

    test "raw Async wait loop observes every task failure before raising the first failure with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "raw-async-all-failures"

          def execute(ids)
            Async do |task|
              failures = []
              tasks = ids.map { |id| task.async { fail_step(id) } }
              tasks.each do |child|
                child.wait
              rescue StandardError => e
                failures << e
              end
              raise failures.first if failures.any?
            end.wait
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

    test "raw Async wait loop does not let workflow suspension mask a sibling step failure with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "raw-async-suspension-failure"

          def execute(input)
            Async do |task|
              errors = []
              [
                task.async { wait_for_release(input.fetch("id")) },
                task.async { fail_sibling(input.fetch("id")) },
              ].each do |child|
                child.wait
              rescue StandardError => e
                errors << e
              end
              error = errors.find { |candidate| !candidate.is_a?(Durababble::WorkflowSuspended) } || errors.first
              raise error if error
            end.wait
          end

          def wait_for_release(id)
            Durababble.wait_until(Time.now + 3600, { "id" => id })
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
        # The sibling failure terminalizes the workflow; fail_workflow then cancels the
        # parked branch's wait so the failed (terminal, non-resumable) workflow does not
        # strand a live `waiting` step.
        assert_equal [["wait_for_release", "canceled"], ["fail_sibling", "failed"]], store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] }
      end
    end

    test "raw Async branch suspension does not prevent already scheduled siblings from completing with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
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
            Durababble.wait_until(Time.now + 3600, { "id" => id, "released" => true })
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

        assert_equal 1, store.wake_due_timers(now: Time.now + 3601)
        completed = Durababble::Engine.new(store:, worker_id: "raw-resume-worker").resume(workflow, workflow_id:)

        assert_equal "completed", completed.status
        assert_equal [{ "id" => "raw-w1", "released" => true }, { "sibling" => "raw-w1" }], completed.result
        assert_equal(
          ["step_scheduled", "step_started", "step_waiting", "step_scheduled", "step_started", "step_completed", "step_completed"],
          store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") },
        )
      end
    end

    test "host fibers keep normal Ruby semantics while workflow execute waits on a step with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        step_started = Async::Condition.new
        host_finished = Async::Condition.new
        host_done = false
        host_result = nil

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "async-host-fiber-interleaving"

          def execute(input)
            wait_for_host(input)
          end

          define_method(:wait_for_host) do |input|
            step_started.signal
            host_finished.wait until host_done
            input.merge("step_read_byte" => File.read("README.md", 1))
          end
          step :wait_for_host
        end

        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "host-ok" })

        run = Async do |task|
          engine_task = task.async do
            Durababble::Engine.new(store:, worker_id: "host-fiber-worker").resume(workflow, workflow_id:)
          end

          host_task = task.async do
            step_started.wait
            Kernel.sleep(0)
            host_result = {
              "time_class" => Time.now.class.name,
              "random_integer" => rand(1_000_000).is_a?(Integer),
              "read_byte" => File.read("README.md", 1),
            }
            host_done = true
            host_finished.signal
          end

          host_task.wait
          engine_task.wait
        end.wait

        assert_equal "completed", run.status
        assert_equal({ "id" => "host-ok", "step_read_byte" => "#" }, run.result)
        assert_equal(
          { "time_class" => "Time", "random_integer" => true, "read_byte" => "#" },
          host_result,
        )
        assert_equal ["step_scheduled", "step_started", "step_completed"], store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }
      end
    end

    test "suspended raw Async branch does not release the lease before root work completes with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "raw-parallel-wait-root-sibling"

          def execute(input)
            Async do |task|
              wait_task = task.async do
                wait_for_release(input.fetch("id"))
              rescue Durababble::WorkflowSuspended => e
                e
              end
              sibling = persist_sibling(input.fetch("id"))
              wait_result = wait_task.wait
              raise wait_result if wait_result.is_a?(Durababble::WorkflowSuspended)

              [wait_result, sibling]
            end.wait
          end

          def wait_for_release(id)
            Durababble.wait_until(Time.now + 3600, { "id" => id, "released" => true })
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

        assert_equal 1, store.wake_due_timers(now: Time.now + 3601)
        completed = Durababble::Engine.new(store:, worker_id: "resume-worker").resume(workflow, workflow_id:)

        assert_equal "completed", completed.status
        assert_equal [{ "id" => "w2", "released" => true }, { "sibling" => "w2" }], completed.result
      end
    end

    test "timer wake during deferred suspension is not lost with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        wake_counts = []

        workflow = Class.new(Durababble::Workflow) do
          workflow_name "parallel-wait-timer-window"

          define_method(:execute) do |input|
            Async do |task|
              errors = []
              results = []
              wait_task = task.async { wait_for_release(input.fetch("id")) }
              wake_task = task.async { wake_release }
              [wait_task, wake_task].each_with_index do |child, index|
                results[index] = child.wait
              rescue StandardError => e
                errors << e
              end
              raise errors.first if errors.any?

              results
            end.wait
          end

          def wait_for_release(id)
            Durababble.wait_until(Time.now + 3600, { "id" => id, "released" => true })
          end
          step :wait_for_release

          define_method(:wake_release) do
            sleep(0.01)
            count = store.wake_due_timers(now: Time.now + 3601)
            wake_counts << count
            { "wakes" => count }
          end
          step :wake_release
        end

        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "window" })
        first = Durababble::Engine.new(store:, worker_id: "window-worker").resume(workflow, workflow_id:)

        assert_equal [1], wake_counts
        assert_equal "pending", first.status

        completed = Durababble::Engine.new(store:, worker_id: "window-resume").resume(workflow, workflow_id:)

        assert_equal "completed", completed.status
        assert_equal [{ "id" => "window", "released" => true }, { "wakes" => 1 }], completed.result
      end
    end

    test "replay fails instead of hanging when history resolves an unscheduled command first with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
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

    test "raw Async replay wakes waiters when sibling branch exits without scheduling historical command with #{backend.name}" do
      with_durababble_store(backend, "async_workflow") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "raw-async-removed-branch-replay"

          def execute(_input)
            Async do |task|
              kept = task.async { fetch_profile(1) }
              removed = task.async { "removed branch" }
              [kept.wait, removed.wait]
            end.wait
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

  def release_gate(order, expected_count:)
    state = { arrived: 0, order: order.dup }
    all_arrived = Async::Condition.new
    turn = Async::Condition.new

    lambda do |id|
      state[:arrived] += 1
      all_arrived.signal if state[:arrived] == expected_count
      all_arrived.wait until state[:arrived] == expected_count

      turn.wait until state[:order].first == id
      state[:order].shift
      turn.signal
    end
  end
end
