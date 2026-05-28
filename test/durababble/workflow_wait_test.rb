# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowWaitTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "sleep waits can run directly from workflow orchestration with #{backend.name}" do
      with_durababble_store(backend, "workflow_wait_sleep") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-sleep-wait"

          def execute(input)
            slept = sleep(3600, input.merge("slept" => true))
            slept.merge("done" => true)
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "sleep" })

        waiting = Durababble::Engine.new(store:, worker_id: "direct-wait").resume(workflow, workflow_id:)

        assert_equal "waiting", waiting.status
        assert_equal [["sleep", "waiting"]], store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] }
        assert_equal ["pending"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
        assert_equal ["step_scheduled", "step_waiting"], store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }

        completed = resume_waiting_workflow(store, workflow, workflow_id, worker_id: "direct-resume")

        assert_equal "completed", completed.status
        assert_equal({ "id" => "sleep", "slept" => true, "done" => true }, completed.result)
        assert_equal ["step_scheduled", "step_waiting", "step_completed"], store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }
      end
    end

    test "sleep_until waits can recover after crashing after wait persistence with #{backend.name}" do
      with_durababble_store(backend, "workflow_wait_timer_crash") do |store|
        store.migrate!
        wake_at = Time.now + 3600
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-sleep-until-crash"

          define_method(:execute) do |input|
            slept = sleep_until(wake_at, input.merge("slept" => true))
            slept.merge("done" => true)
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "timer" })

        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(
            store:,
            worker_id: "direct-crasher",
            crash_after: :wait_recorded,
          ).resume(workflow, workflow_id:)
        end

        assert_hash_includes store.workflow(workflow_id), "status" => "waiting", "locked_by" => nil
        assert_equal ["pending"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }

        recovered = resume_waiting_workflow(store, workflow, workflow_id, worker_id: "direct-recover")

        assert_equal "completed", recovered.status
        assert_equal({ "id" => "timer", "slept" => true, "done" => true }, recovered.result)
        assert_equal 1, store.waits_for(workflow_id).length
        assert_equal ["completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
      end
    end

    test "cancellation cancels direct pending waits and ignores late timer wakeups with #{backend.name}" do
      with_durababble_store(backend, "workflow_wait_cancel") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-wait-cancel"

          def execute(input)
            sleep(3600, input)
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "wait" })

        waiting = Durababble::Engine.new(store:, worker_id: "cancel-wait").resume(workflow, workflow_id:)
        assert_equal "waiting", waiting.status

        workflow.handle(workflow_id, store:).cancel(reason: "stop direct wait")
        assert_equal ["canceled"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
        canceled = Durababble::Engine.new(store:, worker_id: "cancel-resume").resume(workflow, workflow_id:)

        assert_equal "canceled", canceled.status
        assert_equal 0, store.wake_due_timers(now: Time.now + 3601)
      end
    end

    test "direct wait_condition suspends and resumes the workflow fiber with #{backend.name}" do
      with_durababble_store(backend, "workflow_wait_condition") do |store|
        store.migrate!
        checks = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-wait-condition"

          define_method(:execute) do |_input|
            ready = wait_condition(timeout: 1) do
              checks += 1
              checks > 1
            end
            { "ready" => ready, "checks" => checks }
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})

        waiting = Durababble::Engine.new(store:, worker_id: "condition-wait").resume(workflow, workflow_id:)
        assert_equal "waiting", waiting.status

        completed = resume_waiting_workflow(store, workflow, workflow_id, worker_id: "condition-resume")

        assert_equal "completed", completed.status
        assert_equal({ "ready" => true, "checks" => 2 }, completed.result)
      end
    end

    test "direct wait_condition without a timeout polls until the block is true with #{backend.name}" do
      with_durababble_store(backend, "workflow_wait_condition_poll") do |store|
        store.migrate!
        checks = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-wait-condition-poll"

          define_method(:execute) do |_input|
            ready = wait_condition do
              checks += 1
              checks > 1
            end
            { "ready" => ready, "checks" => checks }
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})

        waiting = Durababble::Engine.new(store:, worker_id: "condition-poll-wait").resume(workflow, workflow_id:)
        assert_equal "waiting", waiting.status

        completed = resume_waiting_workflow(store, workflow, workflow_id, worker_id: "condition-poll-resume")

        assert_equal "completed", completed.status
        assert_equal({ "ready" => true, "checks" => 2 }, completed.result)
      end
    end

    test "direct waits let already-started async sibling work finish before suspension with #{backend.name}" do
      with_durababble_store(backend, "workflow_wait_async") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-wait-async-sibling"

          def execute(input)
            Async do |task|
              wait_task = task.async { sleep(3600, { "id" => input.fetch("id"), "released" => true }) }
              work_task = task.async { persist_sibling(input.fetch("id")) }
              [wait_task.wait, work_task.wait]
            end.wait
          end

          def persist_sibling(id)
            sleep(0.01)
            { "sibling" => id }
          end
          step :persist_sibling
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "async" })

        waiting = Durababble::Engine.new(store:, worker_id: "async-wait").resume(workflow, workflow_id:)

        assert_equal "waiting", waiting.status
        assert_equal(
          [["persist_sibling", "completed"], ["sleep", "waiting"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] }.sort_by(&:first),
        )

        completed = resume_waiting_workflow(store, workflow, workflow_id, worker_id: "async-resume")

        assert_equal "completed", completed.status
        assert_equal [{ "id" => "async", "released" => true }, { "sibling" => "async" }], completed.result
      end
    end

    test "concurrent direct timer waits keep the earliest unresolved wake on the workflow row with #{backend.name}" do
      with_durababble_store(backend, "workflow_wait_parallel_timers") do |store|
        store.migrate!
        early = Time.now + 3600
        late = Time.now + 7200
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-wait-parallel-timers"

          define_method(:execute) do |input|
            Async do |task|
              late_wait = task.async { sleep_until(late, input.merge("timer" => "late")) }
              early_wait = task.async { sleep_until(early, input.merge("timer" => "early")) }
              [late_wait.wait, early_wait.wait]
            end.wait
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "parallel" })

        waiting = Durababble::Engine.new(store:, worker_id: "parallel-wait").resume(workflow, workflow_id:)

        assert_equal "waiting", waiting.status
        assert_in_delta early.to_f, timestamp_value(store.workflow(workflow_id).fetch("next_run_at")).to_f, 1
        assert_equal ["pending", "pending"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }

        still_waiting = resume_waiting_workflow(store, workflow, workflow_id, worker_id: "parallel-early")

        assert_equal "waiting", still_waiting.status
        assert_in_delta late.to_f, timestamp_value(store.workflow(workflow_id).fetch("next_run_at")).to_f, 1
        statuses_by_timer = store.waits_for(workflow_id).to_h { |wait| [wait.fetch("context").fetch("timer"), wait.fetch("status")] }
        assert_equal({ "early" => "completed", "late" => "pending" }, statuses_by_timer)

        completed = resume_waiting_workflow(store, workflow, workflow_id, worker_id: "parallel-late")

        assert_equal "completed", completed.status
        assert_equal [{ "id" => "parallel", "timer" => "late" }, { "id" => "parallel", "timer" => "early" }], completed.result
        assert_equal ["completed", "completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
      end
    end
  end

  def timestamp_value(value)
    return value if value.is_a?(Time)

    Time.parse(value.to_s)
  end
end
