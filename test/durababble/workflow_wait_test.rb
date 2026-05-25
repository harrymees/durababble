# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowWaitTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "event waits can run directly from workflow orchestration with #{backend.name}" do
      with_durababble_store(backend, "workflow_wait_event") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-event-wait"

          def execute(input)
            approved = wait_event("direct:#{input.fetch("id")}", input)
            approved.merge("done" => true)
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "event" })

        waiting = Durababble::Engine.new(store:, worker_id: "direct-wait", migrate: false).resume(workflow, workflow_id:)

        assert_equal "waiting", waiting.status
        assert_equal [["wait_event", "waiting"]], store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] }
        assert_equal ["pending"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
        assert_equal ["step_scheduled", "step_waiting"], store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }

        assert_equal 1, store.signal_event("direct:event", payload: { "approved" => true })
        completed = Durababble::Engine.new(store:, worker_id: "direct-resume", migrate: false).resume(workflow, workflow_id:)

        assert_equal "completed", completed.status
        assert_equal({ "id" => "event", "approved" => true, "done" => true }, completed.result)
        assert_equal ["step_scheduled", "step_waiting", "step_completed"], store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }
      end
    end

    test "sleep_until waits can recover after crashing after wait persistence with #{backend.name}" do
      with_durababble_store(backend, "workflow_wait_timer_crash") do |store|
        store.migrate!
        wake_at = Time.utc(2026, 4, 1, 12, 0, 0)
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
            migrate: false,
          ).resume(workflow, workflow_id:)
        end

        assert_hash_includes store.workflow(workflow_id), "status" => "waiting", "locked_by" => nil
        assert_equal ["pending"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }

        assert_equal 1, store.wake_due_timers(now: wake_at + 1)
        recovered = Durababble::Engine.new(store:, worker_id: "direct-recover", migrate: false).resume(workflow, workflow_id:)

        assert_equal "completed", recovered.status
        assert_equal({ "id" => "timer", "slept" => true, "done" => true }, recovered.result)
        assert_equal 1, store.waits_for(workflow_id).length
        assert_equal ["completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
      end
    end

    test "cancellation cancels direct pending waits and ignores late signals with #{backend.name}" do
      with_durababble_store(backend, "workflow_wait_cancel") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-wait-cancel"

          def execute(input)
            wait_event("cancel-direct:#{input.fetch("id")}", input)
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "wait" })

        waiting = Durababble::Engine.new(store:, worker_id: "cancel-wait", migrate: false).resume(workflow, workflow_id:)
        assert_equal "waiting", waiting.status

        workflow.handle(workflow_id, store:).cancel(reason: "stop direct wait")
        assert_equal ["canceled"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
        canceled = Durababble::Engine.new(store:, worker_id: "cancel-resume", migrate: false).resume(workflow, workflow_id:)

        assert_equal "canceled", canceled.status
        assert_equal 0, store.signal_event("cancel-direct:wait", payload: { "late" => true })
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

        waiting = Durababble::Engine.new(store:, worker_id: "condition-wait", migrate: false).resume(workflow, workflow_id:)
        assert_equal "waiting", waiting.status

        assert_equal 1, store.wake_due_timers(now: Time.now + 2)
        completed = Durababble::Engine.new(store:, worker_id: "condition-resume", migrate: false).resume(workflow, workflow_id:)

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
              wait_task = task.async { wait_event("direct-async:#{input.fetch("id")}", { "id" => input.fetch("id") }) }
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

        waiting = Durababble::Engine.new(store:, worker_id: "async-wait", migrate: false).resume(workflow, workflow_id:)

        assert_equal "waiting", waiting.status
        assert_equal(
          [["persist_sibling", "completed"], ["wait_event", "waiting"]],
          store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] }.sort_by(&:first),
        )

        assert_equal 1, store.signal_event("direct-async:async", payload: { "released" => true })
        completed = Durababble::Engine.new(store:, worker_id: "async-resume", migrate: false).resume(workflow, workflow_id:)

        assert_equal "completed", completed.status
        assert_equal [{ "id" => "async", "released" => true }, { "sibling" => "async" }], completed.result
      end
    end
  end
end
