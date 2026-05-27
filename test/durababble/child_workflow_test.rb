# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleChildWorkflowTest < DurababbleTestCase
  class ChildEchoWorkflow < Durababble::Workflow
    workflow_name "child-echo"

    def execute(input)
      echo(input)
    end

    step def echo(input)
      { "echo" => input.fetch("value") }
    end
  end

  class ChildFailWorkflow < Durababble::Workflow
    workflow_name "child-fail"

    def execute(input)
      fail_step(input)
    end

    step def fail_step(input)
      raise ArgumentError, input.fetch("message")
    end
  end

  class ChildFlakyWorkflow < Durababble::Workflow
    workflow_name "child-flaky"

    def execute(input)
      flaky(input)
    end

    step retry: { maximum_attempts: 2, schedule: [0] }
    def flaky(input)
      raise "try again" if step_context.attempt_number == 1

      { "ok" => input.fetch("value") }
    end
  end

  class ParentAwaitWorkflow < Durababble::Workflow
    workflow_name "parent-await-child"

    def execute(input)
      handle = start_child(ChildEchoWorkflow, input.fetch("child"), id: input["child_id"], cancellation: :request_cancel)
      { "child_id" => handle.workflow_id, "result" => handle.await(poll_interval: 0) }
    end
  end

  class ParentHandlesFailureWorkflow < Durababble::Workflow
    workflow_name "parent-handles-child-failure"

    def execute(input)
      handle = start_child(ChildFailWorkflow, input.fetch("child"), id: input["child_id"], cancellation: :abandon)
      handle.await(poll_interval: 0)
    rescue Durababble::ChildWorkflowFailed => e
      { "handled" => e.message }
    end
  end

  class ParentAwaitsFlakyWorkflow < Durababble::Workflow
    workflow_name "parent-awaits-flaky-child"

    def execute(input)
      handle = start_child(ChildFlakyWorkflow, input.fetch("child"), id: input["child_id"], cancellation: :abandon)
      handle.await(poll_interval: 0)
    end
  end

  class ParentCancelableWorkflow < Durababble::Workflow
    workflow_name "parent-cancelable-child"

    def execute(input)
      handle = start_child(ChildEchoWorkflow, input.fetch("child"), id: input["child_id"], cancellation: input.fetch("policy").to_sym)
      handle.await(poll_interval: 60)
    end
  end

  class ParentStartsOtherPoolWorkflow < Durababble::Workflow
    workflow_name "parent-starts-other-pool-child"

    def execute(input)
      handle = start_child(
        ChildEchoWorkflow,
        input.fetch("child"),
        worker_pool: input.fetch("child_pool"),
        cancellation: :abandon,
      )
      { "child_id" => handle.workflow_id, "worker_pool" => handle.worker_pool, "status" => handle.status }
    end
  end

  class ObjectStarter < Durababble::DurableObject
    object_type "child_workflow_object_starter"

    def initialize_state
      { "child_id" => nil, "observed" => nil }
    end

    expose_command def start_child(value)
      handle = start_workflow(ChildEchoWorkflow, { "value" => value })
      schedule_wake(name: "observe-child", at: Time.now, payload: { "child_id" => handle.workflow_id })
      update_state(current_state.merge("child_id" => handle.workflow_id))
      handle.workflow_id
    end

    def on_wake(name:, payload:)
      handle = ChildEchoWorkflow.handle(payload.fetch("child_id"), store: @store)
      update_state(current_state.merge("observed" => { "name" => name, "status" => handle.status, "result" => handle.result }))
    end

    expose def observed
      current_state.fetch("observed")
    end
  end

  durababble_store_backends.each do |backend|
    test "parent starts child and awaits success durably with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_success") do |store|
        workflow_id = store.enqueue_workflow(name: ParentAwaitWorkflow.workflow_name, input: { "child" => { "value" => "ok" }, "child_id" => "child-success" })
        run = run_workers_until_terminal(backend, store, workflow_id, workflows: [ParentAwaitWorkflow, ChildEchoWorkflow])

        assert_equal "completed", run.fetch("status")
        assert_equal({ "child_id" => "child-success", "result" => { "echo" => "ok" } }, run.fetch("result"))
        assert_equal(
          ["child_workflow:child-echo:start", "child_workflow:workflow:observe", "sleep", "child_workflow:workflow:observe"],
          store.steps_for(workflow_id).map { |step| step.fetch("name") },
        )
        assert_hash_includes(
          store.child_workflows_for_parent(parent_workflow_id: workflow_id).first,
          "child_workflow_id" => "child-success",
          "status" => "completed",
          "cancellation_policy" => "request_cancel",
        )
      end
    end

    test "parent replay after child start reattaches without duplicate child with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_start_replay") do |store|
        workflow_id = store.enqueue_workflow(name: ParentAwaitWorkflow.workflow_name, input: { "child" => { "value" => "ok" }, "child_id" => "child-replay" })
        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(store:, worker_id: "crasher", crash_after: :step_completed, migrate: false).resume(ParentAwaitWorkflow, workflow_id:)
        end
        store.release_worker_leases!(worker_id: "crasher")

        run = run_workers_until_terminal(backend, store, workflow_id, workflows: [ParentAwaitWorkflow, ChildEchoWorkflow])

        assert_equal "completed", run.fetch("status")
        assert_equal 1, store.child_workflows_for_parent(parent_workflow_id: workflow_id).length
        assert_equal 1, store.workflow_history_for("child-replay").count { |event| event.fetch("kind") == "step_scheduled" }
      end
    end

    test "parent replay after crash while waiting observes child completion with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_wait_replay") do |store|
        workflow_id = store.enqueue_workflow(name: ParentAwaitWorkflow.workflow_name, input: { "child" => { "value" => "ok" }, "child_id" => "child-wait-replay" })
        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(store:, worker_id: "wait-crasher", crash_after: :wait_recorded, migrate: false).resume(ParentAwaitWorkflow, workflow_id:)
        end
        store.release_worker_leases!(worker_id: "wait-crasher")

        run = run_workers_until_terminal(backend, store, workflow_id, workflows: [ParentAwaitWorkflow, ChildEchoWorkflow])

        assert_equal "completed", run.fetch("status")
        assert_equal({ "child_id" => "child-wait-replay", "result" => { "echo" => "ok" } }, run.fetch("result"))
        assert_equal "completed", store.child_workflows_for_parent(parent_workflow_id: workflow_id).first.fetch("status")
      end
    end

    test "child failure can be handled by parent workflow code with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_failure") do |store|
        workflow_id = store.enqueue_workflow(name: ParentHandlesFailureWorkflow.workflow_name, input: { "child" => { "message" => "bad child" }, "child_id" => "child-fails" })
        run = run_workers_until_terminal(backend, store, workflow_id, workflows: [ParentHandlesFailureWorkflow, ChildFailWorkflow])

        assert_equal "completed", run.fetch("status")
        assert_match(/bad child/, run.fetch("result").fetch("handled"))
        assert_equal "failed", store.child_workflows_for_parent(parent_workflow_id: workflow_id).first.fetch("status")
      end
    end

    test "child retries remain independent and parent awaits final success with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_retry") do |store|
        workflow_id = store.enqueue_workflow(name: ParentAwaitsFlakyWorkflow.workflow_name, input: { "child" => { "value" => "eventual" }, "child_id" => "child-flaky" })
        run = run_workers_until_terminal(backend, store, workflow_id, workflows: [ParentAwaitsFlakyWorkflow, ChildFlakyWorkflow])

        assert_equal "completed", run.fetch("status")
        assert_equal({ "ok" => "eventual" }, run.fetch("result"))
        assert_equal ["failed", "completed"], store.step_attempts_for("child-flaky").map { |attempt| attempt.fetch("status") }
      end
    end

    test "parent cancellation respects child cancellation policy with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_cancellation") do |store|
        propagate_id = store.enqueue_workflow(name: ParentCancelableWorkflow.workflow_name, input: { "child" => { "value" => "propagate" }, "child_id" => "child-propagate", "policy" => "request_cancel" })
        abandon_id = store.enqueue_workflow(name: ParentCancelableWorkflow.workflow_name, input: { "child" => { "value" => "abandon" }, "child_id" => "child-abandon", "policy" => "abandon" })
        run_one_worker_tick(backend, store, workflows: [ParentCancelableWorkflow, ChildEchoWorkflow])
        run_one_worker_tick(backend, store, workflows: [ParentCancelableWorkflow, ChildEchoWorkflow])

        store.request_workflow_cancellation(workflow_id: propagate_id, reason: "stop parent")
        store.request_workflow_cancellation(workflow_id: abandon_id, reason: "stop parent")
        run_workers_until_terminal(backend, store, propagate_id, workflows: [ParentCancelableWorkflow])
        run_workers_until_terminal(backend, store, abandon_id, workflows: [ParentCancelableWorkflow])

        assert_equal "canceling", store.workflow("child-propagate").fetch("status")
        assert_equal "pending", store.workflow("child-abandon").fetch("status")
      end
    end

    test "hard termination does not request child cancellation with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_termination") do |store|
        workflow_id = store.enqueue_workflow(name: ParentCancelableWorkflow.workflow_name, input: { "child" => { "value" => "survive" }, "child_id" => "child-survives-termination", "policy" => "request_cancel" })
        run_one_worker_tick(backend, store, workflows: [ParentCancelableWorkflow])

        ParentCancelableWorkflow.handle(workflow_id, store:).terminate(reason: "operator hard stop")

        assert_equal "terminated", store.workflow(workflow_id).fetch("status")
        assert_equal "pending", store.workflow("child-survives-termination").fetch("status")
      end
    end

    test "explicit child worker pool leaves child for matching workers with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_worker_pool") do |store|
        workflow_id = store.enqueue_workflow(name: ParentStartsOtherPoolWorkflow.workflow_name, input: { "child" => { "value" => "pooled" }, "child_pool" => "child-pool" })
        run = run_workers_until_terminal(backend, store, workflow_id, workflows: [ParentStartsOtherPoolWorkflow, ChildEchoWorkflow])
        child_id = run.fetch("result").fetch("child_id")

        assert_equal "completed", run.fetch("status")
        assert_equal "child-pool", store.workflow(child_id).fetch("worker_pool")
        assert_equal "pending", store.workflow(child_id).fetch("status")

        child_worker = worker_for(backend, store, workflows: [ChildEchoWorkflow], worker_pool: "child-pool")
        child_worker.run_until_idle

        assert_equal "completed", store.workflow(child_id).fetch("status")
        assert_equal "completed", store.observe_child_workflow(child_id).fetch("status")
      end
    end

    test "durable object starts workflow and observes outcome via persisted wake with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_object") do |store|
        message_id = ObjectStarter.tell("starter-1", :start_child, "from-object", store:)
        run_workers_until_idle(backend, store, workflows: [ChildEchoWorkflow], objects: [ObjectStarter])
        child_id = store.inbox_message(message_id).fetch("result")

        assert_equal({ "echo" => "from-object" }, store.workflow(child_id).fetch("result"))
        assert_equal(
          { "name" => "observe-child", "status" => "completed", "result" => { "echo" => "from-object" } },
          ObjectStarter.at("starter-1", store:).observed,
        )
        assert_equal 1, store.child_workflows_for_object(parent_object_type: ObjectStarter.object_type, parent_object_id: "starter-1").length
      end
    end
  end

  private

  def run_workers_until_terminal(backend, store, workflow_id, workflows:, objects: [], max_ticks: 100)
    worker = worker_for(backend, store, workflows:, objects:)
    max_ticks.times do
      run = store.workflow(workflow_id)
      return run if Durababble::WorkflowStatus.terminal?(run)

      worker.run_until_idle(max_ticks: 20)
      run = store.workflow(workflow_id)
      return run if Durababble::WorkflowStatus.terminal?(run)

      store.wake_due_timers(now: Time.now + 3600)
    end
    raise "workflow #{workflow_id} did not finish"
  end

  def run_workers_until_idle(backend, store, workflows:, objects:, max_ticks: 100)
    worker = worker_for(backend, store, workflows:, objects:)
    max_ticks.times do
      worker.run_until_idle(max_ticks: 20)
      store.wake_due_timers(now: Time.now + 3600)
      break if worker.run_until_idle(max_ticks: 20).zero?
    end
  end

  def run_one_worker_tick(backend, store, workflows:, objects: [], worker_pool: "default")
    worker_for(backend, store, workflows:, objects:, worker_pool:).tick
  end

  def worker_for(_backend, store, workflows:, objects: [], worker_pool: "default")
    Durababble::Worker.new(store:, workflows:, objects:, worker_id: "child-workflow-worker", migrate: false, worker_pool:)
  end
end
