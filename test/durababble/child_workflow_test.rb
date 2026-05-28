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

  class ParentEnqueueChildWorkflow < Durababble::Workflow
    workflow_name "parent-enqueue-child"

    def execute(input)
      handle = ChildEchoWorkflow.enqueue(input.fetch("child"), id: input["child_id"], cancellation: :request_cancel)
      { "child_id" => handle.workflow_id, "handle_class" => handle.class.name, "result" => handle.result }
    end
  end

  class ParentStartChildWorkflow < Durababble::Workflow
    workflow_name "parent-start-child"

    def execute(input)
      handle = ChildEchoWorkflow.start(input.fetch("child"), id: input["child_id"], cancellation: :abandon)
      { "child_id" => handle.workflow_id, "result" => handle.result }
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

  class ParentAwaitTimeoutWorkflow < Durababble::Workflow
    workflow_name "parent-await-child-timeout"

    def execute(input)
      handle = ChildEchoWorkflow.enqueue(
        input.fetch("child"),
        id: input.fetch("child_id"),
        worker_pool: input.fetch("child_pool"),
        cancellation: :abandon,
      )
      handle.await(poll_interval: input.fetch("poll_interval"), timeout: input.fetch("timeout"))
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
      handle = ChildEchoWorkflow.enqueue({ "value" => value })
      schedule_wake(name: "observe-child", at: Time.now, payload: { "child_id" => handle.workflow_id })
      update_state(current_state.merge("child_id" => handle.workflow_id))
      handle.workflow_id
    end

    expose_command def start_child_with_policy(value, policy)
      handle = ChildEchoWorkflow.enqueue({ "value" => value }, id: "object-policy-#{policy}", cancellation: policy.to_sym)
      update_state(current_state.merge("child_id" => handle.workflow_id))
      handle.workflow_id
    end

    expose_command def start_child_with_id(value, child_id)
      handle = ChildEchoWorkflow.enqueue({ "value" => value }, id: child_id)
      update_state(current_state.merge("child_id" => handle.workflow_id))
      handle.workflow_id
    end

    expose_command def start_two_children(value)
      first = ChildEchoWorkflow.enqueue({ "value" => value })
      second = ChildEchoWorkflow.enqueue({ "value" => value })
      update_state(current_state.merge("child_ids" => [first.workflow_id, second.workflow_id]))
      [first.workflow_id, second.workflow_id]
    end

    expose def forbidden_query_enqueue(value)
      ChildEchoWorkflow.enqueue({ "value" => value }, id: "query-guard-child", store: @store, worker_pool: @worker_pool)
    end

    def on_wake(name:, payload:)
      handle = ChildEchoWorkflow.handle(payload.fetch("child_id"), store: @store)
      update_state(current_state.merge("observed" => { "name" => name, "status" => handle.status, "result" => handle.result }))
    end

    expose def observed
      current_state.fetch("observed")
    end
  end

  class ObjectRetryShapeStarter < Durababble::DurableObject
    object_type "child_workflow_object_retry_shape_starter"

    expose_command retry: { maximum_attempts: 2, schedule: [0] }
    def start_child_then_retry_with_changed_input
      handle = ChildEchoWorkflow.enqueue({ "value" => "attempt-#{command_context.attempt_number}" })
      raise "retry after child start" if command_context.attempt_number == 1

      handle.workflow_id
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
          ["child_workflow:child-echo:start", "child_workflow:workflow:observe", "child_workflow:workflow:await", "child_workflow:workflow:observe"],
          store.steps_for(workflow_id).map { |step| step.fetch("name") },
        )
        assert_hash_includes(
          store.child_workflow_rows_for_parent(parent_workflow_id: workflow_id).first,
          "child_workflow_id" => "child-success",
          "status" => "completed",
          "cancellation_policy" => "request_cancel",
        )
        assert_hash_includes(
          store.workflow("child-success"),
          "child_origin_kind" => "workflow",
          "parent_workflow_id" => workflow_id,
          "parent_command_id" => 0,
          "child_cancellation_policy" => "request_cancel",
        )
      end
    end

    test "workflow-context enqueue starts a child and result awaits internally with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_enqueue_api") do |store|
        workflow_id = store.enqueue_workflow(name: ParentEnqueueChildWorkflow.workflow_name, input: { "child" => { "value" => "enqueued" }, "child_id" => "child-enqueue-api" })
        run = run_workers_until_terminal(backend, store, workflow_id, workflows: [ParentEnqueueChildWorkflow, ChildEchoWorkflow])

        assert_equal "completed", run.fetch("status")
        assert_equal(
          { "child_id" => "child-enqueue-api", "handle_class" => "Durababble::ChildWorkflowHandle", "result" => { "echo" => "enqueued" } },
          run.fetch("result"),
        )
        assert_equal(
          ["child_workflow:child-echo:start", "child_workflow:workflow:observe", "child_workflow:workflow:await", "child_workflow:workflow:observe"],
          store.steps_for(workflow_id).map { |step| step.fetch("name") },
        )
      end
    end

    test "workflow-context start returns the same child handle shape with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_start_api") do |store|
        workflow_id = store.enqueue_workflow(name: ParentStartChildWorkflow.workflow_name, input: { "child" => { "value" => "started" }, "child_id" => "child-start-api" })
        run = run_workers_until_terminal(backend, store, workflow_id, workflows: [ParentStartChildWorkflow, ChildEchoWorkflow])

        assert_equal "completed", run.fetch("status")
        assert_equal({ "child_id" => "child-start-api", "result" => { "echo" => "started" } }, run.fetch("result"))
        assert_equal "abandon", store.child_workflow_rows_for_parent(parent_workflow_id: workflow_id).first.fetch("cancellation_policy")
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
        assert_equal 1, store.child_workflow_rows_for_parent(parent_workflow_id: workflow_id).length
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
        assert_equal "completed", store.child_workflow_rows_for_parent(parent_workflow_id: workflow_id).first.fetch("status")
      end
    end

    test "child await timeout deadline survives parent replay with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_timeout_replay") do |store|
        workflow_id = store.enqueue_workflow(
          name: ParentAwaitTimeoutWorkflow.workflow_name,
          input: {
            "child" => { "value" => "too-late" },
            "child_id" => "child-await-timeout",
            "child_pool" => "unserved-child-pool",
            "poll_interval" => 60,
            "timeout" => 5,
          },
        )
        worker = worker_for(backend, store, workflows: [ParentAwaitTimeoutWorkflow])

        worker.run_until_idle(max_ticks: 20)
        assert_equal "waiting", store.workflow(workflow_id).fetch("status")
        wake_at = store.workflow(workflow_id).fetch("next_run_at")
        make_workflow_timer_due(store, workflow_id, at: wake_at)

        with_store_current_time(store, wake_at) { worker.run_until_idle(max_ticks: 20) }
        run = store.workflow(workflow_id)

        assert_equal "failed", run.fetch("status")
        assert_match(/timed out waiting for child workflow child-await-timeout/, run.fetch("error"))
        assert_equal "pending", store.workflow("child-await-timeout").fetch("status")
      end
    end

    test "child failure can be handled by parent workflow code with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_failure") do |store|
        workflow_id = store.enqueue_workflow(name: ParentHandlesFailureWorkflow.workflow_name, input: { "child" => { "message" => "bad child" }, "child_id" => "child-fails" })
        run = run_workers_until_terminal(backend, store, workflow_id, workflows: [ParentHandlesFailureWorkflow, ChildFailWorkflow])

        assert_equal "completed", run.fetch("status")
        assert_match(/bad child/, run.fetch("result").fetch("handled"))
        assert_equal "failed", store.child_workflow_rows_for_parent(parent_workflow_id: workflow_id).first.fetch("status")
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
        assert_hash_includes(
          store.workflow(child_id),
          "child_origin_kind" => "object",
          "parent_object_type" => ObjectStarter.object_type,
          "parent_object_id" => "starter-1",
          "child_cancellation_policy" => "abandon",
        )
        assert_equal(
          { "name" => "observe-child", "status" => "completed", "result" => { "echo" => "from-object" } },
          ObjectStarter.at("starter-1", store:).observed,
        )
        assert_equal 1, store.child_workflow_rows_for_object(parent_object_type: ObjectStarter.object_type, parent_object_id: "starter-1").length
      end
    end

    test "durable object duplicate sibling workflow starts get distinct default identity with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_object_fanout") do |store|
        message_id = ObjectStarter.tell("starter-fanout", :start_two_children, "same-shape", store:)
        run_workers_until_idle(backend, store, workflows: [ChildEchoWorkflow], objects: [ObjectStarter])
        child_ids = store.inbox_message(message_id).fetch("result")

        assert_equal 2, child_ids.length
        assert_equal 2, child_ids.uniq.length
        assert_equal(
          child_ids.sort,
          store.child_workflow_rows_for_object(parent_object_type: ObjectStarter.object_type, parent_object_id: "starter-fanout")
            .map { |row| row.fetch("child_workflow_id") }
            .sort,
        )
        assert_equal([{ "echo" => "same-shape" }, { "echo" => "same-shape" }], child_ids.map { |child_id| store.workflow(child_id).fetch("result") })
      end
    end

    test "durable object retry reuses implicit child id and rejects changed shape with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_object_retry_shape") do |store|
        message_id = ObjectRetryShapeStarter.tell("starter-retry-shape", :start_child_then_retry_with_changed_input, store:)
        run_workers_until_idle(backend, store, workflows: [ChildEchoWorkflow], objects: [ObjectRetryShapeStarter])
        message = store.inbox_message(message_id)
        child_rows = store.child_workflow_rows_for_object(parent_object_type: ObjectRetryShapeStarter.object_type, parent_object_id: "starter-retry-shape")

        assert_hash_includes message, "status" => "dead_lettered", "attempts" => 2
        assert_match(/already used for a different child workflow/, message.fetch("error"))
        assert_equal 1, child_rows.length
        assert_equal({ "value" => "attempt-1" }, child_rows.first.fetch("input"))
      end
    end

    test "durable object query cannot enqueue workflows through class helpers with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_object_query_guard") do |store|
        assert_raises_matching(Durababble::Error, /cannot start workflows from an exposed query/) do
          ObjectStarter.at("starter-query-guard", store:).forbidden_query_enqueue("from-query")
        end

        assert_empty store.child_workflow_rows_for_object(parent_object_type: ObjectStarter.object_type, parent_object_id: "starter-query-guard")
        assert_raises(KeyError) { store.workflow("query-guard-child") }
      end
    end

    test "durable object workflow enqueue records cancellation policy with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_object_policy") do |store|
        message_id = ObjectStarter.tell("starter-policy", :start_child_with_policy, "policy", "request_cancel", store:)
        run_workers_until_idle(backend, store, workflows: [ChildEchoWorkflow], objects: [ObjectStarter])
        child_id = store.inbox_message(message_id).fetch("result")

        assert_equal "object-policy-request_cancel", child_id
        assert_hash_includes(
          store.workflow(child_id),
          "child_origin_kind" => "object",
          "parent_object_type" => ObjectStarter.object_type,
          "parent_object_id" => "starter-policy",
          "child_cancellation_policy" => "request_cancel",
        )
      end
    end

    test "external child handle observes, times out, cancels, and terminates with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_external_handle") do |store|
        pending = child_handle_for(store, "external-child-pending", input: { "value" => "external" })

        assert_equal "pending", pending.status
        assert_nil pending.result
        assert_nil pending.error
        assert_raises(Durababble::CommandTimeout) { pending.await(poll_interval: 0, timeout: 0) }

        run_workers_until_terminal(backend, store, "external-child-pending", workflows: [ChildEchoWorkflow])
        assert_equal({ "echo" => "external" }, pending.await(poll_interval: 0))
        assert_equal "completed", pending.status
        assert_equal({ "echo" => "external" }, pending.result)
        assert_nil pending.error

        canceled = child_handle_for(store, "external-child-canceled", input: { "value" => "cancel" })
        canceled.cancel(reason: "stop child")
        run_workers_until_terminal(backend, store, "external-child-canceled", workflows: [ChildEchoWorkflow])
        assert_raises(Durababble::ChildWorkflowCanceled) { canceled.await(poll_interval: 0) }

        terminated = child_handle_for(store, "external-child-terminated", input: { "value" => "terminate" })
        terminated.terminate(reason: "operator stop")
        assert_raises(Durababble::ChildWorkflowTerminated) { terminated.await(poll_interval: 0) }
      end
    end

    test "child workflow id reuse rejects shape and origin changes above the store with #{backend.name}" do
      with_durababble_store(backend, "child_workflow_idempotency_conflict") do |store|
        first_message_id = ObjectStarter.tell("starter-conflict", :start_child_with_id, "one", "child-conflict", store:)
        run_workers_until_idle(backend, store, workflows: [ChildEchoWorkflow], objects: [ObjectStarter])
        assert_equal "completed", store.inbox_message(first_message_id).fetch("status")

        input_conflict_id = ObjectStarter.tell("starter-conflict", :start_child_with_id, "two", "child-conflict", store:)
        run_workers_until_idle(backend, store, workflows: [ChildEchoWorkflow], objects: [ObjectStarter])
        assert_hash_includes store.inbox_message(input_conflict_id), "status" => "dead_lettered"
        assert_match(/workflow id child-conflict already used for a different child workflow/, store.inbox_message(input_conflict_id).fetch("error"))

        origin_conflict_id = ObjectStarter.tell("starter-other-origin", :start_child_with_id, "one", "child-conflict", store:)
        run_workers_until_idle(backend, store, workflows: [ChildEchoWorkflow], objects: [ObjectStarter])
        assert_hash_includes store.inbox_message(origin_conflict_id), "status" => "dead_lettered"
        assert_match(/workflow id child-conflict already used for a different child workflow/, store.inbox_message(origin_conflict_id).fetch("error"))

        assert_raises(KeyError) { store.observe_child_workflow("missing-child-link") }
      end
    end
  end

  private

  def child_handle_for(store, child_id, input:)
    object_id = "external-starter-#{child_id}"
    command_id = store.enqueue_object_command(
      object_type: ObjectStarter.object_type,
      object_id:,
      method_name: "start_child",
      args: [],
      kwargs: {},
    )
    store.claim_object_command(command_id:, worker_id: "external-starter-worker", lease_seconds: 30)
    store.start_child_workflow(
      origin_kind: "object",
      parent_object_type: ObjectStarter.object_type,
      parent_object_id: object_id,
      parent_object_command_id: command_id,
      parent_object_worker_id: "external-starter-worker",
      child_workflow_name: ChildEchoWorkflow.workflow_name,
      child_workflow_id: child_id,
      input:,
      worker_pool: "default",
      cancellation_policy: "abandon",
    )
    Durababble::ChildWorkflowHandle.new(
      ChildEchoWorkflow,
      child_id,
      store:,
      worker_pool: "default",
      cancellation_policy: "abandon",
    )
  end

  def run_workers_until_terminal(backend, store, workflow_id, workflows:, objects: [], max_ticks: 100)
    worker = worker_for(backend, store, workflows:, objects:)
    max_ticks.times do
      run = store.workflow(workflow_id)
      return run if Durababble::WorkflowStatus.terminal?(run)

      worker.run_until_idle(max_ticks: 20)
      run = store.workflow(workflow_id)
      return run if Durababble::WorkflowStatus.terminal?(run)

      if (wake_at = run["next_run_at"])
        make_workflow_timer_due(store, workflow_id, at: wake_at)
        with_store_current_time(store, wake_at) { worker.run_until_idle(max_ticks: 20) }
      end
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
