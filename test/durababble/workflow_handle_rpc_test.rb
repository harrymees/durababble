# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowHandleRpcTest < DurababbleTestCase
  class HandleRpcCounter < Durababble::DurableObject
    object_type "handle_rpc_counter"

    def initialize_state
      { "count" => 0, "attempts" => 0 }
    end

    expose_command retry: { maximum_attempts: 2, schedule: [0] }
    def add(amount)
      update_state(current_state.merge("count" => current_state.fetch("count") + amount))
      current_state.fetch("count")
    end

    expose_command retry: { maximum_attempts: 2, schedule: [0] }
    def flaky_add(amount)
      raise "transient add failure" if command_context.attempt_number == 1

      update_state(current_state.merge("count" => current_state.fetch("count") + amount))
      current_state.fetch("count")
    end

    expose_command def fail
      raise ArgumentError, "bad counter command"
    end

    expose def count
      current_state.fetch("count")
    end
  end

  class HandleRpcApproval < Durababble::Workflow
    workflow_name "handle-rpc-approval"

    def execute(input)
      sleep(3600, input)
    end

    expose def summary(prefix)
      "#{prefix}:#{@__durababble_ref_workflow_id}"
    end

    expose_command def approve(reason:)
      { "approved_by" => reason }
    end
  end

  class HandleRpcObjectCaller < Durababble::Workflow
    workflow_name "handle-rpc-object-caller"

    def execute(input)
      counter = HandleRpcCounter.at(input.fetch("object_id"))
      before_value = before(input.fetch("object_id"))
      first = counter.add(2)
      second = counter.add(3)
      after("before" => before_value, "first" => first, "second" => second, "query" => counter.count)
    end

    step def before(object_id)
      "before:#{object_id}"
    end

    step def after(result)
      result.merge("after" => true)
    end
  end

  class HandleRpcObjectTellCaller < Durababble::Workflow
    workflow_name "handle-rpc-object-tell-caller"

    def execute(input)
      message_id = HandleRpcCounter.tell(input.fetch("object_id"), :add, 7)
      { "message_id" => message_id }
    end
  end

  class HandleRpcCrashCaller < Durababble::Workflow
    workflow_name "handle-rpc-crash-caller"

    def execute(input)
      HandleRpcCounter.at(input.fetch("object_id")).add(1)
    end
  end

  class HandleRpcRetryCaller < Durababble::Workflow
    workflow_name "handle-rpc-retry-caller"

    def execute(input)
      HandleRpcCounter.at(input.fetch("object_id")).flaky_add(4)
    end
  end

  class HandleRpcFailingCaller < Durababble::Workflow
    workflow_name "handle-rpc-failing-caller"

    def execute(input)
      HandleRpcCounter.at(input.fetch("object_id")).fail
    end
  end

  class HandleRpcWorkflowCaller < Durababble::Workflow
    workflow_name "handle-rpc-workflow-caller"

    def execute(input)
      HandleRpcApproval.handle(input.fetch("workflow_id")).approve(reason: "workflow")
    end
  end

  class HandleRpcWorkflowQueryCaller < Durababble::Workflow
    workflow_name "handle-rpc-workflow-query-caller"

    def execute(input)
      HandleRpcApproval.handle(input.fetch("workflow_id")).summary("queried")
    end
  end

  class HandleRpcWorkflowReadCaller < Durababble::Workflow
    workflow_name "handle-rpc-workflow-read-caller"

    def execute(input)
      handle = HandleRpcApproval.handle(input.fetch("workflow_id"))
      { "status" => handle.status, "result" => handle.result, "error" => handle.error }
    end
  end

  class HandleRpcWorkflowCancelCaller < Durababble::Workflow
    workflow_name "handle-rpc-workflow-cancel-caller"

    def execute(input)
      run = HandleRpcApproval.handle(input.fetch("workflow_id")).cancel(reason: "canceled from workflow")
      { "status" => run.status }
    end
  end

  class HandleRpcWorkflowTerminateCaller < Durababble::Workflow
    workflow_name "handle-rpc-workflow-terminate-caller"

    def execute(input)
      run = HandleRpcApproval.handle(input.fetch("workflow_id")).terminate(reason: "terminated from workflow")
      { "status" => run.status }
    end
  end

  class HandleRpcConcurrentObjectCaller < Durababble::Workflow
    workflow_name "handle-rpc-concurrent-object-caller"

    def execute(input)
      object_ids = input.fetch("object_ids")
      Async do |task|
        object_ids.each_with_index.map do |object_id, index|
          task.async { HandleRpcCounter.at(object_id).add(index + 1) }
        end.map(&:wait)
      end.wait
    end
  end

  durababble_store_backends.each do |backend|
    test "records workflow-to-durable-object handle RPCs as ordered workflow history with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_object_rpc") do |store|
        workflow_id = store.enqueue_workflow(name: HandleRpcObjectCaller.workflow_name, input: { "object_id" => "counter-1" })
        run = resume_with_worker(backend, HandleRpcObjectCaller, workflow_id, objects: [HandleRpcCounter])

        assert_equal "completed", run.status
        assert_equal(
          { "before" => "before:counter-1", "first" => 2, "second" => 5, "query" => 5, "after" => true },
          run.result,
        )
        assert_equal(
          [
            "before",
            "handle_rpc:object:handle_rpc_counter:add",
            "handle_rpc:object:handle_rpc_counter:add",
            "handle_rpc:object:handle_rpc_counter:count",
            "after",
          ],
          store.steps_for(workflow_id).map { |step| step.fetch("name") },
        )
        assert_equal ["completed"] * 5, store.steps_for(workflow_id).map { |step| step.fetch("status") }
        assert_equal 2, store.inbox_messages_for(target_kind: "object", target_type: HandleRpcCounter.object_type, target_id: "counter-1").length
      end
    end

    test "records class-level durable-object tells from workflow code with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_object_tell_rpc") do |store|
        workflow_id = store.enqueue_workflow(name: HandleRpcObjectTellCaller.workflow_name, input: { "object_id" => "counter-tell" })
        run = resume_with_worker(backend, HandleRpcObjectTellCaller, workflow_id, objects: [HandleRpcCounter])

        assert_equal "completed", run.status
        refute_empty run.result.fetch("message_id")
        assert_equal(
          ["handle_rpc:object:handle_rpc_counter:add"],
          store.steps_for(workflow_id).map { |step| step.fetch("name") },
        )
        assert_equal ["completed"], store.steps_for(workflow_id).map { |step| step.fetch("status") }
        assert_equal ["tell"], store.inbox_messages_for(target_kind: "object", target_type: HandleRpcCounter.object_type, target_id: "counter-tell").map { |message| message.fetch("message_kind") }
      end
    end

    test "records workflow-to-workflow handle commands from workflow code with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_workflow_rpc") do |store|
        approval_id = store.enqueue_workflow(name: HandleRpcApproval.workflow_name, input: { "request" => "approval" })
        waiting = Durababble::Engine.new(store:, worker_id: "approval-worker", migrate: false).resume(HandleRpcApproval, workflow_id: approval_id)
        assert_equal("waiting", waiting.status)

        caller_id = store.enqueue_workflow(name: HandleRpcWorkflowCaller.workflow_name, input: { "workflow_id" => approval_id })
        run = resume_with_worker(backend, HandleRpcWorkflowCaller, caller_id, workflows: [HandleRpcApproval])

        assert_equal "completed", run.status
        assert_equal({ "approved_by" => "workflow" }, run.result)
        assert_equal ["handle_rpc:workflow:handle-rpc-approval:approve"], store.steps_for(caller_id).map { |step| step.fetch("name") }
        assert_equal ["completed"], store.steps_for(caller_id).map { |step| step.fetch("status") }
        assert_equal 1, store.inbox_messages_for(target_kind: "workflow", target_type: HandleRpcApproval.workflow_name, target_id: approval_id).length
      end
    end

    test "recovery reuses an in-workflow handle RPC result without duplicating the outbound command with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_rpc_recovery") do |store|
        workflow_id = store.enqueue_workflow(name: HandleRpcCrashCaller.workflow_name, input: { "object_id" => "counter-recovery" })
        assert_raises(Durababble::InjectedCrash) do
          resume_with_worker(backend, HandleRpcCrashCaller, workflow_id, objects: [HandleRpcCounter], crash_after: :handle_rpc_completed)
        end

        messages = store.inbox_messages_for(target_kind: "object", target_type: HandleRpcCounter.object_type, target_id: "counter-recovery")
        assert_equal 1, messages.length
        assert_hash_includes messages.first, "status" => "completed", "result" => 1

        store.steal_expired_leases!(now: Time.now + 61)
        recovered = Durababble::Engine.new(store:, worker_id: "recovering-caller", migrate: false).resume(HandleRpcCrashCaller, workflow_id:)

        assert_equal "completed", recovered.status
        assert_equal 1, recovered.result
        assert_equal 1, store.inbox_messages_for(target_kind: "object", target_type: HandleRpcCounter.object_type, target_id: "counter-recovery").length
        assert_equal 1, HandleRpcCounter.at("counter-recovery", store:).count
      end
    end

    test "replay reuses a completed in-workflow handle RPC step without duplicating the outbound command with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_rpc_step_completed_replay") do |store|
        workflow_id = store.enqueue_workflow(name: HandleRpcCrashCaller.workflow_name, input: { "object_id" => "counter-step-replay" })
        assert_raises(Durababble::InjectedCrash) do
          resume_with_worker(backend, HandleRpcCrashCaller, workflow_id, objects: [HandleRpcCounter], crash_after: :step_completed)
        end

        assert_equal ["completed"], store.steps_for(workflow_id).map { |step| step.fetch("status") }
        assert_equal 1, store.inbox_messages_for(target_kind: "object", target_type: HandleRpcCounter.object_type, target_id: "counter-step-replay").length

        store.steal_expired_leases!(now: Time.now + 61)
        recovered = Durababble::Engine.new(store:, worker_id: "replaying-caller", migrate: false).resume(HandleRpcCrashCaller, workflow_id:)

        assert_equal "completed", recovered.status
        assert_equal 1, recovered.result
        assert_equal 1, store.inbox_messages_for(target_kind: "object", target_type: HandleRpcCounter.object_type, target_id: "counter-step-replay").length
        assert_equal 1, HandleRpcCounter.at("counter-step-replay", store:).count
      end
    end

    test "cancellation before an in-workflow handle RPC prevents outbound commands with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_rpc_cancel_before_send") do |store|
        workflow_id = store.enqueue_workflow(name: HandleRpcCrashCaller.workflow_name, input: { "object_id" => "counter-canceled" })
        HandleRpcCrashCaller.handle(workflow_id, store:).cancel(reason: "stop before handle RPC")
        canceled = Durababble::Engine.new(store:, worker_id: "cancel-before-handle-rpc", migrate: false).resume(HandleRpcCrashCaller, workflow_id:)

        assert_equal "canceled", canceled.status
        assert_empty store.steps_for(workflow_id)
        assert_empty store.inbox_messages_for(target_kind: "object", target_type: HandleRpcCounter.object_type, target_id: "counter-canceled")
      end
    end

    test "object command retry and terminal errors propagate through in-workflow handle RPCs with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_rpc_errors") do |store|
        retry_id = store.enqueue_workflow(name: HandleRpcRetryCaller.workflow_name, input: { "object_id" => "counter-retry" })
        retried = resume_with_worker(backend, HandleRpcRetryCaller, retry_id, objects: [HandleRpcCounter])
        assert_equal "completed", retried.status
        assert_equal 4, retried.result
        retry_message = store.inbox_messages_for(target_kind: "object", target_type: HandleRpcCounter.object_type, target_id: "counter-retry").first
        assert_hash_includes retry_message, "status" => "completed", "attempts" => 2

        failing_id = store.enqueue_workflow(name: HandleRpcFailingCaller.workflow_name, input: { "object_id" => "counter-fail" })
        failed = resume_with_worker(backend, HandleRpcFailingCaller, failing_id, objects: [HandleRpcCounter])

        assert_equal "failed", failed.status
        assert_match(/ArgumentError: bad counter command/, failed.error)
        assert_equal ["failed"], store.steps_for(failing_id).map { |step| step.fetch("status") }
      end
    end

    test "records workflow query handle RPCs from workflow code with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_rpc_workflow_query") do |store|
        approval_id = store.enqueue_workflow(name: HandleRpcApproval.workflow_name, input: { "request" => "approval" })
        waiting = Durababble::Engine.new(store:, worker_id: "approval-worker", migrate: false).resume(HandleRpcApproval, workflow_id: approval_id)
        assert_equal("waiting", waiting.status)

        query_owner = "approval-query-owner"
        store.mark_workflow_running(approval_id, worker_id: query_owner, lease_seconds: 60)
        store.local_workflow_rpc_node_id = query_owner
        store.local_workflow_rpc_handlers = {
          "summary" => lambda do |payload|
            "#{payload.fetch("args").first}:#{payload.fetch("workflow_id")}"
          end,
        }

        caller_id = store.enqueue_workflow(name: HandleRpcWorkflowQueryCaller.workflow_name, input: { "workflow_id" => approval_id })
        run = Durababble::Engine.new(store:, worker_id: "query-caller", migrate: false).resume(HandleRpcWorkflowQueryCaller, workflow_id: caller_id)

        assert_equal("completed", run.status)
        assert_equal("queried:#{approval_id}", run.result)
        assert_equal(["handle_rpc:workflow:handle-rpc-approval:summary"], store.steps_for(caller_id).map { |step| step.fetch("name") })
        assert_equal(["completed"], store.steps_for(caller_id).map { |step| step.fetch("status") })
        assert_empty(store.inbox_messages_for(target_kind: "workflow", target_type: HandleRpcApproval.workflow_name, target_id: approval_id))
      ensure
        if store
          store.local_workflow_rpc_node_id = nil
          store.local_workflow_rpc_handlers = nil
        end
      end
    end

    test "reads workflow status, result, and error from workflow code with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_rpc_workflow_reads") do |store|
        approval_id = store.enqueue_workflow(name: HandleRpcApproval.workflow_name, input: { "request" => "approval" })
        waiting = Durababble::Engine.new(store:, worker_id: "approval-worker", migrate: false).resume(HandleRpcApproval, workflow_id: approval_id)
        assert_equal "waiting", waiting.status

        caller_id = store.enqueue_workflow(name: HandleRpcWorkflowReadCaller.workflow_name, input: { "workflow_id" => approval_id })
        run = Durababble::Engine.new(store:, worker_id: "read-caller", migrate: false).resume(HandleRpcWorkflowReadCaller, workflow_id: caller_id)

        assert_equal "completed", run.status
        assert_equal({ "status" => "waiting", "result" => nil, "error" => nil }, run.result)
        assert_equal(
          [
            "handle_rpc:workflow:handle-rpc-approval:status",
            "handle_rpc:workflow:handle-rpc-approval:result",
            "handle_rpc:workflow:handle-rpc-approval:error",
          ],
          store.steps_for(caller_id).map { |step| step.fetch("name") },
        )
        assert_equal ["completed"] * 3, store.steps_for(caller_id).map { |step| step.fetch("status") }
      end
    end

    test "cancels a workflow from workflow code through in-workflow handle RPCs with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_rpc_workflow_cancel") do |store|
        approval_id = store.enqueue_workflow(name: HandleRpcApproval.workflow_name, input: { "request" => "approval" })
        waiting = Durababble::Engine.new(store:, worker_id: "approval-worker", migrate: false).resume(HandleRpcApproval, workflow_id: approval_id)
        assert_equal "waiting", waiting.status

        caller_id = store.enqueue_workflow(name: HandleRpcWorkflowCancelCaller.workflow_name, input: { "workflow_id" => approval_id })
        run = Durababble::Engine.new(store:, worker_id: "cancel-caller", migrate: false).resume(HandleRpcWorkflowCancelCaller, workflow_id: caller_id)

        assert_equal "completed", run.status
        assert_equal({ "status" => "canceling" }, run.result)
        assert_equal ["handle_rpc:workflow:handle-rpc-approval:cancel"], store.steps_for(caller_id).map { |step| step.fetch("name") }
        assert_equal ["completed"], store.steps_for(caller_id).map { |step| step.fetch("status") }

        canceled = Durababble::Engine.new(store:, worker_id: "approval-finish", migrate: false).resume(HandleRpcApproval, workflow_id: approval_id)
        assert_equal "canceled", canceled.status
      end
    end

    test "terminates a workflow from workflow code through in-workflow handle RPCs with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_rpc_workflow_terminate") do |store|
        approval_id = store.enqueue_workflow(name: HandleRpcApproval.workflow_name, input: { "request" => "approval" })
        waiting = Durababble::Engine.new(store:, worker_id: "approval-worker", migrate: false).resume(HandleRpcApproval, workflow_id: approval_id)
        assert_equal "waiting", waiting.status

        caller_id = store.enqueue_workflow(name: HandleRpcWorkflowTerminateCaller.workflow_name, input: { "workflow_id" => approval_id })
        run = Durababble::Engine.new(store:, worker_id: "terminate-caller", migrate: false).resume(HandleRpcWorkflowTerminateCaller, workflow_id: caller_id)

        assert_equal "completed", run.status
        assert_equal({ "status" => "terminated" }, run.result)
        assert_equal ["handle_rpc:workflow:handle-rpc-approval:terminate"], store.steps_for(caller_id).map { |step| step.fetch("name") }
        assert_equal ["completed"], store.steps_for(caller_id).map { |step| step.fetch("status") }
        assert_equal "terminated", store.workflow(approval_id).fetch("status")
      end
    end

    test "records concurrent in-workflow handle RPCs fanned out over the shared store with #{backend.name}" do
      with_durababble_store(backend, "workflow_handle_rpc_concurrent") do |store|
        object_ids = ["concurrent-counter-0", "concurrent-counter-1", "concurrent-counter-2", "concurrent-counter-3"]
        workflow_id = store.enqueue_workflow(
          name: HandleRpcConcurrentObjectCaller.workflow_name,
          input: { "object_ids" => object_ids },
        )
        run = resume_with_worker(backend, HandleRpcConcurrentObjectCaller, workflow_id, objects: [HandleRpcCounter])

        assert_equal "completed", run.status
        # Each branch increments a distinct counter from 0, so its running total is deterministic
        # even though the four handle RPCs are in flight against the shared store at once.
        assert_equal [1, 2, 3, 4], run.result
        steps = store.steps_for(workflow_id)
        assert_equal ["handle_rpc:object:handle_rpc_counter:add"] * 4, steps.map { |step| step.fetch("name") }
        assert_equal ["completed"] * 4, steps.map { |step| step.fetch("status") }
        object_ids.each_with_index do |object_id, index|
          assert_equal index + 1, HandleRpcCounter.at(object_id, store:).count
          assert_equal 1, store.inbox_messages_for(target_kind: "object", target_type: HandleRpcCounter.object_type, target_id: object_id).length
        end
      end
    end
  end

  test "rejects blocks passed to durable object commands" do
    error = assert_raises(ArgumentError) do
      HandleRpcCounter.at("counter-block", store: Object.new).add(1) { :nope }
    end
    assert_match(/blocks cannot be passed/, error.message)
  end

  test "rejects blocks passed to durable object tells" do
    error = assert_raises(ArgumentError) do
      HandleRpcCounter.tell("counter-block", :add, 1, store: Object.new) { :nope }
    end
    assert_match(/blocks cannot be passed/, error.message)
  end

  test "rejects blocks passed to workflow commands" do
    error = assert_raises(ArgumentError) do
      HandleRpcApproval.handle("wf-block", store: Object.new).approve(reason: "later") { :nope }
    end
    assert_match(/blocks cannot be passed/, error.message)
  end

  private

  def resume_with_worker(backend, workflow_class, workflow_id, workflows: {}, objects: [], crash_after: nil)
    worker_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
    caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
    target_worker_id = "handle-rpc-target-worker-#{workflow_id}"
    caller_worker_id = "handle-rpc-caller-worker-#{workflow_id}"
    claimed = caller_store.claim_workflow(
      workflow_id:,
      worker_id: caller_worker_id,
      lease_seconds: Durababble::Engine::DEFAULT_LEASE_SECONDS,
    )
    raise Durababble::LeaseConflict, "workflow #{workflow_id} is leased by another worker" unless claimed

    worker = Durababble::Worker.new(
      store: worker_store,
      workflows:,
      objects:,
      worker_id: target_worker_id,
      migrate: false,
    )
    queue = Queue.new
    caller = Thread.new do
      result = Durababble::Engine.new(
        store: caller_store,
        worker_id: caller_worker_id,
        crash_after:,
        migrate: false,
      ).resume(workflow_class, workflow_id:, claimed:)
      queue << [:ok, result]
    rescue StandardError => e
      queue << [:error, e]
    end

    deadline = Time.now + 20
    while queue.empty?
      worker.tick
      raise "workflow resume did not finish before timeout" if Time.now >= deadline

      sleep(0.01)
    end
    status, value = queue.pop
    caller.join
    raise value if status == :error

    value
  ensure
    caller&.kill if caller&.alive?
    begin
      caller_store&.close
    rescue StandardError
      nil
    end
    begin
      worker_store&.close
    rescue StandardError
      nil
    end
  end
end
