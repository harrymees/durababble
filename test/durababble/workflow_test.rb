# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowTest < DurababbleTestCase
  class ApiTestChargeFailed < StandardError; end

  class ApiTestOrderWorkflow < Durababble::Workflow
    expose def status
      "queryable"
    end

    expose_command def cancel(reason:)
      reason
    end

    def execute(input)
      charged = charge(input)
      finish(charged)
    end

    step retry: { maximum_attempts: 3, initial_interval: 1 }
    def charge(input)
      raise ApiTestChargeFailed, "declined" if input.fetch("decline", false)

      input.merge("idempotency_key" => step_context.idempotency_key)
    end

    step def finish(input)
      input.merge("finished" => true)
    end
  end

  class ApiTestApprovalWorkflow < Durababble::Workflow
    workflow_name "api-test-approval-workflow"

    def execute(input)
      wait_event("workflow:#{workflow_id}:command:approve", input.merge("waiting_for" => "approve"))
    end

    expose_command def approve(reason:)
      reason
    end
  end

  test "registers class-oriented workflow steps and public exposed methods" do
    assert_equal "api_test_order_workflow", ApiTestOrderWorkflow.workflow_name
    assert_equal [:charge, :finish], ApiTestOrderWorkflow.step_order
    assert_equal 3, ApiTestOrderWorkflow.step_definition(:charge).retry_policy.maximum_attempts
    assert_hash_includes ApiTestOrderWorkflow.exposed_queries, status: true
    assert_includes ApiTestOrderWorkflow.exposed_commands.keys, :cancel
  end

  test "does not expose the removed Workflow.define DSL" do
    assert_not_respond_to Durababble::Workflow, :define
  end

  test "derives fallback names for anonymous workflows" do
    anonymous_workflow = Class.new(Durababble::Workflow)

    assert_match(/\A\d+\z/, anonymous_workflow.workflow_name)
  end

  test "ignores unknown pending durable macros while preserving method order identity" do
    odd_workflow = Class.new(Durababble::Workflow)
    odd_workflow.instance_variable_set(:@pending_durable_macro, [:unknown, {}])
    odd_workflow.class_eval { def ignored_macro = true }
    odd_workflow.class_eval do
      def repeat(input) = input
      step :repeat
      step :repeat
    end

    assert_equal [:repeat], odd_workflow.step_order
  end

  test "passes positional arguments to workflow ref query methods" do
    positional_query = Class.new(Durababble::Workflow) do
      expose def describe(prefix)
        "#{prefix}:#{@__durababble_ref_workflow_id}"
      end
    end

    assert_equal "wf:wf-1", positional_query.ref("wf-1", store: Object.new).describe("wf")
  end

  durababble_store_backends.each do |backend|
    test "parks and resumes workflow-level event waits without step rows with #{backend.name}" do
      with_durababble_store(backend, "workflow_waits") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-event-wait"

          def execute(input)
            approval = wait_event("approval:#{input.fetch("id")}", input.merge("waiting_for" => "approval"))
            finish(approval)
          end

          step def finish(input)
            input.merge("finished" => true)
          end
        end
        worker = Durababble::Worker.new(store:, workflows: { workflow.workflow_name => workflow }, worker_id: "worker-a")
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "direct" })

        assert_equal :worked, worker.tick
        assert_equal "waiting", store.workflow(workflow_id).fetch("status")
        assert_empty store.steps_for(workflow_id)
        assert_hash_includes(
          store.waits_for(workflow_id).first,
          "scope" => "workflow",
          "kind" => "event",
          "event_key" => "approval:direct",
          "status" => "pending",
        )

        assert_equal 1, store.signal_event("approval:direct", payload: { "approved" => true })
        assert_equal :worked, worker.tick

        assert_hash_includes(
          store.workflow(workflow_id),
          "status" => "completed",
          "result" => {
            "id" => "direct",
            "waiting_for" => "approval",
            "approved" => true,
            "finished" => true,
          },
        )
        assert_equal [["finish", "completed"]], store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] }
        assert_equal ["completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
      end
    end

    test "parks and resumes workflow-level timer waits returned from execute with #{backend.name}" do
      with_durababble_store(backend, "workflow_waits") do |store|
        store.migrate!
        wake_at = Time.now + 3600
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "direct-timer-wait"

          def execute(input)
            result = Durababble.wait_until(input.fetch("wake_at"), input.merge("slept" => true))
            result.merge("finished" => true)
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "wake_at" => wake_at })

        waiting = Durababble::Engine.new(store:, worker_id: "timer-worker").resume(workflow, workflow_id:)
        assert_equal "waiting", waiting.status
        assert_empty store.steps_for(workflow_id)
        assert_hash_includes store.waits_for(workflow_id).first, "scope" => "workflow", "kind" => "timer", "status" => "pending"

        assert_equal 0, store.wake_due_timers(now: wake_at - 1)
        assert_equal 1, store.wake_due_timers(now: wake_at + 1)
        run = Durababble::Engine.new(store:, worker_id: "timer-worker").resume(workflow, workflow_id:)

        assert_equal "completed", run.status
        assert_hash_includes run.result, "slept" => true, "finished" => true
        assert_empty store.steps_for(workflow_id)
      end
    end

    test "persists exposed workflow commands as durable event waits with #{backend.name}" do
      with_durababble_store(backend, "workflow_commands") do |store|
        worker = Durababble::Worker.new(
          store:,
          workflows: { ApiTestApprovalWorkflow.workflow_name => ApiTestApprovalWorkflow },
          worker_id: "command-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: ApiTestApprovalWorkflow.workflow_name,
          input: { "request_id" => "approval-request" },
        )

        assert_equal :worked, worker.tick
        assert_equal "waiting", store.workflow(workflow_id).fetch("status")

        ref = ApiTestApprovalWorkflow.ref(workflow_id, store:)
        assert_equal 1, ref.approve(reason: "operator")
        assert_equal :worked, worker.tick

        assert_hash_includes(
          store.workflow(workflow_id),
          "status" => "completed",
          "result" => {
            "request_id" => "approval-request",
            "waiting_for" => "approve",
            "method" => "approve",
            "args" => [],
            "kwargs" => { reason: "operator" },
          },
        )
        assert_equal ["completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
      end
    end

    test "rejects waits from durable steps with #{backend.name}" do
      with_durababble_store(backend, "workflow_waits") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "step-wait-rejected"

          def execute(input)
            wait_inside_step(input)
          end

          step def wait_inside_step(input)
            wait_event("approval:#{input.fetch("id")}", input)
          end
        end
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "step" })

        run = Durababble::Engine.new(store:, worker_id: "step-wait-worker").resume(workflow, workflow_id:)

        assert_equal "failed", run.status
        assert_match(/workflow-level only/, run.error)
        assert_empty store.waits_for(workflow_id)
        assert_equal ["failed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end
  end
end
