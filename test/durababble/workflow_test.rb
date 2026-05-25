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
      wait_for_approval(input)
    end

    step def wait_for_approval(input)
      Durababble.wait_event(
        "workflow:#{step_context.workflow_id}:command:approve",
        input.merge("waiting_for" => "approve"),
      )
    end

    expose_command def approve(reason:)
      { "approved_by" => reason }
    end

    expose_command def reject(reason:)
      raise ApiTestChargeFailed, reason
    end
  end

  class TerminalRaceCommandStore
    attr_reader :enqueued

    def initialize
      @enqueued = false
    end

    def migrate!; end

    def workflow(workflow_id)
      { "id" => workflow_id, "status" => "running", "next_run_at" => nil }
    end

    def enqueue_workflow_command(workflow_id:, workflow_name:, method_name:, payload:, idempotency_key:)
      raise Durababble::Error, "workflow #{workflow_id} is terminal"
    end

    def deliver_target_message(**)
      @enqueued = true
    end

    def enqueue_inbox_message(**)
      @enqueued = true
      "raced-command"
    end

    def wait_for_inbox_message(message_id, poll_interval: 0.05, timeout: 10)
      raise Durababble::CommandTimeout, "timed out waiting for inbox message #{message_id}"
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

  test "uses the store's atomic workflow command enqueue path" do
    store = TerminalRaceCommandStore.new
    error = assert_raises(Durababble::Error) do
      ApiTestApprovalWorkflow.ref("wf-race", store:).approve(reason: "late")
    end

    assert_match(/terminal/, error.message)
    refute store.enqueued
  end

  durababble_store_backends.each do |backend|
    test "runs exposed workflow commands from durable inbox activations and returns results with #{backend.name}" do
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

        assert_equal(:worked, worker.tick)
        assert_equal("waiting", store.workflow(workflow_id).fetch("status"))

        result_queue = Queue.new
        caller = Thread.new do
          caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
          begin
            result_queue << [:ok, ApiTestApprovalWorkflow.ref(workflow_id, store: caller_store).approve(reason: "operator")]
          rescue StandardError => e
            result_queue << [:error, e]
          ensure
            caller_store.close
          end
        end

        wait_until { store.target_activation(target_kind: "workflow", target_type: ApiTestApprovalWorkflow.workflow_name, target_id: workflow_id) }
        workflow_messages = store.inbox_messages_for(
          target_kind: "workflow",
          target_type: ApiTestApprovalWorkflow.workflow_name,
          target_id: workflow_id,
        )
        assert_equal(1, workflow_messages.length)
        assert_hash_includes(
          workflow_messages.first,
          "message_kind" => "workflow_command",
          "method_name" => "approve",
          "payload" => { "method" => "approve", "args" => [], "kwargs" => { reason: "operator" } },
        )
        assert_equal(:worked, worker.tick)
        status, value = result_queue.pop
        caller.join
        assert_equal(:ok, status)
        assert_equal({ "approved_by" => "operator" }, value)

        assert_hash_includes(store.workflow(workflow_id), "status" => "waiting")
        assert_equal(["pending"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") })
        assert_nil(store.target_activation(target_kind: "workflow", target_type: ApiTestApprovalWorkflow.workflow_name, target_id: workflow_id))
        assert_hash_includes(store.inbox_message(workflow_messages.first.fetch("id")), "status" => "completed", "result" => { "approved_by" => "operator" })
        assert_includes(store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }, "workflow_command_completed")
      ensure
        caller&.kill if caller&.alive?
      end
    end

    test "returns exposed workflow command errors through the ask row with #{backend.name}" do
      with_durababble_store(backend, "workflow_command_errors") do |store|
        store.migrate!
        worker = Durababble::Worker.new(
          store:,
          workflows: { ApiTestApprovalWorkflow.workflow_name => ApiTestApprovalWorkflow },
          worker_id: "command-error-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: ApiTestApprovalWorkflow.workflow_name,
          input: { "request_id" => "approval-error-request" },
        )

        assert_equal(:worked, worker.tick)
        result_queue = Queue.new
        caller = Thread.new do
          caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
          begin
            ApiTestApprovalWorkflow.ref(workflow_id, store: caller_store).reject(reason: "no")
            result_queue << [:ok, nil]
          rescue StandardError => e
            result_queue << [:error, e]
          ensure
            caller_store.close
          end
        end

        wait_until { store.target_activation(target_kind: "workflow", target_type: ApiTestApprovalWorkflow.workflow_name, target_id: workflow_id) }
        assert_equal(:worked, worker.tick)
        status, error = result_queue.pop
        caller.join
        assert_equal(:error, status)
        assert_kind_of(Durababble::Error, error)
        assert_match(/ApiTestChargeFailed: no/, error.message)

        message = store.inbox_messages_for(target_kind: "workflow", target_type: ApiTestApprovalWorkflow.workflow_name, target_id: workflow_id).first
        assert_hash_includes(message, "status" => "dead_lettered")
        assert_nil(store.target_activation(target_kind: "workflow", target_type: ApiTestApprovalWorkflow.workflow_name, target_id: workflow_id))
        assert_includes(store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }, "workflow_command_failed")
      ensure
        caller&.kill if caller&.alive?
      end
    end
  end

  private

  def wait_until(timeout: 2)
    deadline = Time.now + timeout
    loop do
      value = yield
      return value if value
      raise "condition not met before timeout" if Time.now >= deadline

      sleep(0.01)
    end
  end
end
