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
end
