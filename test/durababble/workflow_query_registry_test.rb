# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowQueryRegistryTest < DurababbleTestCase
  class QueryWorkflow < Durababble::Workflow
    expose def keyword_status(prefix:)
      "#{prefix}:ok"
    end
  end

  test "dispatches registered workflow queries with keyword arguments" do
    registry = Durababble::WorkflowQueryRegistry.new
    workflow = QueryWorkflow.new

    result = registry.register("wf-query", QueryWorkflow, workflow) do
      registry.call(workflow_id: "wf-query", method_name: :keyword_status, args: [], kwargs: { prefix: "ready" })
    end

    assert_equal("ready:ok", result)
  end

  test "raises for inactive leases and unknown query methods" do
    registry = Durababble::WorkflowQueryRegistry.new
    workflow = QueryWorkflow.new

    assert_raises(Durababble::WorkflowRpc::NoActiveLease) do
      registry.call(workflow_id: "wf-missing", method_name: :keyword_status, args: [], kwargs: { prefix: "ready" })
    end

    registry.register("wf-query", QueryWorkflow, workflow) do
      assert_raises(Durababble::WorkflowRpc::UnknownCommand) do
        registry.call(workflow_id: "wf-query", method_name: :missing, args: [], kwargs: {})
      end
    end
  end

  test "does not remove a newer registration when an older activation exits" do
    registry = Durababble::WorkflowQueryRegistry.new
    outer_workflow = QueryWorkflow.new
    inner_workflow = QueryWorkflow.new

    registry.register("wf-query", QueryWorkflow, outer_workflow) do
      registry.register("wf-query", QueryWorkflow, inner_workflow) do
        assert_equal(
          "inner:ok",
          registry.call(workflow_id: "wf-query", method_name: :keyword_status, args: [], kwargs: { prefix: "inner" }),
        )
      end
    end
  end
end
