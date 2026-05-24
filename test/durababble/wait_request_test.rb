# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWaitRequestTest < DurababbleTestCase
  FakeWorkflowExecution = Struct.new(:requests, keyword_init: true) do
    def wait(wait_request)
      requests << wait_request
      { "kind" => wait_request.kind, "event_key" => wait_request.event_key, "context" => wait_request.context }
    end
  end

  test "wait_until rejects non-workflow callers" do
    wake_at = Time.utc(2026, 1, 2, 3, 4, 5)
    context = { "request_id" => "r1" }

    error = assert_raises(Durababble::Error) { Durababble.wait_until(wake_at, context) }

    assert_match(/wait_until must be called from workflow execution/, error.message)
    assert_equal({ "request_id" => "r1" }, context)
  end

  test "wait_event rejects non-workflow callers" do
    context = { "request_id" => "r1", "attempt" => 2 }

    error = assert_raises(Durababble::Error) { Durababble.wait_event("approval:r1", context) }

    assert_match(/wait_event must be called from workflow execution/, error.message)
    assert_equal({ "request_id" => "r1", "attempt" => 2 }, context)
  end

  test "wait_event delegates to workflow execution from thread scoped context" do
    execution = FakeWorkflowExecution.new(requests: [])

    result = Durababble.with_workflow_execution(execution) do
      Durababble.wait_event("approval:r1", { "request_id" => "r1" })
    end

    assert_equal(
      { "kind" => "event", "event_key" => "approval:r1", "context" => { "request_id" => "r1" } },
      result,
    )
    assert_equal 1, execution.requests.length
    assert_hash_includes(
      {
        "kind" => execution.requests.first.kind,
        "event_key" => execution.requests.first.event_key,
        "context" => execution.requests.first.context,
      },
      "kind" => "event",
      "event_key" => "approval:r1",
      "context" => { "request_id" => "r1" },
    )
  end

  test "wait_until delegates to workflow execution from thread scoped context" do
    execution = FakeWorkflowExecution.new(requests: [])
    wake_at = Time.utc(2026, 1, 2, 3, 4, 5)

    result = Durababble.with_workflow_execution(execution) do
      Durababble.wait_until(wake_at, { "request_id" => "r2" })
    end

    assert_equal(
      { "kind" => "timer", "event_key" => nil, "context" => { "request_id" => "r2" } },
      result,
    )
    assert_equal 1, execution.requests.length
    assert_equal wake_at, execution.requests.first.wake_at
  end

  test "wait helpers reject durable step context before workflow execution context" do
    execution = FakeWorkflowExecution.new(requests: [])

    error = Durababble::StepExecutionContext.with_current(Object.new) do
      assert_raises(Durababble::Error) do
        Durababble.with_workflow_execution(execution) do
          Durababble.wait_event("approval:r1", { "request_id" => "r1" })
        end
      end
    end

    assert_match(/workflow-level only/, error.message)
    assert_empty execution.requests
  end
end
