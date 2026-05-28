# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleStatusTest < DurababbleTestCase
  test "status predicates accept raw statuses and rows" do
    assert Durababble::WorkflowStatus.completed?(Durababble::WorkflowStatus::COMPLETED)
    assert Durababble::WorkflowStatus.terminal?({ "status" => Durababble::WorkflowStatus::TERMINATED, "next_run_at" => nil })
    assert Durababble::WorkflowStatus.rpc_not_running?({ "status" => Durababble::WorkflowStatus::CANCELED, "next_run_at" => nil })
    assert Durababble::WorkflowStatus.rpc_not_running?({ "status" => Durababble::WorkflowStatus::FAILED, "next_run_at" => nil })
    refute Durababble::WorkflowStatus.rpc_not_running?({ "status" => Durababble::WorkflowStatus::FAILED, "next_run_at" => Time.now })
    assert Durababble::AttemptStatus.live?({ "status" => Durababble::AttemptStatus::RUNNING })
    assert Durababble::AttemptStatus.live?(Durababble::AttemptStatus::WAITING)
    refute Durababble::AttemptStatus.live?(Durababble::AttemptStatus::COMPLETED)
  end
end
