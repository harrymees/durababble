# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleStatusTest < DurababbleTestCase
  test "status predicates accept raw statuses and rows" do
    assert Durababble::WorkflowStatus.completed?(Durababble::WorkflowStatus::COMPLETED)
    assert Durababble::AttemptStatus.live?({ "status" => Durababble::AttemptStatus::RUNNING })
  end
end
