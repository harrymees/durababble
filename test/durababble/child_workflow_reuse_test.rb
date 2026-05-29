# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleChildWorkflowReuseTest < DurababbleTestCase
  def reuse_args(child, **overrides)
    {
      origin_kind: "workflow",
      child_workflow_name: "child-echo",
      child_workflow_id: "child-1",
      input: { "value" => "v" },
      worker_pool: "default",
      cancellation_policy: "abandon",
      parent_workflow_id: "parent-1",
      parent_command_id: 7,
    }.merge(overrides).then do |args|
      Durababble::ChildWorkflowReuse.validate!(child, **args)
    end
  end

  def child_row(colocation_group_id: nil)
    {
      "origin_kind" => "workflow",
      "parent_workflow_id" => "parent-1",
      "parent_command_id" => 7,
      "parent_object_type" => nil,
      "parent_object_id" => nil,
      "parent_object_command_id" => nil,
      "child_workflow_name" => "child-echo",
      "child_workflow_id" => "child-1",
      "input" => { "value" => "v" },
      "worker_pool" => "default",
      "cancellation_policy" => "abandon",
      "colocation_group_id" => colocation_group_id,
    }
  end

  test "matching non-colocated reuse does not raise" do
    reuse_args(child_row, colocate: false)
  end

  test "matching colocated reuse does not raise when the prior child carried a group" do
    reuse_args(child_row(colocation_group_id: "wf:parent-1"), colocate: true)
  end

  test "requesting colocation against a previously non-colocated child is a conflict" do
    error = assert_raises(Durababble::IdempotencyKeyConflict) do
      reuse_args(child_row, colocate: true)
    end
    assert_match(/workflow id child-1 already used for a different child workflow/, error.message)
  end

  test "dropping colocation against a previously colocated child is a conflict" do
    assert_raises(Durababble::IdempotencyKeyConflict) do
      reuse_args(child_row(colocation_group_id: "wf:parent-1"), colocate: false)
    end
  end

  test "an unrelated mismatch still raises regardless of colocation intent" do
    assert_raises(Durababble::IdempotencyKeyConflict) do
      reuse_args(child_row, input: { "value" => "different" }, colocate: false)
    end
  end
end
