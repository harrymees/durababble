# typed: true
# frozen_string_literal: true

module Durababble
  module ChildWorkflowReuse
    class << self
      #: (Hash[String, Object?], origin_kind: String, child_workflow_name: String, child_workflow_id: String, input: Object?, worker_pool: String, cancellation_policy: String, ?parent_workflow_id: String?, ?parent_command_id: Integer?, ?parent_object_type: String?, ?parent_object_id: String?, ?parent_object_command_id: String?) -> void
      def validate!(child, origin_kind:, child_workflow_name:, child_workflow_id:, input:, worker_pool:, cancellation_policy:, parent_workflow_id: nil, parent_command_id: nil, parent_object_type: nil, parent_object_id: nil, parent_object_command_id: nil)
        mismatch = child.fetch("origin_kind") != origin_kind ||
          child["parent_workflow_id"] != parent_workflow_id ||
          child["parent_command_id"] != parent_command_id ||
          child["parent_object_type"] != parent_object_type ||
          child["parent_object_id"] != parent_object_id ||
          child["parent_object_command_id"] != parent_object_command_id ||
          child.fetch("child_workflow_name") != child_workflow_name ||
          child.fetch("child_workflow_id") != child_workflow_id ||
          child.fetch("input") != input ||
          child.fetch("worker_pool") != worker_pool ||
          child.fetch("cancellation_policy") != cancellation_policy
        return unless mismatch

        raise IdempotencyKeyConflict, "workflow id #{child_workflow_id} already used for a different child workflow"
      end
    end
  end
end
