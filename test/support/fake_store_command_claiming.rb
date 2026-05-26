# typed: false
# frozen_string_literal: true

module Durababble
  module TestSupport
    # Shared fake-store implementation of Store#claim_next_workflow_command.
    #
    # The real SqlStore#claim_next_workflow_command is pure orchestration over
    # target_activation, workflow_owned?, and claim_inbox_messages. The in-memory fakes
    # used by the engine/worker/observability unit tests (and the deterministic virtual
    # store) already implement those three methods, so they include this module instead of
    # each copying the body and risking drift from one another.
    module FakeStoreCommandClaiming
      def claim_next_workflow_command(worker_pool:, workflow_name:, workflow_id:, worker_id:, lease_seconds:)
        return unless target_activation(worker_pool:, target_kind: "workflow", target_type: workflow_name, target_id: workflow_id)
        raise Durababble::LeaseConflict, "workflow #{workflow_id} lease lost" unless workflow_owned?(workflow_id:, worker_id:)

        claim_inbox_messages(
          worker_pool:,
          target_kind: "workflow",
          target_type: workflow_name,
          target_id: workflow_id,
          worker_id:,
          lease_seconds:,
          limit: 1,
        ).first
      end
    end
  end
end
