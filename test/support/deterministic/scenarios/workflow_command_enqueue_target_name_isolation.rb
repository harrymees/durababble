# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_command_enqueue_target_name_isolation(seed)
        run(seed, "workflow_command_enqueue_target_name_isolation") do |h|
          h.store.enqueue_workflow(name: "counter", input: {}, id: "wf-name")

          wrong_command_id = nil
          error = nil
          begin
            wrong_command_id = h.store.enqueue_workflow_command(
              workflow_id: "wf-name",
              workflow_name: "other_counter",
              method_name: "approve",
              payload: { "method" => "approve", "args" => [], "kwargs" => {} },
              idempotency_key: "wrong-name",
            )
          rescue Durababble::Error => e
            error = e.message
          end

          wrong_messages = h.store.inbox_messages_for(
            target_kind: "workflow",
            target_type: "other_counter",
            target_id: "wf-name",
          )
          wrong_activation = h.store.target_activation(
            target_kind: "workflow",
            target_type: "other_counter",
            target_id: "wf-name",
          )
          h.scheduler.trace.event(
            h.scheduler.time,
            "command-enqueue",
            "wrong_workflow_command_name",
            enqueued: !!wrong_command_id,
            wrong_messages: wrong_messages.length,
            wrong_activation: !!wrong_activation,
            error:,
          )

          h.check("workflow commands must be enqueued for the persisted workflow name") do
            wrong_command_id.nil? && error&.include?("workflow wf-name is counter, not other_counter")
          end
          h.check("wrong workflow command target has no inbox messages or activation") do
            wrong_messages.empty? && wrong_activation.nil?
          end
        end
      end
    end
  end
end
