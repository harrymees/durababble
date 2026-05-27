# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_command_target_identity_isolation(seed)
        run(seed, "object_command_target_identity_isolation") do |h|
          command_id = h.store.enqueue_object_command(
            object_type: "counter",
            object_id: "object-a",
            method_name: "write",
            args: [],
            kwargs: {},
          )
          h.store.claim_object_command(command_id:, worker_id: "object-worker", lease_seconds: 30)

          wrong_complete = h.store.complete_object_command(
            command_id:,
            result: { "ok" => true },
            object_type: "counter",
            object_id: "object-b",
            state: { "value" => "wrong-object" },
            worker_id: "object-worker",
          )
          message = h.store.inbox_message(command_id)
          wrong_state = h.store.object_state(object_type: "counter", object_id: "object-b")
          h.scheduler.trace.event(
            h.scheduler.time,
            "object-worker",
            "wrong_object_command_target",
            completed: !!wrong_complete,
            status: message&.fetch("status"),
            wrong_state: !!wrong_state,
          )

          h.check("object command completion must match the command target object") do
            wrong_complete.nil? && message&.fetch("status") == "running" && message&.fetch("locked_by") == "object-worker"
          end
          h.check("wrong target object state is unchanged") do
            wrong_state.nil?
          end
        end
      end
    end
  end
end
