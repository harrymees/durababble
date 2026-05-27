# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_stuck_outbox(seed)
        run(seed, "bug_stuck_outbox") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id)
          # An outbox message left `processing` by a crashed holder, its lease
          # already expired and never reclaimed — the stuck-outbox checker must
          # flag it (symmetric to the stuck-fence fixture).
          h.store.inject_outbox({
            "id" => "stuck-outbox",
            "workflow_id" => id,
            "topic" => "email",
            "payload" => {},
            "key" => "stuck-outbox",
            "status" => "processing",
            "locked_by" => "crashed-worker",
            "locked_until" => h.scheduler.time - 1,
          })
        end
      end
    end
  end
end
