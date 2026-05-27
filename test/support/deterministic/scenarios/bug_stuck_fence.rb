# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_stuck_fence(seed)
        run(seed, "bug_stuck_fence") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id)
          # A fence left running by a crashed holder, its lease already expired and
          # never reclaimed — the stuck-fence checker must flag it.
          h.store.inject_fence({
            "workflow_id" => id,
            "key" => "charge",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "locked_by" => "crashed-worker",
            "locked_until" => h.scheduler.time - 1,
          })
        end
      end
    end
  end
end
