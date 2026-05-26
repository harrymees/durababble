# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_stuck_inbox(seed)
        run(seed, "bug_stuck_inbox") do |h|
          # An inbox message claimed (`running`) by a crashed worker, its lease
          # already expired and never reclaimed — the stuck-inbox checker must
          # flag it (symmetric to the stuck-fence/stuck-outbox fixtures). The
          # inbox is target-oriented, so no workflow row is required.
          h.store.inject_inbox({
            "id" => "stuck-inbox",
            "status" => "running",
            "locked_by" => "crashed-worker",
            "locked_until" => h.scheduler.time - 1,
          })
        end
      end
    end
  end
end
