# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_abandoned_runnable_workflow(seed)
        run(seed, "bug_abandoned_runnable_workflow") do |h|
          h.expect_settled!
          h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          # No workers ever run: the workflow is left pending and immediately
          # runnable, which the liveness checker must flag.
        end
      end
    end
  end
end
