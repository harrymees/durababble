# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_unmet_effect_expectation(seed)
        run(seed, "bug_unmet_effect_expectation") do |h|
          h.expect_side_effects(1)
          h.expect_processed_outbox(1)
          h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          # No fence is ever acquired and no outbox message is processed, so the
          # declared exactly-once expectations are violated.
        end
      end
    end
  end
end
