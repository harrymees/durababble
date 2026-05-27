# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_lost_wakeup(seed)
        run(seed, "bug_lost_wakeup") do |h|
          # An activatable (pending) inbox head for a target with NO matching
          # target_activations row: reconcile is supposed to keep a wakeup row
          # alive while a non-dead-lettered head exists, so the absence means no
          # worker will ever be woken to drain this mailbox. The consistency
          # checker must flag it. We inject only the inbox message (no activation).
          h.store.inject_inbox({
            "id" => "lost-#{seed}",
            "worker_pool" => "default",
            "target_kind" => "workflow",
            "target_type" => "counter",
            "target_id" => "lost-#{seed}",
            "sequence" => 1,
            "status" => "pending",
          })
        end
      end
    end
  end
end
