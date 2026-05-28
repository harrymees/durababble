# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_stuck_activation(seed)
        run(seed, "bug_stuck_activation") do |h|
          # A target_activation (the #69 wakeup row) claimed (`running`) by a
          # crashed worker, its lease already expired and never reclaimed by
          # `claim_target_activation` — the stuck-activation checker must flag
          # it (completes the lease-reclaim quartet alongside
          # fence/outbox/inbox). Composite-keyed, so no workflow row is required.
          h.store.inject_target_activation({
            "worker_pool" => "default",
            "target_kind" => "workflow",
            "target_type" => "counter",
            "target_id" => "stuck-#{seed}",
            "status" => "running",
            "ready_at" => h.scheduler.time - 5,
            "locked_by" => "crashed-worker",
            "locked_until" => h.scheduler.time - 1,
          })
        end
      end
    end
  end
end
