# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_orphaned_activation(seed)
        run(seed, "bug_orphaned_activation") do |h|
          # The reverse of bug_lost_wakeup: a target_activations wakeup row whose
          # mailbox has NO activatable inbox head (here: no inbox messages at
          # all). reconcile_target_activation is supposed to delete the wakeup
          # row once the head is completed/absent, so a surviving activation
          # means a reconcile was skipped (e.g. a crash between the inbox write
          # and the reconcile) and never repaired — a worker will be woken to
          # drain a mailbox with nothing claimable. The activation-inbox
          # consistency checker must flag it. The lease is unexpired so the
          # stuck-activation checker does NOT fire; only the orphan check should.
          h.store.inject_target_activation({
            "worker_pool" => "default",
            "target_kind" => "workflow",
            "target_type" => "counter",
            "target_id" => "orphan-#{seed}",
            "status" => "pending",
            "ready_at" => h.scheduler.time,
            "locked_by" => nil,
            "locked_until" => nil,
          })
        end
      end
    end
  end
end
