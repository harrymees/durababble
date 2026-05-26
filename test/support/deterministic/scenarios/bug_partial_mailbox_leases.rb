# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_partial_mailbox_leases(seed)
        run(seed, "bug_partial_mailbox_leases") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id)

          h.store.inject_inbox({
            "id" => "partial-inbox-#{seed}",
            "worker_pool" => "default",
            "target_kind" => "workflow",
            "target_type" => "counter",
            "target_id" => "partial-inbox-#{seed}",
            "sequence" => 1,
            "status" => "running",
            "locked_by" => "worker-a",
            "locked_until" => nil,
          })
          h.store.inject_target_activation({
            "worker_pool" => "default",
            "target_kind" => "workflow",
            "target_type" => "counter",
            "target_id" => "partial-activation-#{seed}",
            "status" => "running",
            "ready_at" => h.scheduler.time,
            "locked_by" => nil,
            "locked_until" => h.scheduler.time + 10,
          })
          h.store.inject_fence({
            "workflow_id" => id,
            "key" => "partial-fence",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "locked_by" => "worker-a",
            "locked_until" => nil,
          })
        end
      end
    end
  end
end
