# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      # Pins bug 2: a worker crashes while holding a fence (the row is left
      # `running`), and a second worker must reclaim the expired lease and run the
      # side effect exactly once. Reverting `claim_expired_fence` strands the
      # fence: the reclaimer times out, side_effects stays 0, and the stuck-fence
      # checker fires — so this scenario goes red.
      #: (untyped) -> untyped
      def fence_holder_crash_and_reclaim(seed)
        run(seed, "fence_holder_crash_and_reclaim") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.fault_plan.fail_after(:fence_acquired, message: "crash holding fence")

          h.scheduler.schedule(actor: "holder", delay: 5, name: "fence") do
            h.store.with_fence(workflow_id: id, key: "charge") { { "winner" => "holder" } }
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "holder", "crashed_holding_fence")
          end

          # Runs well after the 10-tick fence lease has expired, so the abandoned
          # `running` fence is reclaimable.
          h.scheduler.schedule(actor: "reclaimer", delay: 40, name: "fence") do
            h.store.with_fence(workflow_id: id, key: "charge") { { "winner" => "reclaimer" } }
          rescue FenceTimeout
            h.scheduler.trace.event(h.scheduler.time, "reclaimer", "fence_timed_out")
          end

          h.check("side effect ran exactly once") { h.store.summary.fetch(:side_effects) == 1 }
          h.check("fence reclaimed to completion") do
            h.store.all_fences.any? { |fence| fence.fetch("key") == "charge" && fence.fetch("status") == "completed" }
          end
        end
      end
    end
  end
end
