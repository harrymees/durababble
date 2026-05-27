# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def cancellation_during_suspend_race(seed)
        run(seed, "cancellation_during_suspend_race") do |h|
          # A cancellation request that lands while the workflow is RUNNING -- after
          # it has been claimed but BEFORE its step records a wait -- is a different
          # race from cooperative_cancellation_cleanup, which parks the workflow
          # first so cancellation always sees a genuinely-waiting workflow (and its
          # first-request cleanup, cancel_pending_waits_for_workflow, terminalizes
          # the live wait/step/attempt). Here request_workflow_cancellation runs
          # while status is 'running', so that cleanup finds NOTHING to cancel --
          # the wait/step/attempt do not exist yet. The running step then records
          # its wait, and suspend_workflow's CASE flips the workflow to 'canceling'
          # (the branch that exists precisely for this race). Finalization via
          # cancel_workflow was a bare status UPDATE that never terminalized the
          # now-orphaned waiting step / pending wait / waiting attempt -> a terminal
          # 'canceled' workflow carrying a LIVE waiting step (caught by the harness's
          # always-on "canceled workflow has live step" invariant). The fix
          # terminalizes live waits/steps/attempts inside cancel_workflow's own
          # transaction. Teeth: drop that cleanup -> the invariants below go red.
          cancel_delivered = false
          workflow = workflow_class("suspender") do
            test_step("wait") do |ctx|
              # Model an external cancellation RPC arriving mid-step, before suspend.
              unless cancel_delivered
                cancel_delivered = true
                h.store.request_workflow_cancellation(workflow_id: ctx.fetch("id"), reason: "stop #{seed}")
              end
              Durababble.wait_until(h.store.current_time + 60, ctx)
            end
          end
          h.workflows["suspender"] = workflow
          id = "suspend-race-#{seed}"
          h.store.enqueue_workflow(name: "suspender", input: { "id" => id }, id:)

          # Worker A claims (cancel not yet requested), runs the step, the cancel
          # lands mid-step, and the step suspends -> workflow goes 'canceling' with a
          # freshly recorded waiting step + pending wait.
          h.scheduler.schedule(actor: "worker-a", delay: 1 + h.scheduler.rng.int(3), name: "run_and_suspend") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a", lease_seconds: 30).resume(workflow, workflow_id: id)
          rescue Durababble::LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "worker-a", "suspend_lease_conflict", id:)
          end

          # A finalizer claims the canceling workflow and drives it to terminal.
          [20, 35].each do |delay|
            h.scheduler.schedule(actor: "finalizer", delay:, name: "finalize_cancel") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
              Durababble::Engine.new(store: h.store, worker_id: "finalizer", lease_seconds: 30).resume(workflow, workflow_id: id)
            rescue Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "finalizer", "finalize_lease_conflict", id:)
            end
          end

          h.check("workflow lands terminal canceled") do
            h.store.workflow(id).fetch("status") == "canceled"
          end
          h.check("no waiting step stranded on the canceled workflow") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
          h.check("no pending wait stranded on the canceled workflow") do
            h.store.all_waits.values.none? { |wait| wait.fetch("workflow_id") == id && wait.fetch("status") == "pending" }
          end
          h.check("no waiting step attempt stranded on the canceled workflow") do
            h.store.step_attempts_for(id).none? { |attempt| attempt.fetch("status") == "waiting" }
          end
        end
      end
    end
  end
end
