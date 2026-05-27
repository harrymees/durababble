# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      # Pins bug 1 (step-failure outcome must be atomic + correctly terminal).
      # Two crash flows, both crashing at :step_failed_recorded — the window the
      # fix opened *after* the durable failure write:
      #
      #   * retry path: the failed attempt and the retry scheduling land in one
      #     transaction, so after the crash the workflow is already `pending` and
      #     unleased. A plain resume reclaims and finishes it *without* any
      #     lease-stealing reaper. Revert the atomic write (drop the retry
      #     scheduling) and the workflow is stranded `running`/leased: the
      #     no-steal recovery can't claim it and "workflow completed" goes red.
      #   * exhausted path: the final failure is recorded as terminal history, so
      #     replay re-raises the recorded error instead of re-running the step.
      #     The side effect therefore runs exactly once across crash + recovery.
      #     Drop the terminal marking and recovery re-runs the step — the side
      #     effect fires twice and the count check goes red.
      #: (untyped) -> untyped
      def step_failure_crash_matrix(seed)
        run(seed, "step_failure_crash_matrix") do |h|
          retry_side_effects = 0
          retry_attempts = 0
          h.workflows["retry_crash"] = workflow_class("retry_crash") do
            test_step("charge", retry_policy: { initial_interval: 10, backoff_coefficient: 1, maximum_interval: 10, maximum_attempts: 3 }) do |ctx|
              retry_attempts += 1
              retry_side_effects += 1
              raise "transient charge failure #{retry_attempts}" if retry_attempts < 2

              ctx.merge("charged" => true)
            end
          end

          exhausted_side_effects = 0
          h.workflows["exhausted_crash"] = workflow_class("exhausted_crash") do
            test_step("charge_once", retry_policy: { maximum_attempts: 1 }) do |_ctx|
              exhausted_side_effects += 1
              raise "permanent charge failure"
            end
          end

          retry_id = h.store.enqueue_workflow(name: "retry_crash", input: { "seed" => seed })
          exhausted_id = h.store.enqueue_workflow(name: "exhausted_crash", input: { "seed" => seed })

          # --- retry path: crash right after the atomic failed+retry write ---
          h.scheduler.schedule(actor: "worker-a", delay: 1 + h.scheduler.rng.int(3), name: "retry_attempt_then_crash") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a", crash_after: :step_failed_recorded)
              .resume(h.workflows.fetch("retry_crash"), workflow_id: retry_id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "worker-a", "crashed_after_retry_scheduled", workflow_id: retry_id)
          end
          # No reaper: recovery relies solely on the workflow having been left
          # claimable (pending, unleased) by the atomic retry write.
          h.scheduler.schedule(actor: "worker-b", delay: 20, name: "retry_recover") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-b").resume(h.workflows.fetch("retry_crash"), workflow_id: retry_id)
          rescue LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "worker-b", "retry_recover_blocked", workflow_id: retry_id)
          end

          # --- exhausted path: crash right after the terminal failure write ---
          h.scheduler.schedule(actor: "worker-c", delay: 1 + h.scheduler.rng.int(3), name: "exhausted_attempt_then_crash") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-c", crash_after: :step_failed_recorded)
              .resume(h.workflows.fetch("exhausted_crash"), workflow_id: exhausted_id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "worker-c", "crashed_after_terminal_failure", workflow_id: exhausted_id)
          end
          # The engine never finalized the workflow (it crashed first), so the row
          # is still `running`/leased; steal the expired lease, then replay must
          # re-raise the recorded terminal failure rather than re-run the step.
          h.scheduler.schedule(actor: "reaper", delay: 70, name: "steal") { h.store.steal_expired_leases!(now: h.scheduler.time + 61) }
          h.scheduler.schedule(actor: "worker-d", delay: 80, name: "exhausted_recover") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-d").resume(h.workflows.fetch("exhausted_crash"), workflow_id: exhausted_id)
          rescue Durababble::Error
            h.scheduler.trace.event(h.scheduler.time, "worker-d", "exhausted_recover_failed", workflow_id: exhausted_id)
          end

          h.check("retry workflow completed after crash without a steal") { h.store.workflow(retry_id).fetch("status") == "completed" }
          h.check("retry side effect ran once per attempt") { retry_side_effects == 2 && retry_attempts == 2 }
          h.check("exhausted workflow failed after crash") { h.store.workflow(exhausted_id).fetch("status") == "failed" }
          h.check("exhausted step ran exactly once across recovery") { exhausted_side_effects == 1 }
          h.check("exhausted step not re-run on replay") { h.store.step_attempts_for(exhausted_id).one? }
        end
      end
    end
  end
end
