# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def cancellation_cleanup_crash_fuzz(seed)
        run(seed, "cancellation_cleanup_crash_fuzz") do |h|
          # Crash-fuzzes request_workflow_cancellation, which the existing
          # cooperative_cancellation_cleanup scenario never exercises under
          # crashes. On the FIRST cancellation request it runs a multi-write
          # transaction: set cancel_requested_at + cancel_pending_waits_for_workflow
          # (which itself cancels the pending wait, the waiting step, AND the
          # waiting step attempt -- three writes) + mark_canceling. Every write is
          # gated on first_request (cancel_requested_at IS NULL), so atomicity is
          # load-bearing in a way the original step-failure bug was: if a crash
          # committed cancel_requested_at but NOT the wait/step/attempt
          # cancellations, the idempotent re-request would see first_request=false
          # and SKIP them -- stranding a pending wait, waiting step, and waiting
          # attempt forever. A :mid_transaction crash must roll the whole thing
          # back so a later request redoes it cleanly.
          #
          # The workflow waits on a timer, gets canceled, then runs a cleanup step
          # and lands terminal `canceled`. Crashing clients race the cancel request
          # and crashing workers race the resume/cleanup; reapers reclaim expired
          # leases; a crash-free tail (guaranteed cancel + closer) drives it to a
          # terminal state so progress is assured regardless of which crashes fired.
          cleanup_runs = 0
          h.workflows["cancelable"] = workflow = Class.new(Durababble::Workflow) do
            workflow_name "cancelable"

            define_method(:execute) do |input|
              instance = self #: as untyped
              instance.wait_for_timer(input)
              { "done" => true }
            rescue Durababble::CancellationError => e
              instance.cleanup(input.merge("reason" => e.reason))
            end

            define_method(:wait_for_timer) do |input|
              Durababble.wait_until(h.store.current_time + 60, input)
            end
            step :wait_for_timer

            define_method(:cleanup) do |input|
              instance = self #: as untyped
              cleanup_runs += 1
              instance.step_context.heartbeat.record({ "phase" => "cleanup", "run" => cleanup_runs })
              { "cleaned" => true, "reason" => input.fetch("reason") }
            end
            step :cleanup
          end

          id = h.store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => seed.to_s })

          h.store.enable_write_crashes!(percent: 20)
          h.store.fault_plan.fail_after(:record_step_completed, once: 1, message: "cleanup crash after completion")

          # Park the workflow on its wait timer first (crash-free) so cancellation
          # always races a genuinely-waiting workflow.
          h.scheduler.schedule(actor: "parker", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "parker").resume(workflow, workflow_id: id)
          end

          # Several clients request cancellation concurrently under crashes; the
          # request must be idempotent and atomic.
          3.times do |c|
            h.scheduler.schedule(actor: "canceler-#{c}", delay: 5 + c * 7, name: "request_cancel") do
              h.store.crashable do
                workflow.handle(id, store: h.store).cancel(reason: "stop #{seed}")
              end
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "canceler-#{c}", "cancel_request_crashed", id:)
            end
          end

          # Crashing workers race to resume, deliver the cancellation, and run
          # cleanup.
          4.times do |w|
            h.scheduler.schedule(actor: "cancel-worker-#{w}", delay: 9 + w * 9, name: "resume_with_crashes") do
              h.store.crashable do
                Durababble::Engine.new(store: h.store, worker_id: "cancel-worker-#{w}", lease_seconds: 12).resume(workflow, workflow_id: id)
              end
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "cancel-worker-#{w}", "cancellation_crashed", id:)
            rescue Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "cancel-worker-#{w}", "cancellation_lease_conflict", id:)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 12 + i * 10, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 100, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          # Crash-free guaranteed cancel: ensures the request lands durably even if
          # every crashing canceler above rolled back. Idempotent -- if an earlier
          # request already committed, first_request is false and this is a no-op;
          # if a non-atomic crash stranded the wait/step/attempt, this re-request
          # would (wrongly, under a buggy split) skip them, and the invariants below
          # catch it.
          h.scheduler.schedule(actor: "guaranteed-canceler", delay: 105, name: "ensure_cancel") do
            workflow.handle(id, store: h.store).cancel(reason: "stop #{seed}")
          end
          # Deterministically crash one cleanup resume after the fuzzed cancel
          # requests. This keeps the worker-crash proof stable even when storage
          # query reductions change the seeded generic write-crash draw sequence.
          h.scheduler.schedule(actor: "forced-cancel-worker", delay: 110, name: "forced_cleanup_crash") do
            Durababble::Engine.new(store: h.store, worker_id: "forced-cancel-worker", lease_seconds: 12).resume(workflow, workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "forced-cancel-worker", "cancellation_crashed", id:)
          rescue Durababble::LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "forced-cancel-worker", "cancellation_lease_conflict", id:)
          end

          # Crash-free closer: free any stranded lease, then resume to a terminal
          # state. Two passes in case the first only delivers cancellation and the
          # second runs cleanup. Tolerate a LeaseConflict (final invariant catches
          # a workflow that never reached terminal).
          [125, 140].each do |delay|
            h.scheduler.schedule(actor: "closer", delay:, name: "final_resume") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
              Durababble::Engine.new(store: h.store, worker_id: "closer", lease_seconds: 30).resume(workflow, workflow_id: id)
            rescue Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "closer", "closer_lease_conflict", id:)
            end
          end

          h.check("workflow lands terminal canceled") do
            h.store.workflow(id).fetch("status") == "canceled"
          end
          h.check("cleanup step recorded completed exactly once") do
            cleanup_steps = h.store.steps_for(id).select { |step| step.fetch("name") == "cleanup" }
            cleanup_steps.length == 1 && cleanup_steps.first.fetch("status") == "completed"
          end
          h.check("no pending wait stranded by cancellation (request_workflow_cancellation atomic)") do
            h.store.all_waits.values.none? do |wait|
              wait.fetch("workflow_id") == id && wait.fetch("status") == "pending"
            end
          end
          h.check("no waiting step stranded by cancellation") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
          h.check("no waiting step attempt stranded by cancellation") do
            h.store.step_attempts_for(id).none? { |attempt| attempt.fetch("status") == "waiting" }
          end
          h.check("no wakeup row survives the cancellation drain") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == id }
          end
        end
      end
    end
  end
end
