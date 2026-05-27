# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def workflow_termination_dependents_crash_fuzz(seed)
        run(seed, "workflow_termination_dependents_crash_fuzz") do |h|
          # Crash-fuzzes request_workflow_termination, which no existing scenario
          # exercises under crashes. It is a TERMINAL-GATED multi-write
          # transaction: lock the workflow row, return early if already terminal,
          # otherwise terminate_workflow (status->terminated) +
          # terminate_workflow_dependents (a FIVE-write cascade -- waits, steps,
          # step attempts, inbox, target activations) + append_workflow_history.
          # The early-return gate is `terminal?(status)`, so atomicity is
          # load-bearing in the same way the cancellation request's
          # first_request gate is: if a crash committed the status->terminated
          # write but NOT the dependent cascade, the idempotent re-request would
          # see the workflow already terminal and SKIP the cascade -- stranding a
          # pending wait, waiting step, waiting attempt, and wakeup row forever.
          # A :mid_transaction crash must roll the whole thing back (status AND
          # any cascade writes) so a later request redoes it cleanly. The teeth:
          # move terminate_workflow out of the transaction so it commits before
          # the cascade -> a crash strands the dependents and the gate skips the
          # redo -> the stranding invariants below go red.
          #
          # The workflow parks on a wait timer first (crash-free) so termination
          # always races a genuinely-waiting workflow with live dependents.
          # Crashing clients race the terminate request; reapers reclaim expired
          # leases; a crash-free guaranteed terminator at the tail drives it to a
          # terminal state so progress is assured regardless of which crashes
          # fired.
          h.workflows["terminable"] = workflow = Class.new(Durababble::Workflow) do
            workflow_name "terminable"

            define_method(:execute) do |input|
              instance = self #: as untyped
              instance.wait_for_timer(input)
              { "done" => true }
            end

            define_method(:wait_for_timer) do |input|
              Durababble.wait_until(h.store.current_time + 60, input)
            end
            step :wait_for_timer
          end

          id = h.store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => seed.to_s })

          # Park the workflow on its wait timer first (crash-free) so termination
          # always races a genuinely-waiting workflow with a pending wait,
          # waiting step, and waiting step attempt to strand.
          h.scheduler.schedule(actor: "parker", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "parker").resume(workflow, workflow_id: id)
          end

          h.scheduler.schedule(actor: "enable-crashes", delay: 3, name: "enable_crashes") do
            h.store.enable_write_crashes!(percent: 25)
          end

          # Several clients request termination concurrently under crashes; the
          # request must be idempotent and atomic.
          4.times do |c|
            h.scheduler.schedule(actor: "terminator-#{c}", delay: 5 + c * 6, name: "request_terminate") do
              h.store.crashable do
                workflow.handle(id, store: h.store).terminate(reason: "halt #{seed}")
              end
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "terminator-#{c}", "terminate_request_crashed", id:)
            end
          end

          6.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 8 + i * 9, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + 1)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 80, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          # Crash-free guaranteed terminate: ensures the request lands durably even
          # if every crashing terminator above rolled back. Idempotent -- if an
          # earlier request already committed, the workflow is terminal and this is
          # a no-op; if a non-atomic crash stranded the dependents, this re-request
          # would (wrongly, under a buggy split) skip them, and the invariants
          # below catch it.
          h.scheduler.schedule(actor: "guaranteed-terminator", delay: 85, name: "ensure_terminate") do
            workflow.handle(id, store: h.store).terminate(reason: "halt #{seed}")
          end

          h.check("workflow lands terminal terminated") do
            h.store.workflow(id).fetch("status") == "terminated"
          end
          h.check("no pending wait stranded by termination (request_workflow_termination atomic)") do
            h.store.all_waits.values.none? do |wait|
              wait.fetch("workflow_id") == id && wait.fetch("status") == "pending"
            end
          end
          h.check("no waiting step stranded by termination") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
          h.check("no waiting step attempt stranded by termination") do
            h.store.step_attempts_for(id).none? { |attempt| attempt.fetch("status") == "waiting" }
          end
          h.check("no wakeup row survives the termination drain") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == id }
          end
          h.check("workflow_terminated history entry recorded exactly once") do
            h.store.workflow_history_for(id).one? { |entry| entry.fetch("kind") == "workflow_terminated" }
          end
        end
      end
    end
  end
end
