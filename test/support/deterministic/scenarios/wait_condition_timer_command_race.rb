# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def wait_condition_timer_command_race(seed)
        run(seed, "wait_condition_timer_command_race") do |h|
          h.expect_settled!
          # Races the TWO ways a wait_condition wakes: its timer firing vs. a
          # command satisfying the predicate. wait_condition(timeout: 10) parks
          # the workflow `waiting` on a `timer` wait whose wake_at == enqueue_time
          # + 10. Two independent forces converge on that window:
          #   * timer workers run wake_due_timers -> complete_timer_waits, whose
          #     SELECT picks the due timer wait when the workflow is waiting OR
          #     RUNNING (it locks only the wait row, FOR UPDATE OF w), then
          #     mark_waits_workflows_pending flips the workflow back to pending --
          #     but that UPDATE is gated on status='waiting', so it CANNOT flip a
          #     workflow a command-delivery worker has already claimed to running.
          #   * command-delivery workers claim the waiting workflow -> running and
          #     resume(claimed:), delivering the :signal command inline at the
          #     wait's safe point; the predicate goes true and execute returns ->
          #     complete_workflow.
          # The dangerous interleave: a timer worker completes the timer wait
          # (locking only the wait) WHILE a command worker holds the workflow
          # running. The pending-flip no-ops (not waiting), so the command worker
          # drives the workflow terminal with a now-`completed` timer wait + its
          # step left behind. If complete_workflow does not reconcile that
          # abandoned wait/step, the terminal workflow keeps a live wait or a
          # waiting step -> verify_step_invariants! / expect_settled! fire. The
          # mirror interleave -- timer wins, flips to pending, then the command
          # worker also delivers -- must still leave exactly one terminal outcome
          # and deliver/dead-letter the command exactly once.
          workflow = workflow_class("race-signal") do
            expose_command(:signal)
            define_method(:signal) do
              instance = self #: as untyped
              instance.instance_variable_set(:@signaled, true)
              { "signaled" => true }
            end
            define_method(:execute) do |input|
              instance = self #: as untyped
              instance.wait_condition(timeout: 10) { instance.instance_variable_get(:@signaled) == true }
              input.merge("released" => true)
            end
          end
          h.workflows["race-signal"] = workflow
          id = "race-#{seed}"
          h.store.enqueue_workflow(name: "race-signal", input: { "id" => id }, id:)

          # Park the workflow on its wait_condition timer wait (wake_at ~= 11).
          h.scheduler.schedule(actor: "worker-a", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a", lease_seconds: 30).resume(workflow, workflow_id: id)
          rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled
            h.scheduler.trace.event(h.scheduler.time, "worker-a", "parked", id:)
          end

          # A client sends the signal command just before the timer is due, so the
          # command delivery and the timer wake contend for the same wait.
          h.scheduler.schedule(actor: "client", delay: 6 + h.scheduler.rng.int(4), name: "signal") do
            h.store.enqueue_workflow_command(
              workflow_id: id,
              workflow_name: "race-signal",
              method_name: "signal",
              payload: { "method" => "signal", "args" => [], "kwargs" => {} },
            )
          end

          # A command-delivery worker claims the WAITING workflow to RUNNING under a
          # short lease, then crashes before delivering (models a worker that dies
          # mid-resume holding the workflow lease). This is the only way to get a
          # `running` workflow to persist across scheduler turns under serialized
          # execution -- a non-crashing resume runs to completion atomically.
          h.scheduler.schedule(actor: "crash-worker", delay: 8 + h.scheduler.rng.int(3), name: "claim_then_crash") do
            next if ["completed", "failed", "canceled"].include?(h.store.workflow(id).fetch("status"))

            claimed = h.store.claim_workflow_for_activation(workflow_id: id, worker_id: "crash-worker", lease_seconds: 5)
            h.scheduler.trace.event(h.scheduler.time, "crash-worker", "claimed_then_crashed", id:) if claimed
          rescue Durababble::LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "crash-worker", "claim_conflict", id:)
          end

          # Timer workers fire wake_due_timers in the wake window. complete_timer_waits
          # selects the due `timer` wait when the workflow is waiting OR running, so a
          # timer worker here can complete the wait while crash-worker holds the
          # workflow running -- and mark_waits_workflows_pending (waiting-gated) then
          # CANNOT flip the running workflow back to pending. If completion does not
          # reconcile, the running workflow is left with a completed timer wait.
          3.times do |i|
            h.scheduler.schedule(actor: "timer-#{i}", delay: 10 + h.scheduler.rng.int(6), name: "wake_due_timers") do
              h.store.wake_due_timers(now: h.store.current_time)
            end
          end

          # Recovery: after crash-worker's short lease expires, a plain resume
          # reclaims the expired running lease (mysql_claim_workflow_lock takes
          # status='running' AND locked_until < now) and drives the workflow
          # terminal -- delivering the still-pending signal command at the wait's
          # safe point, or proceeding past the timed-out wait. Several attempts in
          # case the first races the lease boundary.
          [20, 28, 36].each_with_index do |delay, index|
            h.scheduler.schedule(actor: "recover-#{index}", delay:, name: "recover_#{index}") do
              next if ["completed", "failed", "canceled"].include?(h.store.workflow(id).fetch("status"))

              Durababble::Engine.new(store: h.store, worker_id: "recover-#{index}", lease_seconds: 30).resume(workflow, workflow_id: id)
            rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled, Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "recover-#{index}", "recover_yield", id:)
            end
          end

          h.check("workflow reaches a terminal state") do
            ["completed", "failed", "canceled"].include?(h.store.workflow(id).fetch("status"))
          end
          h.check("the signal command is delivered at most once") do
            h.store.workflow_history_for(id).count { |event| event.fetch("kind") == "workflow_command_completed" } <= 1
          end
          h.check("no waiting step stranded on the workflow") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
          h.check("the wait_condition step has exactly one terminal row") do
            steps = h.store.steps_for(id).select { |step| step.fetch("position") == 0 }
            steps.one? && ["completed", "canceled"].include?(steps.first.fetch("status"))
          end
        end
      end
    end
  end
end
