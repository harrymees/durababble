# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def parallel_wait_with_retrying_sibling(seed)
        run(seed, "parallel_wait_with_retrying_sibling") do |h|
          h.expect_settled!
          # A parallel workflow scatters two branches: one parks on a timer wait,
          # the sibling runs a step that fails transiently and reschedules a retry.
          # The retry write leaves the workflow `pending` (next_run_at future,
          # NON-terminal) while the parked branch's wait/step stay live -- which is
          # legitimate, since the workflow will resume. This pins two contracts the
          # earlier terminal scenarios do not exercise:
          #   1. command_id determinism across the retry boundary in a PARALLEL
          #      workflow -- on replay the parked branch must re-establish the SAME
          #      wait (memoized at the same command_id) instead of recording a
          #      duplicate and orphaning the original.
          #   2. clean settlement (expect_settled!): the flaky step re-runs and
          #      succeeds, the workflow suspends to `waiting`, and once the timer
          #      fires it completes with no stranded waits/steps.
          # The wait uses a CONSTANT absolute wake_at (not current_time + delta):
          # `wait_until` is a `timer` wait whose wake_at is part of the replayed
          # shape (validate_scheduled_shape!), so a clock-relative wake_at would
          # be a non-deterministic workflow, not an implementation bug.
          charge_attempts = 0
          workflow = workflow_class("retry-with-parked-sibling") do
            test_step("wait_branch") { |ctx| Durababble.wait_until(500, ctx) }
            test_step(
              "flaky_branch",
              retry_policy: { initial_interval: 10, backoff_coefficient: 1, maximum_interval: 10, maximum_attempts: 3 },
            ) do |ctx|
              charge_attempts += 1
              raise "transient charge failure #{charge_attempts}" if charge_attempts < 2

              ctx.merge("charged" => true)
            end
            define_method(:execute) do |input|
              instance = self #: as untyped
              Async do |task|
                errors = []
                [
                  task.async { instance.wait_branch(input) },
                  task.async { instance.flaky_branch(input) },
                ].each do |child|
                  child.wait
                rescue StandardError => e
                  errors << e
                end
                fatal = errors.find { |candidate| !candidate.is_a?(Durababble::WorkflowSuspended) } || errors.first
                raise fatal if fatal
              end.wait
            end
          end
          h.workflows["retry-with-parked-sibling"] = workflow
          id = "retry-parked-#{seed}"
          h.store.enqueue_workflow(name: "retry-with-parked-sibling", input: { "id" => id }, id:)

          # Initial resume: one branch parks on the wait, the sibling fails its first
          # attempt and the atomic failed+retry write leaves the workflow pending.
          h.scheduler.schedule(actor: "worker-a", delay: 1 + h.scheduler.rng.int(3), name: "initial_resume") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a", lease_seconds: 30).resume(workflow, workflow_id: id)
          rescue Durababble::LeaseConflict, Durababble::StepRetryScheduled
            h.scheduler.trace.event(h.scheduler.time, "worker-a", "initial_resume_yield", id:)
          end

          # Retry claim(s): drive the workflow's due retry (next_run_at ~ +10). Two
          # attempts at jittered offsets so seed-driven delay never misses it.
          [15, 45].each_with_index do |delay, index|
            h.scheduler.schedule(actor: "worker-b", delay:, name: "retry_resume_#{index}") do
              next if ["completed", "failed", "canceled"].include?(h.store.workflow(id).fetch("status"))

              Durababble::Engine.new(store: h.store, worker_id: "worker-b", lease_seconds: 30).resume(workflow, workflow_id: id)
            rescue Durababble::LeaseConflict, Durababble::StepRetryScheduled
              h.scheduler.trace.event(h.scheduler.time, "worker-b", "retry_resume_yield", id:)
            end
          end

          # Fire the timer past the parked wait's wake_at (500), then resume so the
          # parked branch completes and the workflow finishes.
          h.scheduler.schedule(actor: "timer", delay: 550, name: "wake") { h.store.wake_due_timers(now: h.store.current_time + 1000) }
          h.scheduler.schedule(actor: "worker-c", delay: 560, name: "final_resume") do
            next if ["completed", "failed", "canceled"].include?(h.store.workflow(id).fetch("status"))

            Durababble::Engine.new(store: h.store, worker_id: "worker-c", lease_seconds: 30).resume(workflow, workflow_id: id)
          rescue Durababble::LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "worker-c", "final_resume_yield", id:)
          end

          h.check("workflow completes") { h.store.workflow(id).fetch("status") == "completed" }
          h.check("flaky step ran exactly twice (one transient failure + one success)") { charge_attempts == 2 }
          h.check("flaky step lands completed") do
            h.store.steps_for(id).any? { |step| step.fetch("name") == "flaky_branch" && step.fetch("status") == "completed" }
          end
          h.check("wait branch step lands completed") do
            h.store.steps_for(id).any? { |step| step.fetch("name") == "wait_branch" && step.fetch("status") == "completed" }
          end
          h.check("exactly one wait recorded for the parked branch (replay did not duplicate it)") do
            h.store.all_waits.values.one? { |wait| wait.fetch("workflow_id") == id }
          end
          h.check("no waiting step stranded after completion") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
        end
      end
    end
  end
end
