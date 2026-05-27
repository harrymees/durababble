# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def parallel_branch_failure_orphans_step(seed)
        run(seed, "parallel_branch_failure_orphans_step") do |h|
          # A workflow scatters two parallel branches (raw Async, the same shape
          # async_workflow_test exercises): one branch parks on a wait (recording a
          # pending wait + a `waiting` step + a waiting attempt), the sibling raises
          # a terminal failure. The engine surfaces the sibling failure and calls
          # fail_workflow -> the workflow lands `failed`. Unlike request_workflow_
          # termination (which cascades terminate_workflow_dependents) and the now-
          # fixed cancel_workflow (which repeats the wait/step/attempt cleanup),
          # fail_workflow was a bare status UPDATE: it left the parked branch's
          # `waiting` step / pending wait / waiting attempt stranded on a terminal
          # workflow forever. The harness's always-on verify_step_invariants! flags
          # "failed workflow <id> has live step" (a waiting step counts as live);
          # the explicit checks below pin the same clean-terminal contract the
          # other terminal paths already satisfy. Teeth: drop fail_workflow's
          # dependent cleanup -> these go red every seed.
          workflow = workflow_class("parallel-failer") do
            test_step("wait_branch") { |ctx| Durababble.wait_until(h.store.current_time + 3600, ctx) }
            test_step("fail_branch", retry_policy: { maximum_attempts: 1 }) { |ctx| raise "boom #{ctx.fetch("id")}" }
            define_method(:execute) do |input|
              instance = self #: as untyped
              Async do |task|
                errors = []
                [
                  task.async { instance.wait_branch(input) },
                  task.async { instance.fail_branch(input) },
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
          h.workflows["parallel-failer"] = workflow
          id = "parallel-fail-#{seed}"
          h.store.enqueue_workflow(name: "parallel-failer", input: { "id" => id }, id:)

          h.scheduler.schedule(actor: "worker-a", delay: 1 + h.scheduler.rng.int(3), name: "run_branches") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a", lease_seconds: 30).resume(workflow, workflow_id: id)
          rescue Durababble::LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "worker-a", "resume_lease_conflict", id:)
          end

          # A timer fires well after the failure; it must not resurrect the terminal
          # workflow, and the wait it touches must already be terminalized.
          h.scheduler.schedule(actor: "timer", delay: 50, name: "wake_due_timers") do
            h.store.wake_due_timers(now: h.store.current_time + 4000)
          end

          h.check("workflow lands terminal failed") do
            h.store.workflow(id).fetch("status") == "failed"
          end
          h.check("no waiting step stranded on the failed workflow") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
          h.check("no pending wait stranded on the failed workflow") do
            h.store.all_waits.values.none? { |wait| wait.fetch("workflow_id") == id && wait.fetch("status") == "pending" }
          end
          h.check("no waiting step attempt stranded on the failed workflow") do
            h.store.step_attempts_for(id).none? { |attempt| attempt.fetch("status") == "waiting" }
          end
        end
      end
    end
  end
end
