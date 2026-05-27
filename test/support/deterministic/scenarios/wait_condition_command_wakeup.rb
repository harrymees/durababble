# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def wait_condition_command_wakeup(seed)
        run(seed, "wait_condition_command_wakeup") do |h|
          h.expect_settled!
          # Exercises wait_condition -- the predicate-polling wait that NOTHING
          # else in the suite touches. It records a `timer` wait (via call_wait,
          # which goes through schedule_command! -> record_step_scheduled, so the
          # wait gets a real step row at its position) and parks the workflow.
          # When a workflow command later flips the predicate, the command is
          # delivered inline at the wait's safe point: await_command_future raises
          # WorkflowCommandDelivered, the loop re-evaluates the now-true predicate,
          # and execute returns -> complete_workflow. The wait was satisfied by the
          # command, NOT by its timer firing, so the timer wait / its step are left
          # behind unless completion reconciles them. If complete_workflow does not
          # finalize that abandoned wait+step, the terminal workflow keeps a live
          # (waiting) step -> verify_step_invariants! fires; expect_settled! also
          # rejects any leftover pending wait. Same atomicity/cleanup class as the
          # parallel-parked-branch terminal bug, on the wait_condition path.
          workflow = workflow_class("await-signal") do
            expose_command(:signal)
            define_method(:signal) do
              instance = self #: as untyped
              instance.instance_variable_set(:@signaled, true)
              { "signaled" => true }
            end
            define_method(:execute) do |input|
              instance = self #: as untyped
              instance.wait_condition(timeout: 1000) { instance.instance_variable_get(:@signaled) == true }
              input.merge("released" => true)
            end
          end
          h.workflows["await-signal"] = workflow
          id = "await-#{seed}"
          h.store.enqueue_workflow(name: "await-signal", input: { "id" => id }, id:)

          # Park the workflow on its wait_condition timer wait.
          h.scheduler.schedule(actor: "worker-a", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a", lease_seconds: 30).resume(workflow, workflow_id: id)
          rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled
            h.scheduler.trace.event(h.scheduler.time, "worker-a", "parked", id:)
          end

          # A client sends the signal command; the upserted activation models the
          # wakeup. A worker then resumes and delivers the command inline.
          h.scheduler.schedule(actor: "client", delay: 5, name: "signal") do
            h.store.enqueue_workflow_command(
              workflow_id: id,
              workflow_name: "await-signal",
              method_name: "signal",
              payload: { "method" => "signal", "args" => [], "kwargs" => {} },
            )
          end
          [6 + h.scheduler.rng.int(3), 20, 40].each_with_index do |delay, index|
            worker_id = "worker-b-#{index}"
            h.scheduler.schedule(actor: worker_id, delay:, name: "deliver_#{index}") do
              next if ["completed", "failed", "canceled"].include?(h.store.workflow(id).fetch("status"))

              activation = h.store.claim_target_activation(
                worker_id:, lease_seconds: 30, target_kinds: ["workflow"], target_types: ["await-signal"],
              )
              next if activation.nil?

              claimed = h.store.claim_workflow_for_activation(workflow_id: id, worker_id:, lease_seconds: 30)
              if claimed
                Durababble::Engine.new(store: h.store, worker_id:, lease_seconds: 30).resume(workflow, workflow_id: id, claimed:)
              end
              h.store.complete_target_activation(target_kind: "workflow", target_type: "await-signal", target_id: id, worker_id:)
            rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled, Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, worker_id, "resume_yield", id:)
            end
          end

          h.check("workflow completes") { h.store.workflow(id).fetch("status") == "completed" }
          h.check("the signal command was delivered exactly once") do
            h.store.workflow_history_for(id).one? { |event| event.fetch("kind") == "workflow_command_completed" }
          end
          h.check("no waiting step stranded on the terminal workflow") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
        end
      end
    end
  end
end
