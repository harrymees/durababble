# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def cooperative_cancellation_cleanup(seed)
        run(seed, "cooperative_cancellation_cleanup") do |h|
          cleanup_runs = 0
          cleanup_lease_observations = []
          workflow_id_for_cleanup = nil
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
              before = h.store.workflow(workflow_id_for_cleanup)
              h.scheduler.advance(5)
              instance.step_context.heartbeat.record({ "phase" => "cleanup", "run" => cleanup_runs })
              after = h.store.workflow(workflow_id_for_cleanup)
              cleanup_lease_observations << {
                before_locked_by: before.fetch("locked_by"),
                before_locked_until: before.fetch("locked_until"),
                after_locked_by: after.fetch("locked_by"),
                after_locked_until: after.fetch("locked_until"),
              }
              h.scheduler.trace.event(h.scheduler.time, "worker", "cleanup_ran", count: cleanup_runs, reason: input.fetch("reason"))
              { "cleaned" => true, "reason" => input.fetch("reason") }
            end
            step :cleanup
          end

          id = h.store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => seed.to_s })
          workflow_id_for_cleanup = id
          h.scheduler.schedule(actor: "worker-a", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a").resume(workflow, workflow_id: id)
          end
          h.scheduler.schedule(actor: "client", delay: 5, name: "cancel") do
            workflow.handle(id, store: h.store).cancel(reason: "stop #{seed}")
          end
          h.scheduler.schedule(actor: "worker-b", delay: 10, name: "cleanup") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-b", lease_seconds: 20).resume(workflow, workflow_id: id)
          end
          h.scheduler.schedule(actor: "client-timer", delay: 70, name: "late_timer") do
            woken = h.store.wake_due_timers
            h.scheduler.trace.event(h.scheduler.time, "client-timer", "late_timer", woken:)
          end
          h.check("workflow canceled after cleanup") { h.store.workflow(id).fetch("status") == "canceled" }
          h.check("cleanup ran once") { cleanup_runs == 1 }
          h.check("cleanup heartbeat kept ownership") do
            cleanup_lease_observations.any? do |observation|
              observation.fetch(:before_locked_by) == "worker-b" &&
                observation.fetch(:after_locked_by) == "worker-b" &&
                observation.fetch(:after_locked_until) > observation.fetch(:before_locked_until)
            end
          end
          h.check("cleanup heartbeat persisted") { h.store.steps_for(id).any? { |step| step.fetch("name") == "cleanup" && step.fetch("heartbeat_cursor") == { "phase" => "cleanup", "run" => 1 } } }
          h.check("late timer ignored") { h.scheduler.trace.to_s.include?("late_timer woken=0") }
          h.check("waiting attempt canceled") { h.store.step_attempts_for(id).any? { |attempt| attempt.fetch("status") == "canceled" } }
        end
      end
    end
  end
end
