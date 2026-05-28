# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def wait_condition_sequential_command_wakeups(seed)
        run(seed, "wait_condition_sequential_command_wakeups") do |h|
          h.expect_settled!
          # Two SEQUENTIAL control-flow wait_conditions in one execute, each
          # satisfied by a DISTINCT command. The novel path single-wait coverage
          # never reaches: when command A satisfies the first wait_condition, the
          # workflow does NOT terminate -- it advances to the second wait_condition
          # and suspends. So wait A's timer wait is left `waiting` (never completed:
          # the command superseded it, and complete_workflow's cleanup has not run
          # because the workflow is not terminal yet). On the NEXT resume (to
          # deliver command B), execute replays from the top and re-encounters
          # wait A. Re-recording wait A would be a duplicate-key INSERT
          # (waiting step upsert keys by workflow/position) -- the only thing
          # preventing it is that step_waiting is in TERMINAL_KINDS, so
          # terminal_recorded?(A) is true on replay and call_wait skips the record,
          # resolving A's future from the recorded step_waiting instead. This pins
          # that the leftover-waiting-wait replay is correct across resumes AND
          # that complete_workflow finally cleans up BOTH stranded waits (batch
          # cancel_pending_waits) when the workflow terminates.
          workflow = workflow_class("await-two-signals") do
            expose_command(:signal_a)
            expose_command(:signal_b)
            define_method(:signal_a) do
              instance = self #: as untyped
              instance.instance_variable_set(:@signaled_a, true)
              { "signaled" => "a" }
            end
            define_method(:signal_b) do
              instance = self #: as untyped
              instance.instance_variable_set(:@signaled_b, true)
              { "signaled" => "b" }
            end
            define_method(:execute) do |input|
              instance = self #: as untyped
              instance.wait_condition(timeout: 1000) { instance.instance_variable_get(:@signaled_a) == true }
              instance.wait_condition(timeout: 1000) { instance.instance_variable_get(:@signaled_b) == true }
              input.merge("released" => true)
            end
          end
          h.workflows["await-two-signals"] = workflow
          id = "await2-#{seed}"
          h.store.enqueue_workflow(name: "await-two-signals", input: { "id" => id }, id:)

          # Park the workflow on the first wait_condition.
          h.scheduler.schedule(actor: "worker-a", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a", lease_seconds: 30).resume(workflow, workflow_id: id)
          rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled
            h.scheduler.trace.event(h.scheduler.time, "worker-a", "parked", id:)
          end

          # signal_a is enqueued first; signal_b is enqueued LATER (after the first
          # delivery window) so command B is NOT available at wait A's safe point --
          # forcing the workflow to advance to wait B and suspend, then replay wait A
          # on the resume that finally delivers command B.
          h.scheduler.schedule(actor: "client-a", delay: 4, name: "signal_a") do
            h.store.enqueue_workflow_command(
              workflow_id: id,
              workflow_name: "await-two-signals",
              method_name: "signal_a",
              payload: { "method" => "signal_a", "args" => [], "kwargs" => {} },
            )
          end
          h.scheduler.schedule(actor: "client-b", delay: 30, name: "signal_b") do
            h.store.enqueue_workflow_command(
              workflow_id: id,
              workflow_name: "await-two-signals",
              method_name: "signal_b",
              payload: { "method" => "signal_b", "args" => [], "kwargs" => {} },
            )
          end

          # Activation-driven delivery workers spanning both wake windows.
          [6 + h.scheduler.rng.int(3), 12, 20, 32 + h.scheduler.rng.int(4), 44, 60].each_with_index do |delay, index|
            worker_id = "worker-b-#{index}"
            h.scheduler.schedule(actor: worker_id, delay:, name: "deliver_#{index}") do
              next if ["completed", "failed", "canceled"].include?(h.store.workflow(id).fetch("status"))

              activation = h.store.claim_target_activation(
                worker_id:, lease_seconds: 30, target_kinds: ["workflow"], target_types: ["await-two-signals"],
              )
              next if activation.nil?

              claimed = h.store.claim_workflow_for_activation(workflow_id: id, worker_id:, lease_seconds: 30)
              if claimed
                Durababble::Engine.new(store: h.store, worker_id:, lease_seconds: 30).resume(workflow, workflow_id: id, claimed:)
              end
              h.store.complete_target_activation(target_kind: "workflow", target_type: "await-two-signals", target_id: id, worker_id:)
            rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled, Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, worker_id, "resume_yield", id:)
            end
          end

          h.check("workflow completes") { h.store.workflow(id).fetch("status") == "completed" }
          h.check("signal_a delivered exactly once") do
            h.store.workflow_history_for(id).one? do |event|
              event.fetch("kind") == "workflow_command_completed" && event.fetch("payload").to_s.include?("\"a\"")
            end
          end
          h.check("signal_b delivered exactly once") do
            h.store.workflow_history_for(id).one? do |event|
              event.fetch("kind") == "workflow_command_completed" && event.fetch("payload").to_s.include?("\"b\"")
            end
          end
          h.check("no waiting step stranded on the terminal workflow") do
            h.store.steps_for(id).none? { |step| step.fetch("status") == "waiting" }
          end
          h.check("no pending or duplicate waits left behind") do
            waits = h.store.all_waits.values.select { |wait| wait.fetch("workflow_id") == id }
            waits.none? { |wait| wait.fetch("status") == "pending" } &&
              waits.map { |wait| wait.fetch("position") }.then { |positions| positions == positions.uniq }
          end
        end
      end
    end
  end
end
