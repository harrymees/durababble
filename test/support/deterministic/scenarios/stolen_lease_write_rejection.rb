# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def stolen_lease_write_rejection(seed)
        run(seed, "stolen_lease_write_rejection") do |h|
          # Pins the store-level split-brain guard: once a workflow lease is
          # stolen (the prior owner's lease expired and a reaper handed it to a
          # new worker), EVERY durable write the stale prior owner attempts must
          # be rejected with LeaseConflict. This is a different bug class from the
          # crash-atomicity scenarios -- it is a concurrency/ownership invariant.
          # The existing workflow_rpc_owner_state_matrix only exercises the
          # RPC-routing staleness checks (StaleLease / NoActiveLease /
          # WorkflowNotRunning); it never drives the store write guards
          # (assert_workflow_lease_for_update! for step writes,
          # require_fenced_workflow_update! for workflow completion/fail/cancel)
          # through a real steal. If either guard regressed to a no-op, the stale
          # worker could complete/fail/cancel a step or the whole workflow that a
          # new owner is actively running -> double side effects / split brain.
          # The teeth: make assert_workflow_lease_for_update! a no-op -> the stale
          # step writes are accepted -> "no stale write was accepted" goes red.
          h.workflows["counter"] = counter_workflow
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id:, worker_id: "stale-owner", lease_seconds: 10)
          h.store.record_step_scheduled(workflow_id:, command_id: 0, name: "work", args: [])
          h.store.record_step_started(workflow_id:, command_id: 0, name: "work")

          # The lease expires; a reaper reclaims it and a new worker takes over.
          h.scheduler.schedule(actor: "reaper", delay: 20, name: "steal_expired") do
            h.store.steal_expired_leases!(now: h.scheduler.time + 1)
          end
          h.scheduler.schedule(actor: "new-owner", delay: 22, name: "claim") do
            h.store.claim_workflow(workflow_id:, worker_id: "new-owner", lease_seconds: 200)
          end

          # The stale prior owner wakes up after the steal and tries to flush its
          # work. Each durable write carries worker_id: "stale-owner" and must be
          # rejected. Any write that is ACCEPTED is a split-brain bug.
          stale_writes = {
            "record_step_completed" => -> { h.store.record_step_completed(workflow_id:, command_id: 0, result: { "stale" => true }, worker_id: "stale-owner") },
            "record_step_failed" => -> { h.store.record_step_failed(workflow_id:, command_id: 0, error: "stale failure", worker_id: "stale-owner") },
            "record_step_canceled" => -> { h.store.record_step_canceled(workflow_id:, command_id: 0, error: "stale cancel", worker_id: "stale-owner") },
            "complete_workflow" => -> { h.store.complete_workflow(workflow_id, result: { "stale" => true }, worker_id: "stale-owner") },
            "fail_workflow" => -> { h.store.fail_workflow(workflow_id, error: "stale failure", worker_id: "stale-owner") },
            "cancel_workflow" => -> { h.store.cancel_workflow(workflow_id, reason: "stale cancel", worker_id: "stale-owner") },
          }
          stale_writes.each_with_index do |(op, write), i|
            h.scheduler.schedule(actor: "stale-owner", delay: 24 + i, name: "stale_#{op}") do
              write.call
              h.scheduler.trace.event(h.scheduler.time, "stale-owner", "stale_accepted", op:)
            rescue Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "stale-owner", "stale_rejected", op:)
            end
          end

          # The legitimate new owner finishes the step and workflow.
          h.scheduler.schedule(actor: "new-owner", delay: 40, name: "finish") do
            h.store.record_step_completed(workflow_id:, command_id: 0, result: { "ok" => true }, worker_id: "new-owner")
            h.store.complete_workflow(workflow_id, result: { "count" => seed }, worker_id: "new-owner")
          end

          h.check("no stale write was accepted (split-brain guard holds)") do
            !h.scheduler.trace.to_s.include?("stale_accepted")
          end
          h.check("every stale write was rejected with LeaseConflict") do
            stale_writes.keys.all? { |op| h.scheduler.trace.to_s.include?("stale_rejected op=#{op.inspect}") }
          end
          # complete_workflow is fence-guarded (require_fenced_workflow_update!),
          # so a `completed` status proves the new owner -- not the stale owner --
          # drove it to terminal.
          h.check("workflow completed by the new owner (fenced completion)") do
            h.store.workflow(workflow_id).fetch("status") == "completed"
          end
          h.check("step has exactly one completed attempt") do
            h.store.step_attempts_for(workflow_id).one? { |attempt| attempt.fetch("status") == "completed" }
          end
          h.check("no step attempt was failed or canceled by the stale owner") do
            h.store.step_attempts_for(workflow_id).none? { |attempt| ["failed", "canceled"].include?(attempt.fetch("status")) }
          end
        end
      end
    end
  end
end
