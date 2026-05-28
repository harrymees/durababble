# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def timer_wakeup_batch_crash_fuzz(seed)
        run(seed, "timer_wakeup_batch_crash_fuzz") do |h|
          # Crash-fuzzes the new workflow-timer path. A due timer no longer has a
          # separate wake transaction: the workflow row becomes claimable through
          # next_run_at, then the claiming worker completes the wait while holding
          # the workflow lease. Crashes during that resumed execution must either
          # roll back the step completion or leave enough durable state for a
          # later claimant to replay and finish exactly once.
          h.workflows["napper"] = workflow = workflow_class("napper") do
            test_step("nap") { |ctx| Durababble.wait_until(h.store.current_time + 30, ctx.merge("woken" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end

          ids = Array.new(4) do |i|
            h.store.enqueue_workflow(name: "napper", input: { "id" => "#{seed}-#{i}" })
          end

          # Park each workflow on its nap timer (crash-free) so the wake batch
          # always has several genuinely-waiting workflows to flip at once.
          ids.each_with_index do |id, i|
            h.scheduler.schedule(actor: "parker-#{i}", delay: 1 + i, name: "park") do
              Durababble::Engine.new(store: h.store, worker_id: "parker-#{i}").resume(workflow, workflow_id: id)
            end
          end

          h.scheduler.schedule(actor: "enable-crashes", delay: 10, name: "enable_crashes") do
            h.store.enable_write_crashes!(percent: 30)
          end

          # Crashing due-time workers race to claim and replay the waiting
          # workflows. A :mid_transaction crash must roll back the wait
          # completion; a later worker redoes it cleanly.
          5.times do |w|
            h.scheduler.schedule(actor: "waker-#{w}", delay: 40 + w * 5, name: "claim_due_with_crashes") do
              ids.each do |id|
                next if h.store.workflow(id).fetch("status") == "completed"

                resume_workflow_once(
                  h,
                  actor: "waker-#{w}",
                  workflow:,
                  workflow_id: id,
                  crashable: true,
                  crash_event: "wake_crashed",
                )
              end
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 80, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          # Crash-free finishers resume every remaining due workflow to terminal.
          ids.each_with_index do |id, i|
            h.scheduler.schedule(actor: "finisher-#{i}", delay: 90 + i, name: "finish") do
              h.store.steal_expired_leases!(now: h.scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
              resume_workflow_once(h, actor: "finisher-#{i}", workflow:, workflow_id: id, yield_event: "finish_yield")
            end
          end

          h.check("no workflow stranded waiting with a completed wait") do
            ids.none? do |id|
              h.store.workflow(id).fetch("status") == "waiting" &&
                h.store.all_waits.values.any? { |wait| wait.fetch("workflow_id") == id && wait.fetch("status") == "completed" }
            end
          end
          h.check("every nap wait completed exactly once") do
            ids.all? do |id|
              h.store.all_waits.values.one? { |wait| wait.fetch("workflow_id") == id && wait.fetch("status") == "completed" }
            end
          end
          h.check("no workflow left waiting after the wake drain") do
            ids.none? { |id| h.store.workflow(id).fetch("status") == "waiting" }
          end
          h.check("nap step recorded completed exactly once per workflow") do
            ids.all? do |id|
              h.store.steps_for(id).one? { |step| step.fetch("name") == "nap" && step.fetch("status") == "completed" }
            end
          end
        end
      end
    end
  end
end
