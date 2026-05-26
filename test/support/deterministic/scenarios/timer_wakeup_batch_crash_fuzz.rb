# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def timer_wakeup_batch_crash_fuzz(seed)
        run(seed, "timer_wakeup_batch_crash_fuzz") do |h|
          # Crash-fuzzes complete_timer_waits, the batched timer-wakeup
          # transaction that concurrent_timer_wake_once exercises only WITHOUT
          # crashes. For each due wait in the batch it runs complete_wait +
          # record_step_completed_without_transaction (itself 3 writes), then a
          # single mark_waits_workflows_pending that flips every woken workflow
          # from `waiting` back to `pending`. All of it is ONE transaction, so the
          # trailing pending-flip is load-bearing: if it were split into a
          # separate commit, a crash after the waits and step records committed
          # but before the flip would leave workflows in `waiting` with a
          # `completed` wait -- stranded forever, since a waiting workflow is never
          # claimed by a worker. A :mid_transaction crash must roll the whole
          # batch back so a later wake_due_timers redoes it cleanly. Several
          # workflows nap concurrently so the batch carries multiple waits at once
          # (the flip is a single multi-row UPDATE); crashing wakers race
          # wake_due_timers, then a crash-free guaranteed waker + finishers drive
          # everything terminal. Teeth: no-op mark_waits_workflows_pending -> every
          # workflow strands in `waiting` with a completed wait -> red.
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

          # Crashing wakers race the batched wake. A :mid_transaction crash must
          # roll the entire batch back; a later waker redoes it cleanly.
          5.times do |w|
            h.scheduler.schedule(actor: "waker-#{w}", delay: 40 + w * 5, name: "wake_with_crashes") do
              h.store.crashable do
                h.store.wake_due_timers(now: h.scheduler.time)
              end
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "waker-#{w}", "wake_crashed")
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 80, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          # Crash-free guaranteed wake: every still-due wait completes durably even
          # if every crashing waker rolled back.
          h.scheduler.schedule(actor: "guaranteed-waker", delay: 85, name: "ensure_wake") do
            h.store.wake_due_timers(now: h.scheduler.time)
          end
          # Crash-free finishers resume each woken workflow to terminal.
          ids.each_with_index do |id, i|
            h.scheduler.schedule(actor: "finisher-#{i}", delay: 90 + i, name: "finish") do
              Durababble::Engine.new(store: h.store, worker_id: "finisher-#{i}").resume(workflow, workflow_id: id)
            rescue Durababble::LeaseConflict
              h.scheduler.trace.event(h.scheduler.time, "finisher-#{i}", "finish_lease_conflict", id:)
            end
          end

          h.check("no workflow stranded waiting with a completed wait (complete_timer_waits atomic)") do
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
