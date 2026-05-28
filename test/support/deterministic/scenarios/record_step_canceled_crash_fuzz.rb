# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def record_step_canceled_crash_fuzz(seed)
        run(seed, "record_step_canceled_crash_fuzz") do |h|
          # Crash-fuzzes record_step_canceled, the path that cancels an ACTIVELY
          # RUNNING step (a waiting step is canceled by cancel_waiting_steps inside
          # the cancellation request instead, and never reaches here -- which is
          # why cancellation_cleanup_crash_fuzz does not exercise it). It is a
          # THREE-write transaction -- cancel_step (step->canceled) +
          # update_latest_attempt(status=canceled) + append_workflow_history
          # (step_canceled) -- the same atomicity class as the original
          # step-failure bug. A :mid_transaction crash must roll back all three so
          # a retry redoes them cleanly; if append_workflow_history were split out,
          # a crash between the attempt update and the history append would leave
          # the attempt canceled with no history row, and the replay-style "skip if
          # already canceled" guard below would never re-add it -> a step_canceled
          # entry permanently lost. Conversely a history-without-attempt split
          # would let the guard re-run and append a duplicate. The invariant pins
          # exactly one history entry against exactly one canceled attempt.
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id:, worker_id: "owner", lease_seconds: 1000)
          h.store.record_step_scheduled(workflow_id:, command_id: 0, name: "work", args: [], event_index: h.next_event_index(workflow_id))
          h.store.record_step_started(workflow_id:, command_id: 0, name: "work", event_index: h.next_event_index(workflow_id))

          h.store.enable_write_crashes!(percent: 25)

          already_canceled = lambda do
            h.store.step_attempts_for(workflow_id).any? { |attempt| attempt.fetch("status") == "canceled" }
          end

          # Several crashing workers race to cancel the running step. The guard
          # mirrors the engine's replay behaviour: a worker only records the
          # cancellation if no canceled attempt is durably present yet. With the
          # atomic transaction a crash rolls the whole thing back, so the guard
          # still reads "not canceled" and a later worker retries cleanly.
          5.times do |w|
            h.scheduler.schedule(actor: "cancel-worker-#{w}", delay: 3 + w * 7, name: "record_cancel_with_crashes") do
              h.store.crashable do
                next if already_canceled.call

                h.store.record_step_canceled(workflow_id:, command_id: 0, error: "workflow cancellation requested", event_index: h.next_event_index(workflow_id))
              end
            rescue InjectedCrash
              h.scheduler.trace.event(h.scheduler.time, "cancel-worker-#{w}", "record_cancel_crashed", id: workflow_id)
            end
          end

          h.scheduler.schedule(actor: "settler", delay: 60, name: "disable_crashes") do
            h.store.enable_write_crashes!(percent: 0)
          end
          h.scheduler.schedule(actor: "closer", delay: 70, name: "ensure_canceled") do
            h.store.record_step_canceled(workflow_id:, command_id: 0, error: "workflow cancellation requested", event_index: h.next_event_index(workflow_id)) unless already_canceled.call
          end

          h.check("the running step is recorded canceled") do
            step = h.store.steps_for(workflow_id).find { |s| s.fetch("name") == "work" }
            step && step.fetch("status") == "canceled"
          end
          h.check("exactly one step attempt is canceled") do
            h.store.step_attempts_for(workflow_id).one? { |attempt| attempt.fetch("status") == "canceled" }
          end
          h.check("step_canceled history entry recorded exactly once (record_step_canceled atomic)") do
            h.store.workflow_history_for(workflow_id).one? { |entry| entry.fetch("kind") == "step_canceled" }
          end
          h.check("no running step attempt stranded") do
            h.store.step_attempts_for(workflow_id).none? { |attempt| attempt.fetch("status") == "running" }
          end
        end
      end
    end
  end
end
