# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_wake_survives_worker_crash(seed)
        run(seed, "object_wake_survives_worker_crash") do |h|
          alarm = alarm_object_class
          object_id = "alarm-#{seed}"
          h.store.enqueue_object_command(object_type: "alarm", object_id:, method_name: "arm", args: [[{ "name" => "ttl", "at" => h.store.current_time + 10, "payload" => { "kind" => "ttl" } }]], kwargs: {}, message_kind: "tell", max_attempts: 5)

          executor = ->(worker_id) { Durababble::DurableObjectExecutor.new(store: h.store, objects: [alarm], worker_id:, lease_seconds: 10) }
          h.scheduler.schedule(actor: "worker-a", delay: 1, name: "drain_command") { executor.call("worker-a").drain_object_inbox("alarm", object_id:) }
          h.scheduler.schedule(actor: "timer", delay: 12, name: "wake_due_timers") { h.store.wake_due_timers(now: h.store.current_time + 100) }
          h.scheduler.schedule(actor: "crasher", delay: 13, name: "claim_then_crash") do
            messages = h.store.claim_inbox_messages(target_kind: "object", target_type: "alarm", target_id: object_id, worker_id: "crasher", lease_seconds: 10, limit: 1)
            messages.each do |message|
              if h.scheduler.rng.int(2).zero?
                # Crash after running the handler and committing its effect, but before the
                # message is marked complete: redelivery must re-run on_wake idempotently.
                state = Durababble::DurableObject.state_from_store(h.store, object_type: "alarm", object_id:)
                object = alarm.new(durable_id: object_id, state:, store: h.store)
                object.public_send(:on_wake, name: message.fetch("method_name"), payload: message.fetch("payload"))
                h.scheduler.trace.event(h.scheduler.time, "crasher", "wake_committed_then_crash")
              else
                # Crash before running the handler at all.
                h.scheduler.trace.event(h.scheduler.time, "crasher", "wake_claimed_then_crash")
              end
            end
          end
          h.scheduler.schedule(actor: "worker-b", delay: 30, name: "recover") { executor.call("worker-b").drain_object_inbox("alarm", object_id:) }

          h.check("wake effect applied exactly once after recovery") do
            state = h.store.object_state(object_type: "alarm", object_id:)
            state && state.fetch("wakes") == ["ttl"]
          end
          h.check("recovered wake message is completed") do
            h.store.inbox_messages_for(target_kind: "object", target_type: "alarm", target_id: object_id)
              .select { |message| message.fetch("message_kind") == "wake" }
              .all? { |message| message.fetch("status") == "completed" }
          end
        end
      end
    end
  end
end
