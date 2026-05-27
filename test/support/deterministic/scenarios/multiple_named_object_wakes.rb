# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def multiple_named_object_wakes(seed)
        run(seed, "multiple_named_object_wakes") do |h|
          alarm = alarm_object_class
          object_id = "alarm-#{seed}"
          specs = [
            { "name" => "ttl", "at" => h.store.current_time + 10, "payload" => { "kind" => "ttl" } },
            { "name" => "retry", "at" => h.store.current_time + 20, "payload" => { "kind" => "retry" } },
            { "name" => "daily", "at" => h.store.current_time + 30, "payload" => { "kind" => "daily" } },
          ]
          h.store.enqueue_object_command(object_type: "alarm", object_id:, method_name: "arm", args: [specs], kwargs: {}, message_kind: "tell", max_attempts: 5)

          executor = ->(worker_id) { Durababble::DurableObjectExecutor.new(store: h.store, objects: [alarm], worker_id:, lease_seconds: 30) }
          h.scheduler.schedule(actor: "worker", delay: 1, name: "drain_command") { executor.call("worker-a").drain_object_inbox("alarm", object_id:) }
          h.scheduler.schedule(actor: "timer", delay: 40, name: "wake_due_timers") { h.store.wake_due_timers(now: h.store.current_time + 100) }
          h.scheduler.schedule(actor: "worker", delay: 41, name: "drain_wakes") { executor.call("worker-a").drain_object_inbox("alarm", object_id:) }

          h.check("each named wake delivered exactly once") { h.scheduler.trace.to_s.scan("object_wake_delivered").length == 3 }
          h.check("all three wakes handled in due order") do
            state = h.store.object_state(object_type: "alarm", object_id:)
            state && state.fetch("wakes") == ["ttl", "retry", "daily"]
          end
          h.check("no pending wakes remain") do
            h.store.pending_object_wakeups.none? { |wakeup| wakeup.fetch("object_type") == "alarm" && wakeup.fetch("object_id") == object_id }
          end
        end
      end
    end
  end
end
