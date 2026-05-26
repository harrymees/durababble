# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def store_fault_after_outbox_enqueue(seed)
        run(seed, "store_fault_after_outbox_enqueue") do |h|
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.fault_plan.fail_after(:enqueue_outbox, message: "lost connection after durable outbox write")
          outbox_id = nil #: untyped
          h.scheduler.schedule(
            actor: "producer",
            delay: h.scheduler.rng.int(5),
            name: "fault_after_outbox_enqueue",
          ) do
            outbox_id = h.store.enqueue_outbox(
              workflow_id:,
              topic: "email",
              payload: { "seed" => seed },
              key: "email:#{seed}",
            )
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "producer", "store_fault_observed", workflow_id:)
            outbox_id = h.store.enqueue_outbox(
              workflow_id:,
              topic: "email",
              payload: { "seed" => seed, "retry" => true },
              key: "email:#{seed}",
            )
          end
          h.scheduler.schedule(actor: "sender", delay: 20, name: "send") do
            message = h.store.claim_outbox(worker_id: "sender", lease_seconds: 10)
            h.store.ack_outbox(message.fetch("id"), worker_id: "sender") if message
          end
          h.check("fault was injected after outbox write") { h.scheduler.trace.to_s.include?("fault.injected") }
          h.check("retry reused the original outbox message") do
            h.store.outbox_message(outbox_id).fetch("payload") == { "seed" => seed }
          end
          h.check("outbox processed once after enqueue fault") { h.store.summary.fetch(:processed_outbox) == 1 }
        end
      end
    end
  end
end
