# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def object_command_failure_exhaustion(seed)
        run(seed, "object_command_failure_exhaustion") do |h|
          # A durable-object command fails repeatedly. Each non-terminal failure
          # (fail_object_command -> fail_inbox_message) marks the message
          # 'failed' (still activatable, so reconcile re-arms the wakeup row and
          # it is re-delivered) until attempts reach max_attempts, at which point
          # the CASE in fail_inbox_message auto-dead-letters it. Exercises the
          # exhaustion-aware failure path + the 'failed' re-delivery edge of the
          # inbox state machine, plus the object-command claim path — all
          # previously unreached by DST (which only drove workflow commands).
          object_type = "counter-object"
          object_id = "obj-#{seed}"
          max_attempts = 2 + h.scheduler.rng.int(2) # 2..3 attempts before exhaustion

          command_id = h.store.enqueue_object_command(
            object_type:,
            object_id:,
            method_name: "bump",
            args: [seed],
            kwargs: {},
            max_attempts:,
          )

          failures = 0
          h.scheduler.schedule(actor: "object-worker", delay: 5, name: "fail_until_exhausted") do
            # Bounded above max_attempts so a missing dead-letter spins the check
            # red rather than the virtual clock forever.
            (max_attempts + 3).times do
              claimed = h.store.claim_object_command(command_id:, worker_id: "object-worker", lease_seconds: 30)
              break if claimed.nil?

              h.store.fail_object_command(command_id:, error: "boom #{failures}", worker_id: "object-worker", terminal: false)
              failures += 1
            end
          end

          h.check("the command failed exactly max_attempts times before exhaustion") do
            failures == max_attempts
          end
          h.check("the exhausted command is dead-lettered with attempts == max_attempts") do
            message = h.store.inbox_message(command_id)
            message && message.fetch("status") == "dead_lettered" && message.fetch("attempts").to_i == max_attempts
          end
          h.check("wakeup row reconciled away after the command is exhausted") do
            h.store.all_target_activations.none? { |activation| activation.fetch("target_id") == object_id }
          end
        end
      end
    end
  end
end
