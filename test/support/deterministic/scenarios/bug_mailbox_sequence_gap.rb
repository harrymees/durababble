# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_mailbox_sequence_gap(seed)
        run(seed, "bug_mailbox_sequence_gap") do |h|
          target_id = "sequence-gap-#{seed}"
          [1, 3, 3].each_with_index do |sequence, index|
            h.store.inject_inbox({
              "id" => "sequence-gap-#{seed}-#{index}",
              "worker_pool" => "default",
              "target_kind" => "object",
              "target_type" => "counter",
              "target_id" => target_id,
              "sequence" => sequence,
              "status" => "pending",
              "locked_by" => nil,
              "locked_until" => nil,
            })
          end
          h.store.inject_target_activation({
            "worker_pool" => "default",
            "target_kind" => "object",
            "target_type" => "counter",
            "target_id" => target_id,
            "status" => "pending",
            "ready_at" => h.scheduler.time,
            "locked_by" => nil,
            "locked_until" => nil,
          })
        end
      end
    end
  end
end
