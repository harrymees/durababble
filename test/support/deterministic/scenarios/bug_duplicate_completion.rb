# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def bug_duplicate_completion(seed)
        run(seed, "bug_duplicate_completion") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id, worker_id: "bug", lease_seconds: 10)
          h.store.complete_workflow(id, result: { "count" => seed })
          h.store.inject_step({
            "workflow_id" => id,
            "position" => 0,
            "name" => "broken",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
          h.store.inject_attempt({
            "id" => "post-terminal-running-attempt",
            "workflow_id" => id,
            "position" => 0,
            "name" => "broken",
            "status" => "running",
            "result" => nil,
            "error" => nil,
            "heartbeat_cursor" => nil,
          })
        end
      end
    end
  end
end
