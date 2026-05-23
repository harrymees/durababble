# typed: true
# frozen_string_literal: true

module Durababble
  class Worker
    #: (store: untyped, workflows: untyped, worker_id: untyped, ?lease_seconds: untyped, ?migrate: untyped) -> void
    def initialize(store:, workflows:, worker_id:, lease_seconds: Engine::DEFAULT_LEASE_SECONDS, migrate: true)
      @store = store
      @workflows = normalize_workflows(workflows)
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @store.migrate! if migrate
    end

    #: () -> untyped
    def tick
      claimed = @store.claim_runnable_workflow(worker_id: @worker_id, lease_seconds: @lease_seconds, workflow_names: @workflows.keys)
      return :idle unless claimed

      workflow = @workflows.fetch(claimed.fetch("name"))
      Engine.new(store: @store, worker_id: @worker_id, lease_seconds: @lease_seconds, migrate: false).resume(workflow, workflow_id: claimed.fetch("id"), claimed:)
      :worked
    end

    #: (?max_ticks: untyped) -> untyped
    def run_until_idle(max_ticks: 100)
      worked = 0
      max_ticks.times do
        case tick
        when :worked
          worked += 1
        when :idle
          break
        end
      end
      worked
    end

    private

    #: (untyped) -> untyped
    def normalize_workflows(workflows)
      case workflows
      when Hash
        workflows.transform_keys(&:to_s)
      else
        Array(workflows).to_h { |workflow_class| [workflow_class.workflow_name, workflow_class] }
      end
    end
  end
end
