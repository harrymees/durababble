# frozen_string_literal: true

module Durababble
  class Worker
    def initialize(store:, workflows:, worker_id:, lease_seconds: Engine::DEFAULT_LEASE_SECONDS)
      @store = store
      @workflows = workflows
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @store.migrate!
    end

    def tick
      claimed = @store.claim_runnable_workflow(worker_id: @worker_id, lease_seconds: @lease_seconds)
      return :idle unless claimed

      workflow = @workflows.fetch(claimed.fetch("name"))
      Engine.new(store: @store, worker_id: @worker_id, lease_seconds: @lease_seconds).resume(workflow, workflow_id: claimed.fetch("id"))
      :worked
    end

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
  end
end
