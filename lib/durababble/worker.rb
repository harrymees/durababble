# typed: true
# frozen_string_literal: true

require "time"

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
      activation = @store.claim_target_activation(
        worker_id: @worker_id,
        lease_seconds: @lease_seconds,
        target_kinds: ["workflow"],
        target_types: @workflows.keys,
      )
      if activation
        process_target_activation(activation)
        return :worked
      end

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
    def process_target_activation(activation)
      workflow_id = activation.fetch("target_id")
      workflow = @workflows.fetch(activation.fetch("target_type"))
      engine = Engine.new(store: @store, worker_id: @worker_id, lease_seconds: @lease_seconds, migrate: false)
      claimed = @store.claim_workflow_for_activation(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)
      engine.drain_workflow_inbox(workflow, workflow_id:, claimed:) if claimed
      @store.complete_target_activation(
        target_kind: activation.fetch("target_kind"),
        target_type: activation.fetch("target_type"),
        target_id: workflow_id,
        worker_id: @worker_id,
        now: claimed ? Time.now : activation_retry_time(workflow_id),
      )
    end

    #: (untyped) -> untyped
    def activation_retry_time(workflow_id)
      row = @store.workflow(workflow_id)
      locked_until = row["locked_until"]
      return Time.parse(locked_until.to_s) if row.fetch("status") == "running" && locked_until

      Time.now
    rescue KeyError
      Time.now
    end

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
