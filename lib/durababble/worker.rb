# typed: true
# frozen_string_literal: true

require "time"

module Durababble
  class Worker
    ACTIVATION_FORWARD_RETRY_SECONDS = 1

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
      attributes = { "durababble.worker.id" => @worker_id }
      Observability.measure("durababble.worker.tick", attributes) do
        Observability.trace("durababble.worker.tick", attributes) do
          activation = @store.claim_target_activation(
            worker_id: @worker_id,
            lease_seconds: @lease_seconds,
            target_kinds: ["workflow"],
            target_types: @workflows.keys,
          )
          if activation
            process_target_activation(activation)
            Observability.count("durababble.worker.ticks", attributes.merge("durababble.worker.tick.result" => "worked"))
            return :worked
          end

          claimed = @store.claim_runnable_workflow(worker_id: @worker_id, lease_seconds: @lease_seconds, workflow_names: @workflows.keys)
          unless claimed
            Observability.count("durababble.worker.ticks", attributes.merge("durababble.worker.tick.result" => "idle"))
            return :idle
          end

          workflow = @workflows.fetch(claimed.fetch("name"))
          Engine.new(store: @store, worker_id: @worker_id, lease_seconds: @lease_seconds, migrate: false).resume(workflow, workflow_id: claimed.fetch("id"), claimed:)
          Observability.count("durababble.worker.ticks", attributes.merge("durababble.worker.tick.result" => "worked"))
          :worked
        end
      end
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def deliver_target(target_kind:, target_type:, target_id:)
      return :idle unless target_kind == "workflow"
      return :idle unless @workflows.key?(target_type)

      process_target_activation(
        {
          "target_kind" => target_kind,
          "target_type" => target_type,
          "target_id" => target_id,
        },
        advisory: true,
      )
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

    #: (untyped, ?advisory: untyped) -> untyped
    def process_target_activation(activation, advisory: false)
      workflow_id = activation.fetch("target_id")
      workflow = @workflows.fetch(activation.fetch("target_type"))
      engine = Engine.new(store: @store, worker_id: @worker_id, lease_seconds: @lease_seconds, migrate: false)
      claimed = @store.claim_workflow_for_activation(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)
      engine.drain_workflow_inbox(workflow, workflow_id:, claimed:) if claimed
      if advisory
        if claimed
          @store.reconcile_target_activation(
            target_kind: activation.fetch("target_kind"),
            target_type: activation.fetch("target_type"),
            target_id: workflow_id,
          )
        else
          forward_target_activation(activation)
          @store.rearm_target_activation(
            target_kind: activation.fetch("target_kind"),
            target_type: activation.fetch("target_type"),
            target_id: workflow_id,
            ready_at: activation_retry_time(workflow_id),
          )
        end
        return
      end

      forward_target_activation(activation) unless claimed
      @store.complete_target_activation(
        target_kind: activation.fetch("target_kind"),
        target_type: activation.fetch("target_type"),
        target_id: workflow_id,
        worker_id: @worker_id,
        now: claimed ? Time.now : activation_retry_time(workflow_id),
      )
    end

    #: (untyped) -> untyped
    def forward_target_activation(activation)
      @store.deliver_target_message(
        target_kind: activation.fetch("target_kind"),
        target_type: activation.fetch("target_type"),
        target_id: activation.fetch("target_id"),
      )
    end

    #: (untyped) -> untyped
    def activation_retry_time(workflow_id)
      retry_at = Time.now + ACTIVATION_FORWARD_RETRY_SECONDS
      row = @store.workflow(workflow_id)
      locked_until = row["locked_until"]
      if row.fetch("status") == "running" && locked_until
        lease_deadline = Time.parse(locked_until.to_s)
        return [lease_deadline, retry_at].min
      end

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
