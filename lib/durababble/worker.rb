# typed: true
# frozen_string_literal: true

require "time"

module Durababble
  class Worker
    # Base delay before re-attempting to forward a target activation that could
    # not be delivered. Applied through Backoff.jittered so a fleet of workers
    # racing on the same activation does not retry in lockstep.
    ACTIVATION_FORWARD_RETRY_SECONDS = 1

    #: (store: untyped, workflows: untyped, worker_id: untyped, ?objects: untyped, ?lease_seconds: untyped, ?migrate: untyped, ?worker_pool: String) -> void
    def initialize(store:, workflows:, worker_id:, objects: [], lease_seconds: Engine::DEFAULT_LEASE_SECONDS, migrate: true, worker_pool: "default")
      @store = store
      @workflows = normalize_workflows(workflows)
      @objects = normalize_objects(objects)
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @worker_pool = worker_pool
      @store.migrate! if migrate
    end

    #: () -> untyped
    def tick
      attributes = { "durababble.worker.id" => @worker_id }
      Observability.measure("durababble.worker.tick", attributes) do
        activation = claim_next_target_activation
        if activation
          process_target_activation(activation)
          Observability.count("durababble.worker.ticks", attributes.merge("durababble.worker.tick.result" => "worked"))
          return :worked
        end

        claimed = @store.claim_runnable_workflow(worker_id: @worker_id, lease_seconds: @lease_seconds, workflow_names: @workflows.keys, worker_pool: @worker_pool)
        unless claimed
          Observability.count("durababble.worker.ticks", attributes.merge("durababble.worker.tick.result" => "idle"))
          return :idle
        end

        workflow = @workflows.fetch(claimed.fetch("name"))
        Engine.new(store: @store, worker_id: @worker_id, lease_seconds: @lease_seconds, worker_pool: @worker_pool).resume(workflow, workflow_id: claimed.fetch("id"), claimed:)
        Observability.count("durababble.worker.ticks", attributes.merge("durababble.worker.tick.result" => "worked"))
        :worked
      end
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ?worker_pool: String) -> untyped
    def deliver_target(target_kind:, target_type:, target_id:, worker_pool: @worker_pool)
      return :idle unless worker_pool == @worker_pool
      return :idle unless registered_target?(target_kind:, target_type:)

      process_target_activation(
        {
          "target_kind" => target_kind,
          "target_type" => target_type,
          "target_id" => target_id,
          "worker_pool" => worker_pool,
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
      case activation.fetch("target_kind")
      when "workflow"
        process_workflow_activation(activation, advisory:)
      when "object"
        process_object_activation(activation, advisory:)
      end
    end

    #: (untyped, ?advisory: untyped) -> untyped
    def process_workflow_activation(activation, advisory: false)
      workflow_id = activation.fetch("target_id")
      workflow = @workflows.fetch(activation.fetch("target_type"))
      worker_pool = activation_worker_pool(activation)
      engine = Engine.new(store: @store, worker_id: @worker_id, lease_seconds: @lease_seconds, worker_pool:)
      claimed = @store.claim_workflow_for_activation(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds, worker_pool:)
      engine.drain_workflow_inbox(workflow, workflow_id:, claimed:) if claimed
      target = activation_target(activation, worker_pool:)
      if advisory
        if claimed
          @store.reconcile_target_activation(**target)
        else
          forward_target_activation(activation)
          @store.rearm_target_activation(**target, ready_at: activation_retry_time(workflow_id))
        end
        return
      end

      forward_target_activation(activation) unless claimed
      @store.complete_target_activation(
        **target,
        worker_id: @worker_id,
        now: claimed ? Time.now : activation_retry_time(workflow_id),
      )
    end

    #: (untyped, ?advisory: untyped) -> untyped
    def process_object_activation(activation, advisory: false)
      object_type = activation.fetch("target_type")
      object_id = activation.fetch("target_id")
      worker_pool = activation_worker_pool(activation)
      executor = DurableObjectExecutor.new(store: @store, objects: @objects, worker_id: @worker_id, lease_seconds: @lease_seconds, worker_pool:)
      drained = executor.drain_object_inbox(object_type, object_id:)
      target = activation_target(activation, worker_pool:)
      if advisory
        if drained.positive?
          @store.reconcile_target_activation(**target)
        else
          forward_target_activation(activation)
          @store.rearm_target_activation(**target, ready_at: Time.now + Backoff.jittered(ACTIVATION_FORWARD_RETRY_SECONDS))
        end
        return
      end

      forward_target_activation(activation) if drained.zero?
      @store.complete_target_activation(
        **target,
        worker_id: @worker_id,
        now: drained.positive? ? Time.now : Time.now + Backoff.jittered(ACTIVATION_FORWARD_RETRY_SECONDS),
      )
    end

    #: (untyped) -> untyped
    def forward_target_activation(activation)
      @store.deliver_target_message(**activation_target(activation, worker_pool: activation_worker_pool(activation)))
    end

    #: (untyped) -> untyped
    def activation_retry_time(workflow_id)
      retry_at = Time.now + Backoff.jittered(ACTIVATION_FORWARD_RETRY_SECONDS)
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

    #: (untyped) -> untyped
    def normalize_objects(objects)
      case objects
      when Hash
        objects.transform_keys(&:to_s)
      else
        Array(objects).to_h { |object_class| [object_class.object_type, object_class] }
      end
    end

    #: () -> untyped
    def claim_next_target_activation
      unless @workflows.empty?
        activation = @store.claim_target_activation(
          worker_id: @worker_id,
          lease_seconds: @lease_seconds,
          target_kinds: ["workflow"],
          target_types: @workflows.keys,
          worker_pool: @worker_pool,
        )
        return activation if activation
      end

      return if @objects.empty?

      @store.claim_target_activation(
        worker_id: @worker_id,
        lease_seconds: @lease_seconds,
        target_kinds: ["object"],
        target_types: @objects.keys,
        worker_pool: @worker_pool,
      )
    end

    #: (untyped) -> String
    def activation_worker_pool(activation)
      String(activation.fetch("worker_pool", @worker_pool))
    end

    # The target-identity keywords every reconcile/rearm/complete/deliver store
    # call shares for a given activation. Splat with `**` and add the call's own
    # keywords (ready_at:, now:, worker_id:) so the four-key bundle lives once.
    #: (untyped, worker_pool: String) -> Hash[Symbol, untyped]
    def activation_target(activation, worker_pool:)
      {
        target_kind: activation.fetch("target_kind"),
        target_type: activation.fetch("target_type"),
        target_id: activation.fetch("target_id"),
        worker_pool:,
      }
    end

    #: (target_kind: untyped, target_type: untyped) -> bool
    def registered_target?(target_kind:, target_type:)
      case target_kind
      when "workflow"
        @workflows.key?(target_type)
      when "object"
        @objects.key?(target_type)
      else
        false
      end
    end
  end
end
