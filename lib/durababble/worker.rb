# typed: true
# frozen_string_literal: true

require "time"

module Durababble
  class Worker
    # Base delay before re-attempting to forward a target activation that could
    # not be delivered. Applied through Backoff.jittered so a fleet of workers
    # racing on the same activation does not retry in lockstep.
    ACTIVATION_FORWARD_RETRY_SECONDS = 1
    WorkItem = Data.define(:kind, :target_key, :payload)

    #: (store: Store, workflows: Object, worker_id: String, ?objects: Object, ?lease_seconds: Numeric, ?migrate: bool, ?worker_pool: String, ?workflow_query_registry: Object?) -> void
    def initialize(store:, workflows:, worker_id:, objects: [], lease_seconds: Engine::DEFAULT_LEASE_SECONDS, migrate: true, worker_pool: "default", workflow_query_registry: nil)
      @store = store
      @workflows = normalize_workflows(workflows)
      @objects = normalize_objects(objects)
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @worker_pool = worker_pool
      @workflow_query_registry = workflow_query_registry
      @engines = {} #: Hash[String, Engine]
      @store.migrate! if migrate
    end

    #: () -> Symbol
    def tick
      attributes = { "durababble.worker.id" => @worker_id }
      Observability.measure("durababble.worker.tick", attributes) do
        work_item = claim_work
        unless work_item
          Observability.count("durababble.worker.ticks", attributes.merge("durababble.worker.tick.result" => "idle"))
          return :idle
        end

        perform_work(work_item)
        Observability.count("durababble.worker.ticks", attributes.merge("durababble.worker.tick.result" => "worked"))
        :worked
      end
    end

    #: (?excluding_target_keys: untyped) -> untyped
    def claim_work(excluding_target_keys: nil)
      activation = claim_next_target_activation
      return target_activation_work_item(activation) if activation

      claimed = @store.claim_runnable_workflow(
        worker_id: @worker_id,
        lease_seconds: @lease_seconds,
        workflow_names: @workflows.keys,
        worker_pool: @worker_pool,
        excluding_workflow_ids: excluded_workflow_ids(excluding_target_keys),
      )
      return unless claimed

      workflow_name = claimed.fetch("name")
      WorkItem.new(
        :workflow,
        target_key(worker_pool: @worker_pool, target_kind: "workflow", target_type: workflow_name, target_id: claimed.fetch("id")),
        [workflow_name, claimed],
      )
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ?worker_pool: String) -> untyped
    def delivery_work(target_kind:, target_type:, target_id:, worker_pool: @worker_pool)
      return unless worker_pool == @worker_pool
      return unless registered_target?(target_kind:, target_type:)

      activation = {
        "target_kind" => target_kind,
        "target_type" => target_type,
        "target_id" => target_id,
        "worker_pool" => worker_pool,
      }
      WorkItem.new(
        :delivery,
        target_key(worker_pool:, target_kind:, target_type:, target_id:),
        activation,
      )
    end

    #: (WorkItem) -> untyped
    def perform_work(work_item)
      case work_item.kind
      when :target_activation
        process_target_activation(work_item.payload)
      when :delivery
        process_target_activation(work_item.payload, advisory: true)
      when :workflow
        workflow_name, claimed = work_item.payload
        workflow = @workflows.fetch(workflow_name)
        engine_for(@worker_pool).resume(workflow, workflow_id: claimed.fetch("id"), claimed:)
      else
        raise ArgumentError, "unknown worker work item #{work_item.kind.inspect}"
      end
    end

    #: (WorkItem, ready_at: Time) -> untyped
    def defer_claimed_work(work_item, ready_at:)
      return unless work_item.kind == :target_activation

      activation = work_item.payload
      worker_pool = activation_worker_pool(activation)
      complete_activation_target(
        activation_target(activation, worker_pool:),
        worker_id: @worker_id,
        now: ready_at,
      )
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ?worker_pool: String) -> untyped
    def deliver_target(target_kind:, target_type:, target_id:, worker_pool: @worker_pool)
      work_item = delivery_work(target_kind:, target_type:, target_id:, worker_pool:)
      return :idle unless work_item

      perform_work(work_item)
      :worked
    end

    #: (?max_ticks: Integer) -> Integer
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

    # Engines hold only per-worker config (store, worker_id, lease_seconds,
    # worker_pool) and build a fresh WorkflowExecution per call, so one instance
    # per worker_pool is reusable across ticks. Memoize them rather than
    # allocating a new Engine on every workflow claim and activation.
    #: (String) -> Engine
    def engine_for(worker_pool)
      @engines[worker_pool] ||= Engine.new(
        store: @store,
        worker_id: @worker_id,
        lease_seconds: @lease_seconds,
        worker_pool:,
        workflow_query_registry: @workflow_query_registry,
      )
    end

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
      engine = engine_for(worker_pool)
      claimed = @store.claim_workflow_for_activation(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds, worker_pool:)
      engine.resume(workflow, workflow_id:, claimed:) if claimed
      target = activation_target(activation, worker_pool:)
      if advisory
        if claimed
          reconcile_activation_target(target)
        else
          forward_target_activation(activation)
          rearm_activation_target(target, ready_at: activation_retry_time(workflow_id))
        end
        return
      end

      forward_target_activation(activation) unless claimed
      complete_activation_target(target, worker_id: @worker_id, now: claimed ? Time.now : activation_retry_time(workflow_id))
    end

    #: (untyped, ?advisory: untyped) -> untyped
    def process_object_activation(activation, advisory: false)
      object_type = activation.fetch("target_type")
      object_id = activation.fetch("target_id")
      worker_pool = activation_worker_pool(activation)
      target = activation_target(activation, worker_pool:)

      # Establish ownership before any inbox work, mirroring
      # `claim_workflow_for_activation` in `process_workflow_activation`.
      # Without this, an activation that fires on an empty inbox never
      # claims the unified per-object lease (the lazy claim inside
      # `claim_inbox_messages` is gated on finding rows), so consumers
      # polling `current_object_lease` have nothing to find. Claiming
      # eagerly here makes the activation itself the ownership signal.
      holder = @store.claim_object_lease(worker_pool:, object_type:, object_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)

      drained = 0
      if holder
        executor = DurableObjectExecutor.new(store: @store, objects: @objects, worker_id: @worker_id, lease_seconds: @lease_seconds, worker_pool:)
        drained = executor.drain_object_inbox(object_type, object_id:)
        # `drain_object_inbox` releases the lease in its ensure block; the
        # in-claim re-claim inside `claim_inbox_messages` is idempotent for
        # the same worker_id and stays as a safety net for direct callers.
      end

      claimed_and_drained = holder && drained.positive?
      if advisory
        if claimed_and_drained
          reconcile_activation_target(target)
        else
          forward_target_activation(activation)
          rearm_activation_target(target, ready_at: object_activation_retry_time(object_type, object_id))
        end
        return
      end

      forward_target_activation(activation) unless claimed_and_drained
      complete_activation_target(target, worker_id: @worker_id, now: claimed_and_drained ? Time.now : object_activation_retry_time(object_type, object_id))
    end

    #: (untyped) -> untyped
    def forward_target_activation(activation)
      deliver_activation_target(activation_target(activation, worker_pool: activation_worker_pool(activation)))
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

    # Object equivalent of `activation_retry_time`: when another worker holds
    # the per-object lease, bound the rearm/complete `ready_at` by their
    # `locked_until` so we don't busy-poll while they're still draining.
    # Shape differs from the workflow helper (takes `(object_type, object_id)`)
    # because lease identity is `durable_objects.(object_type, object_id)`.
    #: (String, String) -> Time
    def object_activation_retry_time(object_type, object_id)
      retry_at = Time.now + Backoff.jittered(ACTIVATION_FORWARD_RETRY_SECONDS)
      row = @store.current_object_lease(object_type, object_id)
      return Time.now if row.nil?

      locked_until = row["locked_until"]
      if locked_until
        lease_deadline = Time.parse(locked_until.to_s)
        return [lease_deadline, retry_at].min
      end

      Time.now
    rescue ArgumentError, TypeError
      Time.now + Backoff.jittered(ACTIVATION_FORWARD_RETRY_SECONDS)
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

    #: (untyped) -> Array[String]
    def excluded_workflow_ids(target_keys)
      Array(target_keys).filter_map do |target_key|
        key = Array(target_key)
        next unless key[0] == @worker_pool
        next unless key[1] == "workflow"
        next unless @workflows.key?(key[2])

        key[3].to_s
      end
    end

    #: (untyped) -> WorkItem
    def target_activation_work_item(activation)
      worker_pool = activation_worker_pool(activation)
      WorkItem.new(
        :target_activation,
        target_key(
          worker_pool:,
          target_kind: activation.fetch("target_kind"),
          target_type: activation.fetch("target_type"),
          target_id: activation.fetch("target_id"),
        ),
        activation,
      )
    end

    #: (untyped) -> String
    def activation_worker_pool(activation)
      String(activation.fetch("worker_pool", @worker_pool))
    end

    # The target-identity keywords every reconcile/rearm/complete/deliver store
    # call shares for a given activation. Splat with `**` and add the call's own
    # keywords (ready_at:, now:, worker_id:) so the four-key bundle lives once.
    #: (untyped, worker_pool: String) -> Hash[Symbol, String]
    def activation_target(activation, worker_pool:)
      {
        target_kind: String(activation.fetch("target_kind")),
        target_type: String(activation.fetch("target_type")),
        target_id: String(activation.fetch("target_id")),
        worker_pool:,
      }
    end

    #: (Hash[Symbol, String]) -> Object?
    def reconcile_activation_target(target)
      @store.reconcile_target_activation(
        target_kind: target.fetch(:target_kind),
        target_type: target.fetch(:target_type),
        target_id: target.fetch(:target_id),
        worker_pool: target.fetch(:worker_pool),
      )
    end

    #: (Hash[Symbol, String], ready_at: Time) -> Object?
    def rearm_activation_target(target, ready_at:)
      @store.rearm_target_activation(
        target_kind: target.fetch(:target_kind),
        target_type: target.fetch(:target_type),
        target_id: target.fetch(:target_id),
        worker_pool: target.fetch(:worker_pool),
        ready_at:,
      )
    end

    #: (Hash[Symbol, String], worker_id: String, now: Time) -> Object?
    def complete_activation_target(target, worker_id:, now:)
      @store.complete_target_activation(
        target_kind: target.fetch(:target_kind),
        target_type: target.fetch(:target_type),
        target_id: target.fetch(:target_id),
        worker_pool: target.fetch(:worker_pool),
        worker_id:,
        now:,
      )
    end

    #: (Hash[Symbol, String]) -> bool
    def deliver_activation_target(target)
      @store.deliver_target_message(
        target_kind: target.fetch(:target_kind),
        target_type: target.fetch(:target_type),
        target_id: target.fetch(:target_id),
        worker_pool: target.fetch(:worker_pool),
      )
    end

    #: (worker_pool: String, target_kind: untyped, target_type: untyped, target_id: untyped) -> Array[String]
    def target_key(worker_pool:, target_kind:, target_type:, target_id:)
      [worker_pool, target_kind, target_type, target_id].map(&:to_s).freeze
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
