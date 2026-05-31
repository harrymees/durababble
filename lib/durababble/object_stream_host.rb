# typed: true
# frozen_string_literal: true

require "async"

module Durababble
  # Per-process residency host for the unified object lease.
  #
  # The host holds the lease on `(object_type, object_id)` continuously while
  # this node owns the object, keeps a single resident user instance per key,
  # and serves commands, queries, and streams from that one instance. Because
  # `Store#deliver_target_message` is owner-routed (it delivers to the lease
  # holder), a sticky lease naturally routes all work for an object back to its
  # resident owner — guaranteeing at most one materialized instance per key in
  # the cluster.
  #
  # Lifecycle:
  #   - First use claims the lease (proving single ownership) and materializes
  #     the instance: `on_create` when no durable state exists yet, otherwise
  #     `on_load`. The expensive user resource (DB handle, cache, connection)
  #     set up in those hooks stays warm across operations.
  #   - Each operation rebinds the resident instance with fresh durable state
  #     read from the store, so crash-safety never depends on resident memory.
  #   - The lease is retained at refcount 0 (residency span). Idle past
  #     `idle_ttl` runs `on_destroy` and releases the lease so ownership can
  #     rebalance. Takeover (`evict!`), graceful shutdown (`evict_all!`), and
  #     renewal failure also tear the instance down via `on_destroy`.
  #
  # The host runs the renewal loop as an Async task owned by the worker runtime.
  # A renewal that fails flips the entry's `lost` flag; producers see that via
  # `ObjectStreamLeaseWriter` (raises `StaleLease` on emit, and reports
  # `cancelled?` true so indefinite poll-and-emit producers exit). On graceful
  # shutdown the worker calls `evict_all!` so consumers see a terminal
  # `StaleLease` frame rather than a dropped gRPC stream.
  class ObjectStreamHost
    DEFAULT_LEASE_SECONDS = 30 #: Integer

    #: Float
    attr_reader :renew_interval
    #: String
    attr_reader :worker_id
    #: String
    attr_reader :node_id
    #: String
    attr_reader :worker_pool

    # An entry tracks the in-process lifecycle of one held key:
    #   refcount     - how many in-flight `with_lease`/`with_resident` calls reference it
    #   lost         - true once renewal failed / the lease was evicted
    #   instance     - the resident user instance (nil until first materialized)
    #   last_used_at  - monotonic stamp set when refcount drops to 0 (idle clock)
    Entry = Struct.new(:refcount, :lost, :instance, :last_used_at, keyword_init: true)

    #: (store: untyped, worker_id: String, node_id: String, ?worker_pool: String, ?lease_seconds: Integer, ?renew_interval: Float?, ?objects: untyped, ?idle_ttl: Numeric?) -> void
    def initialize(store:, worker_id:, node_id:, worker_pool: "default", lease_seconds: DEFAULT_LEASE_SECONDS, renew_interval: nil, objects: nil, idle_ttl: nil)
      @store = store
      @worker_id = worker_id
      @node_id = node_id
      @worker_pool = worker_pool
      @lease_seconds = lease_seconds
      @renew_interval = (renew_interval || [1.0, lease_seconds / 3.0].max).to_f
      @objects = Durababble.normalize_objects(objects)
      @idle_ttl = (idle_ttl || lease_seconds).to_f
      @entries = {} #: Hash[[String, String], Entry]
      @claims_in_progress = {} #: Hash[[String, String], bool]
      @materializing = {} #: Hash[[String, String], bool]
      @mutex = Mutex.new
      @stopping = false
      @renewal_parent = nil #: Object?
      @renewal_task = nil #: Object?
      @renewal_reactor = nil #: Object?
    end

    #: (?parent: Object?) -> ObjectStreamHost
    def start_async(parent: nil)
      parent ||= Async::Task.current?
      raise ConfigurationError, "ObjectStreamHost#start_async requires an active Async task; pass parent: from the worker supervisor" unless parent

      @mutex.synchronize do
        @renewal_parent = parent
        ensure_renewal_task_locked if @entries.any?
      end
      self
    end

    # Claim the lease on the first opener, refcount on subsequent ones, yield to
    # the producer, then refcount-down and (when reaching 0) release. Yields the
    # entry so callers can branch on `entry.lost` after the producer returns:
    # eviction mid-stream with no concurrent emit must still surface as
    # `StaleLease`, not as a clean end.
    #: (worker_pool: String, object_type: String, object_id: String, ?lease_seconds: Integer?) { (Entry) -> untyped } -> untyped
    def with_lease(worker_pool:, object_type:, object_id:, lease_seconds: nil, &block)
      seconds = (lease_seconds || @lease_seconds).to_i
      key = lease_key(object_type:, object_id:)
      entry = acquire(key, worker_pool:, seconds:)
      begin
        block.call(entry)
      ensure
        release(key)
      end
    end

    # Acquire (or share) the lease, materialize the resident instance on first
    # use, yield it, then refcount-down. The lease is retained at refcount 0
    # (sticky residency) — released only by idle-TTL, `evict!`, or shutdown.
    # Used by the no-owner read path to claim the lease and become the resident
    # owner.
    #: (object_type: String, object_id: String, worker_pool: String, ?lease_seconds: Integer?) { (untyped) -> untyped } -> untyped
    def with_resident(object_type:, object_id:, worker_pool:, lease_seconds: nil, &block)
      seconds = (lease_seconds || @lease_seconds).to_i
      key = lease_key(object_type:, object_id:)
      acquire(key, worker_pool:, seconds:)
      begin
        block.call(ensure_instance(key, object_type:, object_id:, worker_pool:))
      ensure
        release(key)
      end
    end

    # Get-or-materialize the resident instance for a key whose lease this host
    # already holds (a `with_lease`/`with_resident` refcount is open around the
    # call). Raises `NoActiveLease` if the lease is not currently held here.
    #: (object_type: String, object_id: String, worker_pool: String) -> untyped
    def resident_instance(object_type:, object_id:, worker_pool:)
      key = lease_key(object_type:, object_id:)
      ensure_instance(key, object_type:, object_id:, worker_pool:)
    end

    # Marks the entry for `(object_type, object_id)` as lost, tears the resident
    # instance down via `on_destroy`, and releases the row in the store.
    # Idempotent: returns false if no entry is currently held.
    #: (worker_pool: String, object_type: String, object_id: String) -> bool
    def evict!(worker_pool:, object_type:, object_id:)
      key = lease_key(object_type:, object_id:)
      entry = @mutex.synchronize do
        held = @entries.delete(key)
        held&.lost = true
        held
      end
      return false unless entry

      destroy_instance(entry)
      release_lease_safely(key)
      true
    end

    # Marks every held entry as lost, runs `on_destroy`, and releases each row.
    # Called from `WorkerRuntime#stop_rpc_server` *before* the reactor interrupt
    # so any in-flight stream handlers see the loss and end with `StaleLease`.
    #: () -> Integer
    def evict_all!
      entries = @mutex.synchronize do
        held = @entries.values
        held.each { |entry| entry.lost = true }
        snapshot = @entries.to_a
        @entries.clear
        snapshot
      end
      entries.each do |key, entry|
        destroy_instance(entry)
        release_lease_safely(key)
      end
      entries.size
    end

    # Stops the renewal task. Called from `WorkerRuntime#stop_rpc_server` on
    # the way out so the host has no live background work after shutdown.
    #: () -> void
    def stop!
      task, reactor = @mutex.synchronize do
        @stopping = true
        renewal = @renewal_task
        renewal_reactor = @renewal_reactor
        @renewal_task = nil
        @renewal_reactor = nil
        @renewal_parent = nil
        [renewal, renewal_reactor]
      end
      stop_task(task, reactor)
    end

    # True when this host currently holds a live lease for the given key.
    # Convenience for tests and the consumer self-route check.
    #: (worker_pool: String, object_type: String, object_id: String) -> bool
    def holds?(worker_pool:, object_type:, object_id:)
      key = lease_key(object_type:, object_id:)
      entry = @mutex.synchronize { @entries[key] }
      !!(entry && !entry.lost)
    end

    private

    # Resolve refcount > 0 (share an already-claimed entry) vs no entry (prove
    # ownership in the store, then publish the entry). Only one in-process claim
    # attempt per key runs at a time, so a concurrent opener never shares a
    # provisional entry and never races a sibling claim/release window.
    #: ([String, String], worker_pool: String, seconds: Integer) -> Entry
    def acquire(key, worker_pool:, seconds:)
      loop do
        claim_needed = false #: bool
        existing = @mutex.synchronize do
          entry = @entries[key]
          if entry
            entry.refcount += 1
            entry
          elsif @claims_in_progress[key]
            nil
          else
            @claims_in_progress[key] = true
            claim_needed = true
            nil
          end
        end
        return existing if existing

        if claim_needed
          begin
            holder = @store.claim_object_lease(
              worker_pool:,
              object_type: key[0],
              object_id: key[1],
              worker_id: @worker_id,
              lease_seconds: seconds,
            )
            unless holder && holder["worker_id"].to_s == @worker_id
              raise WorkflowRpc::StaleLease, "object lease for #{key.join("/")} held by #{holder&.dig("worker_id") || "(none)"}"
            end

            return publish_claimed_entry(key)
          ensure
            @mutex.synchronize { @claims_in_progress.delete(key) }
          end
        end

        sleep(0.001)
      end
    end

    #: ([String, String]) -> Entry
    def publish_claimed_entry(key)
      @mutex.synchronize do
        entry = @entries[key]
        if entry
          entry.refcount += 1
        else
          entry = Entry.new(refcount: 1, lost: false, instance: nil, last_used_at: nil)
          @entries[key] = entry
          ensure_renewal_task_locked
        end
        entry
      end
    end

    # Refcount-down. The lease (and resident instance) are retained at refcount
    # 0 so the instance stays warm — residency ends only via idle-TTL eviction
    # in `renewal_loop`, `evict!`, `evict_all!`, or `stop!`. Stamps the idle
    # clock on the transition to 0.
    #: ([String, String]) -> void
    def release(key)
      @mutex.synchronize do
        entry = @entries[key]
        next unless entry

        entry.refcount -= 1
        entry.last_used_at = monotonic_now if entry.refcount <= 0
      end
    end

    # Materialize the resident instance once per key, letting concurrent callers
    # wait for the in-flight build rather than each constructing their own.
    #: ([String, String], object_type: String, object_id: String, worker_pool: String) -> untyped
    def ensure_instance(key, object_type:, object_id:, worker_pool:)
      loop do
        materialize = false #: bool
        instance = @mutex.synchronize do
          entry = @entries[key] || raise(WorkflowRpc::NoActiveLease, "object lease for #{key.join("/")} is not held")
          if entry.instance
            entry.instance
          elsif @materializing[key]
            nil
          else
            @materializing[key] = true
            materialize = true
            nil
          end
        end
        return instance if instance

        if materialize
          begin
            built = build_resident_instance(object_type:, object_id:, worker_pool:)
            @mutex.synchronize do
              entry = @entries[key] || raise(WorkflowRpc::NoActiveLease, "object lease for #{key.join("/")} is not held")
              entry.instance = built
            end
            return built
          ensure
            @mutex.synchronize { @materializing.delete(key) }
          end
        end

        sleep(0.001)
      end
    end

    #: (object_type: String, object_id: String, worker_pool: String) -> untyped
    def build_resident_instance(object_type:, object_id:, worker_pool:)
      object_class = @objects.fetch(object_type) do
        raise WorkflowRpc::UnknownCommand, "unknown durable object type #{object_type}"
      end
      state = DurableObject.state_from_store(@store, object_type:, object_id:)
      instance = object_class.new(durable_id: object_id, state:, store: @store, worker_pool:) #: as untyped
      if state.equal?(DurableObject::UNINITIALIZED)
        instance.on_create if instance.respond_to?(:on_create)
      elsif instance.respond_to?(:on_load)
        instance.on_load
      end
      instance
    end

    # Best-effort `on_destroy` on the resident instance, clearing it from the
    # entry so a future op re-materializes. Never raises.
    #: (Entry) -> void
    def destroy_instance(entry)
      instance = entry.instance #: as untyped
      return unless instance

      entry.instance = nil
      instance.on_destroy if instance.respond_to?(:on_destroy)
    rescue StandardError => err
      Durababble.logger&.warn("ObjectStreamHost: on_destroy failed: #{err.class}: #{err.message}")
    end

    #: ([String, String]) -> void
    def release_lease_safely(key)
      @store.release_object_lease(object_type: key[0], object_id: key[1], worker_id: @worker_id)
    rescue StandardError => err
      Durababble.logger&.warn("ObjectStreamHost: release_object_lease failed for #{key.join("/")}: #{err.class}: #{err.message}")
    end

    class ReactorCallback
      #: () { () -> Object? } -> void
      def initialize(&block)
        @block = block #: Proc?
      end

      #: () -> bool
      def alive?
        !@block.nil?
      end

      #: () -> Object?
      def transfer
        block = @block
        @block = nil
        block&.call #: as Object?
      end
    end
    private_constant :ReactorCallback

    # Lazily starts the single renewal task. Held under `@mutex`.
    #: () -> void
    def ensure_renewal_task_locked
      task = @renewal_task #: as untyped
      return if task&.running?
      return if @stopping

      parent = @renewal_parent || Async::Task.current?
      return unless parent

      parent = parent #: as untyped
      @renewal_task = parent.async(transient: true) { renewal_loop }
      @renewal_reactor = @renewal_task.reactor #: as untyped
    end

    #: (Object?, Object?) -> void
    def stop_task(task, reactor)
      return unless task

      task = task #: as untyped
      reactor = reactor #: as untyped
      if reactor && Fiber.scheduler.equal?(reactor)
        task.stop
      elsif reactor&.respond_to?(:unblock)
        reactor.unblock(nil, ReactorCallback.new { task.stop })
      else
        task.stop
      end
    end

    # Drives `Store#renew_object_lease` on every held key once per
    # `@renew_interval` and idle-evicts resident keys past `@idle_ttl`. A lease
    # that fails to renew is flagged lost; producers see that on the next emit
    # (via `ObjectStreamLeaseWriter`) and exit. An idle key (refcount 0, last
    # used longer ago than `@idle_ttl`) is torn down with `on_destroy` and its
    # lease released so ownership can rebalance.
    #: () -> void
    def renewal_loop
      until @mutex.synchronize { @stopping }
        sleep(@renew_interval)
        break if @mutex.synchronize { @stopping }

        evict_idle_entries
        snapshot = @mutex.synchronize { @entries.dup }
        next if snapshot.empty?

        with_dedicated_store_connection do |store|
          snapshot.each do |key, entry|
            next if entry.lost

            renewed = store.renew_object_lease(
              object_type: key[0],
              object_id: key[1],
              worker_id: @worker_id,
              lease_seconds: @lease_seconds,
            )
            entry.lost = true unless renewed
          end
        end
      end
    rescue StandardError => err
      logger = Durababble.logger #: as untyped
      logger&.error("ObjectStreamHost: renewal loop crashed: #{err.class}: #{err.message}")
    end

    # Detaches every idle entry (refcount 0, stamped, idle longer than
    # `@idle_ttl`) under the mutex, then runs `on_destroy` + lease release
    # outside it.
    #: () -> void
    def evict_idle_entries
      now = monotonic_now
      evicted = @mutex.synchronize do
        idle = nil #: Array[[[String, String], Entry]]?
        @entries.each do |key, entry|
          next unless entry.refcount <= 0 && entry.last_used_at && (now - entry.last_used_at) > @idle_ttl

          (idle ||= []) << [key, entry]
        end
        idle&.each { |key, _entry| @entries.delete(key) }
        idle
      end
      return unless evicted

      evicted.each do |key, entry|
        entry.lost = true
        destroy_instance(entry)
        release_lease_safely(key)
      end
    end

    # Renewal writes run on a connection dedicated to this task so they do not
    # contend with the worker loop's reactor connection (see `WorkerRuntime`).
    # Falls back to the store directly for doubles that don't implement it.
    #: () { (untyped) -> void } -> void
    def with_dedicated_store_connection(&block)
      if @store.respond_to?(:with_dedicated_connection)
        @store.with_dedicated_connection(&block)
      else
        block.call(@store)
      end
    end

    #: (object_type: String, object_id: String) -> [String, String]
    def lease_key(object_type:, object_id:)
      [object_type, object_id]
    end

    #: () -> Float
    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC).to_f
    end
  end
end
