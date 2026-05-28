# typed: true
# frozen_string_literal: true

require "async"

module Durababble
  # Per-process host for the unified object lease around exposed streams.
  #
  # When a worker dispatches an exposed object stream it asks the host to take
  # the lease on `(object_type, object_id)`, heartbeat it, and eventually
  # release it. Many in-flight stream RPCs for the same object on this worker
  # share a single lease (the host refcounts per object key) so the worker
  # remains the single, exclusive object owner for its lifetime here.
  #
  # The host also runs the renewal loop as an Async task owned by the worker
  # runtime. A renewal that fails flips the entry's `lost` flag; producers see
  # that via `ObjectStreamLeaseWriter` (raises `StaleLease` on emit, and reports
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
    #   refcount - how many in-flight `with_lease` calls reference it
    #   lost     - true once renewal failed / the lease was evicted
    Entry = Struct.new(:refcount, :lost, keyword_init: true)

    #: (store: untyped, worker_id: String, node_id: String, ?worker_pool: String, ?lease_seconds: Integer, ?renew_interval: Float?) -> void
    def initialize(store:, worker_id:, node_id:, worker_pool: "default", lease_seconds: DEFAULT_LEASE_SECONDS, renew_interval: nil)
      @store = store
      @worker_id = worker_id
      @node_id = node_id
      @worker_pool = worker_pool
      @lease_seconds = lease_seconds
      @renew_interval = (renew_interval || [1.0, lease_seconds / 3.0].max).to_f
      @entries = {} #: Hash[[String, String], Entry]
      @claims_in_progress = {} #: Hash[[String, String], bool]
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

    # Marks the entry for `(object_type, object_id)` as lost and releases the row
    # in the store. Idempotent: returns false if no entry is currently held.
    #: (worker_pool: String, object_type: String, object_id: String) -> bool
    def evict!(worker_pool:, object_type:, object_id:)
      key = lease_key(object_type:, object_id:)
      entry = @mutex.synchronize { @entries[key] }
      return false unless entry

      entry.lost = true
      release_lease_safely(key)
      true
    end

    # Marks every held entry as lost and releases each row. Called from
    # `WorkerRuntime#stop_rpc_server` *before* the reactor interrupt so any
    # in-flight stream handlers see the loss and end with `StaleLease`.
    #: () -> Integer
    def evict_all!
      keys = @mutex.synchronize { @entries.keys }
      keys.each do |key|
        entry = @mutex.synchronize { @entries[key] }
        next unless entry

        entry.lost = true
        release_lease_safely(key)
      end
      keys.size
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
          entry = Entry.new(refcount: 1, lost: false)
          @entries[key] = entry
          ensure_renewal_task_locked
        end
        entry
      end
    end

    #: ([String, String]) -> void
    def release(key)
      release_lease = false #: bool
      @mutex.synchronize do
        entry = @entries[key]
        next unless entry

        entry.refcount -= 1
        if entry.refcount <= 0
          @entries.delete(key)
          release_lease = !entry.lost
        end
      end
      release_lease_safely(key) if release_lease
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
    # `@renew_interval`. A lease that fails to renew is flagged lost; producers
    # see that on the next emit (via `ObjectStreamLeaseWriter`) and exit. The
    # entry stays in `@entries` (with refcount) until producers release it; only
    # then is the (already-lost) row removed.
    #: () -> void
    def renewal_loop
      until @mutex.synchronize { @stopping }
        sleep(@renew_interval)
        break if @mutex.synchronize { @stopping }

        snapshot = @mutex.synchronize { @entries.dup }
        snapshot.each do |key, entry|
          next if entry.lost

          renewed = @store.renew_object_lease(
            object_type: key[0],
            object_id: key[1],
            worker_id: @worker_id,
            lease_seconds: @lease_seconds,
          )
          entry.lost = true unless renewed
        end
      end
    rescue StandardError => err
      logger = Durababble.logger #: as untyped
      logger&.error("ObjectStreamHost: renewal loop crashed: #{err.class}: #{err.message}")
    end

    #: (object_type: String, object_id: String) -> [String, String]
    def lease_key(object_type:, object_id:)
      [object_type, object_id]
    end
  end
end
