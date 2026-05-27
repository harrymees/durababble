# typed: true
# frozen_string_literal: true

require "async"
require "securerandom"

module Durababble
  class WorkerRuntime
    DEFAULT_POLL_INTERVAL = 0.1
    DEFAULT_SHUTDOWN_TIMEOUT = 10

    #: untyped
    attr_reader :store, :workflows, :objects, :worker_pool, :worker_id, :last_error, :rpc_address

    class << self
      #: (**untyped) -> untyped
      def start(**kwargs)
        runtime = self #: as untyped
        runtime.new(**kwargs).tap(&:start)
      end
    end

    #: (workflows: untyped, worker_pool: untyped, ?objects: untyped, ?store: untyped, ?database_url: untyped, ?schema: untyped, ?worker_id: untyped, ?lease_seconds: untyped, ?poll_interval: untyped, ?migrate: untyped, ?rpc_host: untyped, ?rpc_port: untyped, ?rpc_credentials: untyped, ?rpc_pool_size: untyped) -> void
    def initialize(
      workflows:,
      worker_pool:,
      objects: [],
      store: nil,
      database_url: nil,
      schema: nil,
      worker_id: nil,
      lease_seconds: Engine::DEFAULT_LEASE_SECONDS,
      poll_interval: DEFAULT_POLL_INTERVAL,
      migrate: true,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
      rpc_credentials: :this_port_is_insecure,
      rpc_pool_size: 4
    )
      schema ||= Durababble.default_schema unless store
      raise ArgumentError, "provide either store: or database_url:" unless store || database_url
      raise ArgumentError, "rpc_host is required" unless rpc_host
      raise ArgumentError, "rpc_port is required" if rpc_port.nil?

      @store = store || Store.connect(database_url:, schema:)
      @owns_store = store.nil?
      @workflows = workflows
      @objects = objects
      @worker_pool = worker_pool
      @worker_identity_id = worker_id || "#{worker_pool}-#{SecureRandom.hex(6)}"
      @worker_id = @worker_identity_id
      @lease_seconds = lease_seconds
      @poll_interval = poll_interval
      @migrate = migrate
      @rpc_host = rpc_host
      @rpc_port = rpc_port
      @rpc_credentials = rpc_credentials
      @rpc_pool_size = rpc_pool_size
      # @mutex guards the lifecycle state shared between the control thread and
      # the host thread (@stopping, @thread, @deliveries). @wakeups is a
      # thread-safe queue used purely to interrupt the polling fiber's idle
      # sleep; it is signaled cross-thread by the RPC server (enqueue_delivery)
      # and by shutdown. Thread::Queue#pop cooperates with the async reactor —
      # it parks the fiber instead of pinning the host thread.
      @mutex = Mutex.new
      @wakeups = Thread::Queue.new
      @deliveries = []
      @stopping = false
      @thread = nil
      @last_error = nil
      @consecutive_errors = 0
      @rpc_server = nil
      @rpc_address = nil
    end

    #: () -> untyped
    def start
      @mutex.synchronize do
        return self if running?

        @stopping = false
        @last_error = nil
        @consecutive_errors = 0
        @deliveries.clear
        @wakeups.clear
        Observability.count(
          "durababble.worker.runtime.starts",
          "durababble.worker.pool" => @worker_pool,
          "durababble.worker.id" => @worker_id,
        )
        start_rpc_server
        worker = begin
          Worker.new(store: @store, workflows: @workflows, objects: @objects, worker_id: @worker_id, lease_seconds: @lease_seconds, migrate: @migrate, worker_pool: @worker_pool)
        rescue StandardError
          stop_rpc_server
          raise
        end
        # The poll loop runs as a fiber inside an async reactor. A non-blocking
        # background service still needs one host thread to drive the reactor,
        # but the worker logic itself is fiber-based: it yields to the reactor
        # while idle and is woken cooperatively rather than hand-rolling a
        # Mutex/ConditionVariable loop on a bare thread.
        @thread = Thread.new { Async { |task| run_loop(task, worker) } }
      end
      self
    end

    #: (?timeout: untyped) -> untyped
    def shutdown(timeout: DEFAULT_SHUTDOWN_TIMEOUT)
      thread = @mutex.synchronize do
        @stopping = true
        @thread
      end
      @wakeups.push(:stop)
      unless thread
        stop_rpc_server
        return :stopped
      end

      attributes = {
        "durababble.worker.pool" => @worker_pool,
        "durababble.worker.id" => @worker_id,
      }
      if thread.join(timeout)
        stop_rpc_server
        Observability.count("durababble.worker.runtime.shutdowns", attributes.merge("durababble.worker.runtime.result" => "stopped"))
        return :stopped
      end

      released = @store.release_worker_leases!(worker_id: @worker_id)
      stop_rpc_server
      Observability.count("durababble.leases.expired_recovery", attributes, by: released.fetch("workflows", 0).to_i)
      Observability.count("durababble.worker.runtime.shutdowns", attributes.merge("durababble.worker.runtime.result" => "timeout"))
      :timeout
    end

    alias_method :stop, :shutdown

    #: (?timeout: untyped) -> untyped
    def wait(timeout: nil)
      thread = @mutex.synchronize { @thread }
      timeout ? thread&.join(timeout) : thread&.join
    end

    #: () -> untyped
    def running?
      @thread&.alive? || false
    end

    #: () -> untyped
    def close
      shutdown
      @store.close if @owns_store
    end

    private

    #: () -> untyped
    def start_rpc_server
      transient_handler = DurableObjectTransientHandler.new(store: @store, objects: @objects, node_id: -> { @worker_id })
      @rpc_server = Rpc::Server.new(
        node_id: nil,
        store: @store,
        worker_pool: @worker_pool,
        host: @rpc_host,
        port: @rpc_port,
        credentials: @rpc_credentials,
        pool_size: @rpc_pool_size,
        verify_deliver_message_owner: false,
        transient_handler:,
        identity_id: @worker_identity_id,
        deliver_message: method(:enqueue_delivery),
      ).start
      @rpc_address = @rpc_server.address
      @worker_id = @rpc_server.node_id
      @store.local_worker_id = -> { @worker_id } if @store.respond_to?(:local_worker_id=)
      @store.local_transient_handler = transient_handler if @store.respond_to?(:local_transient_handler=)
    end

    #: () -> untyped
    def stop_rpc_server
      server = @rpc_server
      return unless server

      @rpc_server = nil
      @rpc_address = nil
      @store.local_worker_id = nil if @store.respond_to?(:local_worker_id=)
      @store.local_transient_handler = nil if @store.respond_to?(:local_transient_handler=)
      server.stop
    end

    #: (**untyped) -> untyped
    def enqueue_delivery(**delivery)
      return unless delivery.fetch(:worker_pool) == @worker_pool

      @mutex.synchronize do
        @deliveries << {
          worker_pool: delivery.fetch(:worker_pool),
          target_kind: delivery.fetch(:target_kind),
          target_type: delivery[:target_type] || delivery.fetch(:target_class),
          target_id: delivery.fetch(:target_id),
        }
      end
      @wakeups.push(:delivery)
    end

    #: (untyped, untyped) -> untyped
    def run_loop(task, worker)
      loop do
        break if stopping?

        begin
          delivery = next_delivery
          result = if delivery
            worker.deliver_target(
              worker_pool: delivery.fetch(:worker_pool),
              target_kind: delivery.fetch(:target_kind),
              target_type: delivery.fetch(:target_type),
              target_id: delivery.fetch(:target_id),
            )
          else
            worker.tick
          end
          @consecutive_errors = 0
          await_work(task) if result == :idle && !stopping?
        rescue LeaseConflict => e
          @last_error = e
          break if stopping?
        rescue StandardError => e
          @last_error = e
          @consecutive_errors += 1
          break if stopping?

          log_loop_error(e)
          await_work(task)
        end
      end
    ensure
      @mutex.synchronize { @thread = nil if Thread.current == @thread }
    end

    #: () -> untyped
    def stopping?
      @mutex.synchronize { @stopping }
    end

    #: () -> untyped
    def next_delivery
      @mutex.synchronize { @deliveries.shift }
    end

    #: () -> bool
    def deliveries_empty?
      @mutex.synchronize { @deliveries.empty? }
    end

    # Park the polling fiber until a wakeup arrives or @poll_interval elapses,
    # whichever comes first. Blocking on @wakeups yields to the reactor so the
    # fiber never pins the host thread, and a cross-thread push (from
    # enqueue_delivery or shutdown) wakes it promptly even when poll_interval is
    # long. Wakeup tokens are only hints: the real work lives in @deliveries and
    # the store, so draining stale tokens afterward is safe and prevents the
    # loop from spinning through a backlog of signals.
    #: (untyped) -> void
    def await_work(task)
      return if stopping? || !deliveries_empty?

      task.with_timeout(@poll_interval) { @wakeups.pop }
    rescue Async::TimeoutError
      nil
    ensure
      @wakeups.clear
    end

    # Surface unexpected polling failures so a worker that is silently spinning
    # on a recurring error (bad migration, broken handler, lost DB) is visible
    # instead of looking idle. LeaseConflict is normal contention and skips this.
    #: (Exception) -> void
    def log_loop_error(error)
      Durababble.logger&.warn(
        "Durababble worker #{@worker_id} hit an unexpected polling error " \
          "(#{@consecutive_errors} in a row): #{error.class}: #{error.message}",
      )
    end
  end
end
