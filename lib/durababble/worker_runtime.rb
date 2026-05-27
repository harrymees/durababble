# typed: true
# frozen_string_literal: true

require "async"
require "securerandom"

module Durababble
  class WorkerRuntime
    DEFAULT_POLL_INTERVAL = 0.1
    DEFAULT_SHUTDOWN_TIMEOUT = 10
    DUPLICATE_TARGET_RETRY_SECONDS = 0.1

    #: Store
    attr_reader :store
    #: Object
    attr_reader :workflows, :objects
    #: String
    attr_reader :worker_pool, :worker_id
    #: Integer
    attr_reader :concurrency
    #: StandardError?
    attr_reader :last_error
    #: String?
    attr_reader :rpc_address

    class << self
      #: (**Object?) -> WorkerRuntime
      def start(**kwargs)
        runtime = self #: as untyped
        runtime.new(**kwargs).tap(&:start)
      end
    end

    #: (workflows: Object, worker_pool: String, ?objects: Object, ?store: Store?, ?database_url: String?, ?schema: String?, ?worker_id: String?, ?lease_seconds: Numeric, ?poll_interval: Numeric, ?concurrency: Object, ?migrate: bool, ?rpc_host: String?, ?rpc_port: Integer?, ?rpc_credentials: Object?, ?rpc_pool_size: Integer) -> void
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
      concurrency: 1,
      migrate: true,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
      rpc_credentials: :this_port_is_insecure,
      rpc_pool_size: 4
    )
      schema ||= Durababble.default_schema unless store
      raise ArgumentError, "provide either store: or database_url:" unless store || database_url

      @rpc_host = rpc_host || raise(ArgumentError, "rpc_host is required")
      @rpc_port = rpc_port.nil? ? raise(ArgumentError, "rpc_port is required") : rpc_port

      concurrency = begin
        Integer(concurrency)
      rescue ArgumentError, TypeError
        raise ArgumentError, "concurrency must be a positive integer"
      end
      raise ArgumentError, "concurrency must be a positive integer" unless concurrency.positive?

      database_url = database_url #: as untyped
      schema = schema #: as untyped
      @store = store || Store.connect(database_url:, schema:) #: as untyped
      @owns_store = store.nil?
      @workflows = workflows
      @objects = objects
      @worker_pool = worker_pool
      @worker_identity_id = worker_id || "#{worker_pool}-#{SecureRandom.hex(6)}"
      @worker_id = @worker_identity_id
      @lease_seconds = lease_seconds
      @poll_interval = poll_interval
      @concurrency = concurrency
      @migrate = migrate
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
      @workflow_query_registry = WorkflowQueryRegistry.new
      @workflow_rpc_handlers = workflow_rpc_handlers
    end

    #: () -> WorkerRuntime
    def start
      @mutex.synchronize do
        return self if running?

        Durababble.assert_fiber_isolation!
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
          Worker.new(
            store: @store,
            workflows: @workflows,
            objects: @objects,
            worker_id: @worker_id,
            lease_seconds: @lease_seconds,
            migrate: @migrate,
            worker_pool: @worker_pool,
            workflow_query_registry: @workflow_query_registry,
          )
        rescue StandardError
          stop_rpc_server
          raise
        end
        # The scheduler runs as fibers inside one async reactor. A non-blocking
        # background service still needs one host thread to drive the reactor,
        # but the worker logic fans out cooperatively up to @concurrency and is
        # woken by hints rather than hand-rolling worker threads.
        @thread = Thread.new { Async { |task| run_loop(task, worker) } }
      end
      self
    end

    #: (?timeout: Numeric) -> Symbol
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

    #: (?timeout: Numeric?) -> Thread?
    def wait(timeout: nil)
      thread = @mutex.synchronize { @thread }
      timeout ? thread&.join(timeout) : thread&.join
    end

    #: () -> bool
    def running?
      @thread&.alive? || false
    end

    #: () -> void
    def close
      shutdown
      @store.close if @owns_store
    end

    private

    #: () -> untyped
    def start_rpc_server
      @rpc_server = Rpc::Server.new(
        node_id: nil,
        store: @store,
        worker_pool: @worker_pool,
        workflow_handlers: @workflow_rpc_handlers,
        host: @rpc_host,
        port: @rpc_port,
        credentials: @rpc_credentials,
        pool_size: @rpc_pool_size,
        verify_deliver_message_owner: false,
        identity_id: @worker_identity_id,
        deliver_message: method(:enqueue_delivery),
      ).start
      @rpc_address = @rpc_server.address
      @worker_id = @rpc_server.node_id
      configure_local_workflow_rpc(@store)
    end

    #: () -> untyped
    def stop_rpc_server
      server = @rpc_server
      return unless server

      @rpc_server = nil
      @rpc_address = nil
      clear_local_workflow_rpc(@store)
      server.stop
    end

    #: () -> Hash[String, Object]
    def workflow_rpc_handlers
      handlers = {}
      normalize_workflows(@workflows).each_value do |workflow_class|
        workflow_class = workflow_class #: as untyped
        workflow_class.exposed_queries.each_key do |method_name|
          handlers[method_name.to_s] = lambda do |payload|
            @workflow_query_registry.call(
              workflow_id: payload.fetch("workflow_id"),
              method_name: payload.fetch("method", method_name.to_s),
              args: payload.fetch("args", []),
              kwargs: payload.fetch("kwargs", {}),
            )
          end
        end
      end
      handlers
    end

    #: (untyped) -> Hash[String, Object]
    def normalize_workflows(workflows)
      case workflows
      when Hash
        workflows.transform_keys(&:to_s)
      else
        Array(workflows).to_h { |workflow_class| [workflow_class.workflow_name, workflow_class] }
      end
    end

    #: (untyped) -> void
    def configure_local_workflow_rpc(store)
      return unless store.respond_to?(:local_workflow_rpc_node_id=)

      store.local_workflow_rpc_node_id = @worker_id
      store.local_workflow_rpc_handlers = @workflow_rpc_handlers
    end

    #: (untyped) -> void
    def clear_local_workflow_rpc(store)
      return unless store.respond_to?(:local_workflow_rpc_node_id)
      return unless store.local_workflow_rpc_node_id == @worker_id

      store.local_workflow_rpc_node_id = nil
      store.local_workflow_rpc_handlers = nil
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
      active_targets = {}
      loop do
        if stopping?
          break if active_targets.empty?

          await_active_work(task)
          next
        end

        begin
          scheduled = schedule_available_work(task, worker, active_targets)
          next if scheduled

          if active_targets.length >= @concurrency
            await_active_work(task)
          else
            await_work(task)
          end
        rescue LeaseConflict => e
          record_worker_error(e)
        rescue StandardError => e
          record_worker_error(e)
          await_work(task)
        end
      end
    ensure
      @mutex.synchronize { @thread = nil if Thread.current == @thread }
    end

    #: (untyped, untyped, Hash[Array[String], untyped]) -> bool
    def schedule_available_work(task, worker, active_targets)
      scheduled_count = 0
      while active_targets.length < @concurrency && !stopping?
        work_item = next_delivery_work(worker) || worker.claim_work(excluding_target_keys: active_targets.keys)
        break unless work_item

        if active_targets.key?(work_item.target_key)
          defer_duplicate_work(worker, work_item)
          next
        end

        scheduled_count += 1
        active_targets[work_item.target_key] = true
        schedule_work_item(task, worker, active_targets, work_item)
      end
      scheduled_count.positive?
    end

    #: (untyped, untyped, Hash[Array[String], untyped], untyped) -> void
    def schedule_work_item(task, worker, active_targets, scheduled_item)
      task.async do
        worker.perform_work(scheduled_item)
        @consecutive_errors = 0
      rescue LeaseConflict => e
        record_worker_error(e)
      rescue StandardError => e
        record_worker_error(e)
      ensure
        active_targets.delete(scheduled_item.target_key)
        @wakeups.push(:finished)
      end
    end

    #: (untyped) -> untyped
    def next_delivery_work(worker)
      delivery = next_delivery
      return unless delivery

      worker.delivery_work(
        worker_pool: delivery.fetch(:worker_pool),
        target_kind: delivery.fetch(:target_kind),
        target_type: delivery.fetch(:target_type),
        target_id: delivery.fetch(:target_id),
      )
    end

    #: (untyped, untyped) -> void
    def defer_duplicate_work(worker, work_item)
      return unless work_item.kind == :target_activation

      worker.defer_claimed_work(
        work_item,
        ready_at: Time.now + Backoff.jittered(DUPLICATE_TARGET_RETRY_SECONDS),
      )
    end

    #: (StandardError) -> void
    def record_worker_error(error)
      @last_error = error
      return if error.is_a?(LeaseConflict)

      @consecutive_errors += 1
      log_loop_error(error)
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

    #: (untyped) -> void
    def await_active_work(task)
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
