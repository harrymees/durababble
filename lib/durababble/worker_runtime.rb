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

    #: (workflows: Object, worker_pool: String, ?objects: Object, ?store: Store?, ?database_url: String?, ?schema: String?, ?worker_id: String?, ?lease_seconds: Numeric, ?poll_interval: Numeric, ?concurrency: Object, ?migrate: bool, ?rpc_host: String?, ?rpc_port: Integer?) -> void
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
      rpc_port: 0
    )
      schema ||= Durababble.default_schema unless store
      raise ArgumentError, "provide either store: or database_url:" unless store || database_url

      @rpc_host = rpc_host || raise(ArgumentError, "rpc_host is required")
      @rpc_port = rpc_port.nil? ? raise(ArgumentError, "rpc_port is required") : rpc_port

      raw_concurrency = concurrency #: as untyped
      parsed_concurrency = begin
        Integer(raw_concurrency)
      rescue ArgumentError, TypeError
        raise ArgumentError, "concurrency must be a positive integer"
      end
      parsed_concurrency = parsed_concurrency #: as Integer
      raise ArgumentError, "concurrency must be a positive integer" unless parsed_concurrency.positive?

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
      @concurrency = parsed_concurrency
      @migrate = migrate
      # @mutex guards lifecycle state touched by callers and the runtime task.
      # @wakeups is only a hint channel; durable work still lives in the store.
      @mutex = Mutex.new
      @wakeups = Async::Queue.new
      @deliveries = []
      @stopping = false
      @task = nil
      @raise_loop_errors = false
      @last_error = nil
      @consecutive_errors = 0
      @rpc_server = nil
      @rpc_address = nil
      @workflow_query_registry = WorkflowQueryRegistry.new
      @workflow_rpc_handlers = workflow_rpc_handlers
    end

    #: () -> WorkerRuntime
    def start
      start_task(parent: nil, operation: "WorkerRuntime.start", raise_loop_errors: false)
      self
    end

    #: (?parent: Object?) -> Object
    def start_async(parent: nil)
      start_task(parent:, operation: "WorkerRuntime.start_async", raise_loop_errors: true)
    end

    #: (?timeout: Numeric) -> Symbol
    def shutdown(timeout: DEFAULT_SHUTDOWN_TIMEOUT)
      task = @mutex.synchronize do
        @stopping = true
        @task
      end
      @wakeups.push(:stop)
      unless task
        stop_rpc_server
        return :stopped
      end

      attributes = {
        "durababble.worker.pool" => @worker_pool,
        "durababble.worker.id" => @worker_id,
      }
      if wait_for_task_stop(task, timeout:)
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

    #: (?timeout: Numeric?) -> Object?
    def wait(timeout: nil)
      task = @mutex.synchronize { @task }
      return unless task

      wait_for_task_stop(task, timeout:) ? task : nil
    end

    #: () -> bool
    def running?
      @task&.running? || false
    end

    #: () -> void
    def close
      shutdown
      @store.close if @owns_store
    end

    private

    #: (parent: Object?, operation: String, raise_loop_errors: bool) -> Object
    def start_task(parent:, operation:, raise_loop_errors:)
      @mutex.synchronize do
        return @task if running? && @task

        async_parent = parent || current_async_task!(operation)
        async_parent = async_parent #: as untyped
        worker = prepare_start_locked(parent: async_parent, raise_loop_errors:)
        @task = async_parent.async { |task| run_loop(task, worker) }
      end
    end

    #: (String) -> Object
    def current_async_task!(operation)
      Async::Task.current
    rescue RuntimeError
      raise ConfigurationError, "#{operation} requires an active Async task; wrap worker boot in Async { ... } or pass parent: from your application's Async supervisor"
    end

    #: (parent: Object, raise_loop_errors: bool) -> Worker
    def prepare_start_locked(parent:, raise_loop_errors:)
      Durababble.assert_fiber_isolation!
      @stopping = false
      @last_error = nil
      @consecutive_errors = 0
      @deliveries.clear
      clear_wakeups
      @raise_loop_errors = raise_loop_errors
      Observability.count(
        "durababble.worker.runtime.starts",
        "durababble.worker.pool" => @worker_pool,
        "durababble.worker.id" => @worker_id,
      )
      start_rpc_server(parent:)
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
      @raise_loop_errors = false
      raise
    end

    #: (parent: Object) -> untyped
    def start_rpc_server(parent:)
      server = Rpc::Server.new(
        node_id: nil,
        store: @store,
        worker_pool: @worker_pool,
        workflow_handlers: @workflow_rpc_handlers,
        host: @rpc_host,
        port: @rpc_port,
        verify_deliver_message_owner: false,
        identity_id: @worker_identity_id,
        deliver_message: method(:enqueue_delivery),
      )
      @rpc_server = server.start_async(parent:)
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

    #: (untyped, timeout: Numeric?) -> bool
    def wait_for_task_stop(task, timeout:)
      return false unless task

      deadline = timeout && Time.now + timeout
      while task.running?
        return false if deadline && Time.now >= deadline

        sleep_interval = deadline ? [0.005, deadline - Time.now].min : 0.005
        sleep(sleep_interval) if sleep_interval.positive?
      end
      true
    end

    #: () -> void
    def clear_wakeups
      @wakeups.dequeue(timeout: 0) until @wakeups.empty?
    end

    #: () -> Hash[String, Object]
    def workflow_rpc_handlers
      handlers = {}
      Durababble.normalize_workflows(@workflows).each_value do |workflow_class|
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
      until stopping? && active_targets.empty?
        begin
          poll_once(task, worker, active_targets) || wait_for_runtime_event(task, active_targets)
        rescue LeaseConflict => e
          record_worker_error(e)
        rescue StandardError => e
          record_worker_error(e)
          raise if @raise_loop_errors

          wait_for_runtime_event(task, active_targets)
        end
      end
    ensure
      stop_rpc_server
      @mutex.synchronize do
        @task = nil if Async::Task.current?.equal?(@task)
        @raise_loop_errors = false unless @task
      end
    end

    #: (untyped, untyped, Hash[Array[String], untyped]) -> bool
    def poll_once(task, worker, active_targets)
      return false if stopping?
      return false if active_targets.length >= @concurrency

      work_item = next_work_item(worker, active_targets)
      return false unless work_item

      start_work_item(task, worker, active_targets, work_item)
      true
    end

    #: (untyped, untyped, Hash[Array[String], untyped], untyped) -> void
    def start_work_item(task, worker, active_targets, scheduled_item)
      active_targets[scheduled_item.target_key] = true
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

    #: (untyped, Hash[Array[String], untyped]) -> untyped
    def next_work_item(worker, active_targets)
      loop do
        work_item = next_delivery_work(worker) || worker.claim_work(excluding_target_keys: active_targets.keys)
        return unless work_item
        return work_item unless active_targets.key?(work_item.target_key)

        defer_duplicate_work(worker, work_item)
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

    # Wakeup tokens are only hints. After each wake or timeout the loop re-checks
    # deliveries, active targets, and the store as the source of truth.
    #: (untyped, Hash[Array[String], untyped]) -> void
    def wait_for_runtime_event(task, active_targets)
      return if ready_to_poll?(active_targets)

      task.with_timeout(@poll_interval) { @wakeups.dequeue }
    rescue Async::TimeoutError
      nil
    ensure
      clear_wakeups
    end

    #: (Hash[Array[String], untyped]) -> bool
    def ready_to_poll?(active_targets)
      return false if stopping?

      active_targets.length < @concurrency && !deliveries_empty?
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
