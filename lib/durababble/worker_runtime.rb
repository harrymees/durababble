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

    #: (workflows: Object, worker_pool: String, ?objects: Object, ?store: Store?, ?database_url: String?, ?schema: String?, ?worker_id: String?, ?lease_seconds: Numeric, ?object_idle_ttl: Numeric?, ?poll_interval: Numeric, ?concurrency: Object, ?migrate: bool, ?rpc_host: String?, ?rpc_port: Integer?, ?rpc_credentials: Object?, ?rpc_pool_size: Integer) -> void
    def initialize(
      workflows:,
      worker_pool:,
      objects: [],
      store: nil,
      database_url: nil,
      schema: nil,
      worker_id: nil,
      lease_seconds: Engine::DEFAULT_LEASE_SECONDS,
      object_idle_ttl: nil,
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
      # How long a resident object instance stays warm after its last
      # command/query/stream before idle-TTL eviction runs `on_destroy` and
      # releases the lease. Defaults to `lease_seconds` (the host applies that
      # fallback when nil).
      @object_idle_ttl = object_idle_ttl
      @poll_interval = poll_interval
      @concurrency = parsed_concurrency
      @migrate = migrate
      @rpc_credentials = rpc_credentials
      @rpc_pool_size = rpc_pool_size
      # @mutex guards the lifecycle state shared between the control path and
      # the runtime owner (@stopping, @task, @deliveries). @wakeups is
      # only a hint channel to interrupt the polling fiber's idle sleep.
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
      @stream_dispatcher = nil
      @object_stream_host = nil
      @workflow_query_registry = WorkflowQueryRegistry.new
      @workflow_rpc_handlers = workflow_rpc_handlers
    end

    #: () -> WorkerRuntime
    def start
      @mutex.synchronize do
        return self if running?

        parent = Async::Task.current?
        raise ConfigurationError, "WorkerRuntime.start requires an active Async task; wrap worker boot in Async { ... } or call start_async(parent:)" unless parent

        worker = prepare_start_locked(async_parent: parent, raise_loop_errors: false)
        @task = parent.async { |task| run_loop(task, worker) }
      end
      self
    end

    #: (?parent: Object?) -> Object
    def start_async(parent: Async::Task.current?)
      raise ConfigurationError, "WorkerRuntime.start_async requires an active Async task; pass parent: from an Async block" unless parent

      @mutex.synchronize do
        return @task if running? && @task

        worker = prepare_start_locked(async_parent: parent, raise_loop_errors: true)
        @task = parent.async { |task| run_loop(task, worker) }
      end
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

    #: (async_parent: Object?, raise_loop_errors: bool) -> Worker
    def prepare_start_locked(async_parent:, raise_loop_errors:)
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
      start_rpc_server(parent: async_parent)
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

    #: (?parent: Object?) -> untyped
    def start_rpc_server(parent: nil)
      transient_handler = DurableObjectTransientHandler.new(store: @store, objects: @objects, node_id: -> { @worker_id })
      server = Rpc::Server.new(
        node_id: nil,
        store: @store,
        worker_pool: @worker_pool,
        workflow_handlers: @workflow_rpc_handlers,
        transient_handler:,
        host: @rpc_host,
        port: @rpc_port,
        credentials: @rpc_credentials,
        pool_size: @rpc_pool_size,
        verify_deliver_message_owner: false,
        identity_id: @worker_identity_id,
        deliver_message: method(:enqueue_delivery),
        stream_handler: method(:handle_stream),
        evict_lease: method(:handle_evict_lease),
      )
      @rpc_server = parent ? server.start_async(parent:) : server.start
      @rpc_address = @rpc_server.address
      # The server assigns its `node_id` during `start`, so it is non-nil here.
      node_id = @rpc_server.node_id #: as String
      @worker_id = node_id
      @store.local_worker_id = node_id
      @store.local_transient_handler = transient_handler
      # The host's renewal task uses `Store#with_dedicated_connection` so its
      # writes do not contend with the worker loop's reactor connection. Built
      # before the dispatcher so the dispatcher receives it.
      lease_seconds = @lease_seconds.to_i
      @object_stream_host = ObjectStreamHost.new(
        store: @store,
        worker_id: @worker_id,
        node_id:,
        worker_pool: @worker_pool,
        lease_seconds:,
        objects: @objects,
        idle_ttl: @object_idle_ttl,
      )
      @object_stream_host.start_async(parent:) if parent
      # Build the dispatcher once, now that `@worker_id` is assigned. It normalizes
      # the workflow/object registries up front, so reusing it avoids re-running
      # `normalize_registry` per stream open.
      @stream_dispatcher = StreamDispatcher.new(
        store: @store,
        workflows: @workflows,
        objects: @objects,
        node_id:,
        object_stream_host: @object_stream_host,
        lease_seconds:,
      )
      # Publish for `DurableObjectRef#open_object_stream` to self-route when no
      # owner exists yet. Multiple runtimes in one process (test HA only) means
      # last-writer-wins; production has one runtime per process so the caveat
      # does not bite.
      Durababble.local_stream_host = @object_stream_host
      # Register this runtime as the local workflow-query owner so a `WorkflowRef`
      # opened against the same store can dispatch queries in-process instead of
      # opening a loopback gRPC client just to call itself.
      configure_local_workflow_rpc(@store)
    end

    # Handles `Rpc::Server` `evict_lease` calls. For `target_kind == "object"`
    # this surfaces a `StaleLease` terminal frame to in-flight stream consumers
    # and releases the row. Workflow eviction continues to flow through the
    # workflow runtime; this is intentionally object-only here.
    #: (worker_pool: String, target_kind: String, target_class: String, target_id: String) -> void
    def handle_evict_lease(worker_pool:, target_kind:, target_class:, target_id:)
      return unless target_kind == "object"

      @object_stream_host&.evict!(worker_pool:, object_type: target_class, object_id: target_id)
    end

    # Routes a `call_transient_stream` RPC to the memoized `StreamDispatcher` built
    # in `start_rpc_server`.
    #: (request: untyped, args: untyped, writer: untyped) -> void
    def handle_stream(request:, args:, writer:)
      @stream_dispatcher&.call(request:, args:, writer:)
    end

    #: () -> untyped
    def stop_rpc_server
      server = @rpc_server
      return unless server

      # Evict in-flight object streams BEFORE the reactor interrupts; consumers
      # then receive a terminal `StaleLease` frame instead of a dropped gRPC
      # stream. The host's renewal task is stopped on the way out.
      host = @object_stream_host
      if host
        host.evict_all!
        host.stop!
      end

      Durababble.local_stream_host = nil if Durababble.local_stream_host.equal?(host)

      @store.local_worker_id = nil
      @store.local_transient_handler = nil
      clear_local_workflow_rpc(@store)
      @rpc_server = nil
      @rpc_address = nil
      @stream_dispatcher = nil
      @object_stream_host = nil
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
          raise if @raise_loop_errors

          await_work(task)
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
    # whichever comes first. Wakeup tokens are only hints: the real work lives in
    # @deliveries and the store, so draining stale tokens afterward is safe and
    # prevents the loop from spinning through a backlog of signals.
    #: (untyped) -> void
    def await_work(task)
      return if stopping? || !deliveries_empty?

      task.with_timeout(@poll_interval) { @wakeups.dequeue }
    rescue Async::TimeoutError
      nil
    ensure
      clear_wakeups
    end

    #: (untyped) -> void
    def await_active_work(task)
      task.with_timeout(@poll_interval) { @wakeups.dequeue }
    rescue Async::TimeoutError
      nil
    ensure
      clear_wakeups
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
