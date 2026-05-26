# typed: true
# frozen_string_literal: true

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
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @deliveries = []
      @stopping = false
      @thread = nil
      @last_error = nil
      @consecutive_errors = 0
      @rpc_server = nil
      @rpc_address = nil
      @worker_store = nil
      @rpc_store = nil
      @workflow_query_registry = WorkflowQueryRegistry.new
      @workflow_rpc_handlers = workflow_rpc_handlers
    end

    #: () -> untyped
    def start
      @mutex.synchronize do
        return self if running?

        @stopping = false
        @last_error = nil
        @consecutive_errors = 0
        @deliveries.clear
        Observability.count(
          "durababble.worker.runtime.starts",
          "durababble.worker.pool" => @worker_pool,
          "durababble.worker.id" => @worker_id,
        )
        start_rpc_server
        worker = begin
          Worker.new(
            store: worker_store,
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
        @thread = Thread.new { run_loop(worker) }
      end
      self
    end

    #: (?timeout: untyped) -> untyped
    def shutdown(timeout: DEFAULT_SHUTDOWN_TIMEOUT)
      thread = @mutex.synchronize do
        @stopping = true
        @condition.broadcast
        @thread
      end
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
    def worker_store
      @worker_store ||= @store
    end

    #: () -> untyped
    def rpc_store
      @rpc_store ||= @store
    end

    #: () -> untyped
    def start_rpc_server
      @rpc_server = Rpc::Server.new(
        node_id: nil,
        store: rpc_store,
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
        @condition.signal
      end
    end

    #: (untyped) -> untyped
    def run_loop(worker)
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
          wait_for_work if result == :idle && !stopping?
        rescue LeaseConflict => e
          @last_error = e
          break if stopping?
        rescue StandardError => e
          @last_error = e
          @consecutive_errors += 1
          break if stopping?

          log_loop_error(e)
          wait_for_work
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

    #: () -> untyped
    def wait_for_work
      @mutex.synchronize do
        @condition.wait(@mutex, @poll_interval) unless @stopping || !@deliveries.empty?
      end
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
