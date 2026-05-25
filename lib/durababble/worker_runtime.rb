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
      @worker_id = worker_id || "#{worker_pool}-#{SecureRandom.hex(6)}"
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
      @rpc_server = nil
      @rpc_address = nil
      @worker_store = nil
      @rpc_store = nil
    end

    #: () -> untyped
    def start
      @mutex.synchronize do
        return self if running?

        @stopping = false
        @last_error = nil
        @deliveries.clear
        Observability.count(
          "durababble.worker.runtime.starts",
          "durababble.worker.pool" => @worker_pool,
          "durababble.worker.id" => @worker_id,
        )
        start_rpc_server
        worker = begin
          Worker.new(store: worker_store, workflows: @workflows, objects: @objects, worker_id: @worker_id, lease_seconds: @lease_seconds, migrate: @migrate, worker_pool: @worker_pool)
        rescue StandardError
          stop_rpc_server
          close_isolated_stores
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
      close_isolated_stores
      @store.close if @owns_store
    end

    private

    #: () -> untyped
    def worker_store
      @worker_store ||= isolated_store
    end

    #: () -> untyped
    def rpc_store
      @rpc_store ||= isolated_store
    end

    #: () -> untyped
    def start_rpc_server
      @rpc_server = Rpc::Server.new(
        node_id: nil,
        store: rpc_store,
        worker_pool: @worker_pool,
        host: @rpc_host,
        port: @rpc_port,
        credentials: @rpc_credentials,
        pool_size: @rpc_pool_size,
        verify_deliver_message_owner: false,
        deliver_message: method(:enqueue_delivery),
      ).start
      @rpc_address = @rpc_server.address
      @worker_id = @rpc_server.node_id
    end

    #: () -> untyped
    def stop_rpc_server
      server = @rpc_server
      return unless server

      @rpc_server = nil
      @rpc_address = nil
      server.stop
    end

    #: () -> untyped
    def isolated_store
      return @store unless @store.respond_to?(:pooled_connections)

      @store.pooled_connections
    end

    #: () -> void
    def close_isolated_stores
      [@worker_store, @rpc_store].compact.uniq.each do |isolated_store|
        next if isolated_store.equal?(@store)
        next unless isolated_store.respond_to?(:close)

        isolated_store.close
      end
      @worker_store = nil
      @rpc_store = nil
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
          wait_for_work if result == :idle && !stopping?
        rescue LeaseConflict => e
          @last_error = e
          break if stopping?
        rescue StandardError => e
          @last_error = e
          break if stopping?

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
  end
end
