# typed: true
# frozen_string_literal: true

require "securerandom"

module Durababble
  class WorkerRuntime
    DEFAULT_POLL_INTERVAL = 0.1
    DEFAULT_SHUTDOWN_TIMEOUT = 10

    #: untyped
    attr_reader :store, :workflows, :worker_pool, :worker_id, :last_error

    class << self
      #: (**untyped) -> untyped
      def start(**kwargs)
        runtime = self #: as untyped
        runtime.new(**kwargs).tap(&:start)
      end
    end

    #: (workflows: untyped, worker_pool: untyped, ?store: untyped, ?database_url: untyped, ?schema: untyped, ?worker_id: untyped, ?lease_seconds: untyped, ?poll_interval: untyped, ?migrate: untyped) -> void
    def initialize(workflows:, worker_pool:, store: nil, database_url: nil, schema: nil, worker_id: nil, lease_seconds: Engine::DEFAULT_LEASE_SECONDS, poll_interval: DEFAULT_POLL_INTERVAL, migrate: true)
      schema ||= Durababble.default_schema unless store
      raise ArgumentError, "provide either store: or database_url:" unless store || database_url

      @store = store || Store.connect(database_url:, schema:)
      @owns_store = store.nil?
      @workflows = workflows
      @worker_pool = worker_pool
      @worker_id = worker_id || "#{worker_pool}-#{SecureRandom.hex(6)}"
      @lease_seconds = lease_seconds
      @poll_interval = poll_interval
      @migrate = migrate
      @mutex = Mutex.new
      @stopping = false
      @thread = nil
      @last_error = nil
    end

    #: () -> untyped
    def start
      @mutex.synchronize do
        return self if running?

        @stopping = false
        @last_error = nil
        Observability.count(
          "durababble.worker.runtime.starts",
          "durababble.worker.pool" => @worker_pool,
          "durababble.worker.id" => @worker_id,
        )
        worker = Worker.new(store: @store, workflows: @workflows, worker_id: @worker_id, lease_seconds: @lease_seconds, migrate: @migrate)
        @thread = Thread.new { run_loop(worker) }
      end
      self
    end

    #: (?timeout: untyped) -> untyped
    def shutdown(timeout: DEFAULT_SHUTDOWN_TIMEOUT)
      thread = @mutex.synchronize do
        @stopping = true
        @thread
      end
      return :stopped unless thread

      attributes = {
        "durababble.worker.pool" => @worker_pool,
        "durababble.worker.id" => @worker_id,
      }
      if thread.join(timeout)
        Observability.count("durababble.worker.runtime.shutdowns", attributes.merge("durababble.worker.runtime.result" => "stopped"))
        return :stopped
      end

      released = @store.release_worker_leases!(worker_id: @worker_id)
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

    #: (untyped) -> untyped
    def run_loop(worker)
      loop do
        break if stopping?

        begin
          result = worker.tick
          sleep(@poll_interval) if result == :idle && !stopping?
        rescue LeaseConflict => e
          @last_error = e
          break if stopping?
        rescue StandardError => e
          @last_error = e
          break if stopping?

          sleep(@poll_interval)
        end
      end
    ensure
      @mutex.synchronize { @thread = nil if Thread.current == @thread }
    end

    #: () -> untyped
    def stopping?
      @mutex.synchronize { @stopping }
    end
  end
end
