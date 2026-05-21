# frozen_string_literal: true

require "securerandom"

module Durababble
  class WorkerRuntime
    DEFAULT_POLL_INTERVAL = 0.1
    DEFAULT_SHUTDOWN_TIMEOUT = 10

    attr_reader :store, :workflows, :worker_pool, :worker_id, :last_error

    def self.start(**kwargs)
      new(**kwargs).tap(&:start)
    end

    def initialize(store: nil, database_url: nil, schema: "durababble", workflows:, worker_pool:, worker_id: nil, lease_seconds: Engine::DEFAULT_LEASE_SECONDS, poll_interval: DEFAULT_POLL_INTERVAL, migrate: true)
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

    def start
      @mutex.synchronize do
        return self if running?

        @stopping = false
        @last_error = nil
        worker = Worker.new(store: @store, workflows: @workflows, worker_id: @worker_id, lease_seconds: @lease_seconds, migrate: @migrate)
        @thread = Thread.new { run_loop(worker) }
      end
      self
    end

    def shutdown(timeout: DEFAULT_SHUTDOWN_TIMEOUT)
      thread = nil
      @mutex.synchronize do
        @stopping = true
        thread = @thread
      end
      return :stopped unless thread

      return :stopped if thread.join(timeout)

      @store.release_worker_leases!(worker_id: @worker_id)
      :timeout
    end

    alias stop shutdown

    def wait(timeout: nil)
      thread = @mutex.synchronize { @thread }
      timeout ? thread&.join(timeout) : thread&.join
    end

    def running?
      @thread&.alive? || false
    end

    def close
      shutdown
      @store.close if @owns_store
    end

    private

    def run_loop(worker)
      loop do
        break if stopping?

        begin
          result = worker.tick
          sleep @poll_interval if result == :idle && !stopping?
        rescue LeaseConflict => e
          @last_error = e
          break if stopping?
        rescue StandardError => e
          @last_error = e
          break if stopping?

          sleep @poll_interval
        end
      end
    ensure
      @mutex.synchronize { @thread = nil if Thread.current == @thread }
    end

    def stopping?
      @mutex.synchronize { @stopping }
    end
  end
end
