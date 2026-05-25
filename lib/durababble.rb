# typed: true
# frozen_string_literal: true

require "digest"

require_relative "durababble/version"
require_relative "durababble/statuses"
require_relative "durababble/observability"

module Durababble
  DEFAULT_DATABASE_URL = "mysql://root@127.0.0.1:3306/sidekick_server_development"
  DEFAULT_SCHEMA_PREFIX = "durababble"
  MAX_SCHEMA_IDENTIFIER_LENGTH = 63

  class Error < StandardError; end
  class InjectedCrash < Error; end
  class LeaseConflict < Error; end
  class DeterminismError < Error; end
  class NonDeterminismError < DeterminismError; end
  class FenceTimeout < Error; end
  class CommandTimeout < Error; end
  class IdempotencyKeyConflict < Error; end

  class CancellationError < Error
    #: untyped
    attr_reader :workflow_id, :reason

    #: (?untyped, ?workflow_id: untyped) -> void
    def initialize(reason = nil, workflow_id: nil)
      @workflow_id = workflow_id
      @reason = reason
      message = reason.to_s.empty? ? "workflow cancellation requested" : reason.to_s
      super(message)
    end
  end

  class << self
    #: (untyped) -> untyped
    attr_reader :default_store, :default_engine

    #: (untyped) -> untyped
    def default_store=(store)
      @default_store = store
      @default_engine = store ? Engine.new(store:) : nil
    end

    #: (untyped) -> untyped
    def default_engine=(engine)
      @default_engine = engine
      @default_store = engine&.store
    end

    #: () -> String
    def default_database_url
      ENV.fetch("DURABABBLE_DATABASE_URL", DEFAULT_DATABASE_URL)
    end

    #: () -> String
    def default_schema
      ENV.fetch("DURABABBLE_SCHEMA") { workspace_schema }
    end

    #: (?String, ?prefix: String) -> String
    def workspace_schema(workspace_path = ENV.fetch("DURABABBLE_WORKSPACE_ROOT", Dir.pwd), prefix: DEFAULT_SCHEMA_PREFIX)
      expanded_path = File.expand_path(workspace_path)
      path = File.exist?(expanded_path) ? File.realpath(expanded_path) : expanded_path
      suffix = Digest::SHA256.hexdigest(path).slice(0, 12).to_s
      leaf = schema_component(File.basename(path))
      base = "#{schema_component(prefix)}_#{leaf}"
      max_base_length = MAX_SCHEMA_IDENTIFIER_LENGTH - suffix.length - 1
      trimmed_base = base.slice(0, max_base_length).to_s.sub(/_+\z/, "")
      "#{trimmed_base}_#{suffix}"
    end

    #: (database_url: untyped, ?schema: untyped) -> untyped
    def configure(database_url:, schema: default_schema)
      @default_store&.close
      self.default_store = Store.connect(database_url:, schema:)
    end

    #: (?enabled: untyped, ?attributes: untyped) -> untyped
    def configure_observability(enabled: false, attributes: {})
      Observability.configure(enabled:, attributes:)
    end

    #: () -> untyped
    def observability
      Observability.configuration
    end

    #: () -> untyped
    def store
      @default_store || raise(Error, "Durababble.store is not configured; pass store: or call Durababble.configure")
    end

    #: () -> untyped
    def engine
      @default_engine ||= Engine.new(store:)
    end

    #: (?engine: untyped, ?store: untyped) -> untyped
    def engine_for(engine: nil, store: nil)
      raise ArgumentError, "pass store: or engine:, not both" if store && engine

      return engine if engine
      return Engine.new(store:) if store

      self.engine
    end

    #: (?engine: untyped, ?store: untyped) -> untyped
    def store_for(engine: nil, store: nil)
      raise ArgumentError, "pass store: or engine:, not both" if store && engine

      return store if store
      return engine.store if engine

      self.engine.store
    end

    #: (untyped, ?untyped) -> untyped
    def wait_until(time, context = {})
      wait_request = WaitRequest.new(kind: "timer", wake_at: time, event_key: nil, context:)
      if (execution = WorkflowExecutionContext.current)
        return execution.call_wait(wait_request, name: "wait_until", args: [time, context])
      end

      wait_request
    end

    alias_method :sleep_until, :wait_until

    #: (untyped, ?untyped) -> untyped
    def sleep(duration, context = {})
      execution = WorkflowExecutionContext.current
      return Kernel.sleep(duration) unless execution

      wait_request = WaitRequest.new(kind: "timer", wake_at: execution.timer_after(duration), event_key: nil, context:)
      execution.call_wait(wait_request, name: "sleep", args: [duration, context])
    end

    #: (?timeout: untyped) { -> bool } -> bool
    def wait_condition(timeout: nil, &block)
      execution = WorkflowExecutionContext.current
      raise Error, "wait_condition must run inside workflow orchestration" unless execution

      execution.wait_condition(timeout:, &block)
    end

    private

    #: (untyped) -> String
    def schema_component(value)
      component = value.to_s.downcase.gsub(/[^a-z0-9_]+/, "_").gsub(/\A_+|_+\z/, "")
      component.empty? ? "workspace" : component
    end
  end
end

require_relative "durababble/retry_policy"
require_relative "durababble/workflow"
require_relative "durababble/durable_object"
require_relative "durababble/wait_request"
require_relative "durababble/store_queries"
require_relative "durababble/store"
require_relative "durababble/engine"
require_relative "durababble/run"
require_relative "durababble/worker"
require_relative "durababble/worker_runtime"

require_relative "durababble/rpc_client"
require_relative "durababble/workflow_rpc"
require_relative "durababble/rpc_transport"
