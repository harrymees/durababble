# typed: true
# frozen_string_literal: true

require "digest"
require "logger"

require_relative "durababble/version"
require_relative "durababble/statuses"
require_relative "durababble/observability"

module Durababble
  DEFAULT_DATABASE_URL = "mysql://root@127.0.0.1:3306/sidekick_server_development"
  DEFAULT_SCHEMA_PREFIX = "durababble"
  MAX_SCHEMA_IDENTIFIER_LENGTH = 63
  DEFAULT_MAX_WORKFLOW_HISTORY_EVENTS = 10_000
  DEFAULT_WARN_WORKFLOW_HISTORY_EVENTS = 8_000

  class Error < StandardError; end
  class InjectedCrash < Error; end
  class LeaseConflict < Error; end
  class DeterminismError < Error; end
  class NonDeterminismError < DeterminismError; end

  class WorkflowHistoryLimitExceeded < Error
    #: untyped
    attr_reader :workflow_id, :history_events, :max_history_events

    #: (untyped, history_events: Integer, max_history_events: Integer) -> void
    def initialize(workflow_id, history_events:, max_history_events:)
      @workflow_id = workflow_id
      @history_events = history_events
      @max_history_events = max_history_events
      super("workflow #{workflow_id} has #{history_events} history events, exceeding max #{max_history_events}")
    end
  end

  class FenceTimeout < Error; end
  class CommandTimeout < Error; end
  class IdempotencyKeyConflict < Error; end
  class WorkflowAlreadyExists < Error; end

  class CancellationError < Error
    #: String?
    attr_reader :workflow_id
    #: Object?
    attr_reader :reason

    #: (?Object?, ?workflow_id: String?) -> void
    def initialize(reason = nil, workflow_id: nil)
      @workflow_id = workflow_id
      @reason = reason
      message = reason.to_s.empty? ? "workflow cancellation requested" : reason.to_s
      super(message)
    end
  end

  class << self
    #: Store?
    attr_reader :default_store
    #: Engine?
    attr_reader :default_engine
    #: untyped
    attr_writer :logger
    #: untyped
    attr_writer :max_workflow_history_events
    #: untyped
    attr_writer :workflow_history_warning_events

    #: (Store?) -> Engine?
    def default_store=(store)
      @default_store = store
      @default_engine = store ? Engine.new(store:, migrate: false) : nil
    end

    #: (Engine?) -> Store?
    def default_engine=(engine)
      @default_engine = engine
      engine = engine #: as untyped
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

    #: () -> Integer
    def max_workflow_history_events
      configured = if instance_variable_defined?(:@max_workflow_history_events)
        @max_workflow_history_events
      else
        ENV.fetch("DURABABBLE_MAX_WORKFLOW_HISTORY_EVENTS", DEFAULT_MAX_WORKFLOW_HISTORY_EVENTS)
      end
      Integer(configured).tap do |value|
        raise ArgumentError, "max workflow history events must be positive" unless value.positive?
      end
    end

    #: () -> Integer
    def workflow_history_warning_events
      configured = if instance_variable_defined?(:@workflow_history_warning_events)
        @workflow_history_warning_events
      else
        ENV.fetch("DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS", DEFAULT_WARN_WORKFLOW_HISTORY_EVENTS)
      end
      Integer(configured).tap do |value|
        raise ArgumentError, "workflow history warning events must be positive" unless value.positive?
      end
    end

    #: () -> untyped
    def logger
      return @logger if instance_variable_defined?(:@logger)

      @logger = Logger.new($stderr).tap { |logger| logger.level = Logger::WARN }
    end

    #: (workflow_id: untyped, history_events: Integer, max_history_events: Integer) -> bool
    def warn_workflow_history_events(workflow_id:, history_events:, max_history_events:)
      warning_events = workflow_history_warning_events
      return false if history_events < warning_events

      logger&.warn(
        "Durababble workflow #{workflow_id} has #{history_events} workflow history events; " \
          "warning threshold is #{warning_events}, max is #{max_history_events}",
      )
      true
    end

    #: (database_url: String, ?schema: String) -> Store
    def configure(database_url:, schema: default_schema)
      @default_store&.close
      self.default_store = Store.connect(database_url:, schema:)
    end

    #: (?enabled: bool, ?attributes: Hash[String | Symbol, Object?]) -> Observability::Configuration
    def configure_observability(enabled: false, attributes: {})
      Observability.configure(enabled:, attributes:)
    end

    #: () -> Observability::Configuration
    def observability
      Observability.configuration
    end

    #: () -> Store
    def store
      @default_store || raise(Error, "Durababble.store is not configured; pass store: or call Durababble.configure")
    end

    #: () -> Engine
    def engine
      @default_engine ||= Engine.new(store:, migrate: false)
    end

    #: (?engine: Engine?, ?store: Store?) -> Engine
    def engine_for(engine: nil, store: nil)
      raise ArgumentError, "pass store: or engine:, not both" if store && engine

      return engine if engine
      return Engine.new(store:, migrate: false) if store

      self.engine
    end

    #: (?engine: Engine?, ?store: Store?) -> Store
    def store_for(engine: nil, store: nil)
      raise ArgumentError, "pass store: or engine:, not both" if store && engine

      return store if store

      engine = engine #: as untyped
      return engine.store if engine

      self.engine.store
    end

    #: (Time, ?Object?) -> (WaitRequest | Object?)
    def wait_until(time, context = {})
      wait_request = WaitRequest.new(kind: "timer", wake_at: time, event_key: nil, context:)
      if (execution = WorkflowExecutionContext.current)
        return execution.call_wait(wait_request, name: "wait_until", args: [time, context])
      end

      wait_request
    end

    alias_method :sleep_until, :wait_until

    #: (Numeric, ?Object?) -> Object?
    def sleep(duration, context = {})
      execution = WorkflowExecutionContext.current
      return Kernel.sleep(duration) unless execution

      wait_request = WaitRequest.new(kind: "timer", wake_at: execution.timer_after(duration), event_key: nil, context:)
      execution.call_wait(wait_request, name: "sleep", args: [duration, context])
    end

    #: (?timeout: Numeric?) { -> bool } -> bool
    def wait_condition(timeout: nil, &block)
      execution = WorkflowExecutionContext.current
      raise Error, "wait_condition must run inside workflow orchestration" unless execution

      execution.wait_condition(timeout:, &block)
    end

    private

    #: (Object?) -> String
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

require_relative "durababble/workflow_rpc"
require_relative "durababble/rpc_transport"
