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
  DEFAULT_MAX_WORKFLOW_INPUT_BYTES = 4 * 1024 * 1024
  DEFAULT_MAX_WORKFLOW_RESULT_BYTES = 4 * 1024 * 1024
  DEFAULT_MAX_STEP_OUTPUT_BYTES = 4 * 1024 * 1024
  DEFAULT_MAX_OBJECT_STATE_BYTES = 4 * 1024 * 1024
  DEFAULT_MAX_INBOX_PAYLOAD_BYTES = 4 * 1024 * 1024
  DEFAULT_MAX_RPC_ARGUMENT_BYTES = 4 * 1024 * 1024

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

  class PayloadTooLarge < Error
    #: Symbol
    attr_reader :surface
    #: String?
    attr_reader :context
    #: Integer
    attr_reader :bytesize
    #: Integer
    attr_reader :max_bytes

    #: (Symbol | String surface, bytesize: Integer, max_bytes: Integer, ?context: String?) -> void
    def initialize(surface, bytesize:, max_bytes:, context: nil)
      @surface = surface.to_sym
      @context = context
      @bytesize = bytesize
      @max_bytes = max_bytes
      context_suffix = context.to_s.empty? ? "" : " for #{context}"
      super("#{surface_name(@surface)} payload#{context_suffix} is #{bytesize} bytes, exceeding max #{max_bytes} bytes")
    end

    private

    #: (Symbol) -> String
    def surface_name(surface)
      surface.to_s.tr("_", " ")
    end
  end

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
    #: untyped
    attr_writer :max_workflow_input_bytes
    #: untyped
    attr_writer :max_workflow_result_bytes
    #: untyped
    attr_writer :max_step_output_bytes
    #: untyped
    attr_writer :max_object_state_bytes
    #: untyped
    attr_writer :max_inbox_payload_bytes
    #: untyped
    attr_writer :max_rpc_argument_bytes

    #: (Store?) -> Engine?
    def default_store=(store)
      @default_store = store
      @default_engine = store ? Engine.new(store:) : nil
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

    #: () -> Integer
    def max_workflow_input_bytes
      payload_limit(:@max_workflow_input_bytes, "DURABABBLE_MAX_WORKFLOW_INPUT_BYTES", DEFAULT_MAX_WORKFLOW_INPUT_BYTES, "max workflow input bytes", fallback_env: "DURABABBLE_MAX_WORKFLOW_ARGS_BYTES")
    end

    #: () -> Integer
    def max_workflow_args_bytes = max_workflow_input_bytes

    #: ((Integer | String) value) -> (Integer | String)
    def max_workflow_args_bytes=(value)
      self.max_workflow_input_bytes = value
    end

    #: () -> Integer
    def max_workflow_result_bytes
      payload_limit(:@max_workflow_result_bytes, "DURABABBLE_MAX_WORKFLOW_RESULT_BYTES", DEFAULT_MAX_WORKFLOW_RESULT_BYTES, "max workflow result bytes")
    end

    #: () -> Integer
    def max_step_output_bytes
      payload_limit(:@max_step_output_bytes, "DURABABBLE_MAX_STEP_OUTPUT_BYTES", DEFAULT_MAX_STEP_OUTPUT_BYTES, "max step output bytes")
    end

    #: () -> Integer
    def max_object_state_bytes
      payload_limit(:@max_object_state_bytes, "DURABABBLE_MAX_OBJECT_STATE_BYTES", DEFAULT_MAX_OBJECT_STATE_BYTES, "max object state bytes")
    end

    #: () -> Integer
    def max_inbox_payload_bytes
      payload_limit(:@max_inbox_payload_bytes, "DURABABBLE_MAX_INBOX_PAYLOAD_BYTES", DEFAULT_MAX_INBOX_PAYLOAD_BYTES, "max inbox payload bytes")
    end

    #: () -> Integer
    def max_rpc_argument_bytes
      payload_limit(:@max_rpc_argument_bytes, "DURABABBLE_MAX_RPC_ARGUMENT_BYTES", DEFAULT_MAX_RPC_ARGUMENT_BYTES, "max rpc argument bytes")
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

    #: (surface: Symbol | String, bytesize: Integer, ?context: String?) -> true
    def enforce_payload_limit!(surface:, bytesize:, context: nil)
      surface = surface.to_sym
      max_bytes = payload_limit_for_surface(surface)
      return true if bytesize <= max_bytes

      raise PayloadTooLarge.new(surface, bytesize:, max_bytes:, context:)
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
      @default_engine ||= Engine.new(store:)
    end

    #: (?engine: Engine?, ?store: Store?) -> Engine
    def engine_for(engine: nil, store: nil)
      raise ArgumentError, "pass store: or engine:, not both" if store && engine

      return engine if engine
      return Engine.new(store:) if store

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

    #: (Symbol, String, Integer, String, ?fallback_env: String?) -> Integer
    def payload_limit(ivar, env_name, default_value, label, fallback_env: nil)
      configured = if instance_variable_defined?(ivar)
        instance_variable_get(ivar)
      elsif ENV.key?(env_name)
        ENV.fetch(env_name)
      elsif fallback_env && ENV.key?(fallback_env)
        ENV.fetch(fallback_env)
      else
        default_value
      end
      Integer(configured).tap do |value|
        raise ArgumentError, "#{label} must be positive" unless value.positive?
      end
    end

    #: (Symbol) -> Integer
    def payload_limit_for_surface(surface)
      case surface
      when :workflow_input, :workflow_args
        max_workflow_input_bytes
      when :workflow_result, :workflow_error
        max_workflow_result_bytes
      when :step_output
        max_step_output_bytes
      when :object_state
        max_object_state_bytes
      when :inbox_payload
        max_inbox_payload_bytes
      when :rpc_argument
        max_rpc_argument_bytes
      else
        raise ArgumentError, "unknown payload limit surface: #{surface}"
      end
    end

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
require_relative "durababble/worker_identity"
require_relative "durababble/store_queries"
require_relative "durababble/store"
require_relative "durababble/engine"
require_relative "durababble/run"
require_relative "durababble/worker"
require_relative "durababble/worker_runtime"

require_relative "durababble/workflow_rpc"
require_relative "durababble/rpc_transport"
