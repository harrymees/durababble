# typed: true
# frozen_string_literal: true

require "digest"
require "logger"

require_relative "durababble/version"
require_relative "durababble/statuses"
require_relative "durababble/observability"
require_relative "durababble/backoff"

module Durababble
  #: type durable_timestamp = Time | Integer | String
  #: type wait_kind = String
  #: type wait_status = String
  #: type wait_history_value = durable_timestamp | String | Integer | Object?
  #: type wait_history_event = Hash[String, wait_history_value]
  #: type wait_metadata = { "kind" => wait_kind, "event_key" => String?, "wake_at" => durable_timestamp?, "context" => Object? }
  #: type wait_event_payload = { "context" => Object?, "wait" => { "kind" => wait_kind, "event_key" => String?, "wake_at" => durable_timestamp? } }
  #: type wait_snapshot = { "id" => String, "workflow_id" => String, "position" => Integer, "command_id" => Integer, "kind" => wait_kind, "event_key" => String?, "wake_at" => durable_timestamp?, "context" => Object?, "status" => wait_status }

  DEFAULT_SCHEMA_PREFIX = "durababble"
  MAX_SCHEMA_IDENTIFIER_LENGTH = 63
  DEFAULT_MAX_WORKFLOW_HISTORY_EVENTS = 10_000
  DEFAULT_WARN_WORKFLOW_HISTORY_EVENTS = 8_000
  DEFAULT_MAX_PAYLOAD_BYTES = 4 * 1024 * 1024
  PAYLOAD_LIMIT_DEFAULTS = {
    workflow_input: DEFAULT_MAX_PAYLOAD_BYTES,
    workflow_result: DEFAULT_MAX_PAYLOAD_BYTES,
    step_output: DEFAULT_MAX_PAYLOAD_BYTES,
    object_state: DEFAULT_MAX_PAYLOAD_BYTES,
    inbox_payload: DEFAULT_MAX_PAYLOAD_BYTES,
    rpc_argument: DEFAULT_MAX_PAYLOAD_BYTES,
  }.freeze
  PAYLOAD_LIMIT_ENVS = {
    workflow_input: ["DURABABBLE_MAX_WORKFLOW_INPUT_BYTES", "DURABABBLE_MAX_WORKFLOW_ARGS_BYTES"],
    workflow_result: ["DURABABBLE_MAX_WORKFLOW_RESULT_BYTES"],
    step_output: ["DURABABBLE_MAX_STEP_OUTPUT_BYTES"],
    object_state: ["DURABABBLE_MAX_OBJECT_STATE_BYTES"],
    inbox_payload: ["DURABABBLE_MAX_INBOX_PAYLOAD_BYTES"],
    rpc_argument: ["DURABABBLE_MAX_RPC_ARGUMENT_BYTES"],
  }.freeze
  PAYLOAD_LIMIT_LABELS = {
    workflow_input: "workflow input",
    workflow_result: "workflow result",
    step_output: "step output",
    object_state: "object state",
    inbox_payload: "inbox payload",
    rpc_argument: "rpc argument",
  }.freeze
  PAYLOAD_LIMIT_ALIASES = {
    workflow_args: :workflow_input,
    workflow_error: :workflow_result,
  }.freeze

  class Error < StandardError; end
  class ConfigurationError < Error; end
  class InjectedCrash < Error; end
  class LeaseConflict < Error; end

  # DeterminismError rejects unsafe host APIs during the current workflow run;
  # ReplayDivergenceError rejects replay divergence from already persisted history.
  class DeterminismError < Error; end
  class ReplayDivergenceError < DeterminismError; end
  NonDeterminismError = ReplayDivergenceError

  class WorkflowHistoryLimitExceeded < Error
    #: String
    attr_reader :workflow_id
    #: Integer
    attr_reader :history_events, :max_history_events

    #: (String, history_events: Integer, max_history_events: Integer) -> void
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
  class ChildWorkflowError < Error; end
  class ChildWorkflowFailed < ChildWorkflowError; end
  class ChildWorkflowCanceled < ChildWorkflowError; end
  class ChildWorkflowTerminated < ChildWorkflowError; end

  class PayloadTooLarge < Error
    #: Symbol
    attr_reader :surface
    #: String?
    attr_reader :context
    #: Integer
    attr_reader :bytesize
    #: Integer
    attr_reader :max_bytes

    #: (Symbol | String, bytesize: Integer, max_bytes: Integer, ?context: String?) -> void
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
    #: Object?
    attr_writer :logger
    #: Integer | String
    attr_writer :max_workflow_history_events
    #: Integer | String
    attr_writer :workflow_history_warning_events

    #: (Store?) -> Store?
    def default_store=(store)
      @default_engine = store ? Engine.new(store:) : nil
      @default_store = store
    end

    #: (Engine?) -> Engine?
    def default_engine=(engine)
      typed_engine = engine #: as untyped
      @default_store = typed_engine&.store
      @default_engine = engine
    end

    #: () -> String
    def default_database_url
      ENV.fetch("DURABABBLE_DATABASE_URL")
    end

    # A workflow/object registry may be supplied as a Hash (name => class) or as
    # an Array/single class to be keyed by the class's canonical name. Both forms
    # normalize to a String-keyed Hash.
    #: (untyped) -> Hash[String, untyped]
    def normalize_workflows(workflows)
      case workflows
      when Hash
        workflows.transform_keys(&:to_s)
      else
        Array(workflows).to_h { |workflow_class| [workflow_class.workflow_name, workflow_class] }
      end
    end

    #: (untyped) -> Hash[String, untyped]
    def normalize_objects(objects)
      case objects
      when Hash
        objects.transform_keys(&:to_s)
      else
        Array(objects).to_h { |object_class| [object_class.object_type, object_class] }
      end
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

    #: () -> Hash[Symbol, Integer]
    def payload_limits
      PAYLOAD_LIMIT_DEFAULTS.each_with_object({}) do |(surface, default_value), limits|
        limits[surface] = payload_limit(surface, default_value)
      end
    end

    #: (Hash[Symbol | String, Integer | String] limits) -> Hash[Symbol | String, Integer | String]
    def payload_limits=(limits)
      @payload_limits = normalize_payload_limits(limits)
    end

    #: () -> Object?
    def logger
      return @logger if instance_variable_defined?(:@logger)

      @logger = Logger.new($stderr).tap { |logger| logger.level = Logger::WARN }
    end

    #: (workflow_id: String, history_events: Integer, max_history_events: Integer) -> bool
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

    # Durababble runs worker runtimes and workflows inside an Async fiber reactor.
    # Each workflow step, durable-object command drain, handle-RPC dispatch task, and
    # user-spawned fan-out fiber needs its OWN ActiveRecord connection — otherwise
    # concurrent fibers interleave packets on a shared socket and corrupt the wire
    # protocol (trilogy/pg yield to the fiber scheduler mid-query via rb_wait_for_single_fd).
    #
    # ActiveRecord checks connections out per `IsolatedExecutionState.isolation_level`.
    # The default is :thread, which is wrong for us. Hosts running workflows must run
    # with :fiber — Falcon's Railtie already sets this defensively even under Puma.
    #
    # We don't set it ourselves: it's a process-global affecting the entire host. We
    # only refuse to boot a workflow if it isn't right. Call this lazily — at workflow
    # execution time, not at gem load — so the host's initializers (Rails or otherwise)
    # have had a chance to set it.
    #: () -> void
    def assert_fiber_isolation!
      return if isolated_execution_state_isolation_level == :fiber

      raise ConfigurationError, <<~MSG.tr("\n", " ").strip
        Durababble requires ActiveSupport::IsolatedExecutionState.isolation_level = :fiber
        (current: #{isolated_execution_state_isolation_level.inspect}). Worker runtimes
        and workflows run fibers that each need their own ActiveRecord connection; the
        default :thread isolation causes wire-protocol corruption when fibers interleave
        on a shared connection. Set ActiveSupport::IsolatedExecutionState.isolation_level
        = :fiber in your host before booting a Durababble worker. Falcon's Railtie does
        this defensively even under Puma.
      MSG
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

      execution = WorkflowExecutionContext.current #: as untyped
      return execution.store if execution&.respond_to?(:store)

      self.engine.store
    end

    #: (String | Symbol, **Object?) { (*Object?, **Object?) -> Object? } -> Step
    def step(name, **options, &block)
      retry_policy = options.fetch(:retry_policy, options[:retry]) #: as RetryPolicy | Hash[Symbol, Object?] | nil
      Step.new(name:, retry_policy: RetryPolicy.from(retry_policy), body: block)
    end

    #: () -> StepContext
    def step_context
      StepExecutionContext.current || raise(Error, "step_context is only available while a workflow step is executing")
    end

    #: (Time, ?Object?) -> (WaitRequest | Object?)
    def wait_until(time, context = {})
      wait_request = WaitRequest.new(kind: "timer", wake_at: time, event_key: nil, context:)
      if (execution = WorkflowExecutionContext.current)
        return execution.call_wait(wait_request, name: "wait_until", args: [time, context])
      end

      wait_request
    end

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

    # True when the consumer of the streaming-result RPC currently being produced
    # has gone away (closed the stream / cancelled the gRPC response). An
    # `expose_stream` method producing an indefinite stream should poll this and
    # return when it flips. Outside a streaming producer it is always false.
    #: () -> bool
    def stream_cancelled?
      writer = StreamExecutionContext.current
      writer ? writer.cancelled? : false
    end

    # Process-global `ObjectStreamHost` registered by the running worker
    # runtime, used by `DurableObjectRef#open_object_stream` to self-route a
    # first-opener via loopback RPC when no live lease yet exists. Cleared on
    # runtime shutdown. With multiple worker runtimes in one process (test HA
    # only) this is last-writer-wins; the high-value crash / evict / heartbeat
    # tests are written at the RPC + dispatcher level where the host is passed
    # in directly, so the caveat does not bite.
    #: ObjectStreamHost?
    attr_accessor :local_stream_host

    private

    #: () -> Symbol
    def isolated_execution_state_isolation_level
      unless defined?(ActiveSupport::IsolatedExecutionState)
        raise ConfigurationError, "Durababble requires ActiveSupport::IsolatedExecutionState; load active_support before booting a worker."
      end

      ActiveSupport::IsolatedExecutionState.isolation_level
    end

    #: (Symbol, Integer) -> Integer
    def payload_limit(surface, default_value)
      configured_limits = instance_variable_defined?(:@payload_limits) ? normalize_payload_limits(@payload_limits) : {}
      configured = if configured_limits.key?(surface)
        configured_limits.fetch(surface)
      elsif (env_name = PAYLOAD_LIMIT_ENVS.fetch(surface).find { |name| ENV.key?(name) })
        ENV.fetch(env_name)
      else
        default_value
      end
      Integer(configured).tap do |value|
        raise ArgumentError, "#{PAYLOAD_LIMIT_LABELS.fetch(surface)} payload limit must be positive" unless value.positive?
      end
    end

    #: (Symbol | String) -> Integer
    def payload_limit_for_surface(surface)
      payload_limits.fetch(canonical_payload_limit_surface(surface))
    end

    #: (Hash[Symbol | String, Integer | String]) -> Hash[Symbol, Integer | String]
    def normalize_payload_limits(limits)
      limits.each_with_object({}) do |(surface, value), normalized|
        normalized[canonical_payload_limit_surface(surface)] = value
      end
    end

    #: (Symbol | String) -> Symbol
    def canonical_payload_limit_surface(surface)
      surface = surface.to_sym
      surface = PAYLOAD_LIMIT_ALIASES.fetch(surface, surface)
      return surface if PAYLOAD_LIMIT_DEFAULTS.key?(surface)

      raise ArgumentError, "unknown payload limit surface: #{surface}"
    end

    #: (Object?) -> String
    def schema_component(value)
      component = value.to_s.downcase.gsub(/[^a-z0-9_]+/, "_").gsub(/\A_+|_+\z/, "")
      component.empty? ? "workspace" : component
    end
  end
end

require_relative "durababble/retry_policy"
require_relative "durababble/child_workflow_reuse"
require_relative "durababble/workflow"
require_relative "durababble/workflow_query_registry"
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
require_relative "durababble/result_stream"
require_relative "durababble/rpc_transport"
require_relative "durababble/object_stream_host"
require_relative "durababble/stream_dispatcher"
