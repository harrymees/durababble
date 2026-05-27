# typed: true
# frozen_string_literal: true

require "active_record"
require "digest"
require "paquito"
require "securerandom"
require "time"
require "uri"
require_relative "worker_identity"

module Durababble
  class Store
    # Columns whose values are Paquito-serialized blobs. decode_row probes this
    # set once per column per row, so it is a Set for O(1) membership rather than
    # a linear array scan.
    SERIALIZED_COLUMNS = Set["input", "result", "payload", "context", "heartbeat_cursor", "state", "args", "kwargs"].freeze
    SERIALIZER = Paquito::SingleBytePrefixVersion.new(1, 1 => Marshal)
    NO_OBJECT_STATE = Object.new.freeze
    GENERATED_CONNECTION_CONST_IVAR = :@durababble_store_connection_const_name
    GENERATED_CONNECTION_CLASSES = {}
    # Guards both the registry hash above and the matching const_set/remove_const
    # on the Durababble namespace, so concurrent Store.connect calls cannot race
    # on the shared global state.
    GENERATED_CONNECTION_MUTEX = Mutex.new

    #: String
    attr_reader :schema
    #: ActiveRecord::ConnectionAdapters::ConnectionPool
    attr_reader :connection_pool
    #: Object
    attr_accessor :rpc_client_factory
    #: Object
    attr_accessor :workflow_rpc_client_factory
    #: String?
    attr_accessor :local_workflow_rpc_node_id
    #: Hash[String, Object]?
    attr_accessor :local_workflow_rpc_handlers
    #: Object
    attr_accessor :local_worker_id, :local_transient_handler

    class << self
      #: (*Object?, **Object?) ?{ (Object?) -> Object? } -> Store
      def new(*args, **kwargs, &block)
        return super unless equal?(Store)

        raise ArgumentError, "Durababble::Store.new requires connection_pool:" if args.any?

        connection_pool = kwargs[:connection_pool]
        raise ArgumentError, "Durababble::Store.new requires connection_pool:" unless connection_pool

        connection_pool = connection_pool #: as ActiveRecord::ConnectionAdapters::ConnectionPool
        store = from_active_record(connection_pool:, schema: kwargs.fetch(:schema).to_s, owner: kwargs[:owner])
        store #: as Store
      end

      #: (database_url: String, ?schema: String) -> Store
      def connect(database_url:, schema: Durababble.default_schema)
        active_record_class = active_record_class_for(database_url)
        active_record_class = active_record_class #: as untyped
        store = from_active_record(connection_pool: active_record_class.connection_pool, schema:, owner: active_record_class)
        store #: as Store
      rescue StandardError
        remove_active_record_class_const(active_record_class) if active_record_class
        raise
      end

      #: (connection_pool: ActiveRecord::ConnectionAdapters::ConnectionPool, ?schema: String, ?owner: Object?) -> Store
      def from_active_record(connection_pool:, schema: Durababble.default_schema, owner: nil)
        raise ArgumentError, "provide connection_pool:" unless connection_pool.respond_to?(:with_connection)

        adapter = connection_pool.with_connection { |active_connection| active_connection.adapter_name.to_s.downcase }
        if adapter.include?("mysql") || adapter.include?("trilogy")
          return MysqlStore.new(connection_pool, schema:, owner:)
        end

        if adapter.include?("postgres") || adapter.include?("yugabyte")
          return PostgresStore.new(connection_pool, schema:, owner:)
        end

        raise ArgumentError, "unsupported ActiveRecord adapter for Durababble store: #{adapter}"
      end

      private

      #: (String) -> Class
      def active_record_class_for(database_url)
        config = active_record_config_for(database_url)
        connection_name = "StoreConnection#{Process.pid}#{object_id}#{SecureRandom.hex(4)}"
        connection_class = Class.new(ActiveRecord::Base) do
          self.abstract_class = true
          self.connection_class = true
        end
        connection_class.instance_variable_set(GENERATED_CONNECTION_CONST_IVAR, connection_name)
        register_active_record_class_const(connection_name, connection_class)
        begin
          connection_class.establish_connection(config)
        rescue StandardError
          remove_active_record_class_const(connection_class)
          raise
        end
        connection_class
      end

      #: (String, Class) -> void
      def register_active_record_class_const(connection_name, connection_class)
        GENERATED_CONNECTION_MUTEX.synchronize do
          GENERATED_CONNECTION_CLASSES[connection_name] = connection_class
          Durababble.const_set(connection_name, connection_class)
        end
      end

      #: (Object?) -> void
      def remove_active_record_class_const(owner)
        owner = owner #: as untyped
        const_name = owner&.instance_variable_get(GENERATED_CONNECTION_CONST_IVAR)
        return unless const_name.is_a?(String)

        GENERATED_CONNECTION_MUTEX.synchronize do
          next unless GENERATED_CONNECTION_CLASSES[const_name].equal?(owner)
          next unless Durababble.const_defined?(const_name, false)

          Durababble.send(:remove_const, const_name)
          GENERATED_CONNECTION_CLASSES.delete(const_name)
        end
      end

      #: (String) -> Hash[Symbol, Object?]
      def active_record_config_for(database_url)
        uri = URI.parse(database_url)
        username = uri.user
        password = uri.password
        path = uri.path || ""
        adapter = case uri.scheme
        when "mysql", "mysql2", "trilogy"
          "trilogy"
        when "postgres", "postgresql"
          "postgresql"
        else
          uri.scheme
        end
        query_options = uri.query ? URI.decode_www_form(uri.query.to_s).to_h.transform_keys(&:to_sym) : {}
        {
          adapter:,
          host: uri.host,
          port: uri.port,
          username: username && URI.decode_www_form_component(username),
          password: password && URI.decode_www_form_component(password),
          database: path.delete_prefix("/"),
        }.compact.merge(query_options)
      end
    end

    #: (ActiveRecord::ConnectionAdapters::ConnectionPool, schema: String, ?owner: Object?) -> void
    def initialize(connection_pool, schema:, owner: nil)
      raise ArgumentError, "connection_pool must respond to with_connection" unless connection_pool.respond_to?(:with_connection)

      @connection_pool = connection_pool
      @schema = schema
      @owner = owner
      @migrated = false
      @rpc_client_factory = ->(address) { Durababble::Rpc::Client.new(address:) }
      @workflow_rpc_client_factory = ->(address, worker_pool:) { Durababble::Rpc::WorkflowClient.new(address:, worker_pool:) }
      @local_workflow_rpc_node_id = nil
      @local_workflow_rpc_handlers = nil
      @local_worker_id = nil
      @local_transient_handler = nil
    end

    #: () -> void
    def close
      owner = @owner #: as untyped
      begin
        owner&.connection_pool&.disconnect!
      ensure
        self.class.send(:remove_active_record_class_const, owner)
      end
    end

    #: () { (Store) -> Object? } -> Object?
    def with_dedicated_connection(&block)
      block.call(self)
    end

    #: () -> Time
    def current_time = Time.now

    #: (target_kind: String, target_type: String, target_id: String, ?worker_pool: String, ?client_factory: Object?) -> bool
    def deliver_target_message(target_kind:, target_type:, target_id:, worker_pool: "default", client_factory: nil)
      lease = current_target_lease(target_kind:, target_type:, target_id:, worker_pool:)
      return false unless lease

      factory = client_factory || rpc_client_factory
      factory = factory #: as untyped
      expected_worker_id = lease.fetch("worker_id").to_s
      client = factory.call(WorkerIdentity.address_for(expected_worker_id))
      deliver_target_message_with_retry(client, worker_pool: lease.fetch("worker_pool", worker_pool).to_s, target_kind:, target_type:, target_id:, expected_worker_id:)
      true
    rescue Durababble::Rpc::Error, Durababble::WorkflowRpc::Error
      false
    end

    #: (String, ?poll_interval: Numeric, ?timeout: Numeric?) -> Object?
    def wait_for_inbox_message(message_id, poll_interval: 0.05, timeout: 10)
      deadline = timeout && Time.now + timeout
      loop do
        message = inbox_message(message_id)
        raise KeyError, "inbox message not found: #{message_id}" unless message

        case message.fetch("status")
        when "completed"
          return message["result"]
        when "failed", "dead_lettered"
          raise Error, message["error"] || "inbox message #{message_id} failed"
        end
        raise CommandTimeout, "timed out waiting for inbox message #{message_id}" if deadline && Time.now >= deadline

        sleep(poll_interval)
      end
    end

    #: (name: String, input: Object?, ?id: String?, ?worker_pool: String) -> String
    def enqueue_workflow(name:, input:, id: nil, worker_pool: "default")
      raise NotImplementedError
    end

    #: (object_type: String, object_id: String, method_name: Symbol | String, args: Array[Object?], kwargs: Hash[Symbol, Object?], ?message_kind: String, ?idempotency_key: String?, ?max_attempts: Integer?, ?worker_pool: String) -> String
    def enqueue_object_command(object_type:, object_id:, method_name:, args:, kwargs:, message_kind: "ask", idempotency_key: nil, max_attempts: nil, worker_pool: "default")
      enqueue_inbox_message(
        worker_pool:,
        target_kind: "object",
        target_type: object_type,
        target_id: object_id,
        message_kind:,
        method_name: method_name.to_s,
        payload: { "method_name" => method_name.to_s, "args" => args, "kwargs" => kwargs },
        idempotency_key:,
        max_attempts:,
      )
    end

    #: (target_kind: String, target_type: String, target_id: String, ?now: Time, ?worker_pool: String) -> Object?
    def reconcile_target_activation(target_kind:, target_type:, target_id:, now: Time.now, worker_pool: "default")
      transaction { reconcile_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, now:) }
    end

    #: (target_kind: String, target_type: String, target_id: String, ready_at: Time, ?worker_pool: String) -> Object?
    def rearm_target_activation(target_kind:, target_type:, target_id:, ready_at:, worker_pool: "default")
      transaction { set_target_activation_pending_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at:) }
    end

    private

    #: () { (ActiveRecord::ConnectionAdapters::AbstractAdapter) -> Object? } -> Object?
    def with_connection(&block)
      connection_pool.with_connection(&block)
    end

    #: (**Object?) { () -> Object? } -> Object?
    def transaction(**options, &block)
      with_connection do |active_record_connection|
        active_record_connection.transaction(requires_new: true, **options, &block)
      end
    end

    #: (Symbol | String, ?Array[Object?], **Object?) -> untyped
    def execute_store_query(id, params = [], **locals)
      with_connection do
        execute_store_query_sql(store_query_sql(id, **locals), params)
      end
    end

    #: (Symbol | String, **Object?) -> String
    def store_query_sql(id, **locals)
      StoreQueries.sql(qualified_store_query_id(id), self, locals)
    end

    #: (Symbol | String) -> Symbol
    def qualified_store_query_id(id)
      query_id = id.to_sym
      query_id_string = query_id.to_s
      return query_id if query_id_string.start_with?("pg_", "mysql_")

      :"#{store_query_prefix}_#{query_id}"
    end

    #: () -> Symbol
    def store_query_prefix
      raise NotImplementedError
    end

    #: (String, Array[Object?]) -> untyped
    def execute_store_query_sql(sql, params)
      raise NotImplementedError
    end

    #: (String | Symbol) -> String
    def quote_column_name(identifier)
      result = with_connection do |active_record_connection|
        active_record_connection.quote_column_name(identifier.to_s)
      end
      result #: as String
    end

    #: (target_kind: String, target_type: String, target_id: String, worker_pool: String) -> Hash[String, Object?]?
    def current_target_lease(target_kind:, target_type:, target_id:, worker_pool:)
      case target_kind
      when "workflow"
        current_workflow_lease(target_id, worker_pool:)
      when "object"
        current_object_lease(target_type, target_id, worker_pool:)
      end
    end

    #: (String, ?worker_pool: String?) -> Hash[String, Object?]?
    def current_workflow_lease(workflow_id, worker_pool: nil)
      nil
    end

    #: (String, String, ?worker_pool: String) -> Hash[String, Object?]?
    def current_object_lease(object_type, object_id, worker_pool: "default")
      nil
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, ?now: Time) -> Object?
    def reconcile_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, now: Time.now)
      raise NotImplementedError
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, ready_at: Time) -> Object?
    def set_target_activation_pending_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at:)
      raise NotImplementedError
    end

    #: (Object, worker_pool: String, target_kind: String, target_type: String, target_id: String, expected_worker_id: String) -> Object?
    def deliver_target_message_with_retry(client, worker_pool:, target_kind:, target_type:, target_id:, expected_worker_id:)
      client = client #: as untyped
      attempts = 0
      begin
        client.deliver_message(worker_pool:, target_kind:, target_class: target_type, target_id:, expected_worker_id:)
      rescue Durababble::Rpc::Unavailable, Durababble::WorkflowRpc::NodeUnavailable
        attempts += 1
        retry if attempts < 2

        raise
      end
    end

    #: (Hash[String, Object?]) -> bool
    def terminal_for_cancellation?(row)
      WorkflowStatus.terminal?(row)
    end

    #: (untyped) -> String
    def workflow_termination_error(reason)
      reason.to_s.empty? ? "workflow terminated" : reason.to_s
    end

    #: (Array[Hash[String, Object?]], now: Time) -> Array[Hash[String, Object?]]
    def contiguous_claimable_inbox_rows(rows, now:)
      claimable = []
      rows.each do |row|
        break unless inbox_row_claimable?(row, now:)

        claimable << row
      end
      claimable
    end

    #: (Hash[String, Object?], now: Time) -> bool
    def inbox_row_claimable?(row, now:)
      status = row.fetch("status").to_s
      return false if InboxStatus.dead_lettered?(status)

      if InboxStatus.running?(status)
        locked_until = row["locked_until"]
        return false unless locked_until

        return Time.parse(locked_until.to_s) < now
      end

      ready_at = row["ready_at"]
      ready_at.nil? || Time.parse(ready_at.to_s) <= now
    end

    #: (Hash[String, Object?]?) -> bool
    def object_command_message?(row)
      !!(row && (!row.key?("target_kind") || (row.fetch("target_kind") == "object" && ["ask", "tell"].include?(row.fetch("message_kind")))))
    end

    #: (Hash[String, Object?]) -> Hash[String, Object?]
    def object_command_row(row)
      return row unless row.key?("payload")

      payload = row.fetch("payload") #: as untyped
      row.merge(
        "object_type" => row.fetch("target_type"),
        "object_id" => row.fetch("target_id"),
        "method_name" => row["method_name"] || payload.fetch("method_name"),
        "args" => payload.fetch("args"),
        "kwargs" => payload.fetch("kwargs"),
      )
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, message_kind: String, method_name: String?, payload: Object?) -> String
    def inbox_shape_hash(worker_pool:, target_kind:, target_type:, target_id:, message_kind:, method_name:, payload:)
      Digest::SHA256.hexdigest(SERIALIZER.dump({
        "worker_pool" => worker_pool,
        "target_kind" => target_kind,
        "target_type" => target_type,
        "target_id" => target_id,
        "message_kind" => message_kind,
        "method_name" => method_name,
        "payload" => payload,
      }))
    end

    #: (String?, worker_pool: String, target_kind: String, target_type: String, target_id: String) -> String?
    def inbox_idempotency_hash(idempotency_key, worker_pool:, target_kind:, target_type:, target_id:)
      return unless idempotency_key

      Digest::SHA256.hexdigest(SERIALIZER.dump({
        "worker_pool" => worker_pool,
        "target_kind" => target_kind,
        "target_type" => target_type,
        "target_id" => target_id,
        "idempotency_key" => idempotency_key,
      }))
    end

    #: (Hash[String, Object?]) -> Hash[String, Object?]
    def decode_row(row)
      row.each_with_object({}) do |(column, value), decoded|
        decoded[column] = SERIALIZED_COLUMNS.include?(column) ? load_serialized(value) : value
      end
    end

    #: (Hash[String, Object?]) -> Hash[String, Object?]
    def with_command_id(row)
      row["command_id"] = row["position"] if row.key?("position") && !row.key?("command_id")
      row
    end

    #: (Hash[String, Object?], String) -> String
    def row_string(row, key)
      row.fetch(key).to_s
    end

    #: (Object?, ?surface: Symbol?, ?context: String?) -> String
    def dump_serialized_bytes(value, surface: nil, context: nil)
      serialized = SERIALIZER.dump(value)
      Durababble.enforce_payload_limit!(surface:, bytesize: serialized.bytesize, context:) if surface
      serialized
    end

    #: (name: String, input: Object?) -> Object?
    def dump_workflow_input(name:, input:)
      dump_serialized(input, surface: :workflow_input, context: "workflow #{name}")
    end

    #: (workflow_id: String, result: Object?, ?context: String) -> Object?
    def dump_workflow_result(workflow_id:, result:, context: "result")
      dump_serialized(result, surface: :workflow_result, context: "workflow #{workflow_id} #{context}")
    end

    #: (workflow_id: String, command_id: Integer, result: Object?) -> Object?
    def dump_step_output(workflow_id:, command_id:, result:)
      dump_serialized(result, surface: :step_output, context: "workflow #{workflow_id} command #{command_id}")
    end

    #: (object_type: String, object_id: String, state: Object?) -> Object?
    def dump_object_state(object_type:, object_id:, state:)
      dump_serialized(state, surface: :object_state, context: "#{object_type}/#{object_id}")
    end

    #: (target_kind: String, target_type: String, target_id: String, message_kind: String, payload: Object?) -> Object?
    def dump_inbox_payload(target_kind:, target_type:, target_id:, message_kind:, payload:)
      dump_serialized(payload, surface: :inbox_payload, context: "#{target_kind} #{target_type}/#{target_id} #{message_kind}")
    end

    #: (message_id: String, result: Object?) -> Object?
    def dump_inbox_result(message_id:, result:)
      dump_serialized(result, surface: :inbox_payload, context: "inbox message #{message_id} result")
    end

    #: (Hash[String, Object?]) -> String
    def row_worker_pool(row)
      row.fetch("worker_pool", "default").to_s
    end

    #: (Hash[String, Object?], now: Time) -> Object?
    def target_activation_ready_at_for(row, now:)
      return now if inbox_row_claimable?(row, now:)

      row["ready_at"] || row["locked_until"] || now
    end

    #: (workflow_id: String, command_id: Integer, status: String, result: Object?, error: String?) -> Object?
    def update_latest_attempt(workflow_id:, command_id:, status:, result:, error:)
      update_latest_attempt_serialized(workflow_id:, command_id:, status:, serialized_result: dump_serialized(result), error:)
    end

    #: (Hash[String, Object?]?, String) -> nil
    def observe_claim_latency(row, queue)
      return unless row&.key?("created_at")

      created_at = row.fetch("created_at")
      created_at = created_at #: as untyped
      created_at = Time.parse(created_at.to_s) unless created_at.respond_to?(:to_time)
      created_time = created_at.respond_to?(:to_time) ? created_at.to_time : created_at
      Observability.record(
        "durababble.queue.claim_latency",
        [((Time.now - created_time) * 1000.0), 0].max,
        "durababble.queue.name" => queue,
        "durababble.store.backend" => Observability.store_backend(self),
      )
      nil
    rescue StandardError => e
      log_swallowed_metric_error("durababble.queue.claim_latency", e)
    end

    #: (Hash[String, Object?]) -> nil
    def record_wait_latency(wait)
      created_at = wait.fetch("created_at")
      completed_at = wait["completed_at"] || Time.now
      created_at = created_at #: as untyped
      completed_at = completed_at #: as untyped
      created_at = Time.parse(created_at.to_s) unless created_at.respond_to?(:to_time)
      completed_at = Time.parse(completed_at.to_s) unless completed_at.respond_to?(:to_time)
      Observability.record(
        "durababble.wait.latency",
        [((completed_at.to_time - created_at.to_time) * 1000.0), 0].max,
        "durababble.wait.kind" => wait["kind"],
      )
      nil
    rescue StandardError => e
      log_swallowed_metric_error("durababble.wait.latency", e)
    end

    # Latency metrics are best-effort: a malformed timestamp must not break the
    # claim/wait path that produced it. But a silent rescue hides a real bug, so
    # record the swallow on a counter and through the logger before returning.
    #: (String, StandardError) -> nil
    def log_swallowed_metric_error(metric, error)
      Observability.count(
        "durababble.metrics.record_errors",
        "durababble.metric.name" => metric,
        "error.type" => error.class.name,
      )
      Durababble.logger&.warn(
        "Durababble failed to record #{metric}: #{error.class}: #{error.message}",
      )
      nil
    end
  end
end

require_relative "store/postgres_migrations"
require_relative "store/mysql_migrations"
require_relative "store/sql_common"
require_relative "store/postgres"
require_relative "store/mysql"
