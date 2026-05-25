# typed: true
# frozen_string_literal: true

require "active_record"
require "digest"
require "json"
require "paquito"
require "securerandom"
require "time"
require "uri"

module Durababble
  class Store
    SERIALIZED_COLUMNS = ["input", "result", "payload", "context", "heartbeat_cursor", "state", "args", "kwargs"].freeze
    SERIALIZER = Paquito::SingleBytePrefixVersion.new(1, 1 => Marshal)
    NO_OBJECT_STATE = Object.new.freeze
    GENERATED_CONNECTION_CONST_IVAR = :@durababble_store_connection_const_name
    GENERATED_CONNECTION_CLASSES = {}

    class PooledConnections
      #: (Store) -> void
      def initialize(template)
        @template = template
      end

      #: () -> void
      def close
      end

      #: () -> bool
      def pooled_connections?
        true
      end

      #: (Symbol, *untyped, **untyped) ?{ (*untyped) -> untyped } -> untyped
      def method_missing(name, *args, **kwargs, &block)
        @template.send(:with_connection_store) do |store|
          store.public_send(name, *args, **kwargs, &block)
        end
      end

      #: (Symbol, ?bool) -> bool
      def respond_to_missing?(name, include_private = false)
        @template.respond_to?(name, include_private)
      end
    end

    #: String
    attr_reader :schema
    #: Object
    attr_reader :connection
    #: Object
    attr_accessor :rpc_client_factory

    class << self
      #: (*Object?, **Object?) ?{ (Object?) -> Object? } -> Store
      def new(*args, **kwargs, &block)
        return super unless equal?(Store)

        connection = args.first || kwargs[:connection]
        raise ArgumentError, "Durababble::Store.new requires a connection" unless connection

        from_active_record(connection:, schema: kwargs.fetch(:schema).to_s, owner: kwargs[:owner])
      end

      #: (database_url: String, ?schema: String) -> Store
      def connect(database_url:, schema: Durababble.default_schema)
        active_record_class = active_record_class_for(database_url)
        active_record_class = active_record_class #: as untyped
        from_active_record(connection_pool: active_record_class.connection_pool, schema:, owner: active_record_class)
      rescue StandardError
        remove_active_record_class_const(active_record_class) if active_record_class
        raise
      end

      #: (?connection: Object?, ?connection_pool: Object?, ?schema: String, ?owner: Object?) -> Store
      def from_active_record(connection: nil, connection_pool: nil, schema: Durababble.default_schema, owner: nil)
        connection_pool = connection_pool #: as untyped
        connection ||= connection_pool&.lease_connection
        raise ArgumentError, "provide connection: or connection_pool:" unless connection

        connection = connection #: as untyped
        adapter = connection.adapter_name.to_s.downcase
        if adapter.include?("mysql") || adapter.include?("trilogy")
          return MysqlStore.new(connection, schema:, owner:)
        end

        if adapter.include?("postgres") || adapter.include?("yugabyte")
          return PostgresStore.new(connection, schema:, owner:)
        end

        raise ArgumentError, "unsupported ActiveRecord adapter for Durababble store: #{connection.adapter_name}"
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
        GENERATED_CONNECTION_CLASSES[connection_name] = connection_class
        Durababble.const_set(connection_name, connection_class)
        begin
          connection_class.establish_connection(config)
        rescue StandardError
          remove_active_record_class_const(connection_class)
          raise
        end
        connection_class
      end

      #: (Object?) -> void
      def remove_active_record_class_const(owner)
        owner = owner #: as untyped
        const_name = owner&.instance_variable_get(GENERATED_CONNECTION_CONST_IVAR)
        return unless const_name.is_a?(String)
        return unless GENERATED_CONNECTION_CLASSES[const_name].equal?(owner)
        return unless Durababble.const_defined?(const_name, false)

        Durababble.send(:remove_const, const_name)
        GENERATED_CONNECTION_CLASSES.delete(const_name)
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
        {
          adapter:,
          host: uri.host,
          port: uri.port,
          username: username && URI.decode_www_form_component(username),
          password: password && URI.decode_www_form_component(password),
          database: path.delete_prefix("/"),
        }.compact
      end
    end

    #: (Object, schema: String, ?owner: Object?) -> void
    def initialize(connection, schema:, owner: nil)
      @connection = connection #: as untyped
      @schema = schema
      @owner = owner
      @migrated = false
      @rpc_client_factory = ->(address) { Durababble::Rpc::Client.new(address:) }
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

    #: () -> Time
    def current_time = Time.now

    #: () -> PooledConnections
    def pooled_connections
      PooledConnections.new(self)
    end

    #: (target_kind: String, target_type: String, target_id: String, ?worker_pool: String, ?client_factory: Object?) -> bool
    def deliver_target_message(target_kind:, target_type:, target_id:, worker_pool: "default", client_factory: nil)
      lease = current_target_lease(target_kind:, target_type:, target_id:, worker_pool:)
      return false unless lease

      factory = client_factory || rpc_client_factory
      factory = factory #: as untyped
      client = factory.call(lease.fetch("worker_id"))
      deliver_target_message_with_retry(client, worker_pool: lease.fetch("worker_pool", worker_pool).to_s, target_kind:, target_type:, target_id:)
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

    #: (name: String, input: Object?, ?worker_pool: String) -> String
    def enqueue_workflow(name:, input:, worker_pool: "default")
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

    #: () -> Object?
    def active_record_connection_pool
      owner = @owner #: as untyped
      return owner.connection_pool if owner&.respond_to?(:connection_pool)
      return @connection.pool if @connection.respond_to?(:pool)

      nil
    end

    #: () { (?) -> Object? } -> Object?
    def with_connection_store(&block)
      pool = active_record_connection_pool
      raise Error, "Durababble store cannot checkout an isolated connection without an ActiveRecord connection pool" unless pool

      pool = pool #: as untyped
      pool.with_connection do |connection|
        block.call(self.class.from_active_record(connection:, schema: @schema))
      end
    end

    #: () { () -> Object? } -> Object?
    def transaction(&block)
      @connection.transaction(requires_new: true, &block)
    end

    #: (Symbol | String, ?Array[Object?], **Object?) -> untyped
    def execute_store_query(id, params = [], **locals)
      execute_store_query_sql(store_query_sql(id, **locals), params)
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

    #: (Object, worker_pool: String, target_kind: String, target_type: String, target_id: String) -> Object?
    def deliver_target_message_with_retry(client, worker_pool:, target_kind:, target_type:, target_id:)
      client = client #: as untyped
      attempts = 0
      begin
        client.deliver_message(worker_pool:, target_kind:, target_class: target_type, target_id:)
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
    rescue StandardError
      nil
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
    rescue StandardError
      nil
    end
  end
end

require_relative "store/postgres_migrations"
require_relative "store/mysql_migrations"
require_relative "store/sql_common"
require_relative "store/postgres"
require_relative "store/mysql"
