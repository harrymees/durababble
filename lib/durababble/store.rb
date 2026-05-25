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
    VALUE_TYPE = ActiveModel::Type::Value.new

    Result = Struct.new(:rows, :affected_rows) do
      #: () -> untyped
      def first = rows.first
      #: () { (?) -> untyped } -> untyped
      def map(&block) = rows.map(&block)
      #: () -> untyped
      def to_a = rows.to_a
      #: () { (?) -> untyped } -> untyped
      def each(&block) = rows.each(&block)
      #: () -> untyped
      def cmd_tuples = affected_rows
      #: () -> untyped
      def length = rows.length
      alias_method :size, :length
    end

    #: untyped
    attr_reader :schema, :connection
    #: untyped
    attr_accessor :rpc_client_factory

    class << self
      #: (*untyped, **untyped) { (?) -> untyped } -> untyped
      def new(*args, **kwargs, &block)
        return super unless equal?(Store)

        connection = args.first || kwargs[:connection]
        raise ArgumentError, "Durababble::Store.new requires a connection" unless connection

        from_active_record(connection:, schema: kwargs.fetch(:schema), owner: kwargs[:owner])
      end

      #: (database_url: untyped, ?schema: untyped) -> untyped
      def connect(database_url:, schema: Durababble.default_schema)
        active_record_class = active_record_class_for(database_url)
        from_active_record(connection_pool: active_record_class.connection_pool, schema:, owner: active_record_class)
      end

      #: (?connection: untyped, ?connection_pool: untyped, ?schema: untyped, ?owner: untyped) -> untyped
      def from_active_record(connection: nil, connection_pool: nil, schema: Durababble.default_schema, owner: nil)
        connection ||= connection_pool&.lease_connection
        raise ArgumentError, "provide connection: or connection_pool:" unless connection

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

      #: (untyped) -> untyped
      def active_record_class_for(database_url)
        config = active_record_config_for(database_url)
        connection_name = "StoreConnection#{Process.pid}#{object_id}#{SecureRandom.hex(4)}"
        connection_class = Class.new(ActiveRecord::Base) do
          self.abstract_class = true
          self.connection_class = true
        end
        Durababble.const_set(connection_name, connection_class)
        connection_class.establish_connection(config)
        connection_class
      end

      #: (untyped) -> untyped
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

    #: (untyped, schema: untyped, ?owner: untyped) -> void
    def initialize(connection, schema:, owner: nil)
      @connection = connection
      @schema = schema
      @owner = owner
      @migrated = false
      @rpc_client_factory = ->(address) { Durababble::Rpc::Client.new(address:) }
    end

    #: () -> void
    def close
      @owner&.connection_pool&.disconnect!
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ?worker_pool: untyped, ?client_factory: untyped) -> bool
    def deliver_target_message(target_kind:, target_type:, target_id:, worker_pool: "default", client_factory: nil)
      lease = current_target_lease(target_kind:, target_id:)
      return false unless lease

      factory = client_factory || rpc_client_factory
      client = factory.call(lease.fetch("worker_id"))
      deliver_target_message_with_retry(client, worker_pool:, target_kind:, target_type:, target_id:)
      true
    rescue Durababble::Rpc::Error, Durababble::WorkflowRpc::Error
      false
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ?now: untyped) -> untyped
    def reconcile_target_activation(target_kind:, target_type:, target_id:, now: Time.now)
      transaction { reconcile_target_activation_without_transaction(target_kind:, target_type:, target_id:, now:) }
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ready_at: untyped) -> untyped
    def rearm_target_activation(target_kind:, target_type:, target_id:, ready_at:)
      transaction { set_target_activation_pending_without_transaction(target_kind:, target_type:, target_id:, ready_at:) }
    end

    private

    #: () { (?) -> untyped } -> untyped
    def transaction(&block)
      @connection.transaction(requires_new: true, &block)
    end

    #: (target_kind: untyped, target_id: untyped) -> untyped
    def current_target_lease(target_kind:, target_id:)
      case target_kind
      when "workflow"
        current_workflow_lease(target_id)
      end
    end

    #: (untyped, worker_pool: untyped, target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def deliver_target_message_with_retry(client, worker_pool:, target_kind:, target_type:, target_id:)
      attempts = 0
      begin
        client.deliver_message(worker_pool:, target_kind:, target_class: target_type, target_id:)
      rescue Durababble::Rpc::Unavailable, Durababble::WorkflowRpc::NodeUnavailable
        attempts += 1
        retry if attempts < 2

        raise
      end
    end

    #: (untyped) -> untyped
    def affected_rows(result)
      return result if result.is_a?(Integer)

      result.affected_rows || result.to_a.length
    end

    #: (untyped) -> untyped
    def bind_attributes(params)
      params.each_with_index.map do |value, index|
        ActiveRecord::Relation::QueryAttribute.new("durababble_#{index}", value, VALUE_TYPE)
      end
    end

    #: (untyped) -> bool
    def terminal_for_cancellation?(row)
      return true if ["completed", "canceled"].include?(row.fetch("status"))

      row.fetch("status") == "failed" && row["next_run_at"].nil?
    end

    #: (untyped, now: untyped) -> untyped
    def contiguous_claimable_inbox_rows(rows, now:)
      claimable = []
      rows.each do |row|
        break unless inbox_row_claimable?(row, now:)

        claimable << row
      end
      claimable
    end

    #: (untyped, now: untyped) -> bool
    def inbox_row_claimable?(row, now:)
      status = row.fetch("status")
      return false if status == "dead_lettered"

      if status == "running"
        locked_until = row["locked_until"]
        return false unless locked_until

        return Time.parse(locked_until.to_s) < now
      end

      ready_at = row["ready_at"]
      ready_at.nil? || Time.parse(ready_at.to_s) <= now
    end

    #: (untyped) -> bool
    def object_command_message?(row)
      row && (!row.key?("target_kind") || (row.fetch("target_kind") == "object" && row.fetch("message_kind") == "ask"))
    end

    #: (untyped) -> untyped
    def object_command_row(row)
      return row unless row.key?("payload")

      payload = row.fetch("payload")
      row.merge(
        "object_type" => row.fetch("target_type"),
        "object_id" => row.fetch("target_id"),
        "method_name" => row["method_name"] || payload.fetch("method_name"),
        "args" => payload.fetch("args"),
        "kwargs" => payload.fetch("kwargs"),
      )
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, message_kind: untyped, method_name: untyped, payload: untyped) -> String
    def inbox_shape_hash(target_kind:, target_type:, target_id:, message_kind:, method_name:, payload:)
      Digest::SHA256.hexdigest(SERIALIZER.dump({
        "target_kind" => target_kind,
        "target_type" => target_type,
        "target_id" => target_id,
        "message_kind" => message_kind,
        "method_name" => method_name,
        "payload" => payload,
      }))
    end

    #: (untyped) -> untyped
    def decode_inbox_row(row)
      decode_row(row)
    end

    #: (untyped) -> untyped
    def with_command_id(row)
      row["command_id"] = row["position"] if row.key?("position") && !row.key?("command_id")
      row
    end

    #: (workflow_id: untyped, command_id: untyped, status: untyped, result: untyped, error: untyped) -> untyped
    def update_latest_attempt(workflow_id:, command_id:, status:, result:, error:)
      update_latest_attempt_serialized(workflow_id:, command_id:, status:, serialized_result: dump_serialized(result), error:)
    end
  end
end

require_relative "store/postgres_migrations"
require_relative "store/mysql_migrations"
require_relative "store/postgres"
require_relative "store/mysql"
