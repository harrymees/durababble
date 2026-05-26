# typed: true
# frozen_string_literal: true

require "sqlite3"

module Durababble
  # A real-SQL SQLite store. Test-only: it is NOT registered in
  # Store.from_active_record and must be constructed directly (see
  # SqliteStore.build_in_memory) so production never routes here.
  #
  # It exists to give Deterministic Simulation Testing a store that runs the
  # *same* SQL orchestration as the production adapters (claim/lease/fence/
  # outbox/inbox state machines) rather than a hand-written reimplementation.
  # SqliteStore reuses MysqlStore's orchestration wholesale and differs only in
  # dialect:
  #
  #   * Queries are resolved by falling back to the :mysql_* sibling and
  #     translating the rendered SQL to SQLite (see #translate_to_sqlite). Five
  #     upserts that cannot be regex-translated have explicit :sqlite_* variants.
  #   * Timestamps are an integer clock. dura_now() — a per-connection UDF —
  #     returns #current_time, so DEFAULTs and translated NOW(6) references all
  #     read the same monotonic integer. The deterministic subclass overrides
  #     #current_time to return virtual time, making created_at/lease math
  #     deterministic without touching any SQL.
  #   * supports_returning?/supports_skip_locked? are false (the MySQL-style
  #     select-then-update path is reused; FOR UPDATE / SKIP LOCKED are stripped,
  #     which is sound under a single serialized connection).
  class SqliteStore < MysqlStore
    include SqliteMigrations

    class << self
      #: (?schema: String) -> SqliteStore
      def build_in_memory(schema: "durababble")
        connection_name = "SqliteStoreConnection#{Process.pid}#{object_id}#{SecureRandom.hex(4)}"
        connection_class = Class.new(ActiveRecord::Base) do
          self.abstract_class = true
          self.connection_class = true
        end
        connection_class.instance_variable_set(Store::GENERATED_CONNECTION_CONST_IVAR, connection_name)
        Store::GENERATED_CONNECTION_CLASSES[connection_name] = connection_class
        Durababble.const_set(connection_name, connection_class)
        begin
          connection_class.establish_connection(adapter: "sqlite3", database: ":memory:", pool: 1)
          new(connection_class.connection_pool.lease_connection, schema:, owner: connection_class)
        rescue StandardError
          Store.send(:remove_active_record_class_const, connection_class)
          raise
        end
      end
    end

    #: (Object, schema: String, ?owner: Object?) -> void
    def initialize(connection, schema:, owner: nil)
      super
      install_sqlite_runtime!
    end

    private

    #: () -> void
    def install_sqlite_runtime!
      store = self
      raw = @connection.raw_connection
      raw.create_function("dura_now", 0) { |func| func.result = store.current_time }
      @connection.exec_query("PRAGMA foreign_keys = ON")
    end

    public

    # Integer clock shared by dura_now() and every timestamp bind. Microsecond
    # epoch keeps created_at strictly increasing and lexicographically sortable
    # (the inherited candidate ordering uses created_at.to_s). The deterministic
    # subclass overrides this to return virtual ticks.
    #: () -> Integer
    def current_time
      (Time.now.to_r * 1_000_000).to_i
    end

    private

    #: () -> Symbol
    def store_query_prefix
      :sqlite
    end

    #: (Symbol | String) -> Symbol
    def qualified_store_query_id(id)
      query_id = id.to_sym
      query_id_string = query_id.to_s
      return query_id if query_id_string.start_with?("pg_", "mysql_", "sqlite_")

      sqlite_id = :"sqlite_#{query_id}"
      return sqlite_id if StoreQueries.defined?(sqlite_id)

      :"mysql_#{query_id}"
    end

    #: (Symbol | String, **Object?) -> String
    def store_query_sql(id, **locals)
      resolved = qualified_store_query_id(id)
      sql = StoreQueries.sql(resolved, self, locals)
      return sql if resolved.to_s.start_with?("sqlite_")

      translate_to_sqlite(sql)
    end

    # Rewrites MySQL-dialect SQL into the SQLite subset this store speaks. The
    # five ON DUPLICATE KEY upserts are handled by explicit :sqlite_* queries
    # instead (they need a conflict target and VALUES()/IF() rewrites), so this
    # only has to cover the mechanical substitutions.
    #: (String) -> String
    def translate_to_sqlite(sql)
      sql = sql.gsub(/\s*FORCE INDEX \([^)]*\)/i, "")
      sql = sql.gsub(/INSERT IGNORE INTO/i, "INSERT OR IGNORE INTO")
      # The integer clock counts microseconds, so a SECOND interval must be
      # scaled by 1_000_000. Without this, a 30s lease would expire 30µs later.
      sql = sql.gsub(/DATE_ADD\(\s*NOW\(6\)\s*,\s*INTERVAL\s+(.+?)\s+SECOND\)/i) { "(dura_now() + (#{::Regexp.last_match(1)}) * 1000000)" }
      sql = sql.gsub(/NOW\(6\)/i, "dura_now()")
      sql = sql.gsub(/\bLEAST\(/i, "MIN(")
      sql = sql.gsub(/\bGREATEST\(/i, "MAX(")
      sql.gsub(/\s*FOR UPDATE(?:\s+OF\s+\w+)?(?:\s+SKIP\s+LOCKED)?/i, "")
    end

    #: (String) -> untyped
    def execute(sql)
      @connection.exec_query(sql)
    end

    # A couple of orchestration methods build MySQL-dialect SQL inline (FOR
    # UPDATE, NOW(6)) and call execute_params directly, bypassing the query
    # registry and #store_query_sql. Translate those too.
    #: (String, Array[Object?]) -> untyped
    def execute_params(sql, params)
      execute_store_query_sql(translate_to_sqlite(sql), params)
    end

    #: (**Object?) { () -> Object? } -> Object?
    def transaction(**_options, &block)
      # SQLite under a single serialized connection needs no isolation level or
      # deadlock-retry handling; nested transactions become SAVEPOINTs.
      @connection.transaction(requires_new: true, &block)
    end

    #: (String, Array[Object?]) -> untyped
    def execute_store_query_sql(sql, params)
      db = @connection.raw_connection
      statement = db.prepare(sql)
      begin
        result_set = statement.execute(*bind_params(params))
        columns = result_set.columns
        rows = result_set.to_a
        rows = rows.map { |row| row.is_a?(Hash) ? columns.map { |column| row[column] } : row } unless columns.empty?
        ActiveRecord::Result.new(columns, rows, affected_rows: db.changes)
      ensure
        statement.close
      end
    end

    #: (Array[Object?]) -> Array[Object?]
    def bind_params(params)
      params.map do |value|
        case value
        when Time
          timestamp_or_nil(value)
        when String
          value.encoding == Encoding::ASCII_8BIT ? SQLite3::Blob.new(value) : value
        else
          value
        end
      end
    end

    #: (Object?) -> Integer?
    def timestamp_or_nil(time)
      return if time.nil?
      return time if time.is_a?(Integer)

      time = time #: as untyped
      (time.to_r * 1_000_000).to_i
    end

    # Integer-clock variants of the base comparison helpers (the base assumes
    # parseable Time strings; here both sides are integers).
    #: (Hash[String, Object?], now: Object?) -> bool
    def inbox_row_claimable?(row, now:)
      status = row.fetch("status").to_s
      return false if InboxStatus.dead_lettered?(status)

      now_tick = timestamp_or_nil(now)
      if InboxStatus.running?(status)
        locked_until = row["locked_until"]
        return false unless locked_until

        return locked_until.to_i < now_tick
      end

      ready_at = row["ready_at"]
      ready_at.nil? || ready_at.to_i <= now_tick
    end

    #: (Hash[String, Object?], now: Object?) -> Object?
    def target_activation_ready_at_for(row, now:)
      return now if inbox_row_claimable?(row, now:)

      row["ready_at"] || row["locked_until"] || now
    end

    # Observability latency helpers do real-time math on created_at; meaningless
    # for an integer test clock, so make them no-ops.
    #: (Hash[String, Object?]?, String) -> nil
    def observe_claim_latency(_row, _queue)
      nil
    end

    #: (Hash[String, Object?]) -> nil
    def record_wait_latency(_wait)
      nil
    end
  end
end
