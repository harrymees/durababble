# typed: true
# frozen_string_literal: true

require "sqlite3"
require_relative "../store" unless defined?(Durababble::MysqlStore)
require_relative "sqlite_migrations"

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
  #     translating the rendered SQL to SQLite (see #translate_to_sqlite). Six
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
          new(connection_class.connection_pool, schema:, owner: connection_class) #: as SqliteStore
        rescue StandardError
          Store.send(:remove_active_record_class_const, connection_class)
          raise
        end
      end
    end

    #: (ActiveRecord::ConnectionAdapters::ConnectionPool, schema: String, ?owner: Object?) -> void
    def initialize(connection_pool, schema:, owner: nil)
      @store_query_sql_cache = {}
      @sqlite_translation_cache = {}
      super
      # An in-memory SQLite database lives inside a single connection — a second
      # connection from the pool is a different, empty database. Lease one
      # connection for the store's lifetime and route every #with_connection
      # through it so the dura_now() UDF, PRAGMAs, and all data stay on the one
      # database. This mirrors the test-only, single-serialized-connection model.
      @connection = connection_pool.lease_connection
      install_sqlite_runtime!
    end

    private

    # The store binds to one leased connection (see #initialize); always yield it
    # so inherited orchestration runs against the database carrying the UDF.
    #: [T] () { (Object) -> T } -> T
    def with_connection(&block)
      block.call(@connection)
    end

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
      cache_key = store_query_sql_cache_key(id, locals)
      cached = @store_query_sql_cache[cache_key]
      return cached if cached

      resolved = qualified_store_query_id(id)
      sql = StoreQueries.sql(resolved, self, locals)
      sql = translate_to_sqlite(sql) unless resolved.to_s.start_with?("sqlite_")

      @store_query_sql_cache[cache_key] = sql.freeze
    end

    # Rewrites MySQL-dialect SQL into the SQLite subset this store speaks. The
    # five ON DUPLICATE KEY upserts are handled by explicit :sqlite_* queries
    # instead (they need a conflict target and VALUES()/IF() rewrites), so this
    # only has to cover the mechanical substitutions.
    #: (String) -> String
    def translate_to_sqlite(sql)
      source_sql = sql
      cached = @sqlite_translation_cache[source_sql]
      return cached if cached

      sql = sql.gsub(/\s*FORCE INDEX \([^)]*\)/i, "")
      sql = sql.gsub(/INSERT IGNORE INTO/i, "INSERT OR IGNORE INTO")
      # The integer clock counts microseconds, so a SECOND interval must be
      # scaled by #seconds_scale. Without this, a 30s lease would expire 30µs
      # later. The deterministic store runs a 1-tick == 1-second clock and
      # overrides #seconds_scale to 1.
      scale = seconds_scale
      sql = sql.gsub(/DATE_ADD\(\s*NOW\(6\)\s*,\s*INTERVAL\s+(.+?)\s+SECOND\)/i) { "(dura_now() + (#{::Regexp.last_match(1)}) * #{scale})" }
      sql = sql.gsub(/NOW\(6\)/i, "dura_now()")
      sql = sql.gsub(/\bLEAST\(/i, "MIN(")
      sql = sql.gsub(/\bGREATEST\(/i, "MAX(")
      sql = sql.gsub(/\s*FOR UPDATE(?:\s+OF\s+\w+)?(?:\s+SKIP\s+LOCKED)?/i, "")

      @sqlite_translation_cache[source_sql] = sql.freeze
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

    #: (SqliteStore) -> SqliteStore
    def load_migrated_template!(template)
      backup = SQLite3::Backup.new(raw_sqlite_connection, "main", template.send(:raw_sqlite_connection), "main")
      backup.step(-1)
      @migrated = true
      self
    ensure
      backup&.finish
    end

    #: (Symbol | String, Hash[Symbol, Object?]) -> Object
    def store_query_sql_cache_key(id, locals)
      return id.to_sym if locals.empty?

      [
        id.to_sym,
        locals.sort_by { |key, _| key.to_s }.map { |key, value| [key, value] }.freeze,
      ].freeze
    end

    #: () -> SQLite3::Database
    def raw_sqlite_connection
      @connection.raw_connection
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
        # SQLite's changes() counts the most recent INSERT/UPDATE/DELETE and is
        # NOT reset by a SELECT, so reading db.changes after a query returns a
        # stale count from an earlier write. A statement that yields result
        # columns is a read (this store sets supports_returning? = false, so DML
        # never returns columns); report 0 affected rows for it and only trust
        # db.changes for the column-less DML statements.
        affected_rows = columns.empty? ? db.changes : 0
        ActiveRecord::Result.new(columns, rows, affected_rows: affected_rows)
      rescue SQLite3::ConstraintException => e
        # The raw sqlite3 driver bypasses ActiveRecord, so a unique violation
        # surfaces as SQLite3::ConstraintException rather than the
        # ActiveRecord::RecordNotUnique the inherited orchestration rescues
        # (e.g. insert_workflow -> WorkflowAlreadyExists). Translate so the
        # MySQL-style error handling applies unchanged.
        raise ActiveRecord::RecordNotUnique, e.message if e.message.include?("UNIQUE constraint failed")

        raise
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

    # How many integer clock units make up one SECOND interval. The base store's
    # clock counts microseconds, so a NOW(6) + INTERVAL n SECOND must scale n by
    # 1_000_000. The deterministic subclass runs a 1-tick == 1-second clock and
    # overrides this to 1.
    #: () -> Integer
    def seconds_scale
      1_000_000
    end

    # Integer-clock variants of the base comparison helpers (the base assumes
    # parseable Time strings; here both sides are integers).
    #: (Hash[String, Object?], now: Object?) -> bool
    def inbox_row_claimable?(row, now:)
      status = row.fetch("status").to_s
      return false if InboxStatus.dead_lettered?(status)

      now_tick = timestamp_or_nil(now).to_i
      if InboxStatus.running?(status)
        locked_until = row["locked_until"]
        return false unless locked_until

        locked_until = locked_until #: as untyped
        return locked_until.to_i < now_tick
      end

      ready_at = row["ready_at"]
      return true if ready_at.nil?

      ready_at = ready_at #: as untyped
      ready_at.to_i <= now_tick
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
