# typed: true
# frozen_string_literal: true

module Durababble
  module MysqlMigrations
    #: () -> untyped
    def migrate!
      return self if @migrated

      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("workflows")} (
          id VARCHAR(191) PRIMARY KEY,
          name VARCHAR(191) NOT NULL,
          worker_pool VARCHAR(191) NOT NULL DEFAULT 'default',
          status VARCHAR(32) NOT NULL,
          input LONGBLOB NOT NULL,
          result LONGBLOB,
          error TEXT,
          locked_by VARCHAR(191),
          locked_until DATETIME(6),
          next_run_at DATETIME(6),
          cancel_reason TEXT,
          cancel_requested_at DATETIME(6),
          cancel_delivered_at DATETIME(6),
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          updated_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          INDEX #{quote_column_name(index_name("workflows", "queue"))} (worker_pool, status, created_at),
          INDEX #{quote_column_name(index_name("workflows", "runnable_due"))} (worker_pool, status, next_run_at, created_at),
          INDEX #{quote_column_name(index_name("workflows", "expired_lease"))} (worker_pool, status, locked_until, created_at),
          INDEX #{quote_column_name(index_name("workflows", "worker_lease"))} (status, locked_by)
        )
      SQL
      add_column_if_missing("workflows", "worker_pool", "VARCHAR(191) NOT NULL DEFAULT 'default'")
      add_column_if_missing("workflows", "cancel_reason", "TEXT")
      add_column_if_missing("workflows", "cancel_requested_at", "DATETIME(6)")
      add_column_if_missing("workflows", "cancel_delivered_at", "DATETIME(6)")
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("workflow_history")} (
          workflow_id VARCHAR(191) NOT NULL,
          event_index INT NOT NULL,
          kind VARCHAR(64) NOT NULL,
          command_id INT,
          name VARCHAR(191),
          attempt_id VARCHAR(191),
          payload LONGBLOB,
          error TEXT,
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          PRIMARY KEY (workflow_id, event_index),
          INDEX #{quote_column_name(index_name("workflow_history", "command"))} (workflow_id, command_id, event_index),
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("steps")} (
          workflow_id VARCHAR(191) NOT NULL,
          position INT NOT NULL,
          name VARCHAR(191) NOT NULL,
          status VARCHAR(32) NOT NULL,
          result LONGBLOB,
          error TEXT,
          heartbeat_cursor LONGBLOB,
          started_at DATETIME(6),
          completed_at DATETIME(6),
          updated_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          PRIMARY KEY (workflow_id, position),
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("step_attempts")} (
          id VARCHAR(191) PRIMARY KEY,
          workflow_id VARCHAR(191) NOT NULL,
          position INT NOT NULL,
          name VARCHAR(191) NOT NULL,
          status VARCHAR(32) NOT NULL,
          result LONGBLOB,
          error TEXT,
          heartbeat_cursor LONGBLOB,
          started_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          completed_at DATETIME(6),
          INDEX #{quote_column_name(index_name("step_attempts", "workflow_started_position"))} (workflow_id, started_at, position),
          INDEX #{quote_column_name(index_name("step_attempts", "workflow_position_status_started"))} (workflow_id, position, status, started_at),
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("fences")} (
          workflow_id VARCHAR(191) NOT NULL,
          `key` VARCHAR(191) NOT NULL,
          status VARCHAR(32) NOT NULL DEFAULT 'completed',
          result LONGBLOB,
          error TEXT,
          locked_by VARCHAR(191),
          locked_until DATETIME(6),
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          completed_at DATETIME(6),
          PRIMARY KEY (workflow_id, `key`),
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("outbox")} (
          id VARCHAR(191) PRIMARY KEY,
          workflow_id VARCHAR(191) NOT NULL,
          topic VARCHAR(191) NOT NULL,
          payload LONGBLOB NOT NULL,
          `key` VARCHAR(191) NOT NULL UNIQUE,
          status VARCHAR(32) NOT NULL,
          locked_by VARCHAR(191),
          locked_until DATETIME(6),
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          processed_at DATETIME(6),
          INDEX #{quote_column_name(index_name("outbox", "queue"))} (status, created_at),
          INDEX #{quote_column_name(index_name("outbox", "expired_lease"))} (status, locked_until, created_at),
          INDEX #{quote_column_name(index_name("outbox", "worker_lease"))} (status, locked_by),
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("waits")} (
          id VARCHAR(191) PRIMARY KEY,
          workflow_id VARCHAR(191) NOT NULL,
          position INT NOT NULL,
          kind VARCHAR(32) NOT NULL,
          event_key VARCHAR(191),
          wake_at DATETIME(6),
          context LONGBLOB NOT NULL,
          payload LONGBLOB,
          status VARCHAR(32) NOT NULL,
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          completed_at DATETIME(6),
          INDEX #{quote_column_name(index_name("waits", "workflow_created"))} (workflow_id, created_at),
          INDEX #{quote_column_name(index_name("waits", "event_pending"))} (status, kind, event_key, created_at),
          INDEX #{quote_column_name(index_name("waits", "timer_pending"))} (status, kind, wake_at, created_at),
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("durable_objects")} (
          worker_pool VARCHAR(191) NOT NULL DEFAULT 'default',
          object_type VARCHAR(191) NOT NULL,
          object_id VARCHAR(191) NOT NULL,
          state LONGBLOB,
          locked_by VARCHAR(191),
          locked_until DATETIME(6),
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          updated_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          PRIMARY KEY (worker_pool, object_type, object_id)
        )
      SQL
      add_column_if_missing("durable_objects", "worker_pool", "VARCHAR(191) NOT NULL DEFAULT 'default'")
      ensure_worker_pool_primary_key!("durable_objects", ["worker_pool", "object_type", "object_id"])
      create_inbox_tables!
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("durable_object_commands")} (
          id VARCHAR(191) PRIMARY KEY,
          object_type VARCHAR(191) NOT NULL,
          object_id VARCHAR(191) NOT NULL,
          method_name VARCHAR(191) NOT NULL,
          args LONGBLOB NOT NULL,
          kwargs LONGBLOB NOT NULL,
          status VARCHAR(32) NOT NULL,
          result LONGBLOB,
          error TEXT,
          locked_by VARCHAR(191),
          locked_until DATETIME(6),
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          completed_at DATETIME(6),
          INDEX #{quote_column_name(index_name("durable_object_commands", "object_status"))} (object_type, object_id, status, created_at)
        )
      SQL
      create_performance_indexes!
      @migrated = true
      self
    end

    private

    #: (untyped, untyped, untyped) -> untyped
    def add_column_if_missing(table_name, column_name, column_type)
      exists = execute_params(<<~SQL, ["#{table_prefix}_#{table_name}", column_name]).first
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = ?
          AND column_name = ?
        LIMIT 1
      SQL
      return if exists

      execute("ALTER TABLE #{table(table_name)} ADD COLUMN #{quote_column_name(column_name.to_s)} #{column_type}")
    end

    # Widen a legacy PRIMARY KEY so it includes worker_pool. Fresh installs already declare the
    # worker-pool-scoped key in CREATE TABLE, so this is a no-op there; only schemas created before
    # worker_pool existed get rewritten. Without it the old narrower key keeps enforcing identity on
    # (e.g.) (object_type, object_id), so two worker pools sharing an id collide and overwrite state.
    #: (String, Array[String]) -> void
    def ensure_worker_pool_primary_key!(table_name, columns)
      return if key_includes_worker_pool?(table_name, "PRIMARY")

      column_list = columns.map { |column| quote_column_name(column) }.join(", ")
      execute("ALTER TABLE #{table(table_name)} DROP PRIMARY KEY, ADD PRIMARY KEY (#{column_list})")
    end

    # Widen a legacy UNIQUE KEY so it includes worker_pool. The legacy index is matched by its exact
    # column signature (worker_pool removed) so the right index is dropped regardless of its name, then
    # the worker-pool-scoped unique key is recreated. No-op when the widened key already exists.
    #: (String, String, Array[String]) -> void
    def ensure_worker_pool_unique_key!(table_name, index_suffix, columns)
      indexes = unique_indexes_with_columns(table_name)
      return if indexes.value?(columns.join(","))

      legacy_columns = (columns - ["worker_pool"]).join(",")
      legacy_name = indexes.find { |_name, index_columns| index_columns == legacy_columns }&.first
      drop_index_if_present(table_name, legacy_name) if legacy_name
      name = index_name(table_name, index_suffix)
      column_list = columns.map { |column| quote_column_name(column) }.join(", ")
      add_index_if_missing(table_name, name, "UNIQUE KEY #{quote_column_name(name)} (#{column_list})")
    end

    #: (String, String) -> bool
    def key_includes_worker_pool?(table_name, index)
      !execute_params(<<~SQL, [raw_table_name(table_name), index]).first.nil?
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = ?
          AND index_name = ?
          AND column_name = 'worker_pool'
        LIMIT 1
      SQL
    end

    # Maps each UNIQUE index on the table to its ordered, comma-joined column list, so callers can match
    # a legacy key by exact column signature rather than by a possibly-divergent index name.
    #: (String) -> Hash[String, String]
    def unique_indexes_with_columns(table_name)
      rows = execute_params(<<~SQL, [raw_table_name(table_name)]).to_a
        SELECT index_name, seq_in_index, column_name
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = ?
          AND non_unique = 0
        ORDER BY index_name, seq_in_index
      SQL
      grouped = {} #: Hash[String, Array[[Integer, String]]]
      rows.each do |row|
        # information_schema returns uppercase column labels (INDEX_NAME, ...); normalize before fetch.
        normalized = row.transform_keys { |key| key.to_s.downcase }
        (grouped[normalized.fetch("index_name")] ||= []) << [normalized.fetch("seq_in_index").to_i, normalized.fetch("column_name").to_s]
      end
      grouped.transform_values { |entries| entries.sort_by(&:first).map(&:last).join(",") }
    end

    #: () -> untyped
    def create_inbox_tables!
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("mailbox_sequences")} (
          worker_pool VARCHAR(191) NOT NULL DEFAULT 'default',
          target_kind VARCHAR(32) NOT NULL,
          target_type VARCHAR(191) NOT NULL,
          target_id VARCHAR(191) NOT NULL,
          last_sequence BIGINT NOT NULL DEFAULT 0,
          updated_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          PRIMARY KEY (worker_pool, target_kind, target_type, target_id)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("inbox")} (
          id VARCHAR(191) PRIMARY KEY,
          worker_pool VARCHAR(191) NOT NULL DEFAULT 'default',
          target_kind VARCHAR(32) NOT NULL,
          target_type VARCHAR(191) NOT NULL,
          target_id VARCHAR(191) NOT NULL,
          sequence BIGINT NOT NULL,
          message_kind VARCHAR(32) NOT NULL,
          method_name VARCHAR(191),
          operation_id VARCHAR(191) NOT NULL,
          idempotency_key VARCHAR(191),
          idempotency_hash VARCHAR(64),
          shape_hash VARCHAR(64) NOT NULL,
          payload LONGBLOB NOT NULL,
          status VARCHAR(32) NOT NULL,
          attempts INT NOT NULL DEFAULT 0,
          max_attempts INT,
          ready_at DATETIME(6),
          result LONGBLOB,
          error TEXT,
          locked_by VARCHAR(191),
          locked_until DATETIME(6),
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          updated_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          completed_at DATETIME(6),
          dead_lettered_at DATETIME(6),
          UNIQUE KEY #{quote_column_name(index_name("inbox", "target_sequence_unique"))} (worker_pool, target_kind, target_type, target_id, sequence),
          UNIQUE KEY #{quote_column_name(index_name("inbox", "idempotency_hash_unique"))} (idempotency_hash),
          INDEX #{quote_column_name(index_name("inbox", "target_status_sequence"))} (worker_pool, target_kind, target_type, target_id, status, sequence),
          INDEX #{quote_column_name(index_name("inbox", "target_sequence"))} (worker_pool, target_kind, target_type, target_id, sequence),
          INDEX #{quote_column_name(index_name("inbox", "ready"))} (worker_pool, status, ready_at, created_at),
          INDEX #{quote_column_name(index_name("inbox", "worker_lease"))} (status, locked_by)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("target_activations")} (
          worker_pool VARCHAR(191) NOT NULL DEFAULT 'default',
          target_kind VARCHAR(32) NOT NULL,
          target_type VARCHAR(191) NOT NULL,
          target_id VARCHAR(191) NOT NULL,
          status VARCHAR(32) NOT NULL,
          ready_at DATETIME(6) NOT NULL,
          locked_by VARCHAR(191),
          locked_until DATETIME(6),
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          updated_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          PRIMARY KEY (worker_pool, target_kind, target_type, target_id),
          INDEX #{quote_column_name(index_name("target_activations", "queue"))} (worker_pool, status, ready_at, created_at),
          INDEX #{quote_column_name(index_name("target_activations", "expired"))} (worker_pool, status, locked_until, created_at),
          INDEX #{quote_column_name(index_name("target_activations", "worker_lease"))} (status, locked_by)
        )
      SQL
      add_column_if_missing("mailbox_sequences", "worker_pool", "VARCHAR(191) NOT NULL DEFAULT 'default'")
      add_column_if_missing("inbox", "worker_pool", "VARCHAR(191) NOT NULL DEFAULT 'default'")
      add_column_if_missing("inbox", "idempotency_hash", "VARCHAR(64)")
      add_column_if_missing("target_activations", "worker_pool", "VARCHAR(191) NOT NULL DEFAULT 'default'")
      ensure_worker_pool_primary_key!("mailbox_sequences", ["worker_pool", "target_kind", "target_type", "target_id"])
      ensure_worker_pool_unique_key!("inbox", "target_sequence_unique", ["worker_pool", "target_kind", "target_type", "target_id", "sequence"])
      ensure_worker_pool_primary_key!("target_activations", ["worker_pool", "target_kind", "target_type", "target_id"])
      backfill_inbox_idempotency_hashes!
      drop_index_if_present("inbox", "idempotency_key")
      add_index_if_missing("inbox", index_name("inbox", "idempotency_hash_unique"), "UNIQUE KEY #{quote_column_name(index_name("inbox", "idempotency_hash_unique"))} (idempotency_hash)")
    end

    #: () -> untyped
    def backfill_inbox_idempotency_hashes!
      rows = execute_params(<<~SQL, []).to_a
        SELECT id, worker_pool, target_kind, target_type, target_id, idempotency_key
        FROM #{table("inbox")}
        WHERE idempotency_key IS NOT NULL AND idempotency_hash IS NULL
      SQL
      rows.each do |row|
        hash = inbox_idempotency_hash_for_migration(
          row.fetch("idempotency_key"),
          worker_pool: row.fetch("worker_pool"),
          target_kind: row.fetch("target_kind"),
          target_type: row.fetch("target_type"),
          target_id: row.fetch("target_id"),
        )
        execute_params("UPDATE #{table("inbox")} SET idempotency_hash = ? WHERE id = ?", [hash, row.fetch("id")])
      end
    end

    #: (untyped, worker_pool: untyped, target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def inbox_idempotency_hash_for_migration(idempotency_key, worker_pool:, target_kind:, target_type:, target_id:)
      Digest::SHA256.hexdigest(Store::SERIALIZER.dump({
        "worker_pool" => worker_pool,
        "target_kind" => target_kind,
        "target_type" => target_type,
        "target_id" => target_id,
        "idempotency_key" => idempotency_key,
      }))
    end

    #: () -> untyped
    def create_performance_indexes!
      add_index_if_missing("workflows", index_name("workflows", "worker_lease"), "INDEX #{quote_column_name(index_name("workflows", "worker_lease"))} (status, locked_by)")
      add_index_if_missing("outbox", index_name("outbox", "worker_lease"), "INDEX #{quote_column_name(index_name("outbox", "worker_lease"))} (status, locked_by)")
      add_index_if_missing("inbox", index_name("inbox", "worker_lease"), "INDEX #{quote_column_name(index_name("inbox", "worker_lease"))} (status, locked_by)")
      add_index_if_missing("target_activations", index_name("target_activations", "worker_lease"), "INDEX #{quote_column_name(index_name("target_activations", "worker_lease"))} (status, locked_by)")
    end

    #: (untyped, untyped, untyped) -> untyped
    def add_index_if_missing(table_name, index_name, index_definition)
      exists = execute_params(<<~SQL, [raw_table_name(table_name), index_name]).first
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = ?
          AND index_name = ?
        LIMIT 1
      SQL
      return if exists

      execute("ALTER TABLE #{table(table_name)} ADD #{index_definition}")
    end

    #: (untyped, untyped) -> untyped
    def drop_index_if_present(table_name, index_name)
      exists = execute_params(<<~SQL, [raw_table_name(table_name), index_name]).first
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = ?
          AND index_name = ?
        LIMIT 1
      SQL
      return unless exists

      execute("DROP INDEX #{quote_column_name(index_name.to_s)} ON #{table(table_name)}")
    end
  end
end
