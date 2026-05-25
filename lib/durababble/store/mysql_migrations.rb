# typed: true
# frozen_string_literal: true

module Durababble
  module MysqlMigrations
    def migrate!
      return self if @migrated

      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("workflows")} (
          id VARCHAR(191) PRIMARY KEY,
          name VARCHAR(191) NOT NULL,
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
          INDEX #{@connection.quote_column_name(index_name("workflows", "queue"))} (status, created_at),
          INDEX #{@connection.quote_column_name(index_name("workflows", "runnable_due"))} (status, next_run_at, created_at),
          INDEX #{@connection.quote_column_name(index_name("workflows", "expired_lease"))} (status, locked_until, created_at)
        )
      SQL
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
          INDEX #{@connection.quote_column_name(index_name("workflow_history", "command"))} (workflow_id, command_id, event_index),
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
          INDEX #{@connection.quote_column_name(index_name("step_attempts", "workflow_started_position"))} (workflow_id, started_at, position),
          INDEX #{@connection.quote_column_name(index_name("step_attempts", "workflow_position_status_started"))} (workflow_id, position, status, started_at),
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
          INDEX #{@connection.quote_column_name(index_name("outbox", "queue"))} (status, created_at),
          INDEX #{@connection.quote_column_name(index_name("outbox", "expired_lease"))} (status, locked_until, created_at),
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
          INDEX #{@connection.quote_column_name(index_name("waits", "workflow_created"))} (workflow_id, created_at),
          INDEX #{@connection.quote_column_name(index_name("waits", "event_pending"))} (status, kind, event_key, created_at),
          INDEX #{@connection.quote_column_name(index_name("waits", "timer_pending"))} (status, kind, wake_at, created_at),
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("durable_objects")} (
          object_type VARCHAR(191) NOT NULL,
          object_id VARCHAR(191) NOT NULL,
          state LONGBLOB,
          locked_by VARCHAR(191),
          locked_until DATETIME(6),
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          updated_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          PRIMARY KEY (object_type, object_id)
        )
      SQL
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
          INDEX #{@connection.quote_column_name(index_name("durable_object_commands", "object_status"))} (object_type, object_id, status, created_at)
        )
      SQL
      @migrated = true
      self
    end

    #: () -> untyped


    private

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

      execute("ALTER TABLE #{table(table_name)} ADD COLUMN #{@connection.quote_column_name(column_name.to_s)} #{column_type}")
    end

    #: () -> untyped

    def create_inbox_tables!
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("mailbox_sequences")} (
          target_kind VARCHAR(32) NOT NULL,
          target_type VARCHAR(191) NOT NULL,
          target_id VARCHAR(191) NOT NULL,
          last_sequence BIGINT NOT NULL DEFAULT 0,
          updated_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          PRIMARY KEY (target_kind, target_type, target_id)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("inbox")} (
          id VARCHAR(191) PRIMARY KEY,
          target_kind VARCHAR(32) NOT NULL,
          target_type VARCHAR(191) NOT NULL,
          target_id VARCHAR(191) NOT NULL,
          sequence BIGINT NOT NULL,
          message_kind VARCHAR(32) NOT NULL,
          method_name VARCHAR(191),
          operation_id VARCHAR(191) NOT NULL,
          idempotency_key VARCHAR(191),
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
          UNIQUE KEY #{@connection.quote_column_name(index_name("inbox", "target_sequence_unique"))} (target_kind, target_type, target_id, sequence),
          UNIQUE KEY #{@connection.quote_column_name(index_name("inbox", "target_idempotency_unique"))} (target_kind, target_type, target_id, idempotency_key),
          INDEX #{@connection.quote_column_name(index_name("inbox", "target_status_sequence"))} (target_kind, target_type, target_id, status, sequence),
          INDEX #{@connection.quote_column_name(index_name("inbox", "target_sequence"))} (target_kind, target_type, target_id, sequence),
          INDEX #{@connection.quote_column_name(index_name("inbox", "ready"))} (status, ready_at, created_at)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("target_activations")} (
          target_kind VARCHAR(32) NOT NULL,
          target_type VARCHAR(191) NOT NULL,
          target_id VARCHAR(191) NOT NULL,
          status VARCHAR(32) NOT NULL,
          ready_at DATETIME(6) NOT NULL,
          locked_by VARCHAR(191),
          locked_until DATETIME(6),
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          updated_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          PRIMARY KEY (target_kind, target_type, target_id),
          INDEX #{@connection.quote_column_name(index_name("target_activations", "queue"))} (status, ready_at, created_at),
          INDEX #{@connection.quote_column_name(index_name("target_activations", "expired"))} (status, locked_until, created_at)
        )
      SQL
      drop_index_if_present("inbox", "idempotency_key")
      add_index_if_missing("inbox", index_name("inbox", "target_idempotency_unique"), "UNIQUE KEY #{@connection.quote_column_name(index_name("inbox", "target_idempotency_unique"))} (target_kind, target_type, target_id, idempotency_key)")
    end

    #: (command_id: untyped, worker_id: untyped) -> untyped

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

      execute("DROP INDEX #{@connection.quote_column_name(index_name.to_s)} ON #{table(table_name)}")
    end

    #: (untyped) -> bool

  end
end
