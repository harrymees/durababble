# typed: true
# frozen_string_literal: true

module Durababble
  module MysqlMigrations
    # Pre-production: we create every table in its final shape. There is no live data to migrate, so
    # the schema is declared once in CREATE TABLE rather than assembled through incremental ALTERs.
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
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("workflow_history")} (
          workflow_id VARCHAR(191) NOT NULL,
          event_index INT NOT NULL,
          kind VARCHAR(64) NOT NULL,
          command_id INT,
          name TEXT,
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
          name TEXT NOT NULL,
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
          name TEXT NOT NULL,
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
          PRIMARY KEY (object_type, object_id)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("object_wakeups")} (
          worker_pool VARCHAR(191) NOT NULL DEFAULT 'default',
          object_type VARCHAR(191) NOT NULL,
          object_id VARCHAR(191) NOT NULL,
          name VARCHAR(191) NOT NULL,
          wake_at DATETIME(6) NOT NULL,
          payload LONGBLOB NOT NULL,
          created_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          updated_at DATETIME(6) NOT NULL DEFAULT NOW(6),
          PRIMARY KEY (worker_pool, object_type, object_id, name),
          INDEX #{quote_column_name(index_name("object_wakeups", "due"))} (wake_at, created_at)
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
          INDEX #{quote_column_name(index_name("durable_object_commands", "object_status"))} (object_type, object_id, status, created_at)
        )
      SQL
      @migrated = true
      self
    end

    private

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
          PRIMARY KEY (target_kind, target_type, target_id)
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
          UNIQUE KEY #{quote_column_name(index_name("inbox", "target_sequence_unique"))} (target_kind, target_type, target_id, sequence),
          UNIQUE KEY #{quote_column_name(index_name("inbox", "idempotency_hash_unique"))} (idempotency_hash),
          INDEX #{quote_column_name(index_name("inbox", "target_status_sequence"))} (target_kind, target_type, target_id, status, sequence),
          INDEX #{quote_column_name(index_name("inbox", "ready"))} (status, ready_at, created_at),
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
          PRIMARY KEY (target_kind, target_type, target_id),
          INDEX #{quote_column_name(index_name("target_activations", "queue"))} (worker_pool, status, ready_at, created_at),
          INDEX #{quote_column_name(index_name("target_activations", "expired"))} (worker_pool, status, locked_until, created_at),
          INDEX #{quote_column_name(index_name("target_activations", "worker_lease"))} (status, locked_by)
        )
      SQL
    end
  end
end
