# typed: true
# frozen_string_literal: true

module Durababble
  # Minimal SQLite schema mirroring MysqlMigrations, with two deliberate
  # differences suited to a test-only / deterministic-simulation store:
  #
  #   * timestamps are INTEGER (the store's integer clock; see SqliteStore),
  #     defaulting to the per-connection dura_now() UDF so created_at/updated_at
  #     advance with virtual time without the application having to pass them;
  #   * payloads are BLOB and indexes are created with separate CREATE INDEX
  #     statements (SQLite cannot declare indexes inline in CREATE TABLE).
  #
  # Performance indexes are intentionally omitted — correctness under a single
  # serialized connection does not depend on them.
  module SqliteMigrations
    #: () -> untyped
    def migrate!
      return self if @migrated

      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("workflows")} (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          worker_pool TEXT NOT NULL DEFAULT 'default',
          status TEXT NOT NULL,
          input BLOB NOT NULL,
          result BLOB,
          error TEXT,
          locked_by TEXT,
          locked_until INTEGER,
          next_run_at INTEGER,
          cancel_reason TEXT,
          cancel_requested_at INTEGER,
          cancel_delivered_at INTEGER,
          child_origin_kind TEXT,
          parent_workflow_id TEXT,
          parent_command_id INTEGER,
          parent_object_type TEXT,
          parent_object_id TEXT,
          parent_object_command_id TEXT,
          child_cancellation_policy TEXT,
          colocated_owner_object_type TEXT,
          colocated_owner_object_id TEXT,
          created_at INTEGER NOT NULL DEFAULT (dura_now()),
          updated_at INTEGER NOT NULL DEFAULT (dura_now()),
          queue_available_at INTEGER GENERATED ALWAYS AS (
            CASE
              WHEN status IN ('pending', 'canceling') THEN COALESCE(next_run_at, created_at)
              WHEN status = 'waiting' AND next_run_at IS NOT NULL THEN next_run_at
              WHEN status = 'failed' AND next_run_at IS NOT NULL THEN next_run_at
              WHEN status = 'running' AND locked_until IS NOT NULL THEN locked_until
              ELSE NULL
            END
          ) STORED
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("workflow_history")} (
          workflow_id TEXT NOT NULL,
          event_index INTEGER NOT NULL,
          kind TEXT NOT NULL,
          command_id INTEGER,
          name TEXT,
          attempt_id TEXT,
          payload BLOB,
          error TEXT,
          created_at INTEGER NOT NULL DEFAULT (dura_now()),
          PRIMARY KEY (workflow_id, event_index),
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("steps")} (
          workflow_id TEXT NOT NULL,
          position INTEGER NOT NULL,
          name TEXT NOT NULL,
          status TEXT NOT NULL,
          result BLOB,
          error TEXT,
          heartbeat_cursor BLOB,
          started_at INTEGER,
          completed_at INTEGER,
          updated_at INTEGER NOT NULL DEFAULT (dura_now()),
          PRIMARY KEY (workflow_id, position),
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("step_attempts")} (
          id TEXT PRIMARY KEY,
          workflow_id TEXT NOT NULL,
          position INTEGER NOT NULL,
          name TEXT NOT NULL,
          status TEXT NOT NULL,
          result BLOB,
          error TEXT,
          heartbeat_cursor BLOB,
          started_at INTEGER NOT NULL DEFAULT (dura_now()),
          completed_at INTEGER,
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE INDEX IF NOT EXISTS #{index_name("step_attempts", "workflow_position_status_started")}
        ON #{table("step_attempts")} (workflow_id, position, status, started_at)
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("fences")} (
          workflow_id TEXT NOT NULL,
          `key` TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'completed',
          result BLOB,
          error TEXT,
          locked_by TEXT,
          locked_until INTEGER,
          created_at INTEGER NOT NULL DEFAULT (dura_now()),
          completed_at INTEGER,
          PRIMARY KEY (workflow_id, `key`),
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("outbox")} (
          id TEXT PRIMARY KEY,
          workflow_id TEXT NOT NULL,
          topic TEXT NOT NULL,
          payload BLOB NOT NULL,
          `key` TEXT NOT NULL UNIQUE,
          status TEXT NOT NULL,
          locked_by TEXT,
          locked_until INTEGER,
          created_at INTEGER NOT NULL DEFAULT (dura_now()),
          processed_at INTEGER,
          queue_available_at INTEGER GENERATED ALWAYS AS (
            CASE
              WHEN status = 'pending' THEN created_at
              WHEN status = 'processing' AND locked_until IS NOT NULL THEN locked_until
              ELSE NULL
            END
          ) STORED,
          FOREIGN KEY (workflow_id) REFERENCES #{table("workflows")}(id) ON DELETE CASCADE
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("durable_objects")} (
          worker_pool TEXT NOT NULL DEFAULT 'default',
          object_type TEXT NOT NULL,
          object_id TEXT NOT NULL,
          state BLOB,
          locked_by TEXT,
          locked_until INTEGER,
          colocated_owner_object_type TEXT,
          colocated_owner_object_id TEXT,
          created_at INTEGER NOT NULL DEFAULT (dura_now()),
          updated_at INTEGER NOT NULL DEFAULT (dura_now()),
          PRIMARY KEY (object_type, object_id)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("object_wakeups")} (
          worker_pool TEXT NOT NULL DEFAULT 'default',
          object_type TEXT NOT NULL,
          object_id TEXT NOT NULL,
          name TEXT NOT NULL,
          wake_at INTEGER NOT NULL,
          payload BLOB NOT NULL,
          created_at INTEGER NOT NULL DEFAULT (dura_now()),
          updated_at INTEGER NOT NULL DEFAULT (dura_now()),
          PRIMARY KEY (worker_pool, object_type, object_id, name)
        )
      SQL
      execute(<<~SQL)
        CREATE INDEX IF NOT EXISTS #{index_name("object_wakeups", "due")}
        ON #{table("object_wakeups")} (wake_at, created_at)
      SQL
      create_inbox_tables!
      @migrated = true
      self
    end

    private

    #: () -> untyped
    def create_inbox_tables!
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("mailbox_sequences")} (
          worker_pool TEXT NOT NULL DEFAULT 'default',
          target_kind TEXT NOT NULL,
          target_type TEXT NOT NULL,
          target_id TEXT NOT NULL,
          last_sequence INTEGER NOT NULL DEFAULT 0,
          updated_at INTEGER NOT NULL DEFAULT (dura_now()),
          PRIMARY KEY (target_kind, target_type, target_id)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("inbox")} (
          id TEXT PRIMARY KEY,
          worker_pool TEXT NOT NULL DEFAULT 'default',
          target_kind TEXT NOT NULL,
          target_type TEXT NOT NULL,
          target_id TEXT NOT NULL,
          sequence INTEGER NOT NULL,
          message_kind TEXT NOT NULL,
          method_name TEXT,
          operation_id TEXT NOT NULL,
          idempotency_key TEXT,
          idempotency_hash TEXT,
          shape_hash TEXT NOT NULL,
          payload BLOB NOT NULL,
          status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER,
          ready_at INTEGER,
          result BLOB,
          error TEXT,
          locked_by TEXT,
          locked_until INTEGER,
          created_at INTEGER NOT NULL DEFAULT (dura_now()),
          updated_at INTEGER NOT NULL DEFAULT (dura_now()),
          completed_at INTEGER,
          dead_lettered_at INTEGER,
          UNIQUE (target_kind, target_type, target_id, sequence),
          UNIQUE (idempotency_hash)
        )
      SQL
      execute(<<~SQL)
        CREATE INDEX IF NOT EXISTS #{index_name("inbox", "target_status_sequence")}
        ON #{table("inbox")} (target_kind, target_type, target_id, status, sequence)
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("target_activations")} (
          worker_pool TEXT NOT NULL DEFAULT 'default',
          target_kind TEXT NOT NULL,
          target_type TEXT NOT NULL,
          target_id TEXT NOT NULL,
          status TEXT NOT NULL,
          ready_at INTEGER NOT NULL,
          locked_by TEXT,
          locked_until INTEGER,
          created_at INTEGER NOT NULL DEFAULT (dura_now()),
          updated_at INTEGER NOT NULL DEFAULT (dura_now()),
          queue_available_at INTEGER GENERATED ALWAYS AS (
            CASE
              WHEN status = 'pending' THEN ready_at
              WHEN status = 'running' AND locked_until IS NOT NULL THEN locked_until
              ELSE NULL
            END
          ) STORED,
          PRIMARY KEY (target_kind, target_type, target_id)
        )
      SQL
      execute(<<~SQL)
        CREATE INDEX IF NOT EXISTS #{index_name("target_activations", "claim")}
        ON #{table("target_activations")} (worker_pool, target_kind, target_type, queue_available_at, created_at)
      SQL
    end
  end
end
