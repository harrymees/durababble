# typed: true
# frozen_string_literal: true

module Durababble
  module PostgresMigrations
    #: () -> untyped
    def migrate!
      return self if @migrated

      execute("CREATE SCHEMA IF NOT EXISTS #{quoted_schema}")
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("workflows")} (
          id text PRIMARY KEY,
          name text NOT NULL,
          status text NOT NULL,
          input bytea NOT NULL,
          result bytea,
          error text,
          locked_by text,
          locked_until timestamptz,
          next_run_at timestamptz,
          cancel_reason text,
          cancel_requested_at timestamptz,
          cancel_delivered_at timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      SQL
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS locked_by text")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS locked_until timestamptz")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS next_run_at timestamptz")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS cancel_reason text")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS cancel_requested_at timestamptz")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS cancel_delivered_at timestamptz")
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("workflow_history")} (
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          event_index integer NOT NULL,
          kind text NOT NULL,
          command_id integer,
          name text,
          attempt_id text,
          payload bytea,
          error text,
          created_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (workflow_id, event_index)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("steps")} (
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          position integer NOT NULL,
          name text NOT NULL,
          status text NOT NULL,
          result bytea,
          error text,
          heartbeat_cursor bytea,
          started_at timestamptz,
          completed_at timestamptz,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (workflow_id, position)
        )
      SQL
      execute("ALTER TABLE #{table("steps")} ADD COLUMN IF NOT EXISTS heartbeat_cursor bytea")
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("step_attempts")} (
          id text PRIMARY KEY,
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          position integer NOT NULL,
          name text NOT NULL,
          status text NOT NULL,
          result bytea,
          error text,
          heartbeat_cursor bytea,
          started_at timestamptz NOT NULL DEFAULT now(),
          completed_at timestamptz
        )
      SQL
      execute("ALTER TABLE #{table("step_attempts")} ADD COLUMN IF NOT EXISTS heartbeat_cursor bytea")
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("waits")} (
          id text PRIMARY KEY,
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          position integer NOT NULL,
          scope text NOT NULL DEFAULT 'step',
          kind text NOT NULL,
          event_key text,
          wake_at timestamptz,
          context bytea NOT NULL,
          payload bytea,
          status text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          completed_at timestamptz
        )
      SQL
      execute("ALTER TABLE #{table("waits")} ADD COLUMN IF NOT EXISTS scope text NOT NULL DEFAULT 'step'")
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("fences")} (
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          key text NOT NULL,
          status text NOT NULL DEFAULT 'completed',
          result bytea,
          error text,
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          completed_at timestamptz,
          PRIMARY KEY (workflow_id, key)
        )
      SQL
      execute("ALTER TABLE #{table("fences")} ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'completed'")
      execute("ALTER TABLE #{table("fences")} ALTER COLUMN result DROP NOT NULL")
      execute("ALTER TABLE #{table("fences")} ADD COLUMN IF NOT EXISTS error text")
      execute("ALTER TABLE #{table("fences")} ADD COLUMN IF NOT EXISTS locked_by text")
      execute("ALTER TABLE #{table("fences")} ADD COLUMN IF NOT EXISTS locked_until timestamptz")
      execute("ALTER TABLE #{table("fences")} ADD COLUMN IF NOT EXISTS completed_at timestamptz")
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("outbox")} (
          id text PRIMARY KEY,
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          topic text NOT NULL,
          payload bytea NOT NULL,
          key text NOT NULL UNIQUE,
          status text NOT NULL,
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          processed_at timestamptz
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("durable_objects")} (
          object_type text NOT NULL,
          object_id text NOT NULL,
          state bytea,
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (object_type, object_id)
        )
      SQL
      create_inbox_tables!
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("durable_object_commands")} (
          id text PRIMARY KEY,
          object_type text NOT NULL,
          object_id text NOT NULL,
          method_name text NOT NULL,
          args bytea NOT NULL,
          kwargs bytea NOT NULL,
          status text NOT NULL,
          result bytea,
          error text,
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          completed_at timestamptz
        )
      SQL
      create_performance_indexes!
      migrate_serialized_columns!
      @migrated = true
      self
    end

    private

    #: () -> untyped
    def migrate_serialized_columns!
      migrate_serialized_column!("workflows", "input", not_null: true)
      migrate_serialized_column!("workflows", "result")
      migrate_serialized_column!("workflow_history", "payload")
      migrate_serialized_column!("steps", "result")
      migrate_serialized_column!("steps", "heartbeat_cursor")
      migrate_serialized_column!("step_attempts", "result")
      migrate_serialized_column!("step_attempts", "heartbeat_cursor")
      migrate_serialized_column!("waits", "context", not_null: true)
      migrate_serialized_column!("waits", "payload")
      migrate_serialized_column!("fences", "result")
      migrate_serialized_column!("outbox", "payload", not_null: true)
      migrate_serialized_column!("durable_objects", "state")
      migrate_serialized_column!("durable_object_commands", "args", not_null: true)
      migrate_serialized_column!("durable_object_commands", "kwargs", not_null: true)
      migrate_serialized_column!("durable_object_commands", "result")
      migrate_serialized_column!("inbox", "payload", not_null: true)
      migrate_serialized_column!("inbox", "result")
    end

    #: () -> untyped
    def create_inbox_tables!
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("mailbox_sequences")} (
          target_kind text NOT NULL,
          target_type text NOT NULL,
          target_id text NOT NULL,
          last_sequence bigint NOT NULL DEFAULT 0,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (target_kind, target_type, target_id)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("inbox")} (
          id text PRIMARY KEY,
          target_kind text NOT NULL,
          target_type text NOT NULL,
          target_id text NOT NULL,
          sequence bigint NOT NULL,
          message_kind text NOT NULL,
          method_name text,
          operation_id text NOT NULL,
          idempotency_key text,
          shape_hash text NOT NULL,
          payload bytea NOT NULL,
          status text NOT NULL,
          attempts integer NOT NULL DEFAULT 0,
          max_attempts integer,
          ready_at timestamptz,
          result bytea,
          error text,
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          completed_at timestamptz,
          dead_lettered_at timestamptz,
          UNIQUE (target_kind, target_type, target_id, sequence)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("target_activations")} (
          target_kind text NOT NULL,
          target_type text NOT NULL,
          target_id text NOT NULL,
          status text NOT NULL,
          ready_at timestamptz NOT NULL,
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (target_kind, target_type, target_id)
        )
      SQL
    end

    #: () -> untyped
    def create_performance_indexes!
      retry_migration_serialization_failures do
        execute("CREATE INDEX IF NOT EXISTS workflows_queue_idx ON #{table("workflows")} (status, created_at)")
        execute("CREATE INDEX IF NOT EXISTS workflows_runnable_due_idx ON #{table("workflows")} (status, next_run_at, created_at)")
        execute("CREATE INDEX IF NOT EXISTS workflows_expired_lease_idx ON #{table("workflows")} (status, locked_until)")
        execute("CREATE INDEX IF NOT EXISTS workflow_history_command_idx ON #{table("workflow_history")} (workflow_id, command_id, event_index)")
        execute("CREATE INDEX IF NOT EXISTS waits_event_pending_idx ON #{table("waits")} (status, scope, kind, event_key, created_at)")
        execute("CREATE INDEX IF NOT EXISTS waits_timer_pending_idx ON #{table("waits")} (status, scope, kind, wake_at, created_at)")
        execute("CREATE INDEX IF NOT EXISTS waits_workflow_created_idx ON #{table("waits")} (workflow_id, created_at)")
        execute("CREATE INDEX IF NOT EXISTS step_attempts_workflow_started_position_idx ON #{table("step_attempts")} (workflow_id, started_at, position)")
        execute("CREATE INDEX IF NOT EXISTS step_attempts_workflow_position_status_started_idx ON #{table("step_attempts")} (workflow_id, position, status, started_at DESC)")
        execute("CREATE INDEX IF NOT EXISTS outbox_queue_idx ON #{table("outbox")} (status, created_at)")
        execute("CREATE INDEX IF NOT EXISTS outbox_expired_lease_idx ON #{table("outbox")} (status, locked_until)")
        execute("ALTER TABLE #{table("inbox")} DROP CONSTRAINT IF EXISTS inbox_idempotency_key_key")
        execute("CREATE UNIQUE INDEX IF NOT EXISTS inbox_target_idempotency_idx ON #{table("inbox")} (target_kind, target_type, target_id, idempotency_key) WHERE idempotency_key IS NOT NULL")
        execute("CREATE INDEX IF NOT EXISTS inbox_target_status_sequence_idx ON #{table("inbox")} (target_kind, target_type, target_id, status, sequence)")
        execute("CREATE INDEX IF NOT EXISTS inbox_target_sequence_idx ON #{table("inbox")} (target_kind, target_type, target_id, sequence)")
        execute("CREATE INDEX IF NOT EXISTS inbox_ready_idx ON #{table("inbox")} (status, ready_at, created_at)")
        execute("CREATE INDEX IF NOT EXISTS target_activations_queue_idx ON #{table("target_activations")} (status, ready_at, created_at)")
        execute("CREATE INDEX IF NOT EXISTS target_activations_expired_idx ON #{table("target_activations")} (status, locked_until, created_at)")
      end
    end

    #: () { (?) -> untyped } -> untyped
    def retry_migration_serialization_failures(&block)
      attempts = 0
      begin
        block.call
      rescue ActiveRecord::SerializationFailure
        attempts += 1
        Kernel.raise if attempts >= 20

        Kernel.sleep(0.001 * attempts)
        retry
      end
    end

    #: (untyped, untyped, ?not_null: untyped) -> untyped
    def migrate_serialized_column!(table_name, column_name, not_null: false)
      column = execute_params(<<~SQL, [schema, table_name, column_name]).first
        SELECT data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
      SQL
      return unless column
      return if column.fetch("data_type") == "bytea"

      temporary_column = "#{column_name}__paquito"
      execute("ALTER TABLE #{table(table_name)} ADD COLUMN IF NOT EXISTS #{@connection.quote_column_name(temporary_column.to_s)} bytea")
      rows = execute("SELECT * FROM #{table(table_name)}")
      primary_keys = primary_key_columns(table_name)
      rows.each do |row|
        raw = row[column_name]
        value = raw.nil? ? nil : JSON.parse(raw)
        execute_params(<<~SQL, primary_keys.map { |key| row.fetch(key) } + [dump_serialized(value)])
          UPDATE #{table(table_name)}
          SET #{@connection.quote_column_name(temporary_column.to_s)} = $#{primary_keys.length + 1}::bytea
          WHERE #{primary_keys.each_with_index.map { |key, index| "#{@connection.quote_column_name(key.to_s)} = $#{index + 1}" }.join(" AND ")}
        SQL
      end
      if not_null
        execute_params(
          "UPDATE #{table(table_name)} SET #{@connection.quote_column_name(temporary_column.to_s)} = $1::bytea WHERE #{@connection.quote_column_name(temporary_column.to_s)} IS NULL",
          [dump_serialized({})],
        )
      end
      execute("ALTER TABLE #{table(table_name)} DROP COLUMN #{@connection.quote_column_name(column_name.to_s)}")
      execute("ALTER TABLE #{table(table_name)} RENAME COLUMN #{@connection.quote_column_name(temporary_column.to_s)} TO #{@connection.quote_column_name(column_name.to_s)}")
      execute("ALTER TABLE #{table(table_name)} ALTER COLUMN #{@connection.quote_column_name(column_name.to_s)} SET NOT NULL") if not_null
    end

    #: (untyped) -> untyped
    def primary_key_columns(table_name)
      execute_params(<<~SQL, [schema, table_name]).map { |row| row.fetch("column_name") }
        SELECT a.attname AS column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
      SQL
    end
  end
end
