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
          worker_pool text NOT NULL DEFAULT 'default',
          status text NOT NULL,
          input bytea NOT NULL,
          result bytea,
          error text,
          locked_by text,
          locked_until timestamptz,
          next_run_at timestamptz,
          runnable_immediately boolean NOT NULL DEFAULT true,
          cancel_reason text,
          cancel_requested_at timestamptz,
          cancel_delivered_at timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      SQL
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS worker_pool text NOT NULL DEFAULT 'default'")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS locked_by text")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS locked_until timestamptz")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS next_run_at timestamptz")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS runnable_immediately boolean NOT NULL DEFAULT true")
      execute("UPDATE #{table("workflows")} SET runnable_immediately = (next_run_at IS NULL) WHERE status = 'pending'")
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
          worker_pool text NOT NULL DEFAULT 'default',
          object_type text NOT NULL,
          object_id text NOT NULL,
          state bytea,
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (worker_pool, object_type, object_id)
        )
      SQL
      execute("ALTER TABLE #{table("durable_objects")} ADD COLUMN IF NOT EXISTS worker_pool text NOT NULL DEFAULT 'default'")
      ensure_worker_pool_primary_key!("durable_objects", ["worker_pool", "object_type", "object_id"])
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
      @migrated = true
      self
    end

    private

    #: () -> untyped
    def create_inbox_tables!
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("mailbox_sequences")} (
          worker_pool text NOT NULL DEFAULT 'default',
          target_kind text NOT NULL,
          target_type text NOT NULL,
          target_id text NOT NULL,
          last_sequence bigint NOT NULL DEFAULT 0,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (worker_pool, target_kind, target_type, target_id)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("inbox")} (
          id text PRIMARY KEY,
          worker_pool text NOT NULL DEFAULT 'default',
          target_kind text NOT NULL,
          target_type text NOT NULL,
          target_id text NOT NULL,
          sequence bigint NOT NULL,
          message_kind text NOT NULL,
          method_name text,
          operation_id text NOT NULL,
          idempotency_key text,
          idempotency_hash text,
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
          UNIQUE (worker_pool, target_kind, target_type, target_id, sequence)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("target_activations")} (
          worker_pool text NOT NULL DEFAULT 'default',
          target_kind text NOT NULL,
          target_type text NOT NULL,
          target_id text NOT NULL,
          status text NOT NULL,
          ready_at timestamptz NOT NULL,
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (worker_pool, target_kind, target_type, target_id)
        )
      SQL
      execute("ALTER TABLE #{table("mailbox_sequences")} ADD COLUMN IF NOT EXISTS worker_pool text NOT NULL DEFAULT 'default'")
      execute("ALTER TABLE #{table("inbox")} ADD COLUMN IF NOT EXISTS worker_pool text NOT NULL DEFAULT 'default'")
      execute("ALTER TABLE #{table("inbox")} ADD COLUMN IF NOT EXISTS idempotency_hash text")
      execute("ALTER TABLE #{table("target_activations")} ADD COLUMN IF NOT EXISTS worker_pool text NOT NULL DEFAULT 'default'")
      ensure_worker_pool_primary_key!("mailbox_sequences", ["worker_pool", "target_kind", "target_type", "target_id"])
      ensure_worker_pool_unique_key!("inbox", ["worker_pool", "target_kind", "target_type", "target_id", "sequence"])
      ensure_worker_pool_primary_key!("target_activations", ["worker_pool", "target_kind", "target_type", "target_id"])
      backfill_inbox_idempotency_hashes!
    end

    # Widen a legacy PRIMARY KEY so it includes worker_pool. Fresh installs already define the
    # worker-pool-scoped key in CREATE TABLE, so this is a no-op there; only schemas created before
    # worker_pool existed get rebuilt. Without this, ON CONFLICT (worker_pool, ...) upserts fail on
    # Postgres because no matching unique/primary constraint exists for the widened conflict target.
    #: (String, Array[String]) -> void
    def ensure_worker_pool_primary_key!(table_name, columns)
      ensure_worker_pool_key!(table_name, columns, contype: "p", definition_keyword: "PRIMARY KEY")
    end

    # Widen a legacy UNIQUE constraint so it includes worker_pool. Same fresh-install no-op behaviour
    # as the primary-key variant; guards against two worker pools colliding on the old narrower key.
    #: (String, Array[String]) -> void
    def ensure_worker_pool_unique_key!(table_name, columns)
      ensure_worker_pool_key!(table_name, columns, contype: "u", definition_keyword: "UNIQUE")
    end

    #: (String, Array[String], contype: String, definition_keyword: String) -> void
    def ensure_worker_pool_key!(table_name, columns, contype:, definition_keyword:)
      constraints = key_constraints_with_columns(table_name, contype:)
      desired = columns.join(",")
      return if constraints.any? { |row| row.fetch("columns") == desired }

      legacy = (columns - ["worker_pool"]).join(",")
      legacy_constraint = constraints.find { |row| row.fetch("columns") == legacy }
      if legacy_constraint
        execute("ALTER TABLE #{table(table_name)} DROP CONSTRAINT IF EXISTS #{quote_column_name(legacy_constraint.fetch("conname"))}")
      end
      column_list = columns.map { |column| quote_column_name(column) }.join(", ")
      execute("ALTER TABLE #{table(table_name)} ADD #{definition_keyword} (#{column_list})")
    end

    # Returns each PRIMARY KEY ('p') or UNIQUE ('u') constraint on the table with its ordered,
    # comma-joined column list, so the caller can match by exact column signature rather than name.
    #: (String, contype: String) -> Array[Hash[String, Object?]]
    def key_constraints_with_columns(table_name, contype:)
      # contype is a fixed internal value ('p' or 'u') supplied by this module, never user input, so
      # it is inlined as a "char" literal. Binding it as a text parameter trips Postgres' "char" = text
      # operator resolution.
      execute_params(<<~SQL, [schema.to_s, table_name]).to_a
        SELECT con.conname,
               (
                 SELECT string_agg(att.attname, ',' ORDER BY ck.ord)
                 FROM unnest(con.conkey) WITH ORDINALITY AS ck(attnum, ord)
                 JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ck.attnum
               ) AS columns
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2 AND con.contype = '#{contype}'
      SQL
    end

    #: () -> untyped
    def create_performance_indexes!
      create_postgres_index("workflows_queue_idx", "ON #{table("workflows")} (worker_pool ASC, status ASC, created_at ASC)")
      create_postgres_index("workflows_runnable_due_idx", "ON #{table("workflows")} (worker_pool ASC, status ASC, next_run_at ASC, created_at ASC)")
      create_postgres_index("workflows_expired_lease_idx", "ON #{table("workflows")} (worker_pool ASC, status ASC, locked_until ASC)")
      drop_postgres_index("workflows_pending_created_idx")
      create_postgres_index("workflows_pending_created_idx", "ON #{table("workflows")} (worker_pool ASC, status ASC, runnable_immediately ASC, created_at ASC)")
      create_postgres_index("workflows_failed_due_idx", "ON #{table("workflows")} (worker_pool ASC, next_run_at ASC, created_at ASC) WHERE status = 'failed'")
      create_postgres_index("workflows_canceling_created_idx", "ON #{table("workflows")} (worker_pool ASC, created_at ASC) WHERE status = 'canceling'")
      create_postgres_index("workflow_history_command_idx", "ON #{table("workflow_history")} (workflow_id, command_id, event_index)")
      create_postgres_index("waits_event_pending_idx", "ON #{table("waits")} (status ASC, kind ASC, event_key ASC, created_at ASC)")
      create_postgres_index("waits_timer_pending_idx", "ON #{table("waits")} (status ASC, kind ASC, wake_at ASC, created_at ASC)")
      create_postgres_index("waits_workflow_created_idx", "ON #{table("waits")} (workflow_id ASC, created_at ASC)")
      create_postgres_index("waits_workflow_status_idx", "ON #{table("waits")} (workflow_id ASC, status ASC)")
      create_postgres_index("step_attempts_workflow_started_position_idx", "ON #{table("step_attempts")} (workflow_id ASC, started_at ASC, position ASC)")
      create_postgres_index("step_attempts_workflow_position_status_started_idx", "ON #{table("step_attempts")} (workflow_id ASC, position ASC, status ASC, started_at DESC)")
      create_postgres_index("outbox_queue_idx", "ON #{table("outbox")} (status ASC, created_at ASC)")
      create_postgres_index("outbox_expired_lease_idx", "ON #{table("outbox")} (status ASC, locked_until ASC)")
      drop_postgres_index("workflows_worker_lease_idx")
      drop_postgres_index("outbox_worker_lease_idx")
      create_postgres_index("workflows_worker_lease_idx", "ON #{table("workflows")} (locked_by ASC) WHERE status = 'running'")
      create_postgres_index("outbox_worker_lease_idx", "ON #{table("outbox")} (status ASC, locked_by ASC)")
      create_postgres_index("inbox_worker_lease_idx", "ON #{table("inbox")} (status ASC, locked_by ASC)")
      create_postgres_index("target_activations_worker_lease_idx", "ON #{table("target_activations")} (status ASC, locked_by ASC)")
      execute("ALTER TABLE #{table("inbox")} DROP CONSTRAINT IF EXISTS inbox_idempotency_key_key")
      drop_postgres_index("inbox_target_idempotency_idx")
      create_postgres_index("inbox_idempotency_hash_idx", "ON #{table("inbox")} (idempotency_hash) WHERE idempotency_hash IS NOT NULL", unique: true)
      create_postgres_index("inbox_target_status_sequence_idx", "ON #{table("inbox")} (worker_pool, target_kind, target_type, target_id, status, sequence)")
      create_postgres_index("inbox_target_sequence_idx", "ON #{table("inbox")} (worker_pool, target_kind, target_type, target_id, sequence)")
      create_postgres_index("inbox_ready_idx", "ON #{table("inbox")} (worker_pool, status, ready_at, created_at)")
      create_postgres_index("target_activations_queue_idx", "ON #{table("target_activations")} (worker_pool, status, ready_at, created_at)")
      create_postgres_index("target_activations_expired_idx", "ON #{table("target_activations")} (worker_pool, status, locked_until, created_at)")
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
        execute_params("UPDATE #{table("inbox")} SET idempotency_hash = $1 WHERE id = $2", [hash, row.fetch("id")])
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

    #: (untyped, untyped, ?unique: bool) -> untyped
    def create_postgres_index(name, definition, unique: false)
      index_name = postgres_index_name(name)
      exists = execute_params(<<~SQL, [schema, index_name]).first
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'i'
        LIMIT 1
      SQL
      return if exists

      execute("SET search_path TO #{quoted_schema}")
      execute("CREATE #{"UNIQUE " if unique}INDEX IF NOT EXISTS #{quote_column_name(index_name)} #{definition}")
    ensure
      execute("RESET search_path")
    end

    #: (untyped) -> untyped
    def drop_postgres_index(name)
      [name.to_s, postgres_index_name(name)].uniq.each do |index_name|
        execute("DROP INDEX IF EXISTS #{quoted_schema}.#{quote_column_name(index_name)}")
      end
    end

    #: (untyped) -> untyped
    def postgres_index_name(name)
      logical = name.to_s
      max_identifier_length = 63
      return logical if schema.to_s == "public"
      return logical[-max_identifier_length..] if logical.length >= max_identifier_length

      prefix = schema.to_s.gsub(/[^A-Za-z0-9_]/, "_")
      prefix_length = max_identifier_length - logical.length - 1
      "#{prefix[0, prefix_length]}_#{logical}"
    end
  end
end
