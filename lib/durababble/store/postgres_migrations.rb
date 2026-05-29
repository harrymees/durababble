# typed: true
# frozen_string_literal: true

module Durababble
  module PostgresMigrations
    # Pre-production: we create every table in its final shape. There is no live data to migrate, so
    # the schema is declared once in CREATE TABLE rather than assembled through incremental ALTERs.
    # Secondary indexes are still created separately because Postgres cannot declare non-unique,
    # partial, or DESC indexes inline in CREATE TABLE.
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
          cancel_reason text,
          cancel_requested_at timestamptz,
          cancel_delivered_at timestamptz,
          child_origin_kind text,
          parent_workflow_id text,
          parent_command_id integer,
          parent_object_type text,
          parent_object_id text,
          parent_object_command_id text,
          child_cancellation_policy text,
          colocation_group_id text,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("colocation_groups")} (
          id text PRIMARY KEY,
          worker_pool text NOT NULL DEFAULT 'default',
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      SQL
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
          colocation_group_id text,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (object_type, object_id)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("object_wakeups")} (
          worker_pool text NOT NULL DEFAULT 'default',
          object_type text NOT NULL,
          object_id text NOT NULL,
          name text NOT NULL,
          wake_at timestamptz NOT NULL,
          payload bytea NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (worker_pool, object_type, object_id, name)
        )
      SQL
      create_inbox_tables!
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
          PRIMARY KEY (target_kind, target_type, target_id)
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
          UNIQUE (target_kind, target_type, target_id, sequence)
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
          PRIMARY KEY (target_kind, target_type, target_id)
        )
      SQL
    end

    # Postgres cannot declare these inline in CREATE TABLE (non-unique, partial, and DESC indexes all
    # require a separate statement), so the secondary indexes are created here in their final shape.
    #: () -> untyped
    def create_performance_indexes!
      create_postgres_index("workflows_claim_idx", "ON #{table("workflows")} (worker_pool ASC, (#{StoreQueries::POSTGRES_WORKFLOW_CLAIM_EXPRESSION}) ASC, created_at ASC)")
      create_postgres_index("workflows_expired_lease_idx", "ON #{table("workflows")} (worker_pool ASC, status ASC, locked_until ASC)")
      create_postgres_index("workflow_history_command_idx", "ON #{table("workflow_history")} (workflow_id, command_id, event_index)")
      create_postgres_index("object_wakeups_due_idx", "ON #{table("object_wakeups")} (wake_at ASC, created_at ASC)")
      create_postgres_index("step_attempts_workflow_started_position_idx", "ON #{table("step_attempts")} (workflow_id ASC, started_at ASC, position ASC)")
      create_postgres_index("step_attempts_workflow_position_status_started_idx", "ON #{table("step_attempts")} (workflow_id ASC, position ASC, status ASC, started_at DESC)")
      create_postgres_index("outbox_claim_idx", "ON #{table("outbox")} ((#{StoreQueries::POSTGRES_OUTBOX_CLAIM_EXPRESSION}) ASC, created_at ASC)")
      create_postgres_index("workflows_worker_lease_idx", "ON #{table("workflows")} (locked_by ASC) WHERE status = 'running'")
      create_postgres_index("outbox_worker_lease_idx", "ON #{table("outbox")} (status ASC, locked_by ASC)")
      create_postgres_index("inbox_worker_lease_idx", "ON #{table("inbox")} (status ASC, locked_by ASC)")
      create_postgres_index("target_activations_worker_lease_idx", "ON #{table("target_activations")} (status ASC, locked_by ASC)")
      create_postgres_index("inbox_idempotency_hash_idx", "ON #{table("inbox")} (idempotency_hash) WHERE idempotency_hash IS NOT NULL", unique: true)
      create_postgres_index("inbox_target_status_sequence_idx", "ON #{table("inbox")} (target_kind, target_type, target_id, status, sequence)")
      create_postgres_index("inbox_ready_idx", "ON #{table("inbox")} (status, ready_at, created_at)")
      create_postgres_index("target_activations_claim_idx", "ON #{table("target_activations")} (worker_pool ASC, target_kind ASC, target_type ASC, (#{StoreQueries::POSTGRES_TARGET_ACTIVATION_CLAIM_EXPRESSION}) ASC, created_at ASC)")
      create_postgres_index("durable_objects_worker_lease_idx", "ON #{table("durable_objects")} (locked_by) WHERE locked_by IS NOT NULL")
      create_postgres_index("durable_objects_expired_lease_idx", "ON #{table("durable_objects")} (locked_until) WHERE locked_by IS NOT NULL")
      create_postgres_index("colocation_groups_worker_lease_idx", "ON #{table("colocation_groups")} (locked_by) WHERE locked_by IS NOT NULL")
      create_postgres_index("colocation_groups_expired_lease_idx", "ON #{table("colocation_groups")} (locked_until) WHERE locked_by IS NOT NULL")
    end

    #: (untyped, untyped, ?unique: bool) -> untyped
    def create_postgres_index(name, definition, unique: false)
      index_name = postgres_index_name(name)
      legacy_index_name = legacy_postgres_index_name(name)
      exists = execute_params(<<~SQL, [schema, index_name, legacy_index_name]).first
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname IN ($2, $3) AND c.relkind = 'i'
        LIMIT 1
      SQL
      return if exists

      execute("CREATE #{"UNIQUE " if unique}INDEX IF NOT EXISTS #{quote_column_name(index_name)} #{definition}")
    end

    #: (untyped) -> untyped
    def postgres_index_name(name)
      name.to_s
    end

    #: (untyped) -> untyped
    def legacy_postgres_index_name(name)
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
