# typed: true
# frozen_string_literal: true

require "paquito"
require "json"
require "securerandom"
require "time"
require "uri"
require "digest"

module Durababble
  class Store
    SERIALIZED_COLUMNS = ["input", "result", "payload", "context", "heartbeat_cursor", "state", "args", "kwargs"].freeze
    SERIALIZER = Paquito::SingleBytePrefixVersion.new(1, 1 => Marshal)
    NO_OBJECT_STATE = Object.new.freeze

    #: untyped
    attr_reader :schema

    class << self
      #: (database_url: untyped, ?schema: untyped) -> untyped
      def connect(database_url:, schema: Durababble.default_schema)
        uri = URI.parse(database_url)
        if ["mysql", "mysql2", "trilogy"].include?(uri.scheme)
          require "trilogy"
          return MysqlStore.connect(uri:, schema:)
        end

        require "pg"
        new(PG.connect(database_url), schema:)
      end
    end

    #: (untyped, schema: untyped) -> void
    def initialize(connection, schema:)
      @connection = connection
      @schema = schema
      @migrated = false
    end

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

    #: () -> untyped
    def drop_schema!
      execute("DROP SCHEMA IF EXISTS #{quoted_schema} CASCADE")
      @migrated = false
    end

    #: () -> untyped
    def close
      @connection.close unless @connection.finished?
    end

    #: (name: untyped, input: untyped) -> untyped
    def enqueue_workflow(name:, input:)
      # [DURABABBLE-WF-1] The workflow row is durable before any claim path can run it.
      id = SecureRandom.uuid
      execute_params(
        "INSERT INTO #{table("workflows")} (id, name, status, input) VALUES ($1, $2, 'pending', $3::bytea)",
        [id, name, dump_serialized(input)],
      )
      id
    end

    #: (name: untyped, input: untyped) -> untyped
    def create_workflow(name:, input:)
      id = enqueue_workflow(name:, input:)
      mark_workflow_running(id)
      id
    end

    #: (worker_id: untyped, lease_seconds: untyped, ?workflow_names: untyped) -> untyped
    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil)
      return if workflow_names&.empty?

      # [DURABABBLE-LEASE-1] Queue selection and lease assignment are one locked update.
      name_filter = workflow_name_filter(workflow_names)
      row = retry_serialization_failures do
        transaction do
          candidates = []
          candidates.concat(execute_params(<<~SQL, []).to_a)
            SELECT id, created_at FROM #{table("workflows")}
            WHERE status = 'pending'
              AND (next_run_at IS NULL OR next_run_at <= now())
              #{name_filter}
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL
          candidates.concat(execute_params(<<~SQL, []).to_a)
            SELECT id, created_at FROM #{table("workflows")}
            WHERE status = 'failed'
              AND next_run_at IS NOT NULL
              AND next_run_at <= now()
              #{name_filter}
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL
          candidates.concat(execute_params(<<~SQL, []).to_a)
            SELECT id, created_at FROM #{table("workflows")}
            WHERE status = 'canceling'
              AND (next_run_at IS NULL OR next_run_at <= now())
              #{name_filter}
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL
          candidates.concat(execute_params(<<~SQL, []).to_a)
            SELECT id, created_at FROM #{table("workflows")}
            WHERE status = 'running' AND locked_until < now()
              #{name_filter}
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL

          candidate = candidates.min_by { |candidate_row| Time.parse(candidate_row.fetch("created_at")) }
          next nil unless candidate

          execute_params(<<~SQL, [candidate.fetch("id"), worker_id, lease_seconds]).first
            UPDATE #{table("workflows")}
            SET status = 'running', locked_by = $2, locked_until = now() + ($3::int * interval '1 second'), next_run_at = NULL, updated_at = now()
            WHERE id = $1
            RETURNING *
          SQL
        end
      end
      decode_row(row) if row
    end

    #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
    def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
      already_owned = execute_params(<<~SQL, [workflow_id, worker_id]).first
        SELECT * FROM #{table("workflows")}
        WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
      SQL
      return decode_row(already_owned) if already_owned

      row = execute_params(<<~SQL, [workflow_id, worker_id, lease_seconds]).first
        UPDATE #{table("workflows")}
        SET status = 'running', error = NULL, locked_by = $2,
            locked_until = now() + ($3::int * interval '1 second'), next_run_at = NULL, updated_at = now()
        WHERE id = $1
          AND (
            (status = 'pending' AND (next_run_at IS NULL OR next_run_at <= now()))
            OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= now())
            OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= now()))
            OR (status = 'running' AND (locked_by = $2 OR locked_until < now()))
          )
        RETURNING *
      SQL
      decode_row(row) if row
    end

    #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
    def heartbeat(workflow_id:, worker_id:, lease_seconds:)
      # [DURABABBLE-LEASE-2] Heartbeats extend only an unexpired lease held by this worker.
      execute_params(<<~SQL, [workflow_id, worker_id, lease_seconds])
        UPDATE #{table("workflows")}
        SET locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
        WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()
      SQL
    end

    #: (workflow_id: untyped, worker_id: untyped) -> untyped
    def workflow_owned?(workflow_id:, worker_id:)
      !!execute_params(<<~SQL, [workflow_id, worker_id]).first
        SELECT 1
        FROM #{table("workflows")}
        WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()
      SQL
    end

    #: (worker_id: untyped) -> untyped
    def release_worker_leases!(worker_id:)
      # [DURABABBLE-LEASE-3] Shutdown releases owned workflow/outbox leases back to pending.
      transaction do
        workflows = execute_params(<<~SQL, [worker_id]).cmd_tuples
          UPDATE #{table("workflows")}
          SET status = CASE
              WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
              ELSE 'pending'
            END,
            locked_by = NULL, locked_until = NULL, updated_at = now()
          WHERE status = 'running' AND locked_by = $1
        SQL
        outbox = execute_params(<<~SQL, [worker_id]).cmd_tuples
          UPDATE #{table("outbox")}
          SET status = 'pending', locked_by = NULL, locked_until = NULL
          WHERE status = 'processing' AND locked_by = $1
        SQL
        { "workflows" => workflows, "outbox" => outbox }
      end
    end

    #: (workflow_id: untyped, worker_id: untyped, run_at: untyped) -> untyped
    def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
      execute_params(<<~SQL, [workflow_id, worker_id, timestamp(run_at)])
        UPDATE #{table("workflows")}
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, next_run_at = $3::timestamptz, updated_at = now()
        WHERE id = $1 AND status = 'running' AND locked_by = $2
      SQL
    end

    #: (workflow_id: untyped, ?worker_id: untyped) -> untyped
    def suspend_workflow(workflow_id:, worker_id: nil)
      params = [workflow_id]
      owner_filter = ""
      if worker_id
        params << worker_id
        owner_filter = "AND locked_by = $2"
      end
      result = execute_params(<<~SQL, params)
        UPDATE #{table("workflows")}
        SET status = CASE
              WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
              WHEN EXISTS (SELECT 1 FROM #{table("waits")} WHERE workflow_id = $1 AND status = 'pending') THEN 'waiting'
              ELSE 'pending'
            END,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = now()
        WHERE id = $1 AND status = 'running' #{owner_filter}
      SQL
      return true if result.cmd_tuples == 1

      ["pending", "waiting", "canceling"].include?(workflow(workflow_id).fetch("status"))
    end

    #: (untyped, ?now: untyped) -> untyped
    def make_workflow_due!(workflow_id, now: Time.now)
      execute_params("UPDATE #{table("workflows")} SET next_run_at = NULL, updated_at = $2::timestamptz WHERE id = $1", [workflow_id, timestamp(now)])
    end

    #: (workflow_id: untyped, reason: untyped) -> untyped
    def request_workflow_cancellation(workflow_id:, reason:)
      transaction do
        row = execute_params("SELECT * FROM #{table("workflows")} WHERE id = $1 FOR UPDATE", [workflow_id]).first
        raise KeyError, "workflow not found: #{workflow_id}" unless row

        decoded = decode_row(row)
        next decoded if terminal_for_cancellation?(decoded)

        first_request = row["cancel_requested_at"].nil?
        if first_request
          execute_params(<<~SQL, [workflow_id, reason])
            UPDATE #{table("workflows")}
            SET cancel_reason = $2, cancel_requested_at = now(), updated_at = now()
            WHERE id = $1
          SQL
        end
        cancel_pending_waits_for_workflow(workflow_id) if first_request

        if first_request && decoded.fetch("status") != "running"
          execute_params(<<~SQL, [workflow_id])
            UPDATE #{table("workflows")}
            SET status = 'canceling', locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now()
            WHERE id = $1 AND status NOT IN ('completed', 'canceled')
          SQL
        end

        workflow(workflow_id)
      end
    end

    #: (untyped) -> untyped
    def workflow_cancellation(workflow_id)
      row = execute_params(<<~SQL, [workflow_id]).first
        SELECT id AS workflow_id, cancel_reason AS reason,
          cancel_requested_at AS requested_at, cancel_delivered_at AS delivered_at
        FROM #{table("workflows")}
        WHERE id = $1 AND cancel_requested_at IS NOT NULL
      SQL
      row&.transform_values(&:itself)
    end

    #: (workflow_id: untyped) -> untyped
    def mark_workflow_cancellation_delivered(workflow_id:)
      execute_params(<<~SQL, [workflow_id])
        UPDATE #{table("workflows")}
        SET cancel_delivered_at = COALESCE(cancel_delivered_at, now()), updated_at = now()
        WHERE id = $1 AND cancel_requested_at IS NOT NULL
      SQL
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, worker_id: untyped, lease_seconds: untyped, cursor: untyped) -> untyped
    def heartbeat_step(workflow_id:, worker_id:, lease_seconds:, cursor:, command_id: nil, position: nil)
      # [DURABABBLE-LEASE-2] Step heartbeat stores progress only while the workflow lease is live.
      command_id = normalize_command_id(command_id, position)
      renewed = transaction do
        workflow = execute_params(<<~SQL, [workflow_id, worker_id, lease_seconds]).first
          UPDATE #{table("workflows")}
          SET locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
          WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()
          RETURNING locked_until
        SQL
        next nil unless workflow

        serialized_cursor = dump_serialized(cursor)
        step = execute_params(<<~SQL, [workflow_id, command_id, serialized_cursor]).first
          UPDATE #{table("steps")}
          SET heartbeat_cursor = $3::bytea, updated_at = now()
          WHERE workflow_id = $1 AND position = $2 AND status = 'running'
          RETURNING heartbeat_cursor
        SQL
        next nil unless step

        execute_params(<<~SQL, [workflow_id, command_id, serialized_cursor])
          UPDATE #{table("step_attempts")}
          SET heartbeat_cursor = $3::bytea
          WHERE id = (
            SELECT id FROM #{table("step_attempts")}
            WHERE workflow_id = $1 AND position = $2 AND status = 'running'
            ORDER BY started_at DESC
            LIMIT 1
          )
        SQL
        workflow
      end
      renewed&.fetch("locked_until")
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped) -> untyped
    def step_heartbeat_cursor(workflow_id:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      row = execute_params("SELECT heartbeat_cursor FROM #{table("steps")} WHERE workflow_id = $1 AND position = $2", [workflow_id, command_id]).first
      decode_row(row).fetch("heartbeat_cursor") if row
    end

    #: (untyped) -> untyped
    def current_workflow_lease(workflow_id)
      row = execute_params(<<~SQL, [workflow_id]).first
        SELECT id AS workflow_id, locked_by AS worker_id, locked_until
        FROM #{table("workflows")}
        WHERE id = $1 AND status = 'running' AND locked_by IS NOT NULL AND locked_until >= now()
      SQL
      row&.transform_values(&:itself)
    end

    #: (?now: untyped) -> untyped
    def steal_expired_leases!(now: Time.now)
      # [DURABABBLE-LEASE-3] Expired workflow leases are reclaimed durably for another owner.
      result = execute_params(<<~SQL, [timestamp(now)])
        UPDATE #{table("workflows")}
        SET status = CASE
              WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
              ELSE 'pending'
            END,
            locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE status = 'running' AND locked_until < $1::timestamptz
      SQL
      result.cmd_tuples
    end

    #: (untyped, ?worker_id: untyped, ?lease_seconds: untyped) -> untyped
    def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60)
      if worker_id
        claim_workflow(workflow_id:, worker_id:, lease_seconds:)
      else
        execute_params(<<~SQL, [workflow_id])
          UPDATE #{table("workflows")}
          SET status = 'running', error = NULL, updated_at = now()
          WHERE id = $1
        SQL
      end
    end

    #: (untyped, result: untyped) -> untyped
    def complete_workflow(workflow_id, result:)
      execute_params(
        "UPDATE #{table("workflows")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1",
        [workflow_id, dump_serialized(result)],
      )
    end

    #: (untyped, reason: untyped, ?result: untyped) -> untyped
    def cancel_workflow(workflow_id, reason:, result: nil)
      execute_params(
        "UPDATE #{table("workflows")} SET status = 'canceled', result = $2::bytea, error = $3, cancel_reason = COALESCE(cancel_reason, $3), cancel_requested_at = COALESCE(cancel_requested_at, now()), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1",
        [workflow_id, dump_serialized(result), reason],
      )
    end

    #: (untyped, error: untyped) -> untyped
    def fail_workflow(workflow_id, error:)
      transaction do
        execute_params(
          "UPDATE #{table("steps")} SET status = 'failed', error = $2, updated_at = now() WHERE workflow_id = $1 AND status = 'running'",
          [workflow_id, error],
        )
        execute_params(
          "UPDATE #{table("step_attempts")} SET status = 'failed', error = $2, completed_at = now() WHERE workflow_id = $1 AND status = 'running'",
          [workflow_id, error],
        )
        execute_params(
          "UPDATE #{table("workflows")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1",
          [workflow_id, error],
        )
      end
    end

    #: (workflow_id: untyped, command_id: untyped, name: untyped, ?args: untyped, ?kwargs: untyped, ?metadata: untyped) -> untyped
    def record_step_scheduled(workflow_id:, command_id:, name:, args: [], kwargs: {}, metadata: {})
      payload = { "name" => name, "args" => args, "kwargs" => kwargs }.merge(metadata)
      transaction do
        # [DURABABBLE-CONCURRENCY-1] Scheduled command history is the replay contract for concurrent workflow fibers.
        append_workflow_history_without_transaction(workflow_id:, kind: "step_scheduled", command_id:, name:, payload:)
        execute_params(<<~SQL, [workflow_id, command_id, name])
          INSERT INTO #{table("steps")} (workflow_id, position, name, status, updated_at)
          VALUES ($1, $2, $3, 'scheduled', now())
          ON CONFLICT (workflow_id, position) DO NOTHING
        SQL
      end
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, name: untyped) -> untyped
    def record_step_started(workflow_id:, name:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        # [DURABABBLE-STEP-2] Recovery appends a retry attempt and closes stale running attempts.
        execute_params(<<~SQL, [workflow_id, command_id])
          UPDATE #{table("step_attempts")}
          SET status = 'failed', error = 'superseded by retry', completed_at = now()
          WHERE workflow_id = $1 AND position = $2 AND status = 'running'
        SQL
        execute_params(<<~SQL, [workflow_id, command_id, name])
          INSERT INTO #{table("steps")} (workflow_id, position, name, status, started_at, updated_at)
          VALUES ($1, $2, $3, 'running', now(), now())
          ON CONFLICT (workflow_id, position) DO UPDATE
            SET status = 'running', error = NULL, started_at = COALESCE(#{table("steps")}.started_at, now()), updated_at = now()
        SQL
        attempt_id = SecureRandom.uuid
        execute_params(<<~SQL, [attempt_id, workflow_id, command_id, name])
          INSERT INTO #{table("step_attempts")} (id, workflow_id, position, name, status)
          VALUES ($1, $2, $3, $4, 'running')
        SQL
        append_workflow_history_without_transaction(workflow_id:, kind: "step_started", command_id:, name:, attempt_id:)
        attempt_id
      end
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, result: untyped) -> untyped
    def record_step_completed(workflow_id:, result:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction { record_step_completed_without_transaction(workflow_id:, command_id:, result:) }
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped) -> untyped
    def record_step_failed(workflow_id:, error:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction { record_step_failed_without_transaction(workflow_id:, command_id:, error:) }
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped) -> untyped
    def record_step_canceled(workflow_id:, error:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        execute_params(
          "UPDATE #{table("steps")} SET status = 'canceled', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2 AND status IN ('scheduled', 'running', 'waiting')",
          [workflow_id, command_id, error],
        )
        update_latest_attempt_serialized(workflow_id:, command_id:, status: "canceled", serialized_result: dump_serialized(nil), error:)
        append_workflow_history_without_transaction(workflow_id:, kind: "step_canceled", command_id:, error:)
      end
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, name: untyped, wait_request: untyped, ?suspend_workflow: untyped) -> untyped
    def record_wait(workflow_id:, name:, wait_request:, command_id: nil, position: nil, suspend_workflow: true)
      command_id = normalize_command_id(command_id, position)
      transaction do
        execute_params(<<~SQL, [workflow_id, command_id, name, dump_serialized(wait_request.context)])
          INSERT INTO #{table("steps")} (workflow_id, position, name, status, result, started_at, updated_at)
          VALUES ($1, $2, $3, 'waiting', $4::bytea, now(), now())
          ON CONFLICT (workflow_id, position) DO UPDATE
            SET status = 'waiting', result = $4::bytea, error = NULL, updated_at = now()
        SQL
        wait_id = SecureRandom.uuid
        execute_params(<<~SQL, [wait_id, workflow_id, command_id, wait_request.kind, wait_request.event_key, timestamp_or_nil(wait_request.wake_at), dump_serialized(wait_request.context)])
          INSERT INTO #{table("waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status)
          VALUES ($1, $2, $3, $4, $5, $6::timestamptz, $7::bytea, 'pending')
        SQL
        update_latest_attempt(workflow_id:, command_id:, status: "waiting", result: wait_request.context, error: nil)
        append_workflow_history_without_transaction(workflow_id:, kind: "step_waiting", command_id:, name:, payload: wait_request.context)
        suspend_workflow(workflow_id:) if suspend_workflow
        wait_id
      end
    end

    #: (untyped) -> untyped
    def workflow_history_for(workflow_id)
      execute_params("SELECT * FROM #{table("workflow_history")} WHERE workflow_id = $1 ORDER BY event_index", [workflow_id]).map { |row| decode_row(row) }
    end

    #: (?now: untyped) -> untyped
    def wake_due_timers(now: Time.now)
      complete_waits("kind = 'timer' AND wake_at <= $1::timestamptz", [timestamp(now)], {})
    end

    #: (untyped, ?payload: untyped) -> untyped
    def signal_event(event_key, payload: {})
      complete_waits("kind = 'event' AND event_key = $1", [event_key], payload)
    end

    #: (untyped) -> untyped
    def waits_for(workflow_id)
      execute_params("SELECT * FROM #{table("waits")} WHERE workflow_id = $1 ORDER BY created_at", [workflow_id]).map { |row| decode_row(row) }
    end

    #: (workflow_id: untyped, key: untyped, ?poll_interval: untyped, ?timeout: untyped) { (?) -> untyped } -> untyped
    def with_fence(workflow_id:, key:, poll_interval: 0.05, timeout: 10, &block)
      # [DURABABBLE-FENCE-1] The fence row is inserted before yielding to the side effect.
      token = SecureRandom.uuid
      inserted = execute_params(<<~SQL, [workflow_id, key, token, timeout])
        INSERT INTO #{table("fences")} (workflow_id, key, status, locked_by, locked_until)
        VALUES ($1, $2, 'running', $3, now() + ($4::int * interval '1 second'))
        ON CONFLICT (workflow_id, key) DO NOTHING
      SQL

      if inserted.cmd_tuples == 1
        begin
          result = block.call
          execute_params(<<~SQL, [workflow_id, key, token, dump_serialized(result)])
            UPDATE #{table("fences")}
            SET status = 'completed', result = $4::bytea, error = NULL, completed_at = now()
            WHERE workflow_id = $1 AND key = $2 AND locked_by = $3
          SQL
          return result
        rescue StandardError => e
          execute_params(<<~SQL, [workflow_id, key, token, "#{e.class}: #{e.message}"])
            UPDATE #{table("fences")}
            SET status = 'failed', error = $4, completed_at = now()
            WHERE workflow_id = $1 AND key = $2 AND locked_by = $3
          SQL
          raise
        end
      end

      deadline = Time.now + timeout
      loop do
        row = execute_params("SELECT status, result, error FROM #{table("fences")} WHERE workflow_id = $1 AND key = $2", [workflow_id, key]).first
        decoded = decode_row(row) if row
        case decoded&.fetch("status")
        when "completed"
          return decoded.fetch("result")
        when "failed"
          raise Error, decoded.fetch("error")
        end
        raise FenceTimeout, "timed out waiting for fence #{key}" if Time.now >= deadline

        sleep(poll_interval)
      end
    end

    #: (workflow_id: untyped, topic: untyped, payload: untyped, key: untyped) -> untyped
    def enqueue_outbox(workflow_id:, topic:, payload:, key:)
      # [DURABABBLE-OUTBOX-1] Message keys dedupe producer retries to one durable row.
      existing = execute_params("SELECT id FROM #{table("outbox")} WHERE key = $1", [key]).first
      return existing.fetch("id") if existing

      id = SecureRandom.uuid
      execute_params(<<~SQL, [id, workflow_id, topic, dump_serialized(payload), key])
        INSERT INTO #{table("outbox")} (id, workflow_id, topic, payload, key, status)
        VALUES ($1, $2, $3, $4::bytea, $5, 'pending')
        ON CONFLICT (key) DO NOTHING
      SQL
      execute_params("SELECT id FROM #{table("outbox")} WHERE key = $1", [key]).first.fetch("id")
    end

    #: (worker_id: untyped, lease_seconds: untyped) -> untyped
    def claim_outbox(worker_id:, lease_seconds:)
      row = retry_serialization_failures do
        transaction do
          candidates = []
          candidates.concat(execute_params(<<~SQL, []).to_a)
            SELECT id, created_at FROM #{table("outbox")}
            WHERE status = 'pending'
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL
          candidates.concat(execute_params(<<~SQL, []).to_a)
            SELECT id, created_at FROM #{table("outbox")}
            WHERE status = 'processing' AND locked_until < now()
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL

          candidate = candidates.min_by { |candidate_row| Time.parse(candidate_row.fetch("created_at")) }
          next nil unless candidate

          execute_params(<<~SQL, [candidate.fetch("id"), worker_id, lease_seconds]).first
            UPDATE #{table("outbox")}
            SET status = 'processing', locked_by = $2, locked_until = now() + ($3::int * interval '1 second')
            WHERE id = $1
            RETURNING *
          SQL
        end
      end
      decode_row(row) if row
    end

    #: (untyped, worker_id: untyped) -> untyped
    def ack_outbox(outbox_id, worker_id:)
      # [DURABABBLE-OUTBOX-1] Only the sender that owns the processing lease can ack.
      execute_params(
        "UPDATE #{table("outbox")} SET status = 'processed', processed_at = now() WHERE id = $1 AND status = 'processing' AND locked_by = $2 AND locked_until >= now()",
        [outbox_id, worker_id],
      )
    end

    #: (untyped) -> untyped
    def outbox_message(outbox_id)
      decode_row(execute_params("SELECT * FROM #{table("outbox")} WHERE id = $1", [outbox_id]).first)
    end

    #: (untyped) -> untyped
    def workflow(workflow_id)
      result = execute_params("SELECT * FROM #{table("workflows")} WHERE id = $1", [workflow_id])
      row = result.first
      raise KeyError, "workflow not found: #{workflow_id}" unless row

      decode_row(row)
    end

    #: (untyped) -> untyped
    def steps_for(workflow_id)
      execute_params("SELECT * FROM #{table("steps")} WHERE workflow_id = $1 ORDER BY position", [workflow_id]).map { |row| with_command_id(decode_row(row)) }
    end

    #: (untyped) -> untyped
    def step_attempts_for(workflow_id)
      execute_params("SELECT * FROM #{table("step_attempts")} WHERE workflow_id = $1 ORDER BY started_at, position", [workflow_id]).map { |row| with_command_id(decode_row(row)) }
    end

    #: (object_type: untyped, object_id: untyped) -> untyped
    def object_state(object_type:, object_id:)
      row = execute_params("SELECT state FROM #{table("durable_objects")} WHERE object_type = $1 AND object_id = $2", [object_type, object_id]).first
      decode_row(row)&.fetch("state") if row
    end

    #: (object_type: untyped, object_id: untyped, state: untyped) -> untyped
    def save_object_state(object_type:, object_id:, state:)
      execute_params(<<~SQL, [object_type, object_id, dump_serialized(state)])
        INSERT INTO #{table("durable_objects")} (object_type, object_id, state)
        VALUES ($1, $2, $3::bytea)
        ON CONFLICT (object_type, object_id) DO UPDATE
          SET state = $3::bytea, updated_at = now()
      SQL
      state
    end

    #: (object_type: untyped, object_id: untyped, method_name: untyped, args: untyped, kwargs: untyped) -> untyped
    def enqueue_object_command(object_type:, object_id:, method_name:, args:, kwargs:)
      # [DURABABBLE-OBJ-1] Commands are persisted by target identity before execution.
      id = SecureRandom.uuid
      transaction do
        execute_params(<<~SQL, [object_type, object_id])
          INSERT INTO #{table("durable_objects")} (object_type, object_id, state)
          VALUES ($1, $2, NULL)
          ON CONFLICT (object_type, object_id) DO NOTHING
        SQL
        execute_params(<<~SQL, [id, object_type, object_id, method_name, dump_serialized(args), dump_serialized(kwargs)])
          INSERT INTO #{table("durable_object_commands")} (id, object_type, object_id, method_name, args, kwargs, status)
          VALUES ($1, $2, $3, $4, $5::bytea, $6::bytea, 'pending')
        SQL
      end
      id
    end

    #: (command_id: untyped, worker_id: untyped, ?lease_seconds: untyped) -> untyped
    def claim_object_command(command_id:, worker_id:, lease_seconds: 60)
      row = retry_serialization_failures do
        transaction do
          command = execute_params(<<~SQL, [command_id]).first
            SELECT * FROM #{table("durable_object_commands")}
            WHERE id = $1 AND (status IN ('pending', 'failed') OR (status = 'running' AND locked_until < now()))
            FOR UPDATE SKIP LOCKED
          SQL
          next nil unless command

          object_type = command.fetch("object_type")
          object_id = command.fetch("object_id")
          execute_params(<<~SQL, [object_type, object_id])
            INSERT INTO #{table("durable_objects")} (object_type, object_id, state)
            VALUES ($1, $2, NULL)
            ON CONFLICT (object_type, object_id) DO NOTHING
          SQL
          execute_params(<<~SQL, [object_type, object_id]).first
            SELECT 1 FROM #{table("durable_objects")}
            WHERE object_type = $1 AND object_id = $2
            FOR UPDATE
          SQL
          active = execute_params(<<~SQL, [object_type, object_id, command_id]).first
            SELECT 1 FROM #{table("durable_object_commands")}
            WHERE object_type = $1 AND object_id = $2 AND id <> $3
              AND status = 'running' AND locked_until >= now()
            LIMIT 1
          SQL
          next nil if active

          execute_params(<<~SQL, [command_id, worker_id, lease_seconds]).first
            UPDATE #{table("durable_object_commands")}
            SET status = 'running', locked_by = $2, locked_until = now() + ($3::int * interval '1 second')
            WHERE id = $1
            RETURNING *
          SQL
        end
      end
      decode_row(row) if row
    end

    #: (command_id: untyped, result: untyped, ?object_type: untyped, ?object_id: untyped, ?state: untyped, ?worker_id: untyped) -> untyped
    def complete_object_command(command_id:, result:, object_type: nil, object_id: nil, state: NO_OBJECT_STATE, worker_id: nil)
      transaction do
        # [DURABABBLE-OBJ-1] Completion and state update require the command lease.
        command = lock_object_command_for_completion(command_id:, worker_id:)
        next nil unless command

        save_object_state(object_type:, object_id:, state:) unless state.equal?(NO_OBJECT_STATE)
        execute_params(
          "UPDATE #{table("durable_object_commands")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = now() WHERE id = $1",
          [command_id, dump_serialized(result)],
        )
      end
    end

    #: (command_id: untyped, error: untyped, ?worker_id: untyped) -> untyped
    def fail_object_command(command_id:, error:, worker_id: nil)
      if worker_id
        execute_params(
          "UPDATE #{table("durable_object_commands")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL WHERE id = $1 AND status = 'running' AND locked_by = $3 AND locked_until >= now()",
          [command_id, error, worker_id],
        )
      else
        execute_params(
          "UPDATE #{table("durable_object_commands")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL WHERE id = $1",
          [command_id, error],
        )
      end
    end

    private

    #: (untyped) -> bool
    def terminal_for_cancellation?(row)
      return true if ["completed", "canceled"].include?(row.fetch("status"))

      row.fetch("status") == "failed" && row["next_run_at"].nil?
    end

    #: (untyped) -> untyped
    def cancel_pending_waits_for_workflow(workflow_id)
      execute_params(<<~SQL, [workflow_id])
        UPDATE #{table("waits")}
        SET status = 'canceled', completed_at = now()
        WHERE workflow_id = $1 AND status = 'pending'
      SQL
      execute_params(<<~SQL, [workflow_id])
        UPDATE #{table("steps")}
        SET status = 'canceled', error = 'workflow cancellation requested', updated_at = now()
        WHERE workflow_id = $1 AND status = 'waiting'
      SQL
      execute_params(<<~SQL, [workflow_id])
        UPDATE #{table("step_attempts")}
        SET status = 'canceled', error = 'workflow cancellation requested', completed_at = now()
        WHERE workflow_id = $1 AND status = 'waiting'
      SQL
    end

    #: (command_id: untyped, worker_id: untyped) -> untyped
    def lock_object_command_for_completion(command_id:, worker_id:)
      if worker_id
        execute_params(<<~SQL, [command_id, worker_id]).first
          SELECT 1 FROM #{table("durable_object_commands")}
          WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
          FOR UPDATE
        SQL
      else
        execute_params("SELECT 1 FROM #{table("durable_object_commands")} WHERE id = $1 FOR UPDATE", [command_id]).first
      end
    end

    #: (untyped, untyped, untyped) -> untyped
    def complete_waits(where_sql, params, payload)
      transaction do
        # [DURABABBLE-WAIT-1] Locked wait completion makes concurrent wakeups observe one winner.
        returning = execute_params(<<~SQL, params + [dump_serialized(payload)])
          UPDATE #{table("waits")}
          SET status = 'completed', payload = $#{params.length + 1}::bytea, completed_at = now()
          WHERE id IN (
            SELECT w.id FROM #{table("waits")} AS w
            JOIN #{table("workflows")} AS wf ON wf.id = w.workflow_id
            WHERE w.status = 'pending'
              AND wf.status IN ('waiting', 'running')
              AND #{where_sql}
            FOR UPDATE OF w, wf SKIP LOCKED
          )
          RETURNING *
        SQL
        rows = returning.map { |row| decode_row(row) }
        rows.each do |wait|
          context = wait.fetch("context").merge(payload)
          record_step_completed_without_transaction(workflow_id: wait.fetch("workflow_id"), command_id: wait.fetch("position").to_i, result: context)
          execute_params("UPDATE #{table("workflows")} SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id = $1 AND status = 'waiting'", [wait.fetch("workflow_id")])
        end
        rows.length
      end
    end

    #: (workflow_id: untyped, command_id: untyped, result: untyped) -> untyped
    def record_step_completed_without_transaction(workflow_id:, command_id:, result:)
      serialized = dump_serialized(result)
      execute_params(
        "UPDATE #{table("steps")} SET status = 'completed', result = $3::bytea, error = NULL, completed_at = now(), updated_at = now() WHERE workflow_id = $1 AND position = $2",
        [workflow_id, command_id, serialized],
      )
      update_latest_attempt_serialized(workflow_id:, command_id:, status: "completed", serialized_result: serialized, error: nil)
      append_workflow_history_without_transaction(workflow_id:, kind: "step_completed", command_id:, payload: result)
    end

    #: (workflow_id: untyped, command_id: untyped, error: untyped) -> untyped
    def record_step_failed_without_transaction(workflow_id:, command_id:, error:)
      execute_params(
        "UPDATE #{table("steps")} SET status = 'failed', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2",
        [workflow_id, command_id, error],
      )
      update_latest_attempt_serialized(workflow_id:, command_id:, status: "failed", serialized_result: dump_serialized(nil), error:)
      append_workflow_history_without_transaction(workflow_id:, kind: "step_failed", command_id:, error:)
    end

    #: (workflow_id: untyped, command_id: untyped, status: untyped, result: untyped, error: untyped) -> untyped
    def update_latest_attempt(workflow_id:, command_id:, status:, result:, error:)
      update_latest_attempt_serialized(workflow_id:, command_id:, status:, serialized_result: dump_serialized(result), error:)
    end

    #: (workflow_id: untyped, command_id: untyped, status: untyped, serialized_result: untyped, error: untyped) -> untyped
    def update_latest_attempt_serialized(workflow_id:, command_id:, status:, serialized_result:, error:)
      execute_params(<<~SQL, [workflow_id, command_id, status, serialized_result, error])
        UPDATE #{table("step_attempts")}
        SET status = $3, result = $4::bytea, error = $5, completed_at = now()
        WHERE id = (
          SELECT id FROM #{table("step_attempts")}
          WHERE workflow_id = $1 AND position = $2 AND status IN ('running', 'waiting')
          ORDER BY started_at DESC
          LIMIT 1
        )
      SQL
    end

    #: (workflow_id: untyped, kind: untyped, ?command_id: untyped, ?name: untyped, ?attempt_id: untyped, ?payload: untyped, ?error: untyped) -> untyped
    def append_workflow_history_without_transaction(workflow_id:, kind:, command_id: nil, name: nil, attempt_id: nil, payload: nil, error: nil)
      execute_params("SELECT id FROM #{table("workflows")} WHERE id = $1 FOR UPDATE", [workflow_id])
      event_index = execute_params(
        "SELECT COALESCE(MAX(event_index), -1) + 1 AS event_index FROM #{table("workflow_history")} WHERE workflow_id = $1",
        [workflow_id],
      ).first.fetch("event_index").to_i
      execute_params(<<~SQL, [workflow_id, event_index, kind, command_id, name, attempt_id, dump_serialized(payload), error])
        INSERT INTO #{table("workflow_history")} (workflow_id, event_index, kind, command_id, name, attempt_id, payload, error)
        VALUES ($1, $2, $3, $4, $5, $6, $7::bytea, $8)
      SQL
      event_index
    end

    #: (untyped, untyped) -> untyped
    def normalize_command_id(command_id, position)
      id = command_id.nil? ? position : command_id
      raise ArgumentError, "command_id is required" if id.nil?

      id
    end

    #: (?max_attempts: untyped) { (?) -> untyped } -> untyped
    def retry_serialization_failures(max_attempts: 5, &block)
      attempts = 0
      begin
        block.call
      rescue PG::TRSerializationFailure
        attempts += 1
        raise if attempts >= max_attempts

        sleep(0.001 * attempts)
        retry
      end
    end

    #: (untyped) -> untyped
    def execute(sql)
      attempts = 0
      begin
        @connection.exec(sql)
      rescue PG::TRSerializationFailure, PG::TRDeadlockDetected
        attempts += 1
        raise if attempts >= 5

        sleep(0.01 * attempts)
        retry
      end
    end

    #: () { (?) -> untyped } -> untyped
    def transaction(&block)
      @connection.transaction(&block)
    end

    #: (untyped, untyped) -> untyped
    def execute_params(sql, params)
      @connection.exec_params(sql, params)
    end

    #: (untyped) -> untyped
    def workflow_name_filter(workflow_names)
      return "" unless workflow_names

      names = workflow_names.map { |name| @connection.escape_literal(name) }.join(", ")
      "AND name IN (#{names})"
    end

    #: (untyped) -> untyped
    def table(name)
      "#{quoted_schema}.#{PG::Connection.quote_ident(name)}"
    end

    #: () -> untyped
    def quoted_schema
      PG::Connection.quote_ident(schema)
    end

    #: (untyped) -> untyped
    def dump_serialized(value)
      "\\x#{SERIALIZER.dump(value).unpack1("H*")}"
    end

    #: (untyped) -> untyped
    def load_serialized(value)
      return if value.nil?

      SERIALIZER.load(PG::Connection.unescape_bytea(value))
    end

    #: (untyped) -> untyped
    def decode_row(row)
      row.each_with_object({}) do |(column, value), decoded|
        decoded[column] = SERIALIZED_COLUMNS.include?(column) ? load_serialized(value) : value
      end
    end

    #: (untyped) -> untyped
    def with_command_id(row)
      row["command_id"] = row["position"] if row.key?("position") && !row.key?("command_id")
      row
    end

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
    end

    #: () -> untyped
    def create_performance_indexes!
      execute("CREATE INDEX IF NOT EXISTS workflows_queue_idx ON #{table("workflows")} (status, created_at)")
      execute("CREATE INDEX IF NOT EXISTS workflows_runnable_due_idx ON #{table("workflows")} (status, next_run_at, created_at)")
      execute("CREATE INDEX IF NOT EXISTS workflows_expired_lease_idx ON #{table("workflows")} (status, locked_until)")
      execute("CREATE INDEX IF NOT EXISTS workflow_history_command_idx ON #{table("workflow_history")} (workflow_id, command_id, event_index)")
      execute("CREATE INDEX IF NOT EXISTS waits_event_pending_idx ON #{table("waits")} (status, kind, event_key, created_at)")
      execute("CREATE INDEX IF NOT EXISTS waits_timer_pending_idx ON #{table("waits")} (status, kind, wake_at, created_at)")
      execute("CREATE INDEX IF NOT EXISTS waits_workflow_created_idx ON #{table("waits")} (workflow_id, created_at)")
      execute("CREATE INDEX IF NOT EXISTS step_attempts_workflow_started_position_idx ON #{table("step_attempts")} (workflow_id, started_at, position)")
      execute("CREATE INDEX IF NOT EXISTS step_attempts_workflow_position_status_started_idx ON #{table("step_attempts")} (workflow_id, position, status, started_at DESC)")
      execute("CREATE INDEX IF NOT EXISTS outbox_queue_idx ON #{table("outbox")} (status, created_at)")
      execute("CREATE INDEX IF NOT EXISTS outbox_expired_lease_idx ON #{table("outbox")} (status, locked_until)")
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
      execute("ALTER TABLE #{table(table_name)} ADD COLUMN IF NOT EXISTS #{PG::Connection.quote_ident(temporary_column)} bytea")
      rows = execute("SELECT * FROM #{table(table_name)}")
      primary_keys = primary_key_columns(table_name)
      rows.each do |row|
        raw = row[column_name]
        value = raw.nil? ? nil : JSON.parse(raw)
        execute_params(<<~SQL, primary_keys.map { |key| row.fetch(key) } + [dump_serialized(value)])
          UPDATE #{table(table_name)}
          SET #{PG::Connection.quote_ident(temporary_column)} = $#{primary_keys.length + 1}::bytea
          WHERE #{primary_keys.each_with_index.map { |key, index| "#{PG::Connection.quote_ident(key)} = $#{index + 1}" }.join(" AND ")}
        SQL
      end
      if not_null
        execute_params(
          "UPDATE #{table(table_name)} SET #{PG::Connection.quote_ident(temporary_column)} = $1::bytea WHERE #{PG::Connection.quote_ident(temporary_column)} IS NULL",
          [dump_serialized({})],
        )
      end
      execute("ALTER TABLE #{table(table_name)} DROP COLUMN #{PG::Connection.quote_ident(column_name)}")
      execute("ALTER TABLE #{table(table_name)} RENAME COLUMN #{PG::Connection.quote_ident(temporary_column)} TO #{PG::Connection.quote_ident(column_name)}")
      execute("ALTER TABLE #{table(table_name)} ALTER COLUMN #{PG::Connection.quote_ident(column_name)} SET NOT NULL") if not_null
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

    #: (untyped) -> untyped
    def timestamp(time)
      time.utc.iso8601(6)
    end

    #: (untyped) -> untyped
    def timestamp_or_nil(time)
      time ? timestamp(time) : nil
    end
  end

  class MysqlStore < Store
    MysqlResult = Struct.new(:rows, :affected_rows) do
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
    end

    class << self
      #: (uri: untyped, schema: untyped) -> untyped
      def connect(uri:, schema:)
        new(
          Trilogy.new(
            host: uri.host || "127.0.0.1",
            port: uri.port || 3306,
            username: URI.decode_www_form_component(uri.user || "root"),
            password: uri.password && URI.decode_www_form_component(uri.password),
            database: uri.path.delete_prefix("/"),
          ),
          schema:,
        )
      end
    end

    #: () -> untyped
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
          INDEX #{quote_ident(index_name("workflows", "queue"))} (status, created_at),
          INDEX #{quote_ident(index_name("workflows", "runnable_due"))} (status, next_run_at, created_at),
          INDEX #{quote_ident(index_name("workflows", "expired_lease"))} (status, locked_until, created_at)
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
          INDEX #{quote_ident(index_name("workflow_history", "command"))} (workflow_id, command_id, event_index),
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
          INDEX #{quote_ident(index_name("step_attempts", "workflow_started_position"))} (workflow_id, started_at, position),
          INDEX #{quote_ident(index_name("step_attempts", "workflow_position_status_started"))} (workflow_id, position, status, started_at),
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
          INDEX #{quote_ident(index_name("outbox", "queue"))} (status, created_at),
          INDEX #{quote_ident(index_name("outbox", "expired_lease"))} (status, locked_until, created_at),
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
          INDEX #{quote_ident(index_name("waits", "workflow_created"))} (workflow_id, created_at),
          INDEX #{quote_ident(index_name("waits", "event_pending"))} (status, kind, event_key, created_at),
          INDEX #{quote_ident(index_name("waits", "timer_pending"))} (status, kind, wake_at, created_at),
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
          INDEX #{quote_ident(index_name("durable_object_commands", "object_status"))} (object_type, object_id, status, created_at)
        )
      SQL
      @migrated = true
      self
    end

    #: () -> untyped
    def drop_schema!
      ["durable_object_commands", "durable_objects", "waits", "outbox", "fences", "step_attempts", "steps", "workflow_history", "workflows"].each { |name| execute("DROP TABLE IF EXISTS #{table(name)}") }
      @migrated = false
    end

    #: () -> untyped
    def close
      @connection.close
    end

    #: (name: untyped, input: untyped) -> untyped
    def enqueue_workflow(name:, input:)
      id = SecureRandom.uuid
      execute_params("INSERT INTO #{table("workflows")} (id, name, status, input) VALUES (?, ?, 'pending', ?)", [id, name, dump_serialized(input)])
      id
    end

    #: (name: untyped, input: untyped) -> untyped
    def create_workflow(name:, input:)
      id = enqueue_workflow(name:, input:)
      mark_workflow_running(id)
      id
    end

    #: (untyped, ?worker_id: untyped, ?lease_seconds: untyped) -> untyped
    def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60)
      if worker_id
        execute_params(<<~SQL, [worker_id, lease_seconds, workflow_id])
          UPDATE #{table("workflows")}
          SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
          WHERE id = ?
        SQL
      else
        execute_params("UPDATE #{table("workflows")} SET status = 'running', error = NULL, updated_at = NOW(6) WHERE id = ?", [workflow_id])
      end
    end

    #: (worker_id: untyped, lease_seconds: untyped, ?workflow_names: untyped) -> untyped
    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil)
      return if workflow_names&.empty?

      transaction do
        name_sql = workflow_names ? "AND name IN (#{workflow_names.map { "?" }.join(", ")})" : ""
        name_params = workflow_names || []
        candidates = []
        candidates.concat(execute_params(<<~SQL, name_params).to_a)
          SELECT id, created_at FROM #{table("workflows")}
          WHERE status = 'pending'
            AND (next_run_at IS NULL OR next_run_at <= NOW(6))
            #{name_sql}
          ORDER BY created_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        SQL
        candidates.concat(execute_params(<<~SQL, name_params).to_a)
          SELECT id, created_at FROM #{table("workflows")}
          WHERE status = 'failed'
            AND next_run_at IS NOT NULL
            AND next_run_at <= NOW(6)
            #{name_sql}
          ORDER BY created_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        SQL
        candidates.concat(execute_params(<<~SQL, name_params).to_a)
          SELECT id, created_at FROM #{table("workflows")}
          WHERE status = 'canceling'
            AND (next_run_at IS NULL OR next_run_at <= NOW(6))
            #{name_sql}
          ORDER BY created_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        SQL
        candidates.concat(execute_params(<<~SQL, name_params).to_a)
          SELECT id, created_at FROM #{table("workflows")}
          WHERE status = 'running' AND locked_until < NOW(6)
            #{name_sql}
          ORDER BY created_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        SQL
        candidate = candidates.min_by { |candidate_row| candidate_row.fetch("created_at").to_s }
        next unless candidate

        updated = execute_params(<<~SQL, [worker_id, lease_seconds, candidate.fetch("id")])
          UPDATE #{table("workflows")}
          SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), next_run_at = NULL, updated_at = NOW(6)
          WHERE id = ?
            AND (
              (status = 'pending' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
              OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))
              OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
              OR (status = 'running' AND locked_until < NOW(6))
            )
        SQL
        next unless updated.cmd_tuples == 1

        workflow(candidate.fetch("id"))
      end
    end

    #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
    def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
      already_owned = execute_params(<<~SQL, [workflow_id, worker_id]).first
        SELECT * FROM #{table("workflows")}
        WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
      SQL
      return decode_row(already_owned) if already_owned

      transaction do
        row = execute_params(<<~SQL, [workflow_id, worker_id]).first
          SELECT id FROM #{table("workflows")}
          WHERE id = ?
            AND (
              (status = 'pending' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
              OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))
              OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
              OR (status = 'running' AND (locked_by = ? OR locked_until < NOW(6)))
            )
          FOR UPDATE SKIP LOCKED
        SQL
        next unless row

        execute_params(<<~SQL, [worker_id, lease_seconds, workflow_id])
          UPDATE #{table("workflows")}
          SET status = 'running', error = NULL, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), next_run_at = NULL, updated_at = NOW(6)
          WHERE id = ?
        SQL
        workflow(workflow_id)
      end
    end

    #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
    def heartbeat(workflow_id:, worker_id:, lease_seconds:)
      execute_params(<<~SQL, [lease_seconds, workflow_id, worker_id])
        UPDATE #{table("workflows")}
        SET locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
        WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)
      SQL
      MysqlResult.new([], workflow_owned?(workflow_id:, worker_id:) ? 1 : 0)
    end

    #: (workflow_id: untyped, worker_id: untyped) -> untyped
    def workflow_owned?(workflow_id:, worker_id:)
      !!execute_params(<<~SQL, [workflow_id, worker_id]).first
        SELECT 1
        FROM #{table("workflows")}
        WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)
      SQL
    end

    #: (worker_id: untyped) -> untyped
    def release_worker_leases!(worker_id:)
      transaction do
        workflows = execute_params("SELECT COUNT(*) AS count FROM #{table("workflows")} WHERE status = 'running' AND locked_by = ?", [worker_id]).first.fetch("count").to_i
        execute_params(<<~SQL, [worker_id])
          UPDATE #{table("workflows")}
          SET status = CASE
              WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
              ELSE 'pending'
            END,
            locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
          WHERE status = 'running' AND locked_by = ?
        SQL
        outbox = execute_params("SELECT COUNT(*) AS count FROM #{table("outbox")} WHERE status = 'processing' AND locked_by = ?", [worker_id]).first.fetch("count").to_i
        execute_params(<<~SQL, [worker_id])
          UPDATE #{table("outbox")}
          SET status = 'pending', locked_by = NULL, locked_until = NULL
          WHERE status = 'processing' AND locked_by = ?
        SQL
        { "workflows" => workflows, "outbox" => outbox }
      end
    end

    #: (workflow_id: untyped, worker_id: untyped, run_at: untyped) -> untyped
    def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
      execute_params(<<~SQL, [run_at, workflow_id, worker_id])
        UPDATE #{table("workflows")}
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, next_run_at = ?, updated_at = NOW(6)
        WHERE id = ? AND status = 'running' AND locked_by = ?
      SQL
    end

    #: (workflow_id: untyped, ?worker_id: untyped) -> untyped
    def suspend_workflow(workflow_id:, worker_id: nil)
      params = [workflow_id]
      owner_filter = ""
      if worker_id
        owner_filter = "AND locked_by = ?"
        params << worker_id
      end
      params << workflow_id
      result = execute_params(<<~SQL, params)
        UPDATE #{table("workflows")}
        SET status = CASE
              WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
              WHEN EXISTS (SELECT 1 FROM #{table("waits")} WHERE workflow_id = ? AND status = 'pending') THEN 'waiting'
              ELSE 'pending'
            END,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = NOW(6)
        WHERE status = 'running' #{owner_filter} AND id = ?
      SQL
      return true if result.cmd_tuples == 1

      ["pending", "waiting", "canceling"].include?(workflow(workflow_id).fetch("status"))
    end

    #: (untyped, ?now: untyped) -> untyped
    def make_workflow_due!(workflow_id, now: Time.now)
      execute_params("UPDATE #{table("workflows")} SET next_run_at = NULL, updated_at = ? WHERE id = ?", [now, workflow_id])
    end

    #: (workflow_id: untyped, reason: untyped) -> untyped
    def request_workflow_cancellation(workflow_id:, reason:)
      transaction do
        row = execute_params("SELECT * FROM #{table("workflows")} WHERE id = ? FOR UPDATE", [workflow_id]).first
        raise KeyError, "workflow not found: #{workflow_id}" unless row

        decoded = decode_row(row)
        next decoded if terminal_for_cancellation?(decoded)

        first_request = row["cancel_requested_at"].nil?
        if first_request
          execute_params(<<~SQL, [reason, workflow_id])
            UPDATE #{table("workflows")}
            SET cancel_reason = ?, cancel_requested_at = NOW(6), updated_at = NOW(6)
            WHERE id = ?
          SQL
        end
        cancel_pending_waits_for_workflow(workflow_id) if first_request

        if first_request && decoded.fetch("status") != "running"
          execute_params(<<~SQL, [workflow_id])
            UPDATE #{table("workflows")}
            SET status = 'canceling', locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
            WHERE id = ? AND status NOT IN ('completed', 'canceled')
          SQL
        end

        workflow(workflow_id)
      end
    end

    #: (untyped) -> untyped
    def workflow_cancellation(workflow_id)
      execute_params(<<~SQL, [workflow_id]).first
        SELECT id AS workflow_id, cancel_reason AS reason,
          cancel_requested_at AS requested_at, cancel_delivered_at AS delivered_at
        FROM #{table("workflows")}
        WHERE id = ? AND cancel_requested_at IS NOT NULL
      SQL
    end

    #: (workflow_id: untyped) -> untyped
    def mark_workflow_cancellation_delivered(workflow_id:)
      execute_params(<<~SQL, [workflow_id])
        UPDATE #{table("workflows")}
        SET cancel_delivered_at = COALESCE(cancel_delivered_at, NOW(6)), updated_at = NOW(6)
        WHERE id = ? AND cancel_requested_at IS NOT NULL
      SQL
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, worker_id: untyped, lease_seconds: untyped, cursor: untyped) -> untyped
    def heartbeat_step(workflow_id:, worker_id:, lease_seconds:, cursor:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      renewed = transaction do
        next nil unless workflow_owned?(workflow_id:, worker_id:)

        execute_params(<<~SQL, [lease_seconds, workflow_id, worker_id])
          UPDATE #{table("workflows")}
          SET locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
          WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)
        SQL

        serialized_cursor = dump_serialized(cursor)
        step = execute_params("SELECT 1 FROM #{table("steps")} WHERE workflow_id = ? AND position = ? AND status = 'running'", [workflow_id, command_id]).first
        next nil unless step

        execute_params(<<~SQL, [serialized_cursor, workflow_id, command_id])
          UPDATE #{table("steps")}
          SET heartbeat_cursor = ?, updated_at = NOW(6)
          WHERE workflow_id = ? AND position = ? AND status = 'running'
        SQL

        execute_params(<<~SQL, [serialized_cursor, workflow_id, command_id])
          UPDATE #{table("step_attempts")}
          SET heartbeat_cursor = ?
          WHERE workflow_id = ? AND position = ? AND status = 'running'
          ORDER BY started_at DESC
          LIMIT 1
        SQL
        execute_params("SELECT locked_until FROM #{table("workflows")} WHERE id = ?", [workflow_id]).first
      end
      renewed&.fetch("locked_until")
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped) -> untyped
    def step_heartbeat_cursor(workflow_id:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      row = execute_params("SELECT heartbeat_cursor FROM #{table("steps")} WHERE workflow_id = ? AND position = ?", [workflow_id, command_id]).first
      decode_row(row).fetch("heartbeat_cursor") if row
    end

    #: (untyped) -> untyped
    def current_workflow_lease(workflow_id)
      execute_params(<<~SQL, [workflow_id]).first
        SELECT id AS workflow_id, locked_by AS worker_id, locked_until
        FROM #{table("workflows")}
        WHERE id = ? AND status = 'running' AND locked_by IS NOT NULL AND locked_until >= NOW(6)
      SQL
    end

    #: (?now: untyped) -> untyped
    def steal_expired_leases!(now: Time.now)
      expired = execute_params(<<~SQL, [now]).first.fetch("count").to_i
        SELECT COUNT(*) AS count
        FROM #{table("workflows")}
        WHERE status = 'running' AND locked_until < ?
      SQL
      execute_params(<<~SQL, [now])
        UPDATE #{table("workflows")}
        SET status = CASE
              WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
              ELSE 'pending'
            END,
            locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
        WHERE status = 'running' AND locked_until < ?
      SQL
      expired
    end

    #: (workflow_id: untyped, command_id: untyped, name: untyped, ?args: untyped, ?kwargs: untyped, ?metadata: untyped) -> untyped
    def record_step_scheduled(workflow_id:, command_id:, name:, args: [], kwargs: {}, metadata: {})
      payload = { "name" => name, "args" => args, "kwargs" => kwargs }.merge(metadata)
      transaction do
        append_workflow_history_without_transaction(workflow_id:, kind: "step_scheduled", command_id:, name:, payload:)
        execute_params(<<~SQL, [workflow_id, command_id, name])
          INSERT IGNORE INTO #{table("steps")} (workflow_id, position, name, status, updated_at)
          VALUES (?, ?, ?, 'scheduled', NOW(6))
        SQL
      end
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, name: untyped) -> untyped
    def record_step_started(workflow_id:, name:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        execute_params(<<~SQL, [workflow_id, command_id])
          UPDATE #{table("step_attempts")}
          SET status = 'failed', error = 'superseded by retry', completed_at = NOW(6)
          WHERE workflow_id = ? AND position = ? AND status = 'running'
        SQL
        execute_params(<<~SQL, [workflow_id, command_id, name])
          INSERT INTO #{table("steps")} (workflow_id, position, name, status, started_at, updated_at)
          VALUES (?, ?, ?, 'running', NOW(6), NOW(6))
          ON DUPLICATE KEY UPDATE status = 'running', error = NULL, updated_at = NOW(6)
        SQL
        attempt_id = SecureRandom.uuid
        execute_params(<<~SQL, [attempt_id, workflow_id, command_id, name])
          INSERT INTO #{table("step_attempts")} (id, workflow_id, position, name, status)
          VALUES (?, ?, ?, ?, 'running')
        SQL
        append_workflow_history_without_transaction(workflow_id:, kind: "step_started", command_id:, name:, attempt_id:)
        attempt_id
      end
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, result: untyped) -> untyped
    def record_step_completed(workflow_id:, result:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction { record_step_completed_without_transaction(workflow_id:, command_id:, result:) }
    end

    #: (workflow_id: untyped, command_id: untyped, result: untyped) -> untyped
    def record_step_completed_without_transaction(workflow_id:, command_id:, result:)
      serialized = dump_serialized(result)
      execute_params(<<~SQL, [serialized, workflow_id, command_id])
        UPDATE #{table("steps")}
        SET status = 'completed', result = ?, error = NULL, completed_at = NOW(6), updated_at = NOW(6)
        WHERE workflow_id = ? AND position = ?
      SQL
      update_latest_attempt_serialized(workflow_id:, command_id:, status: "completed", serialized_result: serialized, error: nil)
      append_workflow_history_without_transaction(workflow_id:, kind: "step_completed", command_id:, payload: result)
    end

    #: (untyped, result: untyped) -> untyped
    def complete_workflow(workflow_id, result:)
      execute_params(<<~SQL, [dump_serialized(result), workflow_id])
        UPDATE #{table("workflows")}
        SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ?
      SQL
    end

    #: (untyped, reason: untyped, ?result: untyped) -> untyped
    def cancel_workflow(workflow_id, reason:, result: nil)
      execute_params(<<~SQL, [dump_serialized(result), reason, reason, workflow_id])
        UPDATE #{table("workflows")}
        SET status = 'canceled', result = ?, error = ?, cancel_reason = COALESCE(cancel_reason, ?),
          cancel_requested_at = COALESCE(cancel_requested_at, NOW(6)),
          locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ?
      SQL
    end

    #: (untyped, error: untyped) -> untyped
    def fail_workflow(workflow_id, error:)
      transaction do
        execute_params(
          "UPDATE #{table("steps")} SET status = 'failed', error = ?, updated_at = NOW(6) WHERE workflow_id = ? AND status = 'running'",
          [error, workflow_id],
        )
        execute_params(
          "UPDATE #{table("step_attempts")} SET status = 'failed', error = ?, completed_at = NOW(6) WHERE workflow_id = ? AND status = 'running'",
          [error, workflow_id],
        )
        execute_params(<<~SQL, [error, workflow_id])
          UPDATE #{table("workflows")}
          SET status = 'failed', error = ?, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
          WHERE id = ?
        SQL
      end
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped) -> untyped
    def record_step_failed(workflow_id:, error:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction { record_step_failed_without_transaction(workflow_id:, command_id:, error:) }
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped) -> untyped
    def record_step_canceled(workflow_id:, error:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        execute_params(<<~SQL, [error, workflow_id, command_id])
          UPDATE #{table("steps")}
          SET status = 'canceled', error = ?, updated_at = NOW(6)
          WHERE workflow_id = ? AND position = ? AND status IN ('scheduled', 'running', 'waiting')
        SQL
        update_latest_attempt_serialized(workflow_id:, command_id:, status: "canceled", serialized_result: dump_serialized(nil), error:)
        append_workflow_history_without_transaction(workflow_id:, kind: "step_canceled", command_id:, error:)
      end
    end

    #: (workflow_id: untyped, command_id: untyped, error: untyped) -> untyped
    def record_step_failed_without_transaction(workflow_id:, command_id:, error:)
      execute_params(<<~SQL, [error, workflow_id, command_id])
        UPDATE #{table("steps")}
        SET status = 'failed', error = ?, updated_at = NOW(6)
        WHERE workflow_id = ? AND position = ?
      SQL
      update_latest_attempt_serialized(workflow_id:, command_id:, status: "failed", serialized_result: dump_serialized(nil), error:)
      append_workflow_history_without_transaction(workflow_id:, kind: "step_failed", command_id:, error:)
    end

    #: (untyped) -> untyped
    def workflow(workflow_id)
      row = execute_params("SELECT * FROM #{table("workflows")} WHERE id = ?", [workflow_id]).first
      raise KeyError, "workflow not found: #{workflow_id}" unless row

      decode_row(row)
    end

    #: (untyped) -> untyped
    def steps_for(workflow_id)
      execute_params("SELECT * FROM #{table("steps")} WHERE workflow_id = ? ORDER BY position", [workflow_id]).map { |row| with_command_id(decode_row(row)) }
    end

    #: (workflow_id: untyped, topic: untyped, payload: untyped, key: untyped) -> untyped
    def enqueue_outbox(workflow_id:, topic:, payload:, key:)
      existing = execute_params("SELECT id FROM #{table("outbox")} WHERE `key` = ?", [key]).first
      return existing.fetch("id") if existing

      id = SecureRandom.uuid
      execute_params(<<~SQL, [id, workflow_id, topic, dump_serialized(payload), key])
        INSERT IGNORE INTO #{table("outbox")} (id, workflow_id, topic, payload, `key`, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
      SQL
      execute_params("SELECT id FROM #{table("outbox")} WHERE `key` = ?", [key]).first.fetch("id")
    end

    #: (worker_id: untyped, lease_seconds: untyped) -> untyped
    def claim_outbox(worker_id:, lease_seconds:)
      transaction do
        candidates = []
        candidates.concat(execute_params(<<~SQL, []).to_a)
          SELECT id, created_at FROM #{table("outbox")}
          WHERE status = 'pending'
          ORDER BY created_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        SQL
        candidates.concat(execute_params(<<~SQL, []).to_a)
          SELECT id, created_at FROM #{table("outbox")}
          WHERE status = 'processing' AND locked_until < NOW(6)
          ORDER BY created_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        SQL
        candidate = candidates.min_by { |candidate_row| candidate_row.fetch("created_at").to_s }
        next unless candidate

        execute_params(<<~SQL, [worker_id, lease_seconds, candidate.fetch("id")])
          UPDATE #{table("outbox")}
          SET status = 'processing', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND)
          WHERE id = ?
        SQL
        outbox_message(candidate.fetch("id"))
      end
    end

    #: (untyped, worker_id: untyped) -> untyped
    def ack_outbox(outbox_id, worker_id:)
      execute_params(
        "UPDATE #{table("outbox")} SET status = 'processed', processed_at = NOW(6) WHERE id = ? AND status = 'processing' AND locked_by = ? AND locked_until >= NOW(6)",
        [outbox_id, worker_id],
      )
    end

    #: (untyped) -> untyped
    def outbox_message(outbox_id)
      decode_row(execute_params("SELECT * FROM #{table("outbox")} WHERE id = ?", [outbox_id]).first)
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, name: untyped, wait_request: untyped, ?suspend_workflow: untyped) -> untyped
    def record_wait(workflow_id:, name:, wait_request:, command_id: nil, position: nil, suspend_workflow: true)
      command_id = normalize_command_id(command_id, position)
      transaction do
        serialized_context = dump_serialized(wait_request.context)
        execute_params(<<~SQL, [workflow_id, command_id, name, serialized_context])
          INSERT INTO #{table("steps")} (workflow_id, position, name, status, result, started_at, updated_at)
          VALUES (?, ?, ?, 'waiting', ?, NOW(6), NOW(6))
          ON DUPLICATE KEY UPDATE status = 'waiting', result = VALUES(result), error = NULL, updated_at = NOW(6)
        SQL
        wait_id = SecureRandom.uuid
        execute_params(<<~SQL, [wait_id, workflow_id, command_id, wait_request.kind, wait_request.event_key, wait_request.wake_at, dump_serialized(wait_request.context)])
          INSERT INTO #{table("waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status)
          VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
        SQL
        update_latest_attempt_serialized(
          workflow_id:,
          command_id:,
          status: "waiting",
          serialized_result: serialized_context,
          error: nil,
        )
        append_workflow_history_without_transaction(workflow_id:, kind: "step_waiting", command_id:, name:, payload: wait_request.context)
        suspend_workflow(workflow_id:) if suspend_workflow
        wait_id
      end
    end

    #: (untyped) -> untyped
    def workflow_history_for(workflow_id)
      execute_params("SELECT * FROM #{table("workflow_history")} WHERE workflow_id = ? ORDER BY event_index", [workflow_id]).map { |row| decode_row(row) }
    end

    #: (untyped) -> untyped
    def waits_for(workflow_id)
      execute_params("SELECT * FROM #{table("waits")} WHERE workflow_id = ? ORDER BY created_at", [workflow_id]).map { |row| decode_row(row) }
    end

    #: (untyped, ?payload: untyped) -> untyped
    def signal_event(event_key, payload: {})
      complete_waits_mysql("kind = 'event' AND event_key = ?", [event_key], payload)
    end

    #: (?now: untyped) -> untyped
    def wake_due_timers(now: Time.now)
      complete_waits_mysql("kind = 'timer' AND wake_at <= ?", [now], {})
    end

    #: (workflow_id: untyped, key: untyped, ?poll_interval: untyped, ?timeout: untyped) { (?) -> untyped } -> untyped
    def with_fence(workflow_id:, key:, poll_interval: 0.05, timeout: 10, &block)
      token = SecureRandom.uuid
      execute_params(<<~SQL, [workflow_id, key, token, timeout])
        INSERT IGNORE INTO #{table("fences")} (workflow_id, `key`, status, locked_by, locked_until)
        VALUES (?, ?, 'running', ?, DATE_ADD(NOW(6), INTERVAL ? SECOND))
      SQL

      if execute_params("SELECT 1 FROM #{table("fences")} WHERE workflow_id = ? AND `key` = ? AND locked_by = ? AND status = 'running'", [workflow_id, key, token]).first
        begin
          result = block.call
          execute_params(<<~SQL, [dump_serialized(result), workflow_id, key, token])
            UPDATE #{table("fences")}
            SET status = 'completed', result = ?, error = NULL, completed_at = NOW(6)
            WHERE workflow_id = ? AND `key` = ? AND locked_by = ?
          SQL
          return result
        rescue StandardError => e
          execute_params(<<~SQL, ["#{e.class}: #{e.message}", workflow_id, key, token])
            UPDATE #{table("fences")}
            SET status = 'failed', error = ?, completed_at = NOW(6)
            WHERE workflow_id = ? AND `key` = ? AND locked_by = ?
          SQL
          raise
        end
      end

      deadline = Time.now + timeout
      loop do
        row = execute_params("SELECT status, result, error FROM #{table("fences")} WHERE workflow_id = ? AND `key` = ?", [workflow_id, key]).first
        decoded = decode_row(row) if row
        case decoded&.fetch("status")
        when "completed"
          return decoded.fetch("result")
        when "failed"
          raise Error, decoded.fetch("error")
        end
        raise FenceTimeout, "timed out waiting for fence #{key}" if Time.now >= deadline

        sleep(poll_interval)
      end
    end

    #: (untyped) -> untyped
    def step_attempts_for(workflow_id)
      execute_params("SELECT * FROM #{table("step_attempts")} WHERE workflow_id = ? ORDER BY started_at, position", [workflow_id]).map { |row| with_command_id(decode_row(row)) }
    end

    #: (object_type: untyped, object_id: untyped) -> untyped
    def object_state(object_type:, object_id:)
      row = execute_params("SELECT state FROM #{table("durable_objects")} WHERE object_type = ? AND object_id = ?", [object_type, object_id]).first
      decode_row(row)&.fetch("state") if row
    end

    #: (object_type: untyped, object_id: untyped, state: untyped) -> untyped
    def save_object_state(object_type:, object_id:, state:)
      execute_params(<<~SQL, [object_type, object_id, dump_serialized(state)])
        INSERT INTO #{table("durable_objects")} (object_type, object_id, state)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE state = VALUES(state), updated_at = NOW(6)
      SQL
      state
    end

    #: (object_type: untyped, object_id: untyped, method_name: untyped, args: untyped, kwargs: untyped) -> untyped
    def enqueue_object_command(object_type:, object_id:, method_name:, args:, kwargs:)
      id = SecureRandom.uuid
      transaction do
        execute_params(<<~SQL, [object_type, object_id])
          INSERT IGNORE INTO #{table("durable_objects")} (object_type, object_id, state)
          VALUES (?, ?, NULL)
        SQL
        execute_params(<<~SQL, [id, object_type, object_id, method_name, dump_serialized(args), dump_serialized(kwargs)])
          INSERT INTO #{table("durable_object_commands")} (id, object_type, object_id, method_name, args, kwargs, status)
          VALUES (?, ?, ?, ?, ?, ?, 'pending')
        SQL
      end
      id
    end

    #: (command_id: untyped, worker_id: untyped, ?lease_seconds: untyped) -> untyped
    def claim_object_command(command_id:, worker_id:, lease_seconds: 60)
      transaction do
        row = execute_params(<<~SQL, [command_id]).first
          SELECT * FROM #{table("durable_object_commands")}
          WHERE id = ? AND (status IN ('pending', 'failed') OR (status = 'running' AND locked_until < NOW(6)))
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        SQL
        next unless row

        object_type = row.fetch("object_type")
        object_id = row.fetch("object_id")
        execute_params(<<~SQL, [object_type, object_id])
          INSERT IGNORE INTO #{table("durable_objects")} (object_type, object_id, state)
          VALUES (?, ?, NULL)
        SQL
        execute_params(<<~SQL, [object_type, object_id]).first
          SELECT 1 FROM #{table("durable_objects")}
          WHERE object_type = ? AND object_id = ?
          FOR UPDATE
        SQL
        active = execute_params(<<~SQL, [object_type, object_id, command_id]).first
          SELECT 1 FROM #{table("durable_object_commands")}
          WHERE object_type = ? AND object_id = ? AND id <> ?
            AND status = 'running' AND locked_until >= NOW(6)
          LIMIT 1
        SQL
        next if active

        execute_params(<<~SQL, [worker_id, lease_seconds, command_id])
          UPDATE #{table("durable_object_commands")}
          SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND)
          WHERE id = ?
        SQL
        decode_row(execute_params("SELECT * FROM #{table("durable_object_commands")} WHERE id = ?", [command_id]).first)
      end
    end

    #: (command_id: untyped, result: untyped, ?object_type: untyped, ?object_id: untyped, ?state: untyped, ?worker_id: untyped) -> untyped
    def complete_object_command(command_id:, result:, object_type: nil, object_id: nil, state: NO_OBJECT_STATE, worker_id: nil)
      transaction do
        command = lock_object_command_for_completion(command_id:, worker_id:)
        next nil unless command

        save_object_state(object_type:, object_id:, state:) unless state.equal?(NO_OBJECT_STATE)
        execute_params(
          "UPDATE #{table("durable_object_commands")} SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = NOW(6) WHERE id = ?",
          [dump_serialized(result), command_id],
        )
        MysqlResult.new([], 1)
      end
    end

    #: (command_id: untyped, error: untyped, ?worker_id: untyped) -> untyped
    def fail_object_command(command_id:, error:, worker_id: nil)
      if worker_id
        execute_params(
          "UPDATE #{table("durable_object_commands")} SET status = 'failed', error = ?, locked_by = NULL, locked_until = NULL WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)",
          [error, command_id, worker_id],
        )
      else
        execute_params(
          "UPDATE #{table("durable_object_commands")} SET status = 'failed', error = ?, locked_by = NULL, locked_until = NULL WHERE id = ?",
          [error, command_id],
        )
      end
    end

    private

    #: (untyped) -> untyped
    def cancel_pending_waits_for_workflow(workflow_id)
      execute_params(<<~SQL, [workflow_id])
        UPDATE #{table("waits")}
        SET status = 'canceled', completed_at = NOW(6)
        WHERE workflow_id = ? AND status = 'pending'
      SQL
      execute_params(<<~SQL, [workflow_id])
        UPDATE #{table("steps")}
        SET status = 'canceled', error = 'workflow cancellation requested', updated_at = NOW(6)
        WHERE workflow_id = ? AND status = 'waiting'
      SQL
      execute_params(<<~SQL, [workflow_id])
        UPDATE #{table("step_attempts")}
        SET status = 'canceled', error = 'workflow cancellation requested', completed_at = NOW(6)
        WHERE workflow_id = ? AND status = 'waiting'
      SQL
    end

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

      execute("ALTER TABLE #{table(table_name)} ADD COLUMN #{quote_ident(column_name)} #{column_type}")
    end

    #: (command_id: untyped, worker_id: untyped) -> untyped
    def lock_object_command_for_completion(command_id:, worker_id:)
      if worker_id
        execute_params(<<~SQL, [command_id, worker_id]).first
          SELECT 1 FROM #{table("durable_object_commands")}
          WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
          FOR UPDATE
        SQL
      else
        execute_params("SELECT 1 FROM #{table("durable_object_commands")} WHERE id = ? FOR UPDATE", [command_id]).first
      end
    end

    #: (workflow_id: untyped, command_id: untyped, status: untyped, serialized_result: untyped, error: untyped) -> untyped
    def update_latest_attempt_serialized(workflow_id:, command_id:, status:, serialized_result:, error:)
      execute_params(<<~SQL, [status, serialized_result, error, workflow_id, command_id])
        UPDATE #{table("step_attempts")}
        SET status = ?, result = ?, error = ?, completed_at = NOW(6)
        WHERE workflow_id = ? AND position = ? AND status IN ('running', 'waiting')
        ORDER BY started_at DESC
        LIMIT 1
      SQL
    end

    #: (workflow_id: untyped, kind: untyped, ?command_id: untyped, ?name: untyped, ?attempt_id: untyped, ?payload: untyped, ?error: untyped) -> untyped
    def append_workflow_history_without_transaction(workflow_id:, kind:, command_id: nil, name: nil, attempt_id: nil, payload: nil, error: nil)
      execute_params("SELECT id FROM #{table("workflows")} WHERE id = ? FOR UPDATE", [workflow_id])
      event_index = execute_params(
        "SELECT COALESCE(MAX(event_index), -1) + 1 AS event_index FROM #{table("workflow_history")} WHERE workflow_id = ?",
        [workflow_id],
      ).first.fetch("event_index").to_i
      execute_params(<<~SQL, [workflow_id, event_index, kind, command_id, name, attempt_id, dump_serialized(payload), error])
        INSERT INTO #{table("workflow_history")} (workflow_id, event_index, kind, command_id, name, attempt_id, payload, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      SQL
      event_index
    end

    #: (untyped, untyped) -> untyped
    def normalize_command_id(command_id, position)
      id = command_id.nil? ? position : command_id
      raise ArgumentError, "command_id is required" if id.nil?

      id
    end

    #: (untyped, untyped, untyped) -> untyped
    def complete_waits_mysql(where_sql, params, payload)
      transaction do
        waits = execute_params(<<~SQL, params).map { |row| decode_row(row) }
          SELECT w.* FROM #{table("waits")} AS w
          JOIN #{table("workflows")} AS wf ON wf.id = w.workflow_id
          WHERE w.status = 'pending'
            AND wf.status IN ('waiting', 'running')
            AND #{where_sql}
          FOR UPDATE SKIP LOCKED
        SQL
        waits.each do |wait|
          execute_params("UPDATE #{table("waits")} SET status = 'completed', payload = ?, completed_at = NOW(6) WHERE id = ?", [dump_serialized(payload), wait.fetch("id")])
          context = wait.fetch("context").merge(payload)
          record_step_completed_without_transaction(workflow_id: wait.fetch("workflow_id"), command_id: wait.fetch("position").to_i, result: context)
          execute_params("UPDATE #{table("workflows")} SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6) WHERE id = ? AND status = 'waiting'", [wait.fetch("workflow_id")])
        end
        waits.length
      end
    end

    #: (untyped) -> untyped
    def execute(sql)
      result = @connection.query(sql)
      MysqlResult.new(mysql_rows(result), result.affected_rows || @connection.affected_rows)
    end

    #: (untyped, untyped) -> untyped
    def execute_params(sql, params)
      execute(bind_params(sql, params))
    end

    #: () { (?) -> untyped } -> untyped
    def transaction(&block)
      attempts = 0
      begin
        execute("START TRANSACTION")
        result = block.call
        execute("COMMIT")
        result
      rescue StandardError => error
        begin
          execute("ROLLBACK")
        rescue
          nil
        end
        if retryable_mysql_error?(error) && attempts < 5
          attempts += 1
          sleep(0.01 * attempts)
          retry
        end

        raise
      end
    end

    #: (untyped) -> untyped
    def table(name)
      quote_ident("#{table_prefix}_#{name}")
    end

    #: (untyped) -> untyped
    def raw_table_name(name)
      "#{table_prefix}_#{name}"
    end

    #: () -> untyped
    def table_prefix
      prefix = schema.to_s.gsub(/[^a-zA-Z0-9_]/, "_")
      return prefix if prefix.length <= 32

      "dura_#{Digest::SHA1.hexdigest(prefix)[0, 10]}"
    end

    #: (untyped, untyped) -> untyped
    def index_name(table_name, suffix)
      "#{table_prefix}_#{table_name}_#{suffix}_idx"[0, 64]
    end

    #: (untyped) -> untyped
    def quote_ident(identifier)
      "`#{identifier.to_s.gsub("`", "``")}`"
    end

    #: (untyped) -> untyped
    def dump_serialized(value)
      SERIALIZER.dump(value)
    end

    #: (untyped) -> untyped
    def load_serialized(value)
      return if value.nil?

      SERIALIZER.load(value)
    end

    #: (untyped) -> untyped
    def decode_row(row)
      row.each_with_object({}) do |(column, value), decoded|
        decoded[column] = SERIALIZED_COLUMNS.include?(column) ? load_serialized(value) : value
      end
    end

    #: (untyped) -> untyped
    def mysql_rows(result)
      return [] unless result.respond_to?(:each_hash)

      result.each_hash.map { |row| row.transform_keys(&:to_s) }
    end

    #: (untyped) -> untyped
    def mysql_param(value)
      return value unless value.is_a?(Time)

      value.utc.strftime("%Y-%m-%d %H:%M:%S.%6N")
    end

    #: (untyped, untyped) -> untyped
    def bind_params(sql, params)
      index = 0
      bound_sql = sql.gsub("?") do
        raise ArgumentError, "not enough parameters for SQL query" if index >= params.length

        literal = mysql_literal(params.fetch(index))
        index += 1
        literal
      end
      raise ArgumentError, "too many parameters for SQL query" if index != params.length

      bound_sql
    end

    #: (untyped) -> untyped
    def mysql_literal(value)
      value = mysql_param(value)
      case value
      when nil
        "NULL"
      when true
        "TRUE"
      when false
        "FALSE"
      when Numeric
        value.to_s
      when String
        if value.encoding == Encoding::ASCII_8BIT
          "x'#{value.unpack1("H*")}'"
        else
          "'#{@connection.escape(value)}'"
        end
      else
        "'#{@connection.escape(value.to_s)}'"
      end
    end

    #: (untyped) -> untyped
    def retryable_mysql_error?(error)
      error.respond_to?(:error_code) && [1205, 1213].include?(error.error_code)
    end
  end
end
