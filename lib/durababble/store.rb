# frozen_string_literal: true

require "paquito"
require "pg"
require "json"
require "securerandom"
require "time"

module Durababble
  class Store
    SERIALIZED_COLUMNS = %w[input result payload context heartbeat_cursor].freeze
    SERIALIZER = Paquito::SingleBytePrefixVersion.new(1, 1 => Marshal)


    attr_reader :schema

    def self.connect(database_url:, schema: "durababble")
      new(PG.connect(database_url), schema:)
    end

    def initialize(connection, schema:)
      @connection = connection
      @schema = schema
    end

    def migrate!
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
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      SQL
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS locked_by text")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS locked_until timestamptz")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS next_run_at timestamptz")
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
      create_performance_indexes!
      migrate_serialized_columns!
      self
    end

    def drop_schema!
      execute("DROP SCHEMA IF EXISTS #{quoted_schema} CASCADE")
    end

    def close
      @connection.close unless @connection.finished?
    end

    def enqueue_workflow(name:, input:)
      id = SecureRandom.uuid
      execute_params(
        "INSERT INTO #{table("workflows")} (id, name, status, input) VALUES ($1, $2, 'pending', $3::bytea)",
        [id, name, dump_serialized(input)]
      )
      id
    end

    def create_workflow(name:, input:)
      id = enqueue_workflow(name:, input:)
      mark_workflow_running(id)
      id
    end

    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil)
      return nil if workflow_names&.empty?

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
            status IN ('pending', 'failed')
            OR (status = 'running' AND (locked_by = $2 OR locked_until < now()))
          )
        RETURNING *
      SQL
      decode_row(row) if row
    end

    def heartbeat(workflow_id:, worker_id:, lease_seconds:)
      execute_params(<<~SQL, [workflow_id, worker_id, lease_seconds])
        UPDATE #{table("workflows")}
        SET locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
        WHERE id = $1 AND locked_by = $2 AND status = 'running'
      SQL
    end

    def workflow_owned?(workflow_id:, worker_id:)
      !!execute_params(<<~SQL, [workflow_id, worker_id]).first
        SELECT 1
        FROM #{table("workflows")}
        WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()
      SQL
    end

    def release_worker_leases!(worker_id:)
      transaction do
        workflows = execute_params(<<~SQL, [worker_id]).cmd_tuples
          UPDATE #{table("workflows")}
          SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now()
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

    def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
      execute_params(<<~SQL, [workflow_id, worker_id, timestamp(run_at)])
        UPDATE #{table("workflows")}
        SET status = 'pending', locked_by = NULL, locked_until = NULL, next_run_at = $3::timestamptz, updated_at = now()
        WHERE id = $1 AND status = 'running' AND locked_by = $2
      SQL
    end

    def make_workflow_due!(workflow_id, now: Time.now)
      execute_params("UPDATE #{table("workflows")} SET next_run_at = NULL, updated_at = $2::timestamptz WHERE id = $1", [workflow_id, timestamp(now)])
    end

    def heartbeat_step(workflow_id:, position:, worker_id:, lease_seconds:, cursor:)
      renewed = transaction do
        workflow = execute_params(<<~SQL, [workflow_id, worker_id, lease_seconds]).first
          UPDATE #{table("workflows")}
          SET locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
          WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()
          RETURNING locked_until
        SQL
        next nil unless workflow

        serialized_cursor = dump_serialized(cursor)
        step = execute_params(<<~SQL, [workflow_id, position, serialized_cursor]).first
          UPDATE #{table("steps")}
          SET heartbeat_cursor = $3::bytea, updated_at = now()
          WHERE workflow_id = $1 AND position = $2 AND status = 'running'
          RETURNING heartbeat_cursor
        SQL
        next nil unless step

        execute_params(<<~SQL, [workflow_id, position, serialized_cursor])
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

    def step_heartbeat_cursor(workflow_id:, position:)
      row = execute_params("SELECT heartbeat_cursor FROM #{table("steps")} WHERE workflow_id = $1 AND position = $2", [workflow_id, position]).first
      decode_row(row).fetch("heartbeat_cursor") if row
    end

    def current_workflow_lease(workflow_id)
      row = execute_params(<<~SQL, [workflow_id]).first
        SELECT id AS workflow_id, locked_by AS worker_id, locked_until
        FROM #{table("workflows")}
        WHERE id = $1 AND status = 'running' AND locked_by IS NOT NULL AND locked_until >= now()
      SQL
      row&.transform_values(&:itself)
    end

    def steal_expired_leases!(now: Time.now)
      result = execute_params(<<~SQL, [timestamp(now)])
        UPDATE #{table("workflows")}
        SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE status = 'running' AND locked_until < $1::timestamptz
      SQL
      result.cmd_tuples
    end

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

    def complete_workflow(workflow_id, result:)
      execute_params(
        "UPDATE #{table("workflows")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1",
        [workflow_id, dump_serialized(result)]
      )
    end

    def fail_workflow(workflow_id, error:)
      execute_params(
        "UPDATE #{table("workflows")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1",
        [workflow_id, error]
      )
    end

    def record_step_started(workflow_id:, position:, name:)
      transaction do
        execute_params(<<~SQL, [workflow_id, position])
          UPDATE #{table("step_attempts")}
          SET status = 'failed', error = 'superseded by retry', completed_at = now()
          WHERE workflow_id = $1 AND position = $2 AND status = 'running'
        SQL
        execute_params(<<~SQL, [workflow_id, position, name])
          INSERT INTO #{table("steps")} (workflow_id, position, name, status, started_at, updated_at)
          VALUES ($1, $2, $3, 'running', now(), now())
          ON CONFLICT (workflow_id, position) DO UPDATE
            SET status = 'running', error = NULL, started_at = COALESCE(#{table("steps")}.started_at, now()), updated_at = now()
        SQL
        attempt_id = SecureRandom.uuid
        execute_params(<<~SQL, [attempt_id, workflow_id, position, name])
          INSERT INTO #{table("step_attempts")} (id, workflow_id, position, name, status)
          VALUES ($1, $2, $3, $4, 'running')
        SQL
        attempt_id
      end
    end

    def record_step_completed(workflow_id:, position:, result:)
      record_step_completed_without_transaction(workflow_id:, position:, result:)
    end

    def record_step_failed(workflow_id:, position:, error:)
      record_step_failed_without_transaction(workflow_id:, position:, error:)
    end

    def record_wait(workflow_id:, position:, name:, wait_request:)
      transaction do
        execute_params(<<~SQL, [workflow_id, position, name, dump_serialized(wait_request.context)])
          INSERT INTO #{table("steps")} (workflow_id, position, name, status, result, started_at, updated_at)
          VALUES ($1, $2, $3, 'waiting', $4::bytea, now(), now())
          ON CONFLICT (workflow_id, position) DO UPDATE
            SET status = 'waiting', result = $4::bytea, error = NULL, updated_at = now()
        SQL
        wait_id = SecureRandom.uuid
        execute_params(<<~SQL, [wait_id, workflow_id, position, wait_request.kind, wait_request.event_key, timestamp_or_nil(wait_request.wake_at), dump_serialized(wait_request.context)])
          INSERT INTO #{table("waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status)
          VALUES ($1, $2, $3, $4, $5, $6::timestamptz, $7::bytea, 'pending')
        SQL
        execute_params("UPDATE #{table("workflows")} SET status = 'waiting', locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id = $1", [workflow_id])
        update_latest_attempt(workflow_id:, position:, status: "waiting", result: wait_request.context, error: nil)
        wait_id
      end
    end

    def wake_due_timers(now: Time.now)
      complete_waits("kind = 'timer' AND wake_at <= $1::timestamptz", [timestamp(now)], {})
    end

    def signal_event(event_key, payload: {})
      complete_waits("kind = 'event' AND event_key = $1", [event_key], payload)
    end

    def waits_for(workflow_id)
      execute_params("SELECT * FROM #{table("waits")} WHERE workflow_id = $1 ORDER BY created_at", [workflow_id]).map { |row| decode_row(row) }
    end

    def with_fence(workflow_id:, key:, poll_interval: 0.05, timeout: 10)
      token = SecureRandom.uuid
      inserted = execute_params(<<~SQL, [workflow_id, key, token, timeout])
        INSERT INTO #{table("fences")} (workflow_id, key, status, locked_by, locked_until)
        VALUES ($1, $2, 'running', $3, now() + ($4::int * interval '1 second'))
        ON CONFLICT (workflow_id, key) DO NOTHING
      SQL

      if inserted.cmd_tuples == 1
        begin
          result = yield
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

        sleep poll_interval
      end
    end

    def enqueue_outbox(workflow_id:, topic:, payload:, key:)
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

    def ack_outbox(outbox_id, worker_id:)
      execute_params("UPDATE #{table("outbox")} SET status = 'processed', processed_at = now() WHERE id = $1 AND locked_by = $2", [outbox_id, worker_id])
    end

    def outbox_message(outbox_id)
      decode_row(execute_params("SELECT * FROM #{table("outbox")} WHERE id = $1", [outbox_id]).first)
    end

    def workflow(workflow_id)
      result = execute_params("SELECT * FROM #{table("workflows")} WHERE id = $1", [workflow_id])
      row = result.first
      raise KeyError, "workflow not found: #{workflow_id}" unless row

      decode_row(row)
    end

    def steps_for(workflow_id)
      execute_params("SELECT * FROM #{table("steps")} WHERE workflow_id = $1 ORDER BY position", [workflow_id]).map { |row| decode_row(row) }
    end

    def step_attempts_for(workflow_id)
      execute_params("SELECT * FROM #{table("step_attempts")} WHERE workflow_id = $1 ORDER BY started_at, position", [workflow_id]).map { |row| decode_row(row) }
    end

    private

    def complete_waits(where_sql, params, payload)
      transaction do
        returning = execute_params(<<~SQL, params + [dump_serialized(payload)])
          UPDATE #{table("waits")}
          SET status = 'completed', payload = $#{params.length + 1}::bytea, completed_at = now()
          WHERE id IN (
            SELECT w.id FROM #{table("waits")} AS w
            JOIN #{table("workflows")} AS wf ON wf.id = w.workflow_id
            WHERE w.status = 'pending'
              AND wf.status = 'waiting'
              AND #{where_sql}
            FOR UPDATE OF w, wf SKIP LOCKED
          )
          RETURNING *
        SQL
        rows = returning.map { |row| decode_row(row) }
        rows.each do |wait|
          context = wait.fetch("context").merge(payload)
          record_step_completed_without_transaction(workflow_id: wait.fetch("workflow_id"), position: wait.fetch("position").to_i, result: context)
          execute_params("UPDATE #{table("workflows")} SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id = $1 AND status = 'waiting'", [wait.fetch("workflow_id")])
        end
        rows.length
      end
    end

    def record_step_completed_without_transaction(workflow_id:, position:, result:)
      serialized = dump_serialized(result)
      execute_params(
        "UPDATE #{table("steps")} SET status = 'completed', result = $3::bytea, error = NULL, completed_at = now(), updated_at = now() WHERE workflow_id = $1 AND position = $2",
        [workflow_id, position, serialized]
      )
      update_latest_attempt_serialized(workflow_id:, position:, status: "completed", serialized_result: serialized, error: nil)
    end

    def record_step_failed_without_transaction(workflow_id:, position:, error:)
      execute_params(
        "UPDATE #{table("steps")} SET status = 'failed', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2",
        [workflow_id, position, error]
      )
      update_latest_attempt_serialized(workflow_id:, position:, status: "failed", serialized_result: dump_serialized(nil), error:)
    end

    def update_latest_attempt(workflow_id:, position:, status:, result:, error:)
      update_latest_attempt_serialized(workflow_id:, position:, status:, serialized_result: dump_serialized(result), error:)
    end

    def update_latest_attempt_serialized(workflow_id:, position:, status:, serialized_result:, error:)
      execute_params(<<~SQL, [workflow_id, position, status, serialized_result, error])
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

    def retry_serialization_failures(max_attempts: 5)
      attempts = 0
      begin
        yield
      rescue PG::TRSerializationFailure
        attempts += 1
        raise if attempts >= max_attempts

        sleep(0.001 * attempts)
        retry
      end
    end

    def execute(sql)
      @connection.exec(sql)
    end

    def transaction(&block)
      @connection.transaction(&block)
    end

    def execute_params(sql, params)
      @connection.exec_params(sql, params)
    end

    def workflow_name_filter(workflow_names)
      return "" unless workflow_names

      names = workflow_names.map { |name| @connection.escape_literal(name) }.join(", ")
      "AND name IN (#{names})"
    end

    def table(name)
      "#{quoted_schema}.#{PG::Connection.quote_ident(name)}"
    end

    def quoted_schema
      PG::Connection.quote_ident(schema)
    end

    def dump_serialized(value)
      "\\x#{SERIALIZER.dump(value).unpack1("H*")}"
    end

    def load_serialized(value)
      return nil if value.nil?

      SERIALIZER.load(PG::Connection.unescape_bytea(value))
    end

    def decode_row(row)
      row.each_with_object({}) do |(column, value), decoded|
        decoded[column] = SERIALIZED_COLUMNS.include?(column) ? load_serialized(value) : value
      end
    end

    def migrate_serialized_columns!
      migrate_serialized_column!("workflows", "input", not_null: true)
      migrate_serialized_column!("workflows", "result")
      migrate_serialized_column!("steps", "result")
      migrate_serialized_column!("steps", "heartbeat_cursor")
      migrate_serialized_column!("step_attempts", "result")
      migrate_serialized_column!("step_attempts", "heartbeat_cursor")
      migrate_serialized_column!("waits", "context", not_null: true)
      migrate_serialized_column!("waits", "payload")
      migrate_serialized_column!("fences", "result")
      migrate_serialized_column!("outbox", "payload", not_null: true)
    end

    def create_performance_indexes!
      execute("CREATE INDEX IF NOT EXISTS workflows_queue_idx ON #{table("workflows")} (status, created_at)")
      execute("CREATE INDEX IF NOT EXISTS workflows_runnable_due_idx ON #{table("workflows")} (status, next_run_at, created_at)")
      execute("CREATE INDEX IF NOT EXISTS workflows_expired_lease_idx ON #{table("workflows")} (status, locked_until)")
      execute("CREATE INDEX IF NOT EXISTS waits_event_pending_idx ON #{table("waits")} (status, kind, event_key, created_at)")
      execute("CREATE INDEX IF NOT EXISTS waits_timer_pending_idx ON #{table("waits")} (status, kind, wake_at, created_at)")
      execute("CREATE INDEX IF NOT EXISTS waits_workflow_created_idx ON #{table("waits")} (workflow_id, created_at)")
      execute("CREATE INDEX IF NOT EXISTS step_attempts_workflow_started_position_idx ON #{table("step_attempts")} (workflow_id, started_at, position)")
      execute("CREATE INDEX IF NOT EXISTS step_attempts_workflow_position_status_started_idx ON #{table("step_attempts")} (workflow_id, position, status, started_at DESC)")
      execute("CREATE INDEX IF NOT EXISTS outbox_queue_idx ON #{table("outbox")} (status, created_at)")
      execute("CREATE INDEX IF NOT EXISTS outbox_expired_lease_idx ON #{table("outbox")} (status, locked_until)")
    end

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
          [dump_serialized({})]
        )
      end
      execute("ALTER TABLE #{table(table_name)} DROP COLUMN #{PG::Connection.quote_ident(column_name)}")
      execute("ALTER TABLE #{table(table_name)} RENAME COLUMN #{PG::Connection.quote_ident(temporary_column)} TO #{PG::Connection.quote_ident(column_name)}")
      execute("ALTER TABLE #{table(table_name)} ALTER COLUMN #{PG::Connection.quote_ident(column_name)} SET NOT NULL") if not_null
    end

    def primary_key_columns(table_name)
      execute_params(<<~SQL, [schema, table_name]).map { |row| row.fetch("column_name") }
        SELECT a.attname AS column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
      SQL
    end


    def timestamp(time)
      time.utc.iso8601(6)
    end

    def timestamp_or_nil(time)
      time ? timestamp(time) : nil
    end
  end
end
