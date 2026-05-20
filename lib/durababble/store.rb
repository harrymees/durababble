# frozen_string_literal: true

require "json"
require "pg"
require "securerandom"
require "time"

module Durababble
  class Store
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
          input jsonb NOT NULL DEFAULT '{}'::jsonb,
          result jsonb,
          error text,
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      SQL
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS locked_by text")
      execute("ALTER TABLE #{table("workflows")} ADD COLUMN IF NOT EXISTS locked_until timestamptz")
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("steps")} (
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          position integer NOT NULL,
          name text NOT NULL,
          status text NOT NULL,
          result jsonb,
          error text,
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
          result jsonb,
          error text,
          started_at timestamptz NOT NULL DEFAULT now(),
          completed_at timestamptz
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("waits")} (
          id text PRIMARY KEY,
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          position integer NOT NULL,
          kind text NOT NULL,
          event_key text,
          wake_at timestamptz,
          context jsonb NOT NULL DEFAULT '{}'::jsonb,
          payload jsonb,
          status text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          completed_at timestamptz
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("fences")} (
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          key text NOT NULL,
          result jsonb NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (workflow_id, key)
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("outbox")} (
          id text PRIMARY KEY,
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          topic text NOT NULL,
          payload jsonb NOT NULL,
          key text NOT NULL UNIQUE,
          status text NOT NULL,
          locked_by text,
          locked_until timestamptz,
          created_at timestamptz NOT NULL DEFAULT now(),
          processed_at timestamptz
        )
      SQL
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
        "INSERT INTO #{table("workflows")} (id, name, status, input) VALUES ($1, $2, 'pending', $3::jsonb)",
        [id, name, dump_json(input)]
      )
      id
    end

    def create_workflow(name:, input:)
      id = enqueue_workflow(name:, input:)
      mark_workflow_running(id)
      id
    end

    def claim_runnable_workflow(worker_id:, lease_seconds:)
      row = execute_params(<<~SQL, [worker_id, lease_seconds]).first
        UPDATE #{table("workflows")}
        SET status = 'running', locked_by = $1, locked_until = now() + ($2::int * interval '1 second'), updated_at = now()
        WHERE id = (
          SELECT id FROM #{table("workflows")}
          WHERE status IN ('pending', 'failed')
             OR (status = 'running' AND locked_until < now())
          ORDER BY created_at
          LIMIT 1
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

    def steal_expired_leases!(now: Time.now)
      result = execute_params(<<~SQL, [timestamp(now)])
        UPDATE #{table("workflows")}
        SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE status = 'running' AND locked_until < $1::timestamptz
      SQL
      result.cmd_tuples
    end

    def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60)
      execute_params(<<~SQL, [workflow_id, worker_id, lease_seconds])
        UPDATE #{table("workflows")}
        SET status = 'running', error = NULL, locked_by = COALESCE($2, locked_by),
            locked_until = CASE WHEN $2 IS NULL THEN locked_until ELSE now() + ($3::int * interval '1 second') END,
            updated_at = now()
        WHERE id = $1
      SQL
    end

    def complete_workflow(workflow_id, result:)
      execute_params(
        "UPDATE #{table("workflows")} SET status = 'completed', result = $2::jsonb, error = NULL, locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id = $1",
        [workflow_id, dump_json(result)]
      )
    end

    def fail_workflow(workflow_id, error:)
      execute_params(
        "UPDATE #{table("workflows")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id = $1",
        [workflow_id, error]
      )
    end

    def record_step_started(workflow_id:, position:, name:)
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

    def record_step_completed(workflow_id:, position:, result:)
      execute_params(
        "UPDATE #{table("steps")} SET status = 'completed', result = $3::jsonb, error = NULL, completed_at = now(), updated_at = now() WHERE workflow_id = $1 AND position = $2",
        [workflow_id, position, dump_json(result)]
      )
      update_latest_attempt(workflow_id:, position:, status: "completed", result:, error: nil)
    end

    def record_step_failed(workflow_id:, position:, error:)
      execute_params(
        "UPDATE #{table("steps")} SET status = 'failed', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2",
        [workflow_id, position, error]
      )
      update_latest_attempt(workflow_id:, position:, status: "failed", result: nil, error:)
    end

    def record_wait(workflow_id:, position:, name:, wait_request:)
      execute_params(<<~SQL, [workflow_id, position, name, dump_json(wait_request.context)])
        INSERT INTO #{table("steps")} (workflow_id, position, name, status, result, started_at, updated_at)
        VALUES ($1, $2, $3, 'waiting', $4::jsonb, now(), now())
        ON CONFLICT (workflow_id, position) DO UPDATE
          SET status = 'waiting', result = $4::jsonb, error = NULL, updated_at = now()
      SQL
      wait_id = SecureRandom.uuid
      execute_params(<<~SQL, [wait_id, workflow_id, position, wait_request.kind, wait_request.event_key, timestamp_or_nil(wait_request.wake_at), dump_json(wait_request.context)])
        INSERT INTO #{table("waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status)
        VALUES ($1, $2, $3, $4, $5, $6::timestamptz, $7::jsonb, 'pending')
      SQL
      execute_params("UPDATE #{table("workflows")} SET status = 'waiting', locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id = $1", [workflow_id])
      update_latest_attempt(workflow_id:, position:, status: "waiting", result: wait_request.context, error: nil)
      wait_id
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

    def with_fence(workflow_id:, key:)
      existing = execute_params("SELECT result FROM #{table("fences")} WHERE workflow_id = $1 AND key = $2", [workflow_id, key]).first
      return decode_json(existing.fetch("result")) if existing

      result = yield
      execute_params("INSERT INTO #{table("fences")} (workflow_id, key, result) VALUES ($1, $2, $3::jsonb) ON CONFLICT (workflow_id, key) DO NOTHING", [workflow_id, key, dump_json(result)])
      row = execute_params("SELECT result FROM #{table("fences")} WHERE workflow_id = $1 AND key = $2", [workflow_id, key]).first
      decode_json(row.fetch("result"))
    end

    def enqueue_outbox(workflow_id:, topic:, payload:, key:)
      existing = execute_params("SELECT id FROM #{table("outbox")} WHERE key = $1", [key]).first
      return existing.fetch("id") if existing

      id = SecureRandom.uuid
      execute_params(<<~SQL, [id, workflow_id, topic, dump_json(payload), key])
        INSERT INTO #{table("outbox")} (id, workflow_id, topic, payload, key, status)
        VALUES ($1, $2, $3, $4::jsonb, $5, 'pending')
        ON CONFLICT (key) DO NOTHING
      SQL
      execute_params("SELECT id FROM #{table("outbox")} WHERE key = $1", [key]).first.fetch("id")
    end

    def claim_outbox(worker_id:, lease_seconds:)
      row = execute_params(<<~SQL, [worker_id, lease_seconds]).first
        UPDATE #{table("outbox")}
        SET status = 'processing', locked_by = $1, locked_until = now() + ($2::int * interval '1 second')
        WHERE id = (
          SELECT id FROM #{table("outbox")}
          WHERE status = 'pending' OR (status = 'processing' AND locked_until < now())
          ORDER BY created_at
          LIMIT 1
        )
        RETURNING *
      SQL
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
      rows = execute_params("SELECT * FROM #{table("waits")} WHERE status = 'pending' AND #{where_sql}", params).map { |row| decode_row(row) }
      rows.each do |wait|
        context = wait.fetch("context").merge(payload)
        execute_params("UPDATE #{table("waits")} SET status = 'completed', payload = $2::jsonb, completed_at = now() WHERE id = $1", [wait.fetch("id"), dump_json(payload)])
        record_step_completed(workflow_id: wait.fetch("workflow_id"), position: wait.fetch("position").to_i, result: context)
        execute_params("UPDATE #{table("workflows")} SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id = $1", [wait.fetch("workflow_id")])
      end
      rows.length
    end

    def update_latest_attempt(workflow_id:, position:, status:, result:, error:)
      row = execute_params("SELECT id FROM #{table("step_attempts")} WHERE workflow_id = $1 AND position = $2 AND status = 'running' ORDER BY started_at DESC LIMIT 1", [workflow_id, position]).first
      return unless row

      execute_params("UPDATE #{table("step_attempts")} SET status = $2, result = $3::jsonb, error = $4, completed_at = now() WHERE id = $1", [row.fetch("id"), status, dump_json(result), error])
    end

    def execute(sql)
      @connection.exec(sql)
    end

    def execute_params(sql, params)
      @connection.exec_params(sql, params)
    end

    def table(name)
      "#{quoted_schema}.#{PG::Connection.quote_ident(name)}"
    end

    def quoted_schema
      PG::Connection.quote_ident(schema)
    end

    def dump_json(value)
      JSON.generate(value || {})
    end

    def decode_json(value)
      value.is_a?(String) ? JSON.parse(value) : value
    end

    def decode_row(row)
      row.transform_values do |value|
        next value unless value.is_a?(String)

        begin
          JSON.parse(value)
        rescue JSON::ParserError
          value
        end
      end
    end

    def timestamp(time)
      time.utc.iso8601(6)
    end

    def timestamp_or_nil(time)
      time ? timestamp(time) : nil
    end
  end
end
