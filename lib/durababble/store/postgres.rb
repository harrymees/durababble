# typed: true
# frozen_string_literal: true

module Durababble
  class PostgresStore < Store
    include PostgresMigrations

    #: () -> untyped
    def drop_schema!
      execute("DROP SCHEMA IF EXISTS #{quoted_schema} CASCADE")
      @migrated = false
    end

    #: (name: untyped, input: untyped) -> untyped
    def enqueue_workflow(name:, input:)
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

      name_filter, name_params = workflow_name_filter(workflow_names)
      row = retry_serialization_failures do
        @connection.transaction(requires_new: true) do
          candidates = []
          candidates.concat(execute_params(<<~SQL, name_params).to_a)
            SELECT id, created_at FROM #{table("workflows")}
            WHERE status = 'pending'
              AND (next_run_at IS NULL OR next_run_at <= now())
              #{name_filter}
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL
          candidates.concat(execute_params(<<~SQL, name_params).to_a)
            SELECT id, created_at FROM #{table("workflows")}
            WHERE status = 'failed'
              AND next_run_at IS NOT NULL
              AND next_run_at <= now()
              #{name_filter}
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL
          candidates.concat(execute_params(<<~SQL, name_params).to_a)
            SELECT id, created_at FROM #{table("workflows")}
            WHERE status = 'canceling'
              AND (next_run_at IS NULL OR next_run_at <= now())
              #{name_filter}
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL
          candidates.concat(execute_params(<<~SQL, name_params).to_a)
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
            status = 'pending'
            OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= now())
            OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= now()))
            OR (status = 'running' AND (locked_by = $2 OR locked_until < now()))
          )
        RETURNING *
      SQL
      decode_row(row) if row
    end

    #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
    def claim_workflow_for_activation(workflow_id:, worker_id:, lease_seconds:)
      already_owned = execute_params(<<~SQL, [workflow_id, worker_id]).first
        SELECT * FROM #{table("workflows")}
        WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
      SQL
      return decode_row(already_owned) if already_owned

      row = execute_params(<<~SQL, [workflow_id, worker_id, lease_seconds]).first
        UPDATE #{table("workflows")}
        SET status = 'running', error = NULL, locked_by = $2,
            locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
        WHERE id = $1
          AND (
            status IN ('pending', 'waiting', 'canceling')
            OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= now())
            OR (status = 'running' AND (locked_by = $2 OR locked_until < now()))
          )
        RETURNING *
      SQL
      decode_row(row) if row
    end

    #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
    def heartbeat(workflow_id:, worker_id:, lease_seconds:)
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
      @connection.transaction(requires_new: true) do
        workflows = execute_params(<<~SQL, [worker_id]).affected_rows
          UPDATE #{table("workflows")}
          SET status = CASE
              WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
              ELSE 'pending'
            END,
            locked_by = NULL, locked_until = NULL, updated_at = now()
          WHERE status = 'running' AND locked_by = $1
        SQL
        outbox = execute_params(<<~SQL, [worker_id]).affected_rows
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
      result = execute_params(<<~SQL, [workflow_id, worker_id])
        UPDATE #{table("workflows")}
        SET status = CASE
              WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
              WHEN EXISTS (SELECT 1 FROM #{table("waits")} WHERE workflow_id = $1 AND status = 'pending') THEN 'waiting'
              ELSE 'pending'
            END,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = now()
        WHERE id = $1 AND status = 'running' AND ($2 IS NULL OR locked_by = $2)
      SQL
      return true if result.affected_rows == 1

      ["pending", "waiting", "canceling"].include?(workflow(workflow_id).fetch("status"))
    end

    #: (untyped, ?now: untyped) -> untyped
    def make_workflow_due!(workflow_id, now: Time.now)
      execute_params("UPDATE #{table("workflows")} SET next_run_at = NULL, updated_at = $2::timestamptz WHERE id = $1", [workflow_id, timestamp(now)])
    end

    #: (workflow_id: untyped, reason: untyped) -> untyped
    def request_workflow_cancellation(workflow_id:, reason:)
      @connection.transaction(requires_new: true) do
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
      command_id = normalize_command_id(command_id, position)
      renewed = @connection.transaction(requires_new: true) do
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
      result = execute_params(<<~SQL, [timestamp(now)])
        UPDATE #{table("workflows")}
        SET status = CASE
              WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
              ELSE 'pending'
            END,
            locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE status = 'running' AND locked_until < $1::timestamptz
      SQL
      result.affected_rows
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
      execute_params(
        "UPDATE #{table("workflows")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1",
        [workflow_id, error],
      )
    end

    #: (workflow_id: untyped, command_id: untyped, name: untyped, ?args: untyped, ?kwargs: untyped, ?metadata: untyped) -> untyped
    def record_step_scheduled(workflow_id:, command_id:, name:, args: [], kwargs: {}, metadata: {})
      payload = { "name" => name, "args" => args, "kwargs" => kwargs }.merge(metadata)
      @connection.transaction(requires_new: true) do
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
      @connection.transaction(requires_new: true) do
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
      @connection.transaction(requires_new: true) { record_step_completed_without_transaction(workflow_id:, command_id:, result:) }
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped) -> untyped
    def record_step_failed(workflow_id:, error:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      @connection.transaction(requires_new: true) { record_step_failed_without_transaction(workflow_id:, command_id:, error:) }
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped) -> untyped
    def record_step_canceled(workflow_id:, error:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      @connection.transaction(requires_new: true) do
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
      @connection.transaction(requires_new: true) do
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
      complete_timer_waits(timestamp(now))
    end

    #: (untyped, ?payload: untyped) -> untyped
    def signal_event(event_key, payload: {})
      complete_event_waits(event_key, payload)
    end

    #: (untyped) -> untyped
    def waits_for(workflow_id)
      execute_params("SELECT * FROM #{table("waits")} WHERE workflow_id = $1 ORDER BY created_at", [workflow_id]).map { |row| decode_row(row) }
    end

    #: (workflow_id: untyped, key: untyped, ?poll_interval: untyped, ?timeout: untyped) { (?) -> untyped } -> untyped
    def with_fence(workflow_id:, key:, poll_interval: 0.05, timeout: 10, &block)
      token = SecureRandom.uuid
      inserted = execute_params(<<~SQL, [workflow_id, key, token, timeout])
        INSERT INTO #{table("fences")} (workflow_id, key, status, locked_by, locked_until)
        VALUES ($1, $2, 'running', $3, now() + ($4::int * interval '1 second'))
        ON CONFLICT (workflow_id, key) DO NOTHING
      SQL

      if inserted.affected_rows == 1
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
        @connection.transaction(requires_new: true) do
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
      execute_params("UPDATE #{table("outbox")} SET status = 'processed', processed_at = now() WHERE id = $1 AND locked_by = $2", [outbox_id, worker_id])
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

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, message_kind: untyped, ?method_name: untyped, ?payload: untyped, ?idempotency_key: untyped, ?ready_at: untyped, ?max_attempts: untyped) -> untyped
    def enqueue_inbox_message(target_kind:, target_type:, target_id:, message_kind:, method_name: nil, payload: {}, idempotency_key: nil, ready_at: nil, max_attempts: nil)
      shape_hash = inbox_shape_hash(target_kind:, target_type:, target_id:, message_kind:, method_name:, payload:)
      retry_serialization_failures do
        @connection.transaction(requires_new: true) do
          existing = existing_inbox_message_for_idempotency(idempotency_key, target_kind:, target_type:, target_id:)
          if existing
            raise IdempotencyKeyConflict, "idempotency key #{idempotency_key} already used for a different inbox message" unless existing.fetch("shape_hash") == shape_hash

            upsert_target_activation_without_transaction(
              target_kind: existing.fetch("target_kind"),
              target_type: existing.fetch("target_type"),
              target_id: existing.fetch("target_id"),
              ready_at: existing["ready_at"],
            ) if activatable_inbox_status?(existing.fetch("status"))
            next existing.fetch("id")
          end

          sequence = allocate_mailbox_sequence(target_kind:, target_type:, target_id:)
          id = SecureRandom.uuid
          operation_id = id
          execute_params(<<~SQL, [id, target_kind, target_type, target_id, sequence, message_kind, method_name, operation_id, idempotency_key, shape_hash, dump_serialized(payload), timestamp_or_nil(ready_at), max_attempts])
            INSERT INTO #{table("inbox")} (
              id, target_kind, target_type, target_id, sequence, message_kind, method_name,
              operation_id, idempotency_key, shape_hash, payload, status, ready_at, max_attempts
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::bytea, 'pending', $12::timestamptz, $13)
          SQL
          upsert_target_activation_without_transaction(target_kind:, target_type:, target_id:, ready_at:)
          id
        end
      end
    end

    #: (workflow_id: untyped, workflow_name: untyped, method_name: untyped, payload: untyped, ?idempotency_key: untyped) -> untyped
    def enqueue_workflow_command(workflow_id:, workflow_name:, method_name:, payload:, idempotency_key: nil)
      target_kind = "workflow"
      message_kind = "workflow_command"
      shape_hash = inbox_shape_hash(target_kind:, target_type: workflow_name, target_id: workflow_id, message_kind:, method_name:, payload:)
      retry_serialization_failures do
        @connection.transaction(requires_new: true) do
          existing = existing_inbox_message_for_idempotency(idempotency_key, target_kind:, target_type: workflow_name, target_id: workflow_id)
          if existing
            raise IdempotencyKeyConflict, "idempotency key #{idempotency_key} already used for a different inbox message" unless existing.fetch("shape_hash") == shape_hash

            upsert_target_activation_without_transaction(
              target_kind: existing.fetch("target_kind"),
              target_type: existing.fetch("target_type"),
              target_id: existing.fetch("target_id"),
              ready_at: existing["ready_at"],
            ) if activatable_inbox_status?(existing.fetch("status"))
            next existing.fetch("id")
          end

          workflow = execute_params("SELECT * FROM #{table("workflows")} WHERE id = $1 FOR UPDATE", [workflow_id]).first
          raise KeyError, "workflow not found: #{workflow_id}" unless workflow

          raise Error, "workflow #{workflow_id} is terminal" if terminal_for_cancellation?(decode_row(workflow))

          sequence = allocate_mailbox_sequence(target_kind:, target_type: workflow_name, target_id: workflow_id)
          id = SecureRandom.uuid
          execute_params(<<~SQL, [id, target_kind, workflow_name, workflow_id, sequence, message_kind, method_name, id, idempotency_key, shape_hash, dump_serialized(payload)])
            INSERT INTO #{table("inbox")} (
              id, target_kind, target_type, target_id, sequence, message_kind, method_name,
              operation_id, idempotency_key, shape_hash, payload, status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::bytea, 'pending')
          SQL
          upsert_target_activation_without_transaction(target_kind:, target_type: workflow_name, target_id: workflow_id)
          id
        end
      end
    end

    #: (worker_id: untyped, lease_seconds: untyped, ?target_kinds: untyped, ?target_types: untyped, ?now: untyped) -> untyped
    def claim_target_activation(worker_id:, lease_seconds:, target_kinds: nil, target_types: nil, now: Time.now)
      return if target_kinds&.empty? || target_types&.empty?

      filter_sql, filter_params = target_activation_filter(target_kinds:, target_types:, offset: 2)
      row = retry_serialization_failures do
        @connection.transaction(requires_new: true) do
          candidates = []
          candidates.concat(execute_params(<<~SQL, [timestamp(now)] + filter_params).to_a)
            SELECT target_kind, target_type, target_id, ready_at, created_at FROM #{table("target_activations")}
            WHERE status = 'pending' AND ready_at <= $1::timestamptz
              #{filter_sql}
            ORDER BY ready_at, created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL
          candidates.concat(execute_params(<<~SQL, [timestamp(now)] + filter_params).to_a)
            SELECT target_kind, target_type, target_id, ready_at, created_at FROM #{table("target_activations")}
            WHERE status = 'running' AND locked_until < $1::timestamptz
              #{filter_sql}
            ORDER BY ready_at, created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          SQL
          candidate = candidates.min_by { |candidate_row| Time.parse(candidate_row.fetch("created_at").to_s) }
          next nil unless candidate

          execute_params(<<~SQL, [candidate.fetch("target_kind"), candidate.fetch("target_type"), candidate.fetch("target_id"), worker_id, lease_seconds]).first
            UPDATE #{table("target_activations")}
            SET status = 'running',
                locked_by = $4,
                locked_until = now() + ($5::int * interval '1 second'),
                updated_at = now()
            WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
            RETURNING *
          SQL
        end
      end
      decode_row(row) if row
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, worker_id: untyped, ?now: untyped) -> untyped
    def complete_target_activation(target_kind:, target_type:, target_id:, worker_id:, now: Time.now)
      @connection.transaction(requires_new: true) do
        activation = execute_params(<<~SQL, [target_kind, target_type, target_id, worker_id]).first
          SELECT 1 FROM #{table("target_activations")}
          WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
            AND status = 'running' AND locked_by = $4
          FOR UPDATE
        SQL
        next nil unless activation

        reconcile_target_activation_without_transaction(target_kind:, target_type:, target_id:, now:)
      end
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def target_activation(target_kind:, target_type:, target_id:)
      row = execute_params(<<~SQL, [target_kind, target_type, target_id]).first
        SELECT * FROM #{table("target_activations")}
        WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
      SQL
      decode_row(row) if row
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, worker_id: untyped, ?lease_seconds: untyped, ?limit: untyped, ?now: untyped) -> untyped
    def claim_inbox_messages(target_kind:, target_type:, target_id:, worker_id:, lease_seconds: 60, limit: 1, now: Time.now)
      @connection.transaction(requires_new: true) do
        rows = execute_params(<<~SQL, [target_kind, target_type, target_id, limit])
          SELECT *
          FROM #{table("inbox")}
          WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
            AND status IN ('pending', 'failed', 'running', 'dead_lettered')
          ORDER BY sequence
          LIMIT $4
          FOR UPDATE
        SQL
        claimable = contiguous_claimable_inbox_rows(rows, now:)
        claimable.each do |row|
          execute_params(<<~SQL, [row.fetch("id"), worker_id, lease_seconds])
            UPDATE #{table("inbox")}
            SET status = 'running',
                attempts = attempts + 1,
                locked_by = $2,
                locked_until = now() + ($3::int * interval '1 second'),
                updated_at = now()
            WHERE id = $1
          SQL
        end
        claimable.map { |row| decode_row(execute_params("SELECT * FROM #{table("inbox")} WHERE id = $1", [row.fetch("id")]).first) }
      end
    end

    #: (untyped) -> untyped
    def inbox_message(message_id)
      row = execute_params("SELECT * FROM #{table("inbox")} WHERE id = $1", [message_id]).first
      decode_row(row) if row
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def inbox_messages_for(target_kind:, target_type:, target_id:)
      execute_params(<<~SQL, [target_kind, target_type, target_id]).map { |row| decode_row(row) }
        SELECT * FROM #{table("inbox")}
        WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
        ORDER BY sequence
      SQL
    end

    #: (command_id: untyped, worker_id: untyped, ?lease_seconds: untyped) -> untyped
    def claim_object_command(command_id:, worker_id:, lease_seconds: 60)
      row = inbox_message(command_id)
      return unless object_command_message?(row)
      return object_command_row(row) unless row.key?("target_kind")

      claimed = claim_inbox_message_by_id(
        message_id: command_id,
        target_kind: row.fetch("target_kind"),
        target_type: row.fetch("target_type"),
        target_id: row.fetch("target_id"),
        worker_id:,
        lease_seconds:,
      )
      return unless claimed

      object_command_row(claimed)
    end

    #: (command_id: untyped, result: untyped, ?object_type: untyped, ?object_id: untyped, ?state: untyped, ?worker_id: untyped) -> untyped
    def complete_object_command(command_id:, result:, object_type: nil, object_id: nil, state: NO_OBJECT_STATE, worker_id: nil)
      @connection.transaction(requires_new: true) do
        command = lock_object_command_for_completion(command_id:, worker_id:)
        next nil unless command

        save_object_state(object_type:, object_id:, state:) unless state.equal?(NO_OBJECT_STATE)
        updated = execute_params(
          "UPDATE #{table("inbox")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = now(), updated_at = now() WHERE id = $1",
          [command_id, dump_serialized(result)],
        )
        reconcile_target_activation_without_transaction(target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id")) if command.key?("target_kind")
        updated
      end
    end

    #: (command_id: untyped, error: untyped, ?worker_id: untyped) -> untyped
    def fail_object_command(command_id:, error:, worker_id: nil)
      @connection.transaction(requires_new: true) do
        command = lock_inbox_message_for_failure(command_id:, worker_id:)
        next nil unless command

        updated = execute_params(<<~SQL, [command_id, error])
          UPDATE #{table("inbox")}
          SET status = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'dead_lettered' ELSE 'failed' END,
              error = $2,
              locked_by = NULL,
              locked_until = NULL,
              dead_lettered_at = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN now() ELSE dead_lettered_at END,
              updated_at = now()
          WHERE id = $1
        SQL
        reconcile_target_activation_without_transaction(target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id")) if command.key?("target_kind")
        updated
      end
    end

    #: (message_id: untyped, workflow_id: untyped, result: untyped, worker_id: untyped) -> untyped
    def complete_workflow_command(message_id:, workflow_id:, result:, worker_id:)
      @connection.transaction(requires_new: true) do
        command = lock_inbox_message_for_completion(message_id:, worker_id:)
        next nil unless command

        append_workflow_history_without_transaction(
          workflow_id:,
          kind: "workflow_command_completed",
          name: command["method_name"],
          attempt_id: message_id,
          payload: { "message_id" => message_id, "result" => result },
        )
        updated = execute_params(
          "UPDATE #{table("inbox")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = now(), updated_at = now() WHERE id = $1",
          [message_id, dump_serialized(result)],
        )
        reconcile_target_activation_without_transaction(target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id"))
        updated
      end
    end

    #: (message_id: untyped, workflow_id: untyped, error: untyped, worker_id: untyped) -> untyped
    def fail_workflow_command(message_id:, workflow_id:, error:, worker_id:)
      @connection.transaction(requires_new: true) do
        command = lock_inbox_message_for_failure(command_id: message_id, worker_id:)
        next nil unless command

        append_workflow_history_without_transaction(
          workflow_id:,
          kind: "workflow_command_failed",
          name: command["method_name"],
          attempt_id: message_id,
          payload: { "message_id" => message_id },
          error:,
        )
        updated = execute_params(<<~SQL, [message_id, error])
          UPDATE #{table("inbox")}
          SET status = 'dead_lettered',
              error = $2,
              locked_by = NULL,
              locked_until = NULL,
              dead_lettered_at = now(),
              updated_at = now()
          WHERE id = $1
        SQL
        reconcile_target_activation_without_transaction(target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id"))
        updated
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
          SELECT * FROM #{table("inbox")}
          WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
          FOR UPDATE
        SQL
      else
        execute_params("SELECT * FROM #{table("inbox")} WHERE id = $1 FOR UPDATE", [command_id]).first
      end
    end

    #: (message_id: untyped, worker_id: untyped) -> untyped
    def lock_inbox_message_for_completion(message_id:, worker_id:)
      execute_params(<<~SQL, [message_id, worker_id]).first
        SELECT * FROM #{table("inbox")}
        WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
        FOR UPDATE
      SQL
    end

    #: (command_id: untyped, worker_id: untyped) -> untyped
    def lock_inbox_message_for_failure(command_id:, worker_id:)
      if worker_id
        execute_params(<<~SQL, [command_id, worker_id]).first
          SELECT * FROM #{table("inbox")}
          WHERE id = $1 AND status = 'running' AND locked_by = $2
          FOR UPDATE
        SQL
      else
        execute_params("SELECT * FROM #{table("inbox")} WHERE id = $1 FOR UPDATE", [command_id]).first
      end
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ?ready_at: untyped) -> untyped
    def upsert_target_activation_without_transaction(target_kind:, target_type:, target_id:, ready_at: nil)
      ready_timestamp = timestamp_or_nil(ready_at) || timestamp(Time.now)
      execute_params(<<~SQL, [target_kind, target_type, target_id, ready_timestamp])
        INSERT INTO #{table("target_activations")} (target_kind, target_type, target_id, status, ready_at)
        VALUES ($1, $2, $3, 'pending', $4::timestamptz)
        ON CONFLICT (target_kind, target_type, target_id) DO UPDATE
          SET status = CASE
                WHEN #{table("target_activations")}.status = 'running' THEN #{table("target_activations")}.status
                ELSE 'pending'
              END,
              ready_at = LEAST(#{table("target_activations")}.ready_at, EXCLUDED.ready_at),
              updated_at = now()
      SQL
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ?now: untyped) -> untyped
    def reconcile_target_activation_without_transaction(target_kind:, target_type:, target_id:, now: Time.now)
      head = execute_params(<<~SQL, [target_kind, target_type, target_id]).first
        SELECT *
        FROM #{table("inbox")}
        WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
          AND status IN ('pending', 'failed', 'running', 'dead_lettered')
        ORDER BY sequence
        LIMIT 1
        FOR UPDATE
      SQL

      if head && head.fetch("status") != "dead_lettered"
        ready_at = target_activation_ready_at_for(head, now:)
        set_target_activation_pending_without_transaction(target_kind:, target_type:, target_id:, ready_at:)
      else
        execute_params(<<~SQL, [target_kind, target_type, target_id])
          DELETE FROM #{table("target_activations")}
          WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
        SQL
      end
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ready_at: untyped) -> untyped
    def set_target_activation_pending_without_transaction(target_kind:, target_type:, target_id:, ready_at:)
      ready_timestamp = timestamp_or_nil(ready_at) || timestamp(Time.now)
      execute_params(<<~SQL, [target_kind, target_type, target_id, ready_timestamp])
        INSERT INTO #{table("target_activations")} (target_kind, target_type, target_id, status, ready_at)
        VALUES ($1, $2, $3, 'pending', $4::timestamptz)
        ON CONFLICT (target_kind, target_type, target_id) DO UPDATE
          SET status = 'pending',
              ready_at = EXCLUDED.ready_at,
              locked_by = NULL,
              locked_until = NULL,
              updated_at = now()
      SQL
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def allocate_mailbox_sequence(target_kind:, target_type:, target_id:)
      execute_params(<<~SQL, [target_kind, target_type, target_id])
        INSERT INTO #{table("mailbox_sequences")} (target_kind, target_type, target_id, last_sequence)
        VALUES ($1, $2, $3, 0)
        ON CONFLICT (target_kind, target_type, target_id) DO NOTHING
      SQL
      row = execute_params(<<~SQL, [target_kind, target_type, target_id]).first
        SELECT last_sequence
        FROM #{table("mailbox_sequences")}
        WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
        FOR UPDATE
      SQL
      sequence = row.fetch("last_sequence").to_i + 1
      execute_params(<<~SQL, [target_kind, target_type, target_id, sequence])
        UPDATE #{table("mailbox_sequences")}
        SET last_sequence = $4, updated_at = now()
        WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
      SQL
      sequence
    end

    #: (untyped, target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def existing_inbox_message_for_idempotency(idempotency_key, target_kind:, target_type:, target_id:)
      return unless idempotency_key

      execute_params(<<~SQL, [target_kind, target_type, target_id, idempotency_key]).first
        SELECT id, target_kind, target_type, target_id, status, ready_at, shape_hash
        FROM #{table("inbox")}
        WHERE target_kind = $1 AND target_type = $2 AND target_id = $3 AND idempotency_key = $4
        FOR UPDATE
      SQL
    end

    #: (message_id: untyped, target_kind: untyped, target_type: untyped, target_id: untyped, worker_id: untyped, lease_seconds: untyped, ?now: untyped) -> untyped
    def claim_inbox_message_by_id(message_id:, target_kind:, target_type:, target_id:, worker_id:, lease_seconds:, now: Time.now)
      @connection.transaction(requires_new: true) do
        head = execute_params(<<~SQL, [target_kind, target_type, target_id]).first
          SELECT *
          FROM #{table("inbox")}
          WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
            AND status IN ('pending', 'failed', 'running', 'dead_lettered')
          ORDER BY sequence
          LIMIT 1
          FOR UPDATE
        SQL
        next unless head&.fetch("id") == message_id
        next unless inbox_row_claimable?(head, now:)

        execute_params(<<~SQL, [message_id, worker_id, lease_seconds])
          UPDATE #{table("inbox")}
          SET status = 'running',
              attempts = attempts + 1,
              locked_by = $2,
              locked_until = now() + ($3::int * interval '1 second'),
              updated_at = now()
          WHERE id = $1
        SQL
        decode_row(execute_params("SELECT * FROM #{table("inbox")} WHERE id = $1", [message_id]).first)
      end
    end

    #: (untyped) -> bool
    def activatable_inbox_status?(status)
      ["pending", "failed", "running"].include?(status)
    end

    #: (target_kinds: untyped, target_types: untyped, ?offset: untyped) -> untyped
    def target_activation_filter(target_kinds:, target_types:, offset: 1)
      filters = []
      params = []
      if target_kinds
        filters << "target_kind IN (#{postgres_placeholders(offset + params.length, target_kinds.length)})"
        params.concat(target_kinds)
      end
      if target_types
        filters << "target_type IN (#{postgres_placeholders(offset + params.length, target_types.length)})"
        params.concat(target_types)
      end
      return ["", []] if filters.empty?

      ["AND #{filters.join(" AND ")}", params]
    end

    #: (untyped, untyped) -> untyped
    def complete_event_waits(event_key, payload)
      @connection.transaction(requires_new: true) do
        returning = execute_params(<<~SQL, [event_key, dump_serialized(payload)])
          UPDATE #{table("waits")}
          SET status = 'completed', payload = $2::bytea, completed_at = now()
          WHERE id IN (
            SELECT w.id FROM #{table("waits")} AS w
            JOIN #{table("workflows")} AS wf ON wf.id = w.workflow_id
            WHERE w.status = 'pending'
              AND wf.status IN ('waiting', 'running')
              AND w.kind = 'event'
              AND w.event_key = $1
            FOR UPDATE OF w, wf SKIP LOCKED
          )
          RETURNING *
        SQL
        finish_completed_waits(returning, payload)
      end
    end

    #: (untyped) -> untyped
    def complete_timer_waits(now)
      @connection.transaction(requires_new: true) do
        returning = execute_params(<<~SQL, [now, dump_serialized({})])
          UPDATE #{table("waits")}
          SET status = 'completed', payload = $2::bytea, completed_at = now()
          WHERE id IN (
            SELECT w.id FROM #{table("waits")} AS w
            JOIN #{table("workflows")} AS wf ON wf.id = w.workflow_id
            WHERE w.status = 'pending'
              AND wf.status IN ('waiting', 'running')
              AND w.kind = 'timer'
              AND w.wake_at <= $1::timestamptz
            FOR UPDATE OF w, wf SKIP LOCKED
          )
          RETURNING *
        SQL
        finish_completed_waits(returning, {})
      end
    end

    #: (untyped, untyped) -> untyped
    def finish_completed_waits(returning, payload)
      rows = returning.map { |row| decode_row(row) }
      rows.each do |wait|
        context = wait.fetch("context").merge(payload)
        record_step_completed_without_transaction(workflow_id: wait.fetch("workflow_id"), command_id: wait.fetch("position").to_i, result: context)
        execute_params("UPDATE #{table("workflows")} SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id = $1 AND status = 'waiting'", [wait.fetch("workflow_id")])
      end
      rows.length
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
      rescue ActiveRecord::SerializationFailure
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
        @connection.exec_query(sql)
      rescue ActiveRecord::SerializationFailure, ActiveRecord::Deadlocked
        attempts += 1
        raise if attempts >= 5

        sleep(0.01 * attempts)
        retry
      end
    end

    #: (untyped, untyped) -> untyped
    def execute_params(sql, params)
      @connection.exec_query(sql, "Durababble SQL", params, prepare: false)
    end

    #: (untyped) -> untyped
    def workflow_name_filter(workflow_names)
      return ["", []] unless workflow_names

      ["AND name IN (#{postgres_placeholders(1, workflow_names.length)})", workflow_names]
    end

    #: (untyped, untyped) -> untyped
    def postgres_placeholders(offset, count)
      count.times.map { |index| "$#{offset + index}" }.join(", ")
    end

    #: (untyped) -> untyped
    def table(name)
      "#{quoted_schema}.#{@connection.quote_column_name(name.to_s)}"
    end

    #: () -> untyped
    def quoted_schema
      @connection.quote_column_name(schema.to_s)
    end

    #: (untyped) -> untyped
    def dump_serialized(value)
      "\\x#{SERIALIZER.dump(value).unpack1("H*")}"
    end

    #: (untyped) -> untyped
    def load_serialized(value)
      return if value.nil?

      bytes = if value.is_a?(String) && value.start_with?("\\x")
        [value.delete_prefix("\\x")].pack("H*")
      else
        value
      end
      SERIALIZER.load(bytes)
    end

    #: (untyped) -> untyped
    def timestamp(time)
      return time if time.is_a?(String)

      time.utc.iso8601(6)
    end

    #: (untyped) -> untyped
    def timestamp_or_nil(time)
      time ? timestamp(time) : nil
    end
  end
end
