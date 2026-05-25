# typed: true
# frozen_string_literal: true

module Durababble
  class PostgresStore < SqlStore
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

    #: (worker_id: untyped, lease_seconds: untyped, ?workflow_names: untyped) -> untyped
    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil)
      return if workflow_names&.empty?

      name_filter, name_params = workflow_name_filter(workflow_names)
      row = retry_serialization_failures do
        @connection.transaction(requires_new: true) do
          candidates = []
          candidates.concat(execute_params(store_query_sql(:pg_claim_pending_workflow, name_filter:), name_params).to_a)
          candidates.concat(execute_params(store_query_sql(:pg_claim_due_pending_workflow, name_filter:), name_params).to_a)
          candidates.concat(execute_params(store_query_sql(:pg_claim_failed_workflow, name_filter:), name_params).to_a)
          candidates.concat(execute_params(store_query_sql(:pg_claim_canceling_workflow, name_filter:), name_params).to_a)
          candidates.concat(execute_params(store_query_sql(:pg_claim_expired_workflow, name_filter:), name_params).to_a)

          candidate = candidates.min_by { |candidate_row| Time.parse(candidate_row.fetch("created_at").to_s) }
          next nil unless candidate

          execute_params(store_query_sql(:pg_claim_selected_workflow), [candidate.fetch("id"), worker_id, lease_seconds]).first
        end
      end
      observe_claim_latency(row, "workflow") if row
      decode_row(row) if row
    end

    #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
    def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
      already_owned = execute_params(store_query_sql(:pg_claim_workflow_already_owned), [workflow_id, worker_id]).first
      return decode_row(already_owned) if already_owned

      row = execute_params(store_query_sql(:pg_claim_workflow_update), [workflow_id, worker_id, lease_seconds]).first
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
            locked_until = now() + ($3::int * interval '1 second'), next_run_at = NULL, runnable_immediately = true, updated_at = now()
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
      result = execute_params(store_query_sql(:pg_heartbeat_workflow), [workflow_id, worker_id, lease_seconds])
      if result.affected_rows.to_i.positive?
        Observability.count("durababble.leases.heartbeats", "durababble.workflow.id" => workflow_id, "durababble.worker.id" => worker_id)
      else
        Observability.count("durababble.leases.conflicts", "durababble.workflow.id" => workflow_id, "durababble.worker.id" => worker_id)
      end
      result
    end

    #: (workflow_id: untyped, worker_id: untyped) -> untyped
    def workflow_owned?(workflow_id:, worker_id:)
      !!execute_params(store_query_sql(:pg_workflow_owned), [workflow_id, worker_id]).first
    end

    #: (worker_id: untyped) -> untyped
    def release_worker_leases!(worker_id:)
      @connection.transaction(requires_new: true) do
        workflows = execute_params(store_query_sql(:pg_release_workflow_leases), [worker_id]).affected_rows
        outbox = execute_params(store_query_sql(:pg_release_outbox_leases), [worker_id]).affected_rows
        inbox = execute_params(store_query_sql(:pg_release_inbox_leases), [worker_id]).affected_rows
        target_activations = execute_params(store_query_sql(:pg_release_target_activation_leases), [worker_id]).affected_rows
        released = { "workflows" => workflows, "outbox" => outbox, "inbox" => inbox, "target_activations" => target_activations }
        Observability.count("durababble.leases.expired_recovery", { "durababble.worker.id" => worker_id }, by: released.values.sum)
        released
      end
    end

    #: (workflow_id: untyped, worker_id: untyped, run_at: untyped) -> untyped
    def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
      result = execute_params(<<~SQL, [workflow_id, worker_id, timestamp(run_at)])
        UPDATE #{table("workflows")}
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, next_run_at = $3::timestamptz, runnable_immediately = false, updated_at = now()
        WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
      SQL
      result.affected_rows.to_i == 1 ? result : nil
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
            runnable_immediately = true,
            updated_at = now()
        WHERE id = $1 AND status = 'running'
          AND ($2::text IS NULL OR (locked_by = $2::text AND locked_until >= now()))
      SQL
      return true if result.affected_rows == 1

      WorkflowStatus.suspended_or_runnable?(workflow(workflow_id))
    end

    #: (untyped, ?now: untyped) -> untyped
    def make_workflow_due!(workflow_id, now: Time.now)
      execute_params("UPDATE #{table("workflows")} SET next_run_at = NULL, runnable_immediately = true, updated_at = $2::timestamptz WHERE id = $1", [workflow_id, timestamp(now)])
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

        if first_request && !WorkflowStatus.running?(decoded)
          execute_params(<<~SQL, [workflow_id])
            UPDATE #{table("workflows")}
            SET status = 'canceling', locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now()
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
        workflow = execute_params(store_query_sql(:pg_heartbeat_step_workflow), [workflow_id, worker_id, lease_seconds]).first
        next nil unless workflow

        serialized_cursor = dump_serialized(cursor)
        step = execute_params(store_query_sql(:pg_heartbeat_step_row), [workflow_id, command_id, serialized_cursor]).first
        next nil unless step

        execute_params(store_query_sql(:pg_heartbeat_latest_attempt), [workflow_id, command_id, serialized_cursor])
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
      row = execute_params(store_query_sql(:pg_current_workflow_lease), [workflow_id]).first
      row&.transform_values(&:itself)
    end

    #: (untyped, untyped) -> untyped
    def current_object_lease(object_type, object_id)
      row = execute_params(<<~SQL, [object_type, object_id]).first
        SELECT target_id AS object_id, locked_by AS worker_id, locked_until
        FROM #{table("inbox")}
        WHERE target_kind = 'object' AND target_type = $1 AND target_id = $2 AND status = 'running'
          AND locked_by IS NOT NULL AND locked_until >= now()
        ORDER BY sequence
        LIMIT 1
      SQL
      row&.transform_values(&:itself)
    end

    #: (?now: untyped) -> untyped
    def steal_expired_leases!(now: Time.now)
      result = execute_params(store_query_sql(:pg_steal_expired_leases), [timestamp(now)])
      Observability.count("durababble.leases.expired_recovery", by: result.affected_rows.to_i)
      result.affected_rows
    end

    #: (untyped, ?worker_id: untyped, ?lease_seconds: untyped) -> untyped
    def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60)
      if worker_id
        claim_workflow(workflow_id:, worker_id:, lease_seconds:)
      else
        execute_params(<<~SQL, [workflow_id])
          UPDATE #{table("workflows")}
          SET status = 'running', error = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now()
          WHERE id = $1
        SQL
      end
    end

    #: (untyped, result: untyped, ?worker_id: untyped) -> untyped
    def complete_workflow(workflow_id, result:, worker_id: nil)
      update = if worker_id
        execute_params(
          "UPDATE #{table("workflows")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $3 AND locked_until >= now()",
          [workflow_id, dump_serialized(result), worker_id],
        )
      else
        execute_params(
          "UPDATE #{table("workflows")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1",
          [workflow_id, dump_serialized(result)],
        )
      end
      require_fenced_workflow_update!(update, workflow_id:, worker_id:, operation: "workflow completion")
    end

    #: (untyped, reason: untyped, ?result: untyped, ?worker_id: untyped) -> untyped
    def cancel_workflow(workflow_id, reason:, result: nil, worker_id: nil)
      update = if worker_id
        execute_params(
          "UPDATE #{table("workflows")} SET status = 'canceled', result = $2::bytea, error = $3, cancel_reason = COALESCE(cancel_reason, $3), cancel_requested_at = COALESCE(cancel_requested_at, now()), locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $4 AND locked_until >= now()",
          [workflow_id, dump_serialized(result), reason, worker_id],
        )
      else
        execute_params(
          "UPDATE #{table("workflows")} SET status = 'canceled', result = $2::bytea, error = $3, cancel_reason = COALESCE(cancel_reason, $3), cancel_requested_at = COALESCE(cancel_requested_at, now()), locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1",
          [workflow_id, dump_serialized(result), reason],
        )
      end
      require_fenced_workflow_update!(update, workflow_id:, worker_id:, operation: "workflow cancellation")
    end

    #: (untyped, error: untyped, ?worker_id: untyped) -> untyped
    def fail_workflow(workflow_id, error:, worker_id: nil)
      update = if worker_id
        execute_params(
          "UPDATE #{table("workflows")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $3 AND locked_until >= now()",
          [workflow_id, error, worker_id],
        )
      else
        execute_params(
          "UPDATE #{table("workflows")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1",
          [workflow_id, error],
        )
      end
      require_fenced_workflow_update!(update, workflow_id:, worker_id:, operation: "workflow failure")
    end

    #: (workflow_id: untyped, command_id: untyped, name: untyped, ?args: untyped, ?kwargs: untyped, ?metadata: untyped, ?worker_id: untyped) -> untyped
    def record_step_scheduled(workflow_id:, command_id:, name:, args: [], kwargs: {}, metadata: {}, worker_id: nil)
      payload = { "name" => name, "args" => args, "kwargs" => kwargs }.merge(metadata)
      @connection.transaction(requires_new: true) do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        append_workflow_history_without_transaction(workflow_id:, kind: "step_scheduled", command_id:, name:, payload:)
        execute_params(<<~SQL, [workflow_id, command_id, name])
          INSERT INTO #{table("steps")} (workflow_id, position, name, status, updated_at)
          VALUES ($1, $2, $3, 'scheduled', now())
          ON CONFLICT (workflow_id, position) DO NOTHING
        SQL
      end
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, name: untyped, ?worker_id: untyped) -> untyped
    def record_step_started(workflow_id:, name:, command_id: nil, position: nil, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      @connection.transaction(requires_new: true) do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        execute_params(store_query_sql(:pg_supersede_running_step_attempts), [workflow_id, command_id])
        execute_params(store_query_sql(:pg_upsert_step_running), [workflow_id, command_id, name])
        attempt_id = SecureRandom.uuid
        execute_params(store_query_sql(:pg_insert_step_attempt), [attempt_id, workflow_id, command_id, name])
        append_workflow_history_without_transaction(workflow_id:, kind: "step_started", command_id:, name:, attempt_id:)
        attempt_id
      end
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped, ?worker_id: untyped) -> untyped
    def record_step_canceled(workflow_id:, error:, command_id: nil, position: nil, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      @connection.transaction(requires_new: true) do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        execute_params(
          "UPDATE #{table("steps")} SET status = 'canceled', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2 AND status IN ('scheduled', 'running', 'waiting')",
          [workflow_id, command_id, error],
        )
        update_latest_attempt_serialized(workflow_id:, command_id:, status: "canceled", serialized_result: dump_serialized(nil), error:)
        append_workflow_history_without_transaction(workflow_id:, kind: "step_canceled", command_id:, error:)
      end
    end

    #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, name: untyped, wait_request: untyped, ?suspend_workflow: untyped, ?worker_id: untyped) -> untyped
    def record_wait(workflow_id:, name:, wait_request:, command_id: nil, position: nil, suspend_workflow: true, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      @connection.transaction(requires_new: true) do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        execute_params(store_query_sql(:pg_upsert_waiting_step), [workflow_id, command_id, name, dump_serialized(wait_request.context)])
        wait_id = SecureRandom.uuid
        execute_params(store_query_sql(:pg_insert_wait), [wait_id, workflow_id, command_id, wait_request.kind, wait_request.event_key, timestamp_or_nil(wait_request.wake_at), dump_serialized(wait_request.context)])
        update_latest_attempt(workflow_id:, command_id:, status: "waiting", result: wait_request.context, error: nil)
        append_workflow_history_without_transaction(workflow_id:, kind: "step_waiting", command_id:, name:, payload: wait_request.context)
        if suspend_workflow && !suspend_workflow(workflow_id:, worker_id:)
          raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before wait suspension"
        end

        Observability.count(
          "durababble.waits.started",
          "durababble.workflow.id" => workflow_id,
          "durababble.step.index" => command_id,
          "durababble.step.name" => name,
          "durababble.wait.kind" => wait_request.kind,
          "durababble.wait.event_key" => wait_request.event_key,
        )
        wait_id
      end
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
        unless decoded
          raise FenceTimeout, "timed out waiting for fence #{key}" if Time.now >= deadline

          sleep(poll_interval)
          next
        end

        case decoded.fetch("status")
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
      existing = execute_params(store_query_sql(:pg_outbox_by_key), [key]).first
      return existing.fetch("id") if existing

      id = SecureRandom.uuid
      execute_params(store_query_sql(:pg_insert_outbox), [id, workflow_id, topic, dump_serialized(payload), key])
      Observability.count("durababble.outbox.pending", "durababble.workflow.id" => workflow_id, "durababble.outbox.topic" => topic)
      execute_params(store_query_sql(:pg_outbox_by_key), [key]).first.fetch("id")
    end

    #: (worker_id: untyped, lease_seconds: untyped) -> untyped
    def claim_outbox(worker_id:, lease_seconds:)
      row = retry_serialization_failures do
        @connection.transaction(requires_new: true) do
          candidates = []
          candidates.concat(execute_params(store_query_sql(:pg_claim_pending_outbox), []).to_a)
          candidates.concat(execute_params(store_query_sql(:pg_claim_expired_outbox), []).to_a)

          candidate = candidates.min_by { |candidate_row| Time.parse(candidate_row.fetch("created_at").to_s) }
          next nil unless candidate

          execute_params(store_query_sql(:pg_claim_selected_outbox), [candidate.fetch("id"), worker_id, lease_seconds]).first
        end
      end
      observe_claim_latency(row, "outbox") if row
      decode_row(row) if row
    end

    #: (untyped, worker_id: untyped) -> untyped
    def ack_outbox(outbox_id, worker_id:)
      result = execute_params(store_query_sql(:pg_ack_outbox), [outbox_id, worker_id])
      Observability.count("durababble.outbox.processed", "durababble.worker.id" => worker_id) if result.affected_rows.to_i.positive?
      result
    end

    #: (object_type: untyped, object_id: untyped, state: untyped) -> untyped
    def save_object_state(object_type:, object_id:, state:)
      execute_params(store_query_sql(:pg_save_object_state), [object_type, object_id, dump_serialized(state)])
      state
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

    private

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
          WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
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

      if head && !InboxStatus.dead_lettered?(head)
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

    #: (untyped) -> untyped
    def lock_workflow_for_update(workflow_id)
      execute_params("SELECT * FROM #{table("workflows")} WHERE id = $1 FOR UPDATE", [workflow_id]).first
    end

    #: (workflow_id: untyped, worker_id: untyped) -> untyped
    def lock_owned_workflow_for_update(workflow_id:, worker_id:)
      execute_params(<<~SQL, [workflow_id, worker_id]).first
        SELECT 1
        FROM #{table("workflows")}
        WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
        FOR UPDATE
      SQL
    end

    #: (id: untyped, target_kind: untyped, target_type: untyped, target_id: untyped, sequence: untyped, message_kind: untyped, method_name: untyped, operation_id: untyped, idempotency_key: untyped, shape_hash: untyped, payload: untyped, ?ready_at: untyped, ?max_attempts: untyped) -> untyped
    def insert_inbox_message_without_transaction(id:, target_kind:, target_type:, target_id:, sequence:, message_kind:, method_name:, operation_id:, idempotency_key:, shape_hash:, payload:, ready_at: nil, max_attempts: nil)
      execute_params(<<~SQL, [id, target_kind, target_type, target_id, sequence, message_kind, method_name, operation_id, idempotency_key, shape_hash, dump_serialized(payload), timestamp_or_nil(ready_at), max_attempts])
        INSERT INTO #{table("inbox")} (
          id, target_kind, target_type, target_id, sequence, message_kind, method_name,
          operation_id, idempotency_key, shape_hash, payload, status, ready_at, max_attempts
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::bytea, 'pending', $12::timestamptz, $13)
      SQL
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, limit: untyped) -> untyped
    def inbox_claim_rows_for_update(target_kind:, target_type:, target_id:, limit:)
      execute_params(<<~SQL, [target_kind, target_type, target_id, limit])
        SELECT *
        FROM #{table("inbox")}
        WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
          AND status IN ('pending', 'failed', 'running', 'dead_lettered')
        ORDER BY sequence
        LIMIT $4
        FOR UPDATE
      SQL
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def inbox_head_for_update(target_kind:, target_type:, target_id:)
      execute_params(<<~SQL, [target_kind, target_type, target_id]).first
        SELECT *
        FROM #{table("inbox")}
        WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
          AND status IN ('pending', 'failed', 'running', 'dead_lettered')
        ORDER BY sequence
        LIMIT 1
        FOR UPDATE
      SQL
    end

    #: (message_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
    def mark_inbox_row_running_without_transaction(message_id:, worker_id:, lease_seconds:)
      execute_params(<<~SQL, [message_id, worker_id, lease_seconds])
        UPDATE #{table("inbox")}
        SET status = 'running',
            attempts = attempts + 1,
            locked_by = $2,
            locked_until = now() + ($3::int * interval '1 second'),
            updated_at = now()
        WHERE id = $1
      SQL
    end

    #: (message_id: untyped, result: untyped) -> untyped
    def complete_inbox_message_without_transaction(message_id:, result:)
      execute_params(
        "UPDATE #{table("inbox")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = now(), updated_at = now() WHERE id = $1",
        [message_id, dump_serialized(result)],
      )
    end

    #: (message_id: untyped, error: untyped) -> untyped
    def fail_inbox_message_without_transaction(message_id:, error:)
      execute_params(<<~SQL, [message_id, error])
        UPDATE #{table("inbox")}
        SET status = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'dead_lettered' ELSE 'failed' END,
            error = $2,
            locked_by = NULL,
            locked_until = NULL,
            dead_lettered_at = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN now() ELSE dead_lettered_at END,
            updated_at = now()
        WHERE id = $1
      SQL
    end

    #: (message_id: untyped, error: untyped, ready_at: untyped) -> untyped
    def retry_inbox_message_without_transaction(message_id:, error:, ready_at:)
      execute_params(<<~SQL, [message_id, error, timestamp(ready_at)])
        UPDATE #{table("inbox")}
        SET status = 'pending',
            error = $2,
            ready_at = $3::timestamptz,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = now()
        WHERE id = $1
      SQL
    end

    #: (message_id: untyped, error: untyped) -> untyped
    def dead_letter_inbox_message_without_transaction(message_id:, error:)
      execute_params(<<~SQL, [message_id, error])
        UPDATE #{table("inbox")}
        SET status = 'dead_lettered',
            error = $2,
            locked_by = NULL,
            locked_until = NULL,
            dead_lettered_at = now(),
            updated_at = now()
        WHERE id = $1
      SQL
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

    #: (untyped) -> untyped
    def complete_timer_waits(now)
      @connection.transaction(requires_new: true) do
        returning = execute_params(store_query_sql(:pg_complete_waits, where_sql: "w.kind = 'timer'\n    AND w.wake_at <= $1::timestamptz", payload_param: 2), [now, dump_serialized({})])
        finish_completed_waits(returning, {})
      end
    end

    #: (untyped, untyped) -> untyped
    def finish_completed_waits(returning, payload)
      rows = returning.map { |row| decode_row(row) }
      rows.each do |wait|
        record_wait_latency(wait)
        context = wait.fetch("context").merge(payload)
        record_step_completed_without_transaction(workflow_id: wait.fetch("workflow_id"), command_id: wait.fetch("position").to_i, result: context)
        execute_params(store_query_sql(:pg_mark_wait_workflow_pending), [wait.fetch("workflow_id")])
      end
      Observability.count("durababble.waits.completed", by: rows.length)
      rows.length
    end

    #: (workflow_id: untyped, command_id: untyped, result: untyped) -> untyped
    def record_step_completed_without_transaction(workflow_id:, command_id:, result:)
      serialized = dump_serialized(result)
      execute_params(store_query_sql(:pg_complete_step), [workflow_id, command_id, serialized])
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
      execute_params(store_query_sql(:pg_update_latest_attempt), [workflow_id, command_id, status, serialized_result, error])
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
        raise if attempts >= 20

        sleep(0.05 * attempts)
        retry
      end
    end

    #: (untyped, untyped) -> untyped
    def execute_params(sql, params)
      @connection.exec_query(sql, "Durababble SQL", params, prepare: false)
    end

    #: () { (?) -> untyped } -> untyped
    def transaction(&block)
      retry_serialization_failures { @connection.transaction(requires_new: true, &block) }
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
    def placeholder(index)
      "$#{index}"
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
