# typed: true
# frozen_string_literal: true

module Durababble
  class MysqlStore < SqlStore
    include MysqlMigrations

    class << self
      #: (uri: untyped, schema: untyped) -> untyped
      def connect(uri:, schema:)
        Store.connect(database_url: uri.to_s, schema:)
      end
    end

    #: () -> untyped
    def drop_schema!
      ["durable_object_commands", "target_activations", "inbox", "mailbox_sequences", "durable_objects", "waits", "outbox", "fences", "step_attempts", "steps", "workflow_history", "workflows"].each { |name| execute("DROP TABLE IF EXISTS #{table(name)}") }
      @migrated = false
    end

    #: (name: untyped, input: untyped) -> untyped
    def enqueue_workflow(name:, input:)
      id = SecureRandom.uuid
      execute_params("INSERT INTO #{table("workflows")} (id, name, status, input) VALUES (?, ?, 'pending', ?)", [id, name, dump_serialized(input)])
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
        name_sql, name_params = workflow_name_filter(workflow_names)
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
              status = 'pending'
              OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))
              OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
              OR (status = 'running' AND locked_until < NOW(6))
            )
        SQL
        next unless updated.affected_rows == 1

        claimed = workflow(candidate.fetch("id"))
        observe_claim_latency(claimed, "workflow")
        claimed
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
              status = 'pending'
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
    def claim_workflow_for_activation(workflow_id:, worker_id:, lease_seconds:)
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
              status IN ('pending', 'waiting', 'canceling')
              OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))
              OR (status = 'running' AND (locked_by = ? OR locked_until < NOW(6)))
            )
          FOR UPDATE SKIP LOCKED
        SQL
        next unless row

        execute_params(<<~SQL, [worker_id, lease_seconds, workflow_id])
          UPDATE #{table("workflows")}
          SET status = 'running', error = NULL, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
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
      owned = workflow_owned?(workflow_id:, worker_id:)
      if owned
        Observability.count("durababble.leases.heartbeats", "durababble.workflow.id" => workflow_id, "durababble.worker.id" => worker_id)
      else
        Observability.count("durababble.leases.conflicts", "durababble.workflow.id" => workflow_id, "durababble.worker.id" => worker_id)
      end
      ActiveRecord::Result.empty(affected_rows: owned ? 1 : 0)
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
        released = { "workflows" => workflows, "outbox" => outbox }
        Observability.count("durababble.leases.expired_recovery", { "durababble.worker.id" => worker_id }, by: workflows.to_i)
        released
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
      result = execute_params(<<~SQL, [workflow_id, workflow_id, worker_id, worker_id])
        UPDATE #{table("workflows")}
        SET status = CASE
              WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
              WHEN EXISTS (SELECT 1 FROM #{table("waits")} WHERE workflow_id = ? AND status = 'pending') THEN 'waiting'
              ELSE 'pending'
            END,
            locked_by = NULL,
            locked_until = NULL,
            updated_at = NOW(6)
        WHERE id = ? AND status = 'running' AND (? IS NULL OR locked_by = ?)
      SQL
      return true if result.affected_rows == 1

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
      Observability.count("durababble.leases.expired_recovery", by: expired)
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
      execute_params(<<~SQL, [error, workflow_id])
        UPDATE #{table("workflows")}
        SET status = 'failed', error = ?, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ?
      SQL
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

    #: (workflow_id: untyped, topic: untyped, payload: untyped, key: untyped) -> untyped
    def enqueue_outbox(workflow_id:, topic:, payload:, key:)
      existing = execute_params("SELECT id FROM #{table("outbox")} WHERE `key` = ?", [key]).first
      return existing.fetch("id") if existing

      id = SecureRandom.uuid
      execute_params(<<~SQL, [id, workflow_id, topic, dump_serialized(payload), key])
        INSERT IGNORE INTO #{table("outbox")} (id, workflow_id, topic, payload, `key`, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
      SQL
      Observability.count("durababble.outbox.pending", "durababble.workflow.id" => workflow_id, "durababble.outbox.topic" => topic)
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
        message = outbox_message(candidate.fetch("id"))
        observe_claim_latency(message, "outbox")
        message
      end
    end

    #: (untyped, worker_id: untyped) -> untyped
    def ack_outbox(outbox_id, worker_id:)
      result = execute_params("UPDATE #{table("outbox")} SET status = 'processed', processed_at = NOW(6) WHERE id = ? AND locked_by = ?", [outbox_id, worker_id])
      Observability.count("durababble.outbox.processed", "durababble.worker.id" => worker_id) if result.affected_rows.to_i.positive?
      result
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

    #: (object_type: untyped, object_id: untyped, state: untyped) -> untyped
    def save_object_state(object_type:, object_id:, state:)
      execute_params(<<~SQL, [object_type, object_id, dump_serialized(state)])
        INSERT INTO #{table("durable_objects")} (object_type, object_id, state)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE state = VALUES(state), updated_at = NOW(6)
      SQL
      state
    end

    #: (worker_id: untyped, lease_seconds: untyped, ?target_kinds: untyped, ?target_types: untyped, ?now: untyped) -> untyped
    def claim_target_activation(worker_id:, lease_seconds:, target_kinds: nil, target_types: nil, now: Time.now)
      return if target_kinds&.empty? || target_types&.empty?

      transaction do
        filter_sql, filter_params = target_activation_filter_sql(target_kinds:, target_types:)
        candidates = []
        candidates.concat(execute_params(<<~SQL, [now] + filter_params).to_a)
          SELECT target_kind, target_type, target_id, ready_at, created_at FROM #{table("target_activations")}
          WHERE status = 'pending' AND ready_at <= ?
            #{filter_sql}
          ORDER BY ready_at, created_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        SQL
        candidates.concat(execute_params(<<~SQL, [now] + filter_params).to_a)
          SELECT target_kind, target_type, target_id, ready_at, created_at FROM #{table("target_activations")}
          WHERE status = 'running' AND locked_until < ?
            #{filter_sql}
          ORDER BY ready_at, created_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        SQL
        candidate = candidates.min_by { |candidate_row| candidate_row.fetch("created_at").to_s }
        next unless candidate

        execute_params(<<~SQL, [worker_id, lease_seconds, candidate.fetch("target_kind"), candidate.fetch("target_type"), candidate.fetch("target_id")])
          UPDATE #{table("target_activations")}
          SET status = 'running',
              locked_by = ?,
              locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND),
              updated_at = NOW(6)
          WHERE target_kind = ? AND target_type = ? AND target_id = ?
        SQL
        target_activation(target_kind: candidate.fetch("target_kind"), target_type: candidate.fetch("target_type"), target_id: candidate.fetch("target_id"))
      end
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, worker_id: untyped, ?now: untyped) -> untyped
    def complete_target_activation(target_kind:, target_type:, target_id:, worker_id:, now: Time.now)
      transaction do
        activation = execute_params(<<~SQL, [target_kind, target_type, target_id, worker_id]).first
          SELECT 1 FROM #{table("target_activations")}
          WHERE target_kind = ? AND target_type = ? AND target_id = ?
            AND status = 'running' AND locked_by = ?
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

    #: (command_id: untyped, worker_id: untyped) -> untyped
    def lock_object_command_for_completion(command_id:, worker_id:)
      if worker_id
        execute_params(<<~SQL, [command_id, worker_id]).first
          SELECT * FROM #{table("inbox")}
          WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
          FOR UPDATE
        SQL
      else
        execute_params("SELECT * FROM #{table("inbox")} WHERE id = ? FOR UPDATE", [command_id]).first
      end
    end

    #: (message_id: untyped, worker_id: untyped) -> untyped
    def lock_inbox_message_for_completion(message_id:, worker_id:)
      execute_params(<<~SQL, [message_id, worker_id]).first
        SELECT * FROM #{table("inbox")}
        WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
        FOR UPDATE
      SQL
    end

    #: (command_id: untyped, worker_id: untyped) -> untyped
    def lock_inbox_message_for_failure(command_id:, worker_id:)
      if worker_id
        execute_params(<<~SQL, [command_id, worker_id]).first
          SELECT * FROM #{table("inbox")}
          WHERE id = ? AND status = 'running' AND locked_by = ?
          FOR UPDATE
        SQL
      else
        execute_params("SELECT * FROM #{table("inbox")} WHERE id = ? FOR UPDATE", [command_id]).first
      end
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ?ready_at: untyped) -> untyped
    def upsert_target_activation_without_transaction(target_kind:, target_type:, target_id:, ready_at: nil)
      ready_time = ready_at || Time.now.utc
      execute_params(<<~SQL, [target_kind, target_type, target_id, ready_time])
        INSERT INTO #{table("target_activations")} (target_kind, target_type, target_id, status, ready_at)
        VALUES (?, ?, ?, 'pending', ?)
        ON DUPLICATE KEY UPDATE
          status = IF(status = 'running', status, 'pending'),
          ready_at = LEAST(ready_at, VALUES(ready_at)),
          updated_at = NOW(6)
      SQL
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ?now: untyped) -> untyped
    def reconcile_target_activation_without_transaction(target_kind:, target_type:, target_id:, now: Time.now)
      head = execute_params(<<~SQL, [target_kind, target_type, target_id]).first
        SELECT *
        FROM #{table("inbox")}
        WHERE target_kind = ? AND target_type = ? AND target_id = ?
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
          WHERE target_kind = ? AND target_type = ? AND target_id = ?
        SQL
      end
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, ready_at: untyped) -> untyped
    def set_target_activation_pending_without_transaction(target_kind:, target_type:, target_id:, ready_at:)
      ready_time = ready_at || Time.now.utc
      execute_params(<<~SQL, [target_kind, target_type, target_id, ready_time])
        INSERT INTO #{table("target_activations")} (target_kind, target_type, target_id, status, ready_at)
        VALUES (?, ?, ?, 'pending', ?)
        ON DUPLICATE KEY UPDATE
          status = 'pending',
          ready_at = VALUES(ready_at),
          locked_by = NULL,
          locked_until = NULL,
          updated_at = NOW(6)
      SQL
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def allocate_mailbox_sequence(target_kind:, target_type:, target_id:)
      execute_params(<<~SQL, [target_kind, target_type, target_id])
        INSERT IGNORE INTO #{table("mailbox_sequences")} (target_kind, target_type, target_id, last_sequence)
        VALUES (?, ?, ?, 0)
      SQL
      row = execute_params(<<~SQL, [target_kind, target_type, target_id]).first
        SELECT last_sequence
        FROM #{table("mailbox_sequences")}
        WHERE target_kind = ? AND target_type = ? AND target_id = ?
        FOR UPDATE
      SQL
      sequence = row.fetch("last_sequence").to_i + 1
      execute_params(<<~SQL, [sequence, target_kind, target_type, target_id])
        UPDATE #{table("mailbox_sequences")}
        SET last_sequence = ?, updated_at = NOW(6)
        WHERE target_kind = ? AND target_type = ? AND target_id = ?
      SQL
      sequence
    end

    #: (untyped, target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def existing_inbox_message_for_idempotency(idempotency_key, target_kind:, target_type:, target_id:)
      return unless idempotency_key

      execute_params(<<~SQL, [target_kind, target_type, target_id, idempotency_key]).first
        SELECT id, target_kind, target_type, target_id, status, ready_at, shape_hash
        FROM #{table("inbox")}
        WHERE target_kind = ? AND target_type = ? AND target_id = ? AND idempotency_key = ?
        FOR UPDATE
      SQL
    end

    #: (untyped) -> untyped
    def lock_workflow_for_update(workflow_id)
      execute_params("SELECT * FROM #{table("workflows")} WHERE id = ? FOR UPDATE", [workflow_id]).first
    end

    #: (id: untyped, target_kind: untyped, target_type: untyped, target_id: untyped, sequence: untyped, message_kind: untyped, method_name: untyped, operation_id: untyped, idempotency_key: untyped, shape_hash: untyped, payload: untyped, ?ready_at: untyped, ?max_attempts: untyped) -> untyped
    def insert_inbox_message_without_transaction(id:, target_kind:, target_type:, target_id:, sequence:, message_kind:, method_name:, operation_id:, idempotency_key:, shape_hash:, payload:, ready_at: nil, max_attempts: nil)
      execute_params(<<~SQL, [id, target_kind, target_type, target_id, sequence, message_kind, method_name, operation_id, idempotency_key, shape_hash, dump_serialized(payload), ready_at, max_attempts])
        INSERT INTO #{table("inbox")} (
          id, target_kind, target_type, target_id, sequence, message_kind, method_name,
          operation_id, idempotency_key, shape_hash, payload, status, ready_at, max_attempts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
      SQL
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped, limit: untyped) -> untyped
    def inbox_claim_rows_for_update(target_kind:, target_type:, target_id:, limit:)
      # ActiveRecord quotes MySQL sanitized numeric binds, which is invalid in LIMIT.
      execute_params(<<~SQL, [target_kind, target_type, target_id]).to_a
        SELECT *
        FROM #{table("inbox")}
        WHERE target_kind = ? AND target_type = ? AND target_id = ?
          AND status IN ('pending', 'failed', 'running', 'dead_lettered')
        ORDER BY sequence
        LIMIT #{Integer(limit)}
        FOR UPDATE
      SQL
    end

    #: (target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
    def inbox_head_for_update(target_kind:, target_type:, target_id:)
      execute_params(<<~SQL, [target_kind, target_type, target_id]).first
        SELECT *
        FROM #{table("inbox")}
        WHERE target_kind = ? AND target_type = ? AND target_id = ?
          AND status IN ('pending', 'failed', 'running', 'dead_lettered')
        ORDER BY sequence
        LIMIT 1
        FOR UPDATE
      SQL
    end

    #: (message_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
    def mark_inbox_row_running_without_transaction(message_id:, worker_id:, lease_seconds:)
      execute_params(<<~SQL, [worker_id, lease_seconds, message_id])
        UPDATE #{table("inbox")}
        SET status = 'running',
            attempts = attempts + 1,
            locked_by = ?,
            locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND),
            updated_at = NOW(6)
        WHERE id = ?
      SQL
    end

    #: (message_id: untyped, result: untyped) -> untyped
    def complete_inbox_message_without_transaction(message_id:, result:)
      execute_params(
        "UPDATE #{table("inbox")} SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = NOW(6), updated_at = NOW(6) WHERE id = ?",
        [dump_serialized(result), message_id],
      )
    end

    #: (message_id: untyped, error: untyped) -> untyped
    def fail_inbox_message_without_transaction(message_id:, error:)
      execute_params(
        "UPDATE #{table("inbox")} SET status = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'dead_lettered' ELSE 'failed' END, error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN NOW(6) ELSE dead_lettered_at END, updated_at = NOW(6) WHERE id = ?",
        [error, message_id],
      )
    end

    #: (message_id: untyped, error: untyped) -> untyped
    def dead_letter_inbox_message_without_transaction(message_id:, error:)
      execute_params(
        "UPDATE #{table("inbox")} SET status = 'dead_lettered', error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = NOW(6), updated_at = NOW(6) WHERE id = ?",
        [error, message_id],
      )
    end

    #: (target_kinds: untyped, target_types: untyped) -> untyped
    def target_activation_filter_sql(target_kinds:, target_types:)
      filters = []
      params = []
      if target_kinds
        filters << "target_kind IN (#{mysql_placeholders(target_kinds.length)})"
        params.concat(target_kinds)
      end
      if target_types
        filters << "target_type IN (#{mysql_placeholders(target_types.length)})"
        params.concat(target_types)
      end
      return ["", []] if filters.empty?

      ["AND #{filters.join(" AND ")}", params]
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

    #: (untyped, untyped) -> untyped
    def complete_event_waits(event_key, payload)
      transaction do
        waits = execute_params(<<~SQL, [event_key]).map { |row| decode_row(row) }
          SELECT w.* FROM #{table("waits")} AS w
          JOIN #{table("workflows")} AS wf ON wf.id = w.workflow_id
          WHERE w.status = 'pending'
            AND wf.status IN ('waiting', 'running')
            AND w.kind = 'event'
            AND w.event_key = ?
          FOR UPDATE SKIP LOCKED
        SQL
        finish_completed_waits(waits, payload)
      end
    end

    #: (untyped) -> untyped
    def complete_timer_waits(now)
      transaction do
        waits = execute_params(<<~SQL, [now]).map { |row| decode_row(row) }
          SELECT w.* FROM #{table("waits")} AS w
          JOIN #{table("workflows")} AS wf ON wf.id = w.workflow_id
          WHERE w.status = 'pending'
            AND wf.status IN ('waiting', 'running')
            AND w.kind = 'timer'
            AND w.wake_at <= ?
          FOR UPDATE SKIP LOCKED
        SQL
        finish_completed_waits(waits, {})
      end
    end

    #: (untyped, untyped) -> untyped
    def finish_completed_waits(waits, payload)
      waits.each do |wait|
        execute_params("UPDATE #{table("waits")} SET status = 'completed', payload = ?, completed_at = NOW(6) WHERE id = ?", [dump_serialized(payload), wait.fetch("id")])
        record_wait_latency(wait)
        context = wait.fetch("context").merge(payload)
        record_step_completed_without_transaction(workflow_id: wait.fetch("workflow_id"), command_id: wait.fetch("position").to_i, result: context)
        execute_params("UPDATE #{table("workflows")} SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6) WHERE id = ? AND status = 'waiting'", [wait.fetch("workflow_id")])
      end
      Observability.count("durababble.waits.completed", by: waits.length)
      waits.length
    end

    #: (untyped) -> untyped
    def execute(sql)
      @connection.exec_query(sql)
    end

    #: (untyped, untyped) -> untyped
    def execute_params(sql, params)
      if trilogy_connection?
        @connection.exec_query(sanitizer_class.send(:sanitize_sql_array, [sql, *params]), "Durababble SQL")
      else
        @connection.exec_query(sql, "Durababble SQL", params, prepare: false)
      end
    end

    #: () { (?) -> untyped } -> untyped
    def transaction(&block)
      attempts = 0
      begin
        @connection.transaction(requires_new: true, &block)
      rescue StandardError => error
        if retryable_mysql_error?(error) && attempts < 5
          attempts += 1
          sleep(0.01 * attempts)
          retry
        end

        raise
      end
    end

    #: (untyped) -> untyped
    def workflow_name_filter(workflow_names)
      return ["", []] unless workflow_names

      ["AND name IN (#{mysql_placeholders(workflow_names.length)})", workflow_names]
    end

    #: (untyped) -> untyped
    def mysql_placeholders(count)
      Array.new(count, "?").join(", ")
    end

    #: (untyped) -> untyped
    def placeholder(_index)
      "?"
    end

    #: (untyped) -> untyped
    def timestamp_or_nil(time)
      time
    end

    #: (untyped) -> untyped
    def table(name)
      @connection.quote_column_name("#{table_prefix}_#{name}")
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
    def dump_serialized(value)
      SERIALIZER.dump(value)
    end

    #: (untyped) -> untyped
    def load_serialized(value)
      return if value.nil?

      SERIALIZER.load(value)
    end

    #: (untyped) -> untyped
    def retryable_mysql_error?(error)
      error.is_a?(ActiveRecord::Deadlocked) ||
        error.class.name == "ActiveRecord::LockWaitTimeout"
    end

    #: () -> bool
    def trilogy_connection?
      @connection.adapter_name.to_s.downcase.include?("trilogy")
    end

    #: () -> untyped
    def sanitizer_class
      durababble_connection = @connection
      @sanitizer_class ||= Class.new do
        extend ActiveRecord::Sanitization::ClassMethods

        define_singleton_method(:with_connection) do |&block|
          block.call(durababble_connection)
        end
      end
    end
  end
end
