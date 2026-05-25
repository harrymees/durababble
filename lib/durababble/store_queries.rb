# typed: true
# frozen_string_literal: true

module Durababble
  module StoreQueries
    Query = Struct.new(:id, :backend, :builder, keyword_init: true)

    QUERIES = {}

    class << self
      #: (untyped, backend: untyped) { (untyped) -> untyped } -> void
      def define(id, backend:, &builder)
        query_id = id.to_sym
        raise ArgumentError, "duplicate store query id: #{query_id}" if QUERIES.key?(query_id)

        QUERIES[query_id] = Query.new(
          id: query_id,
          backend: backend.to_sym,
          builder:,
        )
      end

      #: (untyped, untyped, ?untyped) -> String
      def sql(id, store, locals = {})
        query = QUERIES.fetch(id.to_sym)
        query.builder.call(store, **locals)
      end

      #: (untyped) -> untyped
      def query_ids(backend)
        QUERIES.values.select { |query| query.backend == backend.to_sym }.map(&:id).sort
      end

      #: () -> untyped
      def hot_query_coverage
        HOT_QUERY_COVERAGE
      end

      private

      #: (untyped, untyped) -> untyped
      def table(store, name)
        store.send(:table, name)
      end
    end

    HOT_QUERY_COVERAGE = {
      "workflow queue claim" => {
        methods: ["Store.claim_runnable_workflow", "MysqlStore.claim_runnable_workflow"],
        indexes: ["workflows_queue_idx", "workflows_runnable_due_idx", "workflows_expired_lease_idx", "workflows_pending_created_idx", "workflows_failed_due_idx", "workflows_canceling_created_idx"],
        assertions: ["no sequential/full table scan", "allowed queue indexes", "LIMIT 1 probes", "FOR UPDATE SKIP LOCKED"],
        benchmarks: ["claim_runnable_workflows", "large_table_claim_scan"],
      },
      "workflow lease lifecycle" => {
        methods: ["Store.claim_workflow", "Store.heartbeat", "Store.release_worker_leases!", "Store.steal_expired_leases!"],
        indexes: ["workflows_pkey", "workflows_expired_lease_idx", "workflows_worker_lease_idx", "inbox_worker_lease_idx", "target_activations_worker_lease_idx"],
        assertions: ["primary-key lease updates", "expired lease index", "worker release index"],
        benchmarks: ["lease_heartbeat", "lease_conflict_check", "expired_workflow_lease_recovery"],
      },
      "wait wake scans" => {
        methods: ["Store.record_wait", "Store.complete_waits", "MysqlStore.complete_waits_mysql"],
        indexes: ["waits_timer_pending_idx", "waits_workflow_status_idx"],
        assertions: ["timer pending index", "workflow join remains indexed", "bounded wake batches"],
        benchmarks: ["large_table_due_timer_scan", "bulk_due_timer_wake_parallel"],
      },
      "step attempt counts" => {
        methods: ["WorkflowStepRunner.attempt_number_for", "Store.step_attempt_count_for"],
        indexes: ["step_attempts_workflow_position_status_started_idx"],
        assertions: ["attempt count remains index-backed"],
        benchmarks: ["step_attempt_number_lookup"],
      },
      "outbox delivery" => {
        methods: ["Store.enqueue_outbox", "Store.claim_outbox", "Store.ack_outbox", "Store.release_worker_leases!"],
        indexes: ["outbox_key_key", "outbox_queue_idx", "outbox_expired_lease_idx", "outbox_worker_lease_idx"],
        assertions: ["unique key lookup", "queue/expired indexes", "worker release index"],
        benchmarks: ["outbox_claim_ack", "outbox_expired_reclaim"],
      },
      "durable object commands" => {
        methods: ["Store.object_state", "Store.save_object_state", "Store.enqueue_object_command", "Store.claim_object_command", "Store.complete_object_command"],
        indexes: ["durable_objects_pkey", "durable_object_commands_pkey", "durable_object_commands_object_status_idx"],
        assertions: ["object primary key lookup", "command primary-key claim", "object-status index exists for future per-object scans"],
        benchmarks: ["durable_object_command_claim"],
      },
    }.freeze

    define(:pg_claim_runnable_workflow, backend: :postgres) do |store, name_filter:|
      workflows = table(store, "workflows")
      "WITH candidate AS (\n  " \
        "SELECT id FROM (\n    " \
        "SELECT id, created_at FROM (\n      " \
        "SELECT id, created_at FROM #{workflows}\n      " \
        "WHERE status = 'pending'\n        " \
        "AND runnable_immediately\n        " \
        "#{name_filter}\n      " \
        "ORDER BY status, runnable_immediately, created_at\n      " \
        "LIMIT 1\n      " \
        "FOR UPDATE SKIP LOCKED\n    " \
        ") pending_candidate\n    " \
        "UNION ALL\n    " \
        "SELECT id, created_at FROM (\n      " \
        "SELECT id, created_at FROM #{workflows}\n      " \
        "WHERE status = 'pending'\n        " \
        "AND next_run_at <= now()\n        " \
        "#{name_filter}\n      " \
        "ORDER BY next_run_at, created_at\n      " \
        "LIMIT 1\n      " \
        "FOR UPDATE SKIP LOCKED\n    " \
        ") due_pending_candidate\n    " \
        "UNION ALL\n    " \
        "SELECT id, created_at FROM (\n      " \
        "SELECT id, created_at FROM #{workflows}\n      " \
        "WHERE status = 'failed'\n        " \
        "AND next_run_at IS NOT NULL\n        " \
        "AND next_run_at <= now()\n        " \
        "#{name_filter}\n      " \
        "ORDER BY created_at\n      " \
        "LIMIT 1\n      " \
        "FOR UPDATE SKIP LOCKED\n    " \
        ") failed_candidate\n    " \
        "UNION ALL\n    " \
        "SELECT id, created_at FROM (\n      " \
        "SELECT id, created_at FROM #{workflows}\n      " \
        "WHERE status = 'canceling'\n        " \
        "AND (next_run_at IS NULL OR next_run_at <= now())\n        " \
        "#{name_filter}\n      " \
        "ORDER BY created_at\n      " \
        "LIMIT 1\n      " \
        "FOR UPDATE SKIP LOCKED\n    " \
        ") canceling_candidate\n    " \
        "UNION ALL\n    " \
        "SELECT id, created_at FROM (\n      " \
        "SELECT id, created_at FROM #{workflows}\n      " \
        "WHERE status = 'running' AND locked_until < now()\n        " \
        "#{name_filter}\n      " \
        "ORDER BY created_at\n      " \
        "LIMIT 1\n      " \
        "FOR UPDATE SKIP LOCKED\n    " \
        ") expired_candidate\n  " \
        ") candidates\n  " \
        "ORDER BY created_at\n  " \
        "LIMIT 1\n" \
        ")\n" \
        "UPDATE #{workflows} AS workflows\n" \
        "SET status = 'running', locked_by = $1, locked_until = now() + ($2::int * interval '1 second'), next_run_at = NULL, runnable_immediately = true, updated_at = now()\n" \
        "FROM candidate\n" \
        "WHERE workflows.id = candidate.id\n" \
        "RETURNING workflows.*"
    end

    define(:pg_claim_workflow_already_owned, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflows")}\n" \
        "WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()"
    end

    define(:pg_claim_workflow_update, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'running', error = NULL, locked_by = $2,\n    " \
        "locked_until = now() + ($3::int * interval '1 second'), next_run_at = NULL, runnable_immediately = true, updated_at = now()\n" \
        "WHERE id = $1\n  " \
        "AND (\n    " \
        "status = 'pending'\n    " \
        "OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= now())\n    " \
        "OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= now()))\n    " \
        "OR (status = 'running' AND (locked_by = $2 OR locked_until < now()))\n  " \
        ")\n" \
        "RETURNING *"
    end

    define(:pg_heartbeat_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET locked_until = now() + ($3::int * interval '1 second'), updated_at = now()\n" \
        "WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()"
    end

    define(:pg_workflow_owned, backend: :postgres) do |store|
      "SELECT 1\n" \
        "FROM #{table(store, "workflows")}\n" \
        "WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()"
    end

    define(:pg_release_workflow_leases, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = CASE\n    " \
        "WHEN cancel_requested_at IS NOT NULL THEN 'canceling'\n    " \
        "ELSE 'pending'\n  " \
        "END,\n  " \
        "locked_by = NULL, locked_until = NULL, runnable_immediately = true, updated_at = now()\n" \
        "WHERE status = 'running' AND locked_by = $1"
    end

    define(:pg_release_outbox_leases, backend: :postgres) do |store|
      "UPDATE #{table(store, "outbox")}\n" \
        "SET status = 'pending', locked_by = NULL, locked_until = NULL\n" \
        "WHERE status = 'processing' AND locked_by = $1"
    end

    define(:pg_release_inbox_leases, backend: :postgres) do |store|
      "UPDATE #{table(store, "inbox")}\n" \
        "SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now()\n" \
        "WHERE status = 'running' AND locked_by = $1"
    end

    define(:pg_release_target_activation_leases, backend: :postgres) do |store|
      "UPDATE #{table(store, "target_activations")}\n" \
        "SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now()\n" \
        "WHERE status = 'running' AND locked_by = $1"
    end

    define(:pg_heartbeat_step_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET locked_until = now() + ($3::int * interval '1 second'), updated_at = now()\n" \
        "WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()\n" \
        "RETURNING locked_until"
    end

    define(:pg_heartbeat_step_row, backend: :postgres) do |store|
      "UPDATE #{table(store, "steps")}\n" \
        "SET heartbeat_cursor = $3::bytea, updated_at = now()\n" \
        "WHERE workflow_id = $1 AND position = $2 AND status = 'running'\n" \
        "RETURNING heartbeat_cursor"
    end

    define(:pg_heartbeat_latest_attempt, backend: :postgres) do |store|
      "UPDATE #{table(store, "step_attempts")}\n" \
        "SET heartbeat_cursor = $3::bytea\n" \
        "WHERE id = (\n  " \
        "SELECT id FROM #{table(store, "step_attempts")}\n  " \
        "WHERE workflow_id = $1 AND position = $2 AND status = 'running'\n  " \
        "ORDER BY started_at DESC\n  " \
        "LIMIT 1\n" \
        ")"
    end

    define(:pg_current_workflow_lease, backend: :postgres) do |store|
      "SELECT id AS workflow_id, locked_by AS worker_id, locked_until\n" \
        "FROM #{table(store, "workflows")}\n" \
        "WHERE id = $1 AND status = 'running' AND locked_by IS NOT NULL AND locked_until >= now()"
    end

    define(:pg_steal_expired_leases, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = CASE\n    " \
        "WHEN cancel_requested_at IS NOT NULL THEN 'canceling'\n    " \
        "ELSE 'pending'\n  " \
        "END,\n  " \
        "locked_by = NULL, locked_until = NULL, runnable_immediately = true, updated_at = now()\n" \
        "WHERE status = 'running' AND locked_until < $1::timestamptz"
    end

    define(:pg_supersede_running_step_attempts, backend: :postgres) do |store|
      "UPDATE #{table(store, "step_attempts")}\n" \
        "SET status = 'failed', error = 'superseded by retry', completed_at = now()\n" \
        "WHERE workflow_id = $1 AND position = $2 AND status = 'running'"
    end

    define(:pg_upsert_step_running, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, started_at, updated_at)\n" \
        "VALUES ($1, $2, $3, 'running', now(), now())\n" \
        "ON CONFLICT (workflow_id, position) DO UPDATE\n  " \
        "SET status = 'running', error = NULL, started_at = COALESCE(#{table(store, "steps")}.started_at, now()), updated_at = now()"
    end

    define(:pg_insert_step_attempt, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "step_attempts")} (id, workflow_id, position, name, status)\n" \
        "VALUES ($1, $2, $3, $4, 'running')"
    end

    define(:pg_upsert_waiting_step, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, result, started_at, updated_at)\n" \
        "VALUES ($1, $2, $3, 'waiting', $4::bytea, now(), now())\n" \
        "ON CONFLICT (workflow_id, position) DO UPDATE\n  " \
        "SET status = 'waiting', result = $4::bytea, error = NULL, updated_at = now()"
    end

    define(:pg_insert_wait, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status)\n" \
        "VALUES ($1, $2, $3, $4, $5, $6::timestamptz, $7::bytea, 'pending')"
    end

    define(:pg_waits_for_workflow, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "waits")} WHERE workflow_id = $1 ORDER BY created_at"
    end

    define(:pg_insert_fence, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "fences")} (workflow_id, key, status, locked_by, locked_until)\n" \
        "VALUES ($1, $2, 'running', $3, now() + ($4::int * interval '1 second'))\n" \
        "ON CONFLICT (workflow_id, key) DO NOTHING"
    end

    define(:pg_complete_fence, backend: :postgres) do |store|
      "UPDATE #{table(store, "fences")}\n" \
        "SET status = 'completed', result = $4::bytea, error = NULL, completed_at = now()\n" \
        "WHERE workflow_id = $1 AND key = $2 AND locked_by = $3"
    end

    define(:pg_fail_fence, backend: :postgres) do |store|
      "UPDATE #{table(store, "fences")}\n" \
        "SET status = 'failed', error = $4, completed_at = now()\n" \
        "WHERE workflow_id = $1 AND key = $2 AND locked_by = $3"
    end

    define(:pg_read_fence, backend: :postgres) do |store|
      "SELECT status, result, error FROM #{table(store, "fences")} WHERE workflow_id = $1 AND key = $2"
    end

    define(:pg_outbox_by_key, backend: :postgres) do |store|
      "SELECT id FROM #{table(store, "outbox")} WHERE key = $1"
    end

    define(:pg_insert_outbox, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "outbox")} (id, workflow_id, topic, payload, key, status)\n" \
        "VALUES ($1, $2, $3, $4::bytea, $5, 'pending')\n" \
        "ON CONFLICT (key) DO NOTHING"
    end

    define(:pg_claim_pending_outbox, backend: :postgres) do |store|
      "SELECT id, created_at FROM #{table(store, "outbox")}\n" \
        "WHERE status = 'pending'\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:pg_claim_expired_outbox, backend: :postgres) do |store|
      "SELECT id, created_at FROM #{table(store, "outbox")}\n" \
        "WHERE status = 'processing' AND locked_until < now()\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:pg_claim_selected_outbox, backend: :postgres) do |store|
      "UPDATE #{table(store, "outbox")}\n" \
        "SET status = 'processing', locked_by = $2, locked_until = now() + ($3::int * interval '1 second')\n" \
        "WHERE id = $1\n" \
        "RETURNING *"
    end

    define(:pg_ack_outbox, backend: :postgres) do |store|
      "UPDATE #{table(store, "outbox")} SET status = 'processed', processed_at = now() WHERE id = $1 AND locked_by = $2"
    end

    define(:pg_outbox_message, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "outbox")} WHERE id = $1"
    end

    define(:pg_workflow, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = $1"
    end

    define(:pg_steps_for, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "steps")} WHERE workflow_id = $1 ORDER BY position"
    end

    define(:pg_step_attempts_for, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "step_attempts")} WHERE workflow_id = $1 ORDER BY started_at, position"
    end

    define(:pg_step_attempt_count_for, backend: :postgres) do |store|
      "SELECT COUNT(*) AS count FROM #{table(store, "step_attempts")} WHERE workflow_id = $1 AND position = $2"
    end

    define(:pg_object_state, backend: :postgres) do |store|
      "SELECT state FROM #{table(store, "durable_objects")} WHERE object_type = $1 AND object_id = $2"
    end

    define(:pg_save_object_state, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "durable_objects")} (object_type, object_id, state)\n" \
        "VALUES ($1, $2, $3::bytea)\n" \
        "ON CONFLICT (object_type, object_id) DO UPDATE\n  " \
        "SET state = $3::bytea, updated_at = now()"
    end

    define(:pg_mark_wait_workflow_pending, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'pending', locked_by = NULL, locked_until = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1 AND status = 'waiting'"
    end

    define(:pg_complete_step, backend: :postgres) do |store|
      "UPDATE #{table(store, "steps")} SET status = 'completed', result = $3::bytea, error = NULL, completed_at = now(), updated_at = now() WHERE workflow_id = $1 AND position = $2"
    end

    define(:pg_update_latest_attempt, backend: :postgres) do |store|
      "UPDATE #{table(store, "step_attempts")}\n" \
        "SET status = $3, result = $4::bytea, error = $5, completed_at = now()\n" \
        "WHERE id = (\n  " \
        "SELECT id FROM #{table(store, "step_attempts")}\n  " \
        "WHERE workflow_id = $1 AND position = $2 AND status IN ('running', 'waiting')\n  " \
        "ORDER BY started_at DESC\n  " \
        "LIMIT 1\n" \
        ")"
    end

    define(:mysql_claim_pending_workflow, backend: :mysql) do |store, name_sql:|
      "SELECT id, created_at FROM #{table(store, "workflows")}\n" \
        "WHERE status = 'pending'\n  " \
        "AND (next_run_at IS NULL OR next_run_at <= NOW(6))\n  " \
        "#{name_sql}\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_failed_workflow, backend: :mysql) do |store, name_sql:|
      "SELECT id, created_at FROM #{table(store, "workflows")}\n" \
        "WHERE status = 'failed'\n  " \
        "AND next_run_at IS NOT NULL\n  " \
        "AND next_run_at <= NOW(6)\n  " \
        "#{name_sql}\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_canceling_workflow, backend: :mysql) do |store, name_sql:|
      "SELECT id, created_at FROM #{table(store, "workflows")}\n" \
        "WHERE status = 'canceling'\n  " \
        "AND (next_run_at IS NULL OR next_run_at <= NOW(6))\n  " \
        "#{name_sql}\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_expired_workflow, backend: :mysql) do |store, name_sql:|
      "SELECT id, created_at FROM #{table(store, "workflows")}\n" \
        "WHERE status = 'running' AND locked_until < NOW(6)\n  " \
        "#{name_sql}\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_selected_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), next_run_at = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ?\n  " \
        "AND (\n    " \
        "status = 'pending'\n    " \
        "OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))\n    " \
        "OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))\n    " \
        "OR (status = 'running' AND locked_until < NOW(6))\n  " \
        ")"
    end

    define(:mysql_claim_workflow_already_owned, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "workflows")}\n" \
        "WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)"
    end

    define(:mysql_claim_workflow_lock, backend: :mysql) do |store|
      "SELECT id FROM #{table(store, "workflows")}\n" \
        "WHERE id = ?\n  " \
        "AND (\n    " \
        "status = 'pending'\n    " \
        "OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))\n    " \
        "OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))\n    " \
        "OR (status = 'running' AND (locked_by = ? OR locked_until < NOW(6)))\n  " \
        ")\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_workflow_update, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'running', error = NULL, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), next_run_at = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ?"
    end

    define(:mysql_heartbeat_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)\n" \
        "WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)"
    end

    define(:mysql_workflow_owned, backend: :mysql) do |store|
      "SELECT 1\n" \
        "FROM #{table(store, "workflows")}\n" \
        "WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)"
    end

    define(:mysql_heartbeat_step_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)\n" \
        "WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)"
    end

    define(:mysql_running_step_exists, backend: :mysql) do |store|
      "SELECT 1 FROM #{table(store, "steps")} WHERE workflow_id = ? AND position = ? AND status = 'running'"
    end

    define(:mysql_heartbeat_step_row, backend: :mysql) do |store|
      "UPDATE #{table(store, "steps")}\n" \
        "SET heartbeat_cursor = ?, updated_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND position = ? AND status = 'running'"
    end

    define(:mysql_heartbeat_latest_attempt, backend: :mysql) do |store|
      "UPDATE #{table(store, "step_attempts")}\n" \
        "SET heartbeat_cursor = ?\n" \
        "WHERE workflow_id = ? AND position = ? AND status = 'running'\n" \
        "ORDER BY started_at DESC\n" \
        "LIMIT 1"
    end

    define(:mysql_workflow_locked_until, backend: :mysql) do |store|
      "SELECT locked_until FROM #{table(store, "workflows")} WHERE id = ?"
    end

    define(:mysql_current_workflow_lease, backend: :mysql) do |store|
      "SELECT id AS workflow_id, locked_by AS worker_id, locked_until\n" \
        "FROM #{table(store, "workflows")}\n" \
        "WHERE id = ? AND status = 'running' AND locked_by IS NOT NULL AND locked_until >= NOW(6)"
    end

    define(:mysql_supersede_running_step_attempts, backend: :mysql) do |store|
      "UPDATE #{table(store, "step_attempts")}\n" \
        "SET status = 'failed', error = 'superseded by retry', completed_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND position = ? AND status = 'running'"
    end

    define(:mysql_upsert_step_running, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, started_at, updated_at)\n" \
        "VALUES (?, ?, ?, 'running', NOW(6), NOW(6))\n" \
        "ON DUPLICATE KEY UPDATE status = 'running', error = NULL, started_at = COALESCE(#{table(store, "steps")}.started_at, NOW(6)), updated_at = NOW(6)"
    end

    define(:mysql_insert_step_attempt, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "step_attempts")} (id, workflow_id, position, name, status)\n" \
        "VALUES (?, ?, ?, ?, 'running')"
    end

    define(:mysql_count_workflow_leases, backend: :mysql) do |store, index: nil|
      force_index = " FORCE INDEX (#{index})" if index
      "SELECT COUNT(*) AS count FROM #{table(store, "workflows")}#{force_index} WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_release_workflow_leases, backend: :mysql) do |store, index: nil|
      force_index = " FORCE INDEX (#{index})" if index
      "UPDATE #{table(store, "workflows")}#{force_index}\n" \
        "SET status = CASE\n    " \
        "WHEN cancel_requested_at IS NOT NULL THEN 'canceling'\n    " \
        "ELSE 'pending'\n  " \
        "END,\n  " \
        "locked_by = NULL, locked_until = NULL, updated_at = NOW(6)\n" \
        "WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_count_outbox_leases, backend: :mysql) do |store, index: nil|
      force_index = " FORCE INDEX (#{index})" if index
      "SELECT COUNT(*) AS count FROM #{table(store, "outbox")}#{force_index} WHERE status = 'processing' AND locked_by = ?"
    end

    define(:mysql_release_outbox_leases, backend: :mysql) do |store, index: nil|
      force_index = " FORCE INDEX (#{index})" if index
      "UPDATE #{table(store, "outbox")}#{force_index}\n" \
        "SET status = 'pending', locked_by = NULL, locked_until = NULL\n" \
        "WHERE status = 'processing' AND locked_by = ?"
    end

    define(:mysql_claim_pending_outbox, backend: :mysql) do |store|
      "SELECT id, created_at FROM #{table(store, "outbox")}\n" \
        "WHERE status = 'pending'\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_expired_outbox, backend: :mysql) do |store|
      "SELECT id, created_at FROM #{table(store, "outbox")}\n" \
        "WHERE status = 'processing' AND locked_until < NOW(6)\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_selected_outbox, backend: :mysql) do |store|
      "UPDATE #{table(store, "outbox")}\n" \
        "SET status = 'processing', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND)\n" \
        "WHERE id = ?"
    end

    define(:mysql_ack_outbox, backend: :mysql) do |store|
      "UPDATE #{table(store, "outbox")} SET status = 'processed', processed_at = NOW(6) WHERE id = ? AND locked_by = ?"
    end

    define(:mysql_outbox_message, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "outbox")} WHERE id = ?"
    end

    define(:mysql_workflow, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = ?"
    end

    define(:mysql_steps_for, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "steps")} WHERE workflow_id = ? ORDER BY position"
    end

    define(:mysql_outbox_by_key, backend: :mysql) do |store|
      "SELECT id FROM #{table(store, "outbox")} WHERE `key` = ?"
    end

    define(:mysql_insert_outbox, backend: :mysql) do |store|
      "INSERT IGNORE INTO #{table(store, "outbox")} (id, workflow_id, topic, payload, `key`, status)\n" \
        "VALUES (?, ?, ?, ?, ?, 'pending')"
    end

    define(:mysql_step_attempts_for, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "step_attempts")} WHERE workflow_id = ? ORDER BY started_at, position"
    end

    define(:mysql_step_attempt_count_for, backend: :mysql) do |store|
      "SELECT COUNT(*) AS count FROM #{table(store, "step_attempts")} WHERE workflow_id = ? AND position = ?"
    end

    define(:mysql_object_state, backend: :mysql) do |store|
      "SELECT state FROM #{table(store, "durable_objects")} WHERE object_type = ? AND object_id = ?"
    end

    define(:pg_drop_schema, backend: :postgres) do |store|
      "DROP SCHEMA IF EXISTS #{store.send(:quoted_schema)} CASCADE"
    end

    define(:pg_insert_workflow, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "workflows")} (id, name, status, input) VALUES ($1, $2, $3, $4::bytea)"
    end

    define(:pg_insert_workflow_with_worker, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "workflows")} (id, name, status, input, locked_by, locked_until) VALUES ($1, $2, $3, $4::bytea, $5, now() + ($6::int * interval '1 second'))"
    end

    define(:pg_claim_workflow_for_activation_update, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'running', error = NULL, locked_by = $2,\n    " \
        "locked_until = now() + ($3::int * interval '1 second'), next_run_at = NULL, runnable_immediately = true, updated_at = now()\n" \
        "WHERE id = $1\n  " \
        "AND (\n    " \
        "status IN ('pending', 'waiting', 'canceling')\n    " \
        "OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= now())\n    " \
        "OR (status = 'running' AND (locked_by = $2 OR locked_until < now()))\n  " \
        ")\n" \
        "RETURNING *"
    end

    define(:pg_schedule_workflow_retry, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = CASE\n    " \
        "WHEN cancel_requested_at IS NOT NULL THEN 'canceling'\n    " \
        "ELSE 'pending'\n  " \
        "END,\n  " \
        "locked_by = NULL, locked_until = NULL, next_run_at = $3::timestamptz, runnable_immediately = false, updated_at = now()\n" \
        "WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()"
    end

    define(:pg_suspend_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = CASE\n    " \
        "WHEN cancel_requested_at IS NOT NULL THEN 'canceling'\n    " \
        "WHEN EXISTS (SELECT 1 FROM #{table(store, "waits")} WHERE workflow_id = $1 AND status = 'pending') THEN 'waiting'\n    " \
        "ELSE 'pending'\n  " \
        "END,\n  " \
        "locked_by = NULL, locked_until = NULL, runnable_immediately = true, updated_at = now()\n" \
        "WHERE id = $1 AND status = 'running'\n  " \
        "AND ($2::text IS NULL OR (locked_by = $2::text AND locked_until >= now()))"
    end

    define(:pg_make_workflow_due, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET next_run_at = NULL, runnable_immediately = true, updated_at = $2::timestamptz WHERE id = $1"
    end

    define(:pg_request_workflow_cancellation, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET cancel_reason = $2, cancel_requested_at = now(), updated_at = now()\n" \
        "WHERE id = $1"
    end

    define(:pg_mark_workflow_canceling_for_request, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'canceling', locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now()\n" \
        "WHERE id = $1 AND status NOT IN ('completed', 'canceled')"
    end

    define(:pg_workflow_cancellation, backend: :postgres) do |store|
      "SELECT id AS workflow_id, cancel_reason AS reason,\n" \
        "cancel_requested_at AS requested_at, cancel_delivered_at AS delivered_at\n" \
        "FROM #{table(store, "workflows")}\n" \
        "WHERE id = $1 AND cancel_requested_at IS NOT NULL"
    end

    define(:pg_mark_workflow_cancellation_delivered, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET cancel_delivered_at = COALESCE(cancel_delivered_at, now()), updated_at = now()\n" \
        "WHERE id = $1 AND cancel_requested_at IS NOT NULL"
    end

    define(:pg_step_heartbeat_cursor, backend: :postgres) do |store|
      "SELECT heartbeat_cursor FROM #{table(store, "steps")} WHERE workflow_id = $1 AND position = $2"
    end

    define(:pg_current_object_lease, backend: :postgres) do |store|
      "SELECT target_id AS object_id, locked_by AS worker_id, locked_until\n" \
        "FROM #{table(store, "inbox")}\n" \
        "WHERE target_kind = 'object' AND target_type = $1 AND target_id = $2 AND status = 'running'\n  " \
        "AND locked_by IS NOT NULL AND locked_until >= now()\n" \
        "ORDER BY sequence\n" \
        "LIMIT 1"
    end

    define(:pg_mark_workflow_running, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'running', error = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now()\n" \
        "WHERE id = $1"
    end

    define(:pg_complete_workflow_with_worker, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $3 AND locked_until >= now()"
    end

    define(:pg_complete_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1"
    end

    define(:pg_cancel_workflow_with_worker, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'canceled', result = $2::bytea, error = $3, cancel_reason = COALESCE(cancel_reason, $3), cancel_requested_at = COALESCE(cancel_requested_at, now()), locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $4 AND locked_until >= now()"
    end

    define(:pg_cancel_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'canceled', result = $2::bytea, error = $3, cancel_reason = COALESCE(cancel_reason, $3), cancel_requested_at = COALESCE(cancel_requested_at, now()), locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1"
    end

    define(:pg_fail_workflow_with_worker, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $3 AND locked_until >= now()"
    end

    define(:pg_fail_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, runnable_immediately = true, updated_at = now() WHERE id = $1"
    end

    define(:pg_insert_scheduled_step, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, updated_at)\n" \
        "VALUES ($1, $2, $3, 'scheduled', now())\n" \
        "ON CONFLICT (workflow_id, position) DO NOTHING"
    end

    define(:pg_cancel_step, backend: :postgres) do |store|
      "UPDATE #{table(store, "steps")} SET status = 'canceled', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2 AND status IN ('scheduled', 'running', 'waiting')"
    end

    define(:pg_fail_step, backend: :postgres) do |store|
      "UPDATE #{table(store, "steps")} SET status = 'failed', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2"
    end

    define(:pg_cancel_pending_waits_for_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "waits")}\n" \
        "SET status = 'canceled', completed_at = now()\n" \
        "WHERE workflow_id = $1 AND status = 'pending'"
    end

    define(:pg_cancel_waiting_steps_for_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "steps")}\n" \
        "SET status = 'canceled', error = 'workflow cancellation requested', updated_at = now()\n" \
        "WHERE workflow_id = $1 AND status = 'waiting'"
    end

    define(:pg_cancel_waiting_step_attempts_for_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "step_attempts")}\n" \
        "SET status = 'canceled', error = 'workflow cancellation requested', completed_at = now()\n" \
        "WHERE workflow_id = $1 AND status = 'waiting'"
    end

    define(:pg_claim_pending_target_activation, backend: :postgres) do |store, filter_sql:|
      "SELECT target_kind, target_type, target_id, ready_at, created_at FROM #{table(store, "target_activations")}\n" \
        "WHERE status = 'pending' AND ready_at <= $1::timestamptz\n  " \
        "#{filter_sql}\n" \
        "ORDER BY ready_at, created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:pg_claim_expired_target_activation, backend: :postgres) do |store, filter_sql:|
      "SELECT target_kind, target_type, target_id, ready_at, created_at FROM #{table(store, "target_activations")}\n" \
        "WHERE status = 'running' AND locked_until < $1::timestamptz\n  " \
        "#{filter_sql}\n" \
        "ORDER BY ready_at, created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:pg_claim_selected_target_activation, backend: :postgres) do |store|
      "UPDATE #{table(store, "target_activations")}\n" \
        "SET status = 'running', locked_by = $4, locked_until = now() + ($5::int * interval '1 second'), updated_at = now()\n" \
        "WHERE target_kind = $1 AND target_type = $2 AND target_id = $3\n" \
        "RETURNING *"
    end

    define(:pg_lock_target_activation_for_completion, backend: :postgres) do |store|
      "SELECT 1 FROM #{table(store, "target_activations")}\n" \
        "WHERE target_kind = $1 AND target_type = $2 AND target_id = $3\n  " \
        "AND status = 'running' AND locked_by = $4\n" \
        "FOR UPDATE"
    end

    define(:pg_upsert_target_activation, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "target_activations")} (target_kind, target_type, target_id, status, ready_at)\n" \
        "VALUES ($1, $2, $3, 'pending', $4::timestamptz)\n" \
        "ON CONFLICT (target_kind, target_type, target_id) DO UPDATE\n  " \
        "SET status = CASE WHEN #{table(store, "target_activations")}.status = 'running' THEN #{table(store, "target_activations")}.status ELSE 'pending' END,\n  " \
        "ready_at = LEAST(#{table(store, "target_activations")}.ready_at, EXCLUDED.ready_at), updated_at = now()"
    end

    define(:pg_delete_target_activation, backend: :postgres) do |store|
      "DELETE FROM #{table(store, "target_activations")} WHERE target_kind = $1 AND target_type = $2 AND target_id = $3"
    end

    define(:pg_set_target_activation_pending, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "target_activations")} (target_kind, target_type, target_id, status, ready_at)\n" \
        "VALUES ($1, $2, $3, 'pending', $4::timestamptz)\n" \
        "ON CONFLICT (target_kind, target_type, target_id) DO UPDATE\n  " \
        "SET status = 'pending', ready_at = EXCLUDED.ready_at, locked_by = NULL, locked_until = NULL, updated_at = now()"
    end

    define(:pg_insert_mailbox_sequence, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "mailbox_sequences")} (target_kind, target_type, target_id, last_sequence)\n" \
        "VALUES ($1, $2, $3, 0)\n" \
        "ON CONFLICT (target_kind, target_type, target_id) DO NOTHING"
    end

    define(:pg_mailbox_sequence_for_update, backend: :postgres) do |store|
      "SELECT last_sequence\n" \
        "FROM #{table(store, "mailbox_sequences")}\n" \
        "WHERE target_kind = $1 AND target_type = $2 AND target_id = $3\n" \
        "FOR UPDATE"
    end

    define(:pg_update_mailbox_sequence, backend: :postgres) do |store|
      "UPDATE #{table(store, "mailbox_sequences")}\n" \
        "SET last_sequence = $4, updated_at = now()\n" \
        "WHERE target_kind = $1 AND target_type = $2 AND target_id = $3"
    end

    define(:pg_existing_inbox_message_for_idempotency, backend: :postgres) do |store|
      "SELECT id, target_kind, target_type, target_id, status, ready_at, shape_hash\n" \
        "FROM #{table(store, "inbox")}\n" \
        "WHERE target_kind = $1 AND target_type = $2 AND target_id = $3 AND idempotency_key = $4\n" \
        "FOR UPDATE"
    end

    define(:pg_lock_workflow_for_update, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = $1 FOR UPDATE"
    end

    define(:pg_lock_owned_workflow_for_update, backend: :postgres) do |store|
      "SELECT 1\n" \
        "FROM #{table(store, "workflows")}\n" \
        "WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()\n" \
        "FOR UPDATE"
    end

    define(:pg_insert_inbox_message, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "inbox")} (\n" \
        "id, target_kind, target_type, target_id, sequence, message_kind, method_name,\n" \
        "operation_id, idempotency_key, shape_hash, payload, status, ready_at, max_attempts\n" \
        ")\n" \
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::bytea, 'pending', $12::timestamptz, $13)"
    end

    define(:pg_inbox_claim_rows_for_update, backend: :postgres) do |store|
      "SELECT *\n" \
        "FROM #{table(store, "inbox")}\n" \
        "WHERE target_kind = $1 AND target_type = $2 AND target_id = $3\n  " \
        "AND status IN ('pending', 'failed', 'running', 'dead_lettered')\n" \
        "ORDER BY sequence\n" \
        "LIMIT $4\n" \
        "FOR UPDATE"
    end

    define(:pg_inbox_head_for_update, backend: :postgres) do |store|
      "SELECT *\n" \
        "FROM #{table(store, "inbox")}\n" \
        "WHERE target_kind = $1 AND target_type = $2 AND target_id = $3\n  " \
        "AND status IN ('pending', 'failed', 'running', 'dead_lettered')\n" \
        "ORDER BY sequence\n" \
        "LIMIT 1\n" \
        "FOR UPDATE"
    end

    define(:pg_lock_inbox_message_for_worker, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "inbox")}\n" \
        "WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()\n" \
        "FOR UPDATE"
    end

    define(:pg_lock_inbox_message, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "inbox")} WHERE id = $1 FOR UPDATE"
    end

    define(:pg_mark_inbox_row_running, backend: :postgres) do |store|
      "UPDATE #{table(store, "inbox")}\n" \
        "SET status = 'running', attempts = attempts + 1, locked_by = $2, locked_until = now() + ($3::int * interval '1 second'), updated_at = now()\n" \
        "WHERE id = $1"
    end

    define(:pg_complete_inbox_message, backend: :postgres) do |store|
      "UPDATE #{table(store, "inbox")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = now(), updated_at = now() WHERE id = $1"
    end

    define(:pg_fail_inbox_message, backend: :postgres) do |store|
      "UPDATE #{table(store, "inbox")}\n" \
        "SET status = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'dead_lettered' ELSE 'failed' END,\n  " \
        "error = $2, locked_by = NULL, locked_until = NULL,\n  " \
        "dead_lettered_at = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN now() ELSE dead_lettered_at END,\n  " \
        "updated_at = now()\n" \
        "WHERE id = $1"
    end

    define(:pg_retry_inbox_message, backend: :postgres) do |store|
      "UPDATE #{table(store, "inbox")}\n" \
        "SET status = 'pending', error = $2, ready_at = $3::timestamptz, locked_by = NULL, locked_until = NULL, updated_at = now()\n" \
        "WHERE id = $1"
    end

    define(:pg_dead_letter_inbox_message, backend: :postgres) do |store|
      "UPDATE #{table(store, "inbox")}\n" \
        "SET status = 'dead_lettered', error = $2, locked_by = NULL, locked_until = NULL, dead_lettered_at = now(), updated_at = now()\n" \
        "WHERE id = $1"
    end

    define(:pg_complete_timer_waits, backend: :postgres) do |store|
      "UPDATE #{table(store, "waits")}\n" \
        "SET status = 'completed', payload = $2::bytea, completed_at = now()\n" \
        "WHERE id IN (\n  " \
        "SELECT w.id FROM #{table(store, "waits")} AS w\n  " \
        "JOIN #{table(store, "workflows")} AS wf ON wf.id = w.workflow_id\n  " \
        "WHERE w.status = 'pending'\n    " \
        "AND wf.status IN ('waiting', 'running')\n    " \
        "AND w.kind = 'timer'\n    " \
        "AND w.wake_at <= $1::timestamptz\n  " \
        "ORDER BY w.wake_at, w.created_at\n  " \
        "LIMIT $3\n  " \
        "FOR UPDATE OF w SKIP LOCKED\n" \
        ")\n" \
        "RETURNING *"
    end

    define(:pg_lock_workflow_history_workflow, backend: :postgres) do |store|
      "SELECT id FROM #{table(store, "workflows")} WHERE id = $1 FOR UPDATE"
    end

    define(:pg_next_workflow_history_event_index, backend: :postgres) do |store|
      "SELECT COALESCE(MAX(event_index), -1) + 1 AS event_index FROM #{table(store, "workflow_history")} WHERE workflow_id = $1"
    end

    define(:pg_insert_workflow_history, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "workflow_history")} (workflow_id, event_index, kind, command_id, name, attempt_id, payload, error)\n" \
        "VALUES ($1, $2, $3, $4, $5, $6, $7::bytea, $8)"
    end

    define(:pg_workflow_history_for, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflow_history")} WHERE workflow_id = $1 ORDER BY event_index"
    end

    define(:pg_inbox_message, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "inbox")} WHERE id = $1"
    end

    define(:pg_inbox_messages_for, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "inbox")}\n" \
        "WHERE target_kind = $1 AND target_type = $2 AND target_id = $3\n" \
        "ORDER BY sequence"
    end

    define(:pg_target_activation, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "target_activations")} WHERE target_kind = $1 AND target_type = $2 AND target_id = $3"
    end

    define(:mysql_drop_table, backend: :mysql) do |store, table_name:|
      "DROP TABLE IF EXISTS #{table(store, table_name)}"
    end

    define(:mysql_insert_workflow, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "workflows")} (id, name, status, input) VALUES (?, ?, ?, ?)"
    end

    define(:mysql_insert_workflow_with_worker, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "workflows")} (id, name, status, input, locked_by, locked_until) VALUES (?, ?, ?, ?, ?, DATE_ADD(NOW(6), INTERVAL ? SECOND))"
    end

    define(:mysql_mark_workflow_running_with_worker, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)\n" \
        "WHERE id = ?"
    end

    define(:mysql_mark_workflow_running, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'running', error = NULL, updated_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_claim_workflow_for_activation_lock, backend: :mysql) do |store|
      "SELECT id FROM #{table(store, "workflows")}\n" \
        "WHERE id = ?\n  " \
        "AND (\n    " \
        "status IN ('pending', 'waiting', 'canceling')\n    " \
        "OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))\n    " \
        "OR (status = 'running' AND (locked_by = ? OR locked_until < NOW(6)))\n  " \
        ")\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_workflow_for_activation_update, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'running', error = NULL, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)\n" \
        "WHERE id = ?"
    end

    define(:mysql_count_inbox_leases, backend: :mysql) do |store, index: nil|
      force_index = " FORCE INDEX (#{index})" if index
      "SELECT COUNT(*) AS count FROM #{table(store, "inbox")}#{force_index} WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_release_inbox_leases, backend: :mysql) do |store, index: nil|
      force_index = " FORCE INDEX (#{index})" if index
      "UPDATE #{table(store, "inbox")}#{force_index}\n" \
        "SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6)\n" \
        "WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_count_target_activation_leases, backend: :mysql) do |store, index: nil|
      force_index = " FORCE INDEX (#{index})" if index
      "SELECT COUNT(*) AS count FROM #{table(store, "target_activations")}#{force_index} WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_release_target_activation_leases, backend: :mysql) do |store, index: nil|
      force_index = " FORCE INDEX (#{index})" if index
      "UPDATE #{table(store, "target_activations")}#{force_index}\n" \
        "SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6)\n" \
        "WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_schedule_workflow_retry, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = CASE\n    " \
        "WHEN cancel_requested_at IS NOT NULL THEN 'canceling'\n    " \
        "ELSE 'pending'\n  " \
        "END,\n  " \
        "locked_by = NULL, locked_until = NULL, next_run_at = ?, updated_at = NOW(6)\n" \
        "WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)"
    end

    define(:mysql_suspend_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = CASE\n    " \
        "WHEN cancel_requested_at IS NOT NULL THEN 'canceling'\n    " \
        "WHEN EXISTS (SELECT 1 FROM #{table(store, "waits")} WHERE workflow_id = ? AND status = 'pending') THEN 'waiting'\n    " \
        "ELSE 'pending'\n  " \
        "END,\n  " \
        "locked_by = NULL, locked_until = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ? AND status = 'running'\n  " \
        "AND (? IS NULL OR (locked_by = ? AND locked_until >= NOW(6)))"
    end

    define(:mysql_make_workflow_due, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")} SET next_run_at = NULL, updated_at = ? WHERE id = ?"
    end

    define(:mysql_request_workflow_cancellation, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET cancel_reason = ?, cancel_requested_at = NOW(6), updated_at = NOW(6)\n" \
        "WHERE id = ?"
    end

    define(:mysql_mark_workflow_canceling_for_request, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'canceling', locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ? AND status NOT IN ('completed', 'canceled')"
    end

    define(:mysql_workflow_cancellation, backend: :mysql) do |store|
      "SELECT id AS workflow_id, cancel_reason AS reason,\n" \
        "cancel_requested_at AS requested_at, cancel_delivered_at AS delivered_at\n" \
        "FROM #{table(store, "workflows")}\n" \
        "WHERE id = ? AND cancel_requested_at IS NOT NULL"
    end

    define(:mysql_mark_workflow_cancellation_delivered, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET cancel_delivered_at = COALESCE(cancel_delivered_at, NOW(6)), updated_at = NOW(6)\n" \
        "WHERE id = ? AND cancel_requested_at IS NOT NULL"
    end

    define(:mysql_step_heartbeat_cursor, backend: :mysql) do |store|
      "SELECT heartbeat_cursor FROM #{table(store, "steps")} WHERE workflow_id = ? AND position = ?"
    end

    define(:mysql_current_object_lease, backend: :mysql) do |store|
      "SELECT target_id AS object_id, locked_by AS worker_id, locked_until\n" \
        "FROM #{table(store, "inbox")}\n" \
        "WHERE target_kind = 'object' AND target_type = ? AND target_id = ? AND status = 'running'\n  " \
        "AND locked_by IS NOT NULL AND locked_until >= NOW(6)\n" \
        "ORDER BY sequence\n" \
        "LIMIT 1"
    end

    define(:mysql_count_expired_workflow_leases, backend: :mysql) do |store|
      "SELECT COUNT(*) AS count FROM #{table(store, "workflows")} WHERE status = 'running' AND locked_until < ?"
    end

    define(:mysql_steal_expired_leases, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = CASE\n    " \
        "WHEN cancel_requested_at IS NOT NULL THEN 'canceling'\n    " \
        "ELSE 'pending'\n  " \
        "END,\n  " \
        "locked_by = NULL, locked_until = NULL, updated_at = NOW(6)\n" \
        "WHERE status = 'running' AND locked_until < ?"
    end

    define(:mysql_insert_scheduled_step, backend: :mysql) do |store|
      "INSERT IGNORE INTO #{table(store, "steps")} (workflow_id, position, name, status, updated_at)\n" \
        "VALUES (?, ?, ?, 'scheduled', NOW(6))"
    end

    define(:mysql_complete_step, backend: :mysql) do |store|
      "UPDATE #{table(store, "steps")}\n" \
        "SET status = 'completed', result = ?, error = NULL, completed_at = NOW(6), updated_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND position = ?"
    end

    define(:mysql_complete_workflow_with_worker, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)"
    end

    define(:mysql_complete_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ?"
    end

    define(:mysql_cancel_workflow_with_worker, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'canceled', result = ?, error = ?, cancel_reason = COALESCE(cancel_reason, ?),\n  " \
        "cancel_requested_at = COALESCE(cancel_requested_at, NOW(6)), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)"
    end

    define(:mysql_cancel_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'canceled', result = ?, error = ?, cancel_reason = COALESCE(cancel_reason, ?),\n  " \
        "cancel_requested_at = COALESCE(cancel_requested_at, NOW(6)), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ?"
    end

    define(:mysql_fail_workflow_with_worker, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'failed', error = ?, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)"
    end

    define(:mysql_fail_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'failed', error = ?, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ?"
    end

    define(:mysql_cancel_step, backend: :mysql) do |store|
      "UPDATE #{table(store, "steps")}\n" \
        "SET status = 'canceled', error = ?, updated_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND position = ? AND status IN ('scheduled', 'running', 'waiting')"
    end

    define(:mysql_fail_step, backend: :mysql) do |store|
      "UPDATE #{table(store, "steps")}\n" \
        "SET status = 'failed', error = ?, updated_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND position = ?"
    end

    define(:mysql_upsert_waiting_step, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, result, started_at, updated_at)\n" \
        "VALUES (?, ?, ?, 'waiting', ?, NOW(6), NOW(6))\n" \
        "ON DUPLICATE KEY UPDATE status = 'waiting', result = VALUES(result), error = NULL, updated_at = NOW(6)"
    end

    define(:mysql_insert_wait, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status)\n" \
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')"
    end

    define(:mysql_insert_fence, backend: :mysql) do |store|
      "INSERT IGNORE INTO #{table(store, "fences")} (workflow_id, `key`, status, locked_by, locked_until)\n" \
        "VALUES (?, ?, 'running', ?, DATE_ADD(NOW(6), INTERVAL ? SECOND))"
    end

    define(:mysql_lock_fence_for_worker, backend: :mysql) do |store|
      "SELECT 1 FROM #{table(store, "fences")} WHERE workflow_id = ? AND `key` = ? AND locked_by = ? AND status = 'running'"
    end

    define(:mysql_complete_fence, backend: :mysql) do |store|
      "UPDATE #{table(store, "fences")}\n" \
        "SET status = 'completed', result = ?, error = NULL, completed_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND `key` = ? AND locked_by = ?"
    end

    define(:mysql_fail_fence, backend: :mysql) do |store|
      "UPDATE #{table(store, "fences")}\n" \
        "SET status = 'failed', error = ?, completed_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND `key` = ? AND locked_by = ?"
    end

    define(:mysql_read_fence, backend: :mysql) do |store|
      "SELECT status, result, error FROM #{table(store, "fences")} WHERE workflow_id = ? AND `key` = ?"
    end

    define(:mysql_save_object_state, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "durable_objects")} (object_type, object_id, state)\n" \
        "VALUES (?, ?, ?)\n" \
        "ON DUPLICATE KEY UPDATE state = VALUES(state), updated_at = NOW(6)"
    end

    define(:mysql_claim_pending_target_activation, backend: :mysql) do |store, filter_sql:|
      "SELECT target_kind, target_type, target_id, ready_at, created_at FROM #{table(store, "target_activations")}\n" \
        "WHERE status = 'pending' AND ready_at <= ?\n  " \
        "#{filter_sql}\n" \
        "ORDER BY ready_at, created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_expired_target_activation, backend: :mysql) do |store, filter_sql:|
      "SELECT target_kind, target_type, target_id, ready_at, created_at FROM #{table(store, "target_activations")}\n" \
        "WHERE status = 'running' AND locked_until < ?\n  " \
        "#{filter_sql}\n" \
        "ORDER BY ready_at, created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_selected_target_activation, backend: :mysql) do |store|
      "UPDATE #{table(store, "target_activations")}\n" \
        "SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)\n" \
        "WHERE target_kind = ? AND target_type = ? AND target_id = ?"
    end

    define(:mysql_lock_target_activation_for_completion, backend: :mysql) do |store|
      "SELECT 1 FROM #{table(store, "target_activations")}\n" \
        "WHERE target_kind = ? AND target_type = ? AND target_id = ?\n  " \
        "AND status = 'running' AND locked_by = ?\n" \
        "FOR UPDATE"
    end

    define(:mysql_lock_owned_workflow_for_update, backend: :mysql) do |store|
      "SELECT 1\n" \
        "FROM #{table(store, "workflows")}\n" \
        "WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)\n" \
        "FOR UPDATE"
    end

    define(:mysql_cancel_pending_waits_for_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "waits")}\n" \
        "SET status = 'canceled', completed_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND status = 'pending'"
    end

    define(:mysql_cancel_waiting_steps_for_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "steps")}\n" \
        "SET status = 'canceled', error = 'workflow cancellation requested', updated_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND status = 'waiting'"
    end

    define(:mysql_cancel_waiting_step_attempts_for_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "step_attempts")}\n" \
        "SET status = 'canceled', error = 'workflow cancellation requested', completed_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND status = 'waiting'"
    end

    define(:mysql_lock_inbox_message_for_worker, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "inbox")}\n" \
        "WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)\n" \
        "FOR UPDATE"
    end

    define(:mysql_lock_inbox_message, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "inbox")} WHERE id = ? FOR UPDATE"
    end

    define(:mysql_upsert_target_activation, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "target_activations")} (target_kind, target_type, target_id, status, ready_at)\n" \
        "VALUES (?, ?, ?, 'pending', ?)\n" \
        "ON DUPLICATE KEY UPDATE status = IF(status = 'running', status, 'pending'), ready_at = LEAST(ready_at, VALUES(ready_at)), updated_at = NOW(6)"
    end

    define(:mysql_delete_target_activation, backend: :mysql) do |store|
      "DELETE FROM #{table(store, "target_activations")} WHERE target_kind = ? AND target_type = ? AND target_id = ?"
    end

    define(:mysql_set_target_activation_pending, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "target_activations")} (target_kind, target_type, target_id, status, ready_at)\n" \
        "VALUES (?, ?, ?, 'pending', ?)\n" \
        "ON DUPLICATE KEY UPDATE status = 'pending', ready_at = VALUES(ready_at), locked_by = NULL, locked_until = NULL, updated_at = NOW(6)"
    end

    define(:mysql_insert_mailbox_sequence, backend: :mysql) do |store|
      "INSERT IGNORE INTO #{table(store, "mailbox_sequences")} (target_kind, target_type, target_id, last_sequence)\n" \
        "VALUES (?, ?, ?, 0)"
    end

    define(:mysql_mailbox_sequence_for_update, backend: :mysql) do |store|
      "SELECT last_sequence\n" \
        "FROM #{table(store, "mailbox_sequences")}\n" \
        "WHERE target_kind = ? AND target_type = ? AND target_id = ?\n" \
        "FOR UPDATE"
    end

    define(:mysql_update_mailbox_sequence, backend: :mysql) do |store|
      "UPDATE #{table(store, "mailbox_sequences")}\n" \
        "SET last_sequence = ?, updated_at = NOW(6)\n" \
        "WHERE target_kind = ? AND target_type = ? AND target_id = ?"
    end

    define(:mysql_existing_inbox_message_for_idempotency, backend: :mysql) do |store|
      "SELECT id, target_kind, target_type, target_id, status, ready_at, shape_hash\n" \
        "FROM #{table(store, "inbox")}\n" \
        "WHERE target_kind = ? AND target_type = ? AND target_id = ? AND idempotency_key = ?\n" \
        "FOR UPDATE"
    end

    define(:mysql_lock_workflow_for_update, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = ? FOR UPDATE"
    end

    define(:mysql_insert_inbox_message, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "inbox")} (\n" \
        "id, target_kind, target_type, target_id, sequence, message_kind, method_name,\n" \
        "operation_id, idempotency_key, shape_hash, payload, status, ready_at, max_attempts\n" \
        ")\n" \
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)"
    end

    define(:mysql_inbox_claim_rows_for_update, backend: :mysql) do |store, limit:|
      "SELECT *\n" \
        "FROM #{table(store, "inbox")}\n" \
        "WHERE target_kind = ? AND target_type = ? AND target_id = ?\n  " \
        "AND status IN ('pending', 'failed', 'running', 'dead_lettered')\n" \
        "ORDER BY sequence\n" \
        "LIMIT #{Integer(limit)}\n" \
        "FOR UPDATE"
    end

    define(:mysql_inbox_head_for_update, backend: :mysql) do |store|
      "SELECT *\n" \
        "FROM #{table(store, "inbox")}\n" \
        "WHERE target_kind = ? AND target_type = ? AND target_id = ?\n  " \
        "AND status IN ('pending', 'failed', 'running', 'dead_lettered')\n" \
        "ORDER BY sequence\n" \
        "LIMIT 1\n" \
        "FOR UPDATE"
    end

    define(:mysql_mark_inbox_row_running, backend: :mysql) do |store|
      "UPDATE #{table(store, "inbox")}\n" \
        "SET status = 'running', attempts = attempts + 1, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)\n" \
        "WHERE id = ?"
    end

    define(:mysql_complete_inbox_message, backend: :mysql) do |store|
      "UPDATE #{table(store, "inbox")} SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = NOW(6), updated_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_fail_inbox_message, backend: :mysql) do |store|
      "UPDATE #{table(store, "inbox")} SET status = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'dead_lettered' ELSE 'failed' END, error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN NOW(6) ELSE dead_lettered_at END, updated_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_retry_inbox_message, backend: :mysql) do |store|
      "UPDATE #{table(store, "inbox")} SET status = 'pending', error = ?, ready_at = ?, locked_by = NULL, locked_until = NULL, updated_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_dead_letter_inbox_message, backend: :mysql) do |store|
      "UPDATE #{table(store, "inbox")} SET status = 'dead_lettered', error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = NOW(6), updated_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_update_latest_attempt, backend: :mysql) do |store|
      "UPDATE #{table(store, "step_attempts")}\n" \
        "SET status = ?, result = ?, error = ?, completed_at = NOW(6)\n" \
        "WHERE workflow_id = ? AND position = ? AND status IN ('running', 'waiting')\n" \
        "ORDER BY started_at DESC\n" \
        "LIMIT 1"
    end

    define(:mysql_complete_timer_waits, backend: :mysql) do |store, limit:|
      "SELECT w.* FROM #{table(store, "waits")} AS w\n" \
        "JOIN #{table(store, "workflows")} AS wf ON wf.id = w.workflow_id\n" \
        "WHERE w.status = 'pending'\n  " \
        "AND wf.status IN ('waiting', 'running')\n  " \
        "AND w.kind = 'timer'\n  " \
        "AND w.wake_at <= ?\n" \
        "ORDER BY w.wake_at, w.created_at\n" \
        "LIMIT #{Integer(limit)}\n" \
        "FOR UPDATE OF w SKIP LOCKED"
    end

    define(:mysql_complete_wait, backend: :mysql) do |store|
      "UPDATE #{table(store, "waits")} SET status = 'completed', payload = ?, completed_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_mark_wait_workflow_pending, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6) WHERE id = ? AND status = 'waiting'"
    end

    define(:mysql_lock_workflow_history_workflow, backend: :mysql) do |store|
      "SELECT id FROM #{table(store, "workflows")} WHERE id = ? FOR UPDATE"
    end

    define(:mysql_next_workflow_history_event_index, backend: :mysql) do |store|
      "SELECT COALESCE(MAX(event_index), -1) + 1 AS event_index FROM #{table(store, "workflow_history")} WHERE workflow_id = ?"
    end

    define(:mysql_insert_workflow_history, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "workflow_history")} (workflow_id, event_index, kind, command_id, name, attempt_id, payload, error)\n" \
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    end

    define(:mysql_workflow_history_for, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "workflow_history")} WHERE workflow_id = ? ORDER BY event_index"
    end

    define(:mysql_waits_for_workflow, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "waits")} WHERE workflow_id = ? ORDER BY created_at"
    end

    define(:mysql_inbox_message, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "inbox")} WHERE id = ?"
    end

    define(:mysql_inbox_messages_for, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "inbox")}\n" \
        "WHERE target_kind = ? AND target_type = ? AND target_id = ?\n" \
        "ORDER BY sequence"
    end

    define(:mysql_target_activation, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "target_activations")} WHERE target_kind = ? AND target_type = ? AND target_id = ?"
    end
  end
end
