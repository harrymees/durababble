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
        assertions: ["timer pending index", "workflow join remains indexed"],
        benchmarks: ["large_table_due_timer_scan"],
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

    define(:pg_claim_pending_workflow, backend: :postgres) do |store, name_filter:|
      "SELECT id, created_at FROM #{table(store, "workflows")}\n" \
        "WHERE status = 'pending'\n  " \
        "AND runnable_immediately\n  " \
        "#{name_filter}\n" \
        "ORDER BY status, runnable_immediately, created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:pg_claim_due_pending_workflow, backend: :postgres) do |store, name_filter:|
      "SELECT id, created_at FROM #{table(store, "workflows")}\n" \
        "WHERE status = 'pending'\n  " \
        "AND next_run_at <= now()\n  " \
        "#{name_filter}\n" \
        "ORDER BY next_run_at, created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:pg_claim_failed_workflow, backend: :postgres) do |store, name_filter:|
      "SELECT id, created_at FROM #{table(store, "workflows")}\n" \
        "WHERE status = 'failed'\n  " \
        "AND next_run_at IS NOT NULL\n  " \
        "AND next_run_at <= now()\n  " \
        "#{name_filter}\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:pg_claim_canceling_workflow, backend: :postgres) do |store, name_filter:|
      "SELECT id, created_at FROM #{table(store, "workflows")}\n" \
        "WHERE status = 'canceling'\n  " \
        "AND (next_run_at IS NULL OR next_run_at <= now())\n  " \
        "#{name_filter}\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:pg_claim_expired_workflow, backend: :postgres) do |store, name_filter:|
      "SELECT id, created_at FROM #{table(store, "workflows")}\n" \
        "WHERE status = 'running' AND locked_until < now()\n  " \
        "#{name_filter}\n" \
        "ORDER BY created_at\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:pg_claim_selected_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'running', locked_by = $2, locked_until = now() + ($3::int * interval '1 second'), next_run_at = NULL, runnable_immediately = true, updated_at = now()\n" \
        "WHERE id = $1\n" \
        "RETURNING *"
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

    define(:pg_mark_workflow_waiting, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'waiting', locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id = $1"
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

    define(:pg_object_state, backend: :postgres) do |store|
      "SELECT state FROM #{table(store, "durable_objects")} WHERE object_type = $1 AND object_id = $2"
    end

    define(:pg_save_object_state, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "durable_objects")} (object_type, object_id, state)\n" \
        "VALUES ($1, $2, $3::bytea)\n" \
        "ON CONFLICT (object_type, object_id) DO UPDATE\n  " \
        "SET state = $3::bytea, updated_at = now()"
    end

    define(:pg_enqueue_object_command, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "durable_object_commands")} (id, object_type, object_id, method_name, args, kwargs, status)\n" \
        "VALUES ($1, $2, $3, $4, $5::bytea, $6::bytea, 'pending')"
    end

    define(:pg_claim_object_command, backend: :postgres) do |store|
      "UPDATE #{table(store, "durable_object_commands")}\n" \
        "SET status = 'running', locked_by = $2, locked_until = now() + ($3::int * interval '1 second')\n" \
        "WHERE id = $1 AND (status IN ('pending', 'failed') OR (status = 'running' AND locked_until < now()))\n" \
        "RETURNING *"
    end

    define(:pg_complete_object_command, backend: :postgres) do |store|
      "UPDATE #{table(store, "durable_object_commands")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = now() WHERE id = $1"
    end

    define(:pg_lock_object_command_for_worker, backend: :postgres) do |store|
      "SELECT 1 FROM #{table(store, "durable_object_commands")}\n" \
        "WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()\n" \
        "FOR UPDATE"
    end

    define(:pg_lock_object_command, backend: :postgres) do |store|
      "SELECT 1 FROM #{table(store, "durable_object_commands")} WHERE id = $1 FOR UPDATE"
    end

    define(:pg_complete_waits, backend: :postgres) do |store, where_sql:, payload_param:|
      "UPDATE #{table(store, "waits")}\n" \
        "SET status = 'completed', payload = $#{payload_param}::bytea, completed_at = now()\n" \
        "WHERE id IN (\n  " \
        "SELECT w.id FROM #{table(store, "waits")} AS w\n  " \
        "JOIN #{table(store, "workflows")} AS wf ON wf.id = w.workflow_id\n  " \
        "WHERE w.status = 'pending'\n    " \
        "AND wf.status IN ('waiting', 'running')\n    " \
        "AND #{where_sql}\n  " \
        "FOR UPDATE OF w, wf SKIP LOCKED\n" \
        ")\n" \
        "RETURNING *"
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

    define(:mysql_claim_due_pending_workflow, backend: :mysql) do |store, name_sql:|
      "SELECT id, created_at FROM #{table(store, "workflows")}\n" \
        "WHERE status = 'pending'\n  " \
        "AND next_run_at <= NOW(6)\n  " \
        "#{name_sql}\n" \
        "ORDER BY next_run_at, created_at\n" \
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
        "ON DUPLICATE KEY UPDATE status = 'running', error = NULL, updated_at = NOW(6)"
    end

    define(:mysql_insert_step_attempt, backend: :mysql) do |store|
      "INSERT INTO #{table(store, "step_attempts")} (id, workflow_id, position, name, status)\n" \
        "VALUES (?, ?, ?, ?, 'running')"
    end

    define(:mysql_count_workflow_leases, backend: :mysql) do |store, index:|
      "SELECT COUNT(*) AS count FROM #{table(store, "workflows")} FORCE INDEX (#{index}) WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_release_workflow_leases, backend: :mysql) do |store, index:|
      "UPDATE #{table(store, "workflows")} FORCE INDEX (#{index})\n" \
        "SET status = CASE\n    " \
        "WHEN cancel_requested_at IS NOT NULL THEN 'canceling'\n    " \
        "ELSE 'pending'\n  " \
        "END,\n  " \
        "locked_by = NULL, locked_until = NULL, updated_at = NOW(6)\n" \
        "WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_count_outbox_leases, backend: :mysql) do |store, index:|
      "SELECT COUNT(*) AS count FROM #{table(store, "outbox")} FORCE INDEX (#{index}) WHERE status = 'processing' AND locked_by = ?"
    end

    define(:mysql_release_outbox_leases, backend: :mysql) do |store, index:|
      "UPDATE #{table(store, "outbox")} FORCE INDEX (#{index})\n" \
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

    define(:mysql_object_state, backend: :mysql) do |store|
      "SELECT state FROM #{table(store, "durable_objects")} WHERE object_type = ? AND object_id = ?"
    end

    define(:mysql_claim_object_command_lock, backend: :mysql) do |store|
      "SELECT id FROM #{table(store, "durable_object_commands")}\n" \
        "WHERE id = ? AND (status IN ('pending', 'failed') OR (status = 'running' AND locked_until < NOW(6)))\n" \
        "LIMIT 1\n" \
        "FOR UPDATE SKIP LOCKED"
    end

    define(:mysql_claim_object_command_update, backend: :mysql) do |store|
      "UPDATE #{table(store, "durable_object_commands")}\n" \
        "SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND)\n" \
        "WHERE id = ?"
    end

    define(:mysql_object_command, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "durable_object_commands")} WHERE id = ?"
    end
  end
end
