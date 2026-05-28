# typed: true
# frozen_string_literal: true

module Durababble
  module StoreQueries
    Query = Struct.new(:id, :backend, :description, :builder, keyword_init: true)

    QUERIES = {}
    POSTGRES_WORKFLOW_CLAIM_EXPRESSION = <<~SQL.chomp.freeze
      CASE
        WHEN status IN ('pending', 'canceling') THEN COALESCE(next_run_at, created_at)
        WHEN status = 'failed' AND next_run_at IS NOT NULL THEN next_run_at
        WHEN status = 'running' AND locked_until IS NOT NULL THEN locked_until
        ELSE NULL
      END
    SQL
    POSTGRES_TARGET_ACTIVATION_CLAIM_EXPRESSION = <<~SQL.chomp.freeze
      CASE
        WHEN status = 'pending' THEN ready_at
        WHEN status = 'running' AND locked_until IS NOT NULL THEN locked_until
        ELSE NULL
      END
    SQL

    class << self
      #: (untyped, backend: untyped, ?description: String?) { (untyped) -> untyped } -> void
      def define(id, backend:, description: nil, &builder)
        query_id = id.to_sym
        raise ArgumentError, "duplicate store query id: #{query_id}" if QUERIES.key?(query_id)

        QUERIES[query_id] = Query.new(
          id: query_id,
          backend: backend.to_sym,
          description: description || inferred_description(query_id),
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

      #: (untyped) -> bool
      def defined?(id)
        QUERIES.key?(id.to_sym)
      end

      #: (untyped) -> String
      def description_for(id)
        QUERIES.fetch(id.to_sym).description
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

      #: (untyped, untyped, untyped) -> untyped
      def index_name(store, table_name, suffix)
        store.send(:index_name, table_name, suffix)
      end

      #: (Symbol) -> String
      def inferred_description(id)
        suffix = id.to_s.sub(/\A(?:pg|mysql|sqlite)_/, "")
        "Run the #{suffix.tr("_", " ")} store query."
      end
    end

    HOT_QUERY_COVERAGE = {
      "workflow queue claim" => {
        methods: ["Store.claim_runnable_workflow", "PostgresStore.claim_runnable_workflow", "MysqlStore.claim_runnable_workflow"],
        indexes: ["workflows_claim_idx"],
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
        methods: ["Store.record_wait", "Store.complete_waits", "Store.complete_object_wakeups", "MysqlStore.complete_waits_mysql"],
        indexes: ["waits_timer_pending_idx", "waits_workflow_status_idx", "object_wakeups_due_idx"],
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
      "durable object mailbox" => {
        methods: ["Store.object_state", "Store.save_object_state", "Store.enqueue_inbox_message", "Store.claim_object_command", "Store.complete_object_command"],
        indexes: ["durable_objects_pkey", "inbox_pkey", "inbox_target_status_sequence_idx", "inbox_idempotency_hash_idx", "target_activations_pkey", "target_activations_claim_idx"],
        assertions: ["object primary-key lookup", "mailbox primary-key claim", "target-sequence and activation claim indexes"],
        benchmarks: ["durable_object_command_claim"],
      },
    }.freeze

    define(:pg_claim_runnable_workflow, backend: :postgres, description: "Probe the unified workflow queue for one runnable candidate in this worker pool.") do |store, name_filter:|
      workflows = table(store, "workflows")
      <<~SQL.chomp
        WITH candidate AS (
          SELECT id FROM #{workflows}
          WHERE worker_pool = $1
            AND (#{POSTGRES_WORKFLOW_CLAIM_EXPRESSION}) <= now()
            #{name_filter}
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        )
        UPDATE #{workflows} AS workflows
        SET status = 'running', locked_by = $2, locked_until = now() + ($3::int * interval '1 second'), next_run_at = NULL, updated_at = now()
        FROM candidate
        WHERE workflows.id = candidate.id AND workflows.worker_pool = $1
        RETURNING workflows.*
      SQL
    end

    define(:pg_claim_workflow_already_owned, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT * FROM #{table(store, "workflows")}
        WHERE id = $1 AND worker_pool = $2 AND status = 'running' AND locked_by = $3 AND locked_until >= now()
      SQL
    end

    define(:pg_claim_workflow_update, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'running', error = NULL, locked_by = $3,
            locked_until = now() + ($4::int * interval '1 second'), next_run_at = NULL, updated_at = now()
        WHERE id = $1 AND worker_pool = $2
          AND (
            (status = 'pending' AND (next_run_at IS NULL OR next_run_at <= now()))
            OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= now())
            OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= now()))
            OR (status = 'running' AND (locked_by = $3 OR locked_until < now()))
          )
        RETURNING *
      SQL
    end

    define(:pg_heartbeat_workflow, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
        WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()
      SQL
    end

    define(:pg_workflow_owned, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT 1
        FROM #{table(store, "workflows")}
        WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()
      SQL
    end

    define(:pg_release_workflow_leases, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE status = 'running' AND locked_by = $1
      SQL
    end

    define(:pg_release_outbox_leases, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "outbox")}
        SET status = 'pending', locked_by = NULL, locked_until = NULL
        WHERE status = 'processing' AND locked_by = $1
      SQL
    end

    define(:pg_release_inbox_leases, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "inbox")}
        SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE status = 'running' AND locked_by = $1
      SQL
    end

    define(:pg_release_target_activation_leases, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "target_activations")}
        SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE status = 'running' AND locked_by = $1
      SQL
    end

    define(:pg_heartbeat_step_workflow, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
        WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()
        RETURNING locked_until
      SQL
    end

    define(:pg_heartbeat_step_row, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET heartbeat_cursor = $3::bytea, updated_at = now()
        WHERE workflow_id = $1 AND position = $2 AND status = 'running'
        RETURNING heartbeat_cursor
      SQL
    end

    define(:pg_heartbeat_latest_attempt, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET heartbeat_cursor = $3::bytea
        WHERE id = (
          SELECT id FROM #{table(store, "step_attempts")}
          WHERE workflow_id = $1 AND position = $2 AND status = 'running'
          ORDER BY started_at DESC
          LIMIT 1
        )
      SQL
    end

    define(:pg_current_workflow_lease, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT id AS workflow_id, worker_pool, locked_by AS worker_id, locked_until
        FROM #{table(store, "workflows")}
        WHERE id = $1 AND status = 'running' AND locked_by IS NOT NULL AND locked_until >= now()
      SQL
    end

    define(:pg_steal_expired_leases, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE status = 'running' AND locked_until < $1::timestamptz
      SQL
    end

    define(:pg_supersede_running_step_attempts, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = 'failed', error = 'superseded by retry', completed_at = now()
        WHERE workflow_id = $1 AND position = $2 AND status = 'running'
      SQL
    end

    define(:pg_upsert_step_running, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, started_at, updated_at)
        VALUES ($1, $2, $3, 'running', now(), now())
        ON CONFLICT (workflow_id, position) DO UPDATE
          SET status = 'running', error = NULL, started_at = COALESCE(#{table(store, "steps")}.started_at, now()), updated_at = now()
      SQL
    end

    define(:pg_insert_step_attempt, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "step_attempts")} (id, workflow_id, position, name, status)
        VALUES ($1, $2, $3, $4, 'running')
      SQL
    end

    define(:pg_upsert_waiting_step, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, result, started_at, updated_at)
        VALUES ($1, $2, $3, 'waiting', $4::bytea, now(), now())
        ON CONFLICT (workflow_id, position) DO UPDATE
          SET status = 'waiting', result = $4::bytea, error = NULL, updated_at = now()
      SQL
    end

    define(:pg_insert_wait, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status)
        VALUES ($1, $2, $3, $4, $5, $6::timestamptz, $7::bytea, 'pending')
      SQL
    end

    define(:pg_waits_for_workflow, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "waits")} WHERE workflow_id = $1 ORDER BY created_at"
    end

    define(:pg_insert_fence, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "fences")} (workflow_id, key, status, locked_by, locked_until)
        VALUES ($1, $2, 'running', $3, now() + ($4::int * interval '1 second'))
        ON CONFLICT (workflow_id, key) DO NOTHING
      SQL
    end

    define(:pg_claim_expired_fence, backend: :postgres) do |store|
      "UPDATE #{table(store, "fences")}\n" \
        "SET locked_by = $1, locked_until = now() + ($2::int * interval '1 second'), result = NULL, error = NULL, completed_at = NULL\n" \
        "WHERE workflow_id = $3 AND key = $4 AND status = 'running' AND locked_until < now()"
    end

    define(:pg_complete_fence, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "fences")}
        SET status = 'completed', result = $4::bytea, error = NULL, completed_at = now()
        WHERE workflow_id = $1 AND key = $2 AND locked_by = $3
      SQL
    end

    define(:pg_fail_fence, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "fences")}
        SET status = 'failed', error = $4, completed_at = now()
        WHERE workflow_id = $1 AND key = $2 AND locked_by = $3
      SQL
    end

    define(:pg_read_fence, backend: :postgres) do |store|
      "SELECT status, result, error FROM #{table(store, "fences")} WHERE workflow_id = $1 AND key = $2"
    end

    define(:pg_outbox_by_key, backend: :postgres) do |store|
      "SELECT id FROM #{table(store, "outbox")} WHERE key = $1"
    end

    define(:pg_insert_outbox, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "outbox")} (id, workflow_id, topic, payload, key, status)
        VALUES ($1, $2, $3, $4::bytea, $5, 'pending')
        ON CONFLICT (key) DO NOTHING
        RETURNING id
      SQL
    end

    define(:pg_claim_pending_outbox, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT id, created_at FROM #{table(store, "outbox")}
        WHERE status = 'pending'
        ORDER BY created_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:pg_claim_expired_outbox, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT id, created_at FROM #{table(store, "outbox")}
        WHERE status = 'processing' AND locked_until < now()
        ORDER BY created_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:pg_claim_selected_outbox, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "outbox")}
        SET status = 'processing', locked_by = $2, locked_until = now() + ($3::int * interval '1 second')
        WHERE id = $1
        RETURNING *
      SQL
    end

    define(:pg_ack_outbox, backend: :postgres) do |store|
      # [DURABABBLE-WF-1] Acks require the live sender lease: an owner whose lease
      # expired before another worker reclaimed the row cannot mark it processed.
      "UPDATE #{table(store, "outbox")} SET status = 'processed', processed_at = now() WHERE id = $1 AND locked_by = $2 AND locked_until >= now()"
    end

    define(:pg_outbox_message, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "outbox")} WHERE id = $1"
    end

    define(:pg_workflow, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = $1"
    end

    define(:pg_child_workflow_by_child_id_for_update, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = $1 AND child_origin_kind IS NOT NULL FOR UPDATE"
    end

    define(:pg_child_workflow_rows_for_parent, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE child_origin_kind = 'workflow' AND parent_workflow_id = $1 ORDER BY created_at ASC"
    end

    define(:pg_child_workflow_rows_for_object, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE child_origin_kind = 'object' AND parent_object_type = $1 AND parent_object_id = $2 ORDER BY created_at ASC"
    end

    define(:pg_insert_child_workflow, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "workflows")} (
          id, name, worker_pool, status, input,
          child_origin_kind, parent_workflow_id, parent_command_id,
          parent_object_type, parent_object_id, parent_object_command_id,
          child_cancellation_policy
        ) VALUES (
          $1, $2, $3, $4, $5::bytea,
          $6, $7, $8,
          $9, $10, $11,
          $12
        )
      SQL
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

    # Lease-only rows (`claim_object_lease` inserts a row before state has been written)
    # carry a NULL `state`; the IS NOT NULL filter makes the read return zero rows in
    # that case so `object_state_entry` reports `NO_OBJECT_STATE` and the caller treats
    # the object as uninitialized.
    define(:pg_object_state, backend: :postgres) do |store|
      "SELECT state FROM #{table(store, "durable_objects")} WHERE object_type = $1 AND object_id = $2 AND state IS NOT NULL"
    end

    # worker_pool is routing metadata, not identity — on conflict the clause leaves it at the first
    # writer's value so the routing pool for an object is fixed at creation time.
    define(:pg_save_object_state, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "durable_objects")} (worker_pool, object_type, object_id, state)
        VALUES ($1, $2, $3, $4::bytea)
        ON CONFLICT (object_type, object_id) DO UPDATE
          SET state = $4::bytea, updated_at = now()
      SQL
    end

    define(:pg_upsert_object_wakeup, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "object_wakeups")} (worker_pool, object_type, object_id, name, wake_at, payload)
        VALUES ($1, $2, $3, $4, $5::timestamptz, $6::bytea)
        ON CONFLICT (worker_pool, object_type, object_id, name) DO UPDATE
          SET wake_at = EXCLUDED.wake_at,
              payload = EXCLUDED.payload,
              updated_at = now()
      SQL
    end

    define(:pg_delete_object_wakeup, backend: :postgres) do |store|
      "DELETE FROM #{table(store, "object_wakeups")} WHERE worker_pool = $1 AND object_type = $2 AND object_id = $3 AND name = $4"
    end

    define(:pg_delete_all_object_wakeups, backend: :postgres) do |store|
      "DELETE FROM #{table(store, "object_wakeups")} WHERE worker_pool = $1 AND object_type = $2 AND object_id = $3"
    end

    define(:pg_mark_waits_workflows_pending, backend: :postgres) do |store, placeholders:|
      "UPDATE #{table(store, "workflows")} SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id IN (#{placeholders}) AND status = 'waiting'"
    end

    define(:pg_complete_step, backend: :postgres) do |store|
      "UPDATE #{table(store, "steps")} SET status = 'completed', result = $3::bytea, error = NULL, completed_at = now(), updated_at = now() WHERE workflow_id = $1 AND position = $2"
    end

    define(:pg_update_latest_attempt, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = $3, result = $4::bytea, error = $5, completed_at = now()
        WHERE id = (
          SELECT id FROM #{table(store, "step_attempts")}
          WHERE workflow_id = $1 AND position = $2 AND status IN ('running', 'waiting')
          ORDER BY started_at DESC
          LIMIT 1
        )
      SQL
    end

    define(:mysql_claim_runnable_workflow, backend: :mysql, description: "Probe the unified workflow queue for one runnable candidate in this worker pool.") do |store, name_sql:|
      <<~SQL.chomp
        SELECT id, created_at FROM #{table(store, "workflows")} FORCE INDEX (#{index_name(store, "workflows", "claim")})
        WHERE worker_pool = ?
          AND queue_available_at <= NOW(6)
          #{name_sql}
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:mysql_claim_selected_workflow, backend: :mysql, description: "Attach this worker lease to the selected workflow candidate.") do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ? AND worker_pool = ?
          AND (
            (status = 'pending' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
            OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))
            OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
            OR (status = 'running' AND locked_until < NOW(6))
          )
      SQL
    end

    define(:mysql_claim_workflow_lock, backend: :mysql, description: "Lock a target workflow row before a MySQL lease update.") do |store|
      <<~SQL.chomp
        SELECT id FROM #{table(store, "workflows")}
        WHERE id = ? AND worker_pool = ?
          AND (
            (status = 'pending' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
            OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))
            OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
            OR (status = 'running' AND (locked_by = ? OR locked_until < NOW(6)))
          )
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:mysql_claim_workflow_update, backend: :mysql, description: "Move a target workflow to running and attach this worker lease.") do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'running', error = NULL, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ? AND worker_pool = ?
      SQL
    end

    define(:mysql_heartbeat_workflow, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
        WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)
      SQL
    end

    define(:mysql_workflow_owned, backend: :mysql) do |store|
      <<~SQL.chomp
        SELECT 1
        FROM #{table(store, "workflows")}
        WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)
      SQL
    end

    define(:mysql_heartbeat_step_workflow, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
        WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)
      SQL
    end

    define(:mysql_running_step_exists, backend: :mysql) do |store|
      "SELECT 1 FROM #{table(store, "steps")} WHERE workflow_id = ? AND position = ? AND status = 'running'"
    end

    define(:mysql_heartbeat_step_row, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET heartbeat_cursor = ?, updated_at = NOW(6)
        WHERE workflow_id = ? AND position = ? AND status = 'running'
      SQL
    end

    define(:mysql_heartbeat_latest_attempt, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET heartbeat_cursor = ?
        WHERE workflow_id = ? AND position = ? AND status = 'running'
        ORDER BY started_at DESC
        LIMIT 1
      SQL
    end

    define(:mysql_workflow_locked_until, backend: :mysql) do |store|
      "SELECT locked_until FROM #{table(store, "workflows")} WHERE id = ?"
    end

    define(:mysql_current_workflow_lease, backend: :mysql) do |store|
      <<~SQL.chomp
        SELECT id AS workflow_id, worker_pool, locked_by AS worker_id, locked_until
        FROM #{table(store, "workflows")}
        WHERE id = ? AND status = 'running' AND locked_by IS NOT NULL AND locked_until >= NOW(6)
      SQL
    end

    define(:mysql_supersede_running_step_attempts, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = 'failed', error = 'superseded by retry', completed_at = NOW(6)
        WHERE workflow_id = ? AND position = ? AND status = 'running'
      SQL
    end

    define(:mysql_upsert_step_running, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, started_at, updated_at)
        VALUES (?, ?, ?, 'running', NOW(6), NOW(6))
        ON DUPLICATE KEY UPDATE status = 'running', error = NULL, started_at = COALESCE(#{table(store, "steps")}.started_at, NOW(6)), updated_at = NOW(6)
      SQL
    end

    define(:mysql_insert_step_attempt, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "step_attempts")} (id, workflow_id, position, name, status)
        VALUES (?, ?, ?, ?, 'running')
      SQL
    end

    define(:mysql_count_workflow_leases, backend: :mysql) do |store, index:|
      "SELECT COUNT(*) AS count FROM #{table(store, "workflows")} FORCE INDEX (#{index}) WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_release_workflow_leases, backend: :mysql) do |store, index:|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")} FORCE INDEX (#{index})
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
        WHERE status = 'running' AND locked_by = ?
      SQL
    end

    define(:mysql_release_outbox_leases, backend: :mysql) do |store, index:|
      <<~SQL.chomp
        UPDATE #{table(store, "outbox")} FORCE INDEX (#{index})
        SET status = 'pending', locked_by = NULL, locked_until = NULL
        WHERE status = 'processing' AND locked_by = ?
      SQL
    end

    define(:mysql_count_outbox_leases, backend: :mysql) do |store, index:|
      "SELECT COUNT(*) AS count FROM #{table(store, "outbox")} FORCE INDEX (#{index}) WHERE status = 'processing' AND locked_by = ?"
    end

    define(:mysql_claim_pending_outbox, backend: :mysql) do |store|
      <<~SQL.chomp
        SELECT id, created_at FROM #{table(store, "outbox")}
        WHERE status = 'pending'
        ORDER BY created_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:mysql_claim_expired_outbox, backend: :mysql) do |store|
      <<~SQL.chomp
        SELECT id, created_at FROM #{table(store, "outbox")} FORCE INDEX (#{index_name(store, "outbox", "expired_lease")})
        WHERE status = 'processing' AND locked_until < NOW(6)
        ORDER BY created_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:mysql_claim_selected_outbox, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "outbox")}
        SET status = 'processing', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND)
        WHERE id = ?
      SQL
    end

    define(:mysql_ack_outbox, backend: :mysql) do |store|
      # [DURABABBLE-WF-1] Acks require the live sender lease: an owner whose lease
      # expired before another worker reclaimed the row cannot mark it processed.
      "UPDATE #{table(store, "outbox")} SET status = 'processed', processed_at = NOW(6) WHERE id = ? AND locked_by = ? AND locked_until >= NOW(6)"
    end

    define(:mysql_outbox_message, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "outbox")} WHERE id = ?"
    end

    define(:mysql_workflow, backend: :mysql, description: "Read one workflow row by id after a claim or for runtime state.") do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = ?"
    end

    define(:mysql_child_workflow_by_child_id_for_update, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = ? AND child_origin_kind IS NOT NULL FOR UPDATE"
    end

    define(:mysql_child_workflow_rows_for_parent, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE child_origin_kind = 'workflow' AND parent_workflow_id = ? ORDER BY created_at ASC"
    end

    define(:mysql_child_workflow_rows_for_object, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE child_origin_kind = 'object' AND parent_object_type = ? AND parent_object_id = ? ORDER BY created_at ASC"
    end

    define(:mysql_insert_child_workflow, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "workflows")} (
          id, name, worker_pool, status, input,
          child_origin_kind, parent_workflow_id, parent_command_id,
          parent_object_type, parent_object_id, parent_object_command_id,
          child_cancellation_policy, created_at, updated_at
        ) VALUES (
          ?, ?, ?, ?, ?,
          ?, ?, ?,
          ?, ?, ?,
          ?, NOW(6), NOW(6)
        )
      SQL
    end

    define(:mysql_steps_for, backend: :mysql, description: "Read step rows for a workflow in command order.") do |store|
      "SELECT * FROM #{table(store, "steps")} WHERE workflow_id = ? ORDER BY position"
    end

    define(:mysql_outbox_by_key, backend: :mysql) do |store|
      "SELECT id FROM #{table(store, "outbox")} WHERE `key` = ?"
    end

    define(:mysql_insert_outbox, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT IGNORE INTO #{table(store, "outbox")} (id, workflow_id, topic, payload, `key`, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
      SQL
    end

    define(:mysql_step_attempts_for, backend: :mysql, description: "Read step attempts for a workflow in replay order.") do |store|
      "SELECT * FROM #{table(store, "step_attempts")} WHERE workflow_id = ? ORDER BY started_at, position"
    end

    define(:mysql_step_attempt_count_for, backend: :mysql, description: "Count attempts for one workflow command.") do |store|
      "SELECT COUNT(*) AS count FROM #{table(store, "step_attempts")} WHERE workflow_id = ? AND position = ?"
    end

    # See pg_object_state — the IS NOT NULL filter hides lease-only rows so reads
    # of an uninitialized object return zero rows.
    define(:mysql_object_state, backend: :mysql) do |store|
      "SELECT state FROM #{table(store, "durable_objects")} WHERE object_type = ? AND object_id = ? AND state IS NOT NULL"
    end

    define(:pg_drop_schema, backend: :postgres) do |store|
      "DROP SCHEMA IF EXISTS #{store.send(:quoted_schema)} CASCADE"
    end

    define(:pg_insert_workflow, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "workflows")} (id, name, worker_pool, status, input) VALUES ($1, $2, $3, $4, $5::bytea)"
    end

    define(:pg_insert_workflow_with_worker, backend: :postgres) do |store|
      "INSERT INTO #{table(store, "workflows")} (id, name, worker_pool, status, input, locked_by, locked_until) VALUES ($1, $2, $3, $4, $5::bytea, $6, now() + ($7::int * interval '1 second'))"
    end

    define(:pg_claim_workflow_for_activation_update, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'running', error = NULL, locked_by = $3,
            locked_until = now() + ($4::int * interval '1 second'), next_run_at = NULL, updated_at = now()
        WHERE id = $1 AND worker_pool = $2
          AND (
            (status = 'pending' AND (next_run_at IS NULL OR next_run_at <= now()))
            OR status = 'waiting'
            OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= now()))
            OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= now())
            OR (status = 'running' AND (locked_by = $3 OR locked_until < now()))
          )
        RETURNING *
      SQL
    end

    define(:pg_schedule_workflow_retry, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, next_run_at = $3::timestamptz, updated_at = now()
        WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
      SQL
    end

    define(:pg_suspend_workflow, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            WHEN EXISTS (SELECT 1 FROM #{table(store, "waits")} WHERE workflow_id = $1 AND status = 'pending') THEN 'waiting'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE id = $1 AND status = 'running'
          AND ($2::text IS NULL OR (locked_by = $2::text AND locked_until >= now()))
      SQL
    end

    define(:pg_make_workflow_due, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET next_run_at = NULL, updated_at = $2::timestamptz WHERE id = $1"
    end

    define(:pg_request_workflow_cancellation, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET cancel_reason = $2, cancel_requested_at = now(), updated_at = now()
        WHERE id = $1
      SQL
    end

    define(:pg_mark_workflow_canceling_for_request, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'canceling', locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now()
        WHERE id = $1 AND status NOT IN ('completed', 'canceled')
      SQL
    end

    define(:pg_workflow_cancellation, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT id AS workflow_id, cancel_reason AS reason,
        cancel_requested_at AS requested_at, cancel_delivered_at AS delivered_at
        FROM #{table(store, "workflows")}
        WHERE id = $1 AND cancel_requested_at IS NOT NULL
      SQL
    end

    define(:pg_mark_workflow_cancellation_delivered, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET cancel_delivered_at = COALESCE(cancel_delivered_at, now()), updated_at = now()
        WHERE id = $1 AND cancel_requested_at IS NOT NULL
      SQL
    end

    define(:pg_step_heartbeat_cursor, backend: :postgres) do |store|
      "SELECT heartbeat_cursor FROM #{table(store, "steps")} WHERE workflow_id = $1 AND position = $2"
    end

    # Live ownership of the unified object lease: the row exists on the durable_objects
    # table (locked_by / locked_until columns), filled by claim_object_lease and cleared on
    # release / expiry. Routes both stream and command lanes to the holder.
    define(:pg_current_object_lease, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT worker_pool, object_type, object_id, locked_by AS worker_id, locked_until
        FROM #{table(store, "durable_objects")}
        WHERE object_type = $1 AND object_id = $2
          AND locked_by IS NOT NULL AND locked_until >= now()
        LIMIT 1
      SQL
    end

    # Atomic object-lease claim. Inserts the durable_objects row if missing; takes
    # ownership only when no live lease exists OR the caller is already the holder.
    # `RETURNING` emits a row only when the INSERT/UPDATE actually fires — on
    # rejection the result is empty, and `Postgres#claim_object_lease` translates
    # that to `nil`. Callers compare against nil to detect a lost claim; we
    # deliberately do not surface the competing holder's identity here.
    define(:pg_claim_object_lease, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "durable_objects")}
          (worker_pool, object_type, object_id, locked_by, locked_until, created_at, updated_at)
        VALUES ($1, $2, $3, $4, now() + ($5::int * interval '1 second'), now(), now())
        ON CONFLICT (object_type, object_id) DO UPDATE
        SET locked_by = EXCLUDED.locked_by,
            locked_until = EXCLUDED.locked_until,
            updated_at = now()
        WHERE #{table(store, "durable_objects")}.locked_by IS NULL
           OR #{table(store, "durable_objects")}.locked_until < now()
           OR #{table(store, "durable_objects")}.locked_by = EXCLUDED.locked_by
        RETURNING worker_pool, object_type, object_id, locked_by AS worker_id, locked_until
      SQL
    end

    # Conditional renew: only succeeds while the caller still holds a live lease.
    # Returning row lets the host detect lost ownership (no row = lost).
    define(:pg_renew_object_lease, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "durable_objects")}
        SET locked_until = now() + ($4::int * interval '1 second'), updated_at = now()
        WHERE object_type = $1 AND object_id = $2
          AND locked_by = $3 AND locked_until >= now()
        RETURNING worker_pool, object_type, object_id, locked_by AS worker_id, locked_until
      SQL
    end

    # Command outcome commits must still own the unified object lease. This
    # mirrors workflow commit fencing: the inbox row lease alone is not enough once
    # reads and streams route to durable_objects.locked_by as the active owner.
    define(:pg_lock_owned_object_for_update, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT 1
        FROM #{table(store, "durable_objects")}
        WHERE object_type = $1 AND object_id = $2
          AND locked_by = $3 AND locked_until >= now()
        FOR UPDATE
      SQL
    end

    # Idempotent release; non-owner workers cannot release someone else's lease.
    define(:pg_release_object_lease, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "durable_objects")}
        SET locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE object_type = $1 AND object_id = $2 AND locked_by = $3
      SQL
    end

    # Reclaim object leases past their locked_until deadline. Paired with the
    # workflow-side steal sweep on the same expiry timer.
    define(:pg_steal_expired_object_leases, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "durable_objects")}
        SET locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE locked_by IS NOT NULL AND locked_until < $1::timestamptz
      SQL
    end

    # Graceful release of every object lease this worker still owns. Called from
    # release_worker_leases! on shutdown.
    define(:pg_release_worker_object_leases, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "durable_objects")}
        SET locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE locked_by = $1
      SQL
    end

    define(:pg_mark_workflow_running, backend: :postgres) do |store|
      # [DURABABBLE-WF-1] The unfenced create path only activates a fresh, unowned pending row.
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'running', error = NULL, next_run_at = NULL, updated_at = now()
        WHERE id = $1 AND worker_pool = $2 AND status = 'pending' AND locked_by IS NULL
      SQL
    end

    define(:pg_mark_workflow_running_with_worker, backend: :postgres) do |store|
      # [DURABABBLE-WF-1] Terminal rows must stay terminal even when a worker holds a lease.
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'running', error = NULL, locked_by = $1,
            locked_until = now() + ($2::int * interval '1 second'), next_run_at = NULL, updated_at = now()
        WHERE id = $3 AND worker_pool = $4
          AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))
      SQL
    end

    define(:pg_lock_workflow_for_termination, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = $1 FOR UPDATE"
    end

    define(:pg_terminate_workflow, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'terminated', result = $2::bytea, error = $3,\n  " \
        "locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now()\n" \
        "WHERE id = $1"
    end

    define(:pg_complete_workflow_with_worker, backend: :postgres) do |store|
      # [DURABABBLE-WF-1] Workflow completion requires the running lease; live durable work
      # is cleaned up after the UPDATE by the surrounding helper.
      "UPDATE #{table(store, "workflows")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() " \
        "WHERE id = $1 AND status = 'running' AND locked_by = $3 AND locked_until >= now()"
    end

    define(:pg_complete_workflow, backend: :postgres) do |store|
      # [DURABABBLE-WF-1] Unfenced completion still rejects terminal rows and live durable work.
      "UPDATE #{table(store, "workflows")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() " \
        "WHERE id = $1 " \
        "AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL)) " \
        "AND NOT EXISTS (SELECT 1 FROM #{table(store, "steps")} WHERE workflow_id = $1 AND status IN ('scheduled', 'running', 'waiting')) " \
        "AND NOT EXISTS (SELECT 1 FROM #{table(store, "step_attempts")} WHERE workflow_id = $1 AND status IN ('running', 'waiting')) " \
        "AND NOT EXISTS (SELECT 1 FROM #{table(store, "waits")} WHERE workflow_id = $1 AND status = 'pending')"
    end

    define(:pg_cancel_workflow_with_worker, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'canceled', result = $2::bytea, error = $3, cancel_reason = COALESCE(cancel_reason, $3), cancel_requested_at = COALESCE(cancel_requested_at, now()), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $4 AND locked_until >= now()"
    end

    define(:pg_cancel_workflow, backend: :postgres) do |store|
      # [DURABABBLE-WF-1] Unfenced cancel never stomps a terminal row.
      "UPDATE #{table(store, "workflows")} SET status = 'canceled', result = $2::bytea, error = $3, cancel_reason = COALESCE(cancel_reason, $3), cancel_requested_at = COALESCE(cancel_requested_at, now()), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1 AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))"
    end

    define(:pg_fail_workflow_with_worker, backend: :postgres) do |store|
      "UPDATE #{table(store, "workflows")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $3 AND locked_until >= now()"
    end

    define(:pg_fail_workflow, backend: :postgres) do |store|
      # [DURABABBLE-WF-1] Unfenced fail never stomps a terminal row.
      "UPDATE #{table(store, "workflows")} SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1 AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))"
    end

    define(:pg_terminate_workflow_waits, backend: :postgres) do |store|
      "UPDATE #{table(store, "waits")} SET status = 'canceled', completed_at = now() WHERE workflow_id = $1 AND status = 'pending'"
    end

    define(:pg_terminate_workflow_steps, backend: :postgres) do |store|
      "UPDATE #{table(store, "steps")} SET status = 'canceled', error = $2, updated_at = now() WHERE workflow_id = $1 AND status IN ('scheduled', 'running', 'waiting')"
    end

    define(:pg_terminate_workflow_step_attempts, backend: :postgres) do |store|
      "UPDATE #{table(store, "step_attempts")} SET status = 'canceled', error = $2, completed_at = now() WHERE workflow_id = $1 AND status IN ('running', 'waiting')"
    end

    define(:pg_terminate_workflow_inbox, backend: :postgres) do |store|
      "UPDATE #{table(store, "inbox")}\n" \
        "SET status = 'dead_lettered', error = $2, locked_by = NULL, locked_until = NULL, dead_lettered_at = now(), updated_at = now()\n" \
        "WHERE target_kind = 'workflow' AND target_id = $1 AND status IN ('pending', 'failed', 'running')"
    end

    define(:pg_terminate_workflow_target_activations, backend: :postgres) do |store|
      "DELETE FROM #{table(store, "target_activations")} WHERE target_kind = 'workflow' AND target_id = $1"
    end

    define(:pg_insert_scheduled_step, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, updated_at)
        VALUES ($1, $2, $3, 'scheduled', now())
        ON CONFLICT (workflow_id, position) DO NOTHING
      SQL
    end

    define(:pg_cancel_step, backend: :postgres) do |store|
      "UPDATE #{table(store, "steps")} SET status = 'canceled', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2 AND status IN ('scheduled', 'running', 'waiting')"
    end

    define(:pg_fail_step, backend: :postgres) do |store|
      "UPDATE #{table(store, "steps")} SET status = 'failed', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2"
    end

    define(:pg_cancel_pending_waits_for_workflow, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "waits")}
        SET status = 'canceled', completed_at = now()
        WHERE workflow_id = $1 AND status = 'pending'
      SQL
    end

    define(:pg_cancel_waiting_steps_for_workflow, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET status = 'canceled', error = 'workflow cancellation requested', updated_at = now()
        WHERE workflow_id = $1 AND status = 'waiting'
      SQL
    end

    define(:pg_cancel_waiting_step_attempts_for_workflow, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = 'canceled', error = 'workflow cancellation requested', completed_at = now()
        WHERE workflow_id = $1 AND status = 'waiting'
      SQL
    end

    define(:pg_cancel_live_steps_for_workflow, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET status = 'canceled', error = 'workflow cancellation requested', updated_at = now()
        WHERE workflow_id = $1 AND status IN ('scheduled', 'running', 'waiting')
      SQL
    end

    define(:pg_cancel_live_step_attempts_for_workflow, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = 'canceled', error = 'workflow cancellation requested', completed_at = now()
        WHERE workflow_id = $1 AND status IN ('running', 'waiting')
      SQL
    end

    define(:pg_fail_live_steps_for_workflow, backend: :postgres) do |store|
      # Failing a workflow turns any actively-running step into a 'failed' step
      # that carries the workflow's error. Scheduled and waiting steps stay on
      # the cancel cleanup path — they never observed the error, so 'canceled'
      # is the truer post-condition.
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET status = 'failed', error = $2, updated_at = now()
        WHERE workflow_id = $1 AND status = 'running'
      SQL
    end

    define(:pg_fail_live_step_attempts_for_workflow, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = 'failed', error = $2, completed_at = now()
        WHERE workflow_id = $1 AND status = 'running'
      SQL
    end

    define(:pg_claim_target_activation, backend: :postgres) do |store, filter_sql:|
      <<~SQL.chomp
        SELECT worker_pool, target_kind, target_type, target_id, ready_at, created_at FROM #{table(store, "target_activations")}
        WHERE worker_pool = $1
          AND (#{POSTGRES_TARGET_ACTIVATION_CLAIM_EXPRESSION}) <= $2::timestamptz
          #{filter_sql}
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:pg_claim_selected_target_activation, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "target_activations")}
        SET status = 'running', locked_by = $5, locked_until = now() + ($6::int * interval '1 second'), updated_at = now()
        WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4
        RETURNING *
      SQL
    end

    define(:pg_lock_target_activation_for_completion, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT 1 FROM #{table(store, "target_activations")}
        WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4
          AND status = 'running' AND locked_by = $5
          AND locked_until >= now()
        FOR UPDATE
      SQL
    end

    # worker_pool is routing metadata, not identity — on conflict the clause leaves it at the first
    # writer's value so the routing pool for a target activation is fixed at creation time.
    define(:pg_upsert_target_activation, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "target_activations")} (worker_pool, target_kind, target_type, target_id, status, ready_at)
        VALUES ($1, $2, $3, $4, 'pending', $5::timestamptz)
        ON CONFLICT (target_kind, target_type, target_id) DO UPDATE
          SET status = CASE WHEN #{table(store, "target_activations")}.status = 'running' THEN #{table(store, "target_activations")}.status ELSE 'pending' END,
          ready_at = LEAST(#{table(store, "target_activations")}.ready_at, EXCLUDED.ready_at), updated_at = now()
      SQL
    end

    define(:pg_delete_target_activation, backend: :postgres) do |store|
      "DELETE FROM #{table(store, "target_activations")} WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4"
    end

    # Explicit rearm/reconcile writes are scoped to the activation's routing
    # pool; a caller from another pool must not clear an active lease.
    define(:pg_set_target_activation_pending, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "target_activations")} (worker_pool, target_kind, target_type, target_id, status, ready_at)
        VALUES ($1, $2, $3, $4, 'pending', $5::timestamptz)
        ON CONFLICT (target_kind, target_type, target_id) DO UPDATE
          SET status = 'pending', ready_at = EXCLUDED.ready_at, locked_by = NULL, locked_until = NULL, updated_at = now()
          WHERE #{table(store, "target_activations")}.worker_pool = EXCLUDED.worker_pool
      SQL
    end

    # worker_pool is routing metadata, not identity — DO NOTHING on conflict keeps the first
    # writer's value so the routing pool for a mailbox is fixed at creation time.
    define(:pg_insert_mailbox_sequence, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "mailbox_sequences")} (worker_pool, target_kind, target_type, target_id, last_sequence)
        VALUES ($1, $2, $3, $4, 0)
        ON CONFLICT (target_kind, target_type, target_id) DO NOTHING
      SQL
    end

    define(:pg_mailbox_sequence_for_update, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT worker_pool, last_sequence
        FROM #{table(store, "mailbox_sequences")}
        WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
        FOR UPDATE
      SQL
    end

    define(:pg_update_mailbox_sequence, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "mailbox_sequences")}
        SET last_sequence = $1, updated_at = now()
        WHERE target_kind = $2 AND target_type = $3 AND target_id = $4
      SQL
    end

    define(:pg_existing_inbox_message_for_idempotency, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT id, worker_pool, target_kind, target_type, target_id, status, ready_at, shape_hash
        FROM #{table(store, "inbox")}
        WHERE idempotency_hash = $1
        FOR UPDATE
      SQL
    end

    define(:pg_lock_workflow_for_update, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = $1 FOR UPDATE"
    end

    define(:pg_lock_owned_workflow_for_update, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT 1
        FROM #{table(store, "workflows")}
        WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
        FOR UPDATE
      SQL
    end

    define(:pg_insert_inbox_message, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "inbox")} (
        id, worker_pool, target_kind, target_type, target_id, sequence, message_kind, method_name,
        operation_id, idempotency_key, idempotency_hash, shape_hash, payload, status, ready_at, max_attempts
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::bytea, 'pending', $14::timestamptz, $15)
      SQL
    end

    define(:pg_inbox_claim_rows_for_update, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT *
        FROM #{table(store, "inbox")}
        WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4
          AND status IN ('pending', 'failed', 'running', 'dead_lettered')
        ORDER BY sequence
        LIMIT $5
        FOR UPDATE
      SQL
    end

    define(:pg_inbox_head_for_update, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT *
        FROM #{table(store, "inbox")}
        WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4
          AND status IN ('pending', 'failed', 'running', 'dead_lettered')
        ORDER BY sequence
        LIMIT 1
        FOR UPDATE
      SQL
    end

    define(:pg_lock_inbox_message_for_worker, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT * FROM #{table(store, "inbox")}
        WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
        FOR UPDATE
      SQL
    end

    define(:pg_lock_inbox_message, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "inbox")} WHERE id = $1 FOR UPDATE"
    end

    define(:pg_mark_inbox_row_running, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "inbox")}
        SET status = 'running', attempts = attempts + 1, locked_by = $2, locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
        WHERE id = $1
      SQL
    end

    define(:pg_complete_inbox_message, backend: :postgres) do |store|
      "UPDATE #{table(store, "inbox")} SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = now(), updated_at = now() WHERE id = $1"
    end

    define(:pg_fail_inbox_message, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "inbox")}
        SET status = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'dead_lettered' ELSE 'failed' END,
          error = $2, locked_by = NULL, locked_until = NULL,
          dead_lettered_at = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN now() ELSE dead_lettered_at END,
          updated_at = now()
        WHERE id = $1
      SQL
    end

    define(:pg_retry_inbox_message, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "inbox")}
        SET status = 'pending', error = $2, ready_at = $3::timestamptz, locked_by = NULL, locked_until = NULL, updated_at = now()
        WHERE id = $1
      SQL
    end

    define(:pg_dead_letter_inbox_message, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "inbox")}
        SET status = 'dead_lettered', error = $2, locked_by = NULL, locked_until = NULL, dead_lettered_at = now(), updated_at = now()
        WHERE id = $1
      SQL
    end

    define(:pg_complete_timer_waits, backend: :postgres) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "waits")}
        SET status = 'completed', payload = $2::bytea, completed_at = now()
        WHERE id IN (
          SELECT w.id FROM #{table(store, "waits")} AS w
          JOIN #{table(store, "workflows")} AS wf ON wf.id = w.workflow_id
          WHERE w.status = 'pending'
            AND wf.status IN ('waiting', 'running')
            AND w.kind = 'timer'
            AND w.wake_at <= $1::timestamptz
          ORDER BY w.wake_at, w.created_at
          LIMIT $3
          FOR UPDATE OF w SKIP LOCKED
        )
        RETURNING *
      SQL
    end

    define(:pg_due_object_wakeups, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT *
        FROM #{table(store, "object_wakeups")}
        WHERE wake_at <= $1::timestamptz
        ORDER BY wake_at, created_at
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:pg_lock_workflow_history_workflow, backend: :postgres) do |store|
      "SELECT id FROM #{table(store, "workflows")} WHERE id = $1 FOR UPDATE"
    end

    define(:pg_insert_workflow_history, backend: :postgres) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "workflow_history")} (workflow_id, event_index, kind, command_id, name, attempt_id, payload, error)
        SELECT $1, COALESCE(MAX(event_index), -1) + 1, $2, $3, $4, $5, $6::bytea, $7
        FROM #{table(store, "workflow_history")}
        WHERE workflow_id = $8
        RETURNING event_index
      SQL
    end

    define(:pg_workflow_history_for, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "workflow_history")} WHERE workflow_id = $1 ORDER BY event_index"
    end

    define(:pg_workflow_history_count_for, backend: :postgres) do |store|
      "SELECT COUNT(*) AS count FROM #{table(store, "workflow_history")} WHERE workflow_id = $1"
    end

    define(:pg_inbox_message, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "inbox")} WHERE id = $1"
    end

    define(:pg_inbox_messages_for, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT * FROM #{table(store, "inbox")}
        WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
        ORDER BY sequence
      SQL
    end

    define(:pg_inbox_messages_for_worker_pool, backend: :postgres) do |store|
      <<~SQL.chomp
        SELECT * FROM #{table(store, "inbox")}
        WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4
        ORDER BY sequence
      SQL
    end

    define(:pg_target_activation, backend: :postgres) do |store|
      "SELECT * FROM #{table(store, "target_activations")} WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4"
    end

    define(:mysql_drop_table, backend: :mysql) do |store, table_name:|
      "DROP TABLE IF EXISTS #{table(store, table_name)}"
    end

    define(:mysql_insert_workflow, backend: :mysql, description: "Insert a pending workflow row with serialized input.") do |store|
      "INSERT INTO #{table(store, "workflows")} (id, name, worker_pool, status, input, created_at, updated_at) VALUES (?, ?, ?, ?, ?, NOW(6), NOW(6))"
    end

    define(:mysql_insert_workflow_with_worker, backend: :mysql, description: "Insert a running workflow row with an initial worker lease.") do |store|
      "INSERT INTO #{table(store, "workflows")} (id, name, worker_pool, status, input, locked_by, locked_until, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, DATE_ADD(NOW(6), INTERVAL ? SECOND), NOW(6), NOW(6))"
    end

    define(:mysql_mark_workflow_running_with_worker, backend: :mysql, description: "Move a workflow to running and attach a worker lease.") do |store|
      # [DURABABBLE-WF-1] Terminal rows must stay terminal even when a worker holds a lease.
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
        WHERE id = ? AND worker_pool = ?
          AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))
      SQL
    end

    define(:mysql_mark_workflow_running, backend: :mysql, description: "Move a workflow to running without attaching a lease.") do |store|
      # [DURABABBLE-WF-1] The unfenced create path only activates a fresh, unowned pending row.
      "UPDATE #{table(store, "workflows")} SET status = 'running', error = NULL, updated_at = NOW(6) WHERE id = ? AND worker_pool = ? AND status = 'pending' AND locked_by IS NULL"
    end

    define(:mysql_claim_workflow_for_activation_lock, backend: :mysql, description: "Lock a workflow activation target before claiming it.") do |store|
      <<~SQL.chomp
        SELECT id FROM #{table(store, "workflows")}
        WHERE id = ? AND worker_pool = ?
          AND (
            (status = 'pending' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
            OR status = 'waiting'
            OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
            OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))
            OR (status = 'running' AND (locked_by = ? OR locked_until < NOW(6)))
          )
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:mysql_claim_workflow_for_activation_update, backend: :mysql, description: "Claim a workflow through its activation path.") do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'running', error = NULL, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
        WHERE id = ? AND worker_pool = ?
      SQL
    end

    define(:mysql_count_inbox_leases, backend: :mysql) do |store, index:|
      "SELECT COUNT(*) AS count FROM #{table(store, "inbox")} FORCE INDEX (#{index}) WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_release_inbox_leases, backend: :mysql) do |store, index:|
      <<~SQL.chomp
        UPDATE #{table(store, "inbox")} FORCE INDEX (#{index})
        SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
        WHERE status = 'running' AND locked_by = ?
      SQL
    end

    define(:mysql_count_target_activation_leases, backend: :mysql) do |store, index:|
      "SELECT COUNT(*) AS count FROM #{table(store, "target_activations")} FORCE INDEX (#{index}) WHERE status = 'running' AND locked_by = ?"
    end

    define(:mysql_release_target_activation_leases, backend: :mysql) do |store, index:|
      <<~SQL.chomp
        UPDATE #{table(store, "target_activations")} FORCE INDEX (#{index})
        SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
        WHERE status = 'running' AND locked_by = ?
      SQL
    end

    define(:mysql_schedule_workflow_retry, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, next_run_at = ?, updated_at = NOW(6)
        WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
      SQL
    end

    define(:mysql_suspend_workflow, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            WHEN EXISTS (SELECT 1 FROM #{table(store, "waits")} FORCE INDEX (#{index_name(store, "waits", "workflow_status")}) WHERE workflow_id = ? AND status = 'pending') THEN 'waiting'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
        WHERE id = ? AND status = 'running'
          AND (? IS NULL OR (locked_by = ? AND locked_until >= NOW(6)))
      SQL
    end

    define(:mysql_make_workflow_due, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")} SET next_run_at = NULL, updated_at = ? WHERE id = ?"
    end

    define(:mysql_request_workflow_cancellation, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET cancel_reason = ?, cancel_requested_at = NOW(6), updated_at = NOW(6)
        WHERE id = ?
      SQL
    end

    define(:mysql_mark_workflow_canceling_for_request, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'canceling', locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ? AND status NOT IN ('completed', 'canceled')
      SQL
    end

    define(:mysql_workflow_cancellation, backend: :mysql) do |store|
      <<~SQL.chomp
        SELECT id AS workflow_id, cancel_reason AS reason,
        cancel_requested_at AS requested_at, cancel_delivered_at AS delivered_at
        FROM #{table(store, "workflows")}
        WHERE id = ? AND cancel_requested_at IS NOT NULL
      SQL
    end

    define(:mysql_mark_workflow_cancellation_delivered, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET cancel_delivered_at = COALESCE(cancel_delivered_at, NOW(6)), updated_at = NOW(6)
        WHERE id = ? AND cancel_requested_at IS NOT NULL
      SQL
    end

    define(:mysql_step_heartbeat_cursor, backend: :mysql) do |store|
      "SELECT heartbeat_cursor FROM #{table(store, "steps")} WHERE workflow_id = ? AND position = ?"
    end

    # MySQL mirror of pg_current_object_lease — see notes on the PG variant.
    define(:mysql_current_object_lease, backend: :mysql) do |store|
      <<~SQL.chomp
        SELECT worker_pool, object_type, object_id, locked_by AS worker_id, locked_until
        FROM #{table(store, "durable_objects")}
        WHERE object_type = ? AND object_id = ?
          AND locked_by IS NOT NULL AND locked_until >= NOW(6)
        LIMIT 1
      SQL
    end

    # MySQL mirror of pg_claim_object_lease, split in two so we can read win/loss
    # off the UPDATE's affected_rows alone. A single-statement
    # `INSERT ... ON DUPLICATE KEY UPDATE` would not work here: the trilogy
    # adapter forces `CLIENT_FOUND_ROWS` on every connection, which makes a
    # no-op "row matched but unchanged" UPDATE report `affected_rows = 1`
    # — indistinguishable from a fresh insert and so unable to tell a winning
    # claim from a rejected one without a follow-up SELECT. Splitting it lets
    # the conditional WHERE on the UPDATE step do the gating: a matching row
    # means we won (insert or takeover or refresh-self); no match means we
    # lost. `mysql_ensure_object_row` just guarantees the PK exists so the
    # UPDATE has something to match.
    define(:mysql_ensure_object_row, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT IGNORE INTO #{table(store, "durable_objects")}
          (worker_pool, object_type, object_id, created_at, updated_at)
        VALUES (?, ?, ?, NOW(6), NOW(6))
      SQL
    end

    # Conditional claim against the guaranteed-present row. The WHERE only
    # matches when the lease is free, expired, or already ours; on a match the
    # UPDATE writes new values and `affected_rows = 1` (under CLIENT_FOUND_ROWS,
    # a matched-but-unchanged row also reports 1, but every matching branch of
    # this WHERE legitimately means we won, so the count is still unambiguous).
    define(:mysql_claim_object_lease, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "durable_objects")}
        SET locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
        WHERE object_type = ? AND object_id = ?
          AND (locked_by IS NULL OR locked_until < NOW(6) OR locked_by = ?)
      SQL
    end

    # MySQL mirror of pg_renew_object_lease.
    define(:mysql_renew_object_lease, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "durable_objects")}
        SET locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
        WHERE object_type = ? AND object_id = ?
          AND locked_by = ? AND locked_until >= NOW(6)
      SQL
    end

    # MySQL mirror of pg_lock_owned_object_for_update.
    define(:mysql_lock_owned_object_for_update, backend: :mysql) do |store|
      <<~SQL.chomp
        SELECT 1
        FROM #{table(store, "durable_objects")}
        WHERE object_type = ? AND object_id = ?
          AND locked_by = ? AND locked_until >= NOW(6)
        FOR UPDATE
      SQL
    end

    # MySQL mirror of pg_release_object_lease.
    define(:mysql_release_object_lease, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "durable_objects")}
        SET locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
        WHERE object_type = ? AND object_id = ? AND locked_by = ?
      SQL
    end

    # MySQL mirror of pg_steal_expired_object_leases.
    define(:mysql_steal_expired_object_leases, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "durable_objects")}
        SET locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
        WHERE locked_by IS NOT NULL AND locked_until < ?
      SQL
    end

    # MySQL mirror of pg_release_worker_object_leases.
    define(:mysql_release_worker_object_leases, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "durable_objects")}
        SET locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
        WHERE locked_by = ?
      SQL
    end

    define(:mysql_count_expired_workflow_leases, backend: :mysql) do |store|
      "SELECT COUNT(*) AS count FROM #{table(store, "workflows")} FORCE INDEX (#{index_name(store, "workflows", "expired_lease")}) WHERE status = 'running' AND locked_until < ?"
    end

    define(:mysql_steal_expired_leases, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")} FORCE INDEX (#{index_name(store, "workflows", "expired_lease")})
        SET status = CASE
            WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
            ELSE 'pending'
          END,
          locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
        WHERE status = 'running' AND locked_until < ?
      SQL
    end

    define(:mysql_insert_scheduled_step, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT IGNORE INTO #{table(store, "steps")} (workflow_id, position, name, status, updated_at)
        VALUES (?, ?, ?, 'scheduled', NOW(6))
      SQL
    end

    define(:mysql_complete_step, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET status = 'completed', result = ?, error = NULL, completed_at = NOW(6), updated_at = NOW(6)
        WHERE workflow_id = ? AND position = ?
      SQL
    end

    define(:mysql_lock_workflow_for_termination, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = ? FOR UPDATE"
    end

    define(:mysql_terminate_workflow, backend: :mysql) do |store|
      "UPDATE #{table(store, "workflows")}\n" \
        "SET status = 'terminated', result = ?, error = ?, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)\n" \
        "WHERE id = ?"
    end

    define(:mysql_complete_workflow_with_worker, backend: :mysql, description: "Mark a workflow completed while verifying the worker lease.") do |store|
      # [DURABABBLE-WF-1] Workflow completion requires the running lease; live durable work
      # is cleaned up after the UPDATE by the surrounding helper.
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
      SQL
    end

    define(:mysql_complete_workflow, backend: :mysql, description: "Mark a workflow completed when no worker lease check is required.") do |store|
      # [DURABABBLE-WF-1] Unfenced completion still rejects terminal rows and live durable work.
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ?
          AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))
          AND NOT EXISTS (SELECT 1 FROM #{table(store, "steps")} WHERE workflow_id = ? AND status IN ('scheduled', 'running', 'waiting'))
          AND NOT EXISTS (SELECT 1 FROM #{table(store, "step_attempts")} WHERE workflow_id = ? AND status IN ('running', 'waiting'))
          AND NOT EXISTS (SELECT 1 FROM #{table(store, "waits")} WHERE workflow_id = ? AND status = 'pending')
      SQL
    end

    define(:mysql_cancel_workflow_with_worker, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'canceled', result = ?, error = ?, cancel_reason = COALESCE(cancel_reason, ?),
          cancel_requested_at = COALESCE(cancel_requested_at, NOW(6)), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
      SQL
    end

    define(:mysql_cancel_workflow, backend: :mysql) do |store|
      # [DURABABBLE-WF-1] Unfenced cancel never stomps a terminal row.
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'canceled', result = ?, error = ?, cancel_reason = COALESCE(cancel_reason, ?),
          cancel_requested_at = COALESCE(cancel_requested_at, NOW(6)), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ? AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))
      SQL
    end

    define(:mysql_fail_workflow_with_worker, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'failed', error = ?, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
      SQL
    end

    define(:mysql_fail_workflow, backend: :mysql) do |store|
      # [DURABABBLE-WF-1] Unfenced fail never stomps a terminal row.
      <<~SQL.chomp
        UPDATE #{table(store, "workflows")}
        SET status = 'failed', error = ?, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
        WHERE id = ? AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))
      SQL
    end

    define(:mysql_terminate_workflow_waits, backend: :mysql) do |store|
      "UPDATE #{table(store, "waits")} SET status = 'canceled', completed_at = NOW(6) WHERE workflow_id = ? AND status = 'pending'"
    end

    define(:mysql_terminate_workflow_steps, backend: :mysql) do |store|
      "UPDATE #{table(store, "steps")} SET status = 'canceled', error = ?, updated_at = NOW(6) WHERE workflow_id = ? AND status IN ('scheduled', 'running', 'waiting')"
    end

    define(:mysql_terminate_workflow_step_attempts, backend: :mysql) do |store|
      "UPDATE #{table(store, "step_attempts")} SET status = 'canceled', error = ?, completed_at = NOW(6) WHERE workflow_id = ? AND status IN ('running', 'waiting')"
    end

    define(:mysql_terminate_workflow_inbox, backend: :mysql) do |store|
      "UPDATE #{table(store, "inbox")}\n" \
        "SET status = 'dead_lettered', error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = NOW(6), updated_at = NOW(6)\n" \
        "WHERE target_kind = 'workflow' AND target_id = ? AND status IN ('pending', 'failed', 'running')"
    end

    define(:mysql_terminate_workflow_target_activations, backend: :mysql) do |store|
      "DELETE FROM #{table(store, "target_activations")} WHERE target_kind = 'workflow' AND target_id = ?"
    end

    define(:mysql_cancel_step, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET status = 'canceled', error = ?, updated_at = NOW(6)
        WHERE workflow_id = ? AND position = ? AND status IN ('scheduled', 'running', 'waiting')
      SQL
    end

    define(:mysql_fail_step, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET status = 'failed', error = ?, updated_at = NOW(6)
        WHERE workflow_id = ? AND position = ?
      SQL
    end

    define(:mysql_upsert_waiting_step, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, result, started_at, updated_at)
        VALUES (?, ?, ?, 'waiting', ?, NOW(6), NOW(6))
        ON DUPLICATE KEY UPDATE status = 'waiting', result = VALUES(result), error = NULL, updated_at = NOW(6)
      SQL
    end

    define(:mysql_insert_wait, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
      SQL
    end

    define(:mysql_insert_fence, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT IGNORE INTO #{table(store, "fences")} (workflow_id, `key`, status, locked_by, locked_until)
        VALUES (?, ?, 'running', ?, DATE_ADD(NOW(6), INTERVAL ? SECOND))
      SQL
    end

    define(:mysql_claim_expired_fence, backend: :mysql) do |store|
      "UPDATE #{table(store, "fences")}\n" \
        "SET locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), result = NULL, error = NULL, completed_at = NULL\n" \
        "WHERE workflow_id = ? AND `key` = ? AND status = 'running' AND locked_until < NOW(6)"
    end

    define(:mysql_lock_fence_for_worker, backend: :mysql) do |store|
      "SELECT 1 FROM #{table(store, "fences")} WHERE workflow_id = ? AND `key` = ? AND locked_by = ? AND status = 'running'"
    end

    define(:mysql_complete_fence, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "fences")}
        SET status = 'completed', result = ?, error = NULL, completed_at = NOW(6)
        WHERE workflow_id = ? AND `key` = ? AND locked_by = ?
      SQL
    end

    define(:mysql_fail_fence, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "fences")}
        SET status = 'failed', error = ?, completed_at = NOW(6)
        WHERE workflow_id = ? AND `key` = ? AND locked_by = ?
      SQL
    end

    define(:mysql_read_fence, backend: :mysql) do |store|
      "SELECT status, result, error FROM #{table(store, "fences")} WHERE workflow_id = ? AND `key` = ?"
    end

    # worker_pool is routing metadata, not identity — on conflict the clause leaves it at the first
    # writer's value so the routing pool for an object is fixed at creation time.
    define(:mysql_save_object_state, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "durable_objects")} (worker_pool, object_type, object_id, state)
        VALUES (?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE state = VALUES(state), updated_at = NOW(6)
      SQL
    end

    define(:mysql_upsert_object_wakeup, backend: :mysql) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "object_wakeups")} (worker_pool, object_type, object_id, name, wake_at, payload)
        VALUES (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          wake_at = VALUES(wake_at),
          payload = VALUES(payload),
          updated_at = NOW(6)
      SQL
    end

    define(:mysql_delete_object_wakeup, backend: :mysql) do |store|
      "DELETE FROM #{table(store, "object_wakeups")} WHERE worker_pool = ? AND object_type = ? AND object_id = ? AND name = ?"
    end

    define(:mysql_delete_all_object_wakeups, backend: :mysql) do |store|
      "DELETE FROM #{table(store, "object_wakeups")} WHERE worker_pool = ? AND object_type = ? AND object_id = ?"
    end

    define(:mysql_claim_target_activation, backend: :mysql, description: "Probe the unified target activation queue for one claimable candidate in this worker pool.") do |store, filter_sql:|
      <<~SQL.chomp
        SELECT worker_pool, target_kind, target_type, target_id, ready_at, created_at FROM #{table(store, "target_activations")} FORCE INDEX (#{index_name(store, "target_activations", "claim")})
        WHERE worker_pool = ?
          AND queue_available_at <= ?
          #{filter_sql}
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:mysql_claim_selected_target_activation, backend: :mysql, description: "Attach this worker lease to the selected target activation.") do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "target_activations")}
        SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
        WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?
      SQL
    end

    define(:mysql_lock_target_activation_for_completion, backend: :mysql, description: "Lock an owned target activation before reconciliation.") do |store|
      <<~SQL.chomp
        SELECT 1 FROM #{table(store, "target_activations")}
        WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?
          AND status = 'running' AND locked_by = ?
          AND locked_until >= NOW(6)
        FOR UPDATE
      SQL
    end

    define(:mysql_lock_owned_workflow_for_update, backend: :mysql) do |store|
      <<~SQL.chomp
        SELECT 1
        FROM #{table(store, "workflows")}
        WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
        FOR UPDATE
      SQL
    end

    define(:mysql_cancel_pending_waits_for_workflow, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "waits")} FORCE INDEX (#{index_name(store, "waits", "workflow_status")})
        SET status = 'canceled', completed_at = NOW(6)
        WHERE workflow_id = ? AND status = 'pending'
      SQL
    end

    define(:mysql_cancel_waiting_steps_for_workflow, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET status = 'canceled', error = 'workflow cancellation requested', updated_at = NOW(6)
        WHERE workflow_id = ? AND status = 'waiting'
      SQL
    end

    define(:mysql_cancel_waiting_step_attempts_for_workflow, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = 'canceled', error = 'workflow cancellation requested', completed_at = NOW(6)
        WHERE workflow_id = ? AND status = 'waiting'
      SQL
    end

    define(:mysql_cancel_live_steps_for_workflow, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET status = 'canceled', error = 'workflow cancellation requested', updated_at = NOW(6)
        WHERE workflow_id = ? AND status IN ('scheduled', 'running', 'waiting')
      SQL
    end

    define(:mysql_cancel_live_step_attempts_for_workflow, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = 'canceled', error = 'workflow cancellation requested', completed_at = NOW(6)
        WHERE workflow_id = ? AND status IN ('running', 'waiting')
      SQL
    end

    define(:mysql_fail_live_steps_for_workflow, backend: :mysql) do |store|
      # Failing a workflow turns any actively-running step into a 'failed' step
      # that carries the workflow's error. Scheduled and waiting steps stay on
      # the cancel cleanup path — they never observed the error, so 'canceled'
      # is the truer post-condition.
      <<~SQL.chomp
        UPDATE #{table(store, "steps")}
        SET status = 'failed', error = ?, updated_at = NOW(6)
        WHERE workflow_id = ? AND status = 'running'
      SQL
    end

    define(:mysql_fail_live_step_attempts_for_workflow, backend: :mysql) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = 'failed', error = ?, completed_at = NOW(6)
        WHERE workflow_id = ? AND status = 'running'
      SQL
    end

    define(:mysql_lock_inbox_message_for_worker, backend: :mysql) do |store|
      <<~SQL.chomp
        SELECT * FROM #{table(store, "inbox")}
        WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
        FOR UPDATE
      SQL
    end

    define(:mysql_lock_inbox_message, backend: :mysql) do |store|
      "SELECT * FROM #{table(store, "inbox")} WHERE id = ? FOR UPDATE"
    end

    # worker_pool is routing metadata, not identity — on conflict the clause leaves it at the first
    # writer's value so the routing pool for a target activation is fixed at creation time.
    define(:mysql_upsert_target_activation, backend: :mysql, description: "Create or refresh a target activation wakeup.") do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "target_activations")} (worker_pool, target_kind, target_type, target_id, status, ready_at)
        VALUES (?, ?, ?, ?, 'pending', ?)
        ON DUPLICATE KEY UPDATE status = IF(status = 'running', status, 'pending'), ready_at = LEAST(ready_at, VALUES(ready_at)), updated_at = NOW(6)
      SQL
    end

    define(:mysql_delete_target_activation, backend: :mysql, description: "Delete a target activation with no claimable mailbox head.") do |store|
      "DELETE FROM #{table(store, "target_activations")} WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?"
    end

    # Explicit rearm/reconcile writes are scoped to the activation's routing
    # pool; a caller from another pool must not clear an active lease.
    define(:mysql_set_target_activation_pending, backend: :mysql, description: "Return a target activation to pending at the chosen ready time.") do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "target_activations")} (worker_pool, target_kind, target_type, target_id, status, ready_at)
        VALUES (?, ?, ?, ?, 'pending', ?)
        ON DUPLICATE KEY UPDATE
          status = IF(worker_pool = VALUES(worker_pool), 'pending', status),
          ready_at = IF(worker_pool = VALUES(worker_pool), VALUES(ready_at), ready_at),
          locked_by = IF(worker_pool = VALUES(worker_pool), NULL, locked_by),
          locked_until = IF(worker_pool = VALUES(worker_pool), NULL, locked_until),
          updated_at = IF(worker_pool = VALUES(worker_pool), NOW(6), updated_at)
      SQL
    end

    # worker_pool is routing metadata, not identity — INSERT IGNORE on conflict keeps the first
    # writer's value so the routing pool for a mailbox is fixed at creation time.
    define(:mysql_insert_mailbox_sequence, backend: :mysql, description: "Create the mailbox sequence row if it does not exist.") do |store|
      <<~SQL.chomp
        INSERT IGNORE INTO #{table(store, "mailbox_sequences")} (worker_pool, target_kind, target_type, target_id, last_sequence)
        VALUES (?, ?, ?, ?, 0)
      SQL
    end

    define(:mysql_mailbox_sequence_for_update, backend: :mysql, description: "Lock the mailbox sequence row before assigning the next message number.") do |store|
      <<~SQL.chomp
        SELECT worker_pool, last_sequence
        FROM #{table(store, "mailbox_sequences")}
        WHERE target_kind = ? AND target_type = ? AND target_id = ?
        FOR UPDATE
      SQL
    end

    define(:mysql_update_mailbox_sequence, backend: :mysql, description: "Persist the next mailbox sequence number.") do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "mailbox_sequences")}
        SET last_sequence = ?, updated_at = NOW(6)
        WHERE target_kind = ? AND target_type = ? AND target_id = ?
      SQL
    end

    define(:mysql_existing_inbox_message_for_idempotency, backend: :mysql, description: "Find an existing inbox message for an idempotent command.") do |store|
      <<~SQL.chomp
        SELECT id, worker_pool, target_kind, target_type, target_id, status, ready_at, shape_hash
        FROM #{table(store, "inbox")}
        WHERE idempotency_hash = ?
        FOR UPDATE
      SQL
    end

    define(:mysql_lock_workflow_for_update, backend: :mysql, description: "Lock a workflow row before a dependent durable mutation.") do |store|
      "SELECT * FROM #{table(store, "workflows")} WHERE id = ? FOR UPDATE"
    end

    define(:mysql_insert_inbox_message, backend: :mysql, description: "Insert a durable inbox message with serialized payload.") do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "inbox")} (
        id, worker_pool, target_kind, target_type, target_id, sequence, message_kind, method_name,
        operation_id, idempotency_key, idempotency_hash, shape_hash, payload, status, ready_at, max_attempts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
      SQL
    end

    define(:mysql_inbox_claim_rows_for_update, backend: :mysql, description: "Lock claimable inbox messages for a single target mailbox.") do |store, limit:|
      <<~SQL.chomp
        SELECT *
        FROM #{table(store, "inbox")}
        WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?
          AND status IN ('pending', 'failed', 'running', 'dead_lettered')
        ORDER BY sequence
        LIMIT #{Integer(limit)}
        FOR UPDATE
      SQL
    end

    define(:mysql_inbox_head_for_update, backend: :mysql, description: "Lock the current mailbox head for activation reconciliation.") do |store|
      <<~SQL.chomp
        SELECT *
        FROM #{table(store, "inbox")}
        WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?
          AND status IN ('pending', 'failed', 'running', 'dead_lettered')
        ORDER BY sequence
        LIMIT 1
        FOR UPDATE
      SQL
    end

    define(:mysql_mark_inbox_row_running, backend: :mysql, description: "Attach this worker lease to an inbox message.") do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "inbox")}
        SET status = 'running', attempts = attempts + 1, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
        WHERE id = ?
      SQL
    end

    define(:mysql_complete_inbox_message, backend: :mysql, description: "Mark an inbox message completed with serialized result.") do |store|
      "UPDATE #{table(store, "inbox")} SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = NOW(6), updated_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_fail_inbox_message, backend: :mysql, description: "Mark an inbox message failed.") do |store|
      "UPDATE #{table(store, "inbox")} SET status = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'dead_lettered' ELSE 'failed' END, error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN NOW(6) ELSE dead_lettered_at END, updated_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_retry_inbox_message, backend: :mysql, description: "Schedule a failed inbox message for retry.") do |store|
      "UPDATE #{table(store, "inbox")} SET status = 'pending', error = ?, ready_at = ?, locked_by = NULL, locked_until = NULL, updated_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_dead_letter_inbox_message, backend: :mysql, description: "Permanently dead-letter an inbox message.") do |store|
      "UPDATE #{table(store, "inbox")} SET status = 'dead_lettered', error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = NOW(6), updated_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_update_latest_attempt, backend: :mysql, description: "Mirror the latest command outcome onto its latest step attempt.") do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = ?, result = ?, error = ?, completed_at = NOW(6)
        WHERE workflow_id = ? AND position = ? AND status IN ('running', 'waiting')
        ORDER BY started_at DESC
        LIMIT 1
      SQL
    end

    define(:mysql_complete_timer_waits, backend: :mysql, description: "Lock due timer waits in a bounded batch.") do |store, limit:|
      <<~SQL.chomp
        SELECT w.* FROM #{table(store, "waits")} AS w
        JOIN #{table(store, "workflows")} AS wf ON wf.id = w.workflow_id
        WHERE w.status = 'pending'
          AND wf.status IN ('waiting', 'running')
          AND w.kind = 'timer'
          AND w.wake_at <= ?
        ORDER BY w.wake_at, w.created_at
        LIMIT #{Integer(limit)}
        FOR UPDATE OF w SKIP LOCKED
      SQL
    end

    define(:mysql_due_object_wakeups, backend: :mysql, description: "Lock due durable object wakeups in a bounded batch.") do |store, limit:|
      <<~SQL.chomp
        SELECT *
        FROM #{table(store, "object_wakeups")}
        WHERE wake_at <= ?
        ORDER BY wake_at, created_at
        LIMIT #{Integer(limit)}
        FOR UPDATE SKIP LOCKED
      SQL
    end

    define(:mysql_complete_wait, backend: :mysql, description: "Mark a wait completed with serialized wake payload.") do |store|
      "UPDATE #{table(store, "waits")} SET status = 'completed', payload = ?, completed_at = NOW(6) WHERE id = ?"
    end

    define(:mysql_mark_waits_workflows_pending, backend: :mysql, description: "Move workflows with completed waits back to pending.") do |store, placeholders:|
      "UPDATE #{table(store, "workflows")} SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6) WHERE id IN (#{placeholders}) AND status = 'waiting'"
    end

    define(:mysql_lock_workflow_history_workflow, backend: :mysql, description: "Lock the workflow row before appending history.") do |store|
      "SELECT id FROM #{table(store, "workflows")} WHERE id = ? FOR UPDATE"
    end

    define(:mysql_insert_workflow_history, backend: :mysql, description: "Append a workflow replay history event.") do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "workflow_history")} (workflow_id, event_index, kind, command_id, name, attempt_id, payload, error)
        SELECT ?, COALESCE(MAX(event_index), -1) + 1, ?, ?, ?, ?, ?, ?
        FROM #{table(store, "workflow_history")}
        WHERE workflow_id = ?
      SQL
    end

    define(:mysql_inserted_workflow_history_event_index, backend: :mysql, description: "Read the workflow history event index inserted by the previous statement.") do |store|
      "SELECT MAX(event_index) AS event_index FROM #{table(store, "workflow_history")} WHERE workflow_id = ?"
    end

    define(:mysql_workflow_history_for, backend: :mysql, description: "Read workflow history events in replay order.") do |store|
      "SELECT * FROM #{table(store, "workflow_history")} WHERE workflow_id = ? ORDER BY event_index"
    end

    define(:mysql_workflow_history_count_for, backend: :mysql, description: "Count workflow history events for the replay guard.") do |store|
      "SELECT COUNT(*) AS count FROM #{table(store, "workflow_history")} WHERE workflow_id = ?"
    end

    define(:mysql_waits_for_workflow, backend: :mysql, description: "Read wait rows for one workflow.") do |store|
      "SELECT * FROM #{table(store, "waits")} WHERE workflow_id = ? ORDER BY created_at"
    end

    define(:mysql_inbox_message, backend: :mysql, description: "Read one inbox message by id.") do |store|
      "SELECT * FROM #{table(store, "inbox")} WHERE id = ?"
    end

    define(:mysql_inbox_messages_for, backend: :mysql, description: "Read inbox messages for one target mailbox.") do |store|
      <<~SQL.chomp
        SELECT * FROM #{table(store, "inbox")}
        WHERE target_kind = ? AND target_type = ? AND target_id = ?
        ORDER BY sequence
      SQL
    end

    define(:mysql_inbox_messages_for_worker_pool, backend: :mysql, description: "Read inbox messages for one worker-pool target mailbox.") do |store|
      <<~SQL.chomp
        SELECT * FROM #{table(store, "inbox")}
        WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?
        ORDER BY sequence
      SQL
    end

    define(:mysql_target_activation, backend: :mysql, description: "Read a target activation by worker pool and target key.") do |store|
      "SELECT * FROM #{table(store, "target_activations")} WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?"
    end

    # SQLite-specific upsert variants. The SqliteStore resolves every other query
    # by falling back to its :mysql_* sibling and translating the rendered SQL
    # (NOW(6) -> dura_now(), DATE_ADD -> +, FOR UPDATE/FORCE INDEX stripped,
    # INSERT IGNORE -> INSERT OR IGNORE, LEAST/GREATEST -> MIN/MAX). The six
    # upserts below cannot be regex-translated because ON DUPLICATE KEY UPDATE
    # needs an explicit conflict target and VALUES()/IF() rewrites, so they are
    # written directly in SQLite dialect. dura_now() is the per-connection UDF
    # returning the store's integer clock.
    define(:sqlite_upsert_step_running, backend: :sqlite) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, started_at, updated_at)
        VALUES (?, ?, ?, 'running', dura_now(), dura_now())
        ON CONFLICT(workflow_id, position) DO UPDATE SET status = 'running', error = NULL, started_at = COALESCE(started_at, dura_now()), updated_at = dura_now()
      SQL
    end

    define(:sqlite_upsert_waiting_step, backend: :sqlite) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "steps")} (workflow_id, position, name, status, result, started_at, updated_at)
        VALUES (?, ?, ?, 'waiting', ?, dura_now(), dura_now())
        ON CONFLICT(workflow_id, position) DO UPDATE SET status = 'waiting', result = excluded.result, error = NULL, updated_at = dura_now()
      SQL
    end

    define(:sqlite_save_object_state, backend: :sqlite) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "durable_objects")} (worker_pool, object_type, object_id, state)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(object_type, object_id) DO UPDATE SET state = excluded.state, updated_at = dura_now()
      SQL
    end

    define(:sqlite_upsert_object_wakeup, backend: :sqlite) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "object_wakeups")} (worker_pool, object_type, object_id, name, wake_at, payload)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(worker_pool, object_type, object_id, name) DO UPDATE SET wake_at = excluded.wake_at, payload = excluded.payload, updated_at = dura_now()
      SQL
    end

    define(:sqlite_upsert_target_activation, backend: :sqlite) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "target_activations")} (worker_pool, target_kind, target_type, target_id, status, ready_at)
        VALUES (?, ?, ?, ?, 'pending', ?)
        ON CONFLICT(target_kind, target_type, target_id) DO UPDATE SET status = CASE WHEN status = 'running' THEN status ELSE 'pending' END, ready_at = MIN(ready_at, excluded.ready_at), updated_at = dura_now()
      SQL
    end

    define(:sqlite_set_target_activation_pending, backend: :sqlite) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "target_activations")} (worker_pool, target_kind, target_type, target_id, status, ready_at)
        VALUES (?, ?, ?, ?, 'pending', ?)
        ON CONFLICT(target_kind, target_type, target_id) DO UPDATE SET status = 'pending', ready_at = excluded.ready_at, locked_by = NULL, locked_until = NULL, updated_at = dura_now()
        WHERE worker_pool = excluded.worker_pool
      SQL
    end

    # Standard SQLite builds do not support UPDATE ... ORDER BY ... LIMIT, so the
    # two "latest attempt" writers select the target primary key via an ordered
    # subquery and update by id. Param order matches the :mysql_* siblings (the
    # WHERE-clause binds move into the subquery unchanged).
    define(:sqlite_update_latest_attempt, backend: :sqlite) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET status = ?, result = ?, error = ?, completed_at = dura_now()
        WHERE id = (
          SELECT id FROM #{table(store, "step_attempts")}
          WHERE workflow_id = ? AND position = ? AND status IN ('running', 'waiting')
          ORDER BY started_at DESC, id DESC
          LIMIT 1
        )
      SQL
    end

    define(:sqlite_heartbeat_latest_attempt, backend: :sqlite) do |store|
      <<~SQL.chomp
        UPDATE #{table(store, "step_attempts")}
        SET heartbeat_cursor = ?
        WHERE id = (
          SELECT id FROM #{table(store, "step_attempts")}
          WHERE workflow_id = ? AND position = ? AND status = 'running'
          ORDER BY started_at DESC, id DESC
          LIMIT 1
        )
      SQL
    end

    define(:sqlite_insert_workflow_history, backend: :sqlite) do |store|
      <<~SQL.chomp
        INSERT INTO #{table(store, "workflow_history")} (workflow_id, event_index, kind, command_id, name, attempt_id, payload, error)
        SELECT ?, COALESCE(MAX(event_index), -1) + 1, ?, ?, ?, ?, ?, ?
        FROM #{table(store, "workflow_history")}
        WHERE workflow_id = ?
      SQL
    end

    define(:sqlite_inserted_workflow_history_event_index, backend: :sqlite) do |store|
      "SELECT MAX(event_index) AS event_index FROM #{table(store, "workflow_history")} WHERE workflow_id = ?"
    end

    # No :sqlite_claim_object_lease override: the two-step
    # :mysql_ensure_object_row + :mysql_claim_object_lease pair translates
    # cleanly through #translate_to_sqlite (INSERT IGNORE → INSERT OR IGNORE,
    # DATE_ADD(NOW(6), INTERVAL n SECOND) → dura_now() + n * seconds_scale).
    # SQLite needs no CLIENT_FOUND_ROWS workaround — `db.changes` reports the
    # actual matched-and-updated count, so the same UPDATE-WHERE win/loss
    # signal applies.
  end
end
