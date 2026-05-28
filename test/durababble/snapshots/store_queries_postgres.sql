-- pg_ack_outbox
UPDATE "durababble_pg_snapshot"."outbox" SET status = 'processed', processed_at = now() WHERE id = $1 AND locked_by = $2 AND locked_until >= now()

-- pg_cancel_live_step_attempts_for_workflow
UPDATE "durababble_pg_snapshot"."step_attempts"
SET status = 'canceled', error = 'workflow cancellation requested', completed_at = now()
WHERE workflow_id = $1 AND status IN ('running', 'waiting')

-- pg_cancel_live_steps_for_workflow
UPDATE "durababble_pg_snapshot"."steps"
SET status = 'canceled', error = 'workflow cancellation requested', updated_at = now()
WHERE workflow_id = $1 AND status IN ('scheduled', 'running', 'waiting')

-- pg_cancel_pending_waits_for_workflow
UPDATE "durababble_pg_snapshot"."waits"
SET status = 'canceled', completed_at = now()
WHERE workflow_id = $1 AND status = 'pending'

-- pg_cancel_step
UPDATE "durababble_pg_snapshot"."steps" SET status = 'canceled', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2 AND status IN ('scheduled', 'running', 'waiting')

-- pg_cancel_waiting_step_attempts_for_workflow
UPDATE "durababble_pg_snapshot"."step_attempts"
SET status = 'canceled', error = 'workflow cancellation requested', completed_at = now()
WHERE workflow_id = $1 AND status = 'waiting'

-- pg_cancel_waiting_steps_for_workflow
UPDATE "durababble_pg_snapshot"."steps"
SET status = 'canceled', error = 'workflow cancellation requested', updated_at = now()
WHERE workflow_id = $1 AND status = 'waiting'

-- pg_cancel_workflow
UPDATE "durababble_pg_snapshot"."workflows" SET status = 'canceled', result = $2::bytea, error = $3, cancel_reason = COALESCE(cancel_reason, $3), cancel_requested_at = COALESCE(cancel_requested_at, now()), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1 AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))

-- pg_cancel_workflow_with_worker
UPDATE "durababble_pg_snapshot"."workflows" SET status = 'canceled', result = $2::bytea, error = $3, cancel_reason = COALESCE(cancel_reason, $3), cancel_requested_at = COALESCE(cancel_requested_at, now()), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $4 AND locked_until >= now()

-- pg_child_workflow_by_child_id_for_update
SELECT * FROM "durababble_pg_snapshot"."workflows" WHERE id = $1 AND child_origin_kind IS NOT NULL FOR UPDATE

-- pg_child_workflow_rows_for_object
SELECT * FROM "durababble_pg_snapshot"."workflows" WHERE child_origin_kind = 'object' AND parent_object_type = $1 AND parent_object_id = $2 ORDER BY created_at ASC

-- pg_child_workflow_rows_for_parent
SELECT * FROM "durababble_pg_snapshot"."workflows" WHERE child_origin_kind = 'workflow' AND parent_workflow_id = $1 ORDER BY created_at ASC

-- pg_claim_expired_fence
UPDATE "durababble_pg_snapshot"."fences"
SET locked_by = $1, locked_until = now() + ($2::int * interval '1 second'), result = NULL, error = NULL, completed_at = NULL
WHERE workflow_id = $3 AND key = $4 AND status = 'running' AND locked_until < now()

-- pg_claim_expired_outbox
SELECT id, created_at FROM "durababble_pg_snapshot"."outbox"
WHERE status = 'processing' AND locked_until < now()
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED

-- pg_claim_object_lease
INSERT INTO "durababble_pg_snapshot"."durable_objects"
  (worker_pool, object_type, object_id, locked_by, locked_until, created_at, updated_at)
VALUES ($1, $2, $3, $4, now() + ($5::int * interval '1 second'), now(), now())
ON CONFLICT (object_type, object_id) DO UPDATE
SET locked_by = EXCLUDED.locked_by,
    locked_until = EXCLUDED.locked_until,
    updated_at = now()
WHERE "durababble_pg_snapshot"."durable_objects".locked_by IS NULL
   OR "durababble_pg_snapshot"."durable_objects".locked_until < now()
   OR "durababble_pg_snapshot"."durable_objects".locked_by = EXCLUDED.locked_by
RETURNING worker_pool, object_type, object_id, locked_by AS worker_id, locked_until

-- pg_claim_pending_outbox
SELECT id, created_at FROM "durababble_pg_snapshot"."outbox"
WHERE status = 'pending'
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED

-- pg_claim_runnable_workflow
WITH candidate AS (
  SELECT id FROM "durababble_pg_snapshot"."workflows"
  WHERE worker_pool = $1
    AND (CASE
  WHEN status IN ('pending', 'canceling') THEN COALESCE(next_run_at, created_at)
  WHEN status = 'failed' AND next_run_at IS NOT NULL THEN next_run_at
  WHEN status = 'running' AND locked_until IS NOT NULL THEN locked_until
  ELSE NULL
END) <= now()
    <name_filter>
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
UPDATE "durababble_pg_snapshot"."workflows" AS workflows
SET status = 'running', locked_by = $2, locked_until = now() + ($3::int * interval '1 second'), next_run_at = NULL, updated_at = now()
FROM candidate
WHERE workflows.id = candidate.id AND workflows.worker_pool = $1
RETURNING workflows.*

-- pg_claim_selected_outbox
UPDATE "durababble_pg_snapshot"."outbox"
SET status = 'processing', locked_by = $2, locked_until = now() + ($3::int * interval '1 second')
WHERE id = $1
RETURNING *

-- pg_claim_selected_target_activation
UPDATE "durababble_pg_snapshot"."target_activations"
SET status = 'running', locked_by = $5, locked_until = now() + ($6::int * interval '1 second'), updated_at = now()
WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4
RETURNING *

-- pg_claim_target_activation
SELECT worker_pool, target_kind, target_type, target_id, ready_at, created_at FROM "durababble_pg_snapshot"."target_activations"
WHERE worker_pool = $1
  AND (CASE
  WHEN status = 'pending' THEN ready_at
  WHEN status = 'running' AND locked_until IS NOT NULL THEN locked_until
  ELSE NULL
END) <= $2::timestamptz
  <filter_sql>
LIMIT 1
FOR UPDATE SKIP LOCKED

-- pg_claim_workflow_already_owned
SELECT * FROM "durababble_pg_snapshot"."workflows"
WHERE id = $1 AND worker_pool = $2 AND status = 'running' AND locked_by = $3 AND locked_until >= now()

-- pg_claim_workflow_for_activation_update
UPDATE "durababble_pg_snapshot"."workflows"
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

-- pg_claim_workflow_update
UPDATE "durababble_pg_snapshot"."workflows"
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

-- pg_complete_fence
UPDATE "durababble_pg_snapshot"."fences"
SET status = 'completed', result = $4::bytea, error = NULL, completed_at = now()
WHERE workflow_id = $1 AND key = $2 AND locked_by = $3

-- pg_complete_inbox_message
UPDATE "durababble_pg_snapshot"."inbox" SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = now(), updated_at = now() WHERE id = $1

-- pg_complete_step
UPDATE "durababble_pg_snapshot"."steps" SET status = 'completed', result = $3::bytea, error = NULL, completed_at = now(), updated_at = now() WHERE workflow_id = $1 AND position = $2

-- pg_complete_timer_waits
UPDATE "durababble_pg_snapshot"."waits"
SET status = 'completed', payload = $2::bytea, completed_at = now()
WHERE id IN (
  SELECT w.id FROM "durababble_pg_snapshot"."waits" AS w
  JOIN "durababble_pg_snapshot"."workflows" AS wf ON wf.id = w.workflow_id
  WHERE w.status = 'pending'
    AND wf.status IN ('waiting', 'running')
    AND w.kind = 'timer'
    AND w.wake_at <= $1::timestamptz
  ORDER BY w.wake_at, w.created_at
  LIMIT $3
  FOR UPDATE OF w SKIP LOCKED
)
RETURNING *

-- pg_complete_workflow
UPDATE "durababble_pg_snapshot"."workflows" SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1 AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL)) AND NOT EXISTS (SELECT 1 FROM "durababble_pg_snapshot"."steps" WHERE workflow_id = $1 AND status IN ('scheduled', 'running', 'waiting')) AND NOT EXISTS (SELECT 1 FROM "durababble_pg_snapshot"."step_attempts" WHERE workflow_id = $1 AND status IN ('running', 'waiting')) AND NOT EXISTS (SELECT 1 FROM "durababble_pg_snapshot"."waits" WHERE workflow_id = $1 AND status = 'pending')

-- pg_complete_workflow_with_worker
UPDATE "durababble_pg_snapshot"."workflows" SET status = 'completed', result = $2::bytea, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $3 AND locked_until >= now()

-- pg_current_object_lease
SELECT worker_pool, object_type, object_id, locked_by AS worker_id, locked_until
FROM "durababble_pg_snapshot"."durable_objects"
WHERE object_type = $1 AND object_id = $2
  AND locked_by IS NOT NULL AND locked_until >= now()
LIMIT 1

-- pg_current_workflow_lease
SELECT id AS workflow_id, worker_pool, locked_by AS worker_id, locked_until
FROM "durababble_pg_snapshot"."workflows"
WHERE id = $1 AND status = 'running' AND locked_by IS NOT NULL AND locked_until >= now()

-- pg_dead_letter_inbox_message
UPDATE "durababble_pg_snapshot"."inbox"
SET status = 'dead_lettered', error = $2, locked_by = NULL, locked_until = NULL, dead_lettered_at = now(), updated_at = now()
WHERE id = $1

-- pg_delete_all_object_wakeups
DELETE FROM "durababble_pg_snapshot"."object_wakeups" WHERE worker_pool = $1 AND object_type = $2 AND object_id = $3

-- pg_delete_object_wakeup
DELETE FROM "durababble_pg_snapshot"."object_wakeups" WHERE worker_pool = $1 AND object_type = $2 AND object_id = $3 AND name = $4

-- pg_delete_target_activation
DELETE FROM "durababble_pg_snapshot"."target_activations" WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4

-- pg_drop_schema
DROP SCHEMA IF EXISTS "durababble_pg_snapshot" CASCADE

-- pg_due_object_wakeups
SELECT *
FROM "durababble_pg_snapshot"."object_wakeups"
WHERE wake_at <= $1::timestamptz
ORDER BY wake_at, created_at
LIMIT $2
FOR UPDATE SKIP LOCKED

-- pg_existing_inbox_message_for_idempotency
SELECT id, worker_pool, target_kind, target_type, target_id, status, ready_at, shape_hash
FROM "durababble_pg_snapshot"."inbox"
WHERE idempotency_hash = $1
FOR UPDATE

-- pg_fail_fence
UPDATE "durababble_pg_snapshot"."fences"
SET status = 'failed', error = $4, completed_at = now()
WHERE workflow_id = $1 AND key = $2 AND locked_by = $3

-- pg_fail_inbox_message
UPDATE "durababble_pg_snapshot"."inbox"
SET status = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'dead_lettered' ELSE 'failed' END,
  error = $2, locked_by = NULL, locked_until = NULL,
  dead_lettered_at = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN now() ELSE dead_lettered_at END,
  updated_at = now()
WHERE id = $1

-- pg_fail_live_step_attempts_for_workflow
UPDATE "durababble_pg_snapshot"."step_attempts"
SET status = 'failed', error = $2, completed_at = now()
WHERE workflow_id = $1 AND status = 'running'

-- pg_fail_live_steps_for_workflow
UPDATE "durababble_pg_snapshot"."steps"
SET status = 'failed', error = $2, updated_at = now()
WHERE workflow_id = $1 AND status = 'running'

-- pg_fail_step
UPDATE "durababble_pg_snapshot"."steps" SET status = 'failed', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2

-- pg_fail_workflow
UPDATE "durababble_pg_snapshot"."workflows" SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1 AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))

-- pg_fail_workflow_with_worker
UPDATE "durababble_pg_snapshot"."workflows" SET status = 'failed', error = $2, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now() WHERE id = $1 AND status = 'running' AND locked_by = $3 AND locked_until >= now()

-- pg_heartbeat_latest_attempt
UPDATE "durababble_pg_snapshot"."step_attempts"
SET heartbeat_cursor = $3::bytea
WHERE id = (
  SELECT id FROM "durababble_pg_snapshot"."step_attempts"
  WHERE workflow_id = $1 AND position = $2 AND status = 'running'
  ORDER BY started_at DESC
  LIMIT 1
)

-- pg_heartbeat_step_row
UPDATE "durababble_pg_snapshot"."steps"
SET heartbeat_cursor = $3::bytea, updated_at = now()
WHERE workflow_id = $1 AND position = $2 AND status = 'running'
RETURNING heartbeat_cursor

-- pg_heartbeat_step_workflow
UPDATE "durababble_pg_snapshot"."workflows"
SET locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()
RETURNING locked_until

-- pg_heartbeat_workflow
UPDATE "durababble_pg_snapshot"."workflows"
SET locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()

-- pg_inbox_claim_rows_for_update
SELECT *
FROM "durababble_pg_snapshot"."inbox"
WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4
  AND status IN ('pending', 'failed', 'running', 'dead_lettered')
ORDER BY sequence
LIMIT $5
FOR UPDATE

-- pg_inbox_head_for_update
SELECT *
FROM "durababble_pg_snapshot"."inbox"
WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4
  AND status IN ('pending', 'failed', 'running', 'dead_lettered')
ORDER BY sequence
LIMIT 1
FOR UPDATE

-- pg_inbox_message
SELECT * FROM "durababble_pg_snapshot"."inbox" WHERE id = $1

-- pg_inbox_messages_for
SELECT * FROM "durababble_pg_snapshot"."inbox"
WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
ORDER BY sequence

-- pg_inbox_messages_for_worker_pool
SELECT * FROM "durababble_pg_snapshot"."inbox"
WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4
ORDER BY sequence

-- pg_insert_child_workflow
INSERT INTO "durababble_pg_snapshot"."workflows" (
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

-- pg_insert_fence
INSERT INTO "durababble_pg_snapshot"."fences" (workflow_id, key, status, locked_by, locked_until)
VALUES ($1, $2, 'running', $3, now() + ($4::int * interval '1 second'))
ON CONFLICT (workflow_id, key) DO NOTHING

-- pg_insert_inbox_message
INSERT INTO "durababble_pg_snapshot"."inbox" (
id, worker_pool, target_kind, target_type, target_id, sequence, message_kind, method_name,
operation_id, idempotency_key, idempotency_hash, shape_hash, payload, status, ready_at, max_attempts
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::bytea, 'pending', $14::timestamptz, $15)

-- pg_insert_mailbox_sequence
INSERT INTO "durababble_pg_snapshot"."mailbox_sequences" (worker_pool, target_kind, target_type, target_id, last_sequence)
VALUES ($1, $2, $3, $4, 0)
ON CONFLICT (target_kind, target_type, target_id) DO NOTHING

-- pg_insert_outbox
INSERT INTO "durababble_pg_snapshot"."outbox" (id, workflow_id, topic, payload, key, status)
VALUES ($1, $2, $3, $4::bytea, $5, 'pending')
ON CONFLICT (key) DO NOTHING

-- pg_insert_scheduled_step
INSERT INTO "durababble_pg_snapshot"."steps" (workflow_id, position, name, status, updated_at)
VALUES ($1, $2, $3, 'scheduled', now())
ON CONFLICT (workflow_id, position) DO NOTHING

-- pg_insert_step_attempt
INSERT INTO "durababble_pg_snapshot"."step_attempts" (id, workflow_id, position, name, status)
VALUES ($1, $2, $3, $4, 'running')

-- pg_insert_wait
INSERT INTO "durababble_pg_snapshot"."waits" (id, workflow_id, position, kind, event_key, wake_at, context, status)
VALUES ($1, $2, $3, $4, $5, $6::timestamptz, $7::bytea, 'pending')

-- pg_insert_workflow
INSERT INTO "durababble_pg_snapshot"."workflows" (id, name, worker_pool, status, input) VALUES ($1, $2, $3, $4, $5::bytea)

-- pg_insert_workflow_history
INSERT INTO "durababble_pg_snapshot"."workflow_history" (workflow_id, event_index, kind, command_id, name, attempt_id, payload, error)
VALUES ($1, $2, $3, $4, $5, $6, $7::bytea, $8)

-- pg_insert_workflow_with_worker
INSERT INTO "durababble_pg_snapshot"."workflows" (id, name, worker_pool, status, input, locked_by, locked_until) VALUES ($1, $2, $3, $4, $5::bytea, $6, now() + ($7::int * interval '1 second'))

-- pg_lock_inbox_message
SELECT * FROM "durababble_pg_snapshot"."inbox" WHERE id = $1 FOR UPDATE

-- pg_lock_inbox_message_for_worker
SELECT * FROM "durababble_pg_snapshot"."inbox"
WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
FOR UPDATE

-- pg_lock_owned_object_for_update
SELECT 1
FROM "durababble_pg_snapshot"."durable_objects"
WHERE object_type = $1 AND object_id = $2
  AND locked_by = $3 AND locked_until >= now()
FOR UPDATE

-- pg_lock_owned_workflow_for_update
SELECT 1
FROM "durababble_pg_snapshot"."workflows"
WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()
FOR UPDATE

-- pg_lock_target_activation_for_completion
SELECT 1 FROM "durababble_pg_snapshot"."target_activations"
WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4
  AND status = 'running' AND locked_by = $5
  AND locked_until >= now()
FOR UPDATE

-- pg_lock_workflow_for_termination
SELECT * FROM "durababble_pg_snapshot"."workflows" WHERE id = $1 FOR UPDATE

-- pg_lock_workflow_for_update
SELECT * FROM "durababble_pg_snapshot"."workflows" WHERE id = $1 FOR UPDATE

-- pg_lock_workflow_history_workflow
SELECT id FROM "durababble_pg_snapshot"."workflows" WHERE id = $1 FOR UPDATE

-- pg_mailbox_sequence_for_update
SELECT worker_pool, last_sequence
FROM "durababble_pg_snapshot"."mailbox_sequences"
WHERE target_kind = $1 AND target_type = $2 AND target_id = $3
FOR UPDATE

-- pg_make_workflow_due
UPDATE "durababble_pg_snapshot"."workflows" SET next_run_at = NULL, updated_at = $2::timestamptz WHERE id = $1

-- pg_mark_inbox_row_running
UPDATE "durababble_pg_snapshot"."inbox"
SET status = 'running', attempts = attempts + 1, locked_by = $2, locked_until = now() + ($3::int * interval '1 second'), updated_at = now()
WHERE id = $1

-- pg_mark_waits_workflows_pending
UPDATE "durababble_pg_snapshot"."workflows" SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now() WHERE id IN (<placeholders>) AND status = 'waiting'

-- pg_mark_workflow_canceling_for_request
UPDATE "durababble_pg_snapshot"."workflows"
SET status = 'canceling', locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now()
WHERE id = $1 AND status NOT IN ('completed', 'canceled')

-- pg_mark_workflow_cancellation_delivered
UPDATE "durababble_pg_snapshot"."workflows"
SET cancel_delivered_at = COALESCE(cancel_delivered_at, now()), updated_at = now()
WHERE id = $1 AND cancel_requested_at IS NOT NULL

-- pg_mark_workflow_running
UPDATE "durababble_pg_snapshot"."workflows"
SET status = 'running', error = NULL, next_run_at = NULL, updated_at = now()
WHERE id = $1 AND worker_pool = $2 AND status = 'pending' AND locked_by IS NULL

-- pg_mark_workflow_running_with_worker
UPDATE "durababble_pg_snapshot"."workflows"
SET status = 'running', error = NULL, locked_by = $1,
    locked_until = now() + ($2::int * interval '1 second'), next_run_at = NULL, updated_at = now()
WHERE id = $3 AND worker_pool = $4
  AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))

-- pg_next_workflow_history_event_index
SELECT COALESCE(MAX(event_index), -1) + 1 AS event_index FROM "durababble_pg_snapshot"."workflow_history" WHERE workflow_id = $1

-- pg_object_state
SELECT state FROM "durababble_pg_snapshot"."durable_objects" WHERE object_type = $1 AND object_id = $2 AND state IS NOT NULL

-- pg_outbox_by_key
SELECT id FROM "durababble_pg_snapshot"."outbox" WHERE key = $1

-- pg_outbox_message
SELECT * FROM "durababble_pg_snapshot"."outbox" WHERE id = $1

-- pg_read_fence
SELECT status, result, error FROM "durababble_pg_snapshot"."fences" WHERE workflow_id = $1 AND key = $2

-- pg_release_inbox_leases
UPDATE "durababble_pg_snapshot"."inbox"
SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now()
WHERE status = 'running' AND locked_by = $1

-- pg_release_object_lease
UPDATE "durababble_pg_snapshot"."durable_objects"
SET locked_by = NULL, locked_until = NULL, updated_at = now()
WHERE object_type = $1 AND object_id = $2 AND locked_by = $3

-- pg_release_outbox_leases
UPDATE "durababble_pg_snapshot"."outbox"
SET status = 'pending', locked_by = NULL, locked_until = NULL
WHERE status = 'processing' AND locked_by = $1

-- pg_release_target_activation_leases
UPDATE "durababble_pg_snapshot"."target_activations"
SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now()
WHERE status = 'running' AND locked_by = $1

-- pg_release_worker_object_leases
UPDATE "durababble_pg_snapshot"."durable_objects"
SET locked_by = NULL, locked_until = NULL, updated_at = now()
WHERE locked_by = $1

-- pg_release_workflow_leases
UPDATE "durababble_pg_snapshot"."workflows"
SET status = CASE
    WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
    ELSE 'pending'
  END,
  locked_by = NULL, locked_until = NULL, updated_at = now()
WHERE status = 'running' AND locked_by = $1

-- pg_renew_object_lease
UPDATE "durababble_pg_snapshot"."durable_objects"
SET locked_until = now() + ($4::int * interval '1 second'), updated_at = now()
WHERE object_type = $1 AND object_id = $2
  AND locked_by = $3 AND locked_until >= now()
RETURNING worker_pool, object_type, object_id, locked_by AS worker_id, locked_until

-- pg_request_workflow_cancellation
UPDATE "durababble_pg_snapshot"."workflows"
SET cancel_reason = $2, cancel_requested_at = now(), updated_at = now()
WHERE id = $1

-- pg_retry_inbox_message
UPDATE "durababble_pg_snapshot"."inbox"
SET status = 'pending', error = $2, ready_at = $3::timestamptz, locked_by = NULL, locked_until = NULL, updated_at = now()
WHERE id = $1

-- pg_save_object_state
INSERT INTO "durababble_pg_snapshot"."durable_objects" (worker_pool, object_type, object_id, state)
VALUES ($1, $2, $3, $4::bytea)
ON CONFLICT (object_type, object_id) DO UPDATE
  SET state = $4::bytea, updated_at = now()

-- pg_schedule_workflow_retry
UPDATE "durababble_pg_snapshot"."workflows"
SET status = CASE
    WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
    ELSE 'pending'
  END,
  locked_by = NULL, locked_until = NULL, next_run_at = $3::timestamptz, updated_at = now()
WHERE id = $1 AND status = 'running' AND locked_by = $2 AND locked_until >= now()

-- pg_set_target_activation_pending
INSERT INTO "durababble_pg_snapshot"."target_activations" (worker_pool, target_kind, target_type, target_id, status, ready_at)
VALUES ($1, $2, $3, $4, 'pending', $5::timestamptz)
ON CONFLICT (target_kind, target_type, target_id) DO UPDATE
  SET status = 'pending', ready_at = EXCLUDED.ready_at, locked_by = NULL, locked_until = NULL, updated_at = now()
  WHERE "durababble_pg_snapshot"."target_activations".worker_pool = EXCLUDED.worker_pool

-- pg_steal_expired_leases
UPDATE "durababble_pg_snapshot"."workflows"
SET status = CASE
    WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
    ELSE 'pending'
  END,
  locked_by = NULL, locked_until = NULL, updated_at = now()
WHERE status = 'running' AND locked_until < $1::timestamptz

-- pg_steal_expired_object_leases
UPDATE "durababble_pg_snapshot"."durable_objects"
SET locked_by = NULL, locked_until = NULL, updated_at = now()
WHERE locked_by IS NOT NULL AND locked_until < $1::timestamptz

-- pg_step_attempt_count_for
SELECT COUNT(*) AS count FROM "durababble_pg_snapshot"."step_attempts" WHERE workflow_id = $1 AND position = $2

-- pg_step_attempts_for
SELECT * FROM "durababble_pg_snapshot"."step_attempts" WHERE workflow_id = $1 ORDER BY started_at, position

-- pg_step_heartbeat_cursor
SELECT heartbeat_cursor FROM "durababble_pg_snapshot"."steps" WHERE workflow_id = $1 AND position = $2

-- pg_steps_for
SELECT * FROM "durababble_pg_snapshot"."steps" WHERE workflow_id = $1 ORDER BY position

-- pg_supersede_running_step_attempts
UPDATE "durababble_pg_snapshot"."step_attempts"
SET status = 'failed', error = 'superseded by retry', completed_at = now()
WHERE workflow_id = $1 AND position = $2 AND status = 'running'

-- pg_suspend_workflow
UPDATE "durababble_pg_snapshot"."workflows"
SET status = CASE
    WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
    WHEN EXISTS (SELECT 1 FROM "durababble_pg_snapshot"."waits" WHERE workflow_id = $1 AND status = 'pending') THEN 'waiting'
    ELSE 'pending'
  END,
  locked_by = NULL, locked_until = NULL, updated_at = now()
WHERE id = $1 AND status = 'running'
  AND ($2::text IS NULL OR (locked_by = $2::text AND locked_until >= now()))

-- pg_target_activation
SELECT * FROM "durababble_pg_snapshot"."target_activations" WHERE worker_pool = $1 AND target_kind = $2 AND target_type = $3 AND target_id = $4

-- pg_terminate_workflow
UPDATE "durababble_pg_snapshot"."workflows"
SET status = 'terminated', result = $2::bytea, error = $3,
  locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = now()
WHERE id = $1

-- pg_terminate_workflow_inbox
UPDATE "durababble_pg_snapshot"."inbox"
SET status = 'dead_lettered', error = $2, locked_by = NULL, locked_until = NULL, dead_lettered_at = now(), updated_at = now()
WHERE target_kind = 'workflow' AND target_id = $1 AND status IN ('pending', 'failed', 'running')

-- pg_terminate_workflow_step_attempts
UPDATE "durababble_pg_snapshot"."step_attempts" SET status = 'canceled', error = $2, completed_at = now() WHERE workflow_id = $1 AND status IN ('running', 'waiting')

-- pg_terminate_workflow_steps
UPDATE "durababble_pg_snapshot"."steps" SET status = 'canceled', error = $2, updated_at = now() WHERE workflow_id = $1 AND status IN ('scheduled', 'running', 'waiting')

-- pg_terminate_workflow_target_activations
DELETE FROM "durababble_pg_snapshot"."target_activations" WHERE target_kind = 'workflow' AND target_id = $1

-- pg_terminate_workflow_waits
UPDATE "durababble_pg_snapshot"."waits" SET status = 'canceled', completed_at = now() WHERE workflow_id = $1 AND status = 'pending'

-- pg_update_latest_attempt
UPDATE "durababble_pg_snapshot"."step_attempts"
SET status = $3, result = $4::bytea, error = $5, completed_at = now()
WHERE id = (
  SELECT id FROM "durababble_pg_snapshot"."step_attempts"
  WHERE workflow_id = $1 AND position = $2 AND status IN ('running', 'waiting')
  ORDER BY started_at DESC
  LIMIT 1
)

-- pg_update_mailbox_sequence
UPDATE "durababble_pg_snapshot"."mailbox_sequences"
SET last_sequence = $1, updated_at = now()
WHERE target_kind = $2 AND target_type = $3 AND target_id = $4

-- pg_upsert_object_wakeup
INSERT INTO "durababble_pg_snapshot"."object_wakeups" (worker_pool, object_type, object_id, name, wake_at, payload)
VALUES ($1, $2, $3, $4, $5::timestamptz, $6::bytea)
ON CONFLICT (worker_pool, object_type, object_id, name) DO UPDATE
  SET wake_at = EXCLUDED.wake_at,
      payload = EXCLUDED.payload,
      updated_at = now()

-- pg_upsert_step_running
INSERT INTO "durababble_pg_snapshot"."steps" (workflow_id, position, name, status, started_at, updated_at)
VALUES ($1, $2, $3, 'running', now(), now())
ON CONFLICT (workflow_id, position) DO UPDATE
  SET status = 'running', error = NULL, started_at = COALESCE("durababble_pg_snapshot"."steps".started_at, now()), updated_at = now()

-- pg_upsert_target_activation
INSERT INTO "durababble_pg_snapshot"."target_activations" (worker_pool, target_kind, target_type, target_id, status, ready_at)
VALUES ($1, $2, $3, $4, 'pending', $5::timestamptz)
ON CONFLICT (target_kind, target_type, target_id) DO UPDATE
  SET status = CASE WHEN "durababble_pg_snapshot"."target_activations".status = 'running' THEN "durababble_pg_snapshot"."target_activations".status ELSE 'pending' END,
  ready_at = LEAST("durababble_pg_snapshot"."target_activations".ready_at, EXCLUDED.ready_at), updated_at = now()

-- pg_upsert_waiting_step
INSERT INTO "durababble_pg_snapshot"."steps" (workflow_id, position, name, status, result, started_at, updated_at)
VALUES ($1, $2, $3, 'waiting', $4::bytea, now(), now())
ON CONFLICT (workflow_id, position) DO UPDATE
  SET status = 'waiting', result = $4::bytea, error = NULL, updated_at = now()

-- pg_waits_for_workflow
SELECT * FROM "durababble_pg_snapshot"."waits" WHERE workflow_id = $1 ORDER BY created_at

-- pg_workflow
SELECT * FROM "durababble_pg_snapshot"."workflows" WHERE id = $1

-- pg_workflow_cancellation
SELECT id AS workflow_id, cancel_reason AS reason,
cancel_requested_at AS requested_at, cancel_delivered_at AS delivered_at
FROM "durababble_pg_snapshot"."workflows"
WHERE id = $1 AND cancel_requested_at IS NOT NULL

-- pg_workflow_history_count_for
SELECT COUNT(*) AS count FROM "durababble_pg_snapshot"."workflow_history" WHERE workflow_id = $1

-- pg_workflow_history_for
SELECT * FROM "durababble_pg_snapshot"."workflow_history" WHERE workflow_id = $1 ORDER BY event_index

-- pg_workflow_owned
SELECT 1
FROM "durababble_pg_snapshot"."workflows"
WHERE id = $1 AND locked_by = $2 AND status = 'running' AND locked_until >= now()
