-- mysql_ack_outbox
UPDATE `durababble_mysql_snapshot_outbox` SET status = 'processed', processed_at = NOW(6) WHERE id = ? AND locked_by = ? AND locked_until >= NOW(6)

-- mysql_cancel_live_step_attempts_for_workflow
UPDATE `durababble_mysql_snapshot_step_attempts`
SET status = 'canceled', error = 'workflow cancellation requested', completed_at = NOW(6)
WHERE workflow_id = ? AND status IN ('running', 'waiting')

-- mysql_cancel_live_steps_for_workflow
UPDATE `durababble_mysql_snapshot_steps`
SET status = 'canceled', error = 'workflow cancellation requested', updated_at = NOW(6)
WHERE workflow_id = ? AND status IN ('scheduled', 'running', 'waiting')

-- mysql_cancel_pending_waits_for_workflow
UPDATE `durababble_mysql_snapshot_waits`
SET status = 'canceled', completed_at = NOW(6)
WHERE workflow_id = ? AND status = 'pending'

-- mysql_cancel_step
UPDATE `durababble_mysql_snapshot_steps`
SET status = 'canceled', error = ?, updated_at = NOW(6)
WHERE workflow_id = ? AND position = ? AND status IN ('scheduled', 'running', 'waiting')

-- mysql_cancel_waiting_step_attempts_for_workflow
UPDATE `durababble_mysql_snapshot_step_attempts`
SET status = 'canceled', error = 'workflow cancellation requested', completed_at = NOW(6)
WHERE workflow_id = ? AND status = 'waiting'

-- mysql_cancel_waiting_steps_for_workflow
UPDATE `durababble_mysql_snapshot_steps`
SET status = 'canceled', error = 'workflow cancellation requested', updated_at = NOW(6)
WHERE workflow_id = ? AND status = 'waiting'

-- mysql_cancel_workflow
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'canceled', result = ?, error = ?, cancel_reason = COALESCE(cancel_reason, ?),
  cancel_requested_at = COALESCE(cancel_requested_at, NOW(6)), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
WHERE id = ? AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))

-- mysql_cancel_workflow_with_worker
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'canceled', result = ?, error = ?, cancel_reason = COALESCE(cancel_reason, ?),
  cancel_requested_at = COALESCE(cancel_requested_at, NOW(6)), locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)

-- mysql_claim_canceling_workflow
SELECT id, created_at FROM `durababble_mysql_snapshot_workflows`
WHERE worker_pool = ?
  AND status = 'canceling'
  AND (next_run_at IS NULL OR next_run_at <= NOW(6))
  <name_sql>
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED

-- mysql_claim_expired_fence
UPDATE `durababble_mysql_snapshot_fences`
SET locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), result = NULL, error = NULL, completed_at = NULL
WHERE workflow_id = ? AND `key` = ? AND status = 'running' AND locked_until < NOW(6)

-- mysql_claim_expired_outbox
SELECT id, created_at FROM `durababble_mysql_snapshot_outbox` FORCE INDEX (durababble_mysql_snapshot_outbox_expired_lease_idx)
WHERE status = 'processing' AND locked_until < NOW(6)
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED

-- mysql_claim_expired_target_activation
SELECT worker_pool, target_kind, target_type, target_id, ready_at, created_at FROM `durababble_mysql_snapshot_target_activations` FORCE INDEX (durababble_mysql_snapshot_target_activations_expired_idx)
WHERE worker_pool = ? AND status = 'running' AND locked_until < ?
  <filter_sql>
ORDER BY ready_at, created_at
LIMIT 1
FOR UPDATE SKIP LOCKED

-- mysql_claim_expired_workflow
SELECT id, created_at FROM `durababble_mysql_snapshot_workflows` FORCE INDEX (durababble_mysql_snapshot_workflows_expired_lease_idx)
WHERE worker_pool = ?
  AND status = 'running' AND locked_until < NOW(6)
  <name_sql>
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED

-- mysql_claim_failed_workflow
SELECT id, created_at FROM `durababble_mysql_snapshot_workflows`
WHERE worker_pool = ?
  AND status = 'failed'
  AND next_run_at IS NOT NULL
  AND next_run_at <= NOW(6)
  <name_sql>
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED

-- mysql_claim_pending_outbox
SELECT id, created_at FROM `durababble_mysql_snapshot_outbox`
WHERE status = 'pending'
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED

-- mysql_claim_pending_target_activation
SELECT worker_pool, target_kind, target_type, target_id, ready_at, created_at FROM `durababble_mysql_snapshot_target_activations`
WHERE worker_pool = ? AND status = 'pending' AND ready_at <= ?
  <filter_sql>
ORDER BY ready_at, created_at
LIMIT 1
FOR UPDATE SKIP LOCKED

-- mysql_claim_pending_workflow
SELECT id, created_at FROM `durababble_mysql_snapshot_workflows`
WHERE worker_pool = ?
  AND status = 'pending'
  AND (next_run_at IS NULL OR next_run_at <= NOW(6))
  <name_sql>
ORDER BY created_at
LIMIT 1
FOR UPDATE SKIP LOCKED

-- mysql_claim_selected_outbox
UPDATE `durababble_mysql_snapshot_outbox`
SET status = 'processing', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND)
WHERE id = ?

-- mysql_claim_selected_target_activation
UPDATE `durababble_mysql_snapshot_target_activations`
SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?

-- mysql_claim_selected_workflow
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), next_run_at = NULL, updated_at = NOW(6)
WHERE id = ? AND worker_pool = ?
  AND (
    status = 'pending'
    OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))
    OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
    OR (status = 'running' AND locked_until < NOW(6))
  )

-- mysql_claim_workflow_already_owned
SELECT * FROM `durababble_mysql_snapshot_workflows`
WHERE id = ? AND worker_pool = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)

-- mysql_claim_workflow_for_activation_lock
SELECT id FROM `durababble_mysql_snapshot_workflows`
WHERE id = ? AND worker_pool = ?
  AND (
    (status = 'pending' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
    OR status = 'waiting'
    OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
    OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))
    OR (status = 'running' AND (locked_by = ? OR locked_until < NOW(6)))
  )
FOR UPDATE SKIP LOCKED

-- mysql_claim_workflow_for_activation_update
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'running', error = NULL, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
WHERE id = ? AND worker_pool = ?

-- mysql_claim_workflow_lock
SELECT id FROM `durababble_mysql_snapshot_workflows`
WHERE id = ? AND worker_pool = ?
  AND (
    (status = 'pending' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
    OR (status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6))
    OR (status = 'canceling' AND (next_run_at IS NULL OR next_run_at <= NOW(6)))
    OR (status = 'running' AND (locked_by = ? OR locked_until < NOW(6)))
  )
FOR UPDATE SKIP LOCKED

-- mysql_claim_workflow_update
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'running', error = NULL, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), next_run_at = NULL, updated_at = NOW(6)
WHERE id = ? AND worker_pool = ?

-- mysql_complete_fence
UPDATE `durababble_mysql_snapshot_fences`
SET status = 'completed', result = ?, error = NULL, completed_at = NOW(6)
WHERE workflow_id = ? AND `key` = ? AND locked_by = ?

-- mysql_complete_inbox_message
UPDATE `durababble_mysql_snapshot_inbox` SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, completed_at = NOW(6), updated_at = NOW(6) WHERE id = ?

-- mysql_complete_step
UPDATE `durababble_mysql_snapshot_steps`
SET status = 'completed', result = ?, error = NULL, completed_at = NOW(6), updated_at = NOW(6)
WHERE workflow_id = ? AND position = ?

-- mysql_complete_timer_waits
SELECT w.* FROM `durababble_mysql_snapshot_waits` AS w
JOIN `durababble_mysql_snapshot_workflows` AS wf ON wf.id = w.workflow_id
WHERE w.status = 'pending'
  AND wf.status IN ('waiting', 'running')
  AND w.kind = 'timer'
  AND w.wake_at <= ?
ORDER BY w.wake_at, w.created_at
LIMIT 100
FOR UPDATE OF w SKIP LOCKED

-- mysql_complete_wait
UPDATE `durababble_mysql_snapshot_waits` SET status = 'completed', payload = ?, completed_at = NOW(6) WHERE id = ?

-- mysql_complete_workflow
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
WHERE id = ?
  AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))
  AND NOT EXISTS (SELECT 1 FROM `durababble_mysql_snapshot_steps` WHERE workflow_id = ? AND status IN ('scheduled', 'running', 'waiting'))
  AND NOT EXISTS (SELECT 1 FROM `durababble_mysql_snapshot_step_attempts` WHERE workflow_id = ? AND status IN ('running', 'waiting'))
  AND NOT EXISTS (SELECT 1 FROM `durababble_mysql_snapshot_waits` WHERE workflow_id = ? AND status = 'pending')

-- mysql_complete_workflow_with_worker
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'completed', result = ?, error = NULL, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)

-- mysql_count_expired_workflow_leases
SELECT COUNT(*) AS count FROM `durababble_mysql_snapshot_workflows` WHERE status = 'running' AND locked_until < ?

-- mysql_count_inbox_leases
SELECT COUNT(*) AS count FROM `durababble_mysql_snapshot_inbox` FORCE INDEX (<index>) WHERE status = 'running' AND locked_by = ?

-- mysql_count_outbox_leases
SELECT COUNT(*) AS count FROM `durababble_mysql_snapshot_outbox` FORCE INDEX (<index>) WHERE status = 'processing' AND locked_by = ?

-- mysql_count_target_activation_leases
SELECT COUNT(*) AS count FROM `durababble_mysql_snapshot_target_activations` FORCE INDEX (<index>) WHERE status = 'running' AND locked_by = ?

-- mysql_count_workflow_leases
SELECT COUNT(*) AS count FROM `durababble_mysql_snapshot_workflows` FORCE INDEX (<index>) WHERE status = 'running' AND locked_by = ?

-- mysql_current_object_lease
SELECT worker_pool, object_id, worker_id, locked_until
FROM (
  SELECT worker_pool, target_id AS object_id, locked_by AS worker_id, locked_until, 0 AS source_priority, CAST(NULL AS SIGNED) AS inbox_sequence
  FROM `durababble_mysql_snapshot_target_activations`
  WHERE target_kind = 'object' AND target_type = ? AND target_id = ? AND status = 'running'
    AND locked_by IS NOT NULL AND locked_until >= NOW(6)
  UNION ALL
  SELECT worker_pool, target_id AS object_id, locked_by AS worker_id, locked_until, 1 AS source_priority, sequence AS inbox_sequence
  FROM `durababble_mysql_snapshot_inbox`
  WHERE target_kind = 'object' AND target_type = ? AND target_id = ? AND status = 'running'
    AND locked_by IS NOT NULL AND locked_until >= NOW(6)
) AS leases
ORDER BY source_priority, inbox_sequence
LIMIT 1

-- mysql_current_workflow_lease
SELECT id AS workflow_id, worker_pool, locked_by AS worker_id, locked_until
FROM `durababble_mysql_snapshot_workflows`
WHERE id = ? AND status = 'running' AND locked_by IS NOT NULL AND locked_until >= NOW(6)

-- mysql_dead_letter_inbox_message
UPDATE `durababble_mysql_snapshot_inbox` SET status = 'dead_lettered', error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = NOW(6), updated_at = NOW(6) WHERE id = ?

-- mysql_delete_all_object_wakeups
DELETE FROM `durababble_mysql_snapshot_object_wakeups` WHERE worker_pool = ? AND object_type = ? AND object_id = ?

-- mysql_delete_object_wakeup
DELETE FROM `durababble_mysql_snapshot_object_wakeups` WHERE worker_pool = ? AND object_type = ? AND object_id = ? AND name = ?

-- mysql_delete_target_activation
DELETE FROM `durababble_mysql_snapshot_target_activations` WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?

-- mysql_drop_table
DROP TABLE IF EXISTS `durababble_mysql_snapshot_workflows`

-- mysql_due_object_wakeups
SELECT *
FROM `durababble_mysql_snapshot_object_wakeups`
WHERE wake_at <= ?
ORDER BY wake_at, created_at
LIMIT 100
FOR UPDATE SKIP LOCKED

-- mysql_existing_inbox_message_for_idempotency
SELECT id, worker_pool, target_kind, target_type, target_id, status, ready_at, shape_hash
FROM `durababble_mysql_snapshot_inbox`
WHERE idempotency_hash = ?
FOR UPDATE

-- mysql_fail_fence
UPDATE `durababble_mysql_snapshot_fences`
SET status = 'failed', error = ?, completed_at = NOW(6)
WHERE workflow_id = ? AND `key` = ? AND locked_by = ?

-- mysql_fail_inbox_message
UPDATE `durababble_mysql_snapshot_inbox` SET status = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN 'dead_lettered' ELSE 'failed' END, error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = CASE WHEN max_attempts IS NOT NULL AND attempts >= max_attempts THEN NOW(6) ELSE dead_lettered_at END, updated_at = NOW(6) WHERE id = ?

-- mysql_fail_live_step_attempts_for_workflow
UPDATE `durababble_mysql_snapshot_step_attempts`
SET status = 'failed', error = ?, completed_at = NOW(6)
WHERE workflow_id = ? AND status = 'running'

-- mysql_fail_live_steps_for_workflow
UPDATE `durababble_mysql_snapshot_steps`
SET status = 'failed', error = ?, updated_at = NOW(6)
WHERE workflow_id = ? AND status = 'running'

-- mysql_fail_step
UPDATE `durababble_mysql_snapshot_steps`
SET status = 'failed', error = ?, updated_at = NOW(6)
WHERE workflow_id = ? AND position = ?

-- mysql_fail_workflow
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'failed', error = ?, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
WHERE id = ? AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))

-- mysql_fail_workflow_with_worker
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'failed', error = ?, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)

-- mysql_heartbeat_latest_attempt
UPDATE `durababble_mysql_snapshot_step_attempts`
SET heartbeat_cursor = ?
WHERE workflow_id = ? AND position = ? AND status = 'running'
ORDER BY started_at DESC
LIMIT 1

-- mysql_heartbeat_step_row
UPDATE `durababble_mysql_snapshot_steps`
SET heartbeat_cursor = ?, updated_at = NOW(6)
WHERE workflow_id = ? AND position = ? AND status = 'running'

-- mysql_heartbeat_step_workflow
UPDATE `durababble_mysql_snapshot_workflows`
SET locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)

-- mysql_heartbeat_workflow
UPDATE `durababble_mysql_snapshot_workflows`
SET locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)

-- mysql_inbox_claim_rows_for_update
SELECT *
FROM `durababble_mysql_snapshot_inbox`
WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?
  AND status IN ('pending', 'failed', 'running', 'dead_lettered')
ORDER BY sequence
LIMIT 100
FOR UPDATE

-- mysql_inbox_head_for_update
SELECT *
FROM `durababble_mysql_snapshot_inbox`
WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?
  AND status IN ('pending', 'failed', 'running', 'dead_lettered')
ORDER BY sequence
LIMIT 1
FOR UPDATE

-- mysql_inbox_message
SELECT * FROM `durababble_mysql_snapshot_inbox` WHERE id = ?

-- mysql_inbox_messages_for
SELECT * FROM `durababble_mysql_snapshot_inbox`
WHERE target_kind = ? AND target_type = ? AND target_id = ?
ORDER BY sequence

-- mysql_inbox_messages_for_worker_pool
SELECT * FROM `durababble_mysql_snapshot_inbox`
WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?
ORDER BY sequence

-- mysql_insert_fence
INSERT IGNORE INTO `durababble_mysql_snapshot_fences` (workflow_id, `key`, status, locked_by, locked_until)
VALUES (?, ?, 'running', ?, DATE_ADD(NOW(6), INTERVAL ? SECOND))

-- mysql_insert_inbox_message
INSERT INTO `durababble_mysql_snapshot_inbox` (
id, worker_pool, target_kind, target_type, target_id, sequence, message_kind, method_name,
operation_id, idempotency_key, idempotency_hash, shape_hash, payload, status, ready_at, max_attempts
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)

-- mysql_insert_mailbox_sequence
INSERT IGNORE INTO `durababble_mysql_snapshot_mailbox_sequences` (worker_pool, target_kind, target_type, target_id, last_sequence)
VALUES (?, ?, ?, ?, 0)

-- mysql_insert_outbox
INSERT IGNORE INTO `durababble_mysql_snapshot_outbox` (id, workflow_id, topic, payload, `key`, status)
VALUES (?, ?, ?, ?, ?, 'pending')

-- mysql_insert_scheduled_step
INSERT IGNORE INTO `durababble_mysql_snapshot_steps` (workflow_id, position, name, status, updated_at)
VALUES (?, ?, ?, 'scheduled', NOW(6))

-- mysql_insert_step_attempt
INSERT INTO `durababble_mysql_snapshot_step_attempts` (id, workflow_id, position, name, status)
VALUES (?, ?, ?, ?, 'running')

-- mysql_insert_wait
INSERT INTO `durababble_mysql_snapshot_waits` (id, workflow_id, position, kind, event_key, wake_at, context, status)
VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')

-- mysql_insert_workflow
INSERT INTO `durababble_mysql_snapshot_workflows` (id, name, worker_pool, status, input) VALUES (?, ?, ?, ?, ?)

-- mysql_insert_workflow_history
INSERT INTO `durababble_mysql_snapshot_workflow_history` (workflow_id, event_index, kind, command_id, name, attempt_id, payload, error)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)

-- mysql_insert_workflow_with_worker
INSERT INTO `durababble_mysql_snapshot_workflows` (id, name, worker_pool, status, input, locked_by, locked_until) VALUES (?, ?, ?, ?, ?, ?, DATE_ADD(NOW(6), INTERVAL ? SECOND))

-- mysql_lock_fence_for_worker
SELECT 1 FROM `durababble_mysql_snapshot_fences` WHERE workflow_id = ? AND `key` = ? AND locked_by = ? AND status = 'running'

-- mysql_lock_inbox_message
SELECT * FROM `durababble_mysql_snapshot_inbox` WHERE id = ? FOR UPDATE

-- mysql_lock_inbox_message_for_worker
SELECT * FROM `durababble_mysql_snapshot_inbox`
WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
FOR UPDATE

-- mysql_lock_owned_workflow_for_update
SELECT 1
FROM `durababble_mysql_snapshot_workflows`
WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)
FOR UPDATE

-- mysql_lock_target_activation_for_completion
SELECT 1 FROM `durababble_mysql_snapshot_target_activations`
WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?
  AND status = 'running' AND locked_by = ?
  AND locked_until >= NOW(6)
FOR UPDATE

-- mysql_lock_workflow_for_termination
SELECT * FROM `durababble_mysql_snapshot_workflows` WHERE id = ? FOR UPDATE

-- mysql_lock_workflow_for_update
SELECT * FROM `durababble_mysql_snapshot_workflows` WHERE id = ? FOR UPDATE

-- mysql_lock_workflow_history_workflow
SELECT id FROM `durababble_mysql_snapshot_workflows` WHERE id = ? FOR UPDATE

-- mysql_mailbox_sequence_for_update
SELECT worker_pool, last_sequence
FROM `durababble_mysql_snapshot_mailbox_sequences`
WHERE target_kind = ? AND target_type = ? AND target_id = ?
FOR UPDATE

-- mysql_make_workflow_due
UPDATE `durababble_mysql_snapshot_workflows` SET next_run_at = NULL, updated_at = ? WHERE id = ?

-- mysql_mark_inbox_row_running
UPDATE `durababble_mysql_snapshot_inbox`
SET status = 'running', attempts = attempts + 1, locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
WHERE id = ?

-- mysql_mark_waits_workflows_pending
UPDATE `durababble_mysql_snapshot_workflows` SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6) WHERE id IN (<placeholders>) AND status = 'waiting'

-- mysql_mark_workflow_canceling_for_request
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'canceling', locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
WHERE id = ? AND status NOT IN ('completed', 'canceled')

-- mysql_mark_workflow_cancellation_delivered
UPDATE `durababble_mysql_snapshot_workflows`
SET cancel_delivered_at = COALESCE(cancel_delivered_at, NOW(6)), updated_at = NOW(6)
WHERE id = ? AND cancel_requested_at IS NOT NULL

-- mysql_mark_workflow_running
UPDATE `durababble_mysql_snapshot_workflows` SET status = 'running', error = NULL, updated_at = NOW(6) WHERE id = ? AND worker_pool = ? AND status = 'pending' AND locked_by IS NULL

-- mysql_mark_workflow_running_with_worker
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL ? SECOND), updated_at = NOW(6)
WHERE id = ? AND worker_pool = ?
  AND NOT (status IN ('completed', 'canceled', 'terminated') OR (status = 'failed' AND next_run_at IS NULL))

-- mysql_next_workflow_history_event_index
SELECT COALESCE(MAX(event_index), -1) + 1 AS event_index FROM `durababble_mysql_snapshot_workflow_history` WHERE workflow_id = ?

-- mysql_object_state
SELECT state FROM `durababble_mysql_snapshot_durable_objects` WHERE object_type = ? AND object_id = ?

-- mysql_outbox_by_key
SELECT id FROM `durababble_mysql_snapshot_outbox` WHERE `key` = ?

-- mysql_outbox_message
SELECT * FROM `durababble_mysql_snapshot_outbox` WHERE id = ?

-- mysql_read_fence
SELECT status, result, error FROM `durababble_mysql_snapshot_fences` WHERE workflow_id = ? AND `key` = ?

-- mysql_release_inbox_leases
UPDATE `durababble_mysql_snapshot_inbox` FORCE INDEX (<index>)
SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
WHERE status = 'running' AND locked_by = ?

-- mysql_release_outbox_leases
UPDATE `durababble_mysql_snapshot_outbox` FORCE INDEX (<index>)
SET status = 'pending', locked_by = NULL, locked_until = NULL
WHERE status = 'processing' AND locked_by = ?

-- mysql_release_target_activation_leases
UPDATE `durababble_mysql_snapshot_target_activations` FORCE INDEX (<index>)
SET status = 'pending', locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
WHERE status = 'running' AND locked_by = ?

-- mysql_release_workflow_leases
UPDATE `durababble_mysql_snapshot_workflows` FORCE INDEX (<index>)
SET status = CASE
    WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
    ELSE 'pending'
  END,
  locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
WHERE status = 'running' AND locked_by = ?

-- mysql_request_workflow_cancellation
UPDATE `durababble_mysql_snapshot_workflows`
SET cancel_reason = ?, cancel_requested_at = NOW(6), updated_at = NOW(6)
WHERE id = ?

-- mysql_retry_inbox_message
UPDATE `durababble_mysql_snapshot_inbox` SET status = 'pending', error = ?, ready_at = ?, locked_by = NULL, locked_until = NULL, updated_at = NOW(6) WHERE id = ?

-- mysql_running_step_exists
SELECT 1 FROM `durababble_mysql_snapshot_steps` WHERE workflow_id = ? AND position = ? AND status = 'running'

-- mysql_save_object_state
INSERT INTO `durababble_mysql_snapshot_durable_objects` (worker_pool, object_type, object_id, state)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE state = VALUES(state), updated_at = NOW(6)

-- mysql_schedule_workflow_retry
UPDATE `durababble_mysql_snapshot_workflows`
SET status = CASE
    WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
    ELSE 'pending'
  END,
  locked_by = NULL, locked_until = NULL, next_run_at = ?, updated_at = NOW(6)
WHERE id = ? AND status = 'running' AND locked_by = ? AND locked_until >= NOW(6)

-- mysql_set_target_activation_pending
INSERT INTO `durababble_mysql_snapshot_target_activations` (worker_pool, target_kind, target_type, target_id, status, ready_at)
VALUES (?, ?, ?, ?, 'pending', ?)
ON DUPLICATE KEY UPDATE
  status = IF(worker_pool = VALUES(worker_pool), 'pending', status),
  ready_at = IF(worker_pool = VALUES(worker_pool), VALUES(ready_at), ready_at),
  locked_by = IF(worker_pool = VALUES(worker_pool), NULL, locked_by),
  locked_until = IF(worker_pool = VALUES(worker_pool), NULL, locked_until),
  updated_at = IF(worker_pool = VALUES(worker_pool), NOW(6), updated_at)

-- mysql_steal_expired_leases
UPDATE `durababble_mysql_snapshot_workflows`
SET status = CASE
    WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
    ELSE 'pending'
  END,
  locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
WHERE status = 'running' AND locked_until < ?

-- mysql_step_attempt_count_for
SELECT COUNT(*) AS count FROM `durababble_mysql_snapshot_step_attempts` WHERE workflow_id = ? AND position = ?

-- mysql_step_attempts_for
SELECT * FROM `durababble_mysql_snapshot_step_attempts` WHERE workflow_id = ? ORDER BY started_at, position

-- mysql_step_heartbeat_cursor
SELECT heartbeat_cursor FROM `durababble_mysql_snapshot_steps` WHERE workflow_id = ? AND position = ?

-- mysql_steps_for
SELECT * FROM `durababble_mysql_snapshot_steps` WHERE workflow_id = ? ORDER BY position

-- mysql_supersede_running_step_attempts
UPDATE `durababble_mysql_snapshot_step_attempts`
SET status = 'failed', error = 'superseded by retry', completed_at = NOW(6)
WHERE workflow_id = ? AND position = ? AND status = 'running'

-- mysql_suspend_workflow
UPDATE `durababble_mysql_snapshot_workflows`
SET status = CASE
    WHEN cancel_requested_at IS NOT NULL THEN 'canceling'
    WHEN EXISTS (SELECT 1 FROM `durababble_mysql_snapshot_waits` WHERE workflow_id = ? AND status = 'pending') THEN 'waiting'
    ELSE 'pending'
  END,
  locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
WHERE id = ? AND status = 'running'
  AND (? IS NULL OR (locked_by = ? AND locked_until >= NOW(6)))

-- mysql_target_activation
SELECT * FROM `durababble_mysql_snapshot_target_activations` WHERE worker_pool = ? AND target_kind = ? AND target_type = ? AND target_id = ?

-- mysql_terminate_workflow
UPDATE `durababble_mysql_snapshot_workflows`
SET status = 'terminated', result = ?, error = ?, locked_by = NULL, locked_until = NULL, next_run_at = NULL, updated_at = NOW(6)
WHERE id = ?

-- mysql_terminate_workflow_inbox
UPDATE `durababble_mysql_snapshot_inbox`
SET status = 'dead_lettered', error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = NOW(6), updated_at = NOW(6)
WHERE target_kind = 'workflow' AND target_id = ? AND status IN ('pending', 'failed', 'running')

-- mysql_terminate_workflow_step_attempts
UPDATE `durababble_mysql_snapshot_step_attempts` SET status = 'canceled', error = ?, completed_at = NOW(6) WHERE workflow_id = ? AND status IN ('running', 'waiting')

-- mysql_terminate_workflow_steps
UPDATE `durababble_mysql_snapshot_steps` SET status = 'canceled', error = ?, updated_at = NOW(6) WHERE workflow_id = ? AND status IN ('scheduled', 'running', 'waiting')

-- mysql_terminate_workflow_target_activations
DELETE FROM `durababble_mysql_snapshot_target_activations` WHERE target_kind = 'workflow' AND target_id = ?

-- mysql_terminate_workflow_waits
UPDATE `durababble_mysql_snapshot_waits` SET status = 'canceled', completed_at = NOW(6) WHERE workflow_id = ? AND status = 'pending'

-- mysql_update_latest_attempt
UPDATE `durababble_mysql_snapshot_step_attempts`
SET status = ?, result = ?, error = ?, completed_at = NOW(6)
WHERE workflow_id = ? AND position = ? AND status IN ('running', 'waiting')
ORDER BY started_at DESC
LIMIT 1

-- mysql_update_mailbox_sequence
UPDATE `durababble_mysql_snapshot_mailbox_sequences`
SET last_sequence = ?, updated_at = NOW(6)
WHERE target_kind = ? AND target_type = ? AND target_id = ?

-- mysql_upsert_object_wakeup
INSERT INTO `durababble_mysql_snapshot_object_wakeups` (worker_pool, object_type, object_id, name, wake_at, payload)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  wake_at = VALUES(wake_at),
  payload = VALUES(payload),
  updated_at = NOW(6)

-- mysql_upsert_step_running
INSERT INTO `durababble_mysql_snapshot_steps` (workflow_id, position, name, status, started_at, updated_at)
VALUES (?, ?, ?, 'running', NOW(6), NOW(6))
ON DUPLICATE KEY UPDATE status = 'running', error = NULL, started_at = COALESCE(`durababble_mysql_snapshot_steps`.started_at, NOW(6)), updated_at = NOW(6)

-- mysql_upsert_target_activation
INSERT INTO `durababble_mysql_snapshot_target_activations` (worker_pool, target_kind, target_type, target_id, status, ready_at)
VALUES (?, ?, ?, ?, 'pending', ?)
ON DUPLICATE KEY UPDATE status = IF(status = 'running', status, 'pending'), ready_at = LEAST(ready_at, VALUES(ready_at)), updated_at = NOW(6)

-- mysql_upsert_waiting_step
INSERT INTO `durababble_mysql_snapshot_steps` (workflow_id, position, name, status, result, started_at, updated_at)
VALUES (?, ?, ?, 'waiting', ?, NOW(6), NOW(6))
ON DUPLICATE KEY UPDATE status = 'waiting', result = VALUES(result), error = NULL, updated_at = NOW(6)

-- mysql_waits_for_workflow
SELECT * FROM `durababble_mysql_snapshot_waits` WHERE workflow_id = ? ORDER BY created_at

-- mysql_workflow
SELECT * FROM `durababble_mysql_snapshot_workflows` WHERE id = ?

-- mysql_workflow_cancellation
SELECT id AS workflow_id, cancel_reason AS reason,
cancel_requested_at AS requested_at, cancel_delivered_at AS delivered_at
FROM `durababble_mysql_snapshot_workflows`
WHERE id = ? AND cancel_requested_at IS NOT NULL

-- mysql_workflow_history_count_for
SELECT COUNT(*) AS count FROM `durababble_mysql_snapshot_workflow_history` WHERE workflow_id = ?

-- mysql_workflow_history_for
SELECT * FROM `durababble_mysql_snapshot_workflow_history` WHERE workflow_id = ? ORDER BY event_index

-- mysql_workflow_locked_until
SELECT locked_until FROM `durababble_mysql_snapshot_workflows` WHERE id = ?

-- mysql_workflow_owned
SELECT 1
FROM `durababble_mysql_snapshot_workflows`
WHERE id = ? AND locked_by = ? AND status = 'running' AND locked_until >= NOW(6)
