# typed: true
# frozen_string_literal: true

module Durababble
  class SqlStore < Store
    TIMER_WAKE_BATCH_SIZE = 100

    #: (name: String, input: Object?, ?id: String?, ?worker_pool: String) -> String
    def enqueue_workflow(name:, input:, id: nil, worker_pool: "default")
      id ||= SecureRandom.uuid
      insert_workflow(name:, input:, status: "pending", id:, worker_pool:)
    end

    #: (name: String, input: Object?, ?worker_id: String?, ?lease_seconds: Numeric, ?worker_pool: String) -> String
    def create_workflow(name:, input:, worker_id: nil, lease_seconds: 60, worker_pool: "default")
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      insert_workflow(name:, input:, status: "running", id: SecureRandom.uuid, worker_id:, lease_microseconds:, worker_pool:)
    end

    #: (worker_id: String, lease_seconds: Numeric, ?workflow_names: Array[String]?, ?worker_pool: String, ?excluding_workflow_ids: Array[String]?) -> Hash[String, Object?]?
    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil, worker_pool: "default", excluding_workflow_ids: nil)
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      claim_runnable_workflow_unchecked(worker_id:, lease_microseconds:, workflow_names:, worker_pool:, excluding_workflow_ids:)
    end

    #: (workflow_id: String, worker_id: String, lease_seconds: Numeric, ?worker_pool: String) -> Hash[String, Object?]?
    def claim_workflow(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      claim_workflow_unchecked(workflow_id:, worker_id:, lease_microseconds:, worker_pool:)
    end

    #: (workflow_id: String, worker_id: String, lease_seconds: Numeric, ?worker_pool: String) -> Hash[String, Object?]?
    def claim_workflow_for_activation(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      claim_workflow_for_activation_unchecked(workflow_id:, worker_id:, lease_microseconds:, worker_pool:)
    end

    #: (workflow_id: String, worker_id: String, lease_seconds: Numeric) -> ActiveRecord::Result
    def heartbeat(workflow_id:, worker_id:, lease_seconds:)
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      heartbeat_unchecked(workflow_id:, worker_id:, lease_microseconds:)
    end

    #: (String, ?worker_id: String?, ?lease_seconds: Numeric, ?worker_pool: String) -> Object?
    def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60, worker_pool: "default")
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      mark_workflow_running_unchecked(workflow_id, worker_id:, lease_microseconds:, worker_pool:)
    end

    #: (workflow_id: String, command_id: Integer, name: String, event_index: Integer, ?args: Array[Object?], ?kwargs: Hash[Symbol, Object?], ?metadata: Hash[String, Object?], ?worker_id: String?) -> Object?
    def record_step_scheduled(workflow_id:, command_id:, name:, event_index:, args: [], kwargs: {}, metadata: {}, worker_id: nil)
      payload = { "name" => name, "args" => args, "kwargs" => kwargs }.merge(metadata)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        append_workflow_history_without_transaction(workflow_id:, kind: "step_scheduled", command_id:, name:, payload:, event_index:)
        execute_store_query(:insert_scheduled_step, [workflow_id, command_id, name])
      end
    end

    #: (workflow_id: String, name: String, event_index: Integer, ?command_id: Integer?, ?position: Integer?, ?worker_id: String?) -> Object?
    def record_step_started(workflow_id:, name:, event_index:, command_id: nil, position: nil, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        execute_store_query(:supersede_running_step_attempts, [workflow_id, command_id])
        execute_store_query(:upsert_step_running, [workflow_id, command_id, name])
        attempt_id = SecureRandom.uuid
        execute_store_query(:insert_step_attempt, [attempt_id, workflow_id, command_id, name])
        append_workflow_history_without_transaction(workflow_id:, kind: "step_started", command_id:, name:, attempt_id:, event_index:)
        attempt_id
      end
    end

    #: (workflow_id: String, result: Object?, event_index: Integer, ?command_id: Integer?, ?position: Integer?, ?worker_id: String?) -> Object?
    def record_step_completed(workflow_id:, result:, event_index:, command_id: nil, position: nil, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        record_step_completed_without_transaction(workflow_id:, command_id:, result:, event_index:)
      end
    end

    #: (workflow_id: String, error: String, event_index: Integer, ?command_id: Integer?, ?position: Integer?, ?worker_id: String?, ?terminal: bool, ?error_class: String?, ?error_message: String?) -> Object?
    def record_step_failed(workflow_id:, error:, event_index:, command_id: nil, position: nil, worker_id: nil, terminal: false, error_class: nil, error_message: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        record_step_failed_without_transaction(workflow_id:, command_id:, error:, terminal:, error_class:, error_message:, event_index:)
      end
    end

    #: (workflow_id: String, error: String, worker_id: String, run_at: Time, event_index: Integer, ?command_id: Integer?, ?position: Integer?) -> Object?
    def record_step_failed_and_schedule_retry(workflow_id:, error:, worker_id:, run_at:, event_index:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:)
        # [DURABABBLE-STEP-2] A retryable failure and its backoff row commit atomically.
        record_step_failed_without_transaction(workflow_id:, command_id:, error:, terminal: false, retrying: true, event_index:)
        scheduled = schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
        raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before workflow retry scheduling" unless scheduled

        scheduled
      end
    end

    #: (wait_history_event, Hash[Integer, wait_metadata]) -> wait_snapshot?
    def wait_snapshot_from_history_event(event, wait_metadata_by_command_id)
      command_id_value = event["command_id"]
      return unless command_id_value

      command_id = command_id_value.to_s.to_i
      wait = wait_payload_from_history_event(event, wait_metadata_by_command_id)
      return unless wait

      workflow_id = event.fetch("workflow_id").to_s
      status = case event.fetch("kind")
      when "step_waiting"
        "pending"
      when "step_completed"
        "completed"
      when "step_canceled", "step_failed"
        "canceled"
      else
        return
      end
      {
        "id" => "#{workflow_id}:#{command_id}",
        "workflow_id" => workflow_id,
        "position" => command_id,
        "command_id" => command_id,
        "kind" => wait["kind"],
        "event_key" => wait["event_key"],
        "wake_at" => wait["wake_at"],
        "context" => wait["context"] || event["payload"],
        "status" => status,
      }
    end

    #: (wait_history_event, Hash[Integer, wait_metadata]) -> wait_metadata?
    def wait_payload_from_history_event(event, wait_metadata_by_command_id)
      payload = event["payload"]
      return unless payload.is_a?(Hash)

      if payload["wait"].is_a?(Hash)
        wait = payload.fetch("wait") #: as untyped
        return {
          "kind" => wait["kind"],
          "event_key" => wait["event_key"],
          "wake_at" => wait["wake_at"],
          "context" => payload["context"] || wait["context"],
        }
      end

      command_id = event.fetch("command_id").to_s.to_i
      wait = wait_metadata_by_command_id[command_id]
      return unless wait

      {
        "kind" => wait["kind"],
        "event_key" => wait["event_key"],
        "wake_at" => wait["wake_at"],
        "context" => payload,
      }
    end

    #: (Array[Hash[String, Object?]]) -> Hash[Integer, wait_metadata]
    def wait_metadata_index(history)
      history.each_with_object({}) do |event, index|
        next unless ["step_scheduled", "step_waiting"].include?(event.fetch("kind"))

        command_id_value = event["command_id"]
        next unless command_id_value

        wait = wait_metadata_from_payload(event["payload"])
        next unless wait

        index[command_id_value.to_s.to_i] = wait
      end
    end

    #: (Object?) -> wait_metadata?
    def wait_metadata_from_payload(payload)
      return unless payload.is_a?(Hash)

      wait = payload["wait"]
      return unless wait.is_a?(Hash)

      wait = wait #: as untyped
      {
        "kind" => wait["kind"],
        "event_key" => wait["event_key"],
        "wake_at" => wait["wake_at"],
        "context" => payload["context"] || wait["context"],
      }
    end

    #: (WaitRequest) -> wait_event_payload
    def wait_payload(wait_request)
      {
        "context" => wait_request.context,
        "wait" => {
          "kind" => wait_request.kind,
          "event_key" => wait_request.event_key,
          "wake_at" => wait_request.wake_at,
        },
      }
    end

    #: (String) -> Object?
    def wake_parent_workflow_if_child_terminal(workflow_id)
      raise NotImplementedError
    end

    #: (String) -> Hash[String, Object?]
    def workflow(workflow_id)
      row = execute_store_query(:workflow, [workflow_id]).first
      raise KeyError, "workflow not found: #{workflow_id}" unless row

      decode_row(row)
    end

    #: (String) -> Array[Hash[String, Object?]]
    def steps_for(workflow_id)
      execute_store_query(:steps_for, [workflow_id])
        .map { |row| with_command_id(decode_row(row)) }
    end

    #: (String) -> Array[Hash[String, Object?]]
    def step_attempts_for(workflow_id)
      execute_store_query(:step_attempts_for, [workflow_id])
        .map { |row| with_command_id(decode_row(row)) }
    end

    #: (object_type: String, object_id: String) -> Object?
    def object_state(object_type:, object_id:)
      state = object_state_entry(object_type:, object_id:)
      state.equal?(Store::NO_OBJECT_STATE) ? nil : state
    end

    #: (object_type: String, object_id: String) -> Object?
    def object_state_entry(object_type:, object_id:)
      row = execute_store_query(:object_state, [object_type, object_id]).first
      row ? decode_row(row).fetch("state") : Store::NO_OBJECT_STATE
    end

    #: (String) -> Array[Hash[String, Object?]]
    def workflow_history_for(workflow_id)
      execute_store_query(:workflow_history_for, [workflow_id])
        .map { |row| decode_row(row) }
    end

    #: (String) -> Integer
    def workflow_history_count_for(workflow_id)
      row = execute_store_query(:workflow_history_count_for, [workflow_id]).first
      return 0 unless row

      row.fetch("count").to_s.to_i
    end

    # The next event_index for an out-of-band writer (e.g. admin termination)
    # that holds no replayed history in memory and so can't allocate from the
    # in-memory counter. Reads back only the highest-indexed row rather than the
    # whole history; next_event_index_after turns it into max+1 (or 0 when none).
    #: (String) -> Integer
    def next_workflow_history_index_for(workflow_id)
      rows = execute_store_query(:last_workflow_history_for, [workflow_id]).to_a
      WorkflowReplayHistory.next_event_index_after(rows)
    end

    #: (?now: Time, ?batch_size: Integer) -> Integer
    def wake_due_timers(now: Time.now, batch_size: TIMER_WAKE_BATCH_SIZE)
      batch_size = Integer(batch_size)
      raise ArgumentError, "batch_size must be positive" unless batch_size.positive?

      total = 0
      timestamp = timestamp_or_nil(now) || now
      loop do
        completed = complete_object_wakeups(timestamp, batch_size)
        total += completed
        break if completed < batch_size
      end
      total
    end

    # Diagnostic/test view reconstructed from workflow history and step state.
    # Runtime wake/claim paths must not call this; they use workflows.next_run_at.
    #: (String) -> Array[wait_snapshot]
    def wait_snapshots_for(workflow_id)
      snapshots = {}
      step_statuses = steps_for(workflow_id).to_h { |step| [step.fetch("position").to_s.to_i, step.fetch("status")] }
      history = workflow_history_for(workflow_id)
      wait_metadata_by_command_id = wait_metadata_index(history)
      history.each do |event|
        snapshot = wait_snapshot_from_history_event(event, wait_metadata_by_command_id)
        next unless snapshot

        if snapshot.fetch("status") == "pending" && step_statuses[snapshot.fetch("command_id").to_s.to_i] == "canceled"
          snapshot = snapshot.merge(
            "status" => "canceled",
          )
        end
        snapshots[snapshot.fetch("id")] = snapshot
      end
      snapshots.values.sort_by { |snapshot| snapshot.fetch("position").to_i }
    end

    #: (String) -> Hash[String, Object?]?
    def outbox_message(outbox_id)
      row = execute_store_query(:outbox_message, [outbox_id]).first
      decode_row(row) if row
    end

    #: (target_kind: String, target_type: String, target_id: String, message_kind: String, ?method_name: String?, ?payload: Object?, ?idempotency_key: String?, ?ready_at: Time?, ?max_attempts: Integer?, ?worker_pool: String) -> String
    def enqueue_inbox_message(target_kind:, target_type:, target_id:, message_kind:, method_name: nil, payload: {}, idempotency_key: nil, ready_at: nil, max_attempts: nil, worker_pool: "default")
      shape_hash = inbox_shape_hash(target_kind:, target_type:, target_id:, message_kind:, method_name:, payload:)
      result = transaction do
        reused = reuse_existing_inbox_message(idempotency_key, target_kind:, target_type:, target_id:, shape_hash:)
        next reused if reused

        sequence, mailbox_worker_pool = allocate_mailbox_sequence(worker_pool:, target_kind:, target_type:, target_id:)
        id = SecureRandom.uuid
        insert_inbox_message_without_transaction(
          id:,
          worker_pool: mailbox_worker_pool,
          target_kind:,
          target_type:,
          target_id:,
          sequence:,
          message_kind:,
          method_name:,
          operation_id: id,
          idempotency_key:,
          shape_hash:,
          payload:,
          ready_at:,
          max_attempts:,
        )
        upsert_target_activation_without_transaction(worker_pool: mailbox_worker_pool, target_kind:, target_type:, target_id:, ready_at:)
        id
      end
      result #: as String
    end

    #: (workflow_id: String, workflow_name: String, method_name: String, payload: Object?, ?idempotency_key: String?, ?max_attempts: Integer?) -> String
    def enqueue_workflow_command(workflow_id:, workflow_name:, method_name:, payload:, idempotency_key: nil, max_attempts: nil)
      result = transaction do
        workflow = lock_workflow_for_update(workflow_id)
        raise KeyError, "workflow not found: #{workflow_id}" unless workflow

        decoded_workflow = decode_row(workflow)
        persisted_workflow_name = decoded_workflow.fetch("name")
        raise Error, "workflow #{workflow_id} is #{persisted_workflow_name}, not #{workflow_name}" unless persisted_workflow_name == workflow_name

        worker_pool = row_worker_pool(decoded_workflow)
        target_kind = "workflow"
        message_kind = "workflow_command"
        shape_hash = inbox_shape_hash(target_kind:, target_type: workflow_name, target_id: workflow_id, message_kind:, method_name:, payload:)
        reused = reuse_existing_inbox_message(idempotency_key, target_kind:, target_type: workflow_name, target_id: workflow_id, shape_hash:)
        next reused if reused

        raise Error, "workflow #{workflow_id} is terminal" if terminal_for_cancellation?(decoded_workflow)

        sequence, mailbox_worker_pool = allocate_mailbox_sequence(worker_pool:, target_kind:, target_type: workflow_name, target_id: workflow_id)
        id = SecureRandom.uuid
        insert_inbox_message_without_transaction(
          id:,
          worker_pool: mailbox_worker_pool,
          target_kind:,
          target_type: workflow_name,
          target_id: workflow_id,
          sequence:,
          message_kind:,
          method_name:,
          operation_id: id,
          idempotency_key:,
          shape_hash:,
          payload:,
          max_attempts:,
        )
        upsert_target_activation_without_transaction(worker_pool: mailbox_worker_pool, target_kind:, target_type: workflow_name, target_id: workflow_id)
        id
      end
      result #: as String
    end

    #: (target_kind: String, target_type: String, target_id: String, worker_id: String, ?lease_seconds: Numeric, ?limit: Integer, ?now: Time, ?worker_pool: String) -> Array[Hash[String, Object?]]
    def claim_inbox_messages(target_kind:, target_type:, target_id:, worker_id:, lease_seconds: 60, limit: 1, now: Time.now, worker_pool: "default")
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      result = transaction do
        rows = inbox_claim_rows_for_update(worker_pool:, target_kind:, target_type:, target_id:, limit:)
        claimable_rows = contiguous_claimable_inbox_rows(rows, now:)
        # Gate object inbox claims on ownership of the unified per-object lease so
        # commands are only processed by the single, exclusive object owner.
        # Lease acquisition happens AFTER claimability is known: an empty or
        # blocked row set means no executable work in this pool, so we must not
        # side-effect a free lease into a held one (that would block the real
        # owner in another pool from progressing). Workflow inbox claims keep
        # the existing workflows.locked_by fence and are unaffected.
        if target_kind == "object" && !claimable_rows.empty?
          holder = claim_object_lease_unchecked(worker_pool:, object_type: target_type, object_id: target_id, worker_id:, lease_microseconds:)
          next [] unless holder
        end
        claimable_rows.map do |row|
          mark_inbox_row_running_without_transaction(message_id: row.fetch("id"), worker_id:, lease_microseconds:)
          claimed_inbox_row(row, worker_id:, lease_seconds:, now:)
        end
      end
      result #: as Array[Hash[String, Object?]]
    end

    # Post-claim view of a row we just row-locked and marked running in this
    # transaction, avoiding a redundant re-read per claimed message. The row
    # lock guarantees no other writer touched the row between the claim read
    # and here, so mirroring the column writes from
    # mark_inbox_row_running_without_transaction yields exactly what a re-read
    # would return.
    #: (Hash[String, Object?], worker_id: String, lease_seconds: Numeric, now: Time) -> Hash[String, Object?]
    def claimed_inbox_row(row, worker_id:, lease_seconds:, now:)
      claimed = decode_row(row)
      attempts = claimed.fetch("attempts") #: as untyped
      claimed["status"] = "running"
      claimed["attempts"] = attempts.to_i + 1
      claimed["locked_by"] = worker_id
      claimed["locked_until"] = now + lease_seconds
      claimed["updated_at"] = now
      claimed
    end

    #: (workflow_id: String, worker_id: String) -> bool
    def workflow_owned?(workflow_id:, worker_id:)
      !!execute_store_query(:workflow_owned, [workflow_id, worker_id]).first
    end

    #: (worker_pool: String, workflow_name: String, workflow_id: String, worker_id: String, lease_seconds: Numeric) -> Hash[String, Object?]?
    def claim_next_workflow_command(worker_pool:, workflow_name:, workflow_id:, worker_id:, lease_seconds:)
      lease_duration_microseconds(lease_seconds)
      return unless target_activation(worker_pool:, target_kind: "workflow", target_type: workflow_name, target_id: workflow_id)
      raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before workflow command claim" unless workflow_owned?(workflow_id:, worker_id:)

      claim_inbox_messages(
        worker_pool:,
        target_kind: "workflow",
        target_type: workflow_name,
        target_id: workflow_id,
        worker_id:,
        lease_seconds:,
        limit: 1,
      ).first
    end

    #: (String) -> Hash[String, Object?]?
    def inbox_message(message_id)
      row = execute_store_query(:inbox_message, [message_id]).first
      decode_row(row) if row
    end

    #: (target_kind: String, target_type: String, target_id: String, ?worker_pool: String?) -> Array[Hash[String, Object?]]
    def inbox_messages_for(target_kind:, target_type:, target_id:, worker_pool: nil)
      if worker_pool
        execute_store_query(:inbox_messages_for_worker_pool, [worker_pool, target_kind, target_type, target_id]).map { |row| decode_row(row) }
      else
        execute_store_query(:inbox_messages_for, [target_kind, target_type, target_id]).map { |row| decode_row(row) }
      end
    end

    #: (command_id: String, worker_id: String, ?lease_seconds: Numeric) -> Hash[String, Object?]?
    def claim_object_command(command_id:, worker_id:, lease_seconds: 60)
      lease_duration_microseconds(lease_seconds)
      row = inbox_message(command_id)
      return unless object_command_message?(row)

      row = row #: as Hash[String, Object?]
      return object_command_row(row) unless row.key?("target_kind")

      claimed = claim_inbox_message_by_id(
        message_id: command_id,
        worker_pool: row_worker_pool(row),
        target_kind: row.fetch("target_kind"),
        target_type: row.fetch("target_type"),
        target_id: row.fetch("target_id"),
        worker_id:,
        lease_seconds:,
      )
      return unless claimed

      claimed = claimed #: as Hash[String, Object?]
      object_command_row(claimed)
    end

    #: (workflow_id: String, worker_id: String, lease_seconds: Numeric, cursor: Object?, ?command_id: Integer?, ?position: Integer?) -> Object?
    def heartbeat_step(workflow_id:, worker_id:, lease_seconds:, cursor:, command_id: nil, position: nil)
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      heartbeat_step_unchecked(workflow_id:, worker_id:, lease_microseconds:, cursor:, command_id:, position:)
    end

    #: (worker_pool: String, object_type: String, object_id: String, worker_id: String, ?lease_seconds: Numeric) -> Hash[String, Object?]?
    def claim_object_lease(worker_pool:, object_type:, object_id:, worker_id:, lease_seconds: 60)
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      claim_object_lease_unchecked(worker_pool:, object_type:, object_id:, worker_id:, lease_microseconds:)
    end

    #: (object_type: String, object_id: String, worker_id: String, ?lease_seconds: Numeric) -> bool
    def renew_object_lease(object_type:, object_id:, worker_id:, lease_seconds: 60)
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      renew_object_lease_unchecked(object_type:, object_id:, worker_id:, lease_microseconds:)
    end

    #: (worker_id: String, lease_seconds: Numeric) -> Hash[String, Object?]?
    def claim_outbox(worker_id:, lease_seconds:)
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      claim_outbox_unchecked(worker_id:, lease_microseconds:)
    end

    #: (worker_id: String, lease_seconds: Numeric, ?target_kinds: Array[String]?, ?target_types: Array[String]?, ?now: Time, ?worker_pool: String) -> Hash[String, Object?]?
    def claim_target_activation(worker_id:, lease_seconds:, target_kinds: nil, target_types: nil, now: Time.now, worker_pool: "default")
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      claim_target_activation_unchecked(worker_id:, lease_microseconds:, target_kinds:, target_types:, now:, worker_pool:)
    end

    #: (command_id: String, result: Object?, ?object_type: String?, ?object_id: String?, ?state: Object?, ?wakeup_changes: Array[ObjectWakeupChange], ?worker_id: String?) -> Object?
    def complete_object_command(command_id:, result:, object_type: nil, object_id: nil, state: Store::NO_OBJECT_STATE, wakeup_changes: [], worker_id: nil)
      transaction do
        command = lock_object_command_for_completion(command_id:, worker_id:)
        next nil unless command
        next nil unless object_command_completion_target_matches?(command, object_type:, object_id:, state:, wakeup_changes:)

        assert_object_command_lease_for_update!(command, worker_id:)
        worker_pool = row_worker_pool(command)
        save_object_state(worker_pool:, object_type:, object_id:, state:) unless state.equal?(Store::NO_OBJECT_STATE)
        apply_object_wakeup_changes_without_transaction(worker_pool:, object_type:, object_id:, wakeup_changes:) unless wakeup_changes.empty?
        updated = complete_inbox_message_without_transaction(message_id: command_id, result:)
        reconcile_command_target_activation(command) if command.key?("target_kind")
        updated
      end
    end

    #: (command_id: String, error: String, ?worker_id: String?, ?terminal: bool) -> Object?
    def fail_object_command(command_id:, error:, worker_id: nil, terminal: false)
      transaction do
        command = lock_inbox_message_for_failure(command_id:, worker_id:)
        next nil unless command

        assert_object_command_lease_for_update!(command, worker_id:)
        updated = if terminal
          dead_letter_inbox_message_without_transaction(message_id: command_id, error:)
        else
          fail_inbox_message_without_transaction(message_id: command_id, error:)
        end
        reconcile_command_target_activation(command) if command.key?("target_kind")
        updated
      end
    end

    #: (command_id: String, error: String, worker_id: String, ready_at: Time) -> Object?
    def retry_object_command(command_id:, error:, worker_id:, ready_at:)
      transaction do
        command = lock_inbox_message_for_failure(command_id:, worker_id:)
        next nil unless command

        assert_object_command_lease_for_update!(command, worker_id:)
        updated = retry_inbox_message_without_transaction(message_id: command_id, error:, ready_at:)
        reconcile_command_target_activation(command) if command.key?("target_kind")
        updated
      end
    end

    #: (message_id: String, workflow_id: String, error: String, worker_id: String, ready_at: Time, event_index: Integer) -> Object?
    def retry_workflow_command(message_id:, workflow_id:, error:, worker_id:, ready_at:, event_index:)
      transaction do
        command = lock_inbox_message_for_failure(command_id: message_id, worker_id:)
        next nil unless command
        next nil unless workflow_command_targets_workflow?(command, workflow_id)

        workflow = lock_workflow_for_update(workflow_id)
        terminal = workflow && WorkflowStatus.terminal?(decode_row(workflow))
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) unless terminal

        if terminal
          updated = dead_letter_inbox_message_without_transaction(message_id:, error: "workflow #{workflow_id} is #{workflow.fetch("status")}")
          reconcile_command_target_activation(command)
          next updated
        end

        append_workflow_history_without_transaction(
          workflow_id:,
          kind: "workflow_command_failed",
          name: command["method_name"],
          attempt_id: message_id,
          payload: workflow_command_history_payload(command, message_id:),
          error:,
          event_index:,
        )
        updated = retry_inbox_message_without_transaction(message_id:, error:, ready_at:)
        reconcile_command_target_activation(command)
        updated
      end
    end

    #: (message_id: String, workflow_id: String, result: Object?, worker_id: String, event_index: Integer) -> Object?
    def complete_workflow_command(message_id:, workflow_id:, result:, worker_id:, event_index:)
      transaction do
        command = lock_inbox_message_for_completion(message_id:, worker_id:)
        next nil unless command
        # Reject misrouted workflow_id before the lease guard so an inbox row
        # belonging to a different workflow is a silent no-op rather than a
        # LeaseConflict against an unrelated (not-running) workflow row.
        next nil unless workflow_command_targets_workflow?(command, workflow_id)

        workflow = lock_workflow_for_update(workflow_id)
        terminal = workflow && WorkflowStatus.terminal?(decode_row(workflow))
        # [DURABABBLE-LEASE-4] Workflow command history commits need the workflow and inbox leases.
        # Terminal workflows skip the lease assert so a late completion can dead-letter the inbox row.
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) unless terminal

        if terminal
          updated = dead_letter_inbox_message_without_transaction(message_id:, error: "workflow #{workflow_id} is #{workflow.fetch("status")}")
          reconcile_command_target_activation(command)
          next updated
        end

        append_workflow_history_without_transaction(
          workflow_id:,
          kind: "workflow_command_completed",
          name: command["method_name"],
          attempt_id: message_id,
          payload: workflow_command_history_payload(command, message_id:, result:, include_result: true),
          event_index:,
        )
        updated = complete_inbox_message_without_transaction(message_id:, result:)
        reconcile_command_target_activation(command)
        updated
      end
    end

    #: (origin_kind: String, child_workflow_name: String, child_workflow_id: String, input: Object?, worker_pool: String, cancellation_policy: String, ?parent_workflow_id: String?, ?parent_command_id: Integer?, ?parent_worker_id: String?, ?parent_object_type: String?, ?parent_object_id: String?, ?parent_object_command_id: String?, ?parent_object_worker_id: String?, ?colocate: bool) -> Hash[String, Object?]
    def start_child_workflow(origin_kind:, child_workflow_name:, child_workflow_id:, input:, worker_pool:, cancellation_policy:, parent_workflow_id: nil, parent_command_id: nil, parent_worker_id: nil, parent_object_type: nil, parent_object_id: nil, parent_object_command_id: nil, parent_object_worker_id: nil, colocate: false)
      result = transaction do
        parent = fence_child_workflow_origin!(
          origin_kind:,
          parent_workflow_id:,
          parent_worker_id:,
          parent_object_type:,
          parent_object_id:,
          parent_object_command_id:,
          parent_object_worker_id:,
          colocate:,
        )
        colocated_owner_object_type = nil
        colocated_owner_object_id = nil
        if colocate
          colocated_owner_object_type, colocated_owner_object_id = resolve_colocated_owner(
            parent_object_type:,
            parent_object_id:,
            parent_row: parent,
          )
        end
        begin
          transaction do
            insert_child_workflow_without_transaction(
              origin_kind:,
              parent_workflow_id:,
              parent_command_id:,
              parent_object_type:,
              parent_object_id:,
              parent_object_command_id:,
              child_workflow_name:,
              child_workflow_id:,
              input:,
              worker_pool:,
              cancellation_policy:,
              colocated_owner_object_type:,
              colocated_owner_object_id:,
            )
          end
        rescue ActiveRecord::RecordNotUnique
          existing = child_workflow_by_child_id_for_update(child_workflow_id)
          unless existing
            raise WorkflowAlreadyExists, "workflow #{child_workflow_id} already exists"
          end

          next child_workflow_row(decode_row(existing))
        end
        created = child_workflow_by_child_id_for_update(child_workflow_id)
        raise KeyError, "child workflow not found after insert: #{child_workflow_id}" unless created

        child_workflow_row(decode_row(created))
      end
      result #: as Hash[String, Object?]
    end

    # Object-parent-only colocated child-object create. Mirrors
    # start_child_workflow: fences the parent object command (and, when
    # colocating, locks the parent object row so the root owner can be flattened),
    # then records the binding on the child durable_objects row. Objects are
    # created lazily, so this writes a lease-only row (NULL state) carrying the
    # owner columns — the child still initializes on its first claim. Idempotent
    # on re-run: the deterministic child id is the PK, so a retried command
    # re-stamps the same owner (a no-op) while a different owner colocating the
    # same id is a conflict. Returns a descriptor for the child object.
    #: (parent_object_type: String, parent_object_id: String, parent_object_command_id: String, parent_object_worker_id: String, child_object_type: String, child_object_id: String, worker_pool: String, ?colocate: bool) -> Hash[String, Object?]
    def start_child_object(parent_object_type:, parent_object_id:, parent_object_command_id:, parent_object_worker_id:, child_object_type:, child_object_id:, worker_pool:, colocate: true)
      result = transaction do
        parent = fence_child_workflow_origin!(
          origin_kind: "object",
          parent_workflow_id: nil,
          parent_worker_id: nil,
          parent_object_type:,
          parent_object_id:,
          parent_object_command_id:,
          parent_object_worker_id:,
          colocate:,
        )
        owner_type = nil
        owner_id = nil
        if colocate
          owner_type, owner_id = resolve_colocated_owner(
            parent_object_type:,
            parent_object_id:,
            parent_row: parent,
          )
        end
        insert_child_object_without_transaction(
          worker_pool:,
          object_type: child_object_type,
          object_id: child_object_id,
          colocated_owner_object_type: owner_type,
          colocated_owner_object_id: owner_id,
        )
        if colocate
          existing_owner_type, existing_owner_id = object_colocated_owner(object_type: child_object_type, object_id: child_object_id)
          unless existing_owner_type.to_s == owner_type.to_s && existing_owner_id.to_s == owner_id.to_s
            raise IdempotencyKeyConflict, "object #{child_object_type}/#{child_object_id} already used for a different colocation owner"
          end
        end
        {
          "object_type" => child_object_type,
          "object_id" => child_object_id,
          "worker_pool" => worker_pool,
          "colocated_owner_object_type" => owner_type,
          "colocated_owner_object_id" => owner_id,
        }
      end
      result #: as Hash[String, Object?]
    end

    #: (String) -> Hash[String, Object?]
    def observe_child_workflow(child_workflow_id)
      result = transaction do
        row = child_workflow_by_child_id_for_update(child_workflow_id)
        raise KeyError, "child workflow not found: #{child_workflow_id}" unless row

        child_workflow_row(decode_row(row))
      end
      result #: as Hash[String, Object?]
    end

    #: (parent_workflow_id: String) -> Array[Hash[String, Object?]]
    def child_workflow_rows_for_parent(parent_workflow_id:)
      execute_store_query(:child_workflow_rows_for_parent, [parent_workflow_id]).map { |row| child_workflow_row(decode_row(row)) }
    end

    #: (parent_object_type: String, parent_object_id: String) -> Array[Hash[String, Object?]]
    def child_workflow_rows_for_object(parent_object_type:, parent_object_id:)
      execute_store_query(:child_workflow_rows_for_object, [parent_object_type, parent_object_id]).map { |row| child_workflow_row(decode_row(row)) }
    end

    #: (message_id: String, workflow_id: String, error: String, worker_id: String, event_index: Integer) -> Object?
    def fail_workflow_command(message_id:, workflow_id:, error:, worker_id:, event_index:)
      transaction do
        command = lock_inbox_message_for_failure(command_id: message_id, worker_id:)
        next nil unless command
        # Reject misrouted workflow_id before the lease guard, mirroring
        # complete_workflow_command above.
        next nil unless workflow_command_targets_workflow?(command, workflow_id)

        workflow = lock_workflow_for_update(workflow_id)
        terminal = workflow && WorkflowStatus.terminal?(decode_row(workflow))
        # [DURABABBLE-LEASE-4] Workflow command failure history is also a workflow commit.
        # Terminal workflows skip the lease assert so a late failure can dead-letter the inbox row.
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) unless terminal

        if terminal
          updated = dead_letter_inbox_message_without_transaction(message_id:, error: "workflow #{workflow_id} is #{workflow.fetch("status")}")
          reconcile_command_target_activation(command)
          next updated
        end

        append_workflow_history_without_transaction(
          workflow_id:,
          kind: "workflow_command_failed",
          name: command["method_name"],
          attempt_id: message_id,
          payload: workflow_command_history_payload(command, message_id:),
          error:,
          event_index:,
        )
        updated = dead_letter_inbox_message_without_transaction(message_id:, error:)
        reconcile_command_target_activation(command)
        updated
      end
    end

    #: (target_kind: Object?, target_type: Object?, target_id: Object?, ?worker_pool: String) -> Hash[String, Object?]?
    def target_activation(target_kind:, target_type:, target_id:, worker_pool: "default")
      row = execute_store_query(:target_activation, [worker_pool, target_kind, target_type, target_id]).first
      decode_row(row) if row
    end

    private

    # Returns the id of a prior inbox message that matches this idempotency key,
    # re-activating its target so the message is processed again, or nil when no
    # prior message exists. Raises IdempotencyKeyConflict when the key was used
    # for a message with a different shape. Callers run this inside the enqueue
    # transaction and short-circuit (`next`) when an id comes back.
    #: (String?, target_kind: String, target_type: String, target_id: String, shape_hash: String) -> String?
    def reuse_existing_inbox_message(idempotency_key, target_kind:, target_type:, target_id:, shape_hash:)
      existing = existing_inbox_message_for_idempotency(idempotency_key, target_kind:, target_type:, target_id:)
      return unless existing

      unless existing.fetch("shape_hash") == shape_hash
        raise IdempotencyKeyConflict, "idempotency key #{idempotency_key} already used for a different inbox message"
      end

      if activatable_inbox_status?(existing.fetch("status"))
        upsert_target_activation_without_transaction(
          worker_pool: row_worker_pool(existing),
          target_kind: existing.fetch("target_kind"),
          target_type: existing.fetch("target_type"),
          target_id: existing.fetch("target_id"),
          ready_at: existing["ready_at"],
        )
      end
      existing.fetch("id") #: as String
    end

    #: (Object?, workflow_id: String, worker_id: String?, operation: String) -> Object?
    def require_fenced_workflow_update!(result, workflow_id:, worker_id:, operation:)
      result = result #: as untyped
      return result unless worker_id
      return result if result&.affected_rows.to_i == 1

      raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before #{operation}"
    end

    # Workflow completion is the one terminal write that must REJECT unfenced
    # callers when nothing changed. Live durable work, an already-terminal row,
    # and a stale lease all surface as a zero-row write; the contract for
    # `complete_workflow` is "I expected to make this row terminal" and any of
    # those mean the caller's belief was wrong. Cancel and fail are best-effort
    # and stay silent in the same situation via require_fenced_workflow_update!.
    # Fenced callers always raise LeaseConflict because their contract is
    # "I own this row".
    #: (Object?, workflow_id: String, worker_id: String?) -> Object?
    def require_workflow_completion_update!(result, workflow_id:, worker_id:)
      result = result #: as untyped
      return result if result&.affected_rows.to_i == 1

      message = "workflow #{workflow_id} cannot complete while incomplete durable work remains"
      raise LeaseConflict, "#{message} or the lease expired or moved" if worker_id

      raise Error, message
    end

    # Terminal workflow writes have one shared postcondition: if the update was
    # made by a leased worker it must still own the row, and the workflow must
    # not strand live waiting steps or attempts after becoming terminal. The
    # `failure_error` lets fail_workflow terminalize live steps/attempts as
    # "failed" (carrying the workflow's own error) while cancel/complete leave
    # them as "canceled".
    #: (workflow_id: String, worker_id: String?, operation: String, ?failure_error: String?) { () -> Object? } -> Object?
    def finalize_terminal_workflow_update!(workflow_id:, worker_id:, operation:, failure_error: nil, &block)
      transaction do
        result = block.call
        require_fenced_workflow_update!(result, workflow_id:, worker_id:, operation:)
        if failure_error
          fail_live_workflow_dependents(workflow_id, failure_error)
        else
          cancel_live_workflow_dependents(workflow_id)
        end
        wake_parent_workflow_if_child_terminal(workflow_id)
        result
      end
    end

    #: (String) -> Object?
    def cancel_pending_waits_for_workflow(workflow_id)
      execute_store_query(:cancel_waiting_steps_for_workflow, [workflow_id])
      execute_store_query(:cancel_waiting_step_attempts_for_workflow, [workflow_id])
    end

    #: (String) -> Object?
    def cancel_live_workflow_dependents(workflow_id)
      execute_store_query(:cancel_live_steps_for_workflow, [workflow_id])
      execute_store_query(:cancel_live_step_attempts_for_workflow, [workflow_id])
    end

    # Terminalize live waiting steps/attempts on workflow failure. A step that was
    # actively running observed the failure and is marked 'failed' with the
    # workflow's error. Scheduled/waiting steps never observed it and are
    # canceled, matching how an abandoned parked branch lands.
    #: (String, String) -> Object?
    def fail_live_workflow_dependents(workflow_id, error)
      execute_fail_live_steps_for_workflow(workflow_id, error)
      execute_fail_live_step_attempts_for_workflow(workflow_id, error)
      # cancel_live_*_for_workflow filter on 'scheduled'/'waiting' (and 'running'
      # for the attempts table) — anything we already marked 'failed' above no
      # longer matches, so the remaining live rows get the canceled terminal.
      execute_store_query(:cancel_live_steps_for_workflow, [workflow_id])
      execute_store_query(:cancel_live_step_attempts_for_workflow, [workflow_id])
    end

    #: (String, String) -> Object?
    def execute_fail_live_steps_for_workflow(workflow_id, error)
      raise NotImplementedError
    end

    #: (String, String) -> Object?
    def execute_fail_live_step_attempts_for_workflow(workflow_id, error)
      raise NotImplementedError
    end

    #: (terminal: bool, error_class: String?, error_message: String?, ?retrying: bool) -> Hash[String, Object?]?
    def step_failure_payload(terminal:, error_class:, error_message:, retrying: false)
      # [DURABABBLE-STEP-2] A retryable failure carries a "retrying" payload so replay treats it as
      # diagnostic rather than terminal.
      return { "retrying" => true } if retrying
      return unless terminal

      payload = { "terminal" => true }
      payload["error_class"] = error_class if error_class
      payload["error_message"] = error_message if error_message
      payload
    end

    #: (workflow_id: String, worker_id: String) -> void
    def assert_workflow_lease_for_update!(workflow_id:, worker_id:)
      return if lock_owned_workflow_for_update(workflow_id:, worker_id:)

      raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before state update"
    end

    # Fences the child start against the parent's live lease and returns the
    # locked parent row so the caller can read the parent's owner columns for
    # flattening. The extra object row lock is only taken when `colocate` is set —
    # non-colocated object-origin starts keep paying for exactly the command-lease
    # fence. Colocation is object-parent-only: workflow-origin colocation is
    # rejected here.
    #: (origin_kind: String, parent_workflow_id: String?, parent_worker_id: String?, parent_object_type: String?, parent_object_id: String?, parent_object_command_id: String?, parent_object_worker_id: String?, ?colocate: bool) -> Hash[String, Object?]?
    def fence_child_workflow_origin!(origin_kind:, parent_workflow_id:, parent_worker_id:, parent_object_type:, parent_object_id:, parent_object_command_id:, parent_object_worker_id:, colocate: false)
      case origin_kind
      when "workflow"
        raise ArgumentError, "workflow-origin child starts require parent_workflow_id" unless parent_workflow_id
        raise ArgumentError, "workflow-origin child starts require parent_worker_id" unless parent_worker_id
        raise ArgumentError, "colocate is only supported from durable object commands" if colocate

        parent = lock_owned_workflow_for_update(workflow_id: parent_workflow_id, worker_id: parent_worker_id)
        raise LeaseConflict, "workflow #{parent_workflow_id} lease expired or moved before child workflow start" unless parent

        parent
      when "object"
        raise ArgumentError, "object-origin child starts require parent_object_command_id" unless parent_object_command_id
        raise ArgumentError, "object-origin child starts require parent_object_worker_id" unless parent_object_worker_id

        command = lock_inbox_message_for_completion(message_id: parent_object_command_id, worker_id: parent_object_worker_id)
        matches_target = command && command.fetch("target_kind", nil) == "object" && command.fetch("target_type", nil) == parent_object_type && command.fetch("target_id", nil) == parent_object_id
        raise LeaseConflict, "object command #{parent_object_command_id} lease expired or moved before child start" unless matches_target

        colocate ? lock_owned_object_for_update(object_type: parent_object_type.to_s, object_id: parent_object_id.to_s, worker_id: parent_object_worker_id) : nil
      else
        raise ArgumentError, "unknown child workflow origin kind: #{origin_kind.inspect}"
      end
    end

    # Flattens the colocation owner for a colocated child start. The only input
    # that matters is the locked parent object row: if the parent is itself a
    # colocated child (its own owner columns are set) the child inherits that root
    # owner, otherwise the parent object itself becomes the owner. Every colocated
    # child therefore points directly at the root object, so the claim gate never
    # resolves a chain. Returns [owner_type, owner_id], or [nil, nil] when there is
    # nothing to colocate against. Runs inside the child-start transaction.
    #: (parent_object_type: String?, parent_object_id: String?, parent_row: Hash[String, Object?]?) -> [String?, String?]
    def resolve_colocated_owner(parent_object_type:, parent_object_id:, parent_row:)
      return [nil, nil] unless parent_object_type && parent_object_id

      inherited_type = parent_row && parent_row["colocated_owner_object_type"]
      inherited_id = parent_row && parent_row["colocated_owner_object_id"]
      return [inherited_type.to_s, inherited_id.to_s] if inherited_type && inherited_id

      [parent_object_type.to_s, parent_object_id.to_s]
    end

    # Conditional acquire of the owner object's lease for a claim path. Succeeds
    # (affected == 1) when the owner is free, already ours, or its lease has
    # expired; fails when a live peer still holds it. This is the single master
    # lease that fences a colocated child against its owner.
    #: (owner_object_type: String, owner_object_id: String, worker_id: String, lease_microseconds: Integer) -> bool
    def acquire_owner_object_lease(owner_object_type:, owner_object_id:, worker_id:, lease_microseconds:)
      result = execute_store_query(:acquire_owner_object_lease, [worker_id, lease_microseconds, owner_object_type, owner_object_id, worker_id])
      result.affected_rows.to_i == 1
    end

    # Renew the owner object's lease while this worker still holds it. Keepalive
    # path only: a worker that has lost the lease writes nothing (the locked_by
    # guard fails), so a re-homed owner is never clobbered.
    #: (owner_object_type: String, owner_object_id: String, worker_id: String, lease_microseconds: Integer) -> Object?
    def renew_owner_object_lease(owner_object_type:, owner_object_id:, worker_id:, lease_microseconds:)
      execute_store_query(:renew_owner_object_lease, [lease_microseconds, owner_object_type, owner_object_id, worker_id])
    end

    # Rides the workflow-step heartbeat: when the heartbeating workflow is a
    # colocated child, push its owner object's lease forward so no peer can poach
    # the owner (or its siblings) while this child stays alive. The owner columns
    # are read off the row the heartbeat already returned, so non-colocated
    # heartbeats pay nothing — the early return fires before any extra statement.
    # Runs inside the heartbeat transaction.
    #: (Hash[String, Object?]?, worker_id: String, lease_microseconds: Integer) -> void
    def keepalive_owner_object_lease(row, worker_id:, lease_microseconds:)
      owner_type = row && row["colocated_owner_object_type"]
      owner_id = row && row["colocated_owner_object_id"]
      return unless owner_type && owner_id

      renew_owner_object_lease(owner_object_type: owner_type.to_s, owner_object_id: owner_id.to_s, worker_id:, lease_microseconds:)
      nil
    end

    # Co-tenancy gate for the claim paths that hold a candidate row in Ruby: the
    # MySQL queue + targeted claims (which lock the row, then must take the owner
    # lease before the child lease) and the PG already-owned re-entry. Returns true
    # when the candidate is non-colocated — nothing to gate, zero extra work — or
    # when this worker now holds the owner object's lease. A false return means a
    # live peer holds the owner and the child must not be claimed. PG's folded
    # claim statements gate in-SQL and never reach this.
    #: (Hash[String, Object?], worker_id: String, lease_microseconds: Integer) -> bool
    def acquire_owner_object_lease_for_claim(candidate, worker_id:, lease_microseconds:)
      owner_type = candidate["colocated_owner_object_type"]
      owner_id = candidate["colocated_owner_object_id"]
      return true unless owner_type && owner_id

      acquire_owner_object_lease(owner_object_type: owner_type.to_s, owner_object_id: owner_id.to_s, worker_id:, lease_microseconds:)
    end

    #: (Hash[String, Object?], worker_id: String?) -> void
    def assert_object_command_lease_for_update!(command, worker_id:)
      return unless worker_id
      return unless command.fetch("target_kind", nil) == "object"

      object_type = command.fetch("target_type").to_s
      object_id = command.fetch("target_id").to_s
      return if lock_owned_object_for_update(object_type:, object_id:, worker_id:)

      raise LeaseConflict, "durable object #{object_type}/#{object_id} lease expired or moved before object command update"
    end

    # Apply an object command's ordered wake mutations within the completion
    # transaction so every change commits atomically with state and command
    # completion. A single command may schedule, replace, and cancel several
    # named wakes; we replay them in order.
    #: (worker_pool: String, object_type: String?, object_id: String?, wakeup_changes: Array[ObjectWakeupChange]) -> void
    def apply_object_wakeup_changes_without_transaction(worker_pool:, object_type:, object_id:, wakeup_changes:)
      wakeup_changes.each do |wakeup_change|
        case wakeup_change.action
        when :schedule
          upsert_object_wakeup_without_transaction(
            worker_pool:,
            object_type:,
            object_id:,
            name: wakeup_change.name,
            wake_at: wakeup_change.wake_at,
            payload: wakeup_change.payload,
          )
        when :cancel
          delete_object_wakeup_without_transaction(worker_pool:, object_type:, object_id:, name: wakeup_change.name)
        when :cancel_all
          delete_all_object_wakeups_without_transaction(worker_pool:, object_type:, object_id:)
        else
          raise ArgumentError, "unknown durable object wakeup change #{wakeup_change.action.inspect}"
        end
      end
    end

    #: (name: String, input: Object?, status: String, id: String, ?worker_id: String?, ?lease_microseconds: Integer?, ?worker_pool: String) -> String
    def insert_workflow(name:, input:, status:, id:, worker_id: nil, lease_microseconds: nil, worker_pool: "default")
      raise NotImplementedError
    end

    #: (worker_id: String, lease_microseconds: Integer, ?workflow_names: Array[String]?, ?worker_pool: String, ?excluding_workflow_ids: Array[String]?) -> Hash[String, Object?]?
    def claim_runnable_workflow_unchecked(worker_id:, lease_microseconds:, workflow_names: nil, worker_pool: "default", excluding_workflow_ids: nil)
      raise NotImplementedError
    end

    #: (workflow_id: String, worker_id: String, lease_microseconds: Integer, ?worker_pool: String) -> Hash[String, Object?]?
    def claim_workflow_unchecked(workflow_id:, worker_id:, lease_microseconds:, worker_pool: "default")
      raise NotImplementedError
    end

    #: (workflow_id: String, worker_id: String, lease_microseconds: Integer, ?worker_pool: String) -> Hash[String, Object?]?
    def claim_workflow_for_activation_unchecked(workflow_id:, worker_id:, lease_microseconds:, worker_pool: "default")
      raise NotImplementedError
    end

    #: (workflow_id: String, worker_id: String, lease_microseconds: Integer) -> ActiveRecord::Result
    def heartbeat_unchecked(workflow_id:, worker_id:, lease_microseconds:)
      raise NotImplementedError
    end

    #: (String, ?worker_id: String?, ?lease_microseconds: Integer, ?worker_pool: String) -> Object?
    def mark_workflow_running_unchecked(workflow_id, worker_id: nil, lease_microseconds: 60_000_000, worker_pool: "default")
      if worker_id
        execute_store_query(:mark_workflow_running_with_worker, [worker_id, lease_microseconds, workflow_id, worker_pool])
      else
        execute_store_query(:mark_workflow_running, [workflow_id, worker_pool])
      end
    end

    #: (workflow_id: String, worker_id: String, lease_microseconds: Integer, cursor: Object?, ?command_id: Integer?, ?position: Integer?) -> Object?
    def heartbeat_step_unchecked(workflow_id:, worker_id:, lease_microseconds:, cursor:, command_id: nil, position: nil)
      raise NotImplementedError
    end

    #: (worker_pool: String, object_type: String, object_id: String, worker_id: String, ?lease_microseconds: Integer) -> Hash[String, Object?]?
    def claim_object_lease_unchecked(worker_pool:, object_type:, object_id:, worker_id:, lease_microseconds: 60_000_000)
      raise NotImplementedError
    end

    #: (object_type: String, object_id: String, worker_id: String, ?lease_microseconds: Integer) -> bool
    def renew_object_lease_unchecked(object_type:, object_id:, worker_id:, lease_microseconds: 60_000_000)
      raise NotImplementedError
    end

    #: (worker_id: String, lease_microseconds: Integer) -> Hash[String, Object?]?
    def claim_outbox_unchecked(worker_id:, lease_microseconds:)
      raise NotImplementedError
    end

    #: (worker_id: String, lease_microseconds: Integer, ?target_kinds: Array[String]?, ?target_types: Array[String]?, ?now: Time, ?worker_pool: String) -> Hash[String, Object?]?
    def claim_target_activation_unchecked(worker_id:, lease_microseconds:, target_kinds: nil, target_types: nil, now: Time.now, worker_pool: "default")
      raise NotImplementedError
    end

    #: (workflow_id: String, worker_id: String, run_at: Time) -> Object?
    def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
      raise NotImplementedError
    end

    #: (String, error: String, ?worker_id: String?) -> Object?
    def fail_workflow(workflow_id, error:, worker_id: nil)
      raise NotImplementedError
    end

    #: (workflow_id: String, ?reason: Object?) -> Hash[String, Object?]
    def request_workflow_termination(workflow_id:, reason: nil)
      raise NotImplementedError
    end

    #: (Integer?, Integer?) -> Integer
    def normalize_command_id(command_id, position)
      id = command_id.nil? ? position : command_id
      raise ArgumentError, "command_id is required" if id.nil?

      id.to_i
    end

    #: (workflow_id: String, command_id: Integer, result: Object?, event_index: Integer) -> Object?
    def record_step_completed_without_transaction(workflow_id:, command_id:, result:, event_index:)
      raise NotImplementedError
    end

    #: (workflow_id: String, command_id: Integer, error: String, event_index: Integer, ?terminal: bool, ?error_class: String?, ?error_message: String?, ?retrying: bool) -> Object?
    def record_step_failed_without_transaction(workflow_id:, command_id:, error:, event_index:, terminal: false, error_class: nil, error_message: nil, retrying: false)
      raise NotImplementedError
    end

    #: (String) -> String
    def table(name)
      raise NotImplementedError
    end

    #: (Time?) -> String?
    def timestamp_or_nil(time)
      raise NotImplementedError
    end

    #: (Object?, Integer) -> Integer
    def complete_object_wakeups(now, batch_size)
      raise NotImplementedError
    end

    # Convert a batch of due object wakeups into ordinary `wake` inbox messages. Must run inside a
    # transaction so the inbox write, wakeup removal, and target activation commit atomically with the
    # row-locking claim that selected the batch. Returns the number of wakeups delivered.
    #: (Array[Hash[String, Object?]]) -> Integer
    def deliver_due_object_wakeups(wakeups)
      wakeups.each do |wakeup|
        wakeup_worker_pool = row_worker_pool(wakeup)
        target_kind = "object"
        target_type = wakeup.fetch("object_type") #: as String
        target_id = wakeup.fetch("object_id") #: as String
        name = wakeup.fetch("name") #: as String
        message_kind = "wake"
        payload = wakeup.fetch("payload")
        message_id = SecureRandom.uuid
        sequence, mailbox_worker_pool = allocate_mailbox_sequence(worker_pool: wakeup_worker_pool, target_kind:, target_type:, target_id:)
        insert_inbox_message_without_transaction(
          id: message_id,
          worker_pool: mailbox_worker_pool,
          target_kind:,
          target_type:,
          target_id:,
          sequence:,
          message_kind:,
          method_name: name,
          operation_id: message_id,
          idempotency_key: nil,
          shape_hash: inbox_shape_hash(target_kind:, target_type:, target_id:, message_kind:, method_name: name, payload:),
          payload:,
        )
        delete_object_wakeup_without_transaction(worker_pool: wakeup_worker_pool, object_type: target_type, object_id: target_id, name:)
        upsert_target_activation_without_transaction(worker_pool: mailbox_worker_pool, target_kind:, target_type:, target_id:)
      end
      wakeups.length
    end

    # Emit the wakeup-completion metric after the batch transaction commits, skipping empty batches so
    # the terminal drain iteration does not report a zero-valued sample.
    #: (Integer) -> void
    def report_object_wakeups_completed(count)
      return unless count.positive?

      Observability.count("durababble.waits.completed", { "durababble.wait.kind" => "object_wakeup" }, by: count)
    end

    #: (worker_pool: String, object_type: String?, object_id: String?, name: String, wake_at: Object?, payload: Object?) -> Object?
    def upsert_object_wakeup_without_transaction(worker_pool:, object_type:, object_id:, name:, wake_at:, payload:)
      raise NotImplementedError
    end

    #: (worker_pool: String, object_type: String?, object_id: String?, name: String) -> Object?
    def delete_object_wakeup_without_transaction(worker_pool:, object_type:, object_id:, name:)
      raise NotImplementedError
    end

    #: (worker_pool: String, object_type: String?, object_id: String?) -> Object?
    def delete_all_object_wakeups_without_transaction(worker_pool:, object_type:, object_id:)
      raise NotImplementedError
    end

    #: (worker_pool: String, target_kind: Object?, target_type: Object?, target_id: Object?, ?ready_at: Object?) -> Object?
    def upsert_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at: nil)
      raise NotImplementedError
    end

    public

    # Transactional wrapper around `upsert_target_activation_without_transaction`
    # for callers outside the store's internal transactional flows. Streaming
    # consumers use this to wake up a worker that will claim the per-object
    # lease (via `process_object_activation`) before they RPC in.
    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, ?ready_at: Object?) -> Object?
    def upsert_target_activation(worker_pool:, target_kind:, target_type:, target_id:, ready_at: nil)
      transaction do
        upsert_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at:)
      end
    end

    private

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String) -> [Integer, String]
    def allocate_mailbox_sequence(worker_pool:, target_kind:, target_type:, target_id:)
      raise NotImplementedError
    end

    #: (String?, target_kind: String, target_type: String, target_id: String) -> Hash[String, Object?]?
    def existing_inbox_message_for_idempotency(idempotency_key, target_kind:, target_type:, target_id:)
      raise NotImplementedError
    end

    #: (String) -> Hash[String, Object?]?
    def lock_workflow_for_update(workflow_id)
      raise NotImplementedError
    end

    #: (workflow_id: String, worker_id: String) -> Hash[String, Object?]?
    def lock_owned_workflow_for_update(workflow_id:, worker_id:)
      raise NotImplementedError
    end

    #: (object_type: String, object_id: String, worker_id: String) -> Hash[String, Object?]?
    def lock_owned_object_for_update(object_type:, object_id:, worker_id:)
      raise NotImplementedError
    end

    #: (id: String, worker_pool: String, target_kind: String, target_type: String, target_id: String, sequence: Integer, message_kind: String, method_name: String?, operation_id: String, idempotency_key: String?, shape_hash: String, payload: Object?, ?ready_at: Time?, ?max_attempts: Integer?) -> Object?
    def insert_inbox_message_without_transaction(id:, worker_pool:, target_kind:, target_type:, target_id:, sequence:, message_kind:, method_name:, operation_id:, idempotency_key:, shape_hash:, payload:, ready_at: nil, max_attempts: nil)
      raise NotImplementedError
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, limit: Integer) -> Array[Hash[String, Object?]]
    def inbox_claim_rows_for_update(worker_pool:, target_kind:, target_type:, target_id:, limit:)
      raise NotImplementedError
    end

    #: (worker_pool: String, target_kind: Object?, target_type: Object?, target_id: Object?) -> Hash[String, Object?]?
    def inbox_head_for_update(worker_pool:, target_kind:, target_type:, target_id:)
      raise NotImplementedError
    end

    #: (worker_pool: String, target_kind: Object?, target_type: Object?, target_id: Object?) -> Hash[String, Object?]?
    def inbox_head_metadata_for_update(worker_pool:, target_kind:, target_type:, target_id:)
      raise NotImplementedError
    end

    #: (message_id: Object?, worker_id: String, lease_microseconds: Integer) -> Object?
    def mark_inbox_row_running_without_transaction(message_id:, worker_id:, lease_microseconds:)
      raise NotImplementedError
    end

    #: (command_id: String, worker_id: String?) -> Hash[String, Object?]?
    def lock_object_command_for_completion(command_id:, worker_id:)
      raise NotImplementedError
    end

    #: (object_type: String?, object_id: String?, state: Object?, ?worker_pool: String) -> Object?
    def save_object_state(object_type:, object_id:, state:, worker_pool: "default")
      raise NotImplementedError
    end

    #: (message_id: String, result: Object?) -> Object?
    def complete_inbox_message_without_transaction(message_id:, result:)
      raise NotImplementedError
    end

    #: (command_id: String, worker_id: String?) -> Hash[String, Object?]?
    def lock_inbox_message_for_failure(command_id:, worker_id:)
      raise NotImplementedError
    end

    #: (message_id: String, error: String) -> Object?
    def fail_inbox_message_without_transaction(message_id:, error:)
      raise NotImplementedError
    end

    #: (message_id: String, error: String, ready_at: Time) -> Object?
    def retry_inbox_message_without_transaction(message_id:, error:, ready_at:)
      raise NotImplementedError
    end

    #: (message_id: String, worker_id: String) -> Hash[String, Object?]?
    def lock_inbox_message_for_completion(message_id:, worker_id:)
      raise NotImplementedError
    end

    #: (Hash[String, Object?], message_id: String, ?result: Object?, ?include_result: bool) -> Hash[String, Object?]
    def workflow_command_history_payload(command, message_id:, result: nil, include_result: false)
      command = decode_row(command) if command.key?("payload") && !command["payload"].is_a?(Hash)
      payload = command["payload"].is_a?(Hash) ? command.fetch("payload") : {}
      payload = payload #: as untyped
      history_payload = {
        "message_id" => message_id,
        "method" => command["method_name"] || payload["method"] || payload["method_name"],
        "args" => payload.fetch("args", []),
        "kwargs" => payload.fetch("kwargs", {}),
        "shape_hash" => command["shape_hash"],
        "sequence" => command["sequence"],
      }
      history_payload["result"] = result if include_result
      history_payload
    end

    #: (Hash[String, Object?], String) -> bool
    def workflow_command_targets_workflow?(command, workflow_id)
      command.fetch("target_kind", nil) == "workflow" && command.fetch("target_id", nil) == workflow_id
    end

    #: (Hash[String, Object?], object_type: String?, object_id: String?, state: Object?, wakeup_changes: Array[ObjectWakeupChange]) -> bool
    def object_command_completion_target_matches?(command, object_type:, object_id:, state:, wakeup_changes:)
      target_required = !object_type.nil? || !object_id.nil? || !state.equal?(Store::NO_OBJECT_STATE) || !wakeup_changes.empty?
      return true unless target_required
      return true unless command.key?("target_kind")

      command.fetch("target_kind") == "object" && command.fetch("target_type") == object_type && command.fetch("target_id") == object_id
    end

    #: (workflow_id: String, kind: String, event_index: Integer, ?command_id: Integer?, ?name: Object?, ?attempt_id: String?, ?payload: Object?, ?error: String?) -> Integer
    def append_workflow_history_without_transaction(workflow_id:, kind:, event_index:, command_id: nil, name: nil, attempt_id: nil, payload: nil, error: nil)
      # The lease holder allocates event_index from the history it already replayed
      # in memory, so the insert is a plain parameterized append: the lease assert
      # locked the workflows row, and no MAX(event_index) read-back is needed.
      execute_store_query(:insert_workflow_history_at, [workflow_id, event_index, kind, command_id, name, attempt_id, dump_serialized(payload), error])
      event_index
    end

    #: (message_id: String, error: String) -> Object?
    def dead_letter_inbox_message_without_transaction(message_id:, error:)
      raise NotImplementedError
    end

    #: (Hash[String, Object?]) -> Object?
    def reconcile_command_target_activation(command)
      reconcile_target_activation_without_transaction(
        worker_pool: row_worker_pool(command),
        target_kind: command.fetch("target_kind").to_s,
        target_type: command.fetch("target_type").to_s,
        target_id: command.fetch("target_id").to_s,
      )
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, ?now: Time) -> Object?
    def reconcile_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, now: Time.now)
      if target_kind == "workflow" && (terminal_error = terminal_workflow_target_error(worker_pool:, workflow_id: target_id))
        dead_letter_terminal_workflow_inbox_without_transaction(
          target_type:,
          target_id:,
          error: terminal_error,
        )
        delete_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:)
        return
      end

      head = inbox_head_metadata_for_update(worker_pool:, target_kind:, target_type:, target_id:)

      if head && !InboxStatus.dead_lettered?(head)
        ready_at = target_activation_ready_at_for(head, now:)
        set_target_activation_pending_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at:)
      else
        delete_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:)
      end
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String) -> Object?
    def delete_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:)
      raise NotImplementedError
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, ready_at: Object?) -> Object?
    def set_target_activation_pending_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at:)
      raise NotImplementedError
    end

    #: (worker_pool: String, workflow_id: String) -> String?
    def terminal_workflow_target_error(worker_pool:, workflow_id:)
      row = terminal_workflow_target_row_for_update(worker_pool:, workflow_id:)
      return unless row && WorkflowStatus.terminal?(row)

      status = row.fetch("status")
      error = row["error"]
      suffix = error.to_s.empty? ? "" : ": #{error}"
      "workflow #{workflow_id} is terminal #{status}#{suffix}"
    end

    #: (worker_pool: String, workflow_id: Object?) -> Hash[String, Object?]?
    def terminal_workflow_target_row_for_update(worker_pool:, workflow_id:)
      raise NotImplementedError
    end

    #: (target_type: String, target_id: String, error: String) -> Object?
    def dead_letter_terminal_workflow_inbox_without_transaction(target_type:, target_id:, error:)
      raise NotImplementedError
    end

    #: (Object?) -> bool
    def activatable_inbox_status?(status)
      status = status #: as untyped
      InboxStatus.activatable?(status)
    end

    #: (message_id: String, worker_pool: String, target_kind: Object?, target_type: Object?, target_id: Object?, worker_id: String, lease_seconds: Numeric, ?now: Time) -> Object?
    def claim_inbox_message_by_id(message_id:, worker_pool:, target_kind:, target_type:, target_id:, worker_id:, lease_seconds:, now: Time.now)
      lease_microseconds = lease_duration_microseconds(lease_seconds)
      transaction do
        head = inbox_head_for_update(worker_pool:, target_kind:, target_type:, target_id:)
        next unless head&.fetch("id") == message_id

        head = head #: as Hash[String, Object?]
        next unless inbox_row_claimable?(head, now:)

        if target_kind == "object"
          holder = claim_object_lease_unchecked(worker_pool:, object_type: target_type.to_s, object_id: target_id.to_s, worker_id:, lease_microseconds:)
          next unless holder
        end

        mark_inbox_row_running_without_transaction(message_id:, worker_id:, lease_microseconds:)
        claimed_inbox_row(head, worker_id:, lease_seconds:, now:)
      end
    end

    #: (String) -> Hash[String, Object?]?
    def child_workflow_by_child_id_for_update(child_workflow_id)
      execute_store_query(:child_workflow_by_child_id_for_update, [child_workflow_id]).first
    end

    #: (Hash[String, Object?]) -> Hash[String, Object?]
    def child_workflow_row(row)
      row.merge(
        "origin_kind" => row["child_origin_kind"],
        "child_workflow_id" => row.fetch("id"),
        "child_workflow_name" => row.fetch("name"),
        "cancellation_policy" => row["child_cancellation_policy"],
      )
    end

    #: (origin_kind: String, parent_workflow_id: String?, parent_command_id: Integer?, parent_object_type: String?, parent_object_id: String?, parent_object_command_id: String?, child_workflow_name: String, child_workflow_id: String, input: Object?, worker_pool: String, cancellation_policy: String, ?colocated_owner_object_type: String?, ?colocated_owner_object_id: String?) -> Object?
    def insert_child_workflow_without_transaction(origin_kind:, parent_workflow_id:, parent_command_id:, parent_object_type:, parent_object_id:, parent_object_command_id:, child_workflow_name:, child_workflow_id:, input:, worker_pool:, cancellation_policy:, colocated_owner_object_type: nil, colocated_owner_object_id: nil)
      params = [
        child_workflow_id,
        child_workflow_name,
        worker_pool,
        "pending",
        dump_workflow_input(name: child_workflow_name, input:),
        origin_kind,
        parent_workflow_id,
        parent_command_id,
        parent_object_type,
        parent_object_id,
        parent_object_command_id,
        cancellation_policy,
        colocated_owner_object_type,
        colocated_owner_object_id,
      ]
      execute_store_query(:insert_child_workflow, params)
    end

    # Idempotent insert of a lease-only child object row carrying its colocation
    # owner. A primary-key conflict is ignored by the per-backend idempotent
    # insert so a retried command does not error; start_child_object reads the
    # row back to confirm the owner matches.
    #: (worker_pool: String, object_type: String, object_id: String, ?colocated_owner_object_type: String?, ?colocated_owner_object_id: String?) -> Object?
    def insert_child_object_without_transaction(worker_pool:, object_type:, object_id:, colocated_owner_object_type: nil, colocated_owner_object_id: nil)
      execute_store_query(:insert_child_object, [worker_pool, object_type, object_id, colocated_owner_object_type, colocated_owner_object_id])
    end

    # Public read accessor: returns a child's persisted flattened owner, or
    # [nil, nil] when the object is not colocated. Used internally by
    # start_child_object and exposed so callers can confirm the owner persisted.
    public

    #: (object_type: String, object_id: String) -> [String?, String?]
    def object_colocated_owner(object_type:, object_id:)
      row = execute_store_query(:object_colocated_owner, [object_type, object_id]).first
      return [nil, nil] unless row

      [row["colocated_owner_object_type"], row["colocated_owner_object_id"]]
    end
  end
end
