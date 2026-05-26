# typed: true
# frozen_string_literal: true

module Durababble
  class SqlStore < Store
    #: (name: String, input: Object?) -> String
    def create_workflow(name:, input:)
      id = enqueue_workflow(name:, input:)
      mark_workflow_running(id)
      id
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, result: Object?, ?worker_id: String?) -> Object?
    def record_step_completed(workflow_id:, result:, command_id: nil, position: nil, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        record_step_completed_without_transaction(workflow_id:, command_id:, result:)
      end
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, error: String, ?worker_id: String?) -> Object?
    def record_step_failed(workflow_id:, error:, command_id: nil, position: nil, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        record_step_failed_without_transaction(workflow_id:, command_id:, error:)
      end
    end

    #: (workflow_id: String, error: String, run_at: Time, worker_id: String, ?command_id: Integer?, ?position: Integer?) -> Object?
    def record_step_failed_and_schedule_retry(workflow_id:, error:, run_at:, worker_id:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:)
        # [DURABABBLE-STEP-2] A retryable failure and its backoff row commit atomically.
        record_step_failed_without_transaction(workflow_id:, command_id:, error:, payload: { "retrying" => true })
        scheduled = schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
        raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before workflow retry scheduling" unless scheduled

        scheduled
      end
    end

    #: (workflow_id: String, worker_id: String, run_at: Time) -> Object?
    def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
      raise NotImplementedError
    end

    #: (String) -> Hash[String, Object?]
    def workflow(workflow_id)
      row = execute_params("SELECT * FROM #{table("workflows")} WHERE id = #{placeholder(1)}", [workflow_id]).first
      raise KeyError, "workflow not found: #{workflow_id}" unless row

      decode_row(row)
    end

    #: (String) -> Array[Hash[String, Object?]]
    def steps_for(workflow_id)
      execute_params("SELECT * FROM #{table("steps")} WHERE workflow_id = #{placeholder(1)} ORDER BY position", [workflow_id])
        .map { |row| with_command_id(decode_row(row)) }
    end

    #: (String) -> Array[Hash[String, Object?]]
    def step_attempts_for(workflow_id)
      execute_params("SELECT * FROM #{table("step_attempts")} WHERE workflow_id = #{placeholder(1)} ORDER BY started_at, position", [workflow_id])
        .map { |row| with_command_id(decode_row(row)) }
    end

    #: (object_type: String, object_id: String) -> Object?
    def object_state(object_type:, object_id:)
      row = execute_params(
        "SELECT state FROM #{table("durable_objects")} WHERE object_type = #{placeholder(1)} AND object_id = #{placeholder(2)}",
        [object_type, object_id],
      ).first
      decode_row(row).fetch("state") if row
    end

    #: (String) -> Array[Hash[String, Object?]]
    def workflow_history_for(workflow_id)
      execute_params("SELECT * FROM #{table("workflow_history")} WHERE workflow_id = #{placeholder(1)} ORDER BY event_index", [workflow_id])
        .map { |row| decode_row(row) }
    end

    #: (?now: Time) -> Integer
    def wake_due_timers(now: Time.now)
      complete_timer_waits(timestamp_or_nil(now) || now)
    end

    #: (String) -> Array[Hash[String, Object?]]
    def waits_for(workflow_id)
      execute_params("SELECT * FROM #{table("waits")} WHERE workflow_id = #{placeholder(1)} ORDER BY created_at", [workflow_id])
        .map { |row| decode_row(row) }
    end

    #: (String) -> Hash[String, Object?]?
    def outbox_message(outbox_id)
      row = execute_params("SELECT * FROM #{table("outbox")} WHERE id = #{placeholder(1)}", [outbox_id]).first
      decode_row(row) if row
    end

    #: (target_kind: String, target_type: String, target_id: String, message_kind: String, ?method_name: String?, ?payload: Object?, ?idempotency_key: String?, ?ready_at: Time?, ?max_attempts: Integer?) -> String
    def enqueue_inbox_message(target_kind:, target_type:, target_id:, message_kind:, method_name: nil, payload: {}, idempotency_key: nil, ready_at: nil, max_attempts: nil)
      # [DURABABBLE-OBJ-1] Durable target commands are persisted by target identity before execution.
      shape_hash = inbox_shape_hash(target_kind:, target_type:, target_id:, message_kind:, method_name:, payload:)
      result = transaction do
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
        insert_inbox_message_without_transaction(
          id:,
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
        upsert_target_activation_without_transaction(target_kind:, target_type:, target_id:, ready_at:)
        id
      end
      result #: as String
    end

    #: (workflow_id: String, workflow_name: String, method_name: String, payload: Object?, ?idempotency_key: String?) -> String
    def enqueue_workflow_command(workflow_id:, workflow_name:, method_name:, payload:, idempotency_key: nil)
      target_kind = "workflow"
      message_kind = "workflow_command"
      shape_hash = inbox_shape_hash(target_kind:, target_type: workflow_name, target_id: workflow_id, message_kind:, method_name:, payload:)
      result = transaction do
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

        workflow = lock_workflow_for_update(workflow_id)
        raise KeyError, "workflow not found: #{workflow_id}" unless workflow

        raise Error, "workflow #{workflow_id} is terminal" if terminal_for_cancellation?(decode_row(workflow))

        sequence = allocate_mailbox_sequence(target_kind:, target_type: workflow_name, target_id: workflow_id)
        id = SecureRandom.uuid
        insert_inbox_message_without_transaction(
          id:,
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
        )
        upsert_target_activation_without_transaction(target_kind:, target_type: workflow_name, target_id: workflow_id)
        id
      end
      result #: as String
    end

    #: (target_kind: String, target_type: String, target_id: String, worker_id: String, ?lease_seconds: Numeric, ?limit: Integer, ?now: Time) -> Array[Hash[String, Object?]]
    def claim_inbox_messages(target_kind:, target_type:, target_id:, worker_id:, lease_seconds: 60, limit: 1, now: Time.now)
      result = transaction do
        assert_workflow_inbox_lease_for_update!(target_kind:, workflow_id: target_id, worker_id:)
        rows = inbox_claim_rows_for_update(target_kind:, target_type:, target_id:, limit:)
        claimable = contiguous_claimable_inbox_rows(rows, now:)
        claimable.each do |row|
          mark_inbox_row_running_without_transaction(message_id: row.fetch("id"), worker_id:, lease_seconds:)
        end
        claimable.map { |row| inbox_message(row.fetch("id").to_s) }
      end
      result #: as Array[Hash[String, Object?]]
    end

    #: (String) -> Hash[String, Object?]?
    def inbox_message(message_id)
      row = execute_params("SELECT * FROM #{table("inbox")} WHERE id = #{placeholder(1)}", [message_id]).first
      decode_row(row) if row
    end

    #: (target_kind: String, target_type: String, target_id: String) -> Array[Hash[String, Object?]]
    def inbox_messages_for(target_kind:, target_type:, target_id:)
      execute_params(<<~SQL, [target_kind, target_type, target_id]).map { |row| decode_row(row) }
        SELECT * FROM #{table("inbox")}
        WHERE target_kind = #{placeholder(1)} AND target_type = #{placeholder(2)} AND target_id = #{placeholder(3)}
        ORDER BY sequence
      SQL
    end

    #: (command_id: String, worker_id: String, ?lease_seconds: Numeric) -> Hash[String, Object?]?
    def claim_object_command(command_id:, worker_id:, lease_seconds: 60)
      # [DURABABBLE-OBJ-1] Object command execution claims the mailbox head for the target identity.
      row = inbox_message(command_id)
      return unless object_command_message?(row)

      row = row #: as Hash[String, Object?]
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

      claimed = claimed #: as Hash[String, Object?]
      object_command_row(claimed)
    end

    #: (command_id: String, result: Object?, ?object_type: String?, ?object_id: String?, ?state: Object?, ?worker_id: String?) -> Object?
    def complete_object_command(command_id:, result:, object_type: nil, object_id: nil, state: Store::NO_OBJECT_STATE, worker_id: nil)
      # [DURABABBLE-OBJ-1] Completion and state update share the command lifecycle transaction.
      transaction do
        command = lock_object_command_for_completion(command_id:, worker_id:)
        next nil unless command

        save_object_state(object_type:, object_id:, state:) unless state.equal?(Store::NO_OBJECT_STATE)
        updated = complete_inbox_message_without_transaction(message_id: command_id, result:)
        reconcile_target_activation_without_transaction(target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id")) if command.key?("target_kind")
        updated
      end
    end

    #: (command_id: String, error: String, ?worker_id: String?, ?terminal: bool) -> Object?
    def fail_object_command(command_id:, error:, worker_id: nil, terminal: false)
      transaction do
        command = lock_inbox_message_for_failure(command_id:, worker_id:)
        next nil unless command

        updated = if terminal
          dead_letter_inbox_message_without_transaction(message_id: command_id, error:)
        else
          fail_inbox_message_without_transaction(message_id: command_id, error:)
        end
        reconcile_target_activation_without_transaction(target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id")) if command.key?("target_kind")
        updated
      end
    end

    #: (command_id: String, error: String, worker_id: String, ready_at: Time) -> Object?
    def retry_object_command(command_id:, error:, worker_id:, ready_at:)
      transaction do
        command = lock_inbox_message_for_failure(command_id:, worker_id:)
        next nil unless command

        updated = retry_inbox_message_without_transaction(message_id: command_id, error:, ready_at:)
        reconcile_target_activation_without_transaction(target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id")) if command.key?("target_kind")
        updated
      end
    end

    #: (message_id: String, workflow_id: String, result: Object?, worker_id: String) -> Object?
    def complete_workflow_command(message_id:, workflow_id:, result:, worker_id:)
      transaction do
        # [DURABABBLE-LEASE-4] Workflow command history commits need the workflow and inbox leases.
        assert_workflow_lease_for_update!(workflow_id:, worker_id:)
        command = lock_inbox_message_for_completion(message_id:, worker_id:)
        next nil unless command

        append_workflow_history_without_transaction(
          workflow_id:,
          kind: "workflow_command_completed",
          name: command["method_name"],
          attempt_id: message_id,
          payload: { "message_id" => message_id, "result" => result },
        )
        updated = complete_inbox_message_without_transaction(message_id:, result:)
        reconcile_target_activation_without_transaction(target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id"))
        updated
      end
    end

    #: (message_id: String, workflow_id: String, error: String, worker_id: String) -> Object?
    def fail_workflow_command(message_id:, workflow_id:, error:, worker_id:)
      transaction do
        # [DURABABBLE-LEASE-4] Workflow command failure history is also a workflow commit.
        assert_workflow_lease_for_update!(workflow_id:, worker_id:)
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
        updated = dead_letter_inbox_message_without_transaction(message_id:, error:)
        reconcile_target_activation_without_transaction(target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id"))
        updated
      end
    end

    #: (target_kind: Object?, target_type: Object?, target_id: Object?) -> Hash[String, Object?]?
    def target_activation(target_kind:, target_type:, target_id:)
      row = execute_params(<<~SQL, [target_kind, target_type, target_id]).first
        SELECT * FROM #{table("target_activations")}
        WHERE target_kind = #{placeholder(1)} AND target_type = #{placeholder(2)} AND target_id = #{placeholder(3)}
      SQL
      decode_row(row) if row
    end

    private

    #: (Object?, workflow_id: String, worker_id: String?, operation: String) -> Object?
    def require_fenced_workflow_update!(result, workflow_id:, worker_id:, operation:)
      result = result #: as untyped
      return result unless worker_id
      return result if result&.affected_rows.to_i == 1

      # [DURABABBLE-LEASE-4] Workflow terminal writes fail when ownership moved or expired.
      raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before #{operation}"
    end

    #: (Object?, workflow_id: String, worker_id: String?) -> Object?
    def require_workflow_completion_update!(result, workflow_id:, worker_id:)
      result = result #: as untyped
      return result if result&.affected_rows.to_i == 1

      message = "workflow #{workflow_id} cannot complete while incomplete durable work remains"
      raise LeaseConflict, "#{message} or the lease expired or moved" if worker_id

      raise Error, message
    end

    #: (target_kind: Object?, workflow_id: Object?, worker_id: String) -> void
    def assert_workflow_inbox_lease_for_update!(target_kind:, workflow_id:, worker_id:)
      return unless target_kind.to_s == "workflow"

      # [DURABABBLE-LEASE-4] Workflow inbox commands run inside the workflow lease boundary.
      assert_workflow_lease_for_update!(workflow_id: workflow_id.to_s, worker_id:)
    end

    #: (workflow_id: String, worker_id: String) -> void
    def assert_workflow_lease_for_update!(workflow_id:, worker_id:)
      return if lock_owned_workflow_for_update(workflow_id:, worker_id:)

      raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before state update"
    end

    #: (name: String, input: Object?) -> String
    def enqueue_workflow(name:, input:)
      raise NotImplementedError
    end

    #: (String, ?worker_id: String?, ?lease_seconds: Numeric) -> Hash[String, Object?]?
    def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60)
      raise NotImplementedError
    end

    #: (Integer?, Integer?) -> Integer
    def normalize_command_id(command_id, position)
      raise NotImplementedError
    end

    #: (workflow_id: String, command_id: Integer, result: Object?) -> Object?
    def record_step_completed_without_transaction(workflow_id:, command_id:, result:)
      raise NotImplementedError
    end

    #: (workflow_id: String, command_id: Integer, error: String, ?payload: Object?) -> Object?
    def record_step_failed_without_transaction(workflow_id:, command_id:, error:, payload: nil)
      raise NotImplementedError
    end

    #: (String, Array[Object?]) -> Array[Hash[String, Object?]]
    def execute_params(sql, params)
      raise NotImplementedError
    end

    #: (String) -> String
    def table(name)
      raise NotImplementedError
    end

    #: (Integer) -> String
    def placeholder(index)
      raise NotImplementedError
    end

    #: (Time?) -> String?
    def timestamp_or_nil(time)
      raise NotImplementedError
    end

    #: (Object?) -> Integer
    def complete_timer_waits(now)
      # [DURABABBLE-WAIT-1] Locked timer completion makes concurrent wakeups observe one winner.
      raise NotImplementedError
    end

    #: (target_kind: Object?, target_type: Object?, target_id: Object?, ?ready_at: Object?) -> Object?
    def upsert_target_activation_without_transaction(target_kind:, target_type:, target_id:, ready_at: nil)
      raise NotImplementedError
    end

    #: (target_kind: String, target_type: String, target_id: String) -> Integer
    def allocate_mailbox_sequence(target_kind:, target_type:, target_id:)
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

    #: (id: String, target_kind: String, target_type: String, target_id: String, sequence: Integer, message_kind: String, method_name: String?, operation_id: String, idempotency_key: String?, shape_hash: String, payload: Object?, ?ready_at: Time?, ?max_attempts: Integer?) -> Object?
    def insert_inbox_message_without_transaction(id:, target_kind:, target_type:, target_id:, sequence:, message_kind:, method_name:, operation_id:, idempotency_key:, shape_hash:, payload:, ready_at: nil, max_attempts: nil)
      raise NotImplementedError
    end

    #: (target_kind: String, target_type: String, target_id: String, limit: Integer) -> Array[Hash[String, Object?]]
    def inbox_claim_rows_for_update(target_kind:, target_type:, target_id:, limit:)
      raise NotImplementedError
    end

    #: (target_kind: Object?, target_type: Object?, target_id: Object?) -> Hash[String, Object?]?
    def inbox_head_for_update(target_kind:, target_type:, target_id:)
      raise NotImplementedError
    end

    #: (message_id: Object?, worker_id: String, lease_seconds: Numeric) -> Object?
    def mark_inbox_row_running_without_transaction(message_id:, worker_id:, lease_seconds:)
      raise NotImplementedError
    end

    #: (command_id: String, worker_id: String?) -> Hash[String, Object?]?
    def lock_object_command_for_completion(command_id:, worker_id:)
      raise NotImplementedError
    end

    #: (object_type: String?, object_id: String?, state: Object?) -> Object?
    def save_object_state(object_type:, object_id:, state:)
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

    #: (workflow_id: String, kind: String, ?command_id: Integer?, ?name: Object?, ?attempt_id: String?, ?payload: Object?, ?error: String?) -> Object?
    def append_workflow_history_without_transaction(workflow_id:, kind:, command_id: nil, name: nil, attempt_id: nil, payload: nil, error: nil)
      raise NotImplementedError
    end

    #: (message_id: String, error: String) -> Object?
    def dead_letter_inbox_message_without_transaction(message_id:, error:)
      raise NotImplementedError
    end

    #: (target_kind: Object?, target_type: Object?, target_id: Object?, ?now: Time) -> Object?
    def reconcile_target_activation_without_transaction(target_kind:, target_type:, target_id:, now: Time.now)
      raise NotImplementedError
    end

    #: (Object?) -> bool
    def activatable_inbox_status?(status)
      status = status #: as untyped
      InboxStatus.activatable?(status)
    end

    #: (message_id: String, target_kind: Object?, target_type: Object?, target_id: Object?, worker_id: String, lease_seconds: Numeric, ?now: Time) -> Object?
    def claim_inbox_message_by_id(message_id:, target_kind:, target_type:, target_id:, worker_id:, lease_seconds:, now: Time.now)
      transaction do
        head = inbox_head_for_update(target_kind:, target_type:, target_id:)
        next unless head&.fetch("id") == message_id

        head = head #: as Hash[String, Object?]
        next unless inbox_row_claimable?(head, now:)

        mark_inbox_row_running_without_transaction(message_id:, worker_id:, lease_seconds:)
        inbox_message(message_id)
      end
    end
  end
end
