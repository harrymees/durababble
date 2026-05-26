# typed: true
# frozen_string_literal: true

module Durababble
  class SqlStore < Store
    TIMER_WAKE_BATCH_SIZE = 100

    #: (name: String, input: Object?, ?worker_pool: String) -> String
    def enqueue_workflow(name:, input:, worker_pool: "default")
      insert_workflow(name:, input:, status: "pending", worker_pool:)
    end

    #: (name: String, input: Object?, ?worker_id: String?, ?lease_seconds: Numeric, ?worker_pool: String) -> String
    def create_workflow(name:, input:, worker_id: nil, lease_seconds: 60, worker_pool: "default")
      insert_workflow(name:, input:, status: "running", worker_id:, lease_seconds:, worker_pool:)
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, result: Object?, ?worker_id: String?) -> Object?
    def record_step_completed(workflow_id:, result:, command_id: nil, position: nil, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        record_step_completed_without_transaction(workflow_id:, command_id:, result:)
      end
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, error: String, ?worker_id: String?, ?terminal: bool, ?error_class: String?, ?error_message: String?) -> Object?
    def record_step_failed(workflow_id:, error:, command_id: nil, position: nil, worker_id: nil, terminal: false, error_class: nil, error_message: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        record_step_failed_without_transaction(workflow_id:, command_id:, error:, terminal:, error_class:, error_message:)
      end
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, error: String, worker_id: String, run_at: Time) -> Object?
    def record_step_failed_and_schedule_retry(workflow_id:, error:, worker_id:, run_at:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:)
        record_step_failed_without_transaction(workflow_id:, command_id:, error:, terminal: false)
        scheduled = schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
        raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before workflow retry scheduling" unless scheduled

        scheduled
      end
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

    #: (object_type: String, object_id: String, ?worker_pool: String) -> Object?
    def object_state(object_type:, object_id:, worker_pool: "default")
      state = object_state_entry(worker_pool:, object_type:, object_id:)
      state.equal?(Store::NO_OBJECT_STATE) ? nil : state
    end

    #: (object_type: String, object_id: String, ?worker_pool: String) -> Object?
    def object_state_entry(object_type:, object_id:, worker_pool: "default")
      row = execute_store_query(:object_state, [worker_pool, object_type, object_id]).first
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

    #: (?now: Time, ?batch_size: Integer) -> Integer
    def wake_due_timers(now: Time.now, batch_size: TIMER_WAKE_BATCH_SIZE)
      batch_size = Integer(batch_size)
      raise ArgumentError, "batch_size must be positive" unless batch_size.positive?

      total = 0
      timestamp = timestamp_or_nil(now) || now
      loop do
        completed = complete_timer_waits(timestamp, batch_size)
        total += completed
        break if completed < batch_size
      end
      total
    end

    # Flip every workflow whose timer just fired back to pending in a single statement.
    # Called once per wake batch instead of once per wait to avoid an N+1 of single-row UPDATEs.
    #: (Array[Hash[String, Object?]]) -> void
    def mark_waits_workflows_pending(waits)
      workflow_ids = waits.map { |wait| wait.fetch("workflow_id") }.uniq
      return if workflow_ids.empty?

      placeholders = workflow_ids.each_index.map { |index| placeholder(index + 1) }.join(", ")
      execute_store_query(:mark_waits_workflows_pending, workflow_ids, placeholders:)
    end

    #: (String) -> Array[Hash[String, Object?]]
    def waits_for(workflow_id)
      execute_store_query(:waits_for_workflow, [workflow_id])
        .map { |row| decode_row(row) }
    end

    #: (String) -> Hash[String, Object?]?
    def outbox_message(outbox_id)
      row = execute_store_query(:outbox_message, [outbox_id]).first
      decode_row(row) if row
    end

    #: (target_kind: String, target_type: String, target_id: String, message_kind: String, ?method_name: String?, ?payload: Object?, ?idempotency_key: String?, ?ready_at: Time?, ?max_attempts: Integer?, ?worker_pool: String) -> String
    def enqueue_inbox_message(target_kind:, target_type:, target_id:, message_kind:, method_name: nil, payload: {}, idempotency_key: nil, ready_at: nil, max_attempts: nil, worker_pool: "default")
      shape_hash = inbox_shape_hash(worker_pool:, target_kind:, target_type:, target_id:, message_kind:, method_name:, payload:)
      result = transaction do
        existing = existing_inbox_message_for_idempotency(idempotency_key, worker_pool:, target_kind:, target_type:, target_id:)
        if existing
          raise IdempotencyKeyConflict, "idempotency key #{idempotency_key} already used for a different inbox message" unless existing.fetch("shape_hash") == shape_hash

          upsert_target_activation_without_transaction(
            worker_pool: row_worker_pool(existing),
            target_kind: existing.fetch("target_kind"),
            target_type: existing.fetch("target_type"),
            target_id: existing.fetch("target_id"),
            ready_at: existing["ready_at"],
          ) if activatable_inbox_status?(existing.fetch("status"))
          next existing.fetch("id")
        end

        sequence = allocate_mailbox_sequence(worker_pool:, target_kind:, target_type:, target_id:)
        id = SecureRandom.uuid
        insert_inbox_message_without_transaction(
          id:,
          worker_pool:,
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
        upsert_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at:)
        id
      end
      result #: as String
    end

    #: (workflow_id: String, workflow_name: String, method_name: String, payload: Object?, ?idempotency_key: String?) -> String
    def enqueue_workflow_command(workflow_id:, workflow_name:, method_name:, payload:, idempotency_key: nil)
      result = transaction do
        workflow = lock_workflow_for_update(workflow_id)
        raise KeyError, "workflow not found: #{workflow_id}" unless workflow

        decoded_workflow = decode_row(workflow)
        worker_pool = row_worker_pool(decoded_workflow)
        target_kind = "workflow"
        message_kind = "workflow_command"
        shape_hash = inbox_shape_hash(worker_pool:, target_kind:, target_type: workflow_name, target_id: workflow_id, message_kind:, method_name:, payload:)
        existing = existing_inbox_message_for_idempotency(idempotency_key, worker_pool:, target_kind:, target_type: workflow_name, target_id: workflow_id)
        if existing
          raise IdempotencyKeyConflict, "idempotency key #{idempotency_key} already used for a different inbox message" unless existing.fetch("shape_hash") == shape_hash

          upsert_target_activation_without_transaction(
            worker_pool: row_worker_pool(existing),
            target_kind: existing.fetch("target_kind"),
            target_type: existing.fetch("target_type"),
            target_id: existing.fetch("target_id"),
            ready_at: existing["ready_at"],
          ) if activatable_inbox_status?(existing.fetch("status"))
          next existing.fetch("id")
        end

        raise Error, "workflow #{workflow_id} is terminal" if terminal_for_cancellation?(decoded_workflow)

        sequence = allocate_mailbox_sequence(worker_pool:, target_kind:, target_type: workflow_name, target_id: workflow_id)
        id = SecureRandom.uuid
        insert_inbox_message_without_transaction(
          id:,
          worker_pool:,
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
        upsert_target_activation_without_transaction(worker_pool:, target_kind:, target_type: workflow_name, target_id: workflow_id)
        id
      end
      result #: as String
    end

    #: (target_kind: String, target_type: String, target_id: String, worker_id: String, ?lease_seconds: Numeric, ?limit: Integer, ?now: Time, ?worker_pool: String) -> Array[Hash[String, Object?]]
    def claim_inbox_messages(target_kind:, target_type:, target_id:, worker_id:, lease_seconds: 60, limit: 1, now: Time.now, worker_pool: "default")
      result = transaction do
        rows = inbox_claim_rows_for_update(worker_pool:, target_kind:, target_type:, target_id:, limit:)
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
      row = execute_store_query(:inbox_message, [message_id]).first
      decode_row(row) if row
    end

    #: (target_kind: String, target_type: String, target_id: String, ?worker_pool: String) -> Array[Hash[String, Object?]]
    def inbox_messages_for(target_kind:, target_type:, target_id:, worker_pool: "default")
      execute_store_query(:inbox_messages_for, [worker_pool, target_kind, target_type, target_id]).map { |row| decode_row(row) }
    end

    #: (command_id: String, worker_id: String, ?lease_seconds: Numeric) -> Hash[String, Object?]?
    def claim_object_command(command_id:, worker_id:, lease_seconds: 60)
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

    #: (command_id: String, result: Object?, ?object_type: String?, ?object_id: String?, ?state: Object?, ?worker_id: String?) -> Object?
    def complete_object_command(command_id:, result:, object_type: nil, object_id: nil, state: Store::NO_OBJECT_STATE, worker_id: nil)
      transaction do
        command = lock_object_command_for_completion(command_id:, worker_id:)
        next nil unless command

        save_object_state(worker_pool: row_worker_pool(command), object_type:, object_id:, state:) unless state.equal?(Store::NO_OBJECT_STATE)
        updated = complete_inbox_message_without_transaction(message_id: command_id, result:)
        reconcile_target_activation_without_transaction(worker_pool: row_worker_pool(command), target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id")) if command.key?("target_kind")
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
        reconcile_target_activation_without_transaction(worker_pool: row_worker_pool(command), target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id")) if command.key?("target_kind")
        updated
      end
    end

    #: (command_id: String, error: String, worker_id: String, ready_at: Time) -> Object?
    def retry_object_command(command_id:, error:, worker_id:, ready_at:)
      transaction do
        command = lock_inbox_message_for_failure(command_id:, worker_id:)
        next nil unless command

        updated = retry_inbox_message_without_transaction(message_id: command_id, error:, ready_at:)
        reconcile_target_activation_without_transaction(worker_pool: row_worker_pool(command), target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id")) if command.key?("target_kind")
        updated
      end
    end

    #: (message_id: String, workflow_id: String, result: Object?, worker_id: String) -> Object?
    def complete_workflow_command(message_id:, workflow_id:, result:, worker_id:)
      transaction do
        command = lock_inbox_message_for_completion(message_id:, worker_id:)
        next nil unless command

        workflow = lock_workflow_for_update(workflow_id)
        if workflow && WorkflowStatus.terminal?(decode_row(workflow))
          updated = dead_letter_inbox_message_without_transaction(message_id:, error: "workflow #{workflow_id} is #{workflow.fetch("status")}")
          reconcile_target_activation_without_transaction(worker_pool: row_worker_pool(command), target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id"))
          next updated
        end

        append_workflow_history_without_transaction(
          workflow_id:,
          kind: "workflow_command_completed",
          name: command["method_name"],
          attempt_id: message_id,
          payload: { "message_id" => message_id, "result" => result },
        )
        updated = complete_inbox_message_without_transaction(message_id:, result:)
        reconcile_target_activation_without_transaction(worker_pool: row_worker_pool(command), target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id"))
        updated
      end
    end

    #: (message_id: String, workflow_id: String, error: String, worker_id: String) -> Object?
    def fail_workflow_command(message_id:, workflow_id:, error:, worker_id:)
      transaction do
        command = lock_inbox_message_for_failure(command_id: message_id, worker_id:)
        next nil unless command

        workflow = lock_workflow_for_update(workflow_id)
        if workflow && WorkflowStatus.terminal?(decode_row(workflow))
          updated = dead_letter_inbox_message_without_transaction(message_id:, error: "workflow #{workflow_id} is #{workflow.fetch("status")}")
          reconcile_target_activation_without_transaction(worker_pool: row_worker_pool(command), target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id"))
          next updated
        end

        append_workflow_history_without_transaction(
          workflow_id:,
          kind: "workflow_command_failed",
          name: command["method_name"],
          attempt_id: message_id,
          payload: { "message_id" => message_id },
          error:,
        )
        updated = dead_letter_inbox_message_without_transaction(message_id:, error:)
        reconcile_target_activation_without_transaction(worker_pool: row_worker_pool(command), target_kind: command.fetch("target_kind"), target_type: command.fetch("target_type"), target_id: command.fetch("target_id"))
        updated
      end
    end

    #: (target_kind: Object?, target_type: Object?, target_id: Object?, ?worker_pool: String) -> Hash[String, Object?]?
    def target_activation(target_kind:, target_type:, target_id:, worker_pool: "default")
      row = execute_store_query(:target_activation, [worker_pool, target_kind, target_type, target_id]).first
      decode_row(row) if row
    end

    private

    #: (Object?, workflow_id: String, worker_id: String?, operation: String) -> Object?
    def require_fenced_workflow_update!(result, workflow_id:, worker_id:, operation:)
      result = result #: as untyped
      return result unless worker_id
      return result if result&.affected_rows.to_i == 1

      raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before #{operation}"
    end

    #: (terminal: bool, error_class: String?, error_message: String?) -> Hash[String, Object?]?
    def step_failure_payload(terminal:, error_class:, error_message:)
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

    #: (name: String, input: Object?, status: String, ?worker_id: String?, ?lease_seconds: Numeric?, ?worker_pool: String) -> String
    def insert_workflow(name:, input:, status:, worker_id: nil, lease_seconds: nil, worker_pool: "default")
      raise NotImplementedError
    end

    #: (String, ?worker_id: String?, ?lease_seconds: Numeric, ?worker_pool: String) -> Hash[String, Object?]?
    def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60, worker_pool: "default")
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
      raise NotImplementedError
    end

    #: (workflow_id: String, command_id: Integer, result: Object?) -> Object?
    def record_step_completed_without_transaction(workflow_id:, command_id:, result:)
      raise NotImplementedError
    end

    #: (workflow_id: String, command_id: Integer, error: String, ?terminal: bool, ?error_class: String?, ?error_message: String?) -> Object?
    def record_step_failed_without_transaction(workflow_id:, command_id:, error:, terminal: false, error_class: nil, error_message: nil)
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

    #: (Object?, Integer) -> Integer
    def complete_timer_waits(now, batch_size)
      raise NotImplementedError
    end

    #: (worker_pool: String, target_kind: Object?, target_type: Object?, target_id: Object?, ?ready_at: Object?) -> Object?
    def upsert_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at: nil)
      raise NotImplementedError
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String) -> Integer
    def allocate_mailbox_sequence(worker_pool:, target_kind:, target_type:, target_id:)
      raise NotImplementedError
    end

    #: (String?, worker_pool: String, target_kind: String, target_type: String, target_id: String) -> Hash[String, Object?]?
    def existing_inbox_message_for_idempotency(idempotency_key, worker_pool:, target_kind:, target_type:, target_id:)
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

    #: (message_id: Object?, worker_id: String, lease_seconds: Numeric) -> Object?
    def mark_inbox_row_running_without_transaction(message_id:, worker_id:, lease_seconds:)
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

    #: (workflow_id: String, kind: String, ?command_id: Integer?, ?name: Object?, ?attempt_id: String?, ?payload: Object?, ?error: String?) -> Object?
    def append_workflow_history_without_transaction(workflow_id:, kind:, command_id: nil, name: nil, attempt_id: nil, payload: nil, error: nil)
      raise NotImplementedError
    end

    #: (message_id: String, error: String) -> Object?
    def dead_letter_inbox_message_without_transaction(message_id:, error:)
      raise NotImplementedError
    end

    #: (worker_pool: String, target_kind: Object?, target_type: Object?, target_id: Object?, ?now: Time) -> Object?
    def reconcile_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, now: Time.now)
      raise NotImplementedError
    end

    #: (Object?) -> bool
    def activatable_inbox_status?(status)
      status = status #: as untyped
      InboxStatus.activatable?(status)
    end

    #: (message_id: String, worker_pool: String, target_kind: Object?, target_type: Object?, target_id: Object?, worker_id: String, lease_seconds: Numeric, ?now: Time) -> Object?
    def claim_inbox_message_by_id(message_id:, worker_pool:, target_kind:, target_type:, target_id:, worker_id:, lease_seconds:, now: Time.now)
      transaction do
        head = inbox_head_for_update(worker_pool:, target_kind:, target_type:, target_id:)
        next unless head&.fetch("id") == message_id

        head = head #: as Hash[String, Object?]
        next unless inbox_row_claimable?(head, now:)

        mark_inbox_row_running_without_transaction(message_id:, worker_id:, lease_seconds:)
        inbox_message(message_id)
      end
    end
  end
end
