# typed: true
# frozen_string_literal: true

module Durababble
  class PostgresStore < SqlStore
    include PostgresMigrations

    # Retry budgets for serialization/deadlock failures. Backoff grows linearly
    # per attempt and is jittered to decorrelate competing transactions.
    # The statement path tolerates more attempts with a coarser step because it
    # wraps individual statements that may legitimately collide repeatedly.
    MAX_SERIALIZATION_RETRY_ATTEMPTS = 5
    SERIALIZATION_RETRY_STEP_SECONDS = 0.001
    MAX_STATEMENT_RETRY_ATTEMPTS = 20
    STATEMENT_RETRY_STEP_SECONDS = 0.05

    #: () -> Object?
    def drop_schema!
      execute_store_query(:drop_schema)
      @migrated = false
    end

    #: (worker_id: String, lease_seconds: Integer, ?workflow_names: Array[String]?, ?worker_pool: String) -> Object?
    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil, worker_pool: "default")
      return if workflow_names&.empty?

      name_filter, name_params = workflow_name_filter(workflow_names, offset: 4)
      row = retry_serialization_failures do
        execute_store_query(:claim_runnable_workflow, [worker_pool, worker_id, lease_seconds] + name_params, name_filter:).first
      end
      typed_row = row #: as untyped
      observe_claim_latency(typed_row, "workflow") if typed_row
      decode_row(typed_row) if typed_row
    end

    #: (workflow_id: String, worker_id: String, lease_seconds: Integer, ?worker_pool: String) -> Object?
    def claim_workflow(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      already_owned = execute_store_query(:claim_workflow_already_owned, [workflow_id, worker_pool, worker_id]).first
      return decode_row(already_owned) if already_owned

      row = execute_store_query(:claim_workflow_update, [workflow_id, worker_pool, worker_id, lease_seconds]).first
      decode_row(row) if row
    end

    #: (workflow_id: String, worker_id: String, lease_seconds: Integer, ?worker_pool: String) -> Object?
    def claim_workflow_for_activation(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      already_owned = execute_store_query(:claim_workflow_already_owned, [workflow_id, worker_pool, worker_id]).first
      return decode_row(already_owned) if already_owned

      row = execute_store_query(:claim_workflow_for_activation_update, [workflow_id, worker_pool, worker_id, lease_seconds]).first
      decode_row(row) if row
    end

    #: (workflow_id: String, worker_id: String, lease_seconds: Integer) -> ActiveRecord::Result
    def heartbeat(workflow_id:, worker_id:, lease_seconds:)
      result = execute_store_query(:heartbeat_workflow, [workflow_id, worker_id, lease_seconds])
      if result.affected_rows.to_i.positive?
        Observability.count("durababble.leases.heartbeats", "durababble.workflow.id" => workflow_id, "durababble.worker.id" => worker_id)
      else
        Observability.count("durababble.leases.conflicts", "durababble.workflow.id" => workflow_id, "durababble.worker.id" => worker_id)
      end
      result
    end

    #: (workflow_id: String, worker_id: String) -> bool
    def workflow_owned?(workflow_id:, worker_id:)
      !!execute_store_query(:workflow_owned, [workflow_id, worker_id]).first
    end

    #: (worker_id: String) -> Object?
    def release_worker_leases!(worker_id:)
      transaction do
        workflows = execute_store_query(:release_workflow_leases, [worker_id]).affected_rows
        outbox = execute_store_query(:release_outbox_leases, [worker_id]).affected_rows
        inbox = execute_store_query(:release_inbox_leases, [worker_id]).affected_rows
        target_activations = execute_store_query(:release_target_activation_leases, [worker_id]).affected_rows
        released = { "workflows" => workflows, "outbox" => outbox, "inbox" => inbox, "target_activations" => target_activations }
        Observability.count("durababble.leases.expired_recovery", { "durababble.worker.id" => worker_id }, by: released.values.sum)
        released
      end
    end

    #: (workflow_id: String, worker_id: String, run_at: Time) -> Object?
    def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
      result = execute_store_query(:schedule_workflow_retry, [workflow_id, worker_id, timestamp(run_at)])
      result.affected_rows.to_i == 1 ? result : nil
    end

    #: (workflow_id: String, ?worker_id: String?) -> bool
    def suspend_workflow(workflow_id:, worker_id: nil)
      result = execute_store_query(:suspend_workflow, [workflow_id, worker_id])
      return true if result.affected_rows == 1

      WorkflowStatus.suspended_or_runnable?(workflow(workflow_id))
    end

    #: (String, ?now: Time) -> Object?
    def make_workflow_due!(workflow_id, now: Time.now)
      execute_store_query(:make_workflow_due, [workflow_id, timestamp(now)])
    end

    #: (workflow_id: String, reason: String) -> Object?
    def request_workflow_cancellation(workflow_id:, reason:)
      transaction do
        row = execute_store_query(:lock_workflow_for_update, [workflow_id]).first
        raise KeyError, "workflow not found: #{workflow_id}" unless row

        decoded = decode_row(row)
        next decoded if terminal_for_cancellation?(decoded)

        first_request = row["cancel_requested_at"].nil?
        if first_request
          execute_store_query(:request_workflow_cancellation, [workflow_id, reason])
        end
        cancel_pending_waits_for_workflow(workflow_id) if first_request

        if first_request && !WorkflowStatus.running?(decoded)
          execute_store_query(:mark_workflow_canceling_for_request, [workflow_id])
        end

        workflow(workflow_id)
      end
    end

    #: (workflow_id: String, ?reason: Object?) -> Hash[String, Object?]
    def request_workflow_termination(workflow_id:, reason: nil)
      error = workflow_termination_error(reason)
      result = transaction do
        row = execute_store_query(:lock_workflow_for_termination, [workflow_id]).first
        raise KeyError, "workflow not found: #{workflow_id}" unless row

        decoded = decode_row(row)
        next decoded if WorkflowStatus.terminal?(decoded)

        execute_store_query(:terminate_workflow, [workflow_id, dump_serialized(nil), error])
        terminate_workflow_dependents(workflow_id, error:)
        append_workflow_history_without_transaction(workflow_id:, kind: "workflow_terminated", payload: { "reason" => error })

        workflow(workflow_id)
      end
      result #: as Hash[String, Object?]
    end

    #: (String) -> Object?
    def workflow_cancellation(workflow_id)
      row = execute_store_query(:workflow_cancellation, [workflow_id]).first
      row&.transform_values(&:itself)
    end

    #: (workflow_id: String) -> Object?
    def mark_workflow_cancellation_delivered(workflow_id:)
      execute_store_query(:mark_workflow_cancellation_delivered, [workflow_id])
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, worker_id: String, lease_seconds: Integer, cursor: Object?) -> Object?
    def heartbeat_step(workflow_id:, worker_id:, lease_seconds:, cursor:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      renewed = transaction do
        workflow = execute_store_query(:heartbeat_step_workflow, [workflow_id, worker_id, lease_seconds]).first
        next nil unless workflow

        serialized_cursor = dump_serialized(cursor)
        step = execute_store_query(:heartbeat_step_row, [workflow_id, command_id, serialized_cursor]).first
        next nil unless step

        execute_store_query(:heartbeat_latest_attempt, [workflow_id, command_id, serialized_cursor])
        workflow
      end
      renewed = renewed #: as untyped
      renewed&.fetch("locked_until")
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?) -> Object?
    def step_heartbeat_cursor(workflow_id:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      row = execute_store_query(:step_heartbeat_cursor, [workflow_id, command_id]).first
      decode_row(row).fetch("heartbeat_cursor") if row
    end

    #: (String, ?worker_pool: String?) -> Object?
    def current_workflow_lease(workflow_id, worker_pool: nil)
      worker_pool_sql = worker_pool ? "AND worker_pool = $2" : ""
      params = worker_pool ? [workflow_id, worker_pool] : [workflow_id]
      row = execute_store_query(:current_workflow_lease, params, worker_pool_sql:).first
      row&.transform_values(&:itself)
    end

    #: (Object?, Object?, ?worker_pool: String) -> Object?
    def current_object_lease(object_type, object_id, worker_pool: "default")
      row = execute_store_query(:current_object_lease, [worker_pool, object_type, object_id]).first
      row&.transform_values(&:itself)
    end

    #: (?now: Time) -> Integer
    def steal_expired_leases!(now: Time.now)
      result = execute_store_query(:steal_expired_leases, [timestamp(now)])
      Observability.count("durababble.leases.expired_recovery", by: result.affected_rows.to_i)
      result.affected_rows
    end

    #: (String, ?worker_id: String?, ?lease_seconds: Integer, ?worker_pool: String) -> Object?
    def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60, worker_pool: "default")
      if worker_id
        claim_workflow(workflow_id:, worker_id:, lease_seconds:, worker_pool:)
      else
        execute_store_query(:mark_workflow_running, [workflow_id, worker_pool])
      end
    end

    #: (String, result: Object?, ?worker_id: String?) -> Object
    def complete_workflow(workflow_id, result:, worker_id: nil)
      update = if worker_id
        execute_store_query(:complete_workflow_with_worker, [workflow_id, dump_serialized(result), worker_id])
      else
        execute_store_query(:complete_workflow, [workflow_id, dump_serialized(result)])
      end
      require_fenced_workflow_update!(update, workflow_id:, worker_id:, operation: "workflow completion")
    end

    #: (String, reason: String, ?result: Object?, ?worker_id: String?) -> Object
    def cancel_workflow(workflow_id, reason:, result: nil, worker_id: nil)
      update = if worker_id
        execute_store_query(:cancel_workflow_with_worker, [workflow_id, dump_serialized(result), reason, worker_id])
      else
        execute_store_query(:cancel_workflow, [workflow_id, dump_serialized(result), reason])
      end
      require_fenced_workflow_update!(update, workflow_id:, worker_id:, operation: "workflow cancellation")
    end

    #: (String, error: String, ?worker_id: String?) -> Object
    def fail_workflow(workflow_id, error:, worker_id: nil)
      update = if worker_id
        execute_store_query(:fail_workflow_with_worker, [workflow_id, error, worker_id])
      else
        execute_store_query(:fail_workflow, [workflow_id, error])
      end
      require_fenced_workflow_update!(update, workflow_id:, worker_id:, operation: "workflow failure")
    end

    #: (workflow_id: String, command_id: Integer, name: String, ?args: Array[Object?], ?kwargs: Hash[Symbol, Object?], ?metadata: Hash[String, Object?], ?worker_id: String?) -> Object?
    def record_step_scheduled(workflow_id:, command_id:, name:, args: [], kwargs: {}, metadata: {}, worker_id: nil)
      payload = { "name" => name, "args" => args, "kwargs" => kwargs }.merge(metadata)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        append_workflow_history_without_transaction(workflow_id:, kind: "step_scheduled", command_id:, name:, payload:)
        execute_store_query(:insert_scheduled_step, [workflow_id, command_id, name])
      end
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, name: String, ?worker_id: String?) -> Object?
    def record_step_started(workflow_id:, name:, command_id: nil, position: nil, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        execute_store_query(:supersede_running_step_attempts, [workflow_id, command_id])
        execute_store_query(:upsert_step_running, [workflow_id, command_id, name])
        attempt_id = SecureRandom.uuid
        execute_store_query(:insert_step_attempt, [attempt_id, workflow_id, command_id, name])
        append_workflow_history_without_transaction(workflow_id:, kind: "step_started", command_id:, name:, attempt_id:)
        attempt_id
      end
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?) -> Integer
    def step_attempt_count_for(workflow_id:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      row = execute_store_query(:step_attempt_count_for, [workflow_id, command_id]).first
      row.fetch("count").to_i
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, error: String, ?worker_id: String?) -> Object?
    def record_step_canceled(workflow_id:, error:, command_id: nil, position: nil, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        execute_store_query(:cancel_step, [workflow_id, command_id, error])
        update_latest_attempt_serialized(workflow_id:, command_id:, status: "canceled", serialized_result: dump_serialized(nil), error:)
        append_workflow_history_without_transaction(workflow_id:, kind: "step_canceled", command_id:, error:)
      end
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, name: String, wait_request: WaitRequest, ?suspend_workflow: bool, ?worker_id: String?) -> Object?
    def record_wait(workflow_id:, name:, wait_request:, command_id: nil, position: nil, suspend_workflow: true, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        execute_store_query(:upsert_waiting_step, [workflow_id, command_id, name, dump_serialized(wait_request.context)])
        wait_id = SecureRandom.uuid
        execute_store_query(:insert_wait, [wait_id, workflow_id, command_id, wait_request.kind, wait_request.event_key, timestamp_or_nil(wait_request.wake_at), dump_serialized(wait_request.context)])
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

    #: (workflow_id: String, key: String, ?poll_interval: Numeric, ?timeout: Numeric) { () -> Object? } -> Object?
    def with_fence(workflow_id:, key:, poll_interval: 0.05, timeout: 10, &block)
      token = SecureRandom.uuid
      inserted = execute_store_query(:insert_fence, [workflow_id, key, token, timeout])
      claimed = inserted.affected_rows == 1
      claimed ||= execute_store_query(:claim_expired_fence, [token, timeout, workflow_id, key]).affected_rows == 1

      if claimed
        begin
          result = block.call
          execute_store_query(:complete_fence, [workflow_id, key, token, dump_serialized(result)])
          return result
        rescue StandardError => e
          execute_store_query(:fail_fence, [workflow_id, key, token, "#{e.class}: #{e.message}"])
          raise
        end
      end

      deadline = Time.now + timeout
      loop do
        row = execute_store_query(:read_fence, [workflow_id, key]).first
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

    #: (workflow_id: String, topic: String, payload: Object?, key: String) -> Object?
    def enqueue_outbox(workflow_id:, topic:, payload:, key:)
      existing = execute_store_query(:outbox_by_key, [key]).first
      return existing.fetch("id") if existing

      id = SecureRandom.uuid
      execute_store_query(:insert_outbox, [id, workflow_id, topic, dump_serialized(payload), key])
      Observability.count("durababble.outbox.pending", "durababble.workflow.id" => workflow_id, "durababble.outbox.topic" => topic)
      execute_store_query(:outbox_by_key, [key]).first.fetch("id")
    end

    #: (worker_id: String, lease_seconds: Integer) -> Object?
    def claim_outbox(worker_id:, lease_seconds:)
      row = retry_serialization_failures do
        transaction do
          candidates = []
          candidates.concat(execute_store_query(:claim_pending_outbox).to_a)
          candidates.concat(execute_store_query(:claim_expired_outbox).to_a)

          candidate = candidates.min_by { |candidate_row| Time.parse(candidate_row.fetch("created_at").to_s) }
          next nil unless candidate

          execute_store_query(:claim_selected_outbox, [candidate.fetch("id"), worker_id, lease_seconds]).first
        end
      end
      typed_row = row #: as untyped
      observe_claim_latency(typed_row, "outbox") if typed_row
      decode_row(typed_row) if typed_row
    end

    #: (String, worker_id: String) -> Object?
    def ack_outbox(outbox_id, worker_id:)
      result = execute_store_query(:ack_outbox, [outbox_id, worker_id])
      Observability.count("durababble.outbox.processed", "durababble.worker.id" => worker_id) if result.affected_rows.to_i.positive?
      result
    end

    #: (object_type: String, object_id: String, state: Object?, ?worker_pool: String) -> Object?
    def save_object_state(object_type:, object_id:, state:, worker_pool: "default")
      execute_store_query(:save_object_state, [worker_pool, object_type, object_id, dump_serialized(state)])
      state
    end

    #: (worker_id: String, lease_seconds: Integer, ?target_kinds: Array[String]?, ?target_types: Array[String]?, ?now: Time, ?worker_pool: String) -> Object?
    def claim_target_activation(worker_id:, lease_seconds:, target_kinds: nil, target_types: nil, now: Time.now, worker_pool: "default")
      return if target_kinds&.empty? || target_types&.empty?

      filter_sql, filter_params = target_activation_filter(target_kinds:, target_types:, offset: 3)
      row = retry_serialization_failures do
        transaction do
          candidates = []
          candidates.concat(execute_store_query(:claim_pending_target_activation, [worker_pool, timestamp(now)] + filter_params, filter_sql:).to_a)
          candidates.concat(execute_store_query(:claim_expired_target_activation, [worker_pool, timestamp(now)] + filter_params, filter_sql:).to_a)
          candidate = candidates.min_by { |candidate_row| Time.parse(candidate_row.fetch("created_at").to_s) }
          next nil unless candidate

          execute_store_query(:claim_selected_target_activation, [worker_pool, candidate.fetch("target_kind"), candidate.fetch("target_type"), candidate.fetch("target_id"), worker_id, lease_seconds]).first
        end
      end
      typed_row = row #: as untyped
      decode_row(typed_row) if typed_row
    end

    #: (target_kind: String, target_type: String, target_id: String, worker_id: String, ?now: Time, ?worker_pool: String) -> Object?
    def complete_target_activation(target_kind:, target_type:, target_id:, worker_id:, now: Time.now, worker_pool: "default")
      transaction do
        activation = execute_store_query(:lock_target_activation_for_completion, [worker_pool, target_kind, target_type, target_id, worker_id]).first
        next nil unless activation

        reconcile_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, now:)
      end
    end

    private

    #: (name: String, input: Object?, status: String, id: String, ?worker_id: String?, ?lease_seconds: Numeric?, ?worker_pool: String) -> String
    def insert_workflow(name:, input:, status:, id:, worker_id: nil, lease_seconds: nil, worker_pool: "default")
      workflow_id = id
      if worker_id
        execute_store_query(:insert_workflow_with_worker, [workflow_id, name, worker_pool, status, dump_serialized(input), worker_id, lease_seconds || 60])
      else
        execute_store_query(:insert_workflow, [workflow_id, name, worker_pool, status, dump_serialized(input)])
      end
      workflow_id
    rescue ActiveRecord::RecordNotUnique
      raise WorkflowAlreadyExists, "workflow #{workflow_id} already exists"
    end

    #: (String) -> Object?
    def cancel_pending_waits_for_workflow(workflow_id)
      execute_store_query(:cancel_pending_waits_for_workflow, [workflow_id])
      execute_store_query(:cancel_waiting_steps_for_workflow, [workflow_id])
      execute_store_query(:cancel_waiting_step_attempts_for_workflow, [workflow_id])
    end

    #: (String, error: String) -> void
    def terminate_workflow_dependents(workflow_id, error:)
      # Called only while request_workflow_termination holds the workflow row lock inside a transaction.
      execute_store_query(:terminate_workflow_waits, [workflow_id])
      execute_store_query(:terminate_workflow_steps, [workflow_id, error])
      execute_store_query(:terminate_workflow_step_attempts, [workflow_id, error])
      execute_store_query(:terminate_workflow_inbox, [workflow_id, error])
      execute_store_query(:terminate_workflow_target_activations, [workflow_id])
    end

    #: (command_id: String, worker_id: String?) -> Object?
    def lock_object_command_for_completion(command_id:, worker_id:)
      if worker_id
        execute_store_query(:lock_inbox_message_for_worker, [command_id, worker_id]).first
      else
        execute_store_query(:lock_inbox_message, [command_id]).first
      end
    end

    #: (message_id: String, worker_id: String) -> Object?
    def lock_inbox_message_for_completion(message_id:, worker_id:)
      execute_store_query(:lock_inbox_message_for_worker, [message_id, worker_id]).first
    end

    #: (command_id: String, worker_id: String?) -> Object?
    def lock_inbox_message_for_failure(command_id:, worker_id:)
      if worker_id
        execute_store_query(:lock_inbox_message_for_worker, [command_id, worker_id]).first
      else
        execute_store_query(:lock_inbox_message, [command_id]).first
      end
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, ?ready_at: Object?) -> Object?
    def upsert_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at: nil)
      ready_timestamp = timestamp_or_nil(ready_at) || timestamp(Time.now)
      execute_store_query(:upsert_target_activation, [worker_pool, target_kind, target_type, target_id, ready_timestamp])
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, ?now: Time) -> Object?
    def reconcile_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, now: Time.now)
      if target_kind == "workflow" && (terminal_error = terminal_workflow_target_error(worker_pool:, workflow_id: target_id))
        dead_letter_terminal_workflow_inbox_without_transaction(
          worker_pool:,
          target_type:,
          target_id:,
          error: terminal_error,
        )
        delete_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:)
        return
      end

      head = execute_store_query(:inbox_head_for_update, [worker_pool, target_kind, target_type, target_id]).first

      if head && !InboxStatus.dead_lettered?(head)
        ready_at = target_activation_ready_at_for(head, now:)
        set_target_activation_pending_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at:)
      else
        delete_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:)
      end
    end

    #: (worker_pool: String, workflow_id: String) -> String?
    def terminal_workflow_target_error(worker_pool:, workflow_id:)
      row = execute_params("SELECT status, error FROM #{table("workflows")} WHERE worker_pool = $1 AND id = $2 FOR UPDATE", [worker_pool, workflow_id]).first
      return unless row && WorkflowStatus.terminal?(row)

      status = row.fetch("status")
      error = row["error"]
      suffix = error.to_s.empty? ? "" : ": #{error}"
      "workflow #{workflow_id} is terminal #{status}#{suffix}"
    end

    #: (worker_pool: String, target_type: String, target_id: String, error: String) -> Object?
    def dead_letter_terminal_workflow_inbox_without_transaction(worker_pool:, target_type:, target_id:, error:)
      execute_params(<<~SQL, [worker_pool, target_type, target_id, error])
        UPDATE #{table("inbox")}
        SET status = 'dead_lettered',
            error = $4,
            locked_by = NULL,
            locked_until = NULL,
            dead_lettered_at = now(),
            updated_at = now()
        WHERE worker_pool = $1 AND target_kind = 'workflow' AND target_type = $2 AND target_id = $3
          AND status IN ('pending', 'failed', 'running')
      SQL
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String) -> Object?
    def delete_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:)
      execute_store_query(:delete_target_activation, [worker_pool, target_kind, target_type, target_id])
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, ready_at: Object?) -> Object?
    def set_target_activation_pending_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at:)
      ready_timestamp = timestamp_or_nil(ready_at) || timestamp(Time.now)
      execute_store_query(:set_target_activation_pending, [worker_pool, target_kind, target_type, target_id, ready_timestamp])
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String) -> Object?
    def allocate_mailbox_sequence(worker_pool:, target_kind:, target_type:, target_id:)
      execute_store_query(:insert_mailbox_sequence, [worker_pool, target_kind, target_type, target_id])
      row = execute_store_query(:mailbox_sequence_for_update, [worker_pool, target_kind, target_type, target_id]).first
      sequence = row.fetch("last_sequence").to_i + 1
      execute_store_query(:update_mailbox_sequence, [worker_pool, target_kind, target_type, target_id, sequence])
      sequence
    end

    #: (String?, worker_pool: String, target_kind: String, target_type: String, target_id: String) -> Object?
    def existing_inbox_message_for_idempotency(idempotency_key, worker_pool:, target_kind:, target_type:, target_id:)
      return unless idempotency_key

      idempotency_hash = inbox_idempotency_hash(idempotency_key, worker_pool:, target_kind:, target_type:, target_id:)
      execute_store_query(:existing_inbox_message_for_idempotency, [idempotency_hash]).first
    end

    #: (String) -> Object?
    def lock_workflow_for_update(workflow_id)
      execute_store_query(:lock_workflow_for_update, [workflow_id]).first
    end

    #: (workflow_id: String, worker_id: String) -> bool
    def lock_owned_workflow_for_update(workflow_id:, worker_id:)
      execute_store_query(:lock_owned_workflow_for_update, [workflow_id, worker_id]).first
    end

    #: (id: String, worker_pool: String, target_kind: String, target_type: String, target_id: String, sequence: Integer, message_kind: String, method_name: String, operation_id: String, idempotency_key: String?, shape_hash: String, payload: Object?, ?ready_at: Object?, ?max_attempts: Integer?) -> Object?
    def insert_inbox_message_without_transaction(id:, worker_pool:, target_kind:, target_type:, target_id:, sequence:, message_kind:, method_name:, operation_id:, idempotency_key:, shape_hash:, payload:, ready_at: nil, max_attempts: nil)
      idempotency_hash = inbox_idempotency_hash(idempotency_key, worker_pool:, target_kind:, target_type:, target_id:)
      execute_store_query(:insert_inbox_message, [id, worker_pool, target_kind, target_type, target_id, sequence, message_kind, method_name, operation_id, idempotency_key, idempotency_hash, shape_hash, dump_serialized(payload), timestamp_or_nil(ready_at), max_attempts])
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, limit: Integer) -> Array[Hash[String, Object?]]
    def inbox_claim_rows_for_update(worker_pool:, target_kind:, target_type:, target_id:, limit:)
      execute_store_query(:inbox_claim_rows_for_update, [worker_pool, target_kind, target_type, target_id, limit])
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String) -> Object?
    def inbox_head_for_update(worker_pool:, target_kind:, target_type:, target_id:)
      execute_store_query(:inbox_head_for_update, [worker_pool, target_kind, target_type, target_id]).first
    end

    #: (message_id: String, worker_id: String, lease_seconds: Integer) -> Object?
    def mark_inbox_row_running_without_transaction(message_id:, worker_id:, lease_seconds:)
      execute_store_query(:mark_inbox_row_running, [message_id, worker_id, lease_seconds])
    end

    #: (message_id: String, result: Object?) -> Object?
    def complete_inbox_message_without_transaction(message_id:, result:)
      execute_store_query(:complete_inbox_message, [message_id, dump_serialized(result)])
    end

    #: (message_id: String, error: String) -> Object?
    def fail_inbox_message_without_transaction(message_id:, error:)
      execute_store_query(:fail_inbox_message, [message_id, error])
    end

    #: (message_id: String, error: String, ready_at: Time) -> Object?
    def retry_inbox_message_without_transaction(message_id:, error:, ready_at:)
      execute_store_query(:retry_inbox_message, [message_id, error, timestamp(ready_at)])
    end

    #: (message_id: String, error: String) -> Object?
    def dead_letter_inbox_message_without_transaction(message_id:, error:)
      execute_store_query(:dead_letter_inbox_message, [message_id, error])
    end

    #: (target_kinds: Array[String]?, target_types: Array[String]?, ?offset: Integer) -> [String, Array[String]]
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

    #: (Time, Integer) -> Integer
    def complete_timer_waits(now, batch_size)
      completed = transaction do
        returning = execute_store_query(:complete_timer_waits, [now, dump_serialized({}), batch_size])
        finish_completed_waits(returning, {})
      end
      completed = completed #: as Integer
      completed
    end

    #: (Object, Hash[String, Object?]) -> Integer
    def finish_completed_waits(returning, payload)
      returning = returning #: as untyped
      rows = returning.map { |row| decode_row(row) }
      rows.each do |wait|
        record_wait_latency(wait)
        context = wait.fetch("context").merge(payload)
        record_step_completed_without_transaction(workflow_id: wait.fetch("workflow_id"), command_id: wait.fetch("position").to_i, result: context)
      end
      mark_waits_workflows_pending(rows)
      Observability.count("durababble.waits.completed", by: rows.length)
      rows.length
    end

    #: (workflow_id: String, command_id: Integer, result: Object?) -> Object?
    def record_step_completed_without_transaction(workflow_id:, command_id:, result:)
      serialized = dump_serialized(result)
      execute_store_query(:complete_step, [workflow_id, command_id, serialized])
      update_latest_attempt_serialized(workflow_id:, command_id:, status: "completed", serialized_result: serialized, error: nil)
      append_workflow_history_without_transaction(workflow_id:, kind: "step_completed", command_id:, payload: result)
    end

    #: (workflow_id: String, command_id: Integer, error: String, ?terminal: bool, ?error_class: String?, ?error_message: String?) -> Object?
    def record_step_failed_without_transaction(workflow_id:, command_id:, error:, terminal: false, error_class: nil, error_message: nil)
      execute_store_query(:fail_step, [workflow_id, command_id, error])
      update_latest_attempt_serialized(workflow_id:, command_id:, status: "failed", serialized_result: dump_serialized(nil), error:)
      payload = step_failure_payload(terminal:, error_class:, error_message:)
      append_workflow_history_without_transaction(workflow_id:, kind: "step_failed", command_id:, payload:, error:)
    end

    #: (workflow_id: String, command_id: Integer, status: String, result: Object?, error: String?) -> Object?
    def update_latest_attempt(workflow_id:, command_id:, status:, result:, error:)
      update_latest_attempt_serialized(workflow_id:, command_id:, status:, serialized_result: dump_serialized(result), error:)
    end

    #: (workflow_id: String, command_id: Integer, status: String, serialized_result: Object?, error: String?) -> Object?
    def update_latest_attempt_serialized(workflow_id:, command_id:, status:, serialized_result:, error:)
      execute_store_query(:update_latest_attempt, [workflow_id, command_id, status, serialized_result, error])
    end

    #: (workflow_id: String, kind: String, ?command_id: Integer?, ?name: String?, ?attempt_id: String?, ?payload: Object?, ?error: String?) -> Integer
    def append_workflow_history_without_transaction(workflow_id:, kind:, command_id: nil, name: nil, attempt_id: nil, payload: nil, error: nil)
      execute_store_query(:lock_workflow_history_workflow, [workflow_id])
      event_index = execute_store_query(:next_workflow_history_event_index, [workflow_id]).first.fetch("event_index").to_i
      execute_store_query(:insert_workflow_history, [workflow_id, event_index, kind, command_id, name, attempt_id, dump_serialized(payload), error])
      event_index
    end

    #: (Integer?, Integer?) -> Integer
    def normalize_command_id(command_id, position)
      id = command_id.nil? ? position : command_id
      raise ArgumentError, "command_id is required" if id.nil?

      id.to_i
    end

    #: (?max_attempts: Integer) { () -> Object? } -> Object?
    def retry_serialization_failures(max_attempts: MAX_SERIALIZATION_RETRY_ATTEMPTS, &block)
      attempts = 0
      begin
        block.call
      rescue ActiveRecord::SerializationFailure
        attempts += 1
        raise if attempts >= max_attempts

        sleep(Backoff.linear(attempts, step: SERIALIZATION_RETRY_STEP_SECONDS))
        retry
      end
    end

    #: (String) -> untyped
    def execute(sql)
      attempts = 0
      begin
        with_connection do |active_record_connection|
          active_record_connection.exec_query(sql)
        end
      rescue ActiveRecord::SerializationFailure, ActiveRecord::Deadlocked
        attempts += 1
        raise if attempts >= MAX_STATEMENT_RETRY_ATTEMPTS

        sleep(Backoff.linear(attempts, step: STATEMENT_RETRY_STEP_SECONDS))
        retry
      end
    end

    #: () -> Symbol
    def store_query_prefix
      :pg
    end

    #: (String, Array[Object?]) -> untyped
    def execute_store_query_sql(sql, params)
      with_connection do |active_record_connection|
        active_record_connection.exec_query(sql, "Durababble SQL", params, prepare: false)
      end
    end

    #: (String, Array[Object?]) -> untyped
    def execute_params(sql, params)
      execute_store_query_sql(sql, params)
    end

    #: (**Object?) { () -> Object? } -> Object?
    def transaction(**options, &block)
      retry_serialization_failures { super(**options, &block) }
    end

    #: (Array[String]?, ?offset: Integer) -> [String, Array[String]]
    def workflow_name_filter(workflow_names, offset: 1)
      return ["", []] unless workflow_names

      ["AND name IN (#{postgres_placeholders(offset, workflow_names.length)})", workflow_names]
    end

    #: (Integer, Integer) -> Object?
    def postgres_placeholders(offset, count)
      count.times.map { |index| "$#{offset + index}" }.join(", ")
    end

    #: (Integer) -> Object?
    def placeholder(index)
      "$#{index}"
    end

    #: (String) -> Object?
    def table(name)
      "#{quoted_schema}.#{quote_column_name(name.to_s)}"
    end

    #: () -> Object?
    def quoted_schema
      quote_column_name(schema.to_s)
    end

    #: (Object?) -> Object?
    def dump_serialized(value)
      "\\x#{SERIALIZER.dump(value).unpack1("H*")}"
    end

    #: (Object?) -> Object?
    def load_serialized(value)
      return if value.nil?

      bytes = if value.is_a?(String) && value.start_with?("\\x")
        [value.delete_prefix("\\x")].pack("H*")
      else
        value
      end
      SERIALIZER.load(bytes)
    end

    #: (Time | String) -> Object?
    def timestamp(time)
      return time if time.is_a?(String)

      time.utc.iso8601(6)
    end

    #: (Object?) -> Object?
    def timestamp_or_nil(time)
      time = time #: as untyped
      time ? timestamp(time) : nil
    end
  end
end
