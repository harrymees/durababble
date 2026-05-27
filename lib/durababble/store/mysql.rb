# typed: true
# frozen_string_literal: true

module Durababble
  class MysqlStore < SqlStore
    include MysqlMigrations

    # Retry budget for transactions that hit a retryable error (deadlock /
    # lock-wait timeout). Backoff grows linearly per attempt and is jittered.
    MAX_TRANSACTION_RETRY_ATTEMPTS = 5
    TRANSACTION_RETRY_STEP_SECONDS = 0.01

    class << self
      #: (uri: Object, schema: String) -> Store
      def connect(uri:, schema:)
        Store.connect(database_url: uri.to_s, schema:)
      end
    end

    #: () -> Object?
    def drop_schema!
      ["target_activations", "inbox", "mailbox_sequences", "object_wakeups", "durable_objects", "waits", "outbox", "fences", "step_attempts", "steps", "workflow_history", "workflows"].each { |name| execute_store_query(:drop_table, [], table_name: name) }
      @migrated = false
    end

    #: (String, ?worker_id: String?, ?lease_seconds: Integer, ?worker_pool: String) -> Object?
    def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60, worker_pool: "default")
      if worker_id
        execute_store_query(:mark_workflow_running_with_worker, [worker_id, lease_seconds, workflow_id, worker_pool])
      else
        execute_store_query(:mark_workflow_running, [workflow_id, worker_pool])
      end
    end

    #: (worker_id: String, lease_seconds: Integer, ?workflow_names: Array[String]?, ?worker_pool: String, ?excluding_workflow_ids: Array[String]?) -> Object?
    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil, worker_pool: "default", excluding_workflow_ids: nil)
      return if workflow_names&.empty?

      transaction do
        name_sql, name_params = workflow_name_filter(workflow_names)
        exclusion_sql, exclusion_params = workflow_exclusion_filter(excluding_workflow_ids)
        workflow_sql = "#{name_sql} #{exclusion_sql}"
        workflow_params = name_params + exclusion_params
        candidates = []
        candidates.concat(execute_store_query(:claim_pending_workflow, [worker_pool] + workflow_params, name_sql: workflow_sql).to_a)
        candidates.concat(execute_store_query(:claim_failed_workflow, [worker_pool] + workflow_params, name_sql: workflow_sql).to_a)
        candidates.concat(execute_store_query(:claim_canceling_workflow, [worker_pool] + workflow_params, name_sql: workflow_sql).to_a)
        candidates.concat(execute_store_query(:claim_expired_workflow, [worker_pool] + workflow_params, name_sql: workflow_sql).to_a)
        candidate = candidates.min_by { |candidate_row| candidate_row.fetch("created_at").to_s }
        next unless candidate

        updated = execute_store_query(:claim_selected_workflow, [worker_id, lease_seconds, candidate.fetch("id"), worker_pool])
        next unless updated.affected_rows == 1

        claimed = workflow(candidate.fetch("id"))
        observe_claim_latency(claimed, "workflow")
        claimed
      end
    end

    #: (workflow_id: String, worker_id: String, lease_seconds: Integer, ?worker_pool: String) -> Object?
    def claim_workflow(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      already_owned = execute_store_query(:claim_workflow_already_owned, [workflow_id, worker_pool, worker_id]).first
      return decode_row(already_owned) if already_owned

      transaction do
        row = execute_store_query(:claim_workflow_lock, [workflow_id, worker_pool, worker_id]).first
        next unless row

        execute_store_query(:claim_workflow_update, [worker_id, lease_seconds, workflow_id, worker_pool])
        workflow(workflow_id)
      end
    end

    #: (workflow_id: String, worker_id: String, lease_seconds: Integer, ?worker_pool: String) -> Object?
    def claim_workflow_for_activation(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      already_owned = execute_store_query(:claim_workflow_already_owned, [workflow_id, worker_pool, worker_id]).first
      return decode_row(already_owned) if already_owned

      transaction do
        row = execute_store_query(:claim_workflow_for_activation_lock, [workflow_id, worker_pool, worker_id]).first
        next unless row

        execute_store_query(:claim_workflow_for_activation_update, [worker_id, lease_seconds, workflow_id, worker_pool])
        workflow(workflow_id)
      end
    end

    #: (workflow_id: String, worker_id: String, lease_seconds: Integer) -> ActiveRecord::Result
    def heartbeat(workflow_id:, worker_id:, lease_seconds:)
      execute_store_query(:heartbeat_workflow, [lease_seconds, workflow_id, worker_id])
      owned = workflow_owned?(workflow_id:, worker_id:)
      if owned
        Observability.count("durababble.leases.heartbeats", "durababble.workflow.id" => workflow_id, "durababble.worker.id" => worker_id)
      else
        Observability.count("durababble.leases.conflicts", "durababble.workflow.id" => workflow_id, "durababble.worker.id" => worker_id)
      end
      ActiveRecord::Result.empty(affected_rows: owned ? 1 : 0)
    end

    #: (worker_id: String) -> Object?
    def release_worker_leases!(worker_id:)
      transaction do
        workflow_index = index_name("workflows", "worker_lease")
        workflows = execute_store_query(:count_workflow_leases, [worker_id], index: workflow_index).first.fetch("count").to_i
        execute_store_query(:release_workflow_leases, [worker_id], index: workflow_index)
        outbox_index = index_name("outbox", "worker_lease")
        outbox = execute_store_query(:count_outbox_leases, [worker_id], index: outbox_index).first.fetch("count").to_i
        execute_store_query(:release_outbox_leases, [worker_id], index: outbox_index)
        inbox_index = index_name("inbox", "worker_lease")
        inbox = execute_store_query(:count_inbox_leases, [worker_id], index: inbox_index).first.fetch("count").to_i
        execute_store_query(:release_inbox_leases, [worker_id], index: inbox_index)
        target_activation_index = index_name("target_activations", "worker_lease")
        target_activations = execute_store_query(:count_target_activation_leases, [worker_id], index: target_activation_index).first.fetch("count").to_i
        execute_store_query(:release_target_activation_leases, [worker_id], index: target_activation_index)
        objects = execute_store_query(:release_worker_object_leases, [worker_id]).affected_rows.to_i
        released = { "workflows" => workflows, "outbox" => outbox, "inbox" => inbox, "target_activations" => target_activations, "durable_objects" => objects }
        Observability.count("durababble.leases.expired_recovery", { "durababble.worker.id" => worker_id }, by: released.values.sum)
        released
      end
    end

    #: (workflow_id: String, worker_id: String, run_at: Time) -> Object?
    def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
      result = execute_store_query(:schedule_workflow_retry, [run_at, workflow_id, worker_id])
      result.affected_rows.to_i == 1 ? result : nil
    end

    #: (workflow_id: String, ?worker_id: String?) -> bool
    def suspend_workflow(workflow_id:, worker_id: nil)
      result = execute_store_query(:suspend_workflow, [workflow_id, workflow_id, worker_id, worker_id])
      return true if result.affected_rows == 1

      WorkflowStatus.suspended_or_runnable?(workflow(workflow_id))
    end

    #: (String, ?now: Time) -> Object?
    def make_workflow_due!(workflow_id, now: Time.now)
      execute_store_query(:make_workflow_due, [now, workflow_id])
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
          execute_store_query(:request_workflow_cancellation, [reason, workflow_id])
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

        execute_store_query(:terminate_workflow, [dump_serialized(nil), error, workflow_id])
        terminate_workflow_dependents(workflow_id, error:)
        append_workflow_history_without_transaction(workflow_id:, kind: "workflow_terminated", payload: { "reason" => error })

        workflow(workflow_id)
      end
      result #: as Hash[String, Object?]
    end

    #: (String) -> Object?
    def workflow_cancellation(workflow_id)
      execute_store_query(:workflow_cancellation, [workflow_id]).first
    end

    #: (workflow_id: String) -> Object?
    def mark_workflow_cancellation_delivered(workflow_id:)
      execute_store_query(:mark_workflow_cancellation_delivered, [workflow_id])
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, worker_id: String, lease_seconds: Integer, cursor: Object?) -> Object?
    def heartbeat_step(workflow_id:, worker_id:, lease_seconds:, cursor:, command_id: nil, position: nil)
      command_id = normalize_command_id(command_id, position)
      renewed = transaction do
        renewal = execute_store_query(:heartbeat_step_workflow, [lease_seconds, workflow_id, worker_id])
        next nil unless renewal.affected_rows == 1

        serialized_cursor = dump_serialized(cursor)
        step = execute_store_query(:running_step_exists, [workflow_id, command_id]).first
        next nil unless step

        execute_store_query(:heartbeat_step_row, [serialized_cursor, workflow_id, command_id])

        execute_store_query(:heartbeat_latest_attempt, [serialized_cursor, workflow_id, command_id])
        execute_store_query(:workflow_locked_until, [workflow_id]).first
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
      _ = worker_pool
      execute_store_query(:current_workflow_lease, [workflow_id]).first
    end

    #: (String, String, ?worker_pool: String?) -> Hash[String, Object?]?
    def current_object_lease(object_type, object_id, worker_pool: nil)
      _ = worker_pool
      execute_store_query(:current_object_lease, [object_type, object_id]).first
    end

    # MySQL has no RETURNING, so claim is split in two: ensure the
    # `durable_objects` row exists (idempotent INSERT IGNORE), then run a
    # conditional UPDATE whose WHERE gates on (free OR expired OR same
    # worker). The UPDATE's affected_rows is the unambiguous win/loss signal
    # — no follow-up SELECT. A single-statement upsert won't do here because
    # the trilogy adapter forces CLIENT_FOUND_ROWS, which makes a rejected
    # "matched but unchanged" branch indistinguishable from a winning fresh
    # insert (both report affected_rows = 1).
    #: (worker_pool: String, object_type: String, object_id: String, worker_id: String, ?lease_seconds: Numeric) -> Hash[String, Object?]?
    def claim_object_lease(worker_pool:, object_type:, object_id:, worker_id:, lease_seconds: 60)
      Store.validate_positive_lease_seconds!(lease_seconds)
      execute_store_query(:ensure_object_row, [worker_pool, object_type, object_id])
      result = execute_store_query(:claim_object_lease, [worker_id, lease_seconds.to_i, object_type, object_id, worker_id])
      return unless result.affected_rows.to_i.positive?

      { "worker_pool" => worker_pool, "object_type" => object_type, "object_id" => object_id, "worker_id" => worker_id }
    end

    # Conditional renew. MySQL reports affected_rows from the UPDATE; 0 means the lease
    # has been lost (evicted, stolen, or released). Object identity is global —
    # `worker_pool` isn't part of the lease key.
    #: (object_type: String, object_id: String, worker_id: String, ?lease_seconds: Numeric) -> bool
    def renew_object_lease(object_type:, object_id:, worker_id:, lease_seconds: 60)
      Store.validate_positive_lease_seconds!(lease_seconds)
      result = execute_store_query(:renew_object_lease, [lease_seconds.to_i, object_type, object_id, worker_id])
      if result.affected_rows.to_i.positive?
        Observability.count("durababble.leases.heartbeats", "durababble.object.type" => object_type, "durababble.object.id" => object_id, "durababble.worker.id" => worker_id)
        true
      else
        Observability.count("durababble.leases.conflicts", "durababble.object.type" => object_type, "durababble.object.id" => object_id, "durababble.worker.id" => worker_id)
        false
      end
    end

    #: (object_type: String, object_id: String, worker_id: String) -> bool
    def release_object_lease(object_type:, object_id:, worker_id:)
      result = execute_store_query(:release_object_lease, [object_type, object_id, worker_id])
      result.affected_rows.to_i.positive?
    end

    #: (?now: Time) -> Integer
    def steal_expired_leases!(now: Time.now)
      expired = execute_store_query(:count_expired_workflow_leases, [now]).first.fetch("count").to_i
      execute_store_query(:steal_expired_leases, [now])
      object_result = execute_store_query(:steal_expired_object_leases, [now])
      stolen = expired + object_result.affected_rows.to_i
      Observability.count("durababble.leases.expired_recovery", by: stolen)
      stolen
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

    #: (workflow_id: String, command_id: Integer, result: Object?) -> Object?
    def record_step_completed_without_transaction(workflow_id:, command_id:, result:)
      serialized = dump_step_output(workflow_id:, command_id:, result:)
      execute_store_query(:complete_step, [serialized, workflow_id, command_id])
      update_latest_attempt_serialized(workflow_id:, command_id:, status: "completed", serialized_result: serialized, error: nil)
      append_workflow_history_without_transaction(workflow_id:, kind: "step_completed", command_id:, payload: result)
    end

    #: (String, result: Object?, ?worker_id: String?) -> Object
    def complete_workflow(workflow_id, result:, worker_id: nil)
      serialized_result = dump_workflow_result(workflow_id:, result:)
      transaction do
        update = if worker_id
          execute_store_query(:complete_workflow_with_worker, [serialized_result, workflow_id, worker_id])
        else
          execute_store_query(:complete_workflow, [serialized_result, workflow_id, workflow_id, workflow_id, workflow_id])
        end
        require_workflow_completion_update!(update, workflow_id:, worker_id:)
        cancel_live_workflow_dependents(workflow_id)
        update
      end
    end

    #: (String, reason: String, ?result: Object?, ?worker_id: String?) -> Object
    def cancel_workflow(workflow_id, reason:, result: nil, worker_id: nil)
      serialized_result = dump_workflow_result(workflow_id:, result:, context: "cancellation result")
      finalize_terminal_workflow_update!(workflow_id:, worker_id:, operation: "workflow cancellation") do
        if worker_id
          execute_store_query(:cancel_workflow_with_worker, [serialized_result, reason, reason, workflow_id, worker_id])
        else
          execute_store_query(:cancel_workflow, [serialized_result, reason, reason, workflow_id])
        end
      end
    end

    #: (String, error: String, ?worker_id: String?) -> Object
    def fail_workflow(workflow_id, error:, worker_id: nil)
      finalize_terminal_workflow_update!(workflow_id:, worker_id:, operation: "workflow failure", failure_error: error) do
        if worker_id
          execute_store_query(:fail_workflow_with_worker, [error, workflow_id, worker_id])
        else
          execute_store_query(:fail_workflow, [error, workflow_id])
        end
      end
    end

    #: (String, String) -> Object?
    def execute_fail_live_steps_for_workflow(workflow_id, error)
      execute_store_query(:fail_live_steps_for_workflow, [error, workflow_id])
    end

    #: (String, String) -> Object?
    def execute_fail_live_step_attempts_for_workflow(workflow_id, error)
      execute_store_query(:fail_live_step_attempts_for_workflow, [error, workflow_id])
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, error: String, ?worker_id: String?) -> Object?
    def record_step_canceled(workflow_id:, error:, command_id: nil, position: nil, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        execute_store_query(:cancel_step, [error, workflow_id, command_id])
        update_latest_attempt_serialized(workflow_id:, command_id:, status: "canceled", serialized_result: dump_serialized(nil), error:)
        append_workflow_history_without_transaction(workflow_id:, kind: "step_canceled", command_id:, error:)
      end
    end

    #: (workflow_id: String, command_id: Integer, error: String, ?terminal: bool, ?error_class: String?, ?error_message: String?, ?retrying: bool) -> Object?
    def record_step_failed_without_transaction(workflow_id:, command_id:, error:, terminal: false, error_class: nil, error_message: nil, retrying: false)
      execute_store_query(:fail_step, [error, workflow_id, command_id])
      update_latest_attempt_serialized(workflow_id:, command_id:, status: "failed", serialized_result: dump_serialized(nil), error:)
      payload = step_failure_payload(terminal:, error_class:, error_message:, retrying:)
      append_workflow_history_without_transaction(workflow_id:, kind: "step_failed", command_id:, payload:, error:)
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
      transaction do
        candidates = []
        candidates.concat(execute_store_query(:claim_pending_outbox).to_a)
        candidates.concat(execute_store_query(:claim_expired_outbox).to_a)
        candidate = candidates.min_by { |candidate_row| candidate_row.fetch("created_at").to_s }
        next unless candidate

        execute_store_query(:claim_selected_outbox, [worker_id, lease_seconds, candidate.fetch("id")])
        message = outbox_message(candidate.fetch("id"))
        observe_claim_latency(message, "outbox")
        message
      end
    end

    #: (String, worker_id: String) -> Object?
    def ack_outbox(outbox_id, worker_id:)
      result = execute_store_query(:ack_outbox, [outbox_id, worker_id])
      Observability.count("durababble.outbox.processed", "durababble.worker.id" => worker_id) if result.affected_rows.to_i.positive?
      result
    end

    #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, name: String, wait_request: WaitRequest, ?suspend_workflow: bool, ?worker_id: String?) -> Object?
    def record_wait(workflow_id:, name:, wait_request:, command_id: nil, position: nil, suspend_workflow: true, worker_id: nil)
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:) if worker_id
        serialized_context = dump_serialized(wait_request.context)
        execute_store_query(:upsert_waiting_step, [workflow_id, command_id, name, serialized_context])
        wait_id = SecureRandom.uuid
        execute_store_query(:insert_wait, [wait_id, workflow_id, command_id, wait_request.kind, wait_request.event_key, wait_request.wake_at, dump_serialized(wait_request.context)])
        update_latest_attempt_serialized(
          workflow_id:,
          command_id:,
          status: "waiting",
          serialized_result: serialized_context,
          error: nil,
        )
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
      execute_store_query(:insert_fence, [workflow_id, key, token, timeout])
      claimed = execute_store_query(:lock_fence_for_worker, [workflow_id, key, token]).first
      claimed ||= execute_store_query(:claim_expired_fence, [token, timeout, workflow_id, key]).affected_rows == 1

      if claimed
        begin
          result = block.call
          execute_store_query(:complete_fence, [dump_serialized(result), workflow_id, key, token])
          return result
        rescue StandardError => e
          execute_store_query(:fail_fence, ["#{e.class}: #{e.message}", workflow_id, key, token])
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

    #: (object_type: String, object_id: String, state: Object?, ?worker_pool: String) -> Object?
    def save_object_state(object_type:, object_id:, state:, worker_pool: "default")
      serialized_state = dump_object_state(object_type:, object_id:, state:)
      execute_store_query(:save_object_state, [worker_pool, object_type, object_id, serialized_state])
      state
    end

    #: (worker_pool: String, object_type: String, object_id: String, name: String, wake_at: Object?, payload: Object?) -> Object?
    def upsert_object_wakeup_without_transaction(worker_pool:, object_type:, object_id:, name:, wake_at:, payload:)
      serialized_payload = dump_serialized(payload, surface: :inbox_payload, context: "object wakeup #{object_type}/#{object_id} (#{name})")
      execute_store_query(:upsert_object_wakeup, [worker_pool, object_type, object_id, name, wake_at, serialized_payload])
    end

    #: (worker_pool: String, object_type: String, object_id: String, name: String) -> Object?
    def delete_object_wakeup_without_transaction(worker_pool:, object_type:, object_id:, name:)
      execute_store_query(:delete_object_wakeup, [worker_pool, object_type, object_id, name])
    end

    #: (worker_pool: String, object_type: String, object_id: String) -> Object?
    def delete_all_object_wakeups_without_transaction(worker_pool:, object_type:, object_id:)
      execute_store_query(:delete_all_object_wakeups, [worker_pool, object_type, object_id])
    end

    #: (worker_id: String, lease_seconds: Integer, ?target_kinds: Array[String]?, ?target_types: Array[String]?, ?now: Time, ?worker_pool: String) -> Object?
    def claim_target_activation(worker_id:, lease_seconds:, target_kinds: nil, target_types: nil, now: Time.now, worker_pool: "default")
      return if target_kinds&.empty? || target_types&.empty?

      transaction do
        filter_sql, filter_params = target_activation_filter_sql(target_kinds:, target_types:)
        candidates = []
        candidates.concat(execute_store_query(:claim_pending_target_activation, [worker_pool, now] + filter_params, filter_sql:).to_a)
        candidates.concat(execute_store_query(:claim_expired_target_activation, [worker_pool, now] + filter_params, filter_sql:).to_a)
        candidate = candidates.min_by { |candidate_row| candidate_row.fetch("created_at").to_s }
        next unless candidate

        execute_store_query(:claim_selected_target_activation, [worker_id, lease_seconds, worker_pool, candidate.fetch("target_kind"), candidate.fetch("target_type"), candidate.fetch("target_id")])
        target_activation(worker_pool:, target_kind: candidate.fetch("target_kind"), target_type: candidate.fetch("target_type"), target_id: candidate.fetch("target_id"))
      end
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
      serialized_input = dump_workflow_input(name:, input:)
      if worker_id
        execute_store_query(:insert_workflow_with_worker, [workflow_id, name, worker_pool, status, serialized_input, worker_id, lease_seconds || 60])
      else
        execute_store_query(:insert_workflow, [workflow_id, name, worker_pool, status, serialized_input])
      end
      workflow_id
    rescue ActiveRecord::RecordNotUnique
      raise WorkflowAlreadyExists, "workflow #{workflow_id} already exists"
    end

    #: (workflow_id: String, worker_id: String) -> bool
    def lock_owned_workflow_for_update(workflow_id:, worker_id:)
      execute_store_query(:lock_owned_workflow_for_update, [workflow_id, worker_id]).first
    end

    #: (String, error: String) -> void
    def terminate_workflow_dependents(workflow_id, error:)
      # Called only while request_workflow_termination holds the workflow row lock inside a transaction.
      execute_store_query(:terminate_workflow_waits, [workflow_id])
      execute_store_query(:terminate_workflow_steps, [error, workflow_id])
      execute_store_query(:terminate_workflow_step_attempts, [error, workflow_id])
      execute_store_query(:terminate_workflow_inbox, [error, workflow_id])
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
      ready_time = ready_at || Time.now.utc
      execute_store_query(:upsert_target_activation, [worker_pool, target_kind, target_type, target_id, ready_time])
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
      row = execute_params("SELECT status, error FROM #{table("workflows")} WHERE worker_pool = ? AND id = ? FOR UPDATE", [worker_pool, workflow_id]).first
      return unless row && WorkflowStatus.terminal?(row)

      status = row.fetch("status")
      error = row["error"]
      suffix = error.to_s.empty? ? "" : ": #{error}"
      "workflow #{workflow_id} is terminal #{status}#{suffix}"
    end

    #: (target_type: String, target_id: String, error: String) -> Object?
    def dead_letter_terminal_workflow_inbox_without_transaction(target_type:, target_id:, error:)
      execute_params(<<~SQL, [error, target_type, target_id])
        UPDATE #{table("inbox")}
        SET status = 'dead_lettered', error = ?, locked_by = NULL, locked_until = NULL, dead_lettered_at = NOW(6), updated_at = NOW(6)
        WHERE target_kind = 'workflow' AND target_type = ? AND target_id = ?
          AND status IN ('pending', 'failed', 'running')
      SQL
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String) -> Object?
    def delete_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:)
      execute_store_query(:delete_target_activation, [worker_pool, target_kind, target_type, target_id])
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, ready_at: Object?) -> Object?
    def set_target_activation_pending_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at:)
      ready_time = ready_at || Time.now.utc
      execute_store_query(:set_target_activation_pending, [worker_pool, target_kind, target_type, target_id, ready_time])
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String) -> [Integer, String]
    def allocate_mailbox_sequence(worker_pool:, target_kind:, target_type:, target_id:)
      execute_store_query(:insert_mailbox_sequence, [worker_pool, target_kind, target_type, target_id])
      row = execute_store_query(:mailbox_sequence_for_update, [target_kind, target_type, target_id]).first
      sequence = row.fetch("last_sequence").to_i + 1
      execute_store_query(:update_mailbox_sequence, [sequence, target_kind, target_type, target_id])
      [sequence, row.fetch("worker_pool").to_s]
    end

    #: (String?, target_kind: String, target_type: String, target_id: String) -> Object?
    def existing_inbox_message_for_idempotency(idempotency_key, target_kind:, target_type:, target_id:)
      return unless idempotency_key

      idempotency_hash = inbox_idempotency_hash(idempotency_key, target_kind:, target_type:, target_id:)
      execute_store_query(:existing_inbox_message_for_idempotency, [idempotency_hash]).first
    end

    #: (String) -> Object?
    def lock_workflow_for_update(workflow_id)
      execute_store_query(:lock_workflow_for_update, [workflow_id]).first
    end

    #: (id: String, worker_pool: String, target_kind: String, target_type: String, target_id: String, sequence: Integer, message_kind: String, method_name: String?, operation_id: String, idempotency_key: String?, shape_hash: String, payload: Object?, ?ready_at: Object?, ?max_attempts: Integer?) -> Object?
    def insert_inbox_message_without_transaction(id:, worker_pool:, target_kind:, target_type:, target_id:, sequence:, message_kind:, method_name:, operation_id:, idempotency_key:, shape_hash:, payload:, ready_at: nil, max_attempts: nil)
      idempotency_hash = inbox_idempotency_hash(idempotency_key, target_kind:, target_type:, target_id:)
      serialized_payload = dump_inbox_payload(target_kind:, target_type:, target_id:, message_kind:, payload:)
      execute_store_query(:insert_inbox_message, [id, worker_pool, target_kind, target_type, target_id, sequence, message_kind, method_name, operation_id, idempotency_key, idempotency_hash, shape_hash, serialized_payload, ready_at, max_attempts])
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String, limit: Integer) -> Array[Hash[String, Object?]]
    def inbox_claim_rows_for_update(worker_pool:, target_kind:, target_type:, target_id:, limit:)
      # ActiveRecord quotes MySQL sanitized numeric binds, which is invalid in LIMIT.
      execute_store_query(:inbox_claim_rows_for_update, [worker_pool, target_kind, target_type, target_id], limit:).to_a
    end

    #: (worker_pool: String, target_kind: String, target_type: String, target_id: String) -> Object?
    def inbox_head_for_update(worker_pool:, target_kind:, target_type:, target_id:)
      execute_store_query(:inbox_head_for_update, [worker_pool, target_kind, target_type, target_id]).first
    end

    #: (message_id: String, worker_id: String, lease_seconds: Integer) -> Object?
    def mark_inbox_row_running_without_transaction(message_id:, worker_id:, lease_seconds:)
      execute_store_query(:mark_inbox_row_running, [worker_id, lease_seconds, message_id])
    end

    #: (message_id: String, result: Object?) -> Object?
    def complete_inbox_message_without_transaction(message_id:, result:)
      serialized_result = dump_inbox_result(message_id:, result:)
      execute_store_query(:complete_inbox_message, [serialized_result, message_id])
    end

    #: (message_id: String, error: String) -> Object?
    def fail_inbox_message_without_transaction(message_id:, error:)
      execute_store_query(:fail_inbox_message, [error, message_id])
    end

    #: (message_id: String, error: String, ready_at: Object?) -> Object?
    def retry_inbox_message_without_transaction(message_id:, error:, ready_at:)
      execute_store_query(:retry_inbox_message, [error, ready_at, message_id])
    end

    #: (message_id: String, error: String) -> Object?
    def dead_letter_inbox_message_without_transaction(message_id:, error:)
      execute_store_query(:dead_letter_inbox_message, [error, message_id])
    end

    #: (target_kinds: Array[String]?, target_types: Array[String]?) -> [String, Array[String]]
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

    #: (workflow_id: String, command_id: Integer, status: String, serialized_result: Object?, error: String?) -> Object?
    def update_latest_attempt_serialized(workflow_id:, command_id:, status:, serialized_result:, error:)
      execute_store_query(:update_latest_attempt, [status, serialized_result, error, workflow_id, command_id])
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

    #: (Time, Integer) -> Integer
    def complete_timer_waits(now, batch_size)
      completed = transaction(isolation: :read_committed) do
        waits = execute_store_query(:complete_timer_waits, [now], limit: batch_size).map { |row| decode_row(row) }
        finish_completed_waits(waits, {})
      end
      completed = completed #: as Integer
      completed
    end

    #: (Time, Integer) -> Integer
    def complete_object_wakeups(now, batch_size)
      completed = transaction(isolation: :read_committed) do
        wakeups = execute_store_query(:due_object_wakeups, [now], limit: batch_size).map { |row| decode_row(row) }
        deliver_due_object_wakeups(wakeups)
      end
      completed = completed #: as Integer
      report_object_wakeups_completed(completed)
      completed
    end

    #: (Array[Hash[String, Object?]], Hash[String, Object?]) -> Integer
    def finish_completed_waits(waits, payload)
      waits.each do |wait|
        wait = wait #: as untyped
        execute_store_query(:complete_wait, [dump_serialized(payload), wait.fetch("id")])
        record_wait_latency(wait)
        context = wait.fetch("context").merge(payload)
        record_step_completed_without_transaction(workflow_id: wait.fetch("workflow_id"), command_id: wait.fetch("position").to_i, result: context)
      end
      mark_waits_workflows_pending(waits)
      Observability.count("durababble.waits.completed", by: waits.length)
      waits.length
    end

    #: (String) -> untyped
    def execute(sql)
      with_connection do |active_record_connection|
        active_record_connection.exec_query(sql)
      end
    end

    #: () -> Symbol
    def store_query_prefix
      :mysql
    end

    #: (String, Array[Object?]) -> untyped
    def execute_store_query_sql(sql, params)
      with_connection do |active_record_connection|
        if trilogy_connection?(active_record_connection)
          sanitized_sql = sanitizer_class(active_record_connection).send(:sanitize_sql_array, [sql, *params])
          active_record_connection.exec_query(sanitized_sql, "Durababble SQL")
        else
          active_record_connection.exec_query(sql, "Durababble SQL", params, prepare: false)
        end
      end
    end

    #: (String, Array[Object?]) -> untyped
    def execute_params(sql, params)
      execute_store_query_sql(sql, params)
    end

    #: (**Object?) { () -> Object? } -> Object?
    def transaction(**options, &block)
      attempts = 0
      begin
        super(**options, &block)
      rescue StandardError => error
        if retryable_mysql_error?(error) && attempts < MAX_TRANSACTION_RETRY_ATTEMPTS
          attempts += 1
          sleep(Backoff.linear(attempts, step: TRANSACTION_RETRY_STEP_SECONDS))
          retry
        end

        raise
      end
    end

    #: (Array[String]?) -> [String, Array[String]]
    def workflow_name_filter(workflow_names)
      return ["", []] unless workflow_names

      ["AND name IN (#{mysql_placeholders(workflow_names.length)})", workflow_names]
    end

    #: (Array[String]?) -> [String, Array[String]]
    def workflow_exclusion_filter(workflow_ids)
      workflow_ids = Array(workflow_ids).uniq
      return ["", []] if workflow_ids.empty?

      ["AND id NOT IN (#{mysql_placeholders(workflow_ids.length)})", workflow_ids]
    end

    #: (Integer) -> Object?
    def mysql_placeholders(count)
      Array.new(count, "?").join(", ")
    end

    #: (Integer) -> Object?
    def placeholder(_index)
      "?"
    end

    #: (Time?) -> Time?
    def timestamp_or_nil(time)
      time
    end

    #: (String) -> Object?
    def table(name)
      quote_column_name("#{table_prefix}_#{name}")
    end

    #: (String) -> Object?
    def raw_table_name(name)
      "#{table_prefix}_#{name}"
    end

    #: () -> Object?
    def table_prefix
      prefix = schema.to_s.gsub(/[^a-zA-Z0-9_]/, "_")
      return prefix if prefix.length <= 32

      "dura_#{Digest::SHA1.hexdigest(prefix)[0, 10]}"
    end

    #: (String, String) -> String
    def index_name(table_name, suffix)
      name = "#{table_prefix}_#{table_name}_#{suffix}_idx"
      name[0, 64] || name
    end

    #: (Object?, ?surface: Symbol?, ?context: String?) -> Object?
    def dump_serialized(value, surface: nil, context: nil)
      dump_serialized_bytes(value, surface:, context:)
    end

    #: (Object?) -> Object?
    def load_serialized(value)
      return if value.nil?

      SERIALIZER.load(value)
    end

    #: (StandardError) -> bool
    def retryable_mysql_error?(error)
      error.is_a?(ActiveRecord::Deadlocked) ||
        error.class.name == "ActiveRecord::LockWaitTimeout"
    end

    #: (ActiveRecord::ConnectionAdapters::AbstractAdapter) -> bool
    def trilogy_connection?(active_record_connection)
      active_record_connection.adapter_name.to_s.downcase.include?("trilogy")
    end

    #: (ActiveRecord::ConnectionAdapters::AbstractAdapter) -> Class
    def sanitizer_class(active_record_connection)
      Class.new do
        extend ActiveRecord::Sanitization::ClassMethods

        define_singleton_method(:with_connection) do |&block|
          block.call(active_record_connection)
        end
      end
    end
  end
end
