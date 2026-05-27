# typed: true
# frozen_string_literal: true

require_relative "core"

module Durababble
  module Deterministic
    class VirtualYugabyte
      include Durababble::TestSupport::FakeStoreCommandClaiming

      # Mirrors the affected-row count the SQL stores hand back from a completion
      # write; `DurableObjectExecutor#complete_message` treats a non-positive
      # count as a lost lease.
      AffectedRows = Data.define(:affected_rows)

      #: untyped
      attr_reader :scheduler, :fault_plan

      #: (scheduler: untyped, ?fault_plan: untyped) -> void
      def initialize(scheduler:, fault_plan: nil)
        @scheduler = scheduler
        @fault_plan = fault_plan || FaultPlan.new(scheduler:)
        @id_seq = 0
        @workflows = {}
        @cancellations = {}
        @history = Hash.new { |hash, key| hash[key] = [] }
        @steps = Hash.new { |hash, key| hash[key] = {} }
        @attempts = Hash.new { |hash, key| hash[key] = [] }
        @waits = {}
        @fences = {}
        @outbox = {}
        @outbox_by_key = {}
        @side_effects = 0
        @object_inbox = {}
        @object_state = {}
        @object_wakeups = {}
        @mailbox_sequences = {}
        @object_wakes_delivered = 0
        trace("init")
      end

      #: () -> untyped
      def migrate! = self
      #: () -> untyped
      def close = nil
      #: () -> untyped
      def drop_schema! = nil
      #: () -> untyped
      def current_time = scheduler.time

      #: (name: untyped, input: untyped, ?id: untyped, ?worker_pool: untyped) -> untyped
      def enqueue_workflow(name:, input:, id: nil, worker_pool: "default")
        id ||= next_id("wf")
        raise WorkflowAlreadyExists, "workflow #{id} already exists" if @workflows.key?(id)

        @workflows[id] = { "id" => id, "name" => name, "worker_pool" => worker_pool, "status" => "pending", "input" => deep(input), "result" => nil, "error" => nil, "locked_by" => nil, "locked_until" => nil, "next_run_at" => nil }
        trace("enqueue_workflow", id:, name:)
        id
      end

      #: (name: untyped, input: untyped, ?worker_id: untyped, ?lease_seconds: untyped, ?worker_pool: untyped) -> untyped
      def create_workflow(name:, input:, worker_id: nil, lease_seconds: 60, worker_pool: "default")
        id = enqueue_workflow(name:, input:, worker_pool:)
        mark_workflow_running(id, worker_id:, lease_seconds:, worker_pool:)
        id
      end

      #: (worker_id: untyped, lease_seconds: untyped, ?workflow_names: untyped, ?worker_pool: untyped) -> untyped
      def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil, worker_pool: "default")
        workflow = @workflows.values.select { |row| row.fetch("worker_pool", "default") == worker_pool && runnable?(row) && (!workflow_names || workflow_names.include?(row.fetch("name"))) }.min_by { |row| row.fetch("id") }
        return unless workflow

        claim_row(workflow, worker_id, lease_seconds)
      end

      #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped, ?worker_pool: untyped) -> untyped
      def claim_workflow(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
        row = @workflows.fetch(workflow_id)
        return unless row.fetch("worker_pool", "default") == worker_pool
        return deep(row) if row.fetch("status") == "running" && row.fetch("locked_by") == worker_id && !expired?(row)
        return unless row.fetch("status") == "pending" ||
          retryable_failed?(row) ||
          canceling_due?(row) ||
          (row.fetch("status") == "running" && (row.fetch("locked_by") == worker_id || expired?(row)))

        claim_row(row, worker_id, lease_seconds)
      end

      #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
      def heartbeat(workflow_id:, worker_id:, lease_seconds:)
        row = @workflows.fetch(workflow_id)
        if row.fetch("locked_by") == worker_id && row.fetch("status") == "running" && !expired?(row)
          row["locked_until"] = scheduler.time + lease_seconds
          trace("heartbeat", id: workflow_id, worker: worker_id)
        end
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, worker_id: untyped, lease_seconds: untyped, cursor: untyped) -> untyped
      def heartbeat_step(workflow_id:, worker_id:, lease_seconds:, cursor:, command_id: nil, position: nil)
        command_id = normalize_command_id(command_id, position)
        row = @workflows.fetch(workflow_id)
        return unless row.fetch("locked_by") == worker_id && row.fetch("status") == "running" && !expired?(row)

        row["locked_until"] = scheduler.time + lease_seconds
        step = @steps[workflow_id][command_id]
        return unless step&.fetch("status") == "running"

        step["heartbeat_cursor"] = deep(cursor)
        latest_attempt = @attempts[workflow_id].reverse.find { |attempt| attempt.fetch("position") == command_id && attempt.fetch("status") == "running" }
        latest_attempt["heartbeat_cursor"] = deep(cursor) if latest_attempt
        trace("step_heartbeat", id: workflow_id, command_id:, worker: worker_id, cursor:)
        row.fetch("locked_until")
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped) -> untyped
      def step_heartbeat_cursor(workflow_id:, command_id: nil, position: nil)
        command_id = normalize_command_id(command_id, position)
        deep(@steps[workflow_id][command_id]&.fetch("heartbeat_cursor", nil))
      end

      #: (untyped, ?worker_pool: untyped) -> untyped
      def current_workflow_lease(workflow_id, worker_pool: nil)
        row = @workflows.fetch(workflow_id)
        return if worker_pool && row.fetch("worker_pool", "default") != worker_pool
        return unless row.fetch("status") == "running" && row.fetch("locked_by") && !expired?(row)

        { "workflow_id" => workflow_id, "worker_pool" => row.fetch("worker_pool", "default"), "worker_id" => row.fetch("locked_by"), "locked_until" => row.fetch("locked_until") }
      end

      #: (?now: untyped) -> untyped
      def steal_expired_leases!(now: nil)
        now ||= scheduler.time
        count = 0
        @workflows.each_value do |row|
          next unless row.fetch("status") == "running" && row.fetch("locked_until") && row.fetch("locked_until") < now

          row["status"] = @cancellations.key?(row.fetch("id")) ? "canceling" : "pending"
          row["locked_by"] = nil
          row["locked_until"] = nil
          count += 1
          trace("steal_expired", id: row.fetch("id"))
        end
        count
      end

      #: (untyped, ?worker_id: untyped, ?lease_seconds: untyped, ?worker_pool: untyped) -> untyped
      def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60, worker_pool: "default")
        row = @workflows.fetch(workflow_id)
        return unless row.fetch("worker_pool", "default") == worker_pool

        row["status"] = "running"
        row["error"] = nil
        if worker_id
          row["locked_by"] = worker_id
          row["locked_until"] = scheduler.time + lease_seconds
          row["next_run_at"] = nil
        end
        deep(row)
      end

      #: (workflow_id: untyped, ?worker_id: untyped) -> untyped
      def suspend_workflow(workflow_id:, worker_id: nil)
        row = @workflows.fetch(workflow_id)
        return true if row.fetch("status") == "waiting"
        return true if row.fetch("status") == "pending"
        return false unless row.fetch("status") == "running"
        return false if worker_id && row.fetch("locked_by") != worker_id
        return false if worker_id && expired?(row)

        row["status"] = @waits.values.any? { |wait| wait.fetch("workflow_id") == workflow_id && wait.fetch("status") == "pending" } ? "waiting" : "pending"
        row["locked_by"] = nil
        row["locked_until"] = nil
        trace("workflow_suspended", id: workflow_id)
        true
      end

      #: (untyped, result: untyped, ?worker_id: untyped) -> untyped
      def complete_workflow(workflow_id, result:, worker_id: nil)
        row = @workflows.fetch(workflow_id)
        require_fenced_workflow_update!(row, workflow_id:, worker_id:, operation: "workflow completion")
        row["status"] = "completed"
        row["result"] = deep(result)
        row["error"] = nil
        row["locked_by"] = nil
        row["locked_until"] = nil
        row["next_run_at"] = nil
        trace("complete_workflow", id: workflow_id, result:)
      end

      #: (untyped, reason: untyped, ?result: untyped, ?worker_id: untyped) -> untyped
      def cancel_workflow(workflow_id, reason:, result: nil, worker_id: nil)
        row = @workflows.fetch(workflow_id)
        require_fenced_workflow_update!(row, workflow_id:, worker_id:, operation: "workflow cancellation")
        row["status"] = "canceled"
        row["result"] = deep(result)
        row["error"] = reason
        row["locked_by"] = nil
        row["locked_until"] = nil
        row["next_run_at"] = nil
        trace("cancel_workflow", id: workflow_id, reason:, result:)
      end

      #: (untyped, error: untyped, ?worker_id: untyped) -> untyped
      def fail_workflow(workflow_id, error:, worker_id: nil)
        row = @workflows.fetch(workflow_id)
        require_fenced_workflow_update!(row, workflow_id:, worker_id:, operation: "workflow failure")
        row["status"] = "failed"
        row["error"] = error
        row["locked_by"] = nil
        row["locked_until"] = nil
        row["next_run_at"] = nil
        trace("fail_workflow", id: workflow_id, error:)
      end

      #: (workflow_id: untyped, command_id: untyped, name: untyped, ?args: untyped, ?kwargs: untyped, ?metadata: untyped, ?worker_id: untyped) -> untyped
      def record_step_scheduled(workflow_id:, command_id:, name:, args: [], kwargs: {}, metadata: {}, worker_id: nil)
        assert_workflow_lease!(workflow_id, worker_id) if worker_id
        payload = { "name" => name, "args" => deep(args), "kwargs" => deep(kwargs) }.merge(deep(metadata))
        append_history(workflow_id:, kind: "step_scheduled", command_id:, name:, payload:)
        @steps[workflow_id][command_id] ||= { "workflow_id" => workflow_id, "position" => command_id, "command_id" => command_id, "name" => name, "status" => "scheduled", "result" => nil, "error" => nil, "heartbeat_cursor" => nil }
        trace("step_scheduled", id: workflow_id, command_id:, name:, payload:)
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, name: untyped, ?worker_id: untyped) -> untyped
      def record_step_started(workflow_id:, name:, command_id: nil, position: nil, worker_id: nil)
        assert_workflow_lease!(workflow_id, worker_id) if worker_id
        command_id = normalize_command_id(command_id, position)
        @attempts[workflow_id].each do |attempt|
          next unless attempt.fetch("position") == command_id && attempt.fetch("status") == "running"

          attempt["status"] = "failed"
          attempt["error"] = "superseded by retry"
        end
        previous_cursor = @steps[workflow_id][command_id]&.fetch("heartbeat_cursor", nil)
        @steps[workflow_id][command_id] = { "workflow_id" => workflow_id, "position" => command_id, "command_id" => command_id, "name" => name, "status" => "running", "result" => nil, "error" => nil, "heartbeat_cursor" => deep(previous_cursor) }
        attempt_id = next_id("attempt")
        @attempts[workflow_id] << { "id" => attempt_id, "workflow_id" => workflow_id, "position" => command_id, "command_id" => command_id, "name" => name, "status" => "running", "result" => nil, "error" => nil, "heartbeat_cursor" => deep(previous_cursor) }
        append_history(workflow_id:, kind: "step_started", command_id:, name:, attempt_id:)
        trace("step_started", id: workflow_id, command_id:, name:)
        attempt_id
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, result: untyped, ?worker_id: untyped) -> untyped
      def record_step_completed(workflow_id:, result:, command_id: nil, position: nil, worker_id: nil)
        assert_workflow_lease!(workflow_id, worker_id) if worker_id
        command_id = normalize_command_id(command_id, position)
        step = @steps[workflow_id].fetch(command_id)
        step["status"] = "completed"
        step["result"] = deep(result)
        update_latest_attempt(workflow_id, command_id, "completed", result, nil)
        append_history(workflow_id:, kind: "step_completed", command_id:, payload: result)
        trace("step_completed", id: workflow_id, command_id:, result:)
        fault_plan.after(:record_step_completed)
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped, ?worker_id: untyped, ?terminal: untyped, ?error_class: untyped, ?error_message: untyped) -> untyped
      def record_step_failed(workflow_id:, error:, command_id: nil, position: nil, worker_id: nil, terminal: false, error_class: nil, error_message: nil)
        assert_workflow_lease!(workflow_id, worker_id) if worker_id
        command_id = normalize_command_id(command_id, position)
        step = @steps[workflow_id].fetch(command_id)
        step["status"] = "failed"
        step["error"] = error
        update_latest_attempt(workflow_id, command_id, "failed", nil, error)
        payload = nil
        if terminal
          payload = { "terminal" => true }
          payload["error_class"] = error_class if error_class
          payload["error_message"] = error_message if error_message
        end
        append_history(workflow_id:, kind: "step_failed", command_id:, payload:, error:)
        trace("step_failed", id: workflow_id, command_id:, error:)
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped, worker_id: untyped, run_at: untyped) -> untyped
      def record_step_failed_and_schedule_retry(workflow_id:, error:, worker_id:, run_at:, command_id: nil, position: nil)
        command_id = normalize_command_id(command_id, position)
        record_step_failed(workflow_id:, command_id:, error:, worker_id:)
        scheduled = schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
        raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before workflow retry scheduling" unless scheduled

        scheduled
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped, ?worker_id: untyped) -> untyped
      def record_step_canceled(workflow_id:, error:, command_id: nil, position: nil, worker_id: nil)
        assert_workflow_lease!(workflow_id, worker_id) if worker_id
        command_id = normalize_command_id(command_id, position)
        step = @steps[workflow_id].fetch(command_id)
        step["status"] = "canceled"
        step["error"] = error
        update_latest_attempt(workflow_id, command_id, "canceled", nil, error)
        append_history(workflow_id:, kind: "step_canceled", command_id:, error:)
        trace("step_canceled", id: workflow_id, command_id:, error:)
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, name: untyped, wait_request: untyped, ?suspend_workflow: untyped, ?worker_id: untyped) -> untyped
      def record_wait(workflow_id:, name:, wait_request:, command_id: nil, position: nil, suspend_workflow: true, worker_id: nil)
        assert_workflow_lease!(workflow_id, worker_id) if worker_id
        command_id = normalize_command_id(command_id, position)
        @steps[workflow_id][command_id] = { "workflow_id" => workflow_id, "position" => command_id, "command_id" => command_id, "name" => name, "status" => "waiting", "result" => deep(wait_request.context), "error" => nil, "heartbeat_cursor" => @steps[workflow_id][command_id]&.fetch("heartbeat_cursor", nil) }
        wait_id = next_id("wait")
        @waits[wait_id] = { "id" => wait_id, "workflow_id" => workflow_id, "position" => command_id, "command_id" => command_id, "kind" => wait_request.kind, "event_key" => wait_request.event_key, "wake_at" => wait_request.wake_at, "context" => deep(wait_request.context), "payload" => nil, "status" => "pending" }
        update_latest_attempt(workflow_id, command_id, "waiting", wait_request.context, nil)
        append_history(workflow_id:, kind: "step_waiting", command_id:, name:, payload: wait_request.context)
        if suspend_workflow && !suspend_workflow(workflow_id:, worker_id:)
          raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before wait suspension"
        end

        trace("wait_recorded", id: workflow_id, wait_id:, kind: wait_request.kind, event_key: wait_request.event_key)
        fault_plan.after(:record_wait)
        wait_id
      end

      #: (?now: untyped) -> untyped
      def wake_due_timers(now: nil)
        now ||= scheduler.time
        completed = complete_waits(@waits.values.select { |wait| wait.fetch("status") == "pending" && wait.fetch("kind") == "timer" && wait.fetch("wake_at") <= now }, {})
        completed + deliver_due_object_wakeups(now)
      end

      #: (untyped) -> untyped
      def waits_for(workflow_id)
        @waits.values.select { |wait| wait.fetch("workflow_id") == workflow_id }.sort_by { |wait| wait.fetch("id") }.map { |row| deep(row) }
      end

      #: (untyped) -> untyped
      def workflow_history_for(workflow_id)
        @history[workflow_id].map { |row| deep(row) }
      end

      #: (untyped) -> untyped
      def workflow_history_count_for(workflow_id)
        @history[workflow_id].length
      end

      #: (workflow_id: untyped, key: untyped, ?poll_interval: untyped, ?timeout: untyped) { (?) -> untyped } -> untyped
      def with_fence(workflow_id:, key:, poll_interval: 0.05, timeout: 10, &block)
        fence_key = [workflow_id, key]
        existing = @fences[fence_key]
        return deep(existing.fetch("result")) if existing&.fetch("status") == "completed"
        raise Error, existing.fetch("error") if existing&.fetch("status") == "failed"
        raise FenceTimeout, "virtual fence already running: #{key}" if existing

        @side_effects += 1
        @fences[fence_key] = { "workflow_id" => workflow_id, "key" => key, "status" => "running", "result" => nil, "error" => nil }
        trace("fence_acquired", id: workflow_id, key:)
        result = block.call
        @fences[fence_key]["status"] = "completed"
        @fences[fence_key]["result"] = deep(result)
        trace("fence_completed", id: workflow_id, key:, result:)
        deep(result)
      rescue StandardError => e
        @fences[fence_key]["status"] = "failed" if @fences[fence_key]
        @fences[fence_key]["error"] = "#{e.class}: #{e.message}" if @fences[fence_key]
        raise
      end

      #: (workflow_id: untyped, topic: untyped, payload: untyped, key: untyped) -> untyped
      def enqueue_outbox(workflow_id:, topic:, payload:, key:)
        return @outbox_by_key.fetch(key) if @outbox_by_key.key?(key)

        id = next_id("outbox")
        @outbox[id] = { "id" => id, "workflow_id" => workflow_id, "topic" => topic, "payload" => deep(payload), "key" => key, "status" => "pending", "locked_by" => nil, "locked_until" => nil }
        @outbox_by_key[key] = id
        trace("outbox_enqueued", id:, key:, topic:)
        fault_plan.after(:enqueue_outbox)
        id
      end

      #: (worker_id: untyped, lease_seconds: untyped) -> untyped
      def claim_outbox(worker_id:, lease_seconds:)
        row = @outbox.values.select { |message| message.fetch("status") == "pending" || (message.fetch("status") == "processing" && message.fetch("locked_until") < scheduler.time) }.min_by { |message| message.fetch("id") }
        return unless row

        row["status"] = "processing"
        row["locked_by"] = worker_id
        row["locked_until"] = scheduler.time + lease_seconds
        trace("outbox_claimed", id: row.fetch("id"), worker: worker_id)
        deep(row)
      end

      #: (untyped, worker_id: untyped) -> untyped
      def ack_outbox(outbox_id, worker_id:)
        row = @outbox.fetch(outbox_id)
        return unless row.fetch("locked_by") == worker_id

        row["status"] = "processed"
        trace("outbox_processed", id: outbox_id, worker: worker_id)
      end

      #: (untyped) -> untyped
      def outbox_message(outbox_id) = deep(@outbox.fetch(outbox_id))
      #: (untyped) -> untyped
      def workflow(workflow_id) = deep(@workflows.fetch(workflow_id))

      #: (target_kind: untyped, target_type: untyped, target_id: untyped) -> untyped
      def target_activation(target_kind:, target_type:, target_id:, worker_pool: "default")
        nil
      end

      #: (workflow_id: untyped, worker_id: untyped) -> untyped
      def workflow_owned?(workflow_id:, worker_id:)
        row = @workflows.fetch(workflow_id)
        row.fetch("status") == "running" && row.fetch("locked_by") == worker_id && !expired?(row)
      end

      #: (workflow_id: untyped, worker_id: untyped, run_at: untyped) -> untyped
      def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
        row = @workflows.fetch(workflow_id)
        return unless row.fetch("status") == "running" && row.fetch("locked_by") == worker_id && !expired?(row)

        row["status"] = @cancellations.key?(workflow_id) ? "canceling" : "pending"
        row["locked_by"] = nil
        row["locked_until"] = nil
        row["next_run_at"] = run_at
        trace("workflow_retry_scheduled", id: workflow_id, run_at:)
        true
      end

      #: (workflow_id: untyped, reason: untyped) -> untyped
      def request_workflow_cancellation(workflow_id:, reason:)
        row = @workflows.fetch(workflow_id)
        return deep(row) if WorkflowStatus.terminal?(row)

        first_request = !@cancellations.key?(workflow_id)
        @cancellations[workflow_id] ||= { "workflow_id" => workflow_id, "reason" => reason, "requested_at" => scheduler.time, "delivered_at" => nil }
        cancel_pending_waits_for_workflow(workflow_id) if first_request
        if first_request && row.fetch("status") != "running"
          row["status"] = "canceling"
          row["locked_by"] = nil
          row["locked_until"] = nil
          row["next_run_at"] = nil
        end
        trace("workflow_cancel_requested", id: workflow_id, reason: @cancellations.fetch(workflow_id).fetch("reason"), status: row.fetch("status"))
        deep(row)
      end

      #: (untyped) -> untyped
      def workflow_cancellation(workflow_id)
        deep(@cancellations[workflow_id])
      end

      #: (workflow_id: untyped) -> untyped
      def mark_workflow_cancellation_delivered(workflow_id:)
        cancellation = @cancellations[workflow_id]
        return unless cancellation

        cancellation["delivered_at"] ||= scheduler.time
        trace("workflow_cancel_delivered", id: workflow_id)
      end

      #: (untyped, ?now: untyped) -> untyped
      def make_workflow_due!(workflow_id, now: scheduler.time)
        row = @workflows.fetch(workflow_id)
        row["next_run_at"] = nil
        trace("workflow_retry_due", id: workflow_id, now:)
      end

      #: (untyped) -> untyped
      def steps_for(workflow_id) = @steps[workflow_id].values.sort_by { |row| row.fetch("position") }.map { |row| deep(row) }
      #: (untyped) -> untyped
      def step_attempts_for(workflow_id) = @attempts[workflow_id].map { |row| deep(row) }

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped) -> untyped
      def step_attempt_count_for(workflow_id:, command_id: nil, position: nil)
        command_id = normalize_command_id(command_id, position)
        @attempts[workflow_id].count { |attempt| attempt.fetch("position") == command_id }
      end

      #: () -> untyped
      def summary
        {
          completed_workflows: @workflows.values.count { |row| row.fetch("status") == "completed" },
          canceled_workflows: @workflows.values.count { |row| row.fetch("status") == "canceled" },
          side_effects: @side_effects,
          processed_outbox: @outbox.values.count { |row| row.fetch("status") == "processed" },
          workflows: @workflows.length,
          object_wakes_delivered: @object_wakes_delivered,
        }
      end

      # --- durable object store contract -------------------------------------
      # The object-command path is exercised directly through
      # DurableObjectExecutor#drain_object_inbox, so the mock implements the same
      # store methods that path touches: command enqueue, inbox claim (with
      # mailbox-sequence ordering and lease-expiry reclaim), completion that
      # applies state + ordered wake mutations atomically, retry/dead-letter, and
      # object-state reads. wake_due_timers converts matured wakes into ordinary
      # `wake` inbox messages carrying the wake name via method_name.

      #: (object_type: untyped, object_id: untyped, method_name: untyped, args: untyped, kwargs: untyped, ?message_kind: untyped, ?idempotency_key: untyped, ?max_attempts: untyped, ?worker_pool: untyped) -> untyped
      def enqueue_object_command(object_type:, object_id:, method_name:, args:, kwargs:, message_kind: "ask", idempotency_key: nil, max_attempts: nil, worker_pool: "default")
        id = next_id("inbox")
        @object_inbox[id] = object_inbox_row(
          id:,
          worker_pool:,
          object_type:,
          object_id:,
          message_kind:,
          method_name: method_name.to_s,
          payload: deep({ "method_name" => method_name.to_s, "args" => args, "kwargs" => kwargs }),
          max_attempts:,
        )
        trace("object_command_enqueued", id:, object_type:, object_id:, method: method_name.to_s)
        id
      end

      #: (target_kind: untyped, target_type: untyped, target_id: untyped, worker_id: untyped, ?lease_seconds: untyped, ?limit: untyped, ?now: untyped, ?worker_pool: untyped) -> untyped
      def claim_inbox_messages(target_kind:, target_type:, target_id:, worker_id:, lease_seconds: 60, limit: 1, now: nil, worker_pool: "default")
        now ||= scheduler.time
        rows = @object_inbox.values.select do |row|
          row.fetch("worker_pool") == worker_pool && row.fetch("target_kind") == target_kind &&
            row.fetch("target_type") == target_type && row.fetch("target_id") == target_id &&
            ["pending", "failed", "running", "dead_lettered"].include?(row.fetch("status"))
        end.sort_by { |row| row.fetch("sequence") }
        claimable = []
        rows.first(limit).each do |row|
          break unless object_inbox_claimable?(row, now)

          claimable << row
        end
        claimable.map do |row|
          row["status"] = "running"
          row["attempts"] = row.fetch("attempts").to_i + 1
          row["locked_by"] = worker_id
          row["locked_until"] = now + lease_seconds
          trace("object_inbox_claimed", id: row.fetch("id"), worker: worker_id)
          deep(row)
        end
      end

      #: (target_kind: untyped, target_type: untyped, target_id: untyped, ?worker_pool: untyped) -> untyped
      def inbox_messages_for(target_kind:, target_type:, target_id:, worker_pool: "default")
        @object_inbox.values.select do |row|
          row.fetch("worker_pool") == worker_pool && row.fetch("target_kind") == target_kind &&
            row.fetch("target_type") == target_type && row.fetch("target_id") == target_id
        end.sort_by { |row| row.fetch("sequence") }.map { |row| deep(row) }
      end

      #: (object_type: untyped, object_id: untyped, ?worker_pool: untyped) -> untyped
      def object_state_entry(object_type:, object_id:, worker_pool: "default")
        key = [worker_pool, object_type, object_id]
        @object_state.key?(key) ? deep(@object_state.fetch(key)) : Store::NO_OBJECT_STATE
      end

      #: (object_type: untyped, object_id: untyped, ?worker_pool: untyped) -> untyped
      def object_state(object_type:, object_id:, worker_pool: "default")
        entry = object_state_entry(worker_pool:, object_type:, object_id:)
        entry.equal?(Store::NO_OBJECT_STATE) ? nil : entry
      end

      #: (object_type: untyped, object_id: untyped, state: untyped, ?worker_pool: untyped) -> untyped
      def save_object_state(object_type:, object_id:, state:, worker_pool: "default")
        @object_state[[worker_pool, object_type, object_id]] = deep(state)
        trace("object_state_saved", object_type:, object_id:)
        AffectedRows.new(1)
      end

      #: (command_id: untyped, result: untyped, ?object_type: untyped, ?object_id: untyped, ?state: untyped, ?wakeup_changes: untyped, ?worker_id: untyped) -> untyped
      def complete_object_command(command_id:, result:, object_type: nil, object_id: nil, state: Store::NO_OBJECT_STATE, wakeup_changes: [], worker_id: nil)
        row = @object_inbox[command_id]
        return unless row
        return if worker_id && !object_command_owned?(row, worker_id)

        worker_pool = row.fetch("worker_pool")
        save_object_state(worker_pool:, object_type:, object_id:, state:) unless state.equal?(Store::NO_OBJECT_STATE)
        apply_object_wakeup_changes(worker_pool:, object_type:, object_id:, wakeup_changes:) unless wakeup_changes.empty?
        row["status"] = "completed"
        row["result"] = deep(result)
        row["error"] = nil
        row["locked_by"] = nil
        row["locked_until"] = nil
        trace("object_command_completed", id: command_id)
        AffectedRows.new(1)
      end

      #: (command_id: untyped, error: untyped, worker_id: untyped, ready_at: untyped) -> untyped
      def retry_object_command(command_id:, error:, worker_id:, ready_at:)
        row = @object_inbox[command_id]
        return unless row
        return if worker_id && !object_command_owned?(row, worker_id)

        row["status"] = "pending"
        row["error"] = error
        row["ready_at"] = ready_at
        row["locked_by"] = nil
        row["locked_until"] = nil
        trace("object_command_retry", id: command_id, ready_at:)
        AffectedRows.new(1)
      end

      #: (command_id: untyped, error: untyped, ?worker_id: untyped, ?terminal: untyped) -> untyped
      def fail_object_command(command_id:, error:, worker_id: nil, terminal: false)
        row = @object_inbox[command_id]
        return unless row
        return if worker_id && !object_command_owned?(row, worker_id)

        max_attempts = row.fetch("max_attempts")
        row["status"] = if terminal || (max_attempts && row.fetch("attempts").to_i >= max_attempts)
          "dead_lettered"
        else
          "failed"
        end
        row["error"] = error
        row["locked_by"] = nil
        row["locked_until"] = nil
        trace("object_command_failed", id: command_id, terminal:, status: row.fetch("status"))
        AffectedRows.new(1)
      end

      private

      #: (id: untyped, worker_pool: untyped, object_type: untyped, object_id: untyped, message_kind: untyped, method_name: untyped, payload: untyped, ?max_attempts: untyped) -> untyped
      def object_inbox_row(id:, worker_pool:, object_type:, object_id:, message_kind:, method_name:, payload:, max_attempts: nil)
        {
          "id" => id,
          "worker_pool" => worker_pool,
          "target_kind" => "object",
          "target_type" => object_type,
          "target_id" => object_id,
          "sequence" => allocate_object_sequence(worker_pool, "object", object_type, object_id),
          "message_kind" => message_kind,
          "method_name" => method_name,
          "payload" => payload,
          "status" => "pending",
          "attempts" => 0,
          "locked_by" => nil,
          "locked_until" => nil,
          "ready_at" => nil,
          "max_attempts" => max_attempts,
          "result" => nil,
          "error" => nil,
        }
      end

      #: (untyped, untyped) -> untyped
      def object_inbox_claimable?(row, now)
        status = row.fetch("status")
        return false if status == "dead_lettered"

        if status == "running"
          locked_until = row.fetch("locked_until")
          return false unless locked_until

          return locked_until < now
        end
        ready_at = row.fetch("ready_at")
        ready_at.nil? || ready_at <= now
      end

      #: (untyped, untyped) -> untyped
      def object_command_owned?(row, worker_id)
        row.fetch("status") == "running" &&
          row.fetch("locked_by") == worker_id &&
          row.fetch("locked_until") &&
          row.fetch("locked_until") >= scheduler.time
      end

      #: (worker_pool: untyped, object_type: untyped, object_id: untyped, wakeup_changes: untyped) -> untyped
      def apply_object_wakeup_changes(worker_pool:, object_type:, object_id:, wakeup_changes:)
        wakeup_changes.each do |change|
          case change.action
          when :schedule
            @object_wakeups[[worker_pool, object_type, object_id, change.name]] = {
              "worker_pool" => worker_pool,
              "object_type" => object_type,
              "object_id" => object_id,
              "name" => change.name,
              "wake_at" => change.wake_at,
              "payload" => deep(change.payload),
            }
            trace("object_wake_scheduled", object_id:, name: change.name, wake_at: change.wake_at)
          when :cancel
            removed = @object_wakeups.delete([worker_pool, object_type, object_id, change.name])
            trace("object_wake_canceled", object_id:, name: change.name) if removed
          when :cancel_all
            keys = @object_wakeups.keys.select { |key| key[0] == worker_pool && key[1] == object_type && key[2] == object_id }
            keys.each { |key| @object_wakeups.delete(key) }
            trace("object_wake_canceled_all", object_id:, removed: keys.length)
          else
            raise ArgumentError, "unknown durable object wakeup change #{change.action.inspect}"
          end
        end
      end

      #: (untyped) -> untyped
      def deliver_due_object_wakeups(now)
        due = @object_wakeups.values
          .select { |wakeup| wakeup.fetch("wake_at") <= now }
          .sort_by { |wakeup| [wakeup.fetch("worker_pool"), wakeup.fetch("object_type"), wakeup.fetch("object_id"), wakeup.fetch("name")] }
        due.each do |wakeup|
          worker_pool = wakeup.fetch("worker_pool")
          object_type = wakeup.fetch("object_type")
          object_id = wakeup.fetch("object_id")
          name = wakeup.fetch("name")
          id = next_id("inbox")
          @object_inbox[id] = object_inbox_row(
            id:,
            worker_pool:,
            object_type:,
            object_id:,
            message_kind: "wake",
            method_name: name,
            payload: deep(wakeup.fetch("payload")),
          )
          @object_wakeups.delete([worker_pool, object_type, object_id, name])
          @object_wakes_delivered += 1
          trace("object_wake_delivered", object_id:, name:)
        end
        due.length
      end

      #: (untyped, untyped, untyped, untyped) -> untyped
      def allocate_object_sequence(worker_pool, target_kind, target_type, target_id)
        key = [worker_pool, target_kind, target_type, target_id]
        @mailbox_sequences[key] = @mailbox_sequences.fetch(key, 0) + 1
      end

      #: (untyped) -> untyped
      def runnable?(row)
        case row.fetch("status")
        when "pending"
          row.fetch("next_run_at", nil).nil? || row.fetch("next_run_at") <= scheduler.time
        when "failed"
          retryable_failed?(row)
        when "canceling"
          canceling_due?(row)
        when "running"
          expired?(row)
        else
          false
        end
      end

      #: (untyped) -> untyped
      def retryable_failed?(row)
        next_run_at = row.fetch("next_run_at", nil)
        row.fetch("status") == "failed" && !next_run_at.nil? && next_run_at <= scheduler.time
      end

      #: (untyped) -> untyped
      def canceling_due?(row)
        next_run_at = row.fetch("next_run_at", nil)
        row.fetch("status") == "canceling" && (next_run_at.nil? || next_run_at <= scheduler.time)
      end

      #: (untyped) -> untyped
      def expired?(row)
        row.fetch("locked_until") && row.fetch("locked_until") < scheduler.time
      end

      #: (untyped, untyped) -> untyped
      def assert_workflow_lease!(workflow_id, worker_id)
        return if workflow_owned?(workflow_id:, worker_id:)

        raise Durababble::LeaseConflict, "workflow #{workflow_id} lease expired or moved before state update"
      end

      #: (untyped, untyped, untyped) -> untyped
      def claim_row(row, worker_id, lease_seconds)
        row["status"] = "running"
        row["locked_by"] = worker_id
        row["locked_until"] = scheduler.time + lease_seconds
        row["next_run_at"] = nil
        trace("workflow_claimed", id: row.fetch("id"), worker: worker_id)
        deep(row)
      end

      #: (untyped, untyped) -> untyped
      def complete_waits(waits, payload)
        completed = 0
        waits.each do |wait|
          row = @workflows.fetch(wait.fetch("workflow_id"))
          next unless [WorkflowStatus::WAITING, WorkflowStatus::RUNNING].include?(row.fetch("status"))

          wait["status"] = "completed"
          wait["payload"] = deep(payload)
          context = wait.fetch("context").merge(payload)
          record_step_completed(workflow_id: wait.fetch("workflow_id"), position: wait.fetch("position"), result: context)
          if row.fetch("status") == "waiting"
            row["status"] = "pending"
            row["locked_by"] = nil
            row["locked_until"] = nil
          end
          completed += 1
          trace("wait_completed", id: wait.fetch("workflow_id"), wait_id: wait.fetch("id"), payload:)
        end
        completed
      end

      #: (untyped) -> untyped
      def cancel_pending_waits_for_workflow(workflow_id)
        @waits.each_value do |wait|
          next unless wait.fetch("workflow_id") == workflow_id && wait.fetch("status") == "pending"

          wait["status"] = "canceled"
          trace("wait_canceled", id: workflow_id, wait_id: wait.fetch("id"))
        end
        @steps[workflow_id].each_value do |step|
          next unless step.fetch("status") == "waiting"

          step["status"] = "canceled"
          step["error"] = "workflow cancellation requested"
        end
        @attempts[workflow_id].each do |attempt|
          next unless attempt.fetch("status") == "waiting"

          attempt["status"] = "canceled"
          attempt["error"] = "workflow cancellation requested"
        end
      end

      #: (untyped, workflow_id: untyped, worker_id: untyped, operation: untyped) -> untyped
      def require_fenced_workflow_update!(row, workflow_id:, worker_id:, operation:)
        return unless worker_id
        return if row.fetch("status") == "running" && row.fetch("locked_by") == worker_id && !expired?(row)

        raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before #{operation}"
      end

      #: (untyped, untyped, untyped, untyped, untyped) -> untyped
      def update_latest_attempt(workflow_id, position, status, result, error)
        attempt = @attempts[workflow_id].reverse.find { |row| row.fetch("position") == position && ["running", "waiting"].include?(row.fetch("status")) }
        return unless attempt

        attempt["status"] = status
        attempt["result"] = deep(result)
        attempt["error"] = error
      end

      #: (workflow_id: untyped, kind: untyped, ?command_id: untyped, ?name: untyped, ?attempt_id: untyped, ?payload: untyped, ?error: untyped) -> untyped
      def append_history(workflow_id:, kind:, command_id: nil, name: nil, attempt_id: nil, payload: nil, error: nil)
        event = {
          "workflow_id" => workflow_id,
          "event_index" => @history[workflow_id].length,
          "kind" => kind,
          "command_id" => command_id,
          "name" => name,
          "attempt_id" => attempt_id,
          "payload" => deep(payload),
          "error" => error,
        }
        @history[workflow_id] << event
        event.fetch("event_index")
      end

      #: (untyped, untyped) -> untyped
      def normalize_command_id(command_id, position)
        id = command_id.nil? ? position : command_id
        raise ArgumentError, "command_id is required" if id.nil?

        id
      end

      #: (untyped) -> untyped
      def next_id(prefix)
        @id_seq += 1
        format("%s-%04d", prefix, @id_seq)
      end

      #: (untyped) -> untyped
      def deep(value)
        Marshal.load(Marshal.dump(value))
      end

      #: (untyped, ?untyped) -> untyped
      def trace(name, fields = {})
        scheduler.trace.event(scheduler.time, "virtual_yugabyte", name, fields)
      end
    end
  end
end
