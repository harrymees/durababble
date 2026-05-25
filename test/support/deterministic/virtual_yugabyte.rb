# typed: true
# frozen_string_literal: true

require_relative "core"

module Durababble
  module Deterministic
    class VirtualYugabyte
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

      #: (name: untyped, input: untyped) -> untyped
      def enqueue_workflow(name:, input:)
        id = next_id("wf")
        @workflows[id] = { "id" => id, "name" => name, "status" => "pending", "input" => deep(input), "result" => nil, "error" => nil, "locked_by" => nil, "locked_until" => nil, "next_run_at" => nil }
        trace("enqueue_workflow", id:, name:)
        id
      end

      #: (name: untyped, input: untyped) -> untyped
      def create_workflow(name:, input:)
        id = enqueue_workflow(name:, input:)
        mark_workflow_running(id)
        id
      end

      #: (worker_id: untyped, lease_seconds: untyped, ?workflow_names: untyped) -> untyped
      def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil)
        workflow = @workflows.values.select { |row| runnable?(row) && (!workflow_names || workflow_names.include?(row.fetch("name"))) }.min_by { |row| row.fetch("id") }
        return unless workflow

        claim_row(workflow, worker_id, lease_seconds)
      end

      #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
      def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
        row = @workflows.fetch(workflow_id)
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

      #: (untyped) -> untyped
      def current_workflow_lease(workflow_id)
        row = @workflows.fetch(workflow_id)
        return unless row.fetch("status") == "running" && row.fetch("locked_by") && !expired?(row)

        { "workflow_id" => workflow_id, "worker_id" => row.fetch("locked_by"), "locked_until" => row.fetch("locked_until") }
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

      #: (untyped, ?worker_id: untyped, ?lease_seconds: untyped) -> untyped
      def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60)
        row = @workflows.fetch(workflow_id)
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

      #: (untyped, result: untyped) -> untyped
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

      #: (untyped, reason: untyped, ?result: untyped) -> untyped
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

      #: (untyped, error: untyped) -> untyped
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

      #: (workflow_id: untyped, command_id: untyped, name: untyped, ?args: untyped, ?kwargs: untyped, ?metadata: untyped) -> untyped
      def record_step_scheduled(workflow_id:, command_id:, name:, args: [], kwargs: {}, metadata: {})
        payload = { "name" => name, "args" => deep(args), "kwargs" => deep(kwargs) }.merge(deep(metadata))
        append_history(workflow_id:, kind: "step_scheduled", command_id:, name:, payload:)
        @steps[workflow_id][command_id] ||= { "workflow_id" => workflow_id, "position" => command_id, "command_id" => command_id, "name" => name, "status" => "scheduled", "result" => nil, "error" => nil, "heartbeat_cursor" => nil }
        trace("step_scheduled", id: workflow_id, command_id:, name:, payload:)
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, name: untyped) -> untyped
      def record_step_started(workflow_id:, name:, command_id: nil, position: nil)
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

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, result: untyped) -> untyped
      def record_step_completed(workflow_id:, result:, command_id: nil, position: nil)
        command_id = normalize_command_id(command_id, position)
        step = @steps[workflow_id].fetch(command_id)
        step["status"] = "completed"
        step["result"] = deep(result)
        update_latest_attempt(workflow_id, command_id, "completed", result, nil)
        append_history(workflow_id:, kind: "step_completed", command_id:, payload: result)
        trace("step_completed", id: workflow_id, command_id:, result:)
        fault_plan.after(:record_step_completed)
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped) -> untyped
      def record_step_failed(workflow_id:, error:, command_id: nil, position: nil)
        command_id = normalize_command_id(command_id, position)
        step = @steps[workflow_id].fetch(command_id)
        step["status"] = "failed"
        step["error"] = error
        update_latest_attempt(workflow_id, command_id, "failed", nil, error)
        append_history(workflow_id:, kind: "step_failed", command_id:, error:)
        trace("step_failed", id: workflow_id, command_id:, error:)
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, error: untyped) -> untyped
      def record_step_canceled(workflow_id:, error:, command_id: nil, position: nil)
        command_id = normalize_command_id(command_id, position)
        step = @steps[workflow_id].fetch(command_id)
        step["status"] = "canceled"
        step["error"] = error
        update_latest_attempt(workflow_id, command_id, "canceled", nil, error)
        append_history(workflow_id:, kind: "step_canceled", command_id:, error:)
        trace("step_canceled", id: workflow_id, command_id:, error:)
      end

      #: (workflow_id: untyped, ?command_id: untyped, ?position: untyped, name: untyped, wait_request: untyped, ?suspend_workflow: untyped) -> untyped
      def record_wait(workflow_id:, name:, wait_request:, command_id: nil, position: nil, suspend_workflow: true, worker_id: nil)
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
        complete_waits(@waits.values.select { |wait| wait.fetch("status") == "pending" && wait.fetch("kind") == "timer" && wait.fetch("wake_at") <= now }, {})
      end

      #: (untyped) -> untyped
      def waits_for(workflow_id)
        @waits.values.select { |wait| wait.fetch("workflow_id") == workflow_id }.sort_by { |wait| wait.fetch("id") }.map { |row| deep(row) }
      end

      #: (untyped) -> untyped
      def workflow_history_for(workflow_id)
        @history[workflow_id].map { |row| deep(row) }
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

      #: () -> untyped
      def summary
        {
          completed_workflows: @workflows.values.count { |row| row.fetch("status") == "completed" },
          canceled_workflows: @workflows.values.count { |row| row.fetch("status") == "canceled" },
          side_effects: @side_effects,
          processed_outbox: @outbox.values.count { |row| row.fetch("status") == "processed" },
          workflows: @workflows.length,
        }
      end

      private

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
