# frozen_string_literal: true

module Durababble
  Heartbeat = Data.define(:cursor, :recorder) do
    def record(cursor)
      recorder.call(cursor)
    end

    alias heartbeat record
  end

  class Engine
    DEFAULT_LEASE_SECONDS = 60

    def initialize(store:, worker_id: "inline-worker", lease_seconds: DEFAULT_LEASE_SECONDS, crash_after: nil, migrate: true)
      @store = store
      @worker_id = worker_id
      @lease_seconds = lease_seconds
      @crash_after = crash_after
      @store.migrate! if migrate
    end

    def run(workflow, input:)
      workflow_id = @store.enqueue_workflow(name: workflow.name, input:)
      resume(workflow, workflow_id:)
    end

    def resume(workflow, workflow_id:, claimed: nil)
      current = claimed || @store.workflow(workflow_id)
      return run_from_row(current) if current.fetch("status") == "completed"

      claimed ||= @store.claim_workflow(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)
      raise LeaseConflict, "workflow #{workflow_id} is leased by another worker" unless claimed

      crash!(:workflow_claimed)
      execute(workflow, workflow_id:, initial_input: claimed.fetch("input"))
    end

    private

    def execute(workflow, workflow_id:, initial_input: nil)
      context = initial_input || initial_context(workflow_id)
      completed_steps = @store.steps_for(workflow_id)
                              .select { |step| step.fetch("status") == "completed" }
                              .to_h { |step| [step.fetch("position").to_i, step] }

      workflow.steps.each_with_index do |step, position|
        if completed_steps.key?(position)
          context = completed_steps.fetch(position).fetch("result")
          next
        end

        @store.record_step_started(workflow_id:, position:, name: step.name)
        crash!(:step_started)
        heartbeat = Heartbeat.new(
          cursor: @store.step_heartbeat_cursor(workflow_id:, position:),
          recorder: lambda do |cursor|
            renewed = @store.heartbeat_step(workflow_id:, position:, worker_id: @worker_id, lease_seconds: @lease_seconds, cursor:)
            raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before heartbeat" unless renewed

            true
          end
        )
        begin
          output = step.call(context, heartbeat)
          if output.is_a?(WaitRequest)
            assert_workflow_lease!(workflow_id)
            @store.record_wait(workflow_id:, position:, name: step.name, wait_request: output)
            crash!(:wait_recorded)
            return snapshot(workflow_id)
          end

          context = output
          assert_workflow_lease!(workflow_id)
          @store.record_step_completed(workflow_id:, position:, result: context)
          crash!(:step_completed)
        rescue StandardError => e
          raise if e.is_a?(InjectedCrash) || e.is_a?(LeaseConflict)

          message = "#{e.class}: #{e.message}"
          assert_workflow_lease!(workflow_id)
          @store.record_step_failed(workflow_id:, position:, error: message)
          if step.retry_policy.retryable?(e, attempt_number: attempt_number(workflow_id, position))
            delay = step.retry_policy.delay_for_attempt(attempt_number(workflow_id, position))
            @store.schedule_workflow_retry(workflow_id:, worker_id: @worker_id, run_at: retry_run_at(delay))
          else
            @store.fail_workflow(workflow_id, error: message)
          end
          return snapshot(workflow_id)
        end
      end

      assert_workflow_lease!(workflow_id)
      @store.complete_workflow(workflow_id, result: context)
      crash!(:workflow_completed)
      snapshot(workflow_id)
    end

    def assert_workflow_lease!(workflow_id)
      return unless @store.respond_to?(:workflow_owned?)
      return if @store.workflow_owned?(workflow_id:, worker_id: @worker_id)

      raise LeaseConflict, "workflow #{workflow_id} lease expired or moved before state update"
    end

    def attempt_number(workflow_id, position)
      @store.step_attempts_for(workflow_id).count { |attempt| attempt.fetch("position").to_i == position }
    end

    def retry_run_at(delay)
      base = @store.respond_to?(:current_time) ? @store.current_time : Time.now
      base + delay
    end

    def initial_context(workflow_id)
      @store.workflow(workflow_id).fetch("input")
    end

    def snapshot(workflow_id)
      run_from_row(@store.workflow(workflow_id))
    end

    def run_from_row(row)
      Run.new(id: row.fetch("id"), status: row.fetch("status"), result: row["result"], error: row["error"])
    end

    def crash!(point)
      raise InjectedCrash, "injected crash after #{point}" if @crash_after == point
    end
  end
end
