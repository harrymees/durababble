# frozen_string_literal: true

module Durababble
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

    def resume(workflow, workflow_id:)
      current = @store.workflow(workflow_id)
      return snapshot(workflow_id) if current.fetch("status") == "completed"

      claimed = @store.claim_workflow(workflow_id:, worker_id: @worker_id, lease_seconds: @lease_seconds)
      raise LeaseConflict, "workflow #{workflow_id} is leased by another worker" unless claimed

      crash!(:workflow_claimed)
      execute(workflow, workflow_id:)
    end

    private

    def execute(workflow, workflow_id:)
      context = initial_context(workflow_id)
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
        begin
          output = step.call(context)
          if output.is_a?(WaitRequest)
            @store.record_wait(workflow_id:, position:, name: step.name, wait_request: output)
            crash!(:wait_recorded)
            return snapshot(workflow_id)
          end

          context = output
          @store.record_step_completed(workflow_id:, position:, result: context)
          crash!(:step_completed)
        rescue StandardError => e
          raise if e.is_a?(InjectedCrash)

          message = "#{e.class}: #{e.message}"
          @store.record_step_failed(workflow_id:, position:, error: message)
          @store.fail_workflow(workflow_id, error: message)
          return snapshot(workflow_id)
        end
      end

      @store.complete_workflow(workflow_id, result: context)
      crash!(:workflow_completed)
      snapshot(workflow_id)
    end

    def initial_context(workflow_id)
      @store.workflow(workflow_id).fetch("input")
    end

    def snapshot(workflow_id)
      row = @store.workflow(workflow_id)
      Run.new(id: row.fetch("id"), status: row.fetch("status"), result: row["result"], error: row["error"])
    end

    def crash!(point)
      raise InjectedCrash, "injected crash after #{point}" if @crash_after == point
    end
  end
end
