# frozen_string_literal: true

module Durababble
  class Engine
    def initialize(store:)
      @store = store
      @store.migrate!
    end

    def run(workflow, input:)
      workflow_id = @store.create_workflow(name: workflow.name, input:)
      execute(workflow, workflow_id:)
    end

    def resume(workflow, workflow_id:)
      @store.mark_workflow_running(workflow_id)
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
        begin
          context = step.call(context)
          @store.record_step_completed(workflow_id:, position:, result: context)
        rescue StandardError => e
          message = "#{e.class}: #{e.message}"
          @store.record_step_failed(workflow_id:, position:, error: message)
          @store.fail_workflow(workflow_id, error: message)
          return snapshot(workflow_id)
        end
      end

      @store.complete_workflow(workflow_id, result: context)
      snapshot(workflow_id)
    end

    def initial_context(workflow_id)
      @store.workflow(workflow_id).fetch("input")
    end

    def snapshot(workflow_id)
      row = @store.workflow(workflow_id)
      Run.new(id: row.fetch("id"), status: row.fetch("status"), result: row["result"], error: row["error"])
    end
  end
end
