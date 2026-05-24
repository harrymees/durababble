# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleTemporalInspiredTest < DurababbleTestCase
  class TemporalCommandWorkflow < Durababble::Workflow
    workflow_name "temporal-command-workflow"

    def execute(input)
      wait_for_approval(input)
    end

    step def wait_for_approval(input)
      Durababble.wait_event(
        "workflow:#{step_context.workflow_id}:command:approve",
        input.merge("waiting_for" => "approve"),
      )
    end

    expose_command def approve(reason:)
      reason
    end
  end

  durababble_store_backends.each do |backend|
    test "replays a large completed step prefix without rerunning side effects with #{backend.name}" do
      with_durababble_store(backend, "temporal_inspired") do |store|
        store.migrate!
        side_effect_count = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "large-history-replay"

          define_method(:execute) do |input|
            ctx = input
            input.fetch("iterations").times do
              ctx = accumulate(ctx)
            end
            finish(wait_for_release(ctx))
          end

          define_method(:accumulate) do |ctx|
            side_effect_count += 1
            ctx.merge("count" => ctx.fetch("count") + 1)
          end

          define_method(:wait_for_release) do |ctx|
            Durababble.wait_event("large-history:#{ctx.fetch("id")}", ctx)
          end

          define_method(:finish) do |ctx|
            ctx.merge("finished" => true)
          end

          step :accumulate
          step :wait_for_release
          step :finish
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "large-history-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: workflow.name,
          input: { "id" => "history", "count" => 0, "iterations" => 75 },
        )

        assert_equal :worked, worker.tick
        assert_equal "waiting", store.workflow(workflow_id).fetch("status")
        assert_equal 75, side_effect_count
        assert_equal 76, store.steps_for(workflow_id).length
        assert_equal(
          75,
          store.steps_for(workflow_id).count do |step|
            step.fetch("name") == "accumulate" && step.fetch("status") == "completed"
          end,
        )

        assert_equal 1, store.signal_event("large-history:history", payload: { "released" => true })
        assert_equal :worked, worker.tick

        assert_hash_includes(
          store.workflow(workflow_id),
          "status" => "completed",
          "result" => {
            "id" => "history",
            "count" => 75,
            "iterations" => 75,
            "released" => true,
            "finished" => true,
          },
        )
        assert_equal 75, side_effect_count
        assert_equal 77, store.steps_for(workflow_id).length
        assert_equal ["completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }.uniq
      end
    end

    test "delivers exposed workflow commands through durable event waits with #{backend.name}" do
      with_durababble_store(backend, "temporal_inspired") do |store|
        store.migrate!
        worker = Durababble::Worker.new(
          store:,
          workflows: { TemporalCommandWorkflow.workflow_name => TemporalCommandWorkflow },
          worker_id: "command-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: TemporalCommandWorkflow.workflow_name,
          input: { "request_id" => "cancel-me" },
        )

        assert_equal :worked, worker.tick
        assert_equal "waiting", store.workflow(workflow_id).fetch("status")

        ref = TemporalCommandWorkflow.ref(workflow_id, store:)
        assert_equal 1, ref.approve(reason: "operator")
        assert_equal :worked, worker.tick

        assert_hash_includes(
          store.workflow(workflow_id),
          "status" => "completed",
          "result" => {
            "request_id" => "cancel-me",
            "waiting_for" => "approve",
            "method" => "approve",
            "args" => [],
            "kwargs" => { reason: "operator" },
          },
        )
        assert_equal ["completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
      end
    end

    test "keeps a durable object command idempotency key stable across retry with #{backend.name}" do
      with_durababble_store(backend, "temporal_inspired") do |store|
        store.migrate!
        seen_keys = []
        retrying_object = Class.new(Durababble::DurableObject) do
          object_type "temporal_retrying_object"

          def initialize_state
            { "committed_attempts" => 0 }
          end

          define_method(:write_with_retry) do |value|
            seen_keys << command_context.idempotency_key
            update_state({
              "committed_attempts" => current_state.fetch("committed_attempts") + 1,
              "value" => value,
            })
            raise "transient command failure" if command_context.attempt_number == 1

            current_state
          end
          expose_command :write_with_retry, retry: { maximum_attempts: 2, schedule: [0] }

          expose def snapshot
            current_state
          end
        end
        object = retrying_object.ref("object-1", store:)

        result = object.write_with_retry("persisted")

        assert_equal({ "committed_attempts" => 1, "value" => "persisted" }, result)
        assert_equal 2, seen_keys.length
        assert_equal 1, seen_keys.uniq.length
        assert_equal result, object.snapshot
      end
    end
  end
end
