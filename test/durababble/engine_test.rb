# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleEngineTest < DurababbleTestCase
  class CommandDrainWorkflow < Durababble::Workflow
    workflow_name "command-drain"

    expose_command def fail_first
      raise "stop"
    end

    expose_command def second
      "should not run"
    end
  end

  class CommandDrainStore
    attr_reader :claim_limits, :completed, :failed, :suspended

    def initialize(workflow_status: "waiting", messages: nil)
      @workflow_status = workflow_status
      @messages = messages || [
        { "id" => "msg-1", "message_kind" => "workflow_command", "method_name" => "fail_first", "payload" => { "method" => "fail_first", "args" => [], "kwargs" => {} } },
        { "id" => "msg-2", "message_kind" => "workflow_command", "method_name" => "second", "payload" => { "method" => "second", "args" => [], "kwargs" => {} } },
      ]
      @claim_limits = []
      @completed = []
      @failed = []
      @suspended = false
    end

    def workflow(workflow_id)
      { "id" => workflow_id, "status" => @workflow_status, "input" => {} }
    end

    def claim_workflow_for_activation(workflow_id:, worker_id:, lease_seconds:)
      { "id" => workflow_id, "status" => "running", "input" => {}, "locked_by" => worker_id, "lease_seconds" => lease_seconds }
    end

    def claim_inbox_messages(target_kind:, target_type:, target_id:, worker_id:, lease_seconds:, limit:)
      @claim_limits << limit
      return [] unless target_kind == "workflow" && target_type == "command-drain" && target_id == "wf-1"
      return [] unless worker_id == "worker-a" && lease_seconds == 9
      return [] unless @failed.empty?

      @messages.first(limit)
    end

    def complete_workflow_command(message_id:, workflow_id:, result:, worker_id:)
      @completed << { message_id:, workflow_id:, result:, worker_id: }
    end

    def fail_workflow_command(message_id:, workflow_id:, error:, worker_id:)
      @failed << { message_id:, workflow_id:, error:, worker_id: }
    end

    def suspend_workflow(workflow_id:, worker_id:)
      @suspended = [workflow_id, worker_id]
    end
  end

  class InlineRunStore
    attr_reader :created_workflows

    def initialize
      @created_workflows = []
      @workflows = {}
    end

    def create_workflow(name:, input:, worker_id:, lease_seconds:)
      @created_workflows << { name:, input:, worker_id:, lease_seconds: }
      @workflows["wf-inline"] = { "id" => "wf-inline", "name" => name, "status" => "running", "input" => input, "locked_by" => worker_id }
      "wf-inline"
    end

    def enqueue_workflow(name:, input:)
      raise "Engine#run should create the leased running workflow directly"
    end

    def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
      raise "Engine#run should not claim a workflow it just created"
    end

    def workflow_history_for(_workflow_id)
      []
    end

    def workflow_owned?(workflow_id:, worker_id:)
      row = @workflows.fetch(workflow_id)
      row.fetch("status") == "running" && row.fetch("locked_by") == worker_id
    end

    def workflow_cancellation(_workflow_id) = nil

    def complete_workflow(workflow_id, result:, worker_id: nil)
      @workflows[workflow_id] = @workflows.fetch(workflow_id).merge("status" => "completed", "result" => result, "locked_by" => nil)
    end

    def workflow(workflow_id)
      @workflows.fetch(workflow_id)
    end
  end

  class MigrationTrackingStore
    attr_reader :migrations, :enqueued

    def initialize
      @migrations = 0
      @enqueued = []
    end

    def migrate!
      @migrations += 1
    end

    def enqueue_workflow(name:, input:)
      @enqueued << { name:, input: }
      "wf-#{@enqueued.length}"
    end
  end

  class ImmediateWorkflow < Durababble::Workflow
    workflow_name "immediate"

    def execute(input)
      input.merge("done" => true)
    end
  end

  class FencedCompletionStore
    attr_reader :completed_with

    def initialize
      @row = { "id" => "wf-1", "status" => "pending", "input" => { "seed" => 1 } }
    end

    def migrate! = nil

    def workflow(workflow_id)
      @row.merge("id" => workflow_id)
    end

    def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
      @row = @row.merge("id" => workflow_id, "status" => "running", "locked_by" => worker_id, "locked_until" => Time.now + lease_seconds)
    end

    def workflow_history_for(_workflow_id) = []
    def workflow_cancellation(_workflow_id) = nil
    def workflow_owned?(workflow_id:, worker_id:) = raise("unexpected client-side lease check for #{workflow_id}/#{worker_id}")

    def complete_workflow(workflow_id, result:, worker_id: nil)
      @completed_with = { workflow_id:, result:, worker_id: }
      @row = @row.merge("status" => "completed", "result" => result, "locked_by" => nil, "locked_until" => nil)
    end
  end

  test "still supports requested injected crash points" do
    no_lease_store = Object.new
    crashy_engine = Durababble::Engine.new(store: no_lease_store, crash_after: :workflow_completed)
    assert_raises(Durababble::InjectedCrash) { crashy_engine.send(:crash!, :workflow_completed) }
  end

  test "does not run migrations from engine construction or enqueue helpers" do
    store = MigrationTrackingStore.new
    engine = Durababble::Engine.new(store:)

    workflow_id = engine.enqueue(ImmediateWorkflow, input: { "seed" => 1 })

    assert_equal "wf-1", workflow_id
    assert_equal 0, store.migrations
    assert_equal [{ name: "immediate", input: { "seed" => 1 } }], store.enqueued
  end

  test "passes worker ownership to terminal workflow status update without prechecking lease" do
    store = FencedCompletionStore.new
    engine = Durababble::Engine.new(store:, worker_id: "owner-a", lease_seconds: 17)

    run = engine.resume(ImmediateWorkflow, workflow_id: "wf-1")

    assert_equal "completed", run.status
    assert_equal(
      { workflow_id: "wf-1", result: { "seed" => 1, "done" => true }, worker_id: "owner-a" },
      store.completed_with,
    )
  end

  test "drains workflow command inbox one message at a time so failed heads block followers" do
    store = CommandDrainStore.new
    engine = Durababble::Engine.new(store:, worker_id: "worker-a", lease_seconds: 9)

    assert_equal 1, engine.drain_workflow_inbox(CommandDrainWorkflow, workflow_id: "wf-1", limit: 10)
    assert_equal [1, 1], store.claim_limits
    assert_empty store.completed
    assert_equal "msg-1", store.failed.fetch(0).fetch(:message_id)
    assert_match(/RuntimeError: stop/, store.failed.fetch(0).fetch(:error))
    assert_equal ["wf-1", "worker-a"], store.suspended
  end

  test "does not drain workflow command inboxes for terminal workflows" do
    store = CommandDrainStore.new(workflow_status: "completed")
    engine = Durababble::Engine.new(store:, worker_id: "worker-a", lease_seconds: 9)

    assert_equal 0, engine.drain_workflow_inbox(CommandDrainWorkflow, workflow_id: "wf-1", limit: 10)
    assert_empty store.claim_limits
    assert_empty store.failed
    assert_equal false, store.suspended
  end

  test "fails unsupported workflow inbox message kinds without dispatching followers" do
    store = CommandDrainStore.new(
      messages: [
        { "id" => "msg-bad", "message_kind" => "object_command" },
        { "id" => "msg-next", "message_kind" => "workflow_command", "method_name" => "second", "payload" => { "method" => "second", "args" => [], "kwargs" => {} } },
      ],
    )
    engine = Durababble::Engine.new(store:, worker_id: "worker-a", lease_seconds: 9)

    assert_equal 1, engine.drain_workflow_inbox(CommandDrainWorkflow, workflow_id: "wf-1", limit: 10)
    assert_empty store.completed
    assert_equal "msg-bad", store.failed.fetch(0).fetch(:message_id)
    assert_match(/unsupported workflow inbox message object_command/, store.failed.fetch(0).fetch(:error))
  end

  test "fails unknown workflow commands without dispatching followers" do
    store = CommandDrainStore.new(
      messages: [
        { "id" => "msg-missing", "message_kind" => "workflow_command", "method_name" => "missing", "payload" => { "method" => "missing", "args" => [], "kwargs" => {} } },
        { "id" => "msg-next", "message_kind" => "workflow_command", "method_name" => "second", "payload" => { "method" => "second", "args" => [], "kwargs" => {} } },
      ],
    )
    engine = Durababble::Engine.new(store:, worker_id: "worker-a", lease_seconds: 9)

    assert_equal 1, engine.drain_workflow_inbox(CommandDrainWorkflow, workflow_id: "wf-1", limit: 10)
    assert_empty store.completed
    assert_equal "msg-missing", store.failed.fetch(0).fetch(:message_id)
    assert_match(/UnknownCommand: missing/, store.failed.fetch(0).fetch(:error))
  end

  test "run creates a leased running workflow directly instead of enqueueing then claiming" do
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "inline-run"

      def execute(input)
        input.merge("done" => true)
      end
    end
    store = InlineRunStore.new
    engine = Durababble::Engine.new(store:, worker_id: "inline-worker", lease_seconds: 9, migrate: false)

    run = engine.run(workflow, input: { "count" => 2 })

    assert_equal "completed", run.status
    assert_equal({ "count" => 2, "done" => true }, run.result)
    assert_equal [{ name: "inline-run", input: { "count" => 2 }, worker_id: "inline-worker", lease_seconds: 9 }], store.created_workflows
  end

  durababble_store_backends.each do |backend|
    test "runs a workflow once and records durable step outputs with #{backend.name}" do
      with_durababble_store(backend, "engine_test") do |store|
        workflow = durababble_test_workflow("counter") do
          test_step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
          test_step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
        end

        engine = Durababble::Engine.new(store:)
        run = engine.run(workflow, input: { "count" => 2 })

        assert_equal "completed", run.status
        assert_equal({ "count" => 6 }, run.result)
        assert_equal(
          [
            ["increment", "completed"],
            ["double", "completed"],
          ],
          store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )
      end
    end

    test "returns a terminal failed workflow instead of trying to reclaim it with #{backend.name}" do
      with_durababble_store(backend, "engine_test") do |store|
        attempts = 0
        workflow = durababble_test_workflow("terminal-failure") do
          test_step("explode") do |_ctx|
            attempts += 1
            raise "boom"
          end
        end
        engine = Durababble::Engine.new(store:, worker_id: "owner")

        first = engine.run(workflow, input: {})
        second = engine.resume(workflow, workflow_id: first.id)

        assert_equal "failed", first.status
        assert_equal first.status, second.status
        assert_equal first.error, second.error
        assert_equal 1, attempts
        assert_nil store.workflow(first.id).fetch("next_run_at")
      end
    end

    test "can resume a due retry without rerunning completed steps with #{backend.name}" do
      with_durababble_store(backend, "engine_test") do |store|
        first_step_runs = 0
        attempts = 0
        workflow = durababble_test_workflow("flaky") do
          test_step("first") do |ctx|
            first_step_runs += 1
            { "count" => ctx.fetch("count") + 1 }
          end
          test_step("flaky", retry_policy: { initial_interval: 1, maximum_attempts: 2 }) do |ctx|
            attempts += 1
            raise "boom" if attempts == 1

            { "count" => ctx.fetch("count") + 10 }
          end
        end

        engine = Durababble::Engine.new(store:)
        scheduled = engine.run(workflow, input: { "count" => 1 })
        assert_equal "pending", scheduled.status
        refute_nil store.workflow(scheduled.id).fetch("next_run_at")

        store.make_workflow_due!(scheduled.id, now: Time.now + 2)
        resumed = engine.resume(workflow, workflow_id: scheduled.id)

        assert_equal "completed", resumed.status
        assert_equal({ "count" => 12 }, resumed.result)
        assert_equal 1, first_step_runs
        assert_equal 2, attempts
        assert_equal ["completed", "completed"], store.steps_for(resumed.id).map { |step| step.fetch("status") }
      end
    end

    test "replays a large completed step prefix without rerunning side effects with #{backend.name}" do
      with_durababble_store(backend, "engine_test") do |store|
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
            Durababble.wait_until(Time.now + 3600, ctx.merge("released" => true))
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

        assert_equal 1, store.wake_due_timers(now: Time.now + 3601)
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
  end
end
