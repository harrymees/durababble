# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleEngineTest < DurababbleTestCase
  class CommandDrainWorkflow < Durababble::Workflow
    workflow_name "command-drain"

    def execute(_input)
      wait_condition { true }
    end

    expose_command def fail_first
      raise "stop"
    end

    expose_command def second
      "should not run"
    end
  end

  class CommandDrainStore
    include Durababble::TestSupport::FakeStoreCommandClaiming

    attr_reader :claim_limits, :completed, :failed, :suspended, :terminal

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
      @terminal = []
    end

    def workflow(workflow_id)
      { "id" => workflow_id, "status" => @workflow_status, "input" => {} }
    end

    def claim_workflow_for_activation(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      @workflow_status = "running"
      { "id" => workflow_id, "status" => "running", "input" => {}, "locked_by" => worker_id, "lease_seconds" => lease_seconds }
    end

    def workflow_history_for(_workflow_id)
      []
    end

    def workflow_history_count_for(_workflow_id)
      0
    end

    def workflow_owned?(workflow_id:, worker_id:)
      workflow_id == "wf-1" && worker_id == "worker-a" && @workflow_status == "running"
    end

    def workflow_cancellation(_workflow_id)
      nil
    end

    def target_activation(target_kind:, target_type:, target_id:, worker_pool: "default")
      return unless target_kind == "workflow" && target_type == "command-drain" && target_id == "wf-1"
      return if @messages.empty?

      { "target_kind" => target_kind, "target_type" => target_type, "target_id" => target_id, "status" => "pending" }
    end

    def claim_inbox_messages(worker_pool: "default", target_kind:, target_type:, target_id:, worker_id:, lease_seconds:, limit:)
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

    def complete_workflow(workflow_id, result:, worker_id: nil)
      @workflow_status = "completed"
      @terminal << { workflow_id:, result:, worker_id: }
    end

    def fail_workflow(workflow_id, error:, worker_id: nil)
      @workflow_status = "failed"
      @terminal << { workflow_id:, error:, worker_id: }
    end

    def suspend_workflow(workflow_id:, worker_id:)
      @suspended = [workflow_id, worker_id]
    end
  end

  class CapturingLogger
    attr_reader :warnings

    def initialize
      @warnings = []
    end

    def warn(message)
      @warnings << message
    end
  end

  def with_workflow_history_limit(limit)
    configured = Durababble.instance_variable_defined?(:@max_workflow_history_events)
    previous = Durababble.instance_variable_get(:@max_workflow_history_events) if configured
    Durababble.max_workflow_history_events = limit
    yield
  ensure
    if configured
      Durababble.max_workflow_history_events = previous
    elsif Durababble.instance_variable_defined?(:@max_workflow_history_events)
      Durababble.remove_instance_variable(:@max_workflow_history_events)
    end
  end

  def with_workflow_history_warning_events(limit)
    configured = Durababble.instance_variable_defined?(:@workflow_history_warning_events)
    previous = Durababble.instance_variable_get(:@workflow_history_warning_events) if configured
    Durababble.workflow_history_warning_events = limit
    yield
  ensure
    if configured
      Durababble.workflow_history_warning_events = previous
    elsif Durababble.instance_variable_defined?(:@workflow_history_warning_events)
      Durababble.remove_instance_variable(:@workflow_history_warning_events)
    end
  end

  def with_durababble_logger(logger)
    configured = Durababble.instance_variable_defined?(:@logger)
    previous = Durababble.instance_variable_get(:@logger) if configured
    Durababble.logger = logger
    yield
  ensure
    if configured
      Durababble.logger = previous
    elsif Durababble.instance_variable_defined?(:@logger)
      Durababble.remove_instance_variable(:@logger)
    end
  end

  def default_retry_shape
    {
      "retry" => {
        "initial_interval" => 1,
        "backoff_coefficient" => 2.0,
        "maximum_interval" => nil,
        "maximum_attempts" => 1,
        "schedule" => [],
        "non_retryable_errors" => [],
      },
    }
  end

  class InlineRunStore
    attr_reader :created_workflows

    def initialize
      @created_workflows = []
      @workflows = {}
    end

    def create_workflow(name:, input:, worker_id:, lease_seconds:, worker_pool: "default")
      @created_workflows << { name:, input:, worker_id:, lease_seconds:, worker_pool: }
      @workflows["wf-inline"] = { "id" => "wf-inline", "name" => name, "worker_pool" => worker_pool, "status" => "running", "input" => input, "locked_by" => worker_id }
      "wf-inline"
    end

    def enqueue_workflow(name:, input:, id: nil, worker_pool: "default")
      raise "Engine#run should create the leased running workflow directly"
    end

    def claim_workflow(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      raise "Engine#run should not claim a workflow it just created"
    end

    def workflow_history_for(_workflow_id)
      []
    end

    def workflow_history_count_for(_workflow_id)
      0
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

    def enqueue_workflow(name:, input:, id: nil, worker_pool: "default")
      @enqueued << { name:, input:, id:, worker_pool: }
      id || "wf-#{@enqueued.length}"
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

    def claim_workflow(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      @row = @row.merge("id" => workflow_id, "status" => "running", "locked_by" => worker_id, "locked_until" => Time.now + lease_seconds)
    end

    def workflow_history_for(_workflow_id) = []
    def workflow_history_count_for(_workflow_id) = 0
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

    assert_match(/\A[0-9a-f-]{36}\z/, workflow_id)
    assert_equal 0, store.migrations
    assert_equal [{ name: "immediate", input: { "seed" => 1 }, id: workflow_id, worker_pool: "default" }], store.enqueued
  end

  test "passes explicit workflow ids to the store enqueue path" do
    store = MigrationTrackingStore.new
    engine = Durababble::Engine.new(store:)

    workflow_id = engine.enqueue(ImmediateWorkflow, input: { "seed" => 1 }, id: "wf-explicit")

    assert_equal "wf-explicit", workflow_id
    assert_equal [{ name: "immediate", input: { "seed" => 1 }, id: "wf-explicit", worker_pool: "default" }], store.enqueued
  end

  test "engine enqueue writes workflows into its worker pool" do
    store = MigrationTrackingStore.new
    engine = Durababble::Engine.new(store:, worker_pool: "critical", migrate: false)

    workflow_id = engine.enqueue(ImmediateWorkflow, input: { "seed" => 1 })

    assert_match(/\A[0-9a-f-]{36}\z/, workflow_id)
    assert_equal [{ name: "immediate", input: { "seed" => 1 }, id: workflow_id, worker_pool: "critical" }], store.enqueued
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

    claimed = store.claim_workflow_for_activation(workflow_id: "wf-1", worker_id: "worker-a", lease_seconds: 9)
    engine.resume(CommandDrainWorkflow, workflow_id: "wf-1", claimed:)
    assert_equal [1, 1], store.claim_limits
    assert_empty store.completed
    assert_equal "msg-1", store.failed.fetch(0).fetch(:message_id)
    assert_match(/RuntimeError: stop/, store.failed.fetch(0).fetch(:error))
    assert_equal false, store.suspended
  end

  test "does not drain workflow command inboxes for terminal workflows" do
    store = CommandDrainStore.new(workflow_status: "completed")
    engine = Durababble::Engine.new(store:, worker_id: "worker-a", lease_seconds: 9)

    engine.resume(CommandDrainWorkflow, workflow_id: "wf-1")
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

    claimed = store.claim_workflow_for_activation(workflow_id: "wf-1", worker_id: "worker-a", lease_seconds: 9)
    engine.resume(CommandDrainWorkflow, workflow_id: "wf-1", claimed:)
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

    claimed = store.claim_workflow_for_activation(workflow_id: "wf-1", worker_id: "worker-a", lease_seconds: 9)
    engine.resume(CommandDrainWorkflow, workflow_id: "wf-1", claimed:)
    assert_empty store.completed
    assert_equal "msg-missing", store.failed.fetch(0).fetch(:message_id)
    assert_match(/UnknownCommand: missing/, store.failed.fetch(0).fetch(:error))
  end

  test "defaults the workflow history warning threshold to 8000 events" do
    configured = Durababble.instance_variable_defined?(:@workflow_history_warning_events)
    previous = Durababble.instance_variable_get(:@workflow_history_warning_events) if configured
    previous_env = ENV.delete("DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS")
    Durababble.remove_instance_variable(:@workflow_history_warning_events) if configured

    assert_equal(8_000, Durababble.workflow_history_warning_events)
  ensure
    ENV["DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS"] = previous_env if previous_env
    Durababble.workflow_history_warning_events = previous if configured
  end

  test "validates and logs workflow history warning configuration branches" do
    warning_configured = Durababble.instance_variable_defined?(:@workflow_history_warning_events)
    previous_warning = Durababble.instance_variable_get(:@workflow_history_warning_events) if warning_configured
    logger_configured = Durababble.instance_variable_defined?(:@logger)
    previous_logger = Durababble.instance_variable_get(:@logger) if logger_configured
    previous_env = ENV["DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS"]
    Durababble.remove_instance_variable(:@workflow_history_warning_events) if warning_configured
    Durababble.remove_instance_variable(:@logger) if logger_configured

    ENV["DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS"] = "12"
    assert_equal(12, Durababble.workflow_history_warning_events)

    Durababble.workflow_history_warning_events = 7
    assert_equal(7, Durababble.workflow_history_warning_events)

    Durababble.workflow_history_warning_events = 0
    assert_raises(ArgumentError) { Durababble.workflow_history_warning_events }

    Durababble.workflow_history_warning_events = 1
    assert_kind_of(Logger, Durababble.logger)
    Durababble.logger = nil
    assert_equal(true, Durababble.warn_workflow_history_events(workflow_id: "wf", history_events: 1, max_history_events: 10))
    assert_equal(false, Durababble.warn_workflow_history_events(workflow_id: "wf", history_events: 0, max_history_events: 10))
  ensure
    if previous_env
      ENV["DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS"] = previous_env
    else
      ENV.delete("DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS")
    end
    if warning_configured
      Durababble.workflow_history_warning_events = previous_warning
    elsif Durababble.instance_variable_defined?(:@workflow_history_warning_events)
      Durababble.remove_instance_variable(:@workflow_history_warning_events)
    end
    if logger_configured
      Durababble.logger = previous_logger
    elsif Durababble.instance_variable_defined?(:@logger)
      Durababble.remove_instance_variable(:@logger)
    end
  end

  test "run creates a leased running workflow directly instead of enqueueing then claiming" do
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "inline-run"

      def execute(input)
        input.merge("done" => true)
      end
    end
    store = InlineRunStore.new
    engine = Durababble::Engine.new(store:, worker_id: "inline-worker", lease_seconds: 9)

    run = engine.run(workflow, input: { "count" => 2 })

    assert_equal "completed", run.status
    assert_equal({ "count" => 2, "done" => true }, run.result)
    assert_equal [{ name: "inline-run", input: { "count" => 2 }, worker_id: "inline-worker", lease_seconds: 9, worker_pool: "default" }], store.created_workflows
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

    test "preserves the failing step backtrace in the persisted workflow error with #{backend.name}" do
      with_durababble_store(backend, "engine_test") do |store|
        workflow = durababble_test_workflow("backtrace-capture") do
          test_step("explode") { |_ctx| raise "boom" }
        end
        engine = Durababble::Engine.new(store:, worker_id: "owner")

        run = engine.run(workflow, input: {})

        assert_equal "failed", run.status
        assert_match(/RuntimeError: boom/, run.error)
        # The backtrace must survive so operators can locate the failing frame.
        assert_match(/engine_test\.rb:\d+/, run.error)
        assert_equal run.error, store.workflow(run.id).fetch("error")
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
          lease_seconds: 300,
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

    test "allows replay to reach the configured history limit exactly with #{backend.name}" do
      with_workflow_history_limit(6) do
        with_durababble_store(backend, "history_limit_at_max") do |store|
          second_step_runs = 0
          workflow = durababble_test_workflow("history-limit-at-max") do
            test_step("first") { |ctx| ctx.merge("count" => ctx.fetch("count") + 1) }
            test_step("second") do |ctx|
              second_step_runs += 1
              ctx.merge("count" => ctx.fetch("count") + 1)
            end
          end
          workflow_id = store.enqueue_workflow(name: workflow.name, input: { "count" => 0 })
          store.record_step_scheduled(workflow_id:, command_id: 0, name: "first", args: [{ "count" => 0 }], metadata: default_retry_shape)
          store.record_step_started(workflow_id:, command_id: 0, name: "first")
          store.record_step_completed(workflow_id:, command_id: 0, result: { "count" => 1 })

          run = Durababble::Engine.new(store:, worker_id: "history-at-max").resume(workflow, workflow_id:)

          assert_equal "completed", run.status
          assert_equal({ "count" => 2 }, run.result)
          assert_equal 1, second_step_runs
          assert_equal 6, store.workflow_history_count_for(workflow_id)
        end
      end
    end

    test "fails oversized histories before fetching replay rows with #{backend.name}" do
      with_workflow_history_limit(3) do
        with_durababble_store(backend, "history_limit_prefetch") do |store|
          workflow_id = store.enqueue_workflow(name: ImmediateWorkflow.workflow_name, input: { "count" => 0 })
          store.record_step_scheduled(workflow_id:, command_id: 0, name: "first", args: [{ "count" => 0 }], metadata: default_retry_shape)
          store.record_step_started(workflow_id:, command_id: 0, name: "first")
          store.record_step_completed(workflow_id:, command_id: 0, result: { "count" => 1 })
          store.record_step_scheduled(workflow_id:, command_id: 1, name: "second", args: [{ "count" => 1 }], metadata: default_retry_shape)
          history_fetches = 0
          store.define_singleton_method(:workflow_history_for) do |_workflow_id|
            history_fetches += 1
            raise "history should not be fetched once the count is over the configured limit"
          end

          run = Durababble::Engine.new(store:, worker_id: "history-prefetch").resume(ImmediateWorkflow, workflow_id:)

          assert_equal "failed", run.status
          assert_match(/Durababble::WorkflowHistoryLimitExceeded: workflow #{workflow_id} has 4 history events, exceeding max 3/, run.error)
          assert_equal 0, history_fetches
          assert_hash_includes store.workflow(workflow_id), "status" => "failed", "error" => run.error, "next_run_at" => nil, "locked_by" => nil
        end
      end
    end

    test "fails before scheduling new work when replay would exceed the configured history limit with #{backend.name}" do
      with_workflow_history_limit(5) do
        with_durababble_store(backend, "history_limit_exceeded") do |store|
          second_step_runs = 0
          workflow = durababble_test_workflow("history-limit-exceeded") do
            test_step("first") { |ctx| ctx.merge("count" => ctx.fetch("count") + 1) }
            test_step("second") do |ctx|
              second_step_runs += 1
              ctx.merge("count" => ctx.fetch("count") + 1)
            end
          end
          workflow_id = store.enqueue_workflow(name: workflow.name, input: { "count" => 0 })
          store.record_step_scheduled(workflow_id:, command_id: 0, name: "first", args: [{ "count" => 0 }], metadata: default_retry_shape)
          store.record_step_started(workflow_id:, command_id: 0, name: "first")
          store.record_step_completed(workflow_id:, command_id: 0, result: { "count" => 1 })

          run = Durababble::Engine.new(store:, worker_id: "history-over").resume(workflow, workflow_id:)
          replay = Durababble::Engine.new(store:, worker_id: "history-over-again").resume(workflow, workflow_id:)

          assert_equal "failed", run.status
          assert_match(/WorkflowHistoryLimitExceeded/, run.error)
          assert_equal run.error, replay.error
          assert_equal 0, second_step_runs
          assert_equal 1, store.steps_for(workflow_id).length
          assert_equal 3, store.workflow_history_count_for(workflow_id)
        end
      end
    end

    test "logs a warning when workflow history reaches the configured warning threshold with #{backend.name}" do
      logger = CapturingLogger.new
      with_workflow_history_limit(6) do
        with_workflow_history_warning_events(3) do
          with_durababble_logger(logger) do
            with_durababble_store(backend, "history_warning") do |store|
              workflow = durababble_test_workflow("history-warning") do
                test_step("first") { |ctx| ctx.merge("count" => ctx.fetch("count") + 1) }
                test_step("second") { |ctx| ctx.merge("count" => ctx.fetch("count") + 1) }
              end
              workflow_id = store.enqueue_workflow(name: workflow.name, input: { "count" => 0 })
              store.record_step_scheduled(workflow_id:, command_id: 0, name: "first", args: [{ "count" => 0 }], metadata: default_retry_shape)
              store.record_step_started(workflow_id:, command_id: 0, name: "first")
              store.record_step_completed(workflow_id:, command_id: 0, result: { "count" => 1 })

              run = Durababble::Engine.new(store:, worker_id: "history-warning").resume(workflow, workflow_id:)

              assert_equal "completed", run.status
              assert logger.warnings.any? { |message| message.include?("workflow #{workflow_id} has 3 workflow history events") }
              assert logger.warnings.any? { |message| message.include?("warning threshold is 3, max is 6") }
            end
          end
        end
      end
    end

    test "dead-letters pending workflow command activations after oversized history failure with #{backend.name}" do
      with_workflow_history_limit(2) do
        with_durababble_store(backend, "history_limit_activation") do |store|
          workflow = Class.new(Durababble::Workflow) do
            workflow_name "history-limit-terminal-activation"

            def execute(input)
              input
            end

            def ping
              "pong"
            end
            expose_command :ping
          end
          workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "count" => 0 })
          store.record_step_scheduled(workflow_id:, command_id: 0, name: "first", args: [{ "count" => 0 }], metadata: default_retry_shape)
          store.record_step_started(workflow_id:, command_id: 0, name: "first")
          store.record_step_completed(workflow_id:, command_id: 0, result: { "count" => 1 })
          message_id = store.enqueue_workflow_command(
            workflow_id:,
            workflow_name: workflow.workflow_name,
            method_name: "ping",
            payload: { "method" => "ping", "args" => [], "kwargs" => {} },
          )
          assert_hash_includes(
            store.target_activation(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id),
            "status" => "pending",
          )

          failed = Durababble::Engine.new(store:, worker_id: "history-terminal").resume(workflow, workflow_id:)
          worker = Durababble::Worker.new(store:, workflows: [workflow], worker_id: "activation-cleanup", migrate: false)
          assert_equal :worked, worker.tick
          assert_equal :idle, worker.tick

          assert_equal "failed", failed.status
          assert_match(/WorkflowHistoryLimitExceeded/, failed.error)
          assert_hash_includes store.workflow(workflow_id), "status" => "failed", "next_run_at" => nil, "locked_by" => nil
          assert_hash_includes store.inbox_message(message_id), "status" => "dead_lettered", "locked_by" => nil, "locked_until" => nil
          assert_match(/terminal failed/, store.inbox_message(message_id).fetch("error"))
          assert_nil store.target_activation(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id)
        end
      end
    end

    test "fails before retrying an incomplete scheduled command when attempt history would exceed the configured limit with #{backend.name}" do
      with_workflow_history_limit(2) do
        with_durababble_store(backend, "history_limit_attempt") do |store|
          step_runs = 0
          workflow = durababble_test_workflow("history-limit-attempt") do
            test_step("first") do |ctx|
              step_runs += 1
              ctx.merge("count" => ctx.fetch("count") + 1)
            end
          end
          workflow_id = store.enqueue_workflow(name: workflow.name, input: { "count" => 0 })
          store.record_step_scheduled(workflow_id:, command_id: 0, name: "first", args: [{ "count" => 0 }], metadata: default_retry_shape)

          run = Durababble::Engine.new(store:, worker_id: "history-attempt").resume(workflow, workflow_id:)

          assert_equal "failed", run.status
          assert_match(/WorkflowHistoryLimitExceeded/, run.error)
          assert_equal 0, step_runs
          assert_equal ["step_scheduled"], store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }
        end
      end
    end

    test "timer waits can complete at the configured history limit after recovery with #{backend.name}" do
      with_workflow_history_limit(3) do
        with_durababble_store(backend, "history_limit_wait") do |store|
          wake_at = Time.utc(2026, 5, 25, 4, 0, 0)
          workflow = Class.new(Durababble::Workflow) do
            workflow_name "history-limit-wait"

            define_method(:execute) do |input|
              sleep_until(wake_at, input.merge("slept" => true))
            end
          end
          workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "wait" })

          waiting = Durababble::Engine.new(store:, worker_id: "history-wait").resume(workflow, workflow_id:)
          assert_equal "waiting", waiting.status
          assert_equal 2, store.workflow_history_count_for(workflow_id)

          assert_equal 1, store.wake_due_timers(now: wake_at + 1)
          completed = Durababble::Engine.new(store:, worker_id: "history-wait-recover").resume(workflow, workflow_id:)

          assert_equal "completed", completed.status
          assert_equal({ "id" => "wait", "slept" => true }, completed.result)
          assert_equal 3, store.workflow_history_count_for(workflow_id)
          assert_equal ["completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
        end
      end
    end
  end
end
