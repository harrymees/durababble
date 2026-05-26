# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkerTest < DurababbleTestCase
  class WorkerTestStore
    include Durababble::TestSupport::FakeStoreCommandClaiming

    attr_reader :migrations, :claims, :resumed, :deliveries

    def initialize(claims)
      @claims = claims.dup
      @migrations = 0
      @resumed = []
      @step_attempts = []
      @deliveries = []
    end

    def migrate!
      @migrations += 1
    end

    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil, worker_pool: "default")
      claim = @claims.shift
      claim&.merge("claimed_by" => worker_id, "lease_seconds" => lease_seconds, "workflow_names" => workflow_names, "worker_pool" => worker_pool)
    end

    def claim_target_activation(worker_id:, lease_seconds:, target_kinds:, target_types:, worker_pool: "default")
      nil
    end

    def claim_workflow(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      {
        "id" => workflow_id,
        "name" => "unit",
        "worker_pool" => worker_pool,
        "status" => "running",
        "input" => { "value" => 1 },
        "claimed_by" => worker_id,
        "lease_seconds" => lease_seconds,
        "locked_by" => worker_id,
      }
    end

    def workflow(workflow_id)
      {
        "id" => workflow_id,
        "name" => "unit",
        "status" => "running",
        "input" => { "value" => 1 },
        "locked_by" => "worker-a",
      }
    end

    def deliver_target_message(**kwargs)
      @deliveries << kwargs
      true
    end

    def steps_for(_workflow_id)
      []
    end

    def step_attempts_for(_workflow_id)
      @step_attempts
    end

    def workflow_cancellation(_workflow_id)
      nil
    end

    def workflow_history_for(_workflow_id)
      []
    end

    def workflow_history_count_for(_workflow_id)
      0
    end

    def target_activation(**)
      nil
    end

    def claim_inbox_messages(**)
      []
    end

    def record_step_scheduled(workflow_id:, command_id:, name:, **)
      @resumed << [:scheduled, workflow_id, command_id, name]
    end

    def record_step_started(workflow_id:, name:, command_id: nil, position: nil, worker_id: nil)
      position ||= command_id
      @step_attempts << { "workflow_id" => workflow_id, "position" => position, "name" => name }
      @resumed << [:started, workflow_id, position, name]
    end

    def heartbeat_step(workflow_id:, worker_id:, lease_seconds:, cursor:, command_id: nil, position: nil)
      position ||= command_id
      @resumed << [:heartbeat, workflow_id, position, worker_id, lease_seconds, cursor]
      true
    end

    def step_heartbeat_cursor(workflow_id:, command_id: nil, position: nil)
      position ||= command_id
      @resumed << [:heartbeat_cursor, workflow_id, position]
      nil
    end

    def workflow_owned?(workflow_id:, worker_id:)
      @resumed << [:owned, workflow_id, worker_id]
      true
    end

    def record_step_completed(workflow_id:, result:, command_id: nil, position: nil, worker_id: nil)
      position ||= command_id
      @resumed << [:completed, workflow_id, position, result]
    end

    def record_step_failed(workflow_id:, error:, command_id: nil, position: nil, worker_id: nil, terminal: false, error_class: nil, error_message: nil)
      position ||= command_id
      @resumed << [:failed, workflow_id, position, error]
    end

    def complete_workflow(workflow_id, result:, worker_id: nil)
      @resumed << [:workflow_completed, workflow_id, result]
    end

    def fail_workflow(workflow_id, error:, worker_id: nil)
      @resumed << [:workflow_failed, workflow_id, error]
    end
  end

  class ActivationDeferralStore < WorkerTestStore
    attr_reader :completed_activations

    def initialize(locked_until:)
      super([])
      @locked_until = locked_until
      @activation_claimed = false
      @completed_activations = []
    end

    def claim_target_activation(worker_id:, lease_seconds:, target_kinds:, target_types:, worker_pool: "default")
      return if @activation_claimed

      @activation_claimed = true
      {
        "worker_pool" => worker_pool,
        "target_kind" => target_kinds.fetch(0),
        "target_type" => target_types.fetch(0),
        "target_id" => "wf-activated",
        "status" => "running",
        "locked_by" => worker_id,
        "lease_seconds" => lease_seconds,
      }
    end

    def claim_workflow_for_activation(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      @resumed << [:activation_claim, workflow_id, worker_id, lease_seconds]
      nil
    end

    def workflow(workflow_id)
      super.merge(
        "id" => workflow_id,
        "status" => "running",
        "locked_by" => "other-worker",
        "locked_until" => @locked_until.utc.iso8601(6),
      )
    end

    def complete_target_activation(**kwargs)
      @completed_activations << kwargs
    end
  end

  class AdvisoryDeliveryStore < WorkerTestStore
    attr_reader :completed_commands, :failed_commands, :rearmed, :reconciled

    def initialize(claimed: true)
      super([])
      @claimed = claimed
      @messages = [
        {
          "id" => "msg-1",
          "target_kind" => "workflow",
          "target_type" => "command-unit",
          "target_id" => "wf-command",
          "message_kind" => "workflow_command",
          "method_name" => "approve",
          "payload" => { "method" => "approve", "args" => [], "kwargs" => { reason: "operator" } },
        },
      ]
      @completed_commands = []
      @failed_commands = []
      @rearmed = []
      @reconciled = []
    end

    def claim_workflow_for_activation(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
      @resumed << [:activation_claim, workflow_id, worker_id, lease_seconds]
      return unless @claimed

      {
        "id" => workflow_id,
        "name" => "command-unit",
        "worker_pool" => worker_pool,
        "status" => "running",
        "input" => {},
        "locked_by" => worker_id,
      }
    end

    def target_activation(**)
      {
        "target_kind" => "workflow",
        "target_type" => "command-unit",
        "target_id" => "wf-command",
        "status" => "running",
      }
    end

    def claim_inbox_messages(**)
      message = @messages.shift
      message ? [message] : []
    end

    def complete_workflow_command(**kwargs)
      @completed_commands << kwargs
    end

    def fail_workflow_command(**kwargs)
      @failed_commands << kwargs
    end

    def suspend_workflow(**kwargs)
      @resumed << [:suspend, kwargs]
    end

    def reconcile_target_activation(**kwargs)
      @reconciled << kwargs
    end

    def rearm_target_activation(**kwargs)
      @rearmed << kwargs
    end
  end

  test "migrates by default and returns idle when no workflow is claimable" do
    store = WorkerTestStore.new([])
    worker = Durababble::Worker.new(store:, workflows: { "unit" => workflow }, worker_id: "worker-a")

    assert_equal :idle, worker.tick
    assert_equal 1, store.migrations
    assert_empty store.resumed
  end

  test "can skip migration for already-migrated stores" do
    store = WorkerTestStore.new([])

    Durababble::Worker.new(store:, workflows: { "unit" => workflow }, worker_id: "worker-a", migrate: false)

    assert_equal 0, store.migrations
  end

  test "resumes a claimed workflow with the same worker id and lease settings" do
    store = WorkerTestStore.new([{ "id" => "wf-1", "name" => "unit", "status" => "running", "input" => { "value" => 1 } }])
    worker = Durababble::Worker.new(
      store:,
      workflows: { "unit" => workflow },
      worker_id: "worker-a",
      lease_seconds: 17,
      migrate: false,
    )

    assert_equal :worked, worker.tick

    assert_includes store.resumed, [:started, "wf-1", 0, "increment"]
    assert_includes store.resumed, [:completed, "wf-1", 0, { "value" => 2 }]
    assert_includes store.resumed, [:workflow_completed, "wf-1", { "value" => 2 }]
  end

  test "stops run_until_idle when max_ticks is reached even if more work is queued" do
    store = WorkerTestStore.new([
      { "id" => "wf-1", "name" => "unit", "status" => "running", "input" => { "value" => 1 } },
      { "id" => "wf-2", "name" => "unit", "status" => "running", "input" => { "value" => 1 } },
      { "id" => "wf-3", "name" => "unit", "status" => "running", "input" => { "value" => 1 } },
    ])
    worker = Durababble::Worker.new(store:, workflows: { "unit" => workflow }, worker_id: "worker-a", migrate: false)

    assert_equal 2, worker.run_until_idle(max_ticks: 2)
    assert_equal 1, store.claims.length
  end

  test "forwards target activation and retries soon when another worker holds the active lease" do
    started_at = Time.now
    locked_until = Time.now + 45
    store = ActivationDeferralStore.new(locked_until:)
    worker = Durababble::Worker.new(
      store:,
      workflows: { "unit" => workflow },
      worker_id: "worker-a",
      lease_seconds: 17,
      migrate: false,
    )

    assert_equal :worked, worker.tick
    assert_equal [[:activation_claim, "wf-activated", "worker-a", 17]], store.resumed
    assert_equal(
      [
        {
          target_kind: "workflow",
          target_type: "unit",
          target_id: "wf-activated",
          worker_pool: "default",
        },
      ],
      store.deliveries,
    )
    assert_equal 1, store.completed_activations.length
    completion = store.completed_activations.first
    assert_hash_includes(
      completion,
      target_kind: "workflow",
      target_type: "unit",
      target_id: "wf-activated",
      worker_pool: "default",
      worker_id: "worker-a",
    )
    assert_in_delta(
      (started_at + Durababble::Worker::ACTIVATION_FORWARD_RETRY_SECONDS).to_f,
      completion.fetch(:now).to_f,
      0.5,
    )
    assert_operator completion.fetch(:now), :<, locked_until
  end

  test "delivers a workflow inbox from an advisory delivery through workflow execution" do
    store = AdvisoryDeliveryStore.new
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "command-unit"

      def execute(input)
        finish(input)
      end

      step def finish(input)
        input
      end

      expose_command def approve(reason:)
        { "approved_by" => reason }
      end
    end
    worker = Durababble::Worker.new(
      store:,
      workflows: { workflow.workflow_name => workflow },
      worker_id: "worker-a",
      lease_seconds: 17,
      migrate: false,
    )

    assert_equal(
      :worked,
      worker.deliver_target(target_kind: "workflow", target_type: "command-unit", target_id: "wf-command"),
    )

    assert_equal(
      [
        [:activation_claim, "wf-command", "worker-a", 17],
        [:scheduled, "wf-command", 0, "finish"],
        [:started, "wf-command", 0, "finish"],
        [:completed, "wf-command", 0, {}],
        [:workflow_completed, "wf-command", {}],
      ],
      store.resumed.reject { |event| [:owned, :heartbeat_cursor].include?(event.first) },
    )
    assert_equal(
      [
        {
          message_id: "msg-1",
          workflow_id: "wf-command",
          result: { "approved_by" => "operator" },
          worker_id: "worker-a",
        },
      ],
      store.completed_commands,
    )
    assert_empty store.failed_commands
    assert_equal(
      [
        {
          target_kind: "workflow",
          target_type: "command-unit",
          target_id: "wf-command",
          worker_pool: "default",
        },
      ],
      store.reconciled,
    )
  end

  test "ignores unsupported advisory deliveries and rearms when another owner holds the workflow lease" do
    store = AdvisoryDeliveryStore.new(claimed: false)
    worker = Durababble::Worker.new(
      store:,
      workflows: { workflow.workflow_name => workflow },
      worker_id: "worker-a",
      lease_seconds: 17,
      migrate: false,
    )

    assert_equal :idle, worker.deliver_target(target_kind: "object", target_type: "unit", target_id: "wf-command")
    assert_equal :idle, worker.deliver_target(target_kind: "workflow", target_type: "missing", target_id: "wf-command")
    assert_equal :worked, worker.deliver_target(target_kind: "workflow", target_type: "unit", target_id: "wf-command")

    assert_empty store.completed_commands
    assert_empty store.failed_commands
    assert_equal(
      [
        [:activation_claim, "wf-command", "worker-a", 17],
      ],
      store.resumed,
    )
    assert_equal 1, store.rearmed.length
    assert_hash_includes(
      store.rearmed.first,
      target_kind: "workflow",
      target_type: "unit",
      target_id: "wf-command",
      worker_pool: "default",
    )
    assert_equal(
      [
        {
          target_kind: "workflow",
          target_type: "unit",
          target_id: "wf-command",
          worker_pool: "default",
        },
      ],
      store.deliveries,
    )
  end

  private

  def workflow
    durababble_test_workflow("unit") do
      test_step("increment") { |ctx| ctx.merge("value" => ctx.fetch("value") + 1) }
    end
  end
end
