# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkerTest < DurababbleTestCase
  class WorkerTestStore
    attr_reader :migrations, :claims, :resumed

    def initialize(claims)
      @claims = claims.dup
      @migrations = 0
      @resumed = []
    end

    def migrate!
      @migrations += 1
    end

    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil)
      claim = @claims.shift
      claim&.merge("claimed_by" => worker_id, "lease_seconds" => lease_seconds, "workflow_names" => workflow_names)
    end

    def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
      {
        "id" => workflow_id,
        "name" => "unit",
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

    def steps_for(_workflow_id)
      []
    end

    def step_attempts_for(_workflow_id)
      []
    end

    def record_step_started(workflow_id:, position:, name:)
      @resumed << [:started, workflow_id, position, name]
    end

    def heartbeat_step(workflow_id:, position:, worker_id:, lease_seconds:, cursor:)
      @resumed << [:heartbeat, workflow_id, position, worker_id, lease_seconds, cursor]
      true
    end

    def step_heartbeat_cursor(workflow_id:, position:)
      @resumed << [:heartbeat_cursor, workflow_id, position]
      nil
    end

    def workflow_owned?(workflow_id:, worker_id:)
      @resumed << [:owned, workflow_id, worker_id]
      true
    end

    def record_step_completed(workflow_id:, position:, result:)
      @resumed << [:completed, workflow_id, position, result]
    end

    def complete_workflow(workflow_id, result:)
      @resumed << [:workflow_completed, workflow_id, result]
    end

    def fail_workflow(workflow_id, error:)
      @resumed << [:workflow_failed, workflow_id, error]
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

  private

  def workflow
    durababble_test_workflow("unit") do
      test_step("increment") { |ctx| ctx.merge("value" => ctx.fetch("value") + 1) }
    end
  end
end
