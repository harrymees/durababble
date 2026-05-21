# frozen_string_literal: true

require "spec_helper"

RSpec.describe Durababble::Worker do
  class WorkerSpecStore
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
      { "id" => workflow_id, "name" => "unit", "status" => "running", "input" => { "value" => 1 }, "claimed_by" => worker_id, "lease_seconds" => lease_seconds }
    end

    def workflow(workflow_id)
      { "id" => workflow_id, "name" => "unit", "status" => "running", "input" => { "value" => 1 } }
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

  let(:workflow) do
    durababble_test_workflow("unit") do
      test_step("increment") { |ctx| ctx.merge("value" => ctx.fetch("value") + 1) }
    end
  end

  it "migrates by default and returns idle when no workflow is claimable" do
    store = WorkerSpecStore.new([])
    worker = described_class.new(store:, workflows: { "unit" => workflow }, worker_id: "worker-a")

    expect(worker.tick).to eq(:idle)
    expect(store.migrations).to eq(1)
    expect(store.resumed).to be_empty
  end

  it "can skip migration for already-migrated stores" do
    store = WorkerSpecStore.new([])

    described_class.new(store:, workflows: { "unit" => workflow }, worker_id: "worker-a", migrate: false)

    expect(store.migrations).to eq(0)
  end

  it "resumes a claimed workflow with the same worker id and lease settings" do
    store = WorkerSpecStore.new([{ "id" => "wf-1", "name" => "unit", "status" => "running", "input" => { "value" => 1 } }])
    worker = described_class.new(store:, workflows: { "unit" => workflow }, worker_id: "worker-a", lease_seconds: 17, migrate: false)

    expect(worker.tick).to eq(:worked)

    expect(store.resumed).to include(
      [:started, "wf-1", 0, "increment"],
      [:completed, "wf-1", 0, { "value" => 2 }],
      [:workflow_completed, "wf-1", { "value" => 2 }]
    )
  end

  it "stops run_until_idle when max_ticks is reached even if more work is queued" do
    store = WorkerSpecStore.new([
      { "id" => "wf-1", "name" => "unit", "status" => "running", "input" => { "value" => 1 } },
      { "id" => "wf-2", "name" => "unit", "status" => "running", "input" => { "value" => 1 } },
      { "id" => "wf-3", "name" => "unit", "status" => "running", "input" => { "value" => 1 } }
    ])
    worker = described_class.new(store:, workflows: { "unit" => workflow }, worker_id: "worker-a", migrate: false)

    expect(worker.run_until_idle(max_ticks: 2)).to eq(2)
    expect(store.claims.length).to eq(1)
  end
end
