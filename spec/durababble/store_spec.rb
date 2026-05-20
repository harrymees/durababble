# frozen_string_literal: true

require "spec_helper"

RSpec.describe Durababble::Store, :integration do
  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_test_#{Process.pid}" }
  let(:store) { described_class.connect(database_url:, schema:) }

  after do
    store&.drop_schema!
    store&.close
  end

  it "migrates and persists workflow plus step state in Yugabyte" do
    store.migrate!
    workflow_id = store.create_workflow(name: "demo", input: { "count" => 1 })

    store.record_step_started(workflow_id:, position: 0, name: "add_one")
    store.record_step_completed(workflow_id:, position: 0, result: { "count" => 2 })
    store.complete_workflow(workflow_id, result: { "count" => 2 })

    workflow = store.workflow(workflow_id)
    expect(workflow.fetch("status")).to eq("completed")
    expect(workflow.fetch("result")).to eq({ "count" => 2 })
    expect(store.steps_for(workflow_id).first.fetch("status")).to eq("completed")
  end
end
