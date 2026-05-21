# frozen_string_literal: true

require "spec_helper"

RSpec.describe Durababble::Engine, :integration do
  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_engine_test_#{Process.pid}" }
  let(:store) { Durababble::Store.connect(database_url:, schema:) }

  after do
    store&.drop_schema!
    store&.close
  end

  it "runs a workflow once and records durable step outputs" do
    workflow = durababble_test_workflow("counter") do
      test_step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
      test_step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
    end

    engine = described_class.new(store:)
    run = engine.run(workflow, input: { "count" => 2 })

    expect(run.status).to eq("completed")
    expect(run.result).to eq({ "count" => 6 })
    expect(store.steps_for(run.id).map { |s| [s.fetch("name"), s.fetch("status")] }).to eq([
      ["increment", "completed"],
      ["double", "completed"]
    ])
  end

  it "can resume a previously failed workflow without rerunning completed steps" do
    attempts = 0
    workflow = durababble_test_workflow("flaky") do
      test_step("first") { |ctx| { "count" => ctx.fetch("count") + 1 } }
      test_step("flaky") do |ctx|
        attempts += 1
        raise "boom" if attempts == 1
        { "count" => ctx.fetch("count") + 10 }
      end
    end

    engine = described_class.new(store:)
    failed = engine.run(workflow, input: { "count" => 1 })
    expect(failed.status).to eq("failed")

    resumed = engine.resume(workflow, workflow_id: failed.id)

    expect(resumed.status).to eq("completed")
    expect(resumed.result).to eq({ "count" => 12 })
    expect(attempts).to eq(2)
    expect(store.steps_for(resumed.id).map { |s| s.fetch("status") }).to eq(["completed", "completed"])
  end
end
