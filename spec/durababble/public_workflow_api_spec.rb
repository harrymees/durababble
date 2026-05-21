# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe "Durababble public workflow API", :integration do
  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_public_workflow_api_#{Process.pid}_#{SecureRandom.hex(4)}" }
  let(:store) { Durababble::Store.connect(database_url:, schema:) }

  class PublicApiCounterWorkflow < Durababble::Workflow
    def execute(input)
      incremented = increment(input)
      double(incremented)
    end

    step def increment(input)
      input.merge("count" => input.fetch("count") + 1, "key" => step_context.idempotency_key)
    end

    step def double(input)
      input.merge("count" => input.fetch("count") * 2)
    end
  end

  after do
    store&.drop_schema!
    store&.close
  end

  it "runs simple-looking workflow code and records method-derived durable steps" do
    run = Durababble::Engine.new(store:).run(PublicApiCounterWorkflow, input: { "count" => 2 })

    expect(run.status).to eq("completed")
    expect(run.result.fetch("count")).to eq(6)
    expect(run.result.fetch("key")).to eq("durababble:v1:workflow:#{run.id}:step:0")
    expect(store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] }).to eq([
      ["increment", "completed"],
      ["double", "completed"]
    ])
  end
end
