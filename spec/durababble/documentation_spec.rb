# frozen_string_literal: true

require "spec_helper"

RSpec.describe "public documentation" do
  let(:root) { File.expand_path("../..", __dir__) }

  def read(path)
    File.read(File.join(root, path))
  end

  it "does not present removed Workflow.define as the current public API" do
    current_docs = %w[README.md docs/spec.md docs/architecture.md]

    current_docs.each do |path|
      expect(read(path)).not_to include("Workflow.define"), "#{path} still documents the removed API"
    end
  end

  it "marks the Shikibu comparison and old faithfulness review as historical" do
    expect(read("docs/shikibu-comparison.md")).to include("Historical note")
    expect(read("docs/spec-faithfulness-review.md")).to include("Historical note")
  end

  it "ships RBS declarations for the public class API" do
    rbs = read("sig/durababble.rbs")

    expect(rbs).to include("class Workflow[Input, Output]")
    expect(rbs).to include("class DurableObject[Id, State]")
    expect(rbs).to include("def self.enqueue: (Input input")
    expect(rbs).to include("def self.ref: (Id durable_id")
  end
end
