# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleDocumentationTest < DurababbleTestCase
  def read(path)
    File.read(File.join(root, path))
  end

  test "does not present removed Workflow.define as the current public API" do
    current_docs = ["README.md", "docs/spec.md", "docs/architecture.md"]

    current_docs.each do |path|
      refute_includes read(path), "Workflow.define", "#{path} still documents the removed API"
    end
  end

  test "marks the Shikibu comparison and old faithfulness review as historical" do
    assert_includes read("docs/shikibu-comparison.md"), "Historical note"
    assert_includes read("docs/spec-faithfulness-review.md"), "Historical note"
  end

  test "ships RBS declarations for the public class API" do
    rbs = read("sig/durababble.rbs")

    assert_includes rbs, "class Workflow[Input, Output]"
    assert_includes rbs, "class DurableObject[Id, State]"
    assert_includes rbs, "def self.enqueue: (Input input"
    assert_includes rbs, "def self.ref: (Id durable_id"
  end

  private

  def root
    File.expand_path("../..", __dir__)
  end
end
