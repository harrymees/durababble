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

  test "keeps README examples on the implemented public API" do
    readme = read("README.md")
    workflow_example = marked_block(readme, "workflow-example")
    object_example = marked_block(readme, "durable-object-example")

    assert_includes workflow_example, "class FulfillOrder < Durababble::Workflow"
    assert_includes workflow_example, "Durababble::Engine.new(store:).run"
    assert_includes workflow_example, "FulfillOrder.ref(run.id, store:)"
    refute_includes workflow_example, "Workflow.start"
    refute_includes workflow_example, "Workflow.handle"

    assert_includes object_example, "class Account < Durababble::DurableObject"
    assert_includes object_example, "Account.ref(\"acct_123\", store:)"
    refute_includes object_example, "DurableObject.at"
    refute_includes object_example, "DurableObject.tell"
  end

  private

  def root
    File.expand_path("../..", __dir__)
  end

  def marked_block(text, marker)
    pattern = /<!-- README:#{Regexp.escape(marker)}:start -->(.*?)<!-- README:#{Regexp.escape(marker)}:end -->/m
    match = text.match(pattern)
    assert(match, "README is missing #{marker} example markers")
    match[1]
  end
end
