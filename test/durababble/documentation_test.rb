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

    assert_includes rbs, "class Workflow[Input, Output, Dispatch = Object]"
    assert_includes rbs, "class DurableObject[Id, State, Dispatch = Object]"
    assert_includes rbs, "def self.enqueue: (Input input"
    assert_includes rbs, "def self.at: (String workflow_id"
    assert_includes rbs, "def self.at: (Id durable_id"
    assert_includes rbs, "def self.ref: (Id durable_id"
  end

  test "evaluates README examples against the implemented public API" do
    readme = read("README.md")
    backend = durababble_store_backends.reverse.first

    with_durababble_store(backend, "readme_examples") do |store|
      remove_readme_example_constants

      workflow_result = evaluate_readme_example(readme, "workflow-example", binding)
      object_result = evaluate_readme_example(readme, "durable-object-example", binding)

      assert_equal({ "payment_id" => "pay_card_123", "label_id" => "label_pay_card_123" }, workflow_result)
      assert_equal(1_000, object_result)
    ensure
      remove_readme_example_constants
    end
  end

  private

  def root
    File.expand_path("../..", __dir__)
  end

  def marked_ruby_code(text, marker)
    pattern = /<!-- README:#{Regexp.escape(marker)}:start -->(.*?)<!-- README:#{Regexp.escape(marker)}:end -->/m
    match = text.match(pattern)
    assert(match, "README is missing #{marker} example markers")
    code = match[1].match(/```ruby\n(.*?)\n```/m)
    assert(code, "README #{marker} marker does not contain a ruby code block")
    code[1]
  end

  def evaluate_readme_example(readme, marker, context)
    # rubocop:disable Security/Eval -- this test intentionally executes marked README examples.
    eval(marked_ruby_code(readme, marker), context, "README #{marker}")
    # rubocop:enable Security/Eval
  end

  def remove_readme_example_constants
    Object.send(:remove_const, :FulfillOrder) if Object.const_defined?(:FulfillOrder)
    Object.send(:remove_const, :Payments) if Object.const_defined?(:Payments)
    Object.send(:remove_const, :Shipping) if Object.const_defined?(:Shipping)
    Object.send(:remove_const, :Account) if Object.const_defined?(:Account)
  end
end
