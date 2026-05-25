# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleDocumentationTest < DurababbleTestCase
  def read(path)
    File.read(File.join(root, path))
  end

  test "does not present removed Workflow.define as the current public API" do
    current_docs = [
      "README.md",
      "docs/content/README.md",
      "docs/content/workflows.md",
      "docs/content/durable-objects.md",
      "docs/content/reference.md",
      "docs/spec.md",
      "docs/content/architecture.md",
    ]

    current_docs.each do |path|
      refute_includes read(path), "Workflow.define", "#{path} still documents the removed API"
    end
  end

  test "ships RBS declarations for the public class API" do
    rbs = read("sig/durababble.rbs")

    assert_includes rbs, "class Workflow[Input, Output]"
    assert_includes rbs, "class DurableObject[Id, State]"
    assert_includes rbs, "def self.enqueue: (Input input"
    assert_includes rbs, "def self.ref: (Id durable_id"
    assert_includes rbs, "def self.at: (Id durable_id"
    assert_includes rbs, "def self.tell: (Id durable_id"
  end

  test "evaluates docs site examples against the implemented public API" do
    workflows = read("docs/content/workflows.md")
    durable_objects = read("docs/content/durable-objects.md")
    backend = durababble_store_backends.reverse.first

    with_durababble_store(backend, "docs_site_examples") do |store|
      remove_documentation_example_constants

      workflow_result = evaluate_marked_example(workflows, "workflow-example", binding)
      object_result = evaluate_marked_example(durable_objects, "durable-object-example", binding)

      assert_equal({ "payment_id" => "pay_card_123", "label_id" => "label_pay_card_123" }, workflow_result)
      assert_equal(1_000, object_result)
    ensure
      remove_documentation_example_constants
    end
  end

  private

  def root
    File.expand_path("../..", __dir__)
  end

  def marked_ruby_code(text, marker)
    pattern = /<!-- DOCS:#{Regexp.escape(marker)}:start -->(.*?)<!-- DOCS:#{Regexp.escape(marker)}:end -->/m
    match = text.match(pattern)
    assert(match, "docs content is missing #{marker} example markers")
    code = match[1].match(/```ruby\n(.*?)\n```/m)
    assert(code, "docs content #{marker} marker does not contain a ruby code block")
    code[1]
  end

  def evaluate_marked_example(markdown, marker, context)
    # rubocop:disable Security/Eval -- this test intentionally executes marked documentation examples.
    eval(marked_ruby_code(markdown, marker), context, "docs/content #{marker}")
    # rubocop:enable Security/Eval
  end

  def remove_documentation_example_constants
    Object.send(:remove_const, :FulfillOrder) if Object.const_defined?(:FulfillOrder)
    Object.send(:remove_const, :Payments) if Object.const_defined?(:Payments)
    Object.send(:remove_const, :Shipping) if Object.const_defined?(:Shipping)
    Object.send(:remove_const, :Account) if Object.const_defined?(:Account)
  end
end
