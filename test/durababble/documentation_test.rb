# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleDocumentationTest < DurababbleTestCase
  # Expected return value for each marked example. The map exists so adding a
  # new example forces a deliberate decision about what the example proves;
  # markers found on disk that are missing from this map fail the test loudly.
  EXPECTED_RESULTS = {
    "workflow-example" => { "payment_id" => "pay_card_123", "label_id" => "label_pay_card_123" },
    "durable-object-example" => 1_000,
    "patterns-sequential" => { "status" => "completed", "row_count" => 2 },
    "patterns-fanout" => [101, 102, 103],
    "patterns-bounded-concurrency" => [1, 2, 3, 4, 5],
    "patterns-saga" => {
      "status" => "failed",
      "steps" => [
        ["reserve_seat", "completed"],
        ["charge_card", "completed"],
        ["issue_ticket", "failed"],
        ["refund_card", "completed"],
        ["release_seat", "completed"],
      ],
    },
  }.freeze

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

  test "evaluates every marked docs example against the implemented public API" do
    examples = discover_marked_examples
    refute_empty examples, "expected to find at least one DOCS:<name>:start marker under docs/content"

    discovered_markers = examples.map { |example| example.fetch(:marker) }.sort
    expected_markers = EXPECTED_RESULTS.keys.sort
    assert_equal(
      expected_markers,
      discovered_markers,
      "EXPECTED_RESULTS keys must match the DOCS markers under docs/content (add or remove entries to match)",
    )

    backend = durababble_store_backends.reverse.first
    examples.each { |example| evaluate_example(example, backend) }
  end

  private

  def root
    File.expand_path("../..", __dir__)
  end

  def discover_marked_examples
    Dir[File.join(root, "docs/content/*.md")].sort.flat_map do |path|
      content = File.read(path)
      content.scan(/<!-- DOCS:([\w-]+):start -->(.*?)<!-- DOCS:\1:end -->/m).map do |marker, body|
        code_match = body.match(/```ruby\n(.*?)\n```/m)
        assert(code_match, "marker #{marker} in #{path} does not wrap a ruby code block")
        { marker:, path:, code: code_match[1] }
      end
    end
  end

  def evaluate_example(example, backend)
    marker = example.fetch(:marker)
    schema_suffix = "docs_#{marker.tr("-", "_")}"

    with_durababble_store(backend, schema_suffix) do |store|
      defined_before = Object.constants
      begin
        result = eval_example_code(example, store)
        if EXPECTED_RESULTS.key?(marker)
          assert_equal(
            EXPECTED_RESULTS.fetch(marker),
            result,
            "docs example #{marker} (#{example.fetch(:path)}) did not match its expected return value",
          )
        end
      ensure
        cleanup_constants(defined_before)
      end
    end
  end

  def eval_example_code(example, store)
    engine = Durababble::Engine.new(store:)
    location = "#{example.fetch(:path)}##{example.fetch(:marker)}"
    binding_for_example = build_example_binding(store, engine)
    # rubocop:disable Security/Eval -- this test intentionally executes marked documentation examples.
    eval(example.fetch(:code), binding_for_example, location)
    # rubocop:enable Security/Eval
  end

  def build_example_binding(store, engine)
    # Local variables defined in this method are visible to `eval` via the
    # returned binding, so `store ||= ...` and `engine ||= ...` in the example
    # pick up the test-provided values rather than connecting to the default
    # database.
    _ = [store, engine]
    binding
  end

  def cleanup_constants(defined_before)
    added = Object.constants - defined_before
    added.each do |const_name|
      Object.send(:remove_const, const_name) if Object.const_defined?(const_name, false)
    end
  end
end
