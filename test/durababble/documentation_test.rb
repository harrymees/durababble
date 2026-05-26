# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

require "uri"

class DurababbleDocumentationTest < DurababbleTestCase
  # Expected return value for each marked example. The map exists so adding a
  # new example forces a deliberate decision about what the example proves;
  # markers found on disk that are missing from this map fail the test loudly.
  EXPECTED_RESULTS = {
    "workflow-example" => { "payment_id" => "pay_card_123", "label_id" => "label_pay_card_123" },
    "workflow-sleep-example" => { "status" => "completed", "sent_to" => "user_123", "message" => "renew subscription" },
    "workflow-cancellation-example" => {
      "status" => "canceled",
      "result" => nil,
      "steps" => [["mark_import_canceled", "completed"]],
    },
    "durable-object-example" => 1_000,
    "object-pattern-counter" => 2,
    "object-pattern-session-registry" => { "country" => "US", "plan" => "pro", "operation_id_recorded" => true },
    "object-pattern-batcher" => {
      "messages" => [],
      "flushes" => [{ "messages" => ["first", "second"], "reason" => "alarm" }],
    },
    "object-pattern-ttl-cache" => { "value" => nil, "expires_at" => nil, "expired" => true },
    "object-pattern-kv-coordinator" => { "enabled" => true, "version_recorded" => true },
    "object-pattern-room" => {
      "members" => ["session-a", "session-b"],
      "transcript" => [{ "from" => "session-a", "body" => "hello", "member_count" => 2 }],
    },
    "object-pattern-stream" => { "chunks" => ["a", "b", "c"], "cursor" => 2, "last_read" => ["a", "b"] },
    "object-pattern-rate-window" => [true, true, false, true],
    "object-pattern-document" => { "revision" => 2, "content" => "Hello world" },
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
      "docs/content/object-patterns.md",
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

    assert_includes rbs, "class Workflow[Input, Output, Dispatch = Object]"
    assert_includes rbs, "class DurableObject[Id, State, Dispatch = Object]"
    assert_includes rbs, "def self.enqueue: (Input input"
    assert_includes rbs, "def self.at: (String workflow_id"
    assert_includes rbs, "def self.at: (Id durable_id"
    assert_includes rbs, "def self.handle: (String workflow_id"
    assert_includes rbs, "def self.handle: (Id durable_id"
    refute_includes rbs, "def self.ref:"
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

  test "docs site examples can hide runnable setup from the presented snippets" do
    workflows = read("docs/content/workflows.md")
    durable_objects = read("docs/content/durable-objects.md")
    object_patterns = read("docs/content/object-patterns.md")

    workflow_visible = visible_marked_ruby_code(workflows, "workflow-example")
    assert_includes workflow_visible, "FulfillOrder.start({"
    assert_includes workflow_visible, "FulfillOrder.at(fulfillment.workflow_id)"
    assert_includes workflow_visible, "fulfillment.result"
    refute_includes workflow_visible, "module Payments"
    refute_includes workflow_visible, "module Shipping"
    refute_includes workflow_visible, "order ||="
    refute_includes workflow_visible, "Durababble::Store.connect"
    refute_includes workflow_visible, "Durababble::Worker.new"
    refute_includes workflow_visible, "Durababble::Engine.new"

    sleep_visible = visible_marked_ruby_code(workflows, "workflow-sleep-example")
    assert_includes sleep_visible, "sleep_until(reminder.fetch(\"send_at\"), reminder)"
    refute_includes sleep_visible, "step def sleep_until_reminder_time"
    refute_includes sleep_visible, "Durababble::Store.connect"
    refute_includes sleep_visible, "Durababble::Worker.new"

    cancellation_visible = visible_marked_ruby_code(workflows, "workflow-cancellation-example")
    assert_includes cancellation_visible, "rescue Durababble::CancellationError"
    assert_includes cancellation_visible, "handle.cancel(reason: \"user uploaded a replacement file\")"
    refute_includes cancellation_visible, "module Importer"
    refute_includes cancellation_visible, "Durababble::Store.connect"
    refute_includes cancellation_visible, "Durababble::Worker.new"

    object_visible = visible_marked_ruby_code(durable_objects, "durable-object-example")
    assert_includes object_visible, "account = Account.at(\"acct_readme\")"
    assert_includes object_visible, "account.credit(1_000)"
    refute_includes object_visible, "Account.tell"
    refute_includes object_visible, "Durababble::Store.connect"
    refute_includes object_visible, "Durababble::Worker.new"

    counter_visible = visible_marked_ruby_code(object_patterns, "object-pattern-counter")
    assert_includes counter_visible, "Counter.tell(\"global\", :increment, 3)"
    assert_includes counter_visible, "Counter.at(\"global\").value"
    refute_includes counter_visible, "Durababble::Store.connect"
    refute_includes counter_visible, "Durababble::Worker.new"
  end

  test "docs theme jsdelivr imports are pinned to explicit package versions" do
    paths = Dir[File.join(root, "docs/theme/**/*")].select do |path|
      File.file?(path) && [".html", ".js"].include?(File.extname(path)) && !path.include?("/vendor/")
    end
    cdn_urls = paths.flat_map do |path|
      File.read(path).scan(%r{https://cdn\.jsdelivr\.net/npm/[^'"\s)]+}).map { |url| [path, url] }
    end
    unpinned = cdn_urls.reject { |_path, url| pinned_jsdelivr_npm_url?(url) }

    assert_empty(
      unpinned.map { |path, url| "#{path.delete_prefix("#{root}/")}: #{url}" },
      "docs theme CDN imports must pin npm packages with an explicit @version",
    )
  end

  private

  def root
    File.expand_path("../..", __dir__)
  end

  def pinned_jsdelivr_npm_url?(url)
    package_path = URI.parse(url).path.delete_prefix("/npm/")
    if package_path.start_with?("@")
      package_path.match?(%r{\A@[^/]+/[^/@]+@[^/]+})
    else
      package_path.match?(%r{\A[^/@]+@[^/]+})
    end
  rescue URI::InvalidURIError
    false
  end

  def discover_marked_examples
    Dir[File.join(root, "docs/content/*.md")].sort.flat_map do |path|
      content = File.read(path)
      content.scan(/<!-- DOCS:([\w-]+):start -->(.*?)<!-- DOCS:\1:end -->/m).map do |marker, body|
        blocks = ruby_blocks(body)
        assert(blocks.any?, "marker #{marker} in #{path} does not wrap a ruby code block")
        { marker:, path:, code: blocks.join("\n\n") }
      end
    end
  end

  def marked_example_content(text, marker)
    pattern = /<!-- DOCS:#{Regexp.escape(marker)}:start -->(.*?)<!-- DOCS:#{Regexp.escape(marker)}:end -->/m
    match = text.match(pattern)
    assert(match, "docs content is missing #{marker} example markers")
    match[1]
  end

  def visible_marked_ruby_code(text, marker)
    content = marked_example_content(text, marker)
    visible_content = content.gsub(/<!-- DOCS:#{Regexp.escape(marker)}:hidden\b.*?-->/m, "")
    code = ruby_blocks(visible_content)
    assert(code.any?, "docs content #{marker} marker does not contain a visible ruby code block")
    code.join("\n\n")
  end

  def ruby_blocks(markdown)
    markdown.scan(/```ruby\n(.*?)\n```/m).map(&:first)
  end

  def evaluate_example(example, backend)
    marker = example.fetch(:marker)
    schema_suffix = "docs_#{marker.tr("-", "_")}"

    with_durababble_store(backend, schema_suffix) do |store|
      defined_before = Object.constants # rubocop:disable Sorbet/ConstantsFromStrings -- snapshotting constants for cleanup
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
        Durababble.default_store = nil
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
    added = Object.constants - defined_before # rubocop:disable Sorbet/ConstantsFromStrings -- diffing constants for cleanup
    added.each do |const_name|
      Object.send(:remove_const, const_name) if Object.const_defined?(const_name, false)
    end
  end
end
