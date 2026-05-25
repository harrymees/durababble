# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkspaceNamespaceTest < DurababbleTestCase
  class NamespaceStoreDouble
    attr_reader :closed

    def initialize
      @closed = false
    end

    def close
      @closed = true
    end
  end

  def with_env(values)
    previous = values.each_with_object({}) { |(key, _), memo| memo[key] = ENV.fetch(key, nil) }
    values.each { |key, value| ENV[key] = value }
    yield
  ensure
    previous.each { |key, value| ENV[key] = value }
  end

  test "derives stable bounded schemas from workspace paths" do
    first = Durababble.workspace_schema("/tmp/symphony-workspaces/durababble/HAR-1263")
    same = Durababble.workspace_schema("/tmp/symphony-workspaces/durababble/HAR-1263")
    other = Durababble.workspace_schema("/tmp/symphony-workspaces/durababble/HAR-1264")

    assert_equal(first, same)
    refute_equal(first, other)
    assert_match(/\Adurababble_har_1263_[0-9a-f]{12}\z/, first)
    assert_operator(first.length, :<=, Durababble::MAX_SCHEMA_IDENTIFIER_LENGTH)
  end

  test "derived schema uses workspace placeholder when path basename has no identifier characters" do
    schema = Durababble.workspace_schema("/tmp/!!!")

    assert_match(/\Adurababble_workspace_[0-9a-f]{12}\z/, schema)
  end

  test "explicit environment overrides win before derived workspace defaults" do
    with_env(
      "DURABABBLE_DATABASE_URL" => "postgresql://example.invalid/explicit",
      "DURABABBLE_SCHEMA" => "explicit_schema",
      "DURABABBLE_WORKSPACE_ROOT" => "/tmp/symphony-workspaces/durababble/HAR-1263",
    ) do
      assert_equal("postgresql://example.invalid/explicit", Durababble.default_database_url)
      assert_equal("explicit_schema", Durababble.default_schema)
    end
  end

  test "derives the default schema from a sanitized workspace root" do
    with_env("DURABABBLE_SCHEMA" => nil, "DURABABBLE_WORKSPACE_ROOT" => "/tmp/Workspace With Caps") do
      assert_match(/\Adurababble_/, Durababble.workspace_schema)
    end
  end

  test "configure and store connection use the selected default schema" do
    with_env("DURABABBLE_SCHEMA" => "selected_workspace_schema") do
      configured = Durababble.configure(database_url: Durababble.default_database_url)

      assert_equal("selected_workspace_schema", configured.schema)
      assert_same(configured, Durababble.store)
    end
  rescue StandardError => e
    skip("Durababble configure smoke requires a reachable SQL database at #{Durababble.default_database_url}: #{e.class}: #{e.message}")
  ensure
    Durababble.default_store&.close
    Durababble.default_store = nil
  end

  test "store accessor raises until configured and returns configured store" do
    store = NamespaceStoreDouble.new
    Durababble.default_store = nil

    error = assert_raises(Durababble::Error) { Durababble.store }
    assert_match(/not configured/, error.message)

    Durababble.default_store = store
    assert_same(store, Durababble.store)
  ensure
    Durababble.default_store = nil
  end

  test "cancellation error exposes workflow id and default message" do
    default_error = Durababble::CancellationError.new(nil, workflow_id: "wf-1")
    reason_error = Durababble::CancellationError.new("operator request", workflow_id: "wf-2")

    assert_equal("workflow cancellation requested", default_error.message)
    assert_equal("wf-1", default_error.workflow_id)
    assert_nil(default_error.reason)
    assert_equal("operator request", reason_error.message)
    assert_equal("wf-2", reason_error.workflow_id)
    assert_equal("operator request", reason_error.reason)
  end

  test "worker runtime threads the selected default schema to owned stores" do
    runtime = nil
    with_env("DURABABBLE_SCHEMA" => "runtime_workspace_schema") do
      runtime = Durababble::WorkerRuntime.new(
        database_url: Durababble.default_database_url,
        workflows: {},
        worker_pool: "namespace-test",
        migrate: false,
      )

      assert_equal("runtime_workspace_schema", runtime.store.schema)
    end
  rescue StandardError => e
    skip("Durababble worker runtime schema smoke requires a reachable SQL database at #{Durababble.default_database_url}: #{e.class}: #{e.message}")
  ensure
    runtime&.close
  end

  private

  def safely_drop_schema(store)
    store.drop_schema!
  rescue StandardError
    nil
  end

  def safely_close(store)
    store.close
  rescue StandardError
    nil
  end
end
