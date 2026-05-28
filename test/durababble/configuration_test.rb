# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleConfigurationTest < DurababbleTestCase
  def with_config(ivar, value)
    configured = Durababble.instance_variable_defined?(ivar)
    previous = Durababble.instance_variable_get(ivar) if configured
    Durababble.instance_variable_set(ivar, value)
    yield
  ensure
    if configured
      Durababble.instance_variable_set(ivar, previous)
    else
      Durababble.remove_instance_variable(ivar)
    end
  end

  test "max_workflow_history_events rejects non-positive limits" do
    with_config(:@max_workflow_history_events, 0) do
      error = assert_raises(ArgumentError) { Durababble.max_workflow_history_events }
      assert_match(/must be positive/, error.message)
    end
  end

  test "workflow_history_warning_events rejects non-positive limits" do
    with_config(:@workflow_history_warning_events, -1) do
      error = assert_raises(ArgumentError) { Durababble.workflow_history_warning_events }
      assert_match(/must be positive/, error.message)
    end
  end

  test "payload byte limits reject non-positive values and keep workflow args alias" do
    with_config(:@payload_limits, { step_output: 0 }) do
      error = assert_raises(ArgumentError) { Durababble.payload_limits }
      assert_match(/must be positive/, error.message)
    end

    with_config(:@payload_limits, { "workflow_args" => 123 }) do
      assert_equal 123, Durababble.payload_limits.fetch(:workflow_input)
      Durababble.payload_limits = { workflow_args: 456 }
      assert_equal 456, Durababble.payload_limits.fetch(:workflow_input)
    end
  end

  test "payload limits read environment overrides and reject unknown surfaces" do
    with_config(:@payload_limits, {}) do
      with_env("DURABABBLE_MAX_STEP_OUTPUT_BYTES", "321") do
        assert_equal 321, Durababble.payload_limits.fetch(:step_output)
      end
    end

    error = assert_raises(ArgumentError) do
      Durababble.enforce_payload_limit!(surface: :unknown_surface, bytesize: 1)
    end
    assert_match(/unknown payload limit surface: unknown_surface/, error.message)
  end

  test "payload-too-large messages omit the context suffix when no context is present" do
    error = Durababble::PayloadTooLarge.new(:workflow_input, bytesize: 2, max_bytes: 1)

    assert_nil(error.context)
    assert_equal("workflow input payload is 2 bytes, exceeding max 1 bytes", error.message)
  end

  test "default engine setter can clear the configured default store" do
    previous_engine = Durababble.default_engine
    previous_store = Durababble.default_store

    Durababble.default_engine = nil

    assert_nil(Durababble.default_engine)
    assert_nil(Durababble.default_store)
  ensure
    if previous_engine
      Durababble.default_engine = previous_engine
    else
      Durababble.default_store = previous_store
    end
  end

  test "warn_workflow_history_events stays silent below the warning threshold" do
    with_config(:@workflow_history_warning_events, 100) do
      refute Durababble.warn_workflow_history_events(workflow_id: "wf", history_events: 10, max_history_events: 1_000)
    end
  end

  test "wait_condition raises when called outside workflow orchestration" do
    error = assert_raises(Durababble::Error) { Durababble.wait_condition { true } }
    assert_match(/must run inside workflow orchestration/, error.message)
  end

  test "assert_fiber_isolation! passes when isolation_level is :fiber" do
    with_isolation_level(:fiber) do
      assert_nil Durababble.assert_fiber_isolation!
    end
  end

  test "assert_fiber_isolation! raises when isolation_level is :thread" do
    with_isolation_level(:thread) do
      error = assert_raises(Durababble::ConfigurationError) { Durababble.assert_fiber_isolation! }
      assert_match(/isolation_level = :fiber/, error.message)
      assert_match(/current: :thread/, error.message)
    end
  end

  test "Engine#execute refuses to boot under :thread isolation" do
    with_durababble_store(durababble_store_backends.first, "isolation_check") do |store|
      workflow_class = Class.new(Durababble::Workflow) do
        workflow_name "isolation-check-workflow"
        def execute(_input)
          :unreachable
        end
      end
      engine = Durababble::Engine.new(store:, worker_id: "isolation-check-worker")
      with_isolation_level(:thread) do
        assert_raises(Durababble::ConfigurationError) { engine.run(workflow_class, input: nil) }
      end
    end
  end

  private

  def with_env(key, value)
    previous = ENV.fetch(key, nil)
    value.nil? ? ENV.delete(key) : ENV[key] = value
    yield
  ensure
    previous.nil? ? ENV.delete(key) : ENV[key] = previous
  end

  def with_isolation_level(level)
    previous = ActiveSupport::IsolatedExecutionState.isolation_level
    ActiveSupport::IsolatedExecutionState.isolation_level = level
    yield
  ensure
    ActiveSupport::IsolatedExecutionState.isolation_level = previous
  end
end
