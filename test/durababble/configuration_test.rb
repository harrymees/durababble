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
    with_config(:@max_step_output_bytes, 0) do
      error = assert_raises(ArgumentError) { Durababble.max_step_output_bytes }
      assert_match(/must be positive/, error.message)
    end

    with_config(:@max_workflow_input_bytes, 123) do
      assert_equal 123, Durababble.max_workflow_args_bytes
      Durababble.max_workflow_args_bytes = 456
      assert_equal 456, Durababble.max_workflow_input_bytes
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
end
