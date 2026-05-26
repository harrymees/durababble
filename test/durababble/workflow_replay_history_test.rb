# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowReplayHistoryTest < DurababbleTestCase
  def scheduled_event(command_id, name: "step", payload: { "name" => name }, event_index: command_id)
    { "kind" => "step_scheduled", "command_id" => command_id, "event_index" => event_index, "name" => name, "payload" => payload }
  end

  def completed_event(command_id, event_index:)
    { "kind" => "step_completed", "command_id" => command_id, "event_index" => event_index, "payload" => { "ok" => true } }
  end

  def workflow_command_event(event_index:)
    { "kind" => "workflow_command_completed", "event_index" => event_index, "name" => "approve", "payload" => { "method" => "approve", "args" => [], "kwargs" => {}, "result" => { "approved" => true } } }
  end

  test "event_count reflects the number of persisted events" do
    history = Durababble::WorkflowReplayHistory.new([scheduled_event(0, event_index: 0), completed_event(0, event_index: 1)])

    assert_equal 2, history.event_count
  end

  test "remember_scheduled records the schedule and grows the event count" do
    history = Durababble::WorkflowReplayHistory.new([])

    history.remember_scheduled(1, step_name: "charge", shape: { "name" => "charge" })

    assert_equal 1, history.event_count
    assert history.recorded_schedule_matches?(1, { "name" => "charge" })
    refute history.recorded_schedule_matches?(1, { "name" => "refund" })
  end

  test "reserve_events! reserves projected history growth" do
    history = Durababble::WorkflowReplayHistory.new([scheduled_event(0, event_index: 0)])

    history.reserve_events!(2)

    assert_equal 3, history.event_count
  end

  test "validate_scheduled_shape! returns false without a recorded schedule" do
    history = Durababble::WorkflowReplayHistory.new([])

    refute history.validate_scheduled_shape!(workflow_id: "wf", command_id: 0, shape: { "name" => "step" })
  end

  test "validate_scheduled_shape! raises on a divergent shape" do
    history = Durababble::WorkflowReplayHistory.new([scheduled_event(0, payload: { "name" => "step" }, event_index: 0)])

    assert history.validate_scheduled_shape!(workflow_id: "wf", command_id: 0, shape: { "name" => "step" })
    assert_raises(Durababble::NonDeterminismError) do
      history.validate_scheduled_shape!(workflow_id: "wf", command_id: 0, shape: { "name" => "different" })
    end
  end

  test "validate_complete! raises when scheduled commands remain unconsumed" do
    history = Durababble::WorkflowReplayHistory.new([scheduled_event(5, name: "leftover", event_index: 0)])

    assert_nil history.validate_complete!(workflow_id: "wf", next_command_id: 6)
    assert_raises(Durababble::NonDeterminismError) do
      history.validate_complete!(workflow_id: "wf", next_command_id: 0)
    end
  end

  test "next_undeliverable_command_id flags terminal events whose future is missing" do
    history = Durababble::WorkflowReplayHistory.new([completed_event(0, event_index: 0)])

    # Future is absent for command 0 -> the terminal event cannot be delivered yet.
    assert_equal 0, history.next_undeliverable_command_id({})

    # Future is present for command 0 -> nothing is undeliverable.
    assert_nil history.next_undeliverable_command_id({ 0 => Durababble::CommandFuture.new(0) })
  end

  test "next_undeliverable_command_id returns nil once all terminal events are delivered" do
    history = Durababble::WorkflowReplayHistory.new([completed_event(0, event_index: 0)])
    delivered = []
    history.deliver_resolutions({ 0 => Durababble::CommandFuture.new(0) }) { |event, _future| delivered << event.fetch("command_id") }

    assert_equal [0], delivered
    assert_nil history.next_undeliverable_command_id({ 0 => Durababble::CommandFuture.new(0) })
  end

  test "workflow command delivery waits for preceding scheduled history consumption" do
    history = Durababble::WorkflowReplayHistory.new([
      scheduled_event(0, payload: { "name" => "wait" }, event_index: 0),
      workflow_command_event(event_index: 1),
    ])
    delivered = []

    assert_equal 0, history.deliver_workflow_commands { |event| delivered << event.fetch("name") }
    assert history.blocked_recorded_workflow_command?

    history.validate_scheduled_shape!(workflow_id: "wf", command_id: 0, shape: { "name" => "wait" })

    assert_equal 1, history.deliver_workflow_commands { |event| delivered << event.fetch("name") }
    assert_equal ["approve"], delivered
    refute history.blocked_recorded_workflow_command?
  end

  test "terminal resolution waits behind an earlier workflow command event" do
    history = Durababble::WorkflowReplayHistory.new([
      scheduled_event(0, payload: { "name" => "wait" }, event_index: 0),
      workflow_command_event(event_index: 1),
      completed_event(0, event_index: 2),
    ])
    future = Durababble::CommandFuture.new(0)
    resolutions = []

    history.validate_scheduled_shape!(workflow_id: "wf", command_id: 0, shape: { "name" => "wait" })
    history.deliver_resolutions({ 0 => future }) { |event, _future| resolutions << event.fetch("kind") }
    assert_empty resolutions

    history.deliver_workflow_commands { |_event| }
    history.deliver_resolutions({ 0 => future }) { |event, _future| resolutions << event.fetch("kind") }

    assert_equal ["step_completed"], resolutions
  end

  test "terminal resolution is delivered before a later workflow command event" do
    history = Durababble::WorkflowReplayHistory.new([
      scheduled_event(0, payload: { "name" => "wait" }, event_index: 0),
      completed_event(0, event_index: 1),
      workflow_command_event(event_index: 2),
    ])
    resolutions = []

    history.validate_scheduled_shape!(workflow_id: "wf", command_id: 0, shape: { "name" => "wait" })
    history.deliver_resolutions({ 0 => Durababble::CommandFuture.new(0) }) { |event, _future| resolutions << event.fetch("kind") }

    assert_equal ["step_completed"], resolutions
  end

  test "scheduled events without persisted indexes can still be consumed" do
    history = Durababble::WorkflowReplayHistory.new([
      { "kind" => "step_scheduled", "command_id" => 0, "name" => "legacy", "payload" => { "name" => "legacy" } },
    ])

    assert history.validate_scheduled_shape!(workflow_id: "wf", command_id: 0, shape: { "name" => "legacy" })
  end

  test "non-terminal step failures are not replay terminal events" do
    history = Durababble::WorkflowReplayHistory.new([
      { "kind" => "step_failed", "command_id" => 0, "event_index" => 0, "error" => "RuntimeError: retry" },
    ])

    assert_nil history.next_undeliverable_command_id({})
  end
end
