# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowReplayHistoryTest < DurababbleTestCase
  test "keeps the old replay divergence constant as a compatibility alias" do
    assert_same Durababble::ReplayDivergenceError, Durababble::NonDeterminismError
  end

  def scheduled_event(command_id, name: "step", payload: { "name" => name }, event_index: command_id)
    { "kind" => "step_scheduled", "command_id" => command_id, "event_index" => event_index, "name" => name, "payload" => payload }
  end

  def completed_event(command_id, event_index:)
    { "kind" => "step_completed", "command_id" => command_id, "event_index" => event_index, "payload" => { "ok" => true } }
  end

  def workflow_command_event(event_index:)
    { "kind" => "workflow_command_completed", "event_index" => event_index, "name" => "approve", "payload" => { "method" => "approve", "args" => [], "kwargs" => {}, "result" => { "approved" => true } } }
  end

  def wait_payload(kind: "timer", wake_at: Time.utc(2026, 1, 1), context: { "slept" => true })
    {
      "wait" => {
        "kind" => kind,
        "event_key" => nil,
        "wake_at" => wake_at,
      },
      "context" => context,
    }
  end

  test "event_count reflects the number of persisted events" do
    history = Durababble::WorkflowReplayHistory.new([scheduled_event(0, event_index: 0), completed_event(0, event_index: 1)])

    assert_equal 2, history.event_count
  end

  test "rejects duplicate scheduled history for the same command" do
    error = assert_raises(Durababble::NonDeterminismError) do
      Durababble::WorkflowReplayHistory.new([
        scheduled_event(0, name: "first", event_index: 0),
        scheduled_event(0, name: "second", event_index: 1),
      ])
    end

    assert_match(/duplicate step_scheduled history for command 0/, error.message)
  end

  test "rejects duplicate terminal history for the same command" do
    error = assert_raises(Durababble::NonDeterminismError) do
      Durababble::WorkflowReplayHistory.new([
        scheduled_event(0, event_index: 0),
        completed_event(0, event_index: 1),
        completed_event(0, event_index: 2),
      ])
    end

    assert_match(/duplicate terminal history for command 0/, error.message)
  end

  test "allows waiting commands to resolve later" do
    history = Durababble::WorkflowReplayHistory.new([
      scheduled_event(0, event_index: 0),
      { "kind" => "step_waiting", "command_id" => 0, "event_index" => 1, "name" => "sleep", "payload" => { "sleeping" => true } },
      completed_event(0, event_index: 2),
    ])
    future = Durababble::CommandFuture.new(0)
    delivered = []

    history.validate_scheduled_shape!(workflow_id: "wf", command_id: 0, shape: { "name" => "step" })
    history.deliver_resolutions({ 0 => future }) { |event, _future| delivered << event.fetch("kind") }

    assert_equal ["step_completed"], delivered
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
    assert_raises(Durababble::ReplayDivergenceError) do
      history.validate_scheduled_shape!(workflow_id: "wf", command_id: 0, shape: { "name" => "different" })
    end
  end

  test "validate_complete! raises when scheduled commands remain unconsumed" do
    history = Durababble::WorkflowReplayHistory.new([scheduled_event(5, name: "leftover", event_index: 0)])

    assert_nil history.validate_complete!(workflow_id: "wf", next_command_id: 6)
    assert_raises(Durababble::ReplayDivergenceError) do
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

  test "retrying step failures are not replay terminal events" do
    history = Durababble::WorkflowReplayHistory.new([
      { "kind" => "step_failed", "command_id" => 0, "event_index" => 0, "error" => "RuntimeError: retry", "payload" => { "retrying" => true } },
    ])

    assert_nil history.next_undeliverable_command_id({})
  end

  test "waiting_timer reads wait metadata from step_waiting history payloads" do
    wake_at = Time.utc(2026, 1, 1)
    history = Durababble::WorkflowReplayHistory.new([
      scheduled_event(0, name: "sleep", payload: { "name" => "sleep" }, event_index: 0),
      { "kind" => "step_waiting", "command_id" => 0, "event_index" => 1, "name" => "sleep", "payload" => wait_payload(wake_at:, context: { "slept" => true }) },
    ])

    assert_equal(
      { "kind" => "timer", "event_key" => nil, "wake_at" => wake_at, "context" => { "slept" => true } },
      history.waiting_timer(0),
    )
    assert_equal wake_at, history.earliest_unresolved_timer_wake_at
  end

  test "earliest_unresolved_timer_wake_at parses ISO timestamps without host time" do
    history = Durababble::WorkflowReplayHistory.new([
      scheduled_event(0, name: "sleep", payload: { "name" => "sleep" }, event_index: 0),
      { "kind" => "step_waiting", "command_id" => 0, "event_index" => 1, "name" => "sleep", "payload" => wait_payload(wake_at: "2026-01-02T00:00:00.000000Z") },
      scheduled_event(1, name: "sleep", payload: { "name" => "sleep" }, event_index: 2),
      { "kind" => "step_waiting", "command_id" => 1, "event_index" => 3, "name" => "sleep", "payload" => wait_payload(wake_at: "2026-01-01T00:00:00.000000Z") },
    ])

    Durababble::WorkflowExecutionContext.with_current(Object.new) do
      Durababble::WorkflowDeterminism.enforce(workflow_id: "wf") do
        assert_equal "2026-01-01T00:00:00.000000Z", history.earliest_unresolved_timer_wake_at
      end
    end
  end

  test "durable timestamp comparison normalizes explicit offsets" do
    utc = Durababble::DurableTime.durable_comparable("2026-01-01T00:00:00.000000Z")

    assert_equal utc, Durababble::DurableTime.durable_comparable("2026-01-01T01:30:00.000000+0130")
    assert_equal utc, Durababble::DurableTime.durable_comparable("2026-01-01T01:30:00.000000+01:30")
  end

  test "durable timestamp parsing supports timer math from backend strings" do
    start = Durababble::DurableTime.comparable("2026-01-01T00:00:00.000000Z")

    assert_equal Time.utc(2026, 1, 1, 1), start + 3600
  end

  test "waiting_timer falls back to scheduled wait metadata for legacy waiting payloads" do
    wake_at = Time.utc(2026, 1, 1)
    history = Durababble::WorkflowReplayHistory.new([
      scheduled_event(0, name: "sleep", payload: { "name" => "sleep", "wait" => { "kind" => "timer", "event_key" => nil, "wake_at" => wake_at } }, event_index: 0),
      { "kind" => "step_waiting", "command_id" => 0, "event_index" => 1, "name" => "sleep", "payload" => { "slept" => true } },
    ])

    assert_equal(
      { "kind" => "timer", "event_key" => nil, "wake_at" => wake_at, "context" => { "slept" => true } },
      history.waiting_timer(0),
    )
  end

  test "waiting_timer ignores non timer waits and forgotten timers" do
    wake_at = Time.utc(2026, 1, 1)
    history = Durababble::WorkflowReplayHistory.new([
      scheduled_event(0, name: "event", payload: { "name" => "event" }, event_index: 0),
      { "kind" => "step_waiting", "command_id" => 0, "event_index" => 1, "name" => "event", "payload" => wait_payload(kind: "event", wake_at:, context: {}) },
      scheduled_event(1, name: "sleep", payload: { "name" => "sleep" }, event_index: 2),
      { "kind" => "step_waiting", "command_id" => 1, "event_index" => 3, "name" => "sleep", "payload" => wait_payload(wake_at:) },
    ])

    assert_nil history.waiting_timer(0)
    refute_nil history.waiting_timer(1)

    history.forget_waiting_timer(1)

    assert_nil history.waiting_timer(1)
    assert_nil history.earliest_unresolved_timer_wake_at
  end

  test "interrupted wait conditions are not treated as timer waits" do
    wake_at = Time.utc(2026, 1, 1)
    history = Durababble::WorkflowReplayHistory.new([
      scheduled_event(0, name: "wait_condition", payload: { "name" => "wait_condition" }, event_index: 0),
      { "kind" => "step_waiting", "command_id" => 0, "event_index" => 1, "name" => "wait_condition", "payload" => wait_payload(wake_at:) },
      workflow_command_event(event_index: 2),
    ])

    assert_nil history.waiting_timer(0)
    assert_nil history.waiting_timer_or_child_workflow(0)
  end
end
