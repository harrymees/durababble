# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

# Tests for the object-activation primitive landed alongside this file: the
# worker now calls `claim_object_lease` at the top of `process_object_activation`
# — mirroring `claim_workflow_for_activation` in `process_workflow_activation` —
# so that an activation establishes ownership in `durable_objects` even when the
# inbox is empty. Before this change the per-object lease was only set lazily
# inside `claim_inbox_messages` (gated on finding inbox rows), so consumers
# polling `current_object_lease` had no signal until an inbox message had been
# enqueued AND a drain iteration had started.
#
# The behavioral seam that PR #2 will widen sits exactly in the "empty inbox"
# scenarios below: activation now briefly takes the lease, drain finds nothing,
# and the ensure block releases. PR #2 will wrap that release with refcount
# logic so an in-flight stream RPC keeps the lease alive past activation.
class DurababbleWorkerObjectActivationTest < DurababbleTestCase
  CounterState = Data.define(:value) do
    def bump(amount) = with(value: value + amount)
  end

  class CounterObject < Durababble::DurableObject
    object_type "activation_counter"

    def initialize_state
      CounterState.new(value: 0)
    end

    expose_command def bump(amount)
      update_state(current_state.bump(amount))
    end

    expose def value
      current_state.value
    end
  end

  # Fake store that records every lease-related call against the unified object
  # lease, so we can assert ordering (claim before drain, release after drain)
  # without needing real DB observability. Each method returns reasonable values
  # for the worker's branches; the recorded `events` list is the contract.
  class RecordingObjectStore
    attr_reader :events, :completed_activations, :rearmed_activations, :reconciled_activations, :deliveries

    def initialize(claim_result: :win, messages: nil, current_lease: nil)
      @events = []
      @claim_result = claim_result
      @messages = messages || []
      @current_lease = current_lease
      @completed_activations = []
      @rearmed_activations = []
      @reconciled_activations = []
      @deliveries = []
    end

    def migrate! = (@events << :migrate!)

    def claim_runnable_workflow(**) = nil
    def claim_target_activation(**) = nil

    def claim_object_lease(worker_pool:, object_type:, object_id:, worker_id:, lease_seconds:)
      @events << [:claim_object_lease, worker_pool, object_type, object_id, worker_id, lease_seconds]
      return if @claim_result == :lose

      { "worker_pool" => worker_pool, "object_type" => object_type, "object_id" => object_id, "worker_id" => worker_id }
    end

    def claim_inbox_messages(target_kind:, target_type:, target_id:, worker_id:, lease_seconds:, limit: 1, worker_pool: "default")
      @events << [:claim_inbox_messages, target_kind, target_type, target_id, worker_id]
      message = @messages.shift
      message ? [message] : []
    end

    def release_object_lease(object_type:, object_id:, worker_id:)
      @events << [:release_object_lease, object_type, object_id, worker_id]
      true
    end

    def current_object_lease(_object_type, _object_id, worker_pool: nil)
      _ = worker_pool
      @current_lease
    end

    def deliver_target_message(**kwargs)
      @events << [:deliver_target_message, kwargs.fetch(:target_kind), kwargs.fetch(:target_type), kwargs.fetch(:target_id)]
      @deliveries << kwargs
      true
    end

    def complete_target_activation(**kwargs)
      @events << [:complete_target_activation, kwargs.fetch(:target_kind), kwargs.fetch(:target_type), kwargs.fetch(:target_id)]
      @completed_activations << kwargs
    end

    def rearm_target_activation(**kwargs)
      @events << [:rearm_target_activation, kwargs.fetch(:target_kind), kwargs.fetch(:target_type), kwargs.fetch(:target_id)]
      @rearmed_activations << kwargs
    end

    def reconcile_target_activation(**kwargs)
      @events << [:reconcile_target_activation, kwargs.fetch(:target_kind), kwargs.fetch(:target_type), kwargs.fetch(:target_id)]
      @reconciled_activations << kwargs
    end

    # Methods called by `drain_object_inbox` when a real message is dispatched.
    def object_state(object_type:, object_id:)
      _ = object_type
      _ = object_id
      { "value" => 0 }
    end

    def object_state_entry(object_type:, object_id:)
      object_state(object_type:, object_id:)
    end

    def complete_object_command(command_id:, result:, **kwargs)
      _ = command_id
      _ = result
      @events << [:complete_object_command]
      _ = kwargs
      ActiveRecord::Result.empty(affected_rows: 1)
    end

    def fail_object_command(**) = nil
    def retry_object_command(**) = nil
  end

  test "advisory activation eagerly claims the object lease before drain runs and releases on the way out" do
    store = RecordingObjectStore.new
    worker = build_worker(store)

    assert_equal :worked, worker.deliver_target(
      target_kind: "object",
      target_type: CounterObject.object_type,
      target_id: "abc",
    )

    # Eager claim must happen before any inbox interaction; release is the last
    # store call inside drain's ensure block.
    claim_index = store.events.index { |event| event[0] == :claim_object_lease }
    inbox_index = store.events.index { |event| event[0] == :claim_inbox_messages }
    release_index = store.events.index { |event| event[0] == :release_object_lease }
    refute_nil claim_index, "expected claim_object_lease to be invoked"
    refute_nil inbox_index, "expected claim_inbox_messages to be invoked"
    refute_nil release_index, "expected release_object_lease to be invoked"
    assert_operator claim_index, :<, inbox_index, "claim_object_lease must fire before any inbox claim"
    assert_operator inbox_index, :<, release_index, "release_object_lease must fire after drain"
  end

  test "advisory activation forwards and rearms (not reconciles) when the inbox is empty even though the lease was held" do
    store = RecordingObjectStore.new(messages: [])
    worker = build_worker(store)

    worker.deliver_target(
      target_kind: "object",
      target_type: CounterObject.object_type,
      target_id: "abc",
    )

    # Empty drain ⇒ forward + rearm. We deliberately do NOT reconcile here even
    # though we held the lease — there was no work to settle.
    assert_equal 1, store.rearmed_activations.length
    assert_empty store.reconciled_activations
    assert_equal 1, store.deliveries.length
    assert_hash_includes(
      store.rearmed_activations.first,
      target_kind: "object",
      target_type: CounterObject.object_type,
      target_id: "abc",
      worker_pool: "default",
    )
  end

  test "advisory activation reconciles when the claim wins and drain actually completes a command" do
    message = {
      "id" => "msg-1",
      "target_kind" => "object",
      "target_type" => CounterObject.object_type,
      "target_id" => "abc",
      "message_kind" => "ask",
      "method_name" => "bump",
      "payload" => { "method_name" => "bump", "args" => [1], "kwargs" => {} },
      "attempts" => 1,
    }
    store = RecordingObjectStore.new(messages: [message])
    worker = build_worker(store)

    worker.deliver_target(
      target_kind: "object",
      target_type: CounterObject.object_type,
      target_id: "abc",
    )

    assert_equal 1, store.reconciled_activations.length
    assert_empty store.rearmed_activations
  end

  test "advisory activation skips drain and rearms when another worker holds the object lease" do
    other_holder_until = Time.now + 12
    store = RecordingObjectStore.new(
      claim_result: :lose,
      current_lease: {
        "worker_id" => "other-worker",
        "locked_until" => other_holder_until.utc.iso8601(6),
      },
    )
    worker = build_worker(store)

    worker.deliver_target(
      target_kind: "object",
      target_type: CounterObject.object_type,
      target_id: "abc",
    )

    # Lost claim ⇒ never call claim_inbox_messages, never call release.
    refute(store.events.any? { |event| event[0] == :claim_inbox_messages })
    refute(store.events.any? { |event| event[0] == :release_object_lease })
    # Forward + rearm bounded by the other worker's locked_until.
    assert_equal 1, store.deliveries.length
    assert_equal 1, store.rearmed_activations.length
    ready_at = store.rearmed_activations.first.fetch(:ready_at)
    assert_kind_of Time, ready_at
    assert_operator ready_at, :<=, other_holder_until + 0.01
  end

  test "non-advisory activation completes the activation row with retry_time when claim is lost" do
    other_holder_until = Time.now + 30
    store = RecordingObjectStore.new(
      claim_result: :lose,
      current_lease: { "worker_id" => "other", "locked_until" => other_holder_until.utc.iso8601(6) },
    )
    worker = build_worker(store)

    activation = {
      "target_kind" => "object",
      "target_type" => CounterObject.object_type,
      "target_id" => "abc",
      "worker_pool" => "default",
    }
    worker.send(:process_target_activation, activation)

    # Worker forwards and then completes the activation row (status -> pending
    # with the future ready_at) so another worker can pick it up.
    assert_equal 1, store.deliveries.length
    assert_equal 1, store.completed_activations.length
    completion = store.completed_activations.first
    assert_hash_includes(
      completion,
      target_kind: "object",
      target_type: CounterObject.object_type,
      target_id: "abc",
      worker_pool: "default",
      worker_id: "worker-a",
    )
    assert_operator completion.fetch(:now), :<=, other_holder_until + 0.01
  end

  test "non-advisory activation forwards and completes when claim wins but inbox is empty" do
    store = RecordingObjectStore.new(messages: [])
    worker = build_worker(store)

    activation = {
      "target_kind" => "object",
      "target_type" => CounterObject.object_type,
      "target_id" => "abc",
      "worker_pool" => "default",
    }
    worker.send(:process_target_activation, activation)

    # Empty drain ⇒ behaves like a lost claim from the activation-row's POV:
    # forward and re-arm via complete_target_activation with a future ready_at,
    # because no work was actually consumed.
    assert_equal 1, store.deliveries.length
    assert_equal 1, store.completed_activations.length

    # But the lease WAS claimed and released — that's the new behavior.
    assert(store.events.any? { |event| event[0] == :claim_object_lease })
    assert(store.events.any? { |event| event[0] == :release_object_lease })
  end

  test "object_activation_retry_time falls back to now when no lease row exists" do
    store = RecordingObjectStore.new(current_lease: nil)
    worker = build_worker(store)

    before = Time.now
    retry_at = worker.send(:object_activation_retry_time, "counter", "abc")
    after = Time.now
    assert_kind_of Time, retry_at
    # No competing holder ⇒ retry immediately (bounded by call duration).
    assert_operator retry_at, :>=, before - 0.01
    assert_operator retry_at, :<=, after + 0.01
  end

  test "object_activation_retry_time clamps to the existing lease's locked_until" do
    other_until = Time.now + 17
    store = RecordingObjectStore.new(
      current_lease: { "worker_id" => "other", "locked_until" => other_until.utc.iso8601(6) },
    )
    worker = build_worker(store)

    retry_at = worker.send(:object_activation_retry_time, "counter", "abc")
    assert_kind_of Time, retry_at
    # locked_until is much later than the base jittered retry, so we should be
    # bounded by the base — but never push past the holder's deadline.
    assert_operator retry_at, :<=, other_until + 0.01
  end

  test "object_activation_retry_time recovers when locked_until is malformed" do
    store = RecordingObjectStore.new(
      current_lease: { "worker_id" => "other", "locked_until" => "not-a-timestamp" },
    )
    worker = build_worker(store)

    before = Time.now
    retry_at = worker.send(:object_activation_retry_time, "counter", "abc")
    # On parse failure we deliberately fall back to a jittered retry instead of
    # silently returning Time.now (a tight retry against a corrupted row would
    # spin); see the helper's rescue clause.
    assert_kind_of Time, retry_at
    assert_operator retry_at, :>=, before - 0.01
  end

  durababble_store_backends.each do |backend|
    test "with #{backend.name}: advisory activation establishes the durable_objects row even on an empty inbox" do
      with_durababble_store(backend, "activation_empty_inbox") do |store|
        assert_nil store.current_object_lease(CounterObject.object_type, "abc")

        worker = Durababble::Worker.new(
          store:,
          workflows: {},
          objects: [CounterObject],
          worker_id: "worker-eager",
          lease_seconds: 30,
          migrate: false,
        )
        worker.deliver_target(
          target_kind: "object",
          target_type: CounterObject.object_type,
          target_id: "abc",
        )

        # Lease was released inside drain's ensure block, so `current_object_lease`
        # is nil — but the eager claim left a `durable_objects` row behind, which
        # didn't exist before this call. That row's presence is the strong signal
        # that ownership was established at some point during processing.
        assert_nil store.current_object_lease(CounterObject.object_type, "abc")
        row = fetch_durable_objects_row(store, CounterObject.object_type, "abc")
        refute_nil row, "expected durable_objects row to be created by eager claim"
      end
    end

    test "with #{backend.name}: advisory activation drains an enqueued command and releases the lease" do
      with_durababble_store(backend, "activation_drains") do |store|
        store.enqueue_object_command(
          object_type: CounterObject.object_type,
          object_id: "abc",
          method_name: "bump",
          args: [3],
          kwargs: {},
        )
        worker = Durababble::Worker.new(
          store:,
          workflows: {},
          objects: [CounterObject],
          worker_id: "worker-drain",
          lease_seconds: 30,
          migrate: false,
        )

        worker.deliver_target(
          target_kind: "object",
          target_type: CounterObject.object_type,
          target_id: "abc",
        )

        # `complete_object_command` persists the Data struct as-is (not a hash);
        # the backends round-trip Ruby objects through `object_state`. Pin the
        # value through the public ref API rather than relying on shape.
        assert_equal 3, CounterObject.handle("abc", store:).value
        assert_nil store.current_object_lease(CounterObject.object_type, "abc")
      end
    end

    test "with #{backend.name}: activation declines to drain when another worker already holds the object lease" do
      with_durababble_store(backend, "activation_contested") do |store|
        store.enqueue_object_command(
          object_type: CounterObject.object_type,
          object_id: "abc",
          method_name: "bump",
          args: [1],
          kwargs: {},
        )
        held = store.claim_object_lease(
          worker_pool: "default",
          object_type: CounterObject.object_type,
          object_id: "abc",
          worker_id: "other-worker",
          lease_seconds: 30,
        )
        refute_nil held, "test fixture: lease pre-claim must succeed"

        worker = Durababble::Worker.new(
          store:,
          workflows: {},
          objects: [CounterObject],
          worker_id: "worker-contender",
          lease_seconds: 30,
          migrate: false,
        )
        worker.deliver_target(
          target_kind: "object",
          target_type: CounterObject.object_type,
          target_id: "abc",
        )

        # The contender lost the claim, so it must not drain — no state was
        # persisted (state stays uninitialized), and the original holder still
        # owns the row.
        assert_nil store.object_state(object_type: CounterObject.object_type, object_id: "abc")
        current = store.current_object_lease(CounterObject.object_type, "abc")
        refute_nil current
        assert_equal "other-worker", current.fetch("worker_id")
      end
    end

    test "with #{backend.name}: an expired competing lease no longer blocks the activation claim" do
      with_durababble_store(backend, "activation_expired_competitor") do |store|
        store.enqueue_object_command(
          object_type: CounterObject.object_type,
          object_id: "abc",
          method_name: "bump",
          args: [4],
          kwargs: {},
        )
        store.claim_object_lease(
          worker_pool: "default",
          object_type: CounterObject.object_type,
          object_id: "abc",
          worker_id: "dead-worker",
          lease_seconds: 30,
        )
        expire_object_lease!(store, backend, CounterObject.object_type, "abc")
        assert_nil store.current_object_lease(CounterObject.object_type, "abc")

        worker = Durababble::Worker.new(
          store:,
          workflows: {},
          objects: [CounterObject],
          worker_id: "worker-recovery",
          lease_seconds: 30,
          migrate: false,
        )
        worker.deliver_target(
          target_kind: "object",
          target_type: CounterObject.object_type,
          target_id: "abc",
        )

        # Activation took over the expired lease, drained the command, then
        # released. Final state: lease released, but command effect applied.
        assert_equal 4, CounterObject.handle("abc", store:).value
        assert_nil store.current_object_lease(CounterObject.object_type, "abc")
      end
    end
  end

  private

  def build_worker(store)
    Durababble::Worker.new(
      store:,
      workflows: {},
      objects: [CounterObject],
      worker_id: "worker-a",
      lease_seconds: 17,
      migrate: false,
    )
  end

  def fetch_durable_objects_row(store, object_type, object_id)
    table = store.send(:table, "durable_objects")
    placeholders = if store.respond_to?(:postgres_placeholders, true)
      "object_type = $1 AND object_id = $2"
    else
      "object_type = ? AND object_id = ?"
    end
    rows = store.send(:execute_params, "SELECT worker_pool, object_type, object_id FROM #{table} WHERE #{placeholders}", [object_type, object_id])
    rows.first
  end

  # Mirrors the conformance suite's expire_object_lease! helper without
  # depending on the conformance test file directly.
  def expire_object_lease!(store, backend, object_type, object_id)
    table = store.send(:table, "durable_objects")
    if backend.postgres?
      store.send(
        :execute_params,
        "UPDATE #{table} SET locked_until = now() - interval '1 hour' WHERE object_type = $1 AND object_id = $2",
        [object_type, object_id],
      )
    elsif backend.sqlite?
      expired_at = ((Time.now.to_r - 3600) * 1_000_000).to_i
      store.send(
        :execute_params,
        "UPDATE #{table} SET locked_until = ? WHERE object_type = ? AND object_id = ?",
        [expired_at, object_type, object_id],
      )
    else
      expired_at = (Time.now - 3600).strftime("%Y-%m-%d %H:%M:%S.%6N")
      store.send(
        :execute_params,
        "UPDATE #{table} SET locked_until = ? WHERE object_type = ? AND object_id = ?",
        [expired_at, object_type, object_id],
      )
    end
  end
end
