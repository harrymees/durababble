# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

# Unit coverage for the streaming-result pieces that need neither a database nor
# a live transport: the `expose_stream` DSL registration, the length-prefixed
# `FrameCodec`, and the server-side `StreamDispatcher` driven against a fake
# store + capturing writer.
class DurababbleStreamDispatcherTest < DurababbleTestCase
  # Captures emitted values and reports a fixed cancellation state, standing in
  # for an `Rpc::StreamWriter` so the dispatcher runs without a live body.
  class CapturingWriter
    attr_reader :values

    def initialize(cancelled: false)
      @values = []
      @cancelled = cancelled
    end

    def emit(value)
      @values << value
    end

    def cancelled?
      @cancelled
    end
  end

  # Minimal store double exposing only what the dispatcher touches. Counts lease
  # lookups so a test can assert the per-emit re-check is throttled.
  class FakeStreamStore
    attr_writer :workflow_lease
    attr_reader :lease_lookup_count, :object_claims, :object_releases

    def initialize(object_state: nil, workflow_lease: nil, object_lease_holder: "node-a")
      @object_state = object_state
      @workflow_lease = workflow_lease
      @lease_lookup_count = 0
      @object_lease_holder = object_lease_holder
      @object_claims = []
      @object_releases = []
    end

    def object_state(object_type:, object_id:)
      @object_state
    end

    def current_workflow_lease(_workflow_id)
      @lease_lookup_count += 1
      @workflow_lease
    end

    def current_object_lease(_object_type, _object_id)
      { "worker_id" => @object_lease_holder }
    end

    def claim_object_lease(worker_pool:, object_type:, object_id:, worker_id:, lease_seconds: 60)
      @object_claims << { worker_pool:, object_type:, object_id:, worker_id:, lease_seconds: }
      { "worker_id" => worker_id, "worker_pool" => worker_pool }
    end

    def renew_object_lease(**_kwargs)
      true
    end

    def release_object_lease(object_type:, object_id:, worker_id:)
      @object_releases << { object_type:, object_id:, worker_id: }
      true
    end
  end

  test "FrameCodec reassembles frames regardless of chunk boundaries" do
    payloads = ["alpha", "", "a-much-longer-payload-than-the-others"]
    wire = payloads.map { |payload| Durababble::FrameCodec.frame(payload) }.join
    buffer = Durababble::FrameCodec::Buffer.new

    decoded = []
    wire.each_char do |char|
      buffer << char
      while (payload = buffer.shift)
        decoded << payload
      end
    end

    assert_equal payloads, decoded
    assert_nil buffer.shift
  end

  test "expose_stream registers streams on workflows and durable objects" do
    workflow_class = Class.new(Durababble::Workflow) do
      workflow_name "expose-stream-workflow"
      expose_stream
      def tail(&block) = nil
    end
    assert workflow_class.exposed_streams.key?(:tail)

    object_class = Class.new(Durababble::DurableObject) do
      object_type "expose_stream_object"
      expose_stream :history
      def history(&block) = nil
    end
    assert object_class.exposed_streams.key?(:history)
  end

  test "dispatches an object stream only when this node owns the object lease" do
    object_class = Class.new(Durababble::DurableObject) do
      object_type "dispatcher_counter"
      expose_stream :ticks
      def ticks(&block)
        Array(current_state).each { |value| block.call(value) }
      end
    end
    writer = CapturingWriter.new
    request = Durababble::Rpc::Messages::TransientRequest.new(
      class_name: "dispatcher_counter",
      durable_object_id: "counter-1",
      method: "ticks",
    )

    dispatcher(objects: [object_class], store: FakeStreamStore.new(object_state: [10, 20, 30]))
      .call(request:, args: { "args" => [], "kwargs" => {} }, writer:)

    assert_equal [10, 20, 30], writer.values
  end

  test "rejects object stream dispatch without an object lease" do
    object_class = Class.new(Durababble::DurableObject) do
      object_type "dispatcher_unowned"
      expose_stream :ticks
      def ticks(&block)
        block.call(1)
      end
    end
    store = FakeStreamStore.new
    store.define_singleton_method(:current_object_lease) { |_object_type, _object_id| nil }
    request = Durababble::Rpc::Messages::TransientRequest.new(
      class_name: "dispatcher_unowned",
      durable_object_id: "counter-1",
      method: "ticks",
    )

    assert_raises(Durababble::WorkflowRpc::NoActiveLease) do
      dispatcher(objects: [object_class], store:).call(request:, args: empty_args, writer: CapturingWriter.new)
    end
  end

  test "forwards positional and keyword arguments to the stream method" do
    object_class = Class.new(Durababble::DurableObject) do
      object_type "dispatcher_args"
      expose_stream :slice
      def slice(prefix, limit:, &block)
        limit.times { |index| block.call("#{prefix}-#{index}") }
      end
    end
    writer = CapturingWriter.new
    request = Durababble::Rpc::Messages::TransientRequest.new(
      class_name: "dispatcher_args",
      durable_object_id: "args-1",
      method: "slice",
    )

    dispatcher(objects: [object_class], store: FakeStreamStore.new)
      .call(request:, args: { "args" => ["row"], "kwargs" => { "limit" => 2 } }, writer:)

    assert_equal ["row-0", "row-1"], writer.values
  end

  test "raises UnknownCommand for an unregistered object" do
    request = Durababble::Rpc::Messages::TransientRequest.new(class_name: "nope", durable_object_id: "x", method: "ticks")

    assert_raises(Durababble::WorkflowRpc::UnknownCommand) do
      dispatcher(objects: [], store: FakeStreamStore.new).call(request:, args: empty_args, writer: CapturingWriter.new)
    end
  end

  test "raises UnknownCommand for an object method that is not an exposed stream" do
    object_class = Class.new(Durababble::DurableObject) do
      object_type "dispatcher_guarded"
      expose_stream :ticks
      def ticks(&block) = nil

      # A plain public method that is NOT registered with expose_stream.
      def secret(&block) = block.call(:leaked)
    end
    request = Durababble::Rpc::Messages::TransientRequest.new(
      class_name: "dispatcher_guarded",
      durable_object_id: "x",
      method: "secret",
    )
    writer = CapturingWriter.new

    assert_raises(Durababble::WorkflowRpc::UnknownCommand) do
      dispatcher(objects: [object_class], store: FakeStreamStore.new).call(request:, args: empty_args, writer:)
    end
    assert_empty writer.values
  end

  test "raises UnknownCommand for a workflow method that is not an exposed stream even when the lease is held" do
    workflow_class = Class.new(Durababble::Workflow) do
      workflow_name "dispatcher_guarded_workflow"
      expose_stream :progress
      def progress(&block) = nil

      def secret(&block) = block.call(:leaked)
    end
    store = FakeStreamStore.new(workflow_lease: { "worker_id" => "node-a" })
    request = Durababble::Rpc::Messages::TransientRequest.new(
      workflow_id: "wf-1",
      class_name: "dispatcher_guarded_workflow",
      method: "secret",
    )
    writer = CapturingWriter.new

    assert_raises(Durababble::WorkflowRpc::UnknownCommand) do
      dispatcher(workflows: [workflow_class], store:, node_id: "node-a").call(request:, args: empty_args, writer:)
    end
    assert_empty writer.values
  end

  test "raises NoActiveLease when a workflow has no lease" do
    assert_raises(Durababble::WorkflowRpc::NoActiveLease) do
      dispatcher(workflows: [progress_workflow_class], store: FakeStreamStore.new(workflow_lease: nil))
        .call(request: workflow_stream_request, args: empty_args, writer: CapturingWriter.new)
    end
  end

  test "raises StaleLease when another node owns the workflow" do
    store = FakeStreamStore.new(workflow_lease: { "worker_id" => "node-b" })

    assert_raises(Durababble::WorkflowRpc::StaleLease) do
      dispatcher(workflows: [progress_workflow_class], store:, node_id: "node-a")
        .call(request: workflow_stream_request, args: empty_args, writer: CapturingWriter.new)
    end
  end

  test "streams a workflow owned by this node" do
    store = FakeStreamStore.new(workflow_lease: { "worker_id" => "node-a" })
    writer = CapturingWriter.new

    dispatcher(workflows: [progress_workflow_class], store:, node_id: "node-a")
      .call(request: workflow_stream_request, args: empty_args, writer:)

    assert_equal [{ "step" => 1 }, { "step" => 2 }], writer.values
  end

  test "raises StaleLease when workflow ownership is lost after the final emit" do
    store = FakeStreamStore.new(workflow_lease: { "worker_id" => "node-a" })
    workflow_class = Class.new(Durababble::Workflow) do
      workflow_name "dispatcher_final_emit_stale"
      expose_stream :progress
      define_method(:progress) do |&block|
        block.call({ "step" => 1 })
        store.workflow_lease = { "worker_id" => "node-b" }
      end
    end
    request = Durababble::Rpc::Messages::TransientRequest.new(
      workflow_id: "wf-1",
      class_name: "dispatcher_final_emit_stale",
      method: "progress",
    )
    writer = CapturingWriter.new

    assert_raises(Durababble::WorkflowRpc::StaleLease) do
      dispatcher(workflows: [workflow_class], store:, node_id: "node-a")
        .call(request:, args: empty_args, writer:)
    end
    assert_equal [{ "step" => 1 }], writer.values
  end

  test "throttles the per-emit lease re-check rather than querying on every quack" do
    burst_workflow_class = Class.new(Durababble::Workflow) do
      workflow_name "dispatcher_burst"
      expose_stream :burst
      def burst(&block)
        50.times { |index| block.call({ "n" => index }) }
      end
    end
    store = FakeStreamStore.new(workflow_lease: { "worker_id" => "node-a" })
    request = Durababble::Rpc::Messages::TransientRequest.new(
      workflow_id: "wf-1",
      class_name: "dispatcher_burst",
      method: "burst",
    )
    writer = CapturingWriter.new

    dispatcher(workflows: [burst_workflow_class], store:, node_id: "node-a")
      .call(request:, args: empty_args, writer:)

    assert_equal 50, writer.values.size
    # 50 emits inside one `RECHECK_INTERVAL` window: only the dispatcher's up-front
    # `assert_workflow_lease!` should hit the store. A per-emit re-check would be 51.
    assert_operator store.lease_lookup_count, :<=, 2
  end

  test "claims and releases the unified object lease when wired with a host" do
    object_class = Class.new(Durababble::DurableObject) do
      object_type "leased_object_stream"
      expose_stream :ticks
      def ticks(&block)
        [1, 2, 3].each { |value| block.call(value) }
      end
    end
    store = FakeStreamStore.new(object_state: nil)
    host = Durababble::ObjectStreamHost.new(store:, worker_id: "host-worker", node_id: "host-worker", lease_seconds: 30, renew_interval: 1.0)
    writer = CapturingWriter.new
    request = Durababble::Rpc::Messages::TransientRequest.new(
      worker_pool: "default",
      class_name: "leased_object_stream",
      durable_object_id: "obj-1",
      method: "ticks",
    )

    dispatcher_with_host = Durababble::StreamDispatcher.new(
      store:,
      workflows: [],
      objects: [object_class],
      node_id: "host-worker",
      object_stream_host: host,
      lease_seconds: 30,
    )
    dispatcher_with_host.call(request:, args: empty_args, writer:)

    assert_equal([1, 2, 3], writer.values)
    assert_equal(1, store.object_claims.size)
    assert_equal(1, store.object_releases.size)
  ensure
    host&.stop!
  end

  test "raises StaleLease when the host loses the lease before any emit" do
    object_class = Class.new(Durababble::DurableObject) do
      object_type "evicted_stream"
      expose_stream :idle
      def idle(&block)
        # No emit — eviction with no in-flight emit must still surface.
      end
    end
    store = FakeStreamStore.new(object_state: nil)
    # Eviction occurs from a sibling thread mid-with_lease; here we simulate it
    # by pre-flagging the entry via a wrapping decorator that flips `lost`
    # before the producer block runs.
    host = Durababble::ObjectStreamHost.new(store:, worker_id: "host-worker", node_id: "host-worker", lease_seconds: 30, renew_interval: 1.0)
    host.define_singleton_method(:with_lease) do |worker_pool: nil, object_type: nil, object_id: nil, lease_seconds: nil, &block|
      _ = [worker_pool, object_type, object_id, lease_seconds]
      entry = Durababble::ObjectStreamHost::Entry.new(refcount: 1, lost: false)
      block.call(entry)
      entry.lost = true
      raise Durababble::WorkflowRpc::StaleLease, "evicted" if entry.lost
    end

    dispatcher_with_host = Durababble::StreamDispatcher.new(
      store:,
      workflows: [],
      objects: [object_class],
      node_id: "host-worker",
      object_stream_host: host,
      lease_seconds: 30,
    )
    writer = CapturingWriter.new
    request = Durababble::Rpc::Messages::TransientRequest.new(
      worker_pool: "default",
      class_name: "evicted_stream",
      durable_object_id: "obj-1",
      method: "idle",
    )

    assert_raises(Durababble::WorkflowRpc::StaleLease) do
      dispatcher_with_host.call(request:, args: empty_args, writer:)
    end
  end

  test "ObjectStreamLeaseWriter raises StaleLease on emit when entry.lost flips" do
    entry = Durababble::ObjectStreamHost::Entry.new(refcount: 1, lost: false)
    inner = CapturingWriter.new
    leased = Durababble::StreamDispatcher::ObjectStreamLeaseWriter.new(inner, entry:)

    leased.emit("ok")
    entry.lost = true

    assert_raises(Durababble::WorkflowRpc::StaleLease) { leased.emit("late") }
    assert_equal ["ok"], inner.values
    assert leased.cancelled?
  end

  test "ObjectStreamLeaseWriter cancelled? ORs entry.lost with inner cancellation" do
    entry = Durababble::ObjectStreamHost::Entry.new(refcount: 1, lost: false)
    inner_alive = CapturingWriter.new
    inner_dead = CapturingWriter.new(cancelled: true)

    leased_alive = Durababble::StreamDispatcher::ObjectStreamLeaseWriter.new(inner_alive, entry:)
    refute leased_alive.cancelled?
    entry.lost = true
    assert leased_alive.cancelled?

    leased_dead = Durababble::StreamDispatcher::ObjectStreamLeaseWriter.new(inner_dead, entry: Durababble::ObjectStreamHost::Entry.new(refcount: 1, lost: false))
    assert leased_dead.cancelled?
  end

  private

  def dispatcher(store:, workflows: [], objects: [], node_id: "node-a")
    Durababble::StreamDispatcher.new(store:, workflows:, objects:, node_id:)
  end

  def progress_workflow_class
    @progress_workflow_class ||= Class.new(Durababble::Workflow) do
      workflow_name "dispatcher_progress"
      expose_stream :progress
      def progress(&block)
        block.call({ "step" => 1 })
        block.call({ "step" => 2 })
      end
    end
  end

  def workflow_stream_request
    Durababble::Rpc::Messages::TransientRequest.new(
      workflow_id: "wf-1",
      class_name: "dispatcher_progress",
      method: "progress",
    )
  end

  def empty_args
    { "args" => [], "kwargs" => {} }
  end
end
