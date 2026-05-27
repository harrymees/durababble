# typed: false
# frozen_string_literal: true

require "timeout"
require_relative "../test_helper"

# Live async-http integration coverage for streaming-result RPCs: a real
# `Rpc::Server` produces frames over localhost HTTP/2 and a real `Rpc::Client`
# consumes them through a `ResultStream`. Mirrors `rpc_transport_test.rb`.
class DurababbleRpcStreamTest < DurababbleTestCase
  def setup
    @durababble_backend = durababble_store_backends.first
    @durababble_schema = "#{@durababble_backend.default_schema_prefix}_rpc_stream_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    @durababble_store = Durababble::Store.connect(database_url:, schema:)
    @durababble_store.migrate!
  end

  def teardown
    @durababble_store&.drop_schema!
    @durababble_store&.close
    @durababble_store = nil
    @durababble_schema = nil
    @durababble_backend = nil
  end

  test "streams every quack to the consumer then ends" do
    server = start_stream_server(->(args:, writer:, **) do
      args.fetch("values").each { |value| writer.emit(value) }
    end)
    client = Durababble::Rpc::Client.new(address: server.address)

    stream = client.call_transient_stream(
      worker_pool: "default",
      class_name: "Counter",
      durable_object_id: "counter-1",
      method: "tail",
      args: { "values" => [10, 20, 30] },
    )

    assert_equal([10, 20, 30], stream.each.to_a)
  ensure
    server&.stop
  end

  test "re-raises a producer error after delivering prior quacks" do
    server = start_stream_server(->(writer:, **) do
      writer.emit(:first)
      raise Durababble::WorkflowRpc::StaleLease, "lease lost mid-stream"
    end)
    client = Durababble::Rpc::Client.new(address: server.address)

    stream = client.call_transient_stream(worker_pool: "default", method: "tail", args: {})

    seen = []
    error = assert_raises(Durababble::WorkflowRpc::StaleLease) do
      stream.each { |value| seen << value }
    end
    assert_equal([:first], seen)
    assert_match(/lease lost mid-stream/, error.message)
  ensure
    server&.stop
  end

  test "closing the consumer cancels the producer mid-stream" do
    observed_cancel = Thread::Queue.new
    server = start_stream_server(->(writer:, **) do
      writer.emit(:tick)
      sleep(0.01) until writer.cancelled?
      observed_cancel << :cancelled
    end)
    client = Durababble::Rpc::Client.new(address: server.address)

    stream = client.call_transient_stream(worker_pool: "default", method: "tail", args: {})

    # `read`/`close` are incremental, so they run within one reactor scope.
    Sync do
      assert_equal(:tick, stream.read)
      stream.close
    end

    assert_equal(:cancelled, Timeout.timeout(5) { observed_cancel.pop })
  ensure
    server&.stop
  end

  test "maps an unreachable node to a typed node-unavailable error" do
    client = Durababble::Rpc::Client.new(address: "127.0.0.1:1", timeout: 0.5)

    stream = client.call_transient_stream(worker_pool: "default", method: "tail", args: {})

    assert_raises(Durababble::WorkflowRpc::NodeUnavailable) { stream.each.to_a }
  end

  test "resumes delivery after the server idles past the consumer poll window" do
    # The consumer's frame loop wakes every STREAM_POLL_TIMEOUT to re-check
    # cancellation, retrying the body read on each timeout. An idle gap several
    # poll windows wide must neither drop nor reorder values: HTTP/2 buffers the
    # late DATA frame and the resumed read delivers it intact.
    idle_gap = Durababble::Rpc::STREAM_POLL_TIMEOUT * 3
    server = start_stream_server(->(writer:, **) do
      writer.emit(:before_idle)
      sleep(idle_gap)
      writer.emit(:after_idle)
    end)
    client = Durababble::Rpc::Client.new(address: server.address)

    stream = client.call_transient_stream(worker_pool: "default", method: "tail", args: {})

    assert_equal([:before_idle, :after_idle], stream.each.to_a)
  ensure
    server&.stop
  end

  test "ends a workflow stream with StaleLease when the lease is lost mid-stream" do
    # Drives the real StreamDispatcher + LeaseCheckingWriter over the wire. The
    # fake store reports this node as the lease owner for the dispatcher's up-front
    # check, then a different owner for the writer's periodic re-check, so the
    # producer raises StaleLease mid-stream and the consumer re-raises it after the
    # values delivered before the hand-off.
    workflow_class = Class.new(Durababble::Workflow) do
      workflow_name "stale_lease_stream_workflow"
      expose_stream :progress
      def progress(&block)
        100.times do |index|
          block.call({ "step" => index })
          sleep(0.05)
        end
      end
    end
    dispatcher = Durababble::StreamDispatcher.new(
      store: LeaseFlippingStore.new(initial_owner: "node-a", later_owner: "node-b"),
      workflows: [workflow_class],
      objects: [],
      node_id: "node-a",
    )
    server = start_stream_server(dispatcher.method(:call))
    client = Durababble::Rpc::Client.new(address: server.address)

    stream = client.call_transient_stream(
      worker_pool: "default",
      workflow_id: "wf-1",
      class_name: "stale_lease_stream_workflow",
      method: "progress",
      args: { "args" => [], "kwargs" => {} },
    )

    seen = []
    assert_raises(Durababble::WorkflowRpc::StaleLease) do
      stream.each { |value| seen << value }
    end

    # The hand-off ends the stream partway, after at least one value but well
    # before the producer's 100 steps, and what arrived is an in-order prefix.
    refute_empty(seen)
    assert_operator(seen.size, :<, 100)
    assert_equal((0...seen.size).map { |index| { "step" => index } }, seen)
  ensure
    server&.stop
  end

  test "forwards keyword arguments over the wire to an exposed object stream" do
    # End-to-end coverage of the dispatcher's symbolize_keys: the client sends
    # string-keyed kwargs inside the args payload, and the object stream method
    # must receive them as real Ruby keyword arguments.
    object_class = Class.new(Durababble::DurableObject) do
      object_type "wire_kwargs_object"
      expose_stream :slice
      def slice(prefix, limit:, &block)
        limit.times { |index| block.call("#{prefix}-#{index}") }
      end
    end
    dispatcher = Durababble::StreamDispatcher.new(
      store: SnapshotlessStore.new,
      workflows: [],
      objects: [object_class],
      node_id: "node-a",
    )
    server = start_stream_server(dispatcher.method(:call))
    client = Durababble::Rpc::Client.new(address: server.address)

    stream = client.call_transient_stream(
      worker_pool: "default",
      class_name: "wire_kwargs_object",
      durable_object_id: "obj-1",
      method: "slice",
      args: { "args" => ["row"], "kwargs" => { "limit" => 3 } },
    )

    assert_equal(["row-0", "row-1", "row-2"], stream.each.to_a)
  ensure
    server&.stop
  end

  test "claims and releases the unified object lease around an object stream" do
    object_class = Class.new(Durababble::DurableObject) do
      object_type "leased_obj_stream"
      expose_stream :ticks
      def ticks(&block)
        [1, 2, 3].each { |value| block.call(value) }
      end
    end
    host, dispatcher = build_leased_dispatcher(objects: [object_class], node_id: "node-a")
    server = start_stream_server(dispatcher.method(:call))
    client = Durababble::Rpc::Client.new(address: server.address)

    stream = client.call_transient_stream(
      worker_pool: "default",
      class_name: "leased_obj_stream",
      durable_object_id: "obj-1",
      method: "ticks",
      args: { "args" => [], "kwargs" => {} },
    )

    assert_equal([1, 2, 3], stream.each.to_a)
    # The host released the lease cleanly on stream end.
    assert_nil(store.current_object_lease("leased_obj_stream", "obj-1"))
  ensure
    host&.stop!
    server&.stop
  end

  test "evict_all! during an in-flight object stream raises StaleLease on the consumer" do
    proceed = Thread::Queue.new
    object_class = Class.new(Durababble::DurableObject) do
      const_set(:PROCEED, proceed)
      object_type "evicted_obj_stream"
      expose_stream :slow
      def slow(&block)
        block.call(:before)
        self.class::PROCEED.pop
        # The lease lane has flipped to lost by this point; the leased writer
        # raises StaleLease on the next emit, which the server turns into a
        # terminal error frame.
        block.call(:after)
      end
    end
    host, dispatcher = build_leased_dispatcher(objects: [object_class], node_id: "node-a")
    server = start_stream_server(dispatcher.method(:call))
    client = Durababble::Rpc::Client.new(address: server.address)

    seen = []
    consumer = Thread.new do
      stream = client.call_transient_stream(
        worker_pool: "default",
        class_name: "evicted_obj_stream",
        durable_object_id: "obj-1",
        method: "slow",
        args: { "args" => [], "kwargs" => {} },
      )
      stream.each { |value| seen << value }
    rescue Durababble::WorkflowRpc::StaleLease => err
      seen << [:error, err.message]
    end

    # Wait for the producer to emit :before, then evict.
    Timeout.timeout(5) do
      Thread.pass until seen.include?(:before)
    end
    host.evict_all!
    proceed << :go
    consumer.join(5)

    assert_includes(seen, :before)
    assert_kind_of(Array, seen.last)
    assert_equal(:error, seen.last.first)
  ensure
    host&.stop!
    server&.stop
  end

  test "back-to-back consumers of the same object reclaim and release the lease" do
    # Refcounted shared-claim semantics are unit-tested in
    # `object_stream_host_test.rb` ("shares the lease across concurrent openers
    # and releases once"). Concurrent fan-out from two consumer threads here would
    # trip the known reactor-shared AR connection limitation on Trilogy/MySQL, so
    # this integration test exercises the sequential reclaim path instead: open,
    # consume, release; open again, consume, release.
    object_class = Class.new(Durababble::DurableObject) do
      object_type "sequential_obj_stream"
      expose_stream :ticks
      def ticks(&block)
        [:a, :b, :c].each { |value| block.call(value) }
      end
    end
    host, dispatcher = build_leased_dispatcher(objects: [object_class], node_id: "node-a")
    server = start_stream_server(dispatcher.method(:call))
    client = Durababble::Rpc::Client.new(address: server.address)

    2.times do
      stream = client.call_transient_stream(
        worker_pool: "default",
        class_name: "sequential_obj_stream",
        durable_object_id: "obj-1",
        method: "ticks",
        args: { "args" => [], "kwargs" => {} },
      )
      assert_equal([:a, :b, :c], stream.each.to_a)
      # After each consumer ends, the host's refcount hits zero and the lease row clears.
      assert_nil(store.current_object_lease("sequential_obj_stream", "obj-1"))
    end
  ensure
    host&.stop!
    server&.stop
  end

  private

  def build_leased_dispatcher(objects:, node_id:)
    host = Durababble::ObjectStreamHost.new(
      store:, worker_id: node_id, node_id:, lease_seconds: 30, renew_interval: 0.5,
    )
    dispatcher = Durababble::StreamDispatcher.new(
      store:,
      workflows: [],
      objects:,
      node_id:,
      object_stream_host: host,
      lease_seconds: 30,
    )
    [host, dispatcher]
  end

  # Reports a flipping lease owner: the first lookup (the dispatcher's up-front
  # `assert_workflow_lease!`) sees `initial_owner`; every later lookup (the
  # `LeaseCheckingWriter` re-check) sees `later_owner`, simulating a hand-off to
  # another node while the stream is in flight.
  class LeaseFlippingStore
    def initialize(initial_owner:, later_owner:)
      @initial_owner = initial_owner
      @later_owner = later_owner
      @calls = 0
    end

    def current_workflow_lease(_workflow_id)
      @calls += 1
      { "worker_id" => @calls <= 1 ? @initial_owner : @later_owner }
    end
  end

  # Minimal store for object-stream dispatch: object streams run against a state
  # snapshot, and the stream methods under test do not read it, so a nil snapshot
  # is enough to exercise the routing + argument-forwarding path.
  class SnapshotlessStore
    def object_state(object_type:, object_id:)
      nil
    end
  end

  def start_stream_server(stream_handler)
    Durababble::Rpc::Server.new(
      node_id: "node-a",
      store:,
      stream_handler:,
      port: 0,
    ).start
  end

  def database_url
    backend_descriptor.database_url
  end
end
