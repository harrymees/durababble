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

  private

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
