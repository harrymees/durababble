# typed: false
# frozen_string_literal: true

require "async"
require "async/http/endpoint"
require "async/grpc"
require "timeout"
require_relative "../test_helper"

# Live async-grpc integration coverage for streaming-result RPCs: a real
# `Rpc::Server` produces gRPC response-stream messages over localhost h2c and a
# real `Rpc::Client` consumes them through a `ResultStream`.
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

  test "streams every value to the consumer then ends" do
    with_stream_server(->(args:, writer:, **) do
      args.fetch("values").each { |value| writer.emit(value) }
    end) do |server|
      client = Durababble::Rpc::Client.new(address: server.address)

      stream = client.call_transient_stream(
        worker_pool: "default",
        class_name: "Counter",
        durable_object_id: "counter-1",
        method: "tail",
        args: { "values" => [10, 20, 30] },
      )

      assert_equal([10, 20, 30], stream.each.to_a)
    end
  end

  test "re-raises a producer error after delivering prior values" do
    with_stream_server(->(writer:, **) do
      writer.emit(:first)
      raise Durababble::WorkflowRpc::StaleLease, "lease lost mid-stream"
    end) do |server|
      client = Durababble::Rpc::Client.new(address: server.address)
      stream = client.call_transient_stream(worker_pool: "default", method: "tail", args: {})

      seen = []
      error = assert_raises(Durababble::WorkflowRpc::StaleLease) do
        stream.each { |value| seen << value }
      end
      assert_equal([:first], seen)
      assert_match(/lease lost mid-stream/, error.message)
    end
  end

  test "closing the consumer cancels the producer mid-stream" do
    observed_cancel = Async::Queue.new
    with_stream_server(->(writer:, **) do
      writer.emit(:tick)
      sleep(0.01) until writer.cancelled?
      observed_cancel.enqueue(:cancelled)
    end) do |server|
      client = Durababble::Rpc::Client.new(address: server.address)
      stream = client.call_transient_stream(worker_pool: "default", method: "tail", args: {})

      assert_equal(:tick, stream.read)
      stream.close

      assert_equal(:cancelled, Async::Task.current.with_timeout(5) { observed_cancel.dequeue })
    end
  end

  test "maps an unreachable node to a typed node-unavailable error" do
    client = Durababble::Rpc::Client.new(address: "127.0.0.1:1", timeout: 0.5)
    stream = client.call_transient_stream(worker_pool: "default", method: "tail", args: {})

    assert_raises(Durababble::WorkflowRpc::NodeUnavailable) { stream.each.to_a }
  end

  test "times out an idle response body after headers arrive" do
    with_stream_server(->(writer:, **) do
      sleep(0.01) until writer.cancelled?
    end) do |server|
      client = Durababble::Rpc::Client.new(address: server.address, timeout: 0.2)
      stream = client.call_transient_stream(worker_pool: "default", method: "tail", args: {})

      Async::Task.current.with_timeout(2) do
        assert_raises(Durababble::WorkflowRpc::NodeUnavailable) { stream.each.to_a }
      end
    end
  end

  test "resumes delivery after the server idles past the consumer poll window" do
    idle_gap = Durababble::Rpc::STREAM_POLL_TIMEOUT * 3
    with_stream_server(->(writer:, **) do
      writer.emit(:before_idle)
      sleep(idle_gap)
      writer.emit(:after_idle)
    end) do |server|
      client = Durababble::Rpc::Client.new(address: server.address)
      stream = client.call_transient_stream(worker_pool: "default", method: "tail", args: {})

      assert_equal([:before_idle, :after_idle], stream.each.to_a)
    end
  end

  test "enforces streaming RPC argument byte limits before sending or dispatching" do
    args = { "body" => "x" * 64 }
    size = Durababble::Rpc::SERIALIZER.dump(args).bytesize
    calls = []
    with_stream_server(->(args:, writer:, **) do
      calls << args
      writer.emit(:ok)
    end) do |server|
      client = Durababble::Rpc::Client.new(address: server.address)

      with_payload_limit(:rpc_argument, size) do
        assert_equal([:ok], client.call_transient_stream(worker_pool: "default", method: "tail", args:).to_a)
      end
      assert_equal([args], calls)

      error = with_payload_limit(:rpc_argument, size - 1) do
        assert_raises(Durababble::PayloadTooLarge) do
          client.call_transient_stream(worker_pool: "default", method: "tail", args:)
        end
      end
      assert_equal(:rpc_argument, error.surface)
      assert_match(/CallTransientStream tail args/, error.message)
      assert_equal(1, calls.length)

      raw_args = Durababble::Rpc::SERIALIZER.dump(args)
      raw_request = Durababble::Rpc::Messages::TransientRequest.new(worker_pool: "default", method: "tail", args: raw_args)
      frames = with_payload_limit(:rpc_argument, size - 1) do
        raw_grpc_call_transient_stream(server.address, raw_request)
      end
      assert_equal(1, frames.size)
      assert(frames.first.error?)
      assert_equal("Durababble::PayloadTooLarge", frames.first.error.klass)
      assert_equal(1, calls.length)
    end
  end

  private

  def with_stream_server(stream_handler)
    server = nil
    runner = lambda do |task|
      server = Durababble::Rpc::Server.new(
        node_id: "node-a",
        store:,
        stream_handler:,
        port: 0,
      )
      server.start_async(parent: task)
      yield server
    ensure
      server&.stop
    end

    if (task = Async::Task.current?)
      runner.call(task)
    else
      Async(&runner).wait
    end
  end

  def raw_grpc_call_transient_stream(address, request)
    endpoint = Async::HTTP::Endpoint.parse("http://#{address}", protocol: Async::HTTP::Protocol::HTTP2)
    client = Async::HTTP::Client.new(endpoint, protocol: Async::HTTP::Protocol::HTTP2)
    frames = []
    grpc_client = Async::GRPC::Client.new(client)
    grpc_client.stub(Durababble::Rpc::Interface, Durababble::Rpc::SERVICE_NAME)
      .call_transient_stream(request) { |frame| frames << frame }
    frames
  ensure
    client&.close
  end

  def with_payload_limit(surface, value)
    configured = Durababble.instance_variable_defined?(:@payload_limits)
    previous = Durababble.instance_variable_get(:@payload_limits) if configured
    Durababble.payload_limits = { surface => value }
    yield
  ensure
    if configured
      Durababble.instance_variable_set(:@payload_limits, previous)
    elsif Durababble.instance_variable_defined?(:@payload_limits)
      Durababble.remove_instance_variable(:@payload_limits)
    end
  end

  def database_url
    backend_descriptor.database_url
  end
end
