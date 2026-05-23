# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "open3"
require "stringio"

class DurababbleRpcClientTest < DurababbleTestCase
  class FakeWaitThread
    attr_reader :pid

    def initialize(join_result: false)
      @join_result = join_result
      @pid = 12_345
    end

    def join(_timeout = nil)
      @join_result
    end
  end

  class FakeInput
    attr_reader :writes, :flushes

    def initialize(write_error: nil)
      @write_error = write_error
      @writes = []
      @flushes = 0
      @closed = false
    end

    def puts(line)
      raise @write_error if @write_error

      @writes << line
    end

    def flush
      @flushes += 1
    end

    def close
      @closed = true
    end

    def closed?
      @closed
    end
  end

  def client(stdin:, stdout:, wait_thread: FakeWaitThread.new, timeout: 0.01)
    Durababble::RpcClient.new(stdin:, stdout:, wait_thread:, timeout:)
  end

  test "writes one JSON line request, skips non-protocol noise, and returns the parsed result" do
    stdin = FakeInput.new
    stdout = StringIO.new("NOTICE: schema exists\n{\"ok\":true,\"result\":{\"pong\":true}}\n")

    result = client(stdin:, stdout:).request("ping", "i" => 1)

    assert_equal({ "pong" => true }, result)
    assert_equal 1, stdin.writes.length
    assert_equal({ "command" => "ping", "payload" => { "i" => 1 } }, JSON.parse(stdin.writes.first))
    assert_equal 1, stdin.flushes
  end

  test "raises a protocol error when the worker returns an application error" do
    stdin = FakeInput.new
    stdout = StringIO.new("{\"ok\":false,\"error\":\"ArgumentError: unknown command\"}\n")

    assert_raises_matching(Durababble::RpcClient::RemoteError, /unknown command/) do
      client(stdin:, stdout:).request("unknown")
    end
  end

  test "raises a connection error for malformed protocol JSON" do
    stdin = FakeInput.new
    stdout = StringIO.new("{not-json}\n")

    assert_raises_matching(Durababble::RpcClient::ConnectionError, /invalid RPC JSON response/) do
      client(stdin:, stdout:).request("ping")
    end
  end

  test "ignores JSON-shaped non-protocol log lines before a valid response" do
    stdin = FakeInput.new
    stdout = StringIO.new("{\"level\":\"info\",\"message\":\"booted\"}\n{\"ok\":true,\"result\":{\"pong\":true}}\n")

    assert_equal({ "pong" => true }, client(stdin:, stdout:).request("ping"))
  end

  test "does not return a stale timed-out response for a later request" do
    stdin = FakeInput.new
    reader, writer = IO.pipe
    rpc = Durababble::RpcClient.new(stdin:, stdout: reader, wait_thread: FakeWaitThread.new, timeout: 0.001)

    assert_raises(Durababble::RpcClient::TimeoutError) { rpc.request("first") }
    writer.write("{\"ok\":true,\"result\":{\"request\":\"first\"}}\n")
    writer.write("{\"ok\":true,\"result\":{\"request\":\"second\"}}\n")

    assert_raises_matching(Durababble::RpcClient::ConnectionError, /timed out and cannot be reused/) do
      rpc.request("second")
    end
  ensure
    writer&.close unless writer&.closed?
    reader&.close unless reader&.closed?
  end

  test "raises EOF when the worker exits before sending a protocol response" do
    stdin = FakeInput.new
    stdout = StringIO.new("")

    assert_raises_matching(Durababble::RpcClient::EOFError, /exited before response/) do
      client(stdin:, stdout:).request("ping")
    end
  end

  test "raises timeout when the worker stays silent" do
    stdin = FakeInput.new
    reader, writer = IO.pipe

    assert_raises_matching(Durababble::RpcClient::TimeoutError, /timed out/) do
      Durababble::RpcClient.new(stdin:, stdout: reader, wait_thread: FakeWaitThread.new, timeout: 0.001).request("ping")
    end
  ensure
    writer&.close unless writer&.closed?
    reader&.close unless reader&.closed?
  end

  test "wraps broken-pipe style write failures as connection errors" do
    stdin = FakeInput.new(write_error: Errno::EPIPE.new)
    stdout = StringIO.new("{\"ok\":true,\"result\":{}}\n")

    assert_raises_matching(Durababble::RpcClient::ConnectionError, /failed to write/) do
      client(stdin:, stdout:).request("ping")
    end
  end

  test "reuses a kept-alive worker across requests" do
    stdin = FakeInput.new
    stdout = StringIO.new("{\"ok\":true,\"result\":{\"first\":true}}\n{\"ok\":true,\"result\":{\"second\":true}}\n")
    rpc = client(stdin:, stdout:)

    assert_equal({ "first" => true }, rpc.request("ping"))
    assert_equal({ "second" => true }, rpc.request("ping"))
    assert_equal 2, stdin.writes.length
  end

  test "reconnects before sending when a kept-alive worker died while idle" do
    dead_stdin = FakeInput.new
    dead_stdout = StringIO.new("")
    live_stdin = FakeInput.new
    live_stdout = StringIO.new("{\"ok\":true,\"result\":{\"reconnected\":true}}\n")

    Open3.expects(:popen2e).twice.returns(
      [dead_stdin, dead_stdout, FakeWaitThread.new(join_result: true)],
      [live_stdin, live_stdout, FakeWaitThread.new(join_result: false)],
    )

    rpc = Durababble::RpcClient.spawn(command: ["ruby", "worker.rb"], env: { "A" => "B" })

    assert_equal({ "reconnected" => true }, rpc.request("ping"))
    assert_empty dead_stdin.writes
    assert_equal 1, live_stdin.writes.length
  end

  test "closes streams without killing a worker that exits promptly" do
    stdin = FakeInput.new
    stdout = StringIO.new("")
    rpc = client(stdin:, stdout:, wait_thread: FakeWaitThread.new(join_result: true))
    Process.expects(:kill).never

    rpc.close

    assert_predicate stdin, :closed?
    assert_predicate stdout, :closed?
  end

  test "terminates a worker that does not exit after stdin is closed" do
    stdin = FakeInput.new
    stdout = StringIO.new("")
    rpc = client(stdin:, stdout:, wait_thread: FakeWaitThread.new(join_result: false))
    Process.expects(:kill).with("TERM", 12_345)

    rpc.close

    assert_predicate stdin, :closed?
    assert_predicate stdout, :closed?
  end

  test "does not close stdin twice when the caller already closed it" do
    stdin = FakeInput.new
    stdout = StringIO.new("")
    stdin.close
    rpc = client(stdin:, stdout:, wait_thread: FakeWaitThread.new(join_result: true))
    Process.expects(:kill).never

    rpc.close

    assert_predicate stdin, :closed?
    assert_predicate stdout, :closed?
  end

  test "does not close stdout twice when it is already closed" do
    stdin = FakeInput.new
    stdout = StringIO.new("")
    stdout.close
    rpc = client(stdin:, stdout:, wait_thread: FakeWaitThread.new(join_result: true))

    rpc.close

    assert_predicate stdin, :closed?
    assert_predicate stdout, :closed?
  end

  test "times out immediately when the read deadline has already elapsed" do
    stdin = FakeInput.new
    reader, writer = IO.pipe

    assert_raises_matching(Durababble::RpcClient::TimeoutError, /timed out/) do
      Durababble::RpcClient.new(stdin:, stdout: reader, wait_thread: FakeWaitThread.new, timeout: -0.001).request("ping")
    end
  ensure
    writer&.close unless writer&.closed?
    reader&.close unless reader&.closed?
  end

  test "does not reconnect a spawned worker that is still alive" do
    stdin = FakeInput.new
    stdout = StringIO.new("{\"ok\":true,\"result\":{\"alive\":true}}\n")

    Open3.expects(:popen2e).once.returns([stdin, stdout, FakeWaitThread.new(join_result: false)])

    rpc = Durababble::RpcClient.spawn(command: ["ruby", "worker.rb"])

    assert_equal({ "alive" => true }, rpc.request("ping"))
  end

  test "reads ready IO-backed responses without waiting for timeout" do
    stdin = FakeInput.new
    reader, writer = IO.pipe
    writer.write("{\"ok\":true,\"result\":{\"io\":true}}\n")
    writer.close

    result = Durababble::RpcClient.new(stdin:, stdout: reader, wait_thread: FakeWaitThread.new, timeout: 1).request("ping")

    assert_equal({ "io" => true }, result)
  ensure
    reader&.close unless reader&.closed?
  end
end
