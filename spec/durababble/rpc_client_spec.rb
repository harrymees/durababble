# frozen_string_literal: true

require "spec_helper"
require "stringio"

RSpec.describe Durababble::RpcClient do
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

    def closed? = @closed
  end

  def client(stdin:, stdout:, wait_thread: FakeWaitThread.new, timeout: 0.01)
    described_class.new(stdin:, stdout:, wait_thread:, timeout:)
  end

  it "writes one JSON line request, skips non-protocol noise, and returns the parsed result" do
    stdin = FakeInput.new
    stdout = StringIO.new("NOTICE: schema exists\n{\"ok\":true,\"result\":{\"pong\":true}}\n")

    result = client(stdin:, stdout:).request("ping", "i" => 1)

    expect(result).to eq("pong" => true)
    expect(stdin.writes.length).to eq(1)
    expect(JSON.parse(stdin.writes.first)).to eq("command" => "ping", "payload" => { "i" => 1 })
    expect(stdin.flushes).to eq(1)
  end

  it "raises a protocol error when the worker returns an application error" do
    stdin = FakeInput.new
    stdout = StringIO.new("{\"ok\":false,\"error\":\"ArgumentError: unknown command\"}\n")

    expect { client(stdin:, stdout:).request("unknown") }.to raise_error(Durababble::RpcClient::RemoteError, /unknown command/)
  end

  it "raises a connection error for malformed protocol JSON" do
    stdin = FakeInput.new
    stdout = StringIO.new("{not-json}\n")

    expect { client(stdin:, stdout:).request("ping") }.to raise_error(Durababble::RpcClient::ConnectionError, /invalid RPC JSON response/)
  end

  it "ignores JSON-shaped non-protocol log lines before a valid response" do
    stdin = FakeInput.new
    stdout = StringIO.new("{\"level\":\"info\",\"message\":\"booted\"}\n{\"ok\":true,\"result\":{\"pong\":true}}\n")

    expect(client(stdin:, stdout:).request("ping")).to eq("pong" => true)
  end

  it "does not return a stale timed-out response for a later request" do
    stdin = FakeInput.new
    reader, writer = IO.pipe
    rpc = described_class.new(stdin:, stdout: reader, wait_thread: FakeWaitThread.new, timeout: 0.001)

    begin
      expect { rpc.request("first") }.to raise_error(Durababble::RpcClient::TimeoutError)
      writer.write("{\"ok\":true,\"result\":{\"request\":\"first\"}}\n")
      writer.write("{\"ok\":true,\"result\":{\"request\":\"second\"}}\n")

      expect { rpc.request("second") }.to raise_error(Durababble::RpcClient::ConnectionError, /timed out and cannot be reused/)
    ensure
      writer.close unless writer.closed?
      reader.close unless reader.closed?
    end
  end

  it "raises EOF when the worker exits before sending a protocol response" do
    stdin = FakeInput.new
    stdout = StringIO.new("")

    expect { client(stdin:, stdout:).request("ping") }.to raise_error(Durababble::RpcClient::EOFError, /exited before response/)
  end

  it "raises timeout when the worker stays silent" do
    stdin = FakeInput.new
    reader, writer = IO.pipe

    begin
      expect { described_class.new(stdin:, stdout: reader, wait_thread: FakeWaitThread.new, timeout: 0.001).request("ping") }.to raise_error(Durababble::RpcClient::TimeoutError, /timed out/)
    ensure
      writer.close
      reader.close
    end
  end

  it "wraps broken-pipe style write failures as connection errors" do
    stdin = FakeInput.new(write_error: Errno::EPIPE.new)
    stdout = StringIO.new("{\"ok\":true,\"result\":{}}\n")

    expect { client(stdin:, stdout:).request("ping") }.to raise_error(Durababble::RpcClient::ConnectionError, /failed to write/)
  end

  it "reuses a kept-alive worker across requests" do
    stdin = FakeInput.new
    stdout = StringIO.new("{\"ok\":true,\"result\":{\"first\":true}}\n{\"ok\":true,\"result\":{\"second\":true}}\n")
    rpc = client(stdin:, stdout:)

    expect(rpc.request("ping")).to eq("first" => true)
    expect(rpc.request("ping")).to eq("second" => true)
    expect(stdin.writes.length).to eq(2)
  end

  it "reconnects before sending when a kept-alive worker died while idle" do
    dead_stdin = FakeInput.new
    dead_stdout = StringIO.new("")
    live_stdin = FakeInput.new
    live_stdout = StringIO.new("{\"ok\":true,\"result\":{\"reconnected\":true}}\n")

    allow(Open3).to receive(:popen2e).and_return(
      [dead_stdin, dead_stdout, FakeWaitThread.new(join_result: true)],
      [live_stdin, live_stdout, FakeWaitThread.new(join_result: false)]
    )

    rpc = described_class.spawn(command: ["ruby", "worker.rb"], env: { "A" => "B" })

    expect(rpc.request("ping")).to eq("reconnected" => true)
    expect(Open3).to have_received(:popen2e).twice
    expect(dead_stdin.writes).to be_empty
    expect(live_stdin.writes.length).to eq(1)
  end

  it "closes streams without killing a worker that exits promptly" do
    stdin = FakeInput.new
    stdout = StringIO.new("")
    rpc = client(stdin:, stdout:, wait_thread: FakeWaitThread.new(join_result: true))

    expect(Process).not_to receive(:kill)

    rpc.close

    expect(stdin).to be_closed
    expect(stdout).to be_closed
  end

  it "terminates a worker that does not exit after stdin is closed" do
    stdin = FakeInput.new
    stdout = StringIO.new("")
    rpc = client(stdin:, stdout:, wait_thread: FakeWaitThread.new(join_result: false))

    expect(Process).to receive(:kill).with("TERM", 12_345)

    rpc.close

    expect(stdin).to be_closed
    expect(stdout).to be_closed
  end

  it "does not close stdin twice when the caller already closed it" do
    stdin = FakeInput.new
    stdout = StringIO.new("")
    stdin.close
    rpc = client(stdin:, stdout:, wait_thread: FakeWaitThread.new(join_result: true))

    expect(Process).not_to receive(:kill)

    rpc.close

    expect(stdin).to be_closed
    expect(stdout).to be_closed
  end

  it "does not close stdout twice when it is already closed" do
    stdin = FakeInput.new
    stdout = StringIO.new("")
    stdout.close
    rpc = client(stdin:, stdout:, wait_thread: FakeWaitThread.new(join_result: true))

    rpc.close

    expect(stdin).to be_closed
    expect(stdout).to be_closed
  end

  it "times out immediately when the read deadline has already elapsed" do
    stdin = FakeInput.new
    reader, writer = IO.pipe

    begin
      expect { described_class.new(stdin:, stdout: reader, wait_thread: FakeWaitThread.new, timeout: -0.001).request("ping") }.to raise_error(Durababble::RpcClient::TimeoutError, /timed out/)
    ensure
      writer.close unless writer.closed?
      reader.close unless reader.closed?
    end
  end

  it "does not reconnect a spawned worker that is still alive" do
    stdin = FakeInput.new
    stdout = StringIO.new("{\"ok\":true,\"result\":{\"alive\":true}}\n")

    allow(Open3).to receive(:popen2e).and_return([stdin, stdout, FakeWaitThread.new(join_result: false)])

    rpc = described_class.spawn(command: ["ruby", "worker.rb"])

    expect(rpc.request("ping")).to eq("alive" => true)
    expect(Open3).to have_received(:popen2e).once
  end

  it "reads ready IO-backed responses without waiting for timeout" do
    stdin = FakeInput.new
    reader, writer = IO.pipe
    writer.write("{\"ok\":true,\"result\":{\"io\":true}}\n")
    writer.close

    begin
      expect(described_class.new(stdin:, stdout: reader, wait_thread: FakeWaitThread.new, timeout: 1).request("ping")).to eq("io" => true)
    ensure
      reader.close unless reader.closed?
    end
  end
end
