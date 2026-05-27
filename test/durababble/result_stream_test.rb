# typed: false
# frozen_string_literal: true

require "async"
require "timeout"
require_relative "../test_helper"

class DurababbleResultStreamTest < DurababbleTestCase
  ResultStream = Durababble::ResultStream

  test "each yields every emitted value then terminates" do
    stream = ResultStream.new do |writer|
      [1, 2, 3].each { |n| writer.emit(n) }
    end

    assert_equal [1, 2, 3], stream.each.to_a
  end

  test "each returns the stream and is empty for a producer that emits nothing" do
    stream = ResultStream.new { |_writer| }

    collected = []
    result = stream.each { |v| collected << v }

    assert_empty collected
    assert_same stream, result
  end

  test "read returns values in order then nil at end" do
    stream = ResultStream.new do |writer|
      writer.emit("a")
      writer.emit("b")
    end

    # `read` is incremental, so it must run within a single reactor scope (the
    # producer task has to survive between pulls).
    Sync do
      assert_equal "a", stream.read
      assert_equal "b", stream.read
      assert_nil stream.read
      assert_nil stream.read
    end
  end

  test "read raises NotOnReactor when called off a reactor" do
    stream = ResultStream.new { |writer| writer.emit(:value) }

    assert_raises(ResultStream::NotOnReactor) { stream.read }
  end

  test "each re-raises a producer error after delivering prior values" do
    stream = ResultStream.new do |writer|
      writer.emit(:first)
      raise Durababble::WorkflowRpc::StaleLease, "lease lost mid-stream"
    end

    seen = []
    error = assert_raises(Durababble::WorkflowRpc::StaleLease) do
      stream.each { |v| seen << v }
    end

    assert_equal [:first], seen
    assert_match(/lease lost mid-stream/, error.message)
  end

  test "supports Enumerable composition" do
    stream = ResultStream.new do |writer|
      (1..5).each { |n| writer.emit(n) }
    end

    assert_equal [2, 4], stream.select(&:even?)
  end

  test "close stops the producer and is idempotent" do
    observed_cancel = Thread::Queue.new
    stream = ResultStream.new do |writer|
      loop do
        break if writer.cancelled?

        begin
          writer.emit(:tick)
        rescue Async::Queue::ClosedError
          break
        end
      end
      observed_cancel << :cancelled
    end

    Sync do
      # Pull one value to guarantee the producer is running, then cancel it.
      assert_equal :tick, stream.read
      stream.close
      stream.close # idempotent
    end

    # close cooperatively cancels and waits for the producer, so by the time the
    # reactor scope ends the producer has observed cancellation and run its tail.
    assert_equal :cancelled, observed_cancel.pop
  end

  test "close before iteration never starts producing values to a consumer" do
    stream = ResultStream.new do |writer|
      writer.emit(:value)
    end

    stream.close

    # After close the bridge is closed; iteration never starts the producer.
    assert_empty stream.each.to_a
  end

  test "back-pressure parks a fast producer until a slow consumer drains" do
    # Producer and consumer are fibers on one reactor bridged by a bounded queue
    # (STREAM_BUFFER). A producer that outruns the consumer fills the queue and
    # parks in `emit`; it cannot run to completion behind a single consumed value.
    # (Over the wire, HTTP/2 flow control adds a second bound on top of this one.)
    total = 1_000
    emitted = []
    stream = ResultStream.new do |writer|
      1.upto(total) do |n|
        writer.emit(n)
        emitted << n
      end
    end

    Sync do
      assert_equal 1, stream.read

      # Park the consumer so the producer runs as far as it possibly can. With the
      # single read having freed one slot, it refills that slot and then parks again
      # on the full queue — pinned near STREAM_BUFFER, nowhere near `total`.
      sleep(0.05)

      assert_operator emitted.size, :<, total
      assert_operator emitted.size, :<=, ResultStream::STREAM_BUFFER + 2

      stream.close
    end
  end

  test "an abandoned partial read has its producer stopped when the reactor scope ends" do
    stopped = Thread::Queue.new

    # Start the producer (one read), then abandon the stream without calling
    # close. There is no background thread: the producer is a transient child of
    # this reactor scope, so when the scope ends it is cancelled and unwinds.
    Sync do
      stream = ResultStream.new(&indefinite_producer(stopped))
      stream.read
    end

    assert_equal :stopped, Timeout.timeout(2) { stopped.pop }
  end

  private

  def indefinite_producer(stopped)
    proc do |writer|
      writer.emit(:tick)
      sleep(0.01) until writer.cancelled?
    ensure
      stopped << :stopped
    end
  end
end
