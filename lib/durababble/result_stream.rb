# typed: true
# frozen_string_literal: true

require "async"
require "async/limited_queue"

module Durababble
  # Enumerable consumer for a streaming-result RPC, built directly on the async
  # reactor — no background threads. The producer block runs as a *transient*
  # child of the consumer's current `Async::Task` and pushes decoded values into
  # a bounded `Async::LimitedQueue`; `each`/`read` dequeue from that queue on the
  # consumer's fiber. Because producer and consumer are fibers on the same
  # reactor, the bounded queue both bridges them and applies back-pressure: a
  # full queue parks the producer fiber (and, for the remote case, HTTP/2 flow
  # control parks the server-side producer too).
  #
  # Consumer contract: `each`/`read` must run on a reactor. `each` (and every
  # `Enumerable` method built on it) self-wraps in `Sync {}` when called off a
  # reactor, so the common "give me the values" use works from a plain thread.
  # `read` is incremental and therefore *requires* an ambient reactor (so the
  # producer task survives between pulls); a plain-thread caller wraps its reads
  # in one `Sync {}`/`Async {}` block. Calling `read` off a reactor raises
  # `NotOnReactor`.
  #
  # Lifetime: the producer is owned by the consumer's reactor task. It is stopped
  # when iteration ends, when `close` is called, or when the surrounding reactor
  # scope finishes (a transient task is cancelled once only transient tasks
  # remain). There is no detached thread or connection to leak: a stream that is
  # started and then abandoned has its producer unwound when its reactor scope
  # ends, and a stream that is never iterated never starts a producer at all.
  class ResultStream
    include Enumerable

    # Bounded producer/consumer bridge depth. Bounds in-flight, undelivered values
    # for back-pressure; HTTP/2 flow control bounds bytes on the wire separately.
    STREAM_BUFFER = 16

    # How long `close`/teardown waits for a producer to observe cancellation and
    # unwind cooperatively before hard-stopping it. Well-behaved producers (which
    # poll `cancelled?` or are mid-`emit`) unwind well within this; the backstop
    # only bounds a producer that ignores cancellation, so `close` cannot deadlock.
    STREAM_CLOSE_TIMEOUT = 5

    # Raised when `read` is called without an ambient reactor. `each` self-wraps,
    # but incremental `read` needs the producer task to persist across calls, so
    # the caller must establish one reactor scope (`Sync {}` / `Async {}`).
    class NotOnReactor < Durababble::Error; end

    # Producer-facing handle. The producer block is called with a Writer and must
    # `emit` values until it is done or `cancelled?` (the consumer went away).
    class Writer
      #: (untyped) -> void
      def initialize(queue)
        @queue = queue
      end

      # Enqueues a value, parking the producer fiber while the bounded queue is
      # full (back-pressure). Raises `Async::Queue::ClosedError` once the consumer
      # has closed the queue, which unwinds an emitting producer.
      #: (Object?) -> void
      def emit(value)
        @queue.enqueue([:value, value])
      end

      #: () -> bool
      def cancelled?
        @queue.closed?
      end
    end

    #: () { (Writer) -> void } -> void
    def initialize(&producer)
      @producer = producer
      @queue = Async::LimitedQueue.new(STREAM_BUFFER) #: untyped
      @started = false
      @closed = false
      @producer_task = nil #: untyped
    end

    # @override
    #: () ?{ (Object?) -> void } -> (ResultStream | Enumerator[Object?])
    def each(&block)
      return enum_for(:each) unless block

      if Async::Task.current?
        consume(&block)
      else
        Sync { consume(&block) }
      end
    end

    # Returns the next value, or nil once the stream has ended. Re-raises a
    # producer error. Must run on a reactor (see `NotOnReactor`). Note: a stream
    # that emits nil values is indistinguishable from end-of-stream via `read`;
    # use `each` for such streams.
    #: () -> Object?
    def read
      ensure_started
      decode(@queue.dequeue)
    end

    # Cancels the producer and releases the bridge. Idempotent. Closing the queue
    # flips `cancelled?` (so a polling producer breaks and a parked `emit` raises),
    # then we wait for the producer to unwind so its teardown (e.g. resetting the
    # remote HTTP/2 stream, running an `ensure`) completes before `close` returns.
    #: () -> void
    def close
      return if @closed

      @closed = true
      @queue.close
      wait_for_producer
      nil
    end

    private

    # Core consume loop. Runs on the caller's reactor task: starts the producer as
    # a transient child, then dequeues until the queue closes (graceful end) or a
    # terminal error frame is dequeued (re-raised). The `ensure` cancels and waits
    # for the producer so it never outlives the consume call — an early `break`, an
    # exception, or a normal end all unwind it.
    #: () { (Object?) -> void } -> ResultStream
    def consume(&block)
      ensure_started
      while (item = @queue.dequeue)
        block.call(decode_value(item))
      end
      self
    ensure
      @queue.close
      wait_for_producer
    end

    # Lazily spawns the producer the first time the stream is consumed. Requires
    # an ambient reactor task to parent the producer; the producer is transient so
    # it is cancelled when the parent reactor scope finishes even if the consumer
    # neither finishes iterating nor calls `close`.
    #: () -> void
    def ensure_started
      return if @started || @closed

      parent = Async::Task.current?
      raise NotOnReactor, "ResultStream must be consumed on a reactor; wrap consumption in Sync { } or Async { }" unless parent

      @started = true
      @producer_task = self.class.spawn_producer(parent, @producer, @queue)
    end

    # Cancels (cooperatively) and waits for the producer. The queue is already
    # closed by the caller, so a well-behaved producer unwinds promptly and runs
    # its own teardown; we wait for that within a grace window. If the producer
    # ignores cancellation, the grace expires and we hard-stop it, bounding the
    # wait so `close`/teardown cannot deadlock. Off a reactor there is nothing to
    # wait on (the producer's reactor scope has already ended).
    #: () -> void
    def wait_for_producer
      task = @producer_task
      return unless task

      @producer_task = nil
      parent = Async::Task.current?
      return unless parent

      begin
        parent.with_timeout(STREAM_CLOSE_TIMEOUT) { task.wait }
      rescue Async::TimeoutError
        task.stop
        task.wait
      end
    rescue StandardError, Async::Stop
      nil
    end

    # Raises on a terminal error frame, otherwise returns the value. `nil` (queue
    # closed) maps to `nil` end-of-stream.
    #: ([Symbol, Object?]?) -> Object?
    def decode(item)
      return unless item

      decode_value(item)
    end

    #: ([Symbol, Object?]) -> Object?
    def decode_value(item)
      tag, payload = item
      if tag == :error
        error = payload #: as Exception
        raise error
      end

      payload
    end

    # Producer-task helpers live in the singleton class so a spawned task block
    # captures only the `ResultStream` class (a constant) and the plain
    # producer/queue arguments — never a stream instance.
    class << self
      # Spawns the producer as a transient child of the consumer's task. The task
      # is transient: it does not by itself keep the reactor alive, and it is
      # cancelled when only transient tasks remain.
      #: (untyped, ^(Writer) -> void, untyped) -> untyped
      def spawn_producer(parent, producer, queue)
        parent.async(transient: true) { run_producer(producer, queue) }
      end

      # Runs on the producer task's fiber. Takes the producer block and queue as
      # plain arguments (not `self`) so the task does not reference the stream. A
      # producer error becomes a terminal error frame; the `ensure` closes the
      # queue so the consumer's `dequeue` returns nil (graceful end). `Async::Stop`
      # (from cancellation) is not a `StandardError`, so it unwinds past the rescue
      # and only runs the `ensure`.
      #: (^(Writer) -> void, untyped) -> void
      def run_producer(producer, queue)
        producer.call(Writer.new(queue))
      rescue StandardError => e
        enqueue_error(queue, e)
      ensure
        queue.close
      end

      # Delivers a terminal error frame unless the consumer already closed the
      # queue (in which case there is nothing to deliver and the enqueue raises).
      #: (untyped, Exception) -> void
      def enqueue_error(queue, error)
        queue.enqueue([:error, error]) unless queue.closed?
      rescue Async::Queue::ClosedError
        nil
      end
    end
  end
end
