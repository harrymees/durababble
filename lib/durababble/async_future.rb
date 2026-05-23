# typed: true
# frozen_string_literal: true

module Durababble
  class AsyncFuture
    class << self
      #: (*untyped) -> untyped
      def await_all(*futures)
        flattened = futures.flatten
        flattened.each(&:start)

        results = []
        first_error = nil #: untyped
        flattened.each_with_index do |future, index|
          results[index] = future.value
        rescue StandardError => e
          first_error ||= e #: untyped
        end

        raise first_error if first_error

        results
      end
    end

    #: untyped
    attr_reader :position

    #: (execution: untyped, workflow: untyped, position: Integer, block: untyped) -> void
    def initialize(execution:, workflow:, position:, block:)
      @execution = execution
      @workflow = workflow
      @position = position
      @block = block
      @mutex = Mutex.new
      @state = :pending
      @thread = nil
      @result = nil
      @error = nil
      @observed = false
    end

    #: () -> untyped
    def start
      thread = @mutex.synchronize do
        return self unless @state == :pending

        @state = :running
        @thread = Thread.new { run }
      end

      thread.abort_on_exception = false
      self
    end

    #: () -> untyped
    def value
      start
      thread = @mutex.synchronize { @thread }
      thread&.join

      @mutex.synchronize do
        @observed = true
        case @state
        when :completed
          @result
        when :canceled
          raise AsyncCanceled, "async step at position #{@position} was canceled"
        when :failed
          raise @error
        else
          raise AsyncBoundaryError, "async step at position #{@position} did not settle"
        end
      end
    end

    #: () -> bool
    def cancel
      @mutex.synchronize do
        return false unless @state == :pending
        return false unless @execution.cancel_async_position(@position)

        @state = :canceled
        @observed = true
        true
      end
    end

    #: () -> bool
    def pending?
      @mutex.synchronize { @state == :pending }
    end

    #: () -> bool
    def running?
      @mutex.synchronize { @state == :running }
    end

    #: () -> bool
    def settled?
      @mutex.synchronize { [:completed, :failed, :canceled].include?(@state) }
    end

    #: () -> Symbol
    def state
      @mutex.synchronize { @state }
    end

    #: () -> bool
    def observed?
      @mutex.synchronize { @observed }
    end

    #: () -> void
    def wait_if_started
      thread = @mutex.synchronize { @thread }
      thread&.join
    end

    private

    #: () -> void
    def run
      result = @execution.run_async_position(@position) do
        @workflow.instance_exec(&@block)
      end
      @mutex.synchronize do
        @result = result
        @state = :completed
      end
    rescue StandardError => e
      @mutex.synchronize do
        @error = e
        @state = e.is_a?(AsyncCanceled) ? :canceled : :failed
      end
    end
  end
end
