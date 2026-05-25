# typed: true
# frozen_string_literal: true

require "async/condition"

module Durababble
  class CommandFuture
    #: (untyped) -> void
    def initialize(command_id)
      @command_id = command_id
      @condition = Async::Condition.new
      @done = false
      @result = nil
      @error = nil
    end

    #: () -> bool
    def done?
      @done
    end

    #: () -> void
    def wait
      @condition.wait unless @done
    end

    #: () -> void
    def wake
      @condition.signal
    end

    #: () -> untyped
    def value
      raise @error if @error

      @result
    end

    #: (untyped) -> void
    def resolve(result)
      return if @done

      @done = true
      @result = result
      @condition.signal
    end

    #: (untyped) -> void
    def reject(error)
      return if @done

      @done = true
      @error = error
      @condition.signal
    end
  end
end
