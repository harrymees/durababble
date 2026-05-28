# typed: true
# frozen_string_literal: true

require "time"

require_relative "workflow_determinism"

module Durababble
  module DurableTime
    class << self
      #: (Object) -> Object
      def comparable(value)
        return parse(value) if value.is_a?(String)

        value
      end

      #: (Object) -> Object
      def durable_comparable(value)
        time = value.is_a?(String) ? parse(value) : value
        return (time.to_i * 1_000_000) + time.usec if time.is_a?(Time)

        value
      end

      #: (String) -> Time
      def parse(value)
        WorkflowDeterminism.allow_host_operations do
          Time.iso8601(value)
        rescue ArgumentError
          Time.parse(value)
        end
      end
    end
  end
end
