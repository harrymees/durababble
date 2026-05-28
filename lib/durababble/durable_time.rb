# typed: true
# frozen_string_literal: true

require "time"

require_relative "workflow_determinism"

module Durababble
  module DurableTime
    STRICT_TIMESTAMP_PATTERN = /\A(?<year>\d{4})-(?<month>\d{2})-(?<day>\d{2})[T ](?<hour>\d{2}):(?<minute>\d{2}):(?<second>\d{2})(?:\.(?<fraction>\d{1,9}))?(?:(?<z>Z)|(?:\s*UTC)|(?:\s*(?<offset>[+-]\d{2}:?\d{2})))?\z/

    class << self
      #: (Object) -> Object
      def comparable(value)
        return parse(value) if value.is_a?(String)

        value
      end

      #: (Object) -> Object
      def durable_comparable(value)
        return timestamp_microseconds_from_string(value) if value.is_a?(String)
        return (value.to_i * 1_000_000) + value.usec if value.is_a?(Time)

        value
      end

      #: (String) -> Time
      def parse(value)
        parsed = STRICT_TIMESTAMP_PATTERN.match(value)
        if parsed
          microseconds = timestamp_microseconds_from_match(parsed)
          return (Time.utc(1970, 1, 1) + Rational(microseconds, 1_000_000)).getlocal(normalize_offset(parsed[:offset]))
        end

        # Time.parse consults Time.now for missing fields, so keep it outside the
        # workflow determinism guard and only use it for legacy non-ISO values.
        WorkflowDeterminism.allow_host_operations { Time.parse(value) }
      end

      private

      #: (String) -> Integer
      def timestamp_microseconds_from_string(value)
        parsed = STRICT_TIMESTAMP_PATTERN.match(value)
        return timestamp_microseconds_from_match(parsed) if parsed

        time = WorkflowDeterminism.allow_host_operations { Time.parse(value) }
        (time.to_i * 1_000_000) + time.usec
      end

      #: (MatchData) -> Integer
      def timestamp_microseconds_from_match(parsed)
        fraction = parsed[:fraction].to_s
        microseconds = fraction.empty? ? 0 : fraction.ljust(6, "0")[0, 6].to_i
        offset = normalize_offset(parsed[:offset])
        time = Time.utc(
          parsed[:year].to_i,
          parsed[:month].to_i,
          parsed[:day].to_i,
          parsed[:hour].to_i,
          parsed[:minute].to_i,
          parsed[:second].to_i,
          microseconds,
        )
        ((time.to_i - offset_seconds(offset)) * 1_000_000) + microseconds
      end

      #: (String?) -> String
      def normalize_offset(offset)
        return "+00:00" unless offset
        return offset if offset.include?(":")

        "#{offset[0, 3]}:#{offset[3, 2]}"
      end

      #: (String) -> Integer
      def offset_seconds(offset)
        sign = offset.start_with?("-") ? -1 : 1
        (offset[1, 2].to_i * 3600 + offset[4, 2].to_i * 60) * sign
      end
    end
  end
end
