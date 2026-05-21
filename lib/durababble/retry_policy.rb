# frozen_string_literal: true

module Durababble
  class RetryPolicy
    DEFAULT_INITIAL_INTERVAL = 1
    DEFAULT_BACKOFF_COEFFICIENT = 2.0

    attr_reader :initial_interval, :backoff_coefficient, :maximum_interval, :maximum_attempts, :schedule, :non_retryable_errors

    def self.from(value)
      return value if value.is_a?(self)
      return new(maximum_attempts: 1) if value.nil?

      new(**value)
    end

    def initialize(initial_interval: DEFAULT_INITIAL_INTERVAL, backoff_coefficient: DEFAULT_BACKOFF_COEFFICIENT, maximum_interval: nil, maximum_attempts: 1, schedule: nil, non_retryable_errors: [])
      @initial_interval = interval_seconds(initial_interval)
      @backoff_coefficient = Float(backoff_coefficient)
      @maximum_interval = maximum_interval && interval_seconds(maximum_interval)
      @maximum_attempts = maximum_attempts.nil? ? Float::INFINITY : Integer(maximum_attempts)
      @schedule = Array(schedule).map { |interval| interval_seconds(interval) }
      @non_retryable_errors = Array(non_retryable_errors)
    end

    def retryable?(error, attempt_number:)
      return false if attempt_number >= maximum_attempts
      return false if non_retryable_errors.any? { |error_class| error.is_a?(error_class) || error.class.name == error_class.to_s }

      true
    end

    def delay_for_attempt(attempt_number)
      explicit = schedule[attempt_number - 1]
      return explicit if explicit

      delay = initial_interval * (backoff_coefficient**(attempt_number - 1))
      delay = [delay, maximum_interval].min if maximum_interval
      delay
    end

    private

    def interval_seconds(value)
      return value.to_f if value.is_a?(Numeric)
      return value.to_f if value.respond_to?(:to_f)

      raise ArgumentError, "retry intervals must be numeric seconds"
    end
  end
end
