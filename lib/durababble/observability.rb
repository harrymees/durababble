# typed: true
# frozen_string_literal: true

require "time"
require "opentelemetry-api"
require "opentelemetry-metrics-api"

module Durababble
  module Observability
    INSTRUMENTATION_NAME = "durababble"
    KNOWN_STORE_TABLES = [
      "workflows",
      "steps",
      "step_attempts",
      "waits",
      "fences",
      "outbox",
      "durable_objects",
      "durable_object_commands",
    ].freeze

    class Configuration
      #: (enabled: untyped, ?attributes: untyped) -> void
      def initialize(enabled:, attributes: {})
        @enabled = enabled
        @attributes = attributes.transform_keys(&:to_s).freeze
        @tracer = nil
        @meter = nil
        @instruments = {}
      end

      #: untyped
      attr_reader :attributes

      #: () -> untyped
      def enabled? = @enabled

      #: () -> untyped
      def tracer
        return unless enabled?

        @tracer ||= OpenTelemetry.tracer_provider.tracer(INSTRUMENTATION_NAME, Durababble::VERSION)
      end

      #: () -> untyped
      def meter
        return unless enabled?

        @meter ||= OpenTelemetry.meter_provider.meter(INSTRUMENTATION_NAME, version: Durababble::VERSION)
      end

      #: (untyped, untyped) -> untyped
      def instrument(kind, name)
        return unless enabled?

        key = [kind, name]
        @instruments.fetch(key) { @instruments[key] = build_instrument(kind, name) }
      end

      private

      #: (untyped, untyped) -> untyped
      def build_instrument(kind, name)
        current_meter = meter

        case kind
        when :counter
          current_meter.create_counter(name, unit: "1")
        when :histogram
          current_meter.create_histogram(name, unit: "ms")
        end
      end
    end

    class << self
      #: (?enabled: untyped, ?attributes: untyped) -> untyped
      def configure(enabled: false, attributes: {})
        @configuration = Configuration.new(enabled:, attributes:)
      end

      #: () -> untyped
      def configuration
        @configuration ||= Configuration.new(enabled: false)
      end

      #: (untyped, ?untyped, **untyped) { (untyped) -> untyped } -> untyped
      def trace(name, attributes = nil, **keyword_attributes, &block)
        attributes = normalize_attribute_args(attributes, keyword_attributes)
        config = configuration
        return block.call(nil) unless config.enabled?

        tracer = config.tracer
        tracer.in_span(name, attributes: clean_attributes(config.attributes.merge(attributes))) do |span|
          block.call(span)
        rescue StandardError => e
          annotate_error(span, e)
          raise
        end
      end

      #: (untyped, ?untyped, ?by: untyped, **untyped) -> untyped
      def count(name, attributes = nil, by: 1, **keyword_attributes)
        attributes = normalize_attribute_args(attributes, keyword_attributes)
        instrument = configuration.instrument(:counter, name)
        return unless instrument

        instrument.add(by, attributes: clean_attributes(configuration.attributes.merge(attributes)))
      end

      #: (untyped, untyped, ?untyped, **untyped) -> untyped
      def record(name, value, attributes = nil, **keyword_attributes)
        attributes = normalize_attribute_args(attributes, keyword_attributes)
        instrument = configuration.instrument(:histogram, name)
        return unless instrument

        instrument.record(value, attributes: clean_attributes(configuration.attributes.merge(attributes)))
      end

      #: () -> untyped
      def monotonic_ms
        Process.clock_gettime(Process::CLOCK_MONOTONIC, :float_millisecond)
      end

      #: (untyped, ?untyped, **untyped) { (?) -> untyped } -> untyped
      def measure(name, attributes = nil, **keyword_attributes, &block)
        attributes = normalize_attribute_args(attributes, keyword_attributes)
        started_at = monotonic_ms
        block.call
      rescue StandardError
        count("#{name}.errors", attributes)
        raise
      ensure
        record("#{name}.duration", monotonic_ms - started_at, attributes)
      end

      #: (untyped) -> untyped
      def store_backend(store)
        store.is_a?(Durababble::MysqlStore) ? "mysql" : "postgresql"
      end

      #: (untyped) -> untyped
      def query_shape(sql)
        operation = sql.to_s[/\A\s*([a-z]+)/i, 1]&.downcase || "unknown"
        table = KNOWN_STORE_TABLES.find { |name| sql.to_s.include?(name) } || "unknown"
        "#{operation}.#{table}"
      end

      #: (untyped, untyped) -> untyped
      def annotate_error(span, error)
        return unless span
        return if ["Durababble::WorkflowSuspended", "Durababble::StepRetryScheduled"].include?(error.class.name)

        attrs = { "error.type" => error.class.name, "error.message" => error.message.to_s }
        span.add_attributes(clean_attributes(attrs)) if span.respond_to?(:add_attributes)
        span.record_exception(error) if span.respond_to?(:record_exception)
      end

      #: (untyped) -> untyped
      def clean_attributes(attributes)
        attributes.each_with_object({}) do |(key, value), cleaned|
          next if value.nil?

          cleaned[key.to_s] = case value
          when String, Numeric, TrueClass, FalseClass
            value
          when Symbol
            value.to_s
          when Time
            value.utc.iso8601(6)
          else
            value.to_s
          end
        end
      end

      #: (untyped, untyped) -> untyped
      def normalize_attribute_args(attributes, keyword_attributes)
        base = attributes || {}
        base = base.to_h if base.respond_to?(:to_h)
        base.merge(keyword_attributes)
      end
    end
  end
end
