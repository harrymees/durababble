# typed: true
# frozen_string_literal: true

require "time"
require "opentelemetry-api"
require "opentelemetry-metrics-api"

module Durababble
  module Observability
    INSTRUMENTATION_NAME = "durababble"

    class Configuration
      #: (enabled: bool, ?attributes: Hash[Symbol | String, Object?]) -> void
      def initialize(enabled:, attributes: {})
        @enabled = enabled
        @attributes = attributes.transform_keys(&:to_s).freeze
        @tracer = nil
        @meter = nil
        @instruments = {}
      end

      #: Hash[String, Object?]
      attr_reader :attributes

      #: () -> bool
      def enabled? = @enabled

      #: () -> Object?
      def tracer
        return unless enabled?

        @tracer ||= OpenTelemetry.tracer_provider.tracer(INSTRUMENTATION_NAME, Durababble::VERSION)
      end

      #: () -> Object?
      def meter
        return unless enabled?

        @meter ||= OpenTelemetry.meter_provider.meter(INSTRUMENTATION_NAME, version: Durababble::VERSION)
      end

      #: (Symbol, String) -> Object?
      def instrument(kind, name)
        return unless enabled?

        key = [kind, name]
        @instruments.fetch(key) { @instruments[key] = build_instrument(kind, name) }
      end

      private

      #: (Symbol, String) -> Object?
      def build_instrument(kind, name)
        current_meter = meter #: as untyped

        case kind
        when :counter
          current_meter.create_counter(name, unit: "1")
        when :histogram
          current_meter.create_histogram(name, unit: "ms")
        end
      end
    end

    class << self
      #: (?enabled: bool, ?attributes: Hash[Symbol | String, Object?]) -> Configuration
      def configure(enabled: false, attributes: {})
        @configuration = Configuration.new(enabled:, attributes:)
      end

      #: () -> Configuration
      def configuration
        @configuration ||= Configuration.new(enabled: false)
      end

      #: [Result] (String, ?Hash[Symbol | String, Object?]?, **Object?) { (Object?) -> Result } -> Result
      def trace(name, attributes = nil, **keyword_attributes, &block)
        config = configuration
        return block.call(nil) unless config.enabled?

        tracer = config.tracer #: as untyped
        tracer.in_span(name, attributes: instrumentation_attributes(config, attributes, keyword_attributes)) do |span|
          block.call(span)
        rescue StandardError => e
          annotate_error(span, e)
          raise
        end
      end

      #: (String, ?Hash[Symbol | String, Object?]?, ?by: Numeric, **Object?) -> void
      def count(name, attributes = nil, by: 1, **keyword_attributes)
        config = configuration
        instrument = config.instrument(:counter, name) #: as untyped
        return unless instrument

        instrument.add(by, attributes: instrumentation_attributes(config, attributes, keyword_attributes))
      end

      #: (String, Numeric, ?Hash[Symbol | String, Object?]?, **Object?) -> void
      def record(name, value, attributes = nil, **keyword_attributes)
        config = configuration
        instrument = config.instrument(:histogram, name) #: as untyped
        return unless instrument

        instrument.record(value, attributes: instrumentation_attributes(config, attributes, keyword_attributes))
      end

      #: () -> Float
      def monotonic_ms
        Process.clock_gettime(Process::CLOCK_MONOTONIC, :float_millisecond).to_f
      end

      #: [Result] (String, ?Hash[Symbol | String, Object?]?, **Object?) { () -> Result } -> Result
      def measure(name, attributes = nil, **keyword_attributes, &block)
        config = configuration
        return block.call unless config.enabled?

        attributes = normalize_attribute_args(attributes, keyword_attributes)
        metric_attributes = attributes.transform_keys(&:to_sym)
        started_at = monotonic_ms
        begin
          block.call
        rescue StandardError
          count("#{name}.errors", metric_attributes, by: 1)
          raise
        ensure
          record("#{name}.duration", monotonic_ms - started_at, metric_attributes)
        end
      end

      #: (Object) -> String
      def store_backend(store)
        store.is_a?(Durababble::MysqlStore) ? "mysql" : "postgresql"
      end

      #: (Object, StandardError) -> Object?
      def annotate_error(span, error)
        span = span #: as untyped
        return if ["Durababble::WorkflowSuspended", "Durababble::StepRetryScheduled"].include?(error.class.name)

        span.add_attributes("error.type" => error.class.name)
        span.record_exception(error)
      end

      #: (Hash[Symbol | String, Object?]?, Hash[Symbol, Object?]) -> Hash[String, Object?]
      def normalize_attribute_args(attributes, keyword_attributes)
        normalized = {}
        base = attributes || {}
        base = base.to_h if base.respond_to?(:to_h)
        base.each { |key, value| normalized[key.to_s] = value }
        return normalized if keyword_attributes.empty?

        normalized.merge(keyword_attributes.transform_keys(&:to_s))
      end

      #: (Configuration, Hash[Symbol | String, Object?]?, Hash[Symbol, Object?]) -> Hash[String, Object?]
      def instrumentation_attributes(config, attributes, keyword_attributes)
        dynamic_attributes = normalize_attribute_args(attributes, keyword_attributes)
        return config.attributes if dynamic_attributes.empty?

        config.attributes.merge(dynamic_attributes)
      end
    end
  end
end
