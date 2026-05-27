# typed: true
# frozen_string_literal: true

require "durababble"

module Durababble
  module Deterministic
    Result = Data.define(:scenario, :seed, :trace, :digest, :violations, :summary)

    class << self
      #: (untyped, seed: untyped) -> untyped
      def prove(scenario, seed:)
        Scenarios.fetch(scenario).call(seed)
      end

      #: (untyped, seeds: untyped) -> untyped
      def search(scenario, seeds:)
        seeds.filter_map do |seed|
          result = prove(scenario, seed:)
          [seed, result.violations] unless result.violations.empty?
        end
      end
    end

    class Rng
      MASK = (1 << 64) - 1

      #: (untyped) -> void
      def initialize(seed)
        @state = seed & MASK
      end

      #: () -> untyped
      def next_u64
        @state = (@state * 6_364_136_223_846_793_005 + 1_442_695_040_888_963_407) & MASK
      end

      #: (untyped) -> untyped
      def int(max)
        raise ArgumentError, "max must be positive" unless max.positive?

        next_u64 % max
      end

      #: (untyped) -> untyped
      def chance(percent)
        int(100) < percent
      end
    end

    class Trace
      #: untyped
      attr_reader :lines

      #: () -> void
      def initialize
        @lines = []
      end

      #: (untyped, untyped, untyped, ?untyped) -> untyped
      def event(time, actor, name, fields = {})
        stable = fields.sort_by { |key, _| key.to_s }.map { |key, value| "#{key}=#{stable_value(value)}" }.join(" ")
        @lines << format("t=%06d actor=%s event=%s%s", time, actor, name, stable.empty? ? "" : " #{stable}")
      end

      #: () -> untyped
      def to_s
        @lines.join("\n")
      end

      private

      #: (untyped) -> untyped
      def stable_value(value)
        case value
        when Hash
          "{" + value.sort_by { |key, _| key.to_s }.map { |key, val| "#{key}:#{stable_value(val)}" }.join(",") + "}"
        when Array
          "[" + value.map { |val| stable_value(val) }.join(",") + "]"
        else
          value.inspect
        end
      end
    end

    class Scheduler
      #: untyped
      attr_reader :time, :rng, :trace

      #: (seed: untyped, ?trace: untyped) -> void
      def initialize(seed:, trace: Trace.new)
        @rng = Rng.new(seed)
        @trace = trace
        @time = 0
        @seq = 0
        @events = []
      end

      #: (actor: untyped, delay: untyped, name: untyped) { (?) -> untyped } -> untyped
      def schedule(actor:, delay:, name:, &block)
        @seq += 1
        event = [@time + delay, @seq, actor, name, block]
        @events << event
        @events.sort_by! { |time, seq, _actor, _name, _block| [time, seq] }
        trace.event(@time, actor, "schedule", at: @time + delay, name:)
      end

      #: (untyped) -> untyped
      def advance(duration)
        @time += duration
        trace.event(@time, "scheduler", "advance", by: duration)
      end

      #: (?max_events: untyped, ?after_event: untyped) -> untyped
      def run(max_events: 10_000, after_event: nil)
        count = 0
        until @events.empty?
          raise "deterministic scheduler exceeded #{max_events} events" if count >= max_events

          event_time, _seq, actor, name, block = @events.shift
          @time = event_time
          trace.event(@time, actor, "run", name:)
          block.call
          after_event&.call(actor:, name:, time: @time)
          count += 1
        end
      end
    end

    class VirtualNetwork
      #: (untyped) -> untyped
      attr_accessor :duplicate_percent

      #: (scheduler: untyped, ?min_latency: untyped, ?max_latency: untyped, ?drop_percent: untyped) -> void
      def initialize(scheduler:, min_latency: 1, max_latency: 9, drop_percent: 0)
        @scheduler = scheduler
        @min_latency = min_latency
        @max_latency = max_latency
        @drop_percent = drop_percent
        @duplicate_percent = 0
        @partitions = {}
      end

      #: (untyped, untyped) -> untyped
      def partition(source, target)
        @partitions[[source, target]] = true
        @scheduler.trace.event(@scheduler.time, "network", "partition", source:, target:)
      end

      #: (untyped, untyped) -> untyped
      def heal(source, target)
        @partitions.delete([source, target])
        @scheduler.trace.event(@scheduler.time, "network", "heal", source:, target:)
      end

      #: (source: untyped, target: untyped, type: untyped, ?payload: untyped) { (?) -> untyped } -> untyped
      def send(source:, target:, type:, payload: {}, &handler)
        if @partitions[[source, target]] || @scheduler.rng.chance(@drop_percent)
          @scheduler.trace.event(@scheduler.time, "network", "network.drop", source:, target:, type:)
          return
        end

        delay = @min_latency + @scheduler.rng.int(@max_latency - @min_latency + 1)
        @scheduler.trace.event(@scheduler.time, "network", "network.send", source:, target:, type:, delay:)
        schedule_delivery(source:, target:, type:, payload:, delay:, duplicate: false, &handler)
        return unless @scheduler.rng.chance(@duplicate_percent)

        duplicate_delay = delay + 1 + @scheduler.rng.int(3)
        @scheduler.trace.event(@scheduler.time, "network", "network.duplicate", source:, target:, type:, delay: duplicate_delay)
        schedule_delivery(source:, target:, type:, payload:, delay: duplicate_delay, duplicate: true, &handler)
      end

      private

      #: (source: untyped, target: untyped, type: untyped, payload: untyped, delay: untyped, duplicate: untyped) { (?) -> untyped } -> untyped
      def schedule_delivery(source:, target:, type:, payload:, delay:, duplicate:, &handler)
        @scheduler.schedule(actor: target, delay:, name: duplicate ? "deliver_duplicate:#{type}" : "deliver:#{type}") do
          @scheduler.trace.event(@scheduler.time, "network", "deliver", source:, target:, type:)
          handler.call(payload)
        end
      end
    end

    class FaultPlan
      #: (scheduler: untyped) -> void
      def initialize(scheduler:)
        @scheduler = scheduler
        @after = Hash.new { |hash, key| hash[key] = [] }
        @counts = Hash.new(0)
      end

      #: (untyped, ?once: untyped, ?message: untyped) -> untyped
      def fail_after(operation, once: 1, message: nil)
        @after[operation.to_s] << { remaining: once, message: message || "injected fault after #{operation}" }
      end

      #: (untyped) -> untyped
      def after(operation)
        operation = operation.to_s
        @counts[operation] += 1
        fault = @after[operation].find { |candidate| candidate.fetch(:remaining).positive? }
        return unless fault

        fault[:remaining] -= 1
        @scheduler.trace.event(@scheduler.time, "fault", "fault.injected", operation:, count: @counts.fetch(operation), message: fault.fetch(:message))
        raise InjectedCrash, fault.fetch(:message)
      end
    end
  end
end
