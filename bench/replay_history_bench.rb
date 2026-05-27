#!/usr/bin/env ruby
# typed: false
# frozen_string_literal: true

# Micro-benchmark for the replay engine's history index (WorkflowReplayHistory).
#
# Replaying a workflow drives the per-command "safe point" hot path: for every
# command the engine asks whether replay is still blocked by recorded history
# (blocked_by_replay_history?) and whether each recorded workflow command is yet
# deliverable (workflow_command_event_deliverable?). Both questions are answered
# against the sorted set of blocking event indexes. A naive implementation scans
# that whole set on every question, so replaying a workflow whose history holds N
# events costs O(N) per command x O(N) commands = O(N^2).
#
# This benchmark builds synthetic histories of increasing size and times a driver
# that mirrors the engine's replay loop, so the before/after numbers isolate the
# algorithmic complexity of the history index without any database in the path.
#
# Usage:
#   mise exec -- ruby bench/replay_history_bench.rb
#   DURABABBLE_BENCH_SIZES=1000,2000,4000,8000 mise exec -- ruby bench/replay_history_bench.rb

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "durababble"

module Durababble
  module ReplayHistoryBench
    extend self

    # A future stand-in: deliver_resolutions only needs done?/resolve/reject here.
    class FakeFuture
      def initialize
        @done = false
      end

      def done?
        @done
      end

      def resolve(_result)
        @done = true
      end

      def reject(_error)
        @done = true
      end
    end

    # Build a recorded history of `steps` durable steps, each scheduled then
    # completed, with a workflow-command event injected every `command_every`
    # steps. This is the shape a long-running workflow accumulates and then
    # replays from scratch after a crash or lease move.
    def build_history(steps:, command_every:)
      events = []
      event_index = 0
      steps.times do |i|
        shape = { "name" => "step_#{i}", "args" => [i], "kwargs" => {} }
        events << {
          "kind" => "step_scheduled",
          "command_id" => i,
          "event_index" => event_index,
          "name" => "step_#{i}",
          "payload" => shape,
        }
        event_index += 1
        events << {
          "kind" => "step_completed",
          "command_id" => i,
          "event_index" => event_index,
          "payload" => i * 2,
        }
        event_index += 1
        next unless command_every.positive? && ((i + 1) % command_every).zero?

        events << {
          "kind" => "workflow_command_completed",
          "command_id" => nil,
          "event_index" => event_index,
          "name" => "ping",
          "payload" => { "method" => "ping", "args" => [], "kwargs" => {}, "result" => nil },
        }
        event_index += 1
      end
      events
    end

    # Drive one full replay over a freshly-built history, mirroring the engine's
    # per-command safe-point sequence: deliver recorded workflow commands, ask the
    # two "are we blocked?" questions, then consume the next command's scheduled
    # and terminal events.
    def replay(history, steps:)
      rh = WorkflowReplayHistory.new(history)
      futures = {}
      command_id = 0
      loop do
        rh.deliver_workflow_commands { |_event| nil }
        rh.blocked_recorded_workflow_command?
        rh.blocked_by_replay_history?
        break if command_id >= steps

        shape = { "name" => "step_#{command_id}", "args" => [command_id], "kwargs" => {} }
        rh.validate_scheduled_shape!(workflow_id: "wf", command_id:, shape:)
        future = FakeFuture.new
        futures[command_id] = future
        rh.deliver_resolutions(futures) { |_event, f| f.resolve(nil) }
        command_id += 1
      end
    end

    def measure_once(steps:, command_every:)
      history = build_history(steps:, command_every:)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      replay(history, steps:)
      Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    end

    def median(values)
      sorted = values.sort
      mid = sorted.length / 2
      sorted.length.odd? ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2.0
    end

    def run
      sizes = ENV.fetch("DURABABBLE_BENCH_SIZES", "1000,2000,4000,8000").split(",").map { |s| Integer(s.strip) }
      command_every = Integer(ENV.fetch("DURABABBLE_BENCH_COMMAND_EVERY", "50"))
      warmup = Integer(ENV.fetch("DURABABBLE_BENCH_WARMUP", "1"))
      samples = Integer(ENV.fetch("DURABABBLE_BENCH_SAMPLES", "5"))

      puts "replay-history micro-benchmark (command_every=#{command_every}, samples=#{samples})"
      puts format("%-10s %12s %14s", "steps", "median_ms", "ms_per_step")
      previous = nil
      sizes.each do |steps|
        warmup.times { measure_once(steps:, command_every:) }
        timings = Array.new(samples) { measure_once(steps:, command_every:) }
        median_ms = median(timings) * 1000.0
        per_step_us = (median_ms / steps) * 1000.0
        scaling = previous ? format(" (%.2fx for %.2fx steps)", median_ms / previous[:median_ms], steps.to_f / previous[:steps]) : ""
        puts format("%-10d %12.3f %14.4f%s", steps, median_ms, per_step_us, scaling)
        previous = { steps:, median_ms: }
      end
    end
  end
end

Durababble::ReplayHistoryBench.run if $PROGRAM_NAME == __FILE__
