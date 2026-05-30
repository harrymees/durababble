#!/usr/bin/env ruby
# typed: true
# frozen_string_literal: true

require "fileutils"
require "json"
require "optparse"
require "time"

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
$LOAD_PATH.unshift(File.expand_path("../test", __dir__))

options = {
  seeds: 1..100,
  scenarios: nil,
  coverage_dir: File.expand_path("../coverage/dst", __dir__),
  command_name: "dst-coverage",
  minimum_line: nil,
  minimum_branch: nil,
}
original_argv = ARGV.dup

parser = OptionParser.new do |opts|
  opts.banner = "Usage: scripts/dst-coverage.rb [options]"

  opts.on("--seeds RANGE", "Seed range to run, for example 1..100") do |value|
    first, last = value.split("..", 2).map { |part| Integer(part.strip) }
    raise OptionParser::InvalidArgument, "expected RANGE like 1..100" if last.nil?

    options[:seeds] = first..last
  end

  opts.on("--scenarios LIST", "Comma-separated scenario names; defaults to all seed-varying fuzz scenarios") do |value|
    options[:scenarios] = value.split(",").map(&:strip).reject(&:empty?)
  end

  opts.on("--coverage-dir DIR", "Coverage output directory; defaults to coverage/dst") do |value|
    options[:coverage_dir] = File.expand_path(value)
  end

  opts.on("--command-name NAME", "SimpleCov command name; defaults to dst-coverage") do |value|
    options[:command_name] = value
  end

  opts.on("--minimum-line PERCENT", Float, "Optional global line coverage minimum for this advisory run") do |value|
    options[:minimum_line] = value
  end

  opts.on("--minimum-branch PERCENT", Float, "Optional global branch coverage minimum for this advisory run") do |value|
    options[:minimum_branch] = value
  end
end
parser.parse!(ARGV)

require "simplecov"

# rubocop:disable Sorbet/ConstantsFromStrings -- SimpleCov and the DST harness are dynamic test-only dependencies without checked RBI.
simplecov = Object.const_get(:SimpleCov)
simplecov.public_send(:command_name, options.fetch(:command_name))
simplecov.public_send(:coverage_dir, options.fetch(:coverage_dir))
simplecov.public_send(:start) do
  public_send(:enable_coverage, :branch)
  public_send(:primary_coverage, :line)
  public_send(:root, File.expand_path("..", __dir__))
  public_send(:track_files, "lib/**/*.rb")

  public_send(:add_filter, "/test/")
  public_send(:add_filter, "/bench/")
  public_send(:add_filter, "/docs/")
  public_send(:add_filter, "/examples/")
  public_send(:add_filter, "/scripts/")
  public_send(:add_filter, "/sig/")
  public_send(:add_filter, "/sorbet/")
  public_send(:add_filter, "lib/durababble/version.rb")

  public_send(:add_group, "Library", "lib")

  public_send(:minimum_coverage, line: options.fetch(:minimum_line)) if options.fetch(:minimum_line)
  public_send(:minimum_coverage, branch: options.fetch(:minimum_branch)) if options.fetch(:minimum_branch)
end

require "active_support/isolated_execution_state"

ActiveSupport::IsolatedExecutionState.isolation_level = :fiber

require "support/deterministic"
require "support/deterministic/scenario_sets"

deterministic = Durababble.const_get(:Deterministic)
scenario_sets = deterministic.const_get(:ScenarioSets)
scenario_set = scenario_sets.const_get(:FUZZ_SCENARIOS)
# rubocop:enable Sorbet/ConstantsFromStrings
scenarios = options.fetch(:scenarios) || scenario_set
unknown_scenarios = scenarios - scenario_set
unless unknown_scenarios.empty?
  warn "Unknown or non-fuzz scenario(s): #{unknown_scenarios.join(", ")}"
  warn "Allowed fuzz scenarios: #{scenario_set.join(", ")}"
  exit 2
end

seeds = options.fetch(:seeds).to_a
summary = {
  generated_at: Time.now.utc.iso8601,
  command: "scripts/dst-coverage.rb #{original_argv.join(" ")}".strip,
  coverage_dir: options.fetch(:coverage_dir),
  scenarios: scenarios,
  seeds: {
    first: seeds.first,
    last: seeds.last,
    count: seeds.length,
  },
  runs: [],
}

total = scenarios.length * seeds.length
completed = 0
started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

scenarios.each do |scenario|
  seeds.each do |seed|
    result = deterministic.public_send(:prove, scenario, seed:)
    completed += 1
    summary.fetch(:runs) << {
      scenario:,
      seed:,
      digest: result.digest,
      violations: result.violations,
      summary: result.summary,
    }

    if result.violations.any?
      warn "DST scenario #{scenario.inspect} seed #{seed} had invariant violations:"
      result.violations.each { |violation| warn "  - #{violation}" }
    end

    puts format("[%<completed>d/%<total>d] %<scenario>s seed=%<seed>d violations=%<violations>d", completed:, total:, scenario:, seed:, violations: result.violations.length)
  end
end

summary[:duration_seconds] = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at).round(3)
summary[:violating_runs] = summary.fetch(:runs).count { |run| run.fetch(:violations).any? }

FileUtils.mkdir_p(options.fetch(:coverage_dir))
summary_path = File.join(options.fetch(:coverage_dir), "dst-summary.json")
File.write(summary_path, JSON.pretty_generate(summary) + "\n")
puts "DST coverage summary written to #{summary_path}"

exit(summary.fetch(:violating_runs).zero? ? 0 : 1)
