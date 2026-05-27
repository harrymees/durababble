#!/usr/bin/env ruby
# typed: true
# frozen_string_literal: true

require "fileutils"
require "optparse"

require_relative "mysql-hot-path-report"

module DurababbleDocsQueryReports
  DEFAULT_OUTPUT_DIR = File.expand_path("../docs/static/query-perf", __dir__)

  class Generator
    def initialize(
      output_dir: DEFAULT_OUTPUT_DIR,
      database_url: DurababbleMysqlHotPathReport.default_database_url,
      fixture_size: Integer(ENV.fetch("DURABABBLE_QUERY_PERF_FIXTURE_SIZE", "0")),
      scenario_names: DurababbleMysqlHotPathReport.scenarios.keys.sort,
      runner: nil
    )
      @output_dir = output_dir
      @database_url = database_url
      @fixture_size = fixture_size
      @scenario_names = scenario_names
      @runner = runner
    end

    def run
      FileUtils.mkdir_p(@output_dir)
      Dir[File.join(@output_dir, "*.html")].each { |path| FileUtils.rm_f(path) }

      @scenario_names.each do |scenario_name|
        run_scenario(scenario_name)
      end
    end

    private

    def run_scenario(scenario_name)
      output = File.join(@output_dir, "#{scenario_name}.html")
      options = DurababbleMysqlHotPathReport.parse_options([
        "--scenario",
        scenario_name,
        "--database-url",
        @database_url,
        "--fixture-size",
        @fixture_size.to_s,
        "--format",
        "html",
        "--output",
        output,
      ])

      if @runner
        @runner.call(options)
      else
        DurababbleMysqlHotPathReport::Runner.new(options).run
      end
    end
  end

  class << self
    def parse_options(argv)
      options = {
        output_dir: DEFAULT_OUTPUT_DIR,
        database_url: DurababbleMysqlHotPathReport.default_database_url,
        fixture_size: Integer(ENV.fetch("DURABABBLE_QUERY_PERF_FIXTURE_SIZE", "0")),
      }

      OptionParser.new do |opts|
        opts.banner = "Usage: #{$PROGRAM_NAME} [options]"
        opts.on("--output-dir DIR", "Directory for generated docs query reports") { |value| options[:output_dir] = value }
        opts.on("--database-url URL", "MySQL URL for report generation") { |value| options[:database_url] = value }
        opts.on("--fixture-size N", Integer, "Seed N unrelated workflows before tracing each scenario") { |value| options[:fixture_size] = value }
      end.parse!(argv)

      raise ArgumentError, "--fixture-size must be non-negative" if options.fetch(:fixture_size).negative?

      options
    end
  end
end

if $PROGRAM_NAME == __FILE__
  options = DurababbleDocsQueryReports.parse_options(ARGV)
  DurababbleDocsQueryReports::Generator.new(**options).run
end
