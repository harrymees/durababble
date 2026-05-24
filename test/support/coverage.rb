# typed: false
# frozen_string_literal: true

return unless ENV["DURABABBLE_COVERAGE"] == "1"

require "simplecov"

SimpleCov.start do
  enable_coverage(:branch)
  primary_coverage(:line)
  root(File.expand_path("../..", __dir__))
  track_files("lib/**/*.rb")

  add_filter("/test/")
  add_filter("/bench/")
  add_filter("/docs/")
  add_filter("/examples/")
  add_filter("/sig/")
  add_filter("/sorbet/")
  add_filter("lib/durababble/version.rb")

  add_group("Library", "lib")

  minimum_coverage(line: 90.0, branch: 85.0)
  minimum_coverage_by_file(line: 59, branch: 41)
end
