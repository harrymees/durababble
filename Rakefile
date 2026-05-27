# frozen_string_literal: true

require "bundler/gem_tasks"
require "rbconfig"
require "rake/testtask"

desc "Format Markdown files with Prettier"
task "format:markdown" do
  sh("pnpm run format:markdown")
end

desc "Check Markdown files with Prettier"
task "check:markdown" do
  sh("pnpm run check:markdown")
end

task :rubocop do
  sh("bundle exec rubocop")
end

task :rbs do
  sh("bundle exec rbs validate")
end

task :rbs_strict do
  ruby("scripts/validate_rbs_strict.rb")
end

task :typecheck do
  Rake::Task[:rbs].invoke
  Rake::Task[:rbs_strict].invoke
  sh("bundle exec srb tc")
end

task :alloy do
  sh("scripts/verify-alloy.sh")
end

# Sigil drift is checked by `test/durababble/formal_sigil_drift_test.rb` which
# runs on every PR with the rest of the fast `test` suite. The `formal` task
# only triggers the slow Alloy verifier; no separate sigil step is needed.
task formal: [:alloy]

task lint: [:rubocop, :typecheck, "check:markdown"]

# The Deterministic Simulation Testing files. They run uninstrumented in their
# own CI job (`rake dst`) rather than under the coverage gate: the seed sweeps
# iterate the same lib/ lines thousands of times, adding ~no incremental
# coverage but ~2.5x wall time under SimpleCov. They exercise only the in-memory
# SQLite store, so the job needs no database service.
DST_TEST_FILES = [
  "test/durababble/deterministic_test.rb",
  "test/durababble/dst_mutation_test.rb",
].freeze

# The coverage-gated suite: every test EXCEPT the DST simulation files.
Rake::TestTask.new(:test) do |task|
  task.libs << "test"
  task.test_files = FileList["test/**/*_test.rb"].exclude(*DST_TEST_FILES)
end

desc "Run the Deterministic Simulation Testing suite (uninstrumented; full seed sweep)"
Rake::TestTask.new(:dst) do |task|
  task.libs << "test"
  task.test_files = FileList[*DST_TEST_FILES]
end

namespace :test do
  desc "Run the (non-DST) test suite with SimpleCov line and branch coverage gates"
  task :coverage do
    ENV["DURABABBLE_COVERAGE"] = "1"
    Rake::Task[:test].invoke
  end

  desc "DST mutation check: revert each known crash-recovery fix and confirm a scenario goes red"
  task :mutation do
    ruby("-Ilib -Itest test/durababble/dst_mutation_test.rb")
  end
end

task default: [:test, :dst]
