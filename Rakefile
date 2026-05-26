# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

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

task lint: [:rubocop, :typecheck]

Rake::TestTask.new(:test) do |task|
  task.libs << "test"
  task.pattern = "test/**/*_test.rb"
end

namespace :test do
  desc "Run the full test suite with SimpleCov line and branch coverage gates"
  task :coverage do
    ENV["DURABABBLE_COVERAGE"] = "1"
    Rake::Task[:test].invoke
  end

  desc "DST mutation check: revert each known crash-recovery fix and confirm a scenario goes red"
  task :mutation do
    ruby("-Ilib -Itest test/durababble/dst_mutation_test.rb")
  end
end

task default: :test
