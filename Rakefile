# frozen_string_literal: true

require "bundler/gem_tasks"
require "rbconfig"
require "rake/testtask"

task :rubocop do
  sh("bundle exec rubocop")
end

task :rbs do
  sh("bundle exec rbs validate")
end

task :typecheck do
  sh("bundle exec srb tc")
end

task :sigils do
  sh(RbConfig.ruby, "scripts/validate-durababble-sigils.rb")
end

task :alloy do
  sh("scripts/verify-alloy.sh")
end

task formal: [:alloy, :sigils]

task lint: [:rubocop, :rbs, :typecheck]

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
end

task default: :test
