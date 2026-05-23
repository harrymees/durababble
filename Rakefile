# frozen_string_literal: true

require "bundler/gem_tasks"
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
  sh("node scripts/validate-durababble-sigils.js")
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

task default: :test
