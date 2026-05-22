# frozen_string_literal: true

require "simplecov"

SimpleCov.start do
  enable_coverage :branch
  add_filter "/spec/"
  add_filter "/pkg/"
  minimum_coverage line: 85, branch: 60
end

require "durababble"
require_relative "support/store_backends"

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end

def durababble_test_workflow(name, &definition)
  Class.new(Durababble::Workflow) do
    workflow_name name

    def execute(input)
      self.class.step_order.reduce(input) { |ctx, method_name| public_send(method_name, ctx) }
    end

    def self.test_step(name, retry_policy: nil, &block)
      define_method(name) do |ctx|
        if block.arity >= 2
          block.call(ctx, step_context.heartbeat)
        else
          block.call(ctx)
        end
      end
      step name, retry: retry_policy
    end

    class_eval(&definition) if definition
  end
end
