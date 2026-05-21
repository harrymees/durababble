# frozen_string_literal: true

module Durababble
  Step = Data.define(:name, :handler, :retry_policy) do
    def call(context, heartbeat = nil)
      handler.call(context, heartbeat)
    end
  end

  class Workflow
    attr_reader :name, :steps

    def self.define(name, &block)
      workflow = new(name)
      workflow.instance_eval(&block) if block
      workflow
    end

    def initialize(name)
      @name = String(name)
      @steps = []
    end

    def step(name, retry_policy: nil, &block)
      raise ArgumentError, "step requires a block" unless block

      @steps << Step.new(name: String(name), handler: block, retry_policy: RetryPolicy.from(retry_policy))
    end
  end
end
