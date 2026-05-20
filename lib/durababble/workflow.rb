# frozen_string_literal: true

module Durababble
  Step = Data.define(:name, :handler) do
    def call(context)
      handler.call(context)
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

    def step(name, &block)
      raise ArgumentError, "step requires a block" unless block

      @steps << Step.new(name: String(name), handler: block)
    end
  end
end
