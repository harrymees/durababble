# typed: false
# frozen_string_literal: true

require "active_support/isolated_execution_state"
require_relative "../lib/durababble"

# Durababble requires :fiber isolation so each reactor fiber checks out its own
# ActiveRecord connection. In a Rails+Falcon host the Falcon Railtie sets this
# defensively; standalone scripts like this one set it explicitly.
ActiveSupport::IsolatedExecutionState.isolation_level = :fiber

database_url = Durababble.default_database_url
store = Durababble::Store.connect(database_url:)
engine = Durababble::Engine.new(store:)

class CounterWorkflow < Durababble::Workflow
  workflow_name "counter"

  def execute(input)
    double(increment(input))
  end

  step def increment(input)
    { "count" => input.fetch("count") + 1 }
  end

  step def double(input)
    { "count" => input.fetch("count") * 2 }
  end
end

run = engine.run(CounterWorkflow, input: { "count" => 2 })
puts "#{run.id} #{run.status} #{run.result.inspect}"
