# frozen_string_literal: true

require_relative "../lib/durababble"

database_url = ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte")
store = Durababble::Store.connect(database_url:, schema: "durababble_example")
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
