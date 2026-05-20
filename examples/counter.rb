# frozen_string_literal: true

require_relative "../lib/durababble"

database_url = ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte")
store = Durababble::Store.connect(database_url:, schema: "durababble_example")
engine = Durababble::Engine.new(store:)

workflow = Durababble::Workflow.define("counter") do
  step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
  step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
end

run = engine.run(workflow, input: { "count" => 2 })
puts "#{run.id} #{run.status} #{run.result.inspect}"
