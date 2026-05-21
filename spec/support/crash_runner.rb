# frozen_string_literal: true

require "durababble"

store = Durababble::Store.connect(
  database_url: ENV.fetch("DURABABBLE_DATABASE_URL"),
  schema: ENV.fetch("DURABABBLE_SCHEMA")
)
workflow_id = ENV.fetch("DURABABBLE_WORKFLOW_ID")
crash_after = ENV.fetch("DURABABBLE_CRASH_AFTER").to_sym

workflow = Durababble::Workflow.define("counter") do
  step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
  step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
end

Durababble::Engine.new(store:, worker_id: "crasher", crash_after:).resume(workflow, workflow_id:)
