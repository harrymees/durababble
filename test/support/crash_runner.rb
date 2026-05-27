# typed: false
# frozen_string_literal: true

require "active_support/isolated_execution_state"
require "durababble"

# Match the test suite / production hosts — Durababble.assert_fiber_isolation! refuses
# to boot under the default :thread isolation.
ActiveSupport::IsolatedExecutionState.isolation_level = :fiber

store = Durababble::Store.connect(
  database_url: ENV.fetch("DURABABBLE_DATABASE_URL"),
  schema: ENV.fetch("DURABABBLE_SCHEMA"),
)
workflow_id = ENV.fetch("DURABABBLE_WORKFLOW_ID")
crash_after = ENV.fetch("DURABABBLE_CRASH_AFTER").to_sym

class CrashRunnerCounterWorkflow < Durababble::Workflow
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

Durababble::Engine.new(store:, worker_id: "crasher", crash_after:).resume(CrashRunnerCounterWorkflow, workflow_id:)
