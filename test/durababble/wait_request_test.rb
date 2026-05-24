# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWaitRequestTest < DurababbleTestCase
  test "wait_until rejects non-workflow callers" do
    wake_at = Time.utc(2026, 1, 2, 3, 4, 5)
    context = { "request_id" => "r1" }

    error = assert_raises(Durababble::Error) { Durababble.wait_until(wake_at, context) }

    assert_match(/wait_until must be called from workflow execution/, error.message)
    assert_equal({ "request_id" => "r1" }, context)
  end

  test "wait_event rejects non-workflow callers" do
    context = { "request_id" => "r1", "attempt" => 2 }

    error = assert_raises(Durababble::Error) { Durababble.wait_event("approval:r1", context) }

    assert_match(/wait_event must be called from workflow execution/, error.message)
    assert_equal({ "request_id" => "r1", "attempt" => 2 }, context)
  end
end
