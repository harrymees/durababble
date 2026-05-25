# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWaitRequestTest < DurababbleTestCase
  test "wait_until builds a timer wait request without mutating the supplied context" do
    wake_at = Time.utc(2026, 1, 2, 3, 4, 5)
    context = { "request_id" => "r1" }

    wait = Durababble.wait_until(wake_at, context)

    assert_equal "timer", wait.kind
    assert_equal wake_at, wait.wake_at
    assert_nil wait.event_key
    assert_equal context, wait.context
    assert_equal({ "request_id" => "r1" }, context)
  end
end
