# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleErrorFormattingTest < DurababbleTestCase
  test "formats errors without backtraces as a single message" do
    error = RuntimeError.new("boom")
    assert_equal("RuntimeError: boom", Durababble::ErrorFormatting.format_error(error))

    error.set_backtrace([])
    assert_equal("RuntimeError: boom", Durababble::ErrorFormatting.format_error(error))
  end
end
