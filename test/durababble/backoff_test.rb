# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleBackoffTest < DurababbleTestCase
  test "jittered never returns less than the base delay" do
    1000.times do
      delay = Durababble::Backoff.jittered(2.0)
      assert_operator delay, :>=, 2.0
    end
  end

  test "jittered stays within the base plus the jitter fraction" do
    1000.times do
      delay = Durababble::Backoff.jittered(2.0, jitter: 0.25)
      assert_operator delay, :<=, 2.0 * 1.25
    end
  end

  test "jittered actually varies so synchronized callers decorrelate" do
    samples = Array.new(50) { Durababble::Backoff.jittered(1.0) }
    assert_operator samples.uniq.length, :>, 1
  end

  test "linear grows the base delay with the attempt number" do
    early = Durababble::Backoff.linear(1, step: 0.01, jitter: 0.0)
    late = Durababble::Backoff.linear(5, step: 0.01, jitter: 0.0)
    assert_in_delta 0.01, early, 1e-9
    assert_in_delta 0.05, late, 1e-9
  end

  test "linear caps the pre-jitter delay at max" do
    1000.times do
      delay = Durababble::Backoff.linear(100, step: 0.01, max: 0.05, jitter: 0.25)
      assert_operator delay, :>=, 0.05
      assert_operator delay, :<=, 0.05 * 1.25
    end
  end
end
