# frozen_string_literal: true

require "spec_helper"

RSpec.describe Durababble do
  describe ".wait_until" do
    it "builds a timer wait request without mutating the supplied context" do
      wake_at = Time.utc(2026, 1, 2, 3, 4, 5)
      context = { "request_id" => "r1" }

      wait = described_class.wait_until(wake_at, context)

      expect(wait).to have_attributes(
        kind: "timer",
        wake_at:,
        event_key: nil,
        context:
      )
      expect(context).to eq({ "request_id" => "r1" })
    end
  end

  describe ".wait_event" do
    it "builds an event wait request with the exact event key and context" do
      context = { "request_id" => "r1", "attempt" => 2 }

      wait = described_class.wait_event("approval:r1", context)

      expect(wait).to have_attributes(
        kind: "event",
        wake_at: nil,
        event_key: "approval:r1",
        context:
      )
    end
  end
end
