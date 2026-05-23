# typed: true
# frozen_string_literal: true

module Durababble
  WaitRequest = Data.define(:kind, :wake_at, :event_key, :context)
end
