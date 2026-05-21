# frozen_string_literal: true

require_relative "durababble/version"
require_relative "durababble/workflow"
require_relative "durababble/wait_request"
require_relative "durababble/store"
require_relative "durababble/engine"
require_relative "durababble/run"
require_relative "durababble/worker"

module Durababble
  class Error < StandardError; end
  class InjectedCrash < Error; end
  class LeaseConflict < Error; end
  class FenceTimeout < Error; end

  def self.wait_until(time, context = {})
    WaitRequest.new(kind: "timer", wake_at: time, event_key: nil, context:)
  end

  def self.wait_event(event_key, context = {})
    WaitRequest.new(kind: "event", wake_at: nil, event_key:, context:)
  end
end
