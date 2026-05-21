# frozen_string_literal: true

require_relative "durababble/version"

module Durababble
  class Error < StandardError; end
  class InjectedCrash < Error; end
  class LeaseConflict < Error; end
  class FenceTimeout < Error; end

  class << self
    attr_accessor :default_store

    def configure(database_url:, schema: "durababble")
      @default_store&.close
      @default_store = Store.connect(database_url:, schema:)
    end

    def store
      @default_store || raise(Error, "Durababble.store is not configured; pass store: or call Durababble.configure")
    end
  end
end

require_relative "durababble/retry_policy"
require_relative "durababble/workflow"
require_relative "durababble/durable_object"
require_relative "durababble/wait_request"
require_relative "durababble/store"
require_relative "durababble/engine"
require_relative "durababble/run"
require_relative "durababble/worker"
require_relative "durababble/worker_runtime"
require_relative "durababble/deterministic"

module Durababble
  def self.wait_until(time, context = {})
    WaitRequest.new(kind: "timer", wake_at: time, event_key: nil, context:)
  end

  def self.wait_event(event_key, context = {})
    WaitRequest.new(kind: "event", wake_at: nil, event_key:, context:)
  end
end

require_relative "durababble/rpc_client"
require_relative "durababble/workflow_rpc"
