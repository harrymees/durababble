# typed: strict
# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
$LOAD_PATH.unshift(__dir__)

require_relative "support/coverage"

require "minitest/autorun"
require "mocha/minitest"
require "securerandom"
require "active_support/isolated_execution_state"
require "durababble"
require "durababble/store/sqlite"

# Durababble requires :fiber isolation so each reactor fiber checks out its own AR
# connection (see Durababble.assert_fiber_isolation!). In a Rails+Falcon host the Falcon
# Railtie sets this defensively; in our standalone test suite we set it here to match.
ActiveSupport::IsolatedExecutionState.isolation_level = :fiber

require_relative "support/test_workflow_helper"
require_relative "support/store_backends"
require_relative "support/fake_store_command_claiming"

module DurababbleMinitestHelper
  #: (untyped, String, ?migrate: bool) { (untyped) -> untyped } -> untyped
  def with_durababble_store(backend, schema_suffix, migrate: true, &block)
    @durababble_backend = backend #: untyped
    @durababble_schema = "#{backend.default_schema_prefix}_#{schema_suffix}_#{Process.pid}_#{SecureRandom.hex(4)}" #: String?
    @durababble_store = if backend.sqlite?
      Durababble::SqliteStore.build_in_memory(schema: @durababble_schema)
    else
      Durababble::Store.connect(database_url: backend.database_url, schema: @durababble_schema)
    end #: untyped
    @durababble_store.migrate! if migrate

    block.call(@durababble_store)
  ensure
    drop_durababble_test_schema
    @durababble_store&.close
    @durababble_store = nil #: untyped
    @durababble_schema = nil #: String?
    @durababble_backend = nil #: untyped
  end

  #: () -> void
  def drop_durababble_test_schema
    attempts = 0
    begin
      @durababble_store&.drop_schema!
    rescue ActiveRecord::Deadlocked, ActiveRecord::SerializationFailure
      attempts += 1
      raise if attempts >= 10

      sleep(0.05 * attempts)
      retry
    end
  end

  #: (Hash[untyped, untyped], Hash[untyped, untyped]) -> void
  def assert_hash_includes(actual, expected)
    test_case = self #: as untyped
    expected.each do |key, expected_value|
      test_case.assert(actual.key?(key), "expected #{actual.inspect} to include key #{key.inspect}")
      if expected_value.nil?
        test_case.assert_nil(actual.fetch(key))
      else
        test_case.assert_equal(expected_value, actual.fetch(key))
      end
    end
  end

  #: (T::Class[untyped], ?Regexp?) { -> untyped } -> untyped
  def assert_raises_matching(error_class, pattern = nil, &block)
    test_case = self #: as untyped
    error = test_case.assert_raises(error_class, &block)
    test_case.assert_match(pattern, error.message) if pattern
    error
  end

  #: (untyped, Symbol, ?String?) -> void
  def assert_not_respond_to(object, method_name, message = nil)
    test_case = self #: as untyped
    test_case.refute_respond_to(object, method_name, message)
  end

  #: () -> untyped
  def store
    @durababble_store || Kernel.raise("Durababble store has not been configured for this test")
  end

  #: (untyped, Class, String, ?worker_id: String) -> untyped
  def resume_waiting_workflow(store, workflow, workflow_id, worker_id: "timer-resume")
    row = store.workflow(workflow_id)
    run_at = row["next_run_at"]
    make_workflow_timer_due(store, workflow_id, at: run_at) if run_at
    with_store_current_time(store, run_at) do
      Durababble::Engine.new(store:, worker_id:).resume(workflow, workflow_id:)
    end
  end

  #: (untyped, String, at: Object?) -> void
  def make_workflow_timer_due(store, workflow_id, at:)
    store.make_workflow_due!(workflow_id, now: Time.now - 60)
  end

  #: (untyped, Object?) { -> untyped } -> untyped
  def with_store_current_time(store, now)
    return yield unless now

    original_current_time = store.method(:current_time)
    store.define_singleton_method(:current_time) { now }
    yield
  ensure
    store.define_singleton_method(:current_time) { original_current_time.call } if original_current_time
  end

  #: () -> String
  def schema
    @durababble_schema || Kernel.raise("Durababble schema has not been configured for this test")
  end

  #: () -> untyped
  def backend_descriptor
    @durababble_backend || Kernel.raise("Durababble backend has not been configured for this test")
  end
end

class DurababbleTestCase < Minitest::Test
  include DurababbleTestWorkflowHelper
  include DurababbleMinitestHelper

  class << self
    #: (String) { -> untyped } -> void
    def test(name, &block)
      define_method("test_#{name.gsub(/[^a-zA-Z0-9]+/, "_")}", &block)
    end
  end
end
