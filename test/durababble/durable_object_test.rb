# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleDurableObjectTest < DurababbleTestCase
  AccountState = Data.define(:balance_cents) do
    def credit(amount)
      with(balance_cents: balance_cents + amount)
    end

    def debit(amount)
      with(balance_cents: balance_cents - amount)
    end
  end

  class ApiTestAccount < Durababble::DurableObject
    def initialize_state
      AccountState.new(balance_cents: 0)
    end

    expose_command retry: { maximum_attempts: 2 }
    def credit(amount_cents)
      update_state(current_state.credit(amount_cents))
    end

    expose_command def debit(amount_cents)
      raise ArgumentError, "insufficient funds" if current_state.balance_cents < amount_cents

      update_state(current_state.debit(amount_cents))
    end

    expose def balance
      current_state.balance_cents
    end
  end

  class ClaimlessTestStore
    attr_reader :state, :completed, :failed

    def migrate! = true
    def enqueue_object_command(**) = "cmd-1"
    def claim_object_command(**) = nil
    def object_state(**) = state
    def save_object_state(state:, **) = @state = state
    def complete_object_command(command_id:, result:, **) = @completed = [command_id, result]
    def fail_object_command(command_id:, error:, **) = @failed = [command_id, error]
  end

  class ClaimlessTestObject < Durababble::DurableObject
    expose_command def mutate
      update_state({ "mutated" => true })
    end
  end

  class ClaimlessMailboxStore < ClaimlessTestStore
    def claim_next_object_command(**) = nil
  end

  class BranchCommandStore
    attr_reader :completed, :failed

    def initialize(complete_result: ActiveRecord::Result.empty(affected_rows: 1))
      @complete_result = complete_result
      @completed = []
      @failed = []
    end

    def migrate! = self
    def object_state(object_type:, object_id:) = { "value" => 1 }
    def enqueue_object_command(object_type:, object_id:, method_name:, args:, kwargs:) = "cmd-1"
    def claim_object_command(command_id:, worker_id:) = { "id" => command_id }

    def complete_object_command(command_id:, result:, **_kwargs)
      @completed << [command_id, result]
      @complete_result
    end

    def fail_object_command(command_id:, error:, worker_id:)
      @failed << [command_id, error, worker_id]
    end
  end

  class CleanCommandObject < Durababble::DurableObject
    expose_command def read_only
      "unchanged"
    end
  end

  class RetryStateTestCounter < Durababble::DurableObject
    def initialize_state
      { "count" => 0 }
    end

    expose_command retry: { maximum_attempts: 2, schedule: [0] }
    def increment_with_transient_failure
      update_state({ "count" => current_state.fetch("count") + 1 })
      raise "transient after state update" if command_context.attempt_number == 1

      current_state
    end

    expose_command def increment_and_fail
      update_state({ "count" => current_state.fetch("count") + 1 })
      raise "permanent after state update"
    end

    expose def count
      current_state.fetch("count")
    end
  end

  class SleepyTestObject < Durababble::DurableObject
    object_type "sleepy_test_object"

    def initialize_state
      { "events" => [] }
    end

    expose_command def schedule(label, wake_at)
      update_state({ "events" => current_state.fetch("events") + [["schedule", label]] })
      sleep_until(at: wake_at, payload: { "label" => label })
      current_state
    end

    expose_command def cancel(label)
      update_state({ "events" => current_state.fetch("events") + [["cancel", label]] })
      cancel_sleep
      current_state
    end

    expose_command def touch(label)
      update_state({ "events" => current_state.fetch("events") + [["touch", label]] })
      current_state
    end

    def on_wake(payload: nil)
      update_state({ "events" => current_state.fetch("events") + [["wake", payload.fetch("label")]] })
      current_state
    end

    expose def events
      current_state.fetch("events")
    end
  end

  class SleepOnlyTestObject < Durababble::DurableObject
    object_type "sleep_only_test_object"

    expose_command def schedule(wake_at)
      sleep_until(at: wake_at, payload: { "kind" => "only" })
      "scheduled"
    end
  end

  test "does not execute a durable object command when its lease cannot be claimed" do
    store = ClaimlessTestStore.new

    assert_raises_matching(Durababble::LeaseConflict, /could not claim durable object command/) do
      ClaimlessTestObject.ref("object-1", store:).mutate
    end
    assert_nil store.state
    assert_nil store.completed
    assert_nil store.failed
  end

  test "does not execute a durable object command when the mailbox head cannot be claimed" do
    store = ClaimlessMailboxStore.new

    assert_raises_matching(Durababble::LeaseConflict, /could not claim durable object command/) do
      ClaimlessTestObject.ref("object-1", store:).mutate
    end
    assert_nil store.state
    assert_nil store.completed
    assert_nil store.failed
  end

  test "derives fallback object types, ignores unknown macros, and saves state through the store" do
    anonymous_object = Class.new(Durababble::DurableObject)
    assert_match(/\A\d+\z/, anonymous_object.object_type)

    odd_object = Class.new(Durababble::DurableObject)
    odd_object.instance_variable_set(:@pending_durable_macro, [:unknown, {}])
    odd_object.class_eval { def ignored_macro = true }

    save_store = Object.new
    saved = []
    save_store.define_singleton_method(:save_object_state) { |**kwargs| saved << kwargs }
    object = anonymous_object.new(durable_id: "obj-1", store: save_store)
    object.update_state({ "saved" => true })
    assert_equal [{ object_type: anonymous_object.object_type, object_id: "obj-1", state: { "saved" => true } }], saved
  end

  test "completes read-only commands and fails them when the completion lease is lost" do
    clean_command_store = BranchCommandStore.new
    clean_ref = CleanCommandObject.ref("clean", store: clean_command_store)
    assert_equal "unchanged", clean_ref.read_only
    assert_equal 1, clean_command_store.completed.length

    lost_lease_store = BranchCommandStore.new(complete_result: ActiveRecord::Result.empty(affected_rows: 0))
    assert_raises(Durababble::LeaseConflict) { CleanCommandObject.ref("lost", store: lost_lease_store).read_only }
    assert_equal 1, lost_lease_store.failed.length
  end

  durababble_store_backends.each do |backend|
    test "exposes commands and queries without step semantics with #{backend.name}" do
      with_durababble_store(backend, "durable_object_api") do |store|
        account = ApiTestAccount.ref("acct-1", store:)

        assert_not_respond_to ApiTestAccount, :step
        assert_equal 0, account.balance
        assert_equal 500, account.credit(500).balance_cents
        assert_equal 375, account.debit(125).balance_cents
        assert_equal 375, account.balance
      end
    end

    test "persists durable object state atomically with command completion with #{backend.name}" do
      with_durababble_store(backend, "durable_object_api") do |store|
        counter = RetryStateTestCounter.ref("counter-1", store:)

        assert_equal({ "count" => 1 }, counter.increment_with_transient_failure)
        assert_equal 1, counter.count

        assert_raises_matching(RuntimeError, /permanent after state update/) do
          counter.increment_and_fail
        end
        assert_equal 1, counter.count
      end
    end

    test "keeps a command idempotency key stable across retry with #{backend.name}" do
      with_durababble_store(backend, "durable_object_api") do |store|
        seen_keys = []
        retrying_object = Class.new(Durababble::DurableObject) do
          object_type "retrying_object"

          def initialize_state
            { "committed_attempts" => 0 }
          end

          define_method(:write_with_retry) do |value|
            seen_keys << command_context.idempotency_key
            update_state({
              "committed_attempts" => current_state.fetch("committed_attempts") + 1,
              "value" => value,
            })
            raise "transient command failure" if command_context.attempt_number == 1

            current_state
          end
          expose_command :write_with_retry, retry: { maximum_attempts: 2, schedule: [0] }

          expose def snapshot
            current_state
          end
        end
        object = retrying_object.ref("object-1", store:)

        result = object.write_with_retry("persisted")

        assert_equal({ "committed_attempts" => 1, "value" => "persisted" }, result)
        assert_equal 2, seen_keys.length
        assert_equal 1, seen_keys.uniq.length
        assert_equal result, object.snapshot
      end
    end

    test "replaces a pending durable object sleep and delivers only the latest wake with #{backend.name}" do
      with_durababble_store(backend, "durable_object_sleep") do |store|
        store.migrate!
        object = SleepyTestObject.ref("object-1", store:)
        first_wake = Time.utc(2026, 5, 24, 21, 0, 0)
        second_wake = Time.utc(2026, 5, 24, 21, 5, 0)

        object.schedule("first", first_wake)
        object.schedule("second", second_wake)

        sleep_row = store.object_sleep(object_type: SleepyTestObject.object_type, object_id: "object-1")
        assert_equal second_wake.to_i, Time.parse(sleep_row.fetch("wake_at").to_s).to_i
        assert_equal({ "label" => "second" }, sleep_row.fetch("payload"))

        assert_equal 1, store.wake_due_object_sleeps(now: second_wake + 1)
        assert_nil store.object_sleep(object_type: SleepyTestObject.object_type, object_id: "object-1")
        assert_equal 0, store.wake_due_object_sleeps(now: second_wake + 1)

        object.touch("after")

        assert_equal [
          ["schedule", "first"],
          ["schedule", "second"],
          ["wake", "second"],
          ["touch", "after"],
        ],
          object.events
        wake_rows = store.object_commands_for(object_type: SleepyTestObject.object_type, object_id: "object-1").select { |row| row.fetch("method_name") == Durababble::DurableObjectRef::WAKE_METHOD_NAME }
        assert_equal 1, wake_rows.length
        assert_equal "completed", wake_rows.first.fetch("status")
      end
    end

    test "cancels a pending durable object sleep in the command completion transaction with #{backend.name}" do
      with_durababble_store(backend, "durable_object_sleep") do |store|
        store.migrate!
        object = SleepyTestObject.ref("object-2", store:)
        wake_at = Time.utc(2026, 5, 24, 22, 0, 0)

        object.schedule("canceled", wake_at)
        object.cancel("canceled")

        assert_nil store.object_sleep(object_type: SleepyTestObject.object_type, object_id: "object-2")
        assert_equal 0, store.wake_due_object_sleeps(now: wake_at + 1)
        object.touch("after")

        assert_equal [
          ["schedule", "canceled"],
          ["cancel", "canceled"],
          ["touch", "after"],
        ],
          object.events
      end
    end

    test "persists a sleep-only command without object state changes with #{backend.name}" do
      with_durababble_store(backend, "durable_object_sleep") do |store|
        store.migrate!
        object = SleepOnlyTestObject.ref("object-5", store:)
        wake_at = Time.utc(2026, 5, 25, 1, 0, 0)

        assert_equal "scheduled", object.schedule(wake_at)
        assert_nil store.object_state(object_type: SleepOnlyTestObject.object_type, object_id: "object-5")
        sleep_row = store.object_sleep(object_type: SleepOnlyTestObject.object_type, object_id: "object-5")
        assert_equal({ "kind" => "only" }, sleep_row.fetch("payload"))
      end
    end

    test "orders wakes with earlier and later mailbox commands with #{backend.name}" do
      with_durababble_store(backend, "durable_object_sleep") do |store|
        store.migrate!
        object = SleepyTestObject.ref("object-3", store:)
        wake_at = Time.utc(2026, 5, 24, 23, 0, 0)

        object.schedule("wake", wake_at)
        store.enqueue_object_command(
          object_type: SleepyTestObject.object_type,
          object_id: "object-3",
          method_name: "touch",
          args: ["before"],
          kwargs: {},
        )
        assert_equal 1, store.wake_due_object_sleeps(now: wake_at + 1)

        object.touch("after")

        assert_equal [
          ["schedule", "wake"],
          ["touch", "before"],
          ["wake", "wake"],
          ["touch", "after"],
        ],
          object.events
      end
    end

    test "reclaims an expired wake lease without losing or duplicating the wake with #{backend.name}" do
      with_durababble_store(backend, "durable_object_sleep") do |store|
        store.migrate!
        object = SleepyTestObject.ref("object-4", store:)
        wake_at = Time.utc(2026, 5, 25, 0, 0, 0)

        object.schedule("takeover", wake_at)
        assert_equal 1, store.wake_due_object_sleeps(now: wake_at + 1)
        claimed = store.claim_next_object_command(
          object_type: SleepyTestObject.object_type,
          object_id: "object-4",
          worker_id: "crashed-worker",
          lease_seconds: 0,
        )
        assert_equal Durababble::DurableObjectRef::WAKE_METHOD_NAME, claimed.fetch("method_name")
        sleep 0.02

        object.touch("after")

        assert_equal [
          ["schedule", "takeover"],
          ["wake", "takeover"],
          ["touch", "after"],
        ],
          object.events
        wake_rows = store.object_commands_for(object_type: SleepyTestObject.object_type, object_id: "object-4").select { |row| row.fetch("method_name") == Durababble::DurableObjectRef::WAKE_METHOD_NAME }
        assert_equal 1, wake_rows.length
        assert_equal "completed", wake_rows.first.fetch("status")
      end
    end
  end
end
