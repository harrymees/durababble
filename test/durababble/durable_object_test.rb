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

  test "does not execute a durable object command when its lease cannot be claimed" do
    store = ClaimlessTestStore.new

    assert_raises_matching(Durababble::LeaseConflict, /could not claim durable object command/) do
      # [DURABABBLE-OBJ-1] Commands do not run unless their durable command lease is claimed.
      ClaimlessTestObject.ref("object-1", store:).mutate
    end
    assert_nil store.state
    assert_nil store.completed
    assert_nil store.failed
  end

  durababble_store_backends.each do |backend|
    test "exposes commands and queries without step semantics with #{backend.name}" do
      with_durababble_store(backend, "durable_object_api") do |store|
        store.migrate!
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
        store.migrate!
        counter = RetryStateTestCounter.ref("counter-1", store:)

        # [DURABABBLE-OBJ-1] Command completion persists state with the command lifecycle.
        assert_equal({ "count" => 1 }, counter.increment_with_transient_failure)
        assert_equal 1, counter.count

        assert_raises_matching(RuntimeError, /permanent after state update/) do
          counter.increment_and_fail
        end
        assert_equal 1, counter.count
      end
    end
  end
end
