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

  class BranchCommandStore
    attr_reader :completed, :failed

    def initialize(complete_result: Durababble::MysqlStore::MysqlResult.new([], 1))
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

  test "does not execute a durable object command when its lease cannot be claimed" do
    store = ClaimlessTestStore.new

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

    lost_lease_store = BranchCommandStore.new(complete_result: Durababble::MysqlStore::MysqlResult.new([], 0))
    assert_raises(Durababble::LeaseConflict) { CleanCommandObject.ref("lost", store: lost_lease_store).read_only }
    assert_equal 1, lost_lease_store.failed.length
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
        store.migrate!
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
  end
end
