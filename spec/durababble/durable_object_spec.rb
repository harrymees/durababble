# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe Durababble::DurableObject, :integration do
  AccountState = Data.define(:balance_cents) do
    def credit(amount)
      with(balance_cents: balance_cents + amount)
    end

    def debit(amount)
      with(balance_cents: balance_cents - amount)
    end
  end unless const_defined?(:AccountState)

  class ApiSpecAccount < Durababble::DurableObject
    def initialize_state
      AccountState.new(balance_cents: 0)
    end

    expose_command retry: { maximum_attempts: 2 }
    def credit(amount_cents)
      update_state current_state.credit(amount_cents)
    end

    expose_command def debit(amount_cents)
      raise ArgumentError, "insufficient funds" if current_state.balance_cents < amount_cents

      update_state current_state.debit(amount_cents)
    end

    expose def balance
      current_state.balance_cents
    end
  end

  class ClaimlessSpecStore
    attr_reader :state, :completed, :failed

    def migrate! = true
    def enqueue_object_command(**) = "cmd-1"
    def claim_object_command(**) = nil
    def object_state(**) = state
    def save_object_state(state:, **) = @state = state
    def complete_object_command(command_id:, result:) = @completed = [command_id, result]
    def fail_object_command(command_id:, error:) = @failed = [command_id, error]
  end unless const_defined?(:ClaimlessSpecStore)

  class ClaimlessSpecObject < Durababble::DurableObject
    expose_command def mutate
      update_state({ "mutated" => true })
    end
  end unless const_defined?(:ClaimlessSpecObject)

  class RetryStateSpecCounter < Durababble::DurableObject
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
  end unless const_defined?(:RetryStateSpecCounter)

  it "does not execute a durable object command when its lease cannot be claimed" do
    store = ClaimlessSpecStore.new

    expect do
      ClaimlessSpecObject.ref("object-1", store:).mutate
    end.to raise_error(Durababble::LeaseConflict, /could not claim durable object command/)

    expect(store.state).to be_nil
    expect(store.completed).to be_nil
    expect(store.failed).to be_nil
  end

  durababble_store_backends.each do |backend|
    context "with #{backend.name}" do
      let(:schema) { "#{backend.default_schema_prefix}_durable_object_api_#{Process.pid}_#{SecureRandom.hex(4)}" }
      let(:store) { Durababble::Store.connect(database_url: backend.database_url, schema:) }

      after do
        store&.drop_schema!
        store&.close
      end

      it "exposes commands and queries without step semantics" do
        account = ApiSpecAccount.ref("acct-1", store:)

        expect(ApiSpecAccount).not_to respond_to(:step)
        expect(account.balance).to eq(0)
        expect(account.credit(500).balance_cents).to eq(500)
        expect(account.debit(125).balance_cents).to eq(375)
        expect(account.balance).to eq(375)
      end

      it "persists durable object state atomically with command completion" do
        counter = RetryStateSpecCounter.ref("counter-1", store:)

        expect(counter.increment_with_transient_failure).to eq("count" => 1)
        expect(counter.count).to eq(1)

        expect do
          counter.increment_and_fail
        end.to raise_error(RuntimeError, /permanent after state update/)
        expect(counter.count).to eq(1)
      end
    end
  end
end
