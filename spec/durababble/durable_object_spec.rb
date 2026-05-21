# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe Durababble::DurableObject, :integration do
  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_durable_object_api_#{Process.pid}_#{SecureRandom.hex(4)}" }
  let(:store) { Durababble::Store.connect(database_url:, schema:) }

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
end
