# typed: false
# frozen_string_literal: true

require_relative "../lib/durababble"

database_url = Durababble.default_database_url
store = Durababble::Store.connect(database_url:)
engine = Durababble::Engine.new(store:)

class AsyncPriceBasketWorkflow < Durababble::Workflow
  workflow_name "async_price_basket"

  def execute(input)
    futures = input.fetch("items").map { |item| async(:quote_item, item) }
    quotes = await_all(futures)

    {
      "quotes" => quotes,
      "total" => quotes.sum { |quote| quote.fetch("price_cents") },
    }
  end

  step retry: { maximum_attempts: 3 }
  def quote_item(item)
    prices = { "book" => 1_499, "mug" => 899, "pen" => 199 }

    {
      "sku" => item,
      "price_cents" => prices.fetch(item),
      "idempotency_key" => step_context.idempotency_key,
    }
  end
end

run = engine.run(AsyncPriceBasketWorkflow, input: { "items" => ["book", "mug", "pen"] })
puts "#{run.id} #{run.status} #{run.result.inspect}"
