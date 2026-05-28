---
title: "Testing"
weight: 50
---

# Testing

Use two kinds of tests: plain Ruby tests for your logic, and database-backed worker tests for Durababble behavior. Worker tests use a real store, a fresh schema or table prefix, public handles, a local `Durababble::Worker`, and assertions through public reads.

The examples below assume `store` is a migrated Durababble test store. In real tests, clean up with `ensure` and call `store.drop_schema!` and `store.close`.

Prefer a fresh schema or table prefix per test process so tests do not see each other's rows. MySQL and MariaDB use the schema value as Durababble's table prefix; PostgreSQL and YugabyteDB use it as a SQL schema. Non-Rails tests that execute workflows should set `ActiveSupport::IsolatedExecutionState.isolation_level = :fiber` before running workers.

## Testing Objects

Object command bodies are ordinary Ruby methods. For simple state rules, instantiate the object directly. This is fast, but it does not exercise inbox ordering, retries, wakeups, leases, or persistence.

```ruby
class Cart < Durababble::DurableObject
  def initialize_state
    { "items" => [] }
  end

  expose_command def add_item(sku)
    update_state("items" => current_state.fetch("items") + [sku])
  end

  expose def items = current_state.fetch("items")
end

cart = Cart.new(durable_id: "cart-1", state: { "items" => [] })
cart.add_item("sku-1")
raise "bad state" unless cart.items == ["sku-1"]
```

Use a local object worker for the durable path. `tell` is the simplest same-thread shape: enqueue the command, drain the inbox, then read state through `at`.

```ruby
Cart.tell("cart-1", :add_item, "sku-1", store:)

worker = Durababble::Worker.new(
  store:,
  workflows: [],
  objects: [Cart],
  worker_id: "object-test-worker",
  migrate: false,
)
worker.run_until_idle

raise "bad persisted state" unless Cart.at("cart-1", store:).items == ["sku-1"]
```

Use a synchronous handle command such as `Cart.at("cart-1", store:).add_item("sku-1")` when a worker is already running and the caller needs the command result; otherwise the call waits for completion. For durable-object scheduled wakes, enqueue the command that calls `schedule_wake`, run the worker, call `store.wake_due_timers(now: due_time)`, then run the worker again.

For fuller examples, see `test/durababble/durable_object_test.rb`, `test/examples/chat_room_test.rb`, and `test/examples/agent_loop_test.rb`.

## Testing Workflows

Workflow `step` methods are durable boundaries, not ordinary public methods. During a workflow run, Durababble wraps each step call so it can record history, inject `step_context`, retry failures, and replay completed results. A direct call such as `FulfillOrder.new.charge_card(order)` raises because there is no workflow execution context.

Keep step methods thin and put the effect into application code you can call explicitly from unit tests:

```ruby
module FulfillmentSteps
  def self.charge_card(order, idempotency_key:)
    Payments.charge(
      order.fetch("card_token"),
      amount: order.fetch("total_cents"),
      idempotency_key:,
    )
  end
end

class FulfillOrder < Durababble::Workflow
  def execute(order)
    payment = charge_card(order)
    { "payment_id" => payment.fetch("id") }
  end

  step def charge_card(order)
    FulfillmentSteps.charge_card(order, idempotency_key: step_context.idempotency_key)
  end
end

payment = FulfillmentSteps.charge_card(
  { "card_token" => "card_123", "total_cents" => 5_000 },
  idempotency_key: "test-charge",
)
```

Use a local workflow worker for orchestration, replay, retries, waits, cancellation, handle RPCs, and final workflow state:

```ruby
order = { "card_token" => "card_123", "total_cents" => 5_000 }
handle = FulfillOrder.start(order, id: "fulfillment-order-123", store:)

worker = Durababble::Worker.new(
  store:,
  workflows: [FulfillOrder],
  worker_id: "workflow-test-worker",
  migrate: false,
)
worker.run_until_idle

raise "workflow did not complete" unless handle.status == "completed"
raise "unexpected result" unless handle.result == { "payment_id" => "pay_card_123" }
```

`run_until_idle` drains currently runnable work and then stops. If the workflow parks on a timer, advance the store/database clock or otherwise make the workflow row's `next_run_at` due, then run the worker again so the normal workflow claim path resumes it. If the workflow waits for a command, send it through the workflow handle and run the worker until the status or result changes.

For fuller examples, see `test/durababble/workflow_test.rb`, `test/durababble/workflow_wait_test.rb`, `test/durababble/workflow_cancellation_test.rb`, and the end-to-end tests under `test/examples/`.

Durababble's public testing helpers are still minimal: the reliable integration path is a real store plus a local worker. Helpers for isolated test stores, running workers to a condition, advancing timers, and asserting workflow/object history would be valuable. Contributions are welcome.
