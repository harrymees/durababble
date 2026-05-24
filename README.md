# Durababble

Durababble is durable executor for workflows and durable objects. It is for work that might run for a long time and must survive process exits, retries, deploys, and other changes in which process is actually running the code. It adds durability by storing state in your existing database.

The library gives you two primitives:

| Primitive         | Use it for                                                                       | Current API                                                                                                               |
| ----------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Durable workflows | One-off executions with durable steps, waits, retries, cancellation, and results | `Durababble::Workflow`, `Workflow.start`, `Workflow.handle`, `Durababble::Engine#run`, `Workflow.enqueue`, `Workflow.ref` |
| Durable objects   | Long-lived instances with durable state, like Cloudflare's Durable Objects       | `Durababble::DurableObject`, `DurableObject.ref`, `expose`, `expose_command`                                              |

Detailed guarantees live in [docs/spec.md](docs/spec.md) and [docs/architecture.md](docs/architecture.md).

## Why It Exists

Applications often need orchestration that supports more sophisticated patterns than background jobs or non-durable actors, but deploying and running a whole durable workflow system is operationally undesirable. Instead, Durababble reuses your existing, robust backend storage for the durability part, and then orchestrates everything using familiar looking background worker processes.

In this middle ground:

- workflow code is ordinary Ruby with explicit durable `step` boundaries;
- every durable boundary is persisted before and after execution;
- workers claim work with SQL leases and fence stale ownership;
- completed steps replay from storage instead of rerunning side effects;
- waits, fences, outbox rows, and durable-object commands are database state, not in-memory coordination;
- deterministic and crash-recovery tests exercise the failure model directly.

Durababble also has one important feature: cheap RPCs between durable entities. This lets you easily query and command your durable entities to do useful stuff! This makes them spiritually similar to actors in actor frameworks, or genservers in BEAM/OTP, but with added durable goodness.

## Quickstart API Examples

Workflow code is deterministic orchestration in `#execute`; side-effecting methods become durable boundaries with `step`. Completed steps replay from persisted results, and replay shape checks fail with `Durababble::NonDeterminismError` if code reaches a different completed step method or returns before consuming completed history. Cancellation is cooperative: `Workflow.handle(run_id).cancel(reason:)` durably records a request and the next deterministic yield point raises `Durababble::CancellationError` inside workflow code. User code can rescue it, run durable cleanup steps, and finish the run as `canceled`; cleanup failures are recorded as workflow failures.

<!-- README:workflow-example:start -->

```ruby
class FulfillOrder < Durababble::Workflow
  workflow_name "fulfill_order"

  expose def status
    "queryable"
  end

  def execute(order)
    payment = charge_card(order)
    label = buy_shipping_label(order, payment)

    { "payment_id" => payment.fetch("id"), "label_id" => label.fetch("id") }
  end

  step retry: { maximum_attempts: 5, schedule: [1, 5, 30] }
  def charge_card(order)
    Payments.charge(
      order.fetch("card_token"),
      amount: order.fetch("total_cents"),
      idempotency_key: step_context.idempotency_key,
    )
  end

  step def buy_shipping_label(order, payment)
    Shipping.buy_label(
      order.fetch("address"),
      payment_id: payment.fetch("id"),
      idempotency_key: step_context.idempotency_key,
    )
  end
end

module Payments
  def self.charge(card_token, amount:, idempotency_key:)
    { "id" => "pay_#{card_token}", "amount" => amount, "key" => idempotency_key }
  end
end

module Shipping
  def self.buy_label(address, payment_id:, idempotency_key:)
    { "id" => "label_#{payment_id}", "address" => address, "key" => idempotency_key }
  end
end

store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
order ||= {
  "card_token" => "card_123",
  "total_cents" => 5_000,
  "address" => { "postal_code" => "10001" },
}

run = Durababble::Engine.new(store:).run(FulfillOrder, input: order)
FulfillOrder.ref(run.id, store:).status
```

<!-- README:workflow-example:end -->

Durable objects are addressed by id. Queries declared with `expose` read the latest persisted state; commands declared with `expose_command` record durable command rows and persist state changes through `update_state`.

<!-- README:durable-object-example:start -->

```ruby
class Account < Durababble::DurableObject
  object_type "account"

  def initialize_state
    { "balance_cents" => 0 }
  end

  expose_command retry: { maximum_attempts: 5 }
  def credit(amount_cents)
    update_state(
      "balance_cents" => current_state.fetch("balance_cents") + amount_cents,
    )
  end

  expose def balance
    current_state.fetch("balance_cents")
  end
end

store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!

account = Account.ref("acct_readme", store:)
account.credit(1_000)
account.balance
```

<!-- README:durable-object-example:end -->
