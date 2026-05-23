# Durababble

Durababble is an incubating Ruby library for durable workflow orchestration, durable objects, and lease-routed RPCs.
This copy is vendored into `agent-server` as a local path gem while the API settles.

The app depends on it with:

```ruby
gem "durababble", path: "gems/durababble"
```

Bundler loads the library directly from this folder.
There is no publish step while it is incubating in the app.

## Layout

```text
gems/durababble/
  durababble.gemspec
  lib/
    durababble.rb
    durababble/version.rb
  test/
```

The gemspec owns runtime dependencies used by the app bundle.
`Durababble::Store.connect` selects the Trilogy-backed MySQL adapter by default, with PostgreSQL/YSQL available when a PostgreSQL URL is provided.

## Testing

The tests use the same Minitest stack as `agent-server`.
They live under `gems/durababble/test` and run directly from the gem without booting Rails.

Useful commands from the `agent-server` zone:

```sh
cd gems/durababble
shadowenv exec -- bundle exec ruby -I lib -I test test/run_all.rb
cd ../..
shadowenv exec -- bundle exec ruby -I gems/durababble/lib -I gems/durababble/test gems/durababble/test/run_all.rb
DURABABBLE_YUGABYTE_DATABASE_URL=postgresql://127.0.0.1:5433/durababble_test shadowenv exec -- bundle exec ruby -I gems/durababble/lib -I gems/durababble/test gems/durababble/test/run_all.rb
/opt/dev/bin/dev check
```

## Public API

Durababble exposes two complementary abstractions on the same durable store:

| Abstraction                 | Best for                                                                            | Mental model                     |
| --------------------------- | ----------------------------------------------------------------------------------- | -------------------------------- |
| `Durababble::Workflow`      | One-off processes: indexing pipelines, multi-step tool sequences, resumable work    | Function that survives restarts  |
| `Durababble::DurableObject` | Sessions, agent contexts, project state, anything addressed by id and mutable state | Addressed object with durability |

Workflow code is plain Ruby in `#execute`.
Methods declared with `step` are durable side-effect boundaries; replay returns persisted step results instead of rerunning completed work.

```ruby
class FulfillOrder < Durababble::Workflow
  workflow_name "fulfill_order"

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
      idempotency_key: step_context.idempotency_key,
    )
  end
end
```

Durable objects are identity-addressed state holders.
Exposed commands are the durable boundary for state changes.

```ruby
class Account < Durababble::DurableObject
  object_type "account"

  def initialize_state
    { "balance_cents" => 0 }
  end

  expose_command retry: { maximum_attempts: 5 }
  def credit(amount_cents)
    update_state("balance_cents" => current_state.fetch("balance_cents") + amount_cents)
  end

  expose def balance
    current_state.fetch("balance_cents")
  end
end

account = Account.ref("acct_123", store:)
account.credit(1_000)
account.balance
```

## Implemented Prototype Scope

- Class-oriented workflow API with `#execute`, `step def`, retry policy, step idempotency keys, and class-method enqueueing.
- Class-oriented durable object API with `ref`, `expose`, `expose_command`, command idempotency keys, and explicit state updates.
- PostgreSQL/YSQL and MySQL/MariaDB store implementations.
- Durable workflow, step, wait, attempt, fence, outbox, durable-object, and durable-object-command persistence.
- Worker polling with leased workflow claims.
- Heartbeats, stale lease recovery, and lease-aware resume.
- Timer waits, external event waits, side-effect fences, and durable outbox primitives.
- Lease-routed workflow RPC helpers.
- Deterministic simulation tests for workflow safety and crash-recovery scenarios.

This is still a prototype, not a production Temporal replacement.
The implementation is intentionally kept as a plain Ruby gem until it needs Rails hooks, initializers, models, or routes.
