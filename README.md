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
  docs/
    operator-web-ui.md
  test/
```

The gemspec owns runtime dependencies used by the app bundle.
`Durababble::Store.connect` selects the Trilogy-backed MySQL adapter by default, with PostgreSQL/YSQL available when a PostgreSQL URL is provided.

## Workspace/database namespace isolation

Durababble chooses its default database namespace from environment in this order:

1. `DURABABBLE_SCHEMA`, when explicitly set.
2. `Durababble.workspace_schema(DURABABBLE_WORKSPACE_ROOT || Dir.pwd)`, a deterministic, bounded schema name derived from the checkout/workspace path.

Two active checkouts or Symphony workspaces therefore do not share Durababble internal tables by default. The PostgreSQL/YSQL adapter uses the selected value as a SQL schema. The MySQL/MariaDB adapter uses it as a sanitized table-name prefix inside the configured database, so backend conformance still runs without creating separate MySQL databases per worktree.

Symphony-created workspaces write a local `mise.local.toml` with `DURABABBLE_DATABASE_URL`, `DURABABBLE_YUGABYTE_DATABASE_URL`, `DURABABBLE_WORKSPACE_ROOT`, and `DURABABBLE_SCHEMA`; trust it; install the bundle; migrate the isolated namespace; and leave `.durababble-workspace.env` for inspection. These local files are ignored by git.

Inspect the selected namespace:

```sh
mise exec -- bundle exec ruby -Ilib -e 'require "durababble"; puts Durababble.default_schema'
```

Override it deliberately:

```sh
DURABABBLE_SCHEMA=durababble_experiment mise exec -- bundle exec ruby -Ilib -e 'require "durababble"; store = Durababble::Store.connect(database_url: Durababble.default_database_url); store.migrate!; store.close'
```

`Durababble::Store.connect`, `Durababble.configure`, `Durababble::WorkerRuntime`, examples, and benchmark defaults all honor the selected namespace unless a `schema:` argument or benchmark `--schema` flag is provided.

## Testing

The tests use the same Minitest stack as `agent-server`.
They live under `gems/durababble/test` and run directly from the gem without booting Rails.

CI runs the coverage gate with SimpleCov branch coverage enabled:

```sh
mise exec -- bundle exec rake test:coverage
```

The gate measures library files under `lib/**/*.rb`, excluding the gem metadata version file because Bundler loads it before SimpleCov starts. The current ratchet fails below 90% line coverage or 85% branch coverage globally, and below 59% line coverage or 41% branch coverage for any measured library file. The target ratchet is 95% line coverage and 90% branch coverage; raise the configured minimums as meaningful tests lift coverage. The HTML report and SimpleCov result JSON are written to `coverage/`, and GitHub Actions uploads that directory as the `coverage-report` artifact for failed and passing runs.

## Public API

Durababble exposes two complementary abstractions on the same durable store:

| Abstraction                 | Best for                                                                            | Mental model                     |
| --------------------------- | ----------------------------------------------------------------------------------- | -------------------------------- |
| `Durababble::Workflow`      | One-off processes: indexing pipelines, multi-step tool sequences, resumable work    | Function that survives restarts  |
| `Durababble::DurableObject` | Sessions, agent contexts, project state, anything addressed by id and mutable state | Addressed object with durability |

Workflow code is plain Ruby in `#execute`.
Methods declared with `step` are durable side-effect boundaries; replay returns persisted step results instead of rerunning completed work.
If replayed code reaches a different step method at a completed position, or returns before consuming all completed step positions, the run fails with `Durababble::NonDeterminismError` instead of silently reusing incompatible history.

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
- Retry due-time claims distinguish retryable failures from terminal failed workflows.
- Lease-routed workflow RPC helpers.
- Deterministic simulation tests for workflow safety and crash-recovery scenarios.

This is still a prototype, not a production Temporal replacement.
The implementation is intentionally kept as a plain Ruby gem until it needs Rails hooks, initializers, models, or routes.

## Operator UI

Durababble ships a small Rack-compatible operator UI prototype:

```ruby
require "durababble"

store = Durababble::Store.connect(
  database_url: ENV.fetch("DURABABBLE_DATABASE_URL"),
  schema: Durababble.default_schema,
)

run Durababble::Operator::App.new(store:)
```

In Rails, mount the callable behind the host application's own authentication
and authorization middleware. Falcon runs the normal Rails Rack stack, so no
Durababble-specific server adapter is required:

```ruby
# config/routes.rb
mount(
  MyAdminAuthMiddleware.new(Durababble::Operator::App.new(store: Durababble.store)),
  at: "/durababble/operator",
)
```

The prototype UI is intentionally read-only. It renders persisted workflow,
step, attempt, wait, outbox, durable-object, and command rows through
`Durababble::Store`; it does not inspect worker memory or own authentication.
`docs/operator-web-ui.md` specifies the broader target screens, management
actions, security posture, Store/API gaps, and follow-up implementation work.
