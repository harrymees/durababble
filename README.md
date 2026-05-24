# Durababble

Durababble is a Ruby 4 durable-execution prototype for workflows and durable objects backed by SQL. It is for work that must survive process exits, retries, lease movement, and worker restarts without hiding the recovery model behind process-local state.

The current library gives you two primitives:

| Primitive | Use it for | Current API |
| --- | --- | --- |
| Durable workflows | One-off processes with durable steps, waits, retries, cancellation, and results | `Durababble::Workflow`, `Workflow.start`, `Workflow.handle`, `Durababble::Engine#run`, `Workflow.enqueue`, `Workflow.ref` |
| Durable objects | Id-addressed state with durable command rows and explicit state updates | `Durababble::DurableObject`, `DurableObject.ref`, `expose`, `expose_command` |

Durababble is not a production Temporal replacement. It is a correctness-oriented prototype for proving the Ruby API, SQL state machine, recovery behavior, and backend conformance before widening the product surface. Detailed guarantees live in [docs/spec.md](docs/spec.md) and [docs/architecture.md](docs/architecture.md).

## Why It Exists

Ruby applications often need orchestration that is stronger than a background job retry loop but smaller than adopting a full external workflow platform. Durababble explores that middle ground:

- workflow code is ordinary Ruby with explicit durable `step` boundaries;
- every durable boundary is persisted before and after execution;
- workers claim work with SQL leases and refuse stale ownership;
- completed steps replay from storage instead of rerunning side effects;
- waits, fences, outbox rows, and durable-object commands are database state, not in-memory coordination;
- deterministic and crash-recovery tests exercise the failure model directly.

## Where It Is Strong

- **Durability boundaries:** workflows are persisted before execution, steps persist start/attempt/success/failure/wait transitions, and replay validates method/order step identity.
- **SQL-backed recovery:** PostgreSQL/YSQL and MySQL/MariaDB adapters store workflow rows, step history, waits, fences, outbox rows, and durable-object state using Paquito binary payloads (`bytea` / `LONGBLOB`).
- **Lease-aware workers:** worker claims write `locked_by` / `locked_until`; heartbeats extend owned leases; stale workers cannot commit after ownership moves.
- **Waits and signals where implemented:** `Durababble.wait_until` persists timer waits, and `Durababble.wait_event` plus `Store#signal_event` persists external event waits and wakes.
- **Durable objects:** `expose_command` records command rows, gives each command a stable idempotency key, and persists object state updates explicitly.
- **Idempotency, fences, and outbox:** workflow step and object command contexts expose generated idempotency keys; `Store#with_fence` deduplicates fenced side effects; outbox rows are unique, leased, reclaimable, and acknowledged.
- **Deterministic testing:** the local deterministic harness virtualizes time, scheduling, networking, crashes, and the store to prove replay and recovery invariants across seeds.
- **Backend conformance:** common store tests plus backend-specific query-plan tests keep MySQL/MariaDB and PostgreSQL/YSQL behavior aligned where semantics are shared.

## Quickstart

Install the Ruby toolchain and bundle through mise:

```sh
mise install
mise exec -- bundle install
```

Choose a local database URL. The standalone library default is the local MySQL/MariaDB development URL in `Durababble.default_database_url`; Symphony workspaces may also provide a local `mise.local.toml` with isolated `DURABABBLE_*` values. For the host-local Yugabyte/YSQL smoke path, use:

```sh
export DURABABBLE_DATABASE_URL=postgresql://yugabyte@127.0.0.1:15433/yugabyte
export DURABABBLE_YUGABYTE_DATABASE_URL=$DURABABBLE_DATABASE_URL
```

For the default MySQL/MariaDB test path, either rely on the current `DURABABBLE_MYSQL_*` environment or set:

```sh
export DURABABBLE_DATABASE_URL=mysql://root@127.0.0.1:3306/sidekick_server_test
```

Inspect the workspace-isolated namespace that will be used for tables:

```sh
mise exec -- bundle exec ruby -Ilib -e 'require "durababble"; puts Durababble.default_schema'
```

Create or migrate that namespace:

```sh
mise exec -- bundle exec ruby -Ilib -e 'require "durababble"; store = Durababble::Store.connect(database_url: Durababble.default_database_url); store.migrate!; store.close'
```

Run the counter example against the selected database:

```sh
mise exec -- ruby examples/counter.rb
```

Run the full local suite:

```sh
mise exec -- bundle exec rake test
```

Set `DURABABBLE_YUGABYTE_DATABASE_URL` to include optional Yugabyte-backed tests. Without it, the shared backend suite runs against the configured MySQL/MariaDB path.

## Current API Examples

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

## Prototype Boundaries

The README describes the implemented prototype. The spec also records the intended public direction so reviewers can distinguish current behavior from target behavior.

- Class-oriented workflow API with `#execute`, `step def`, retry policy, step idempotency keys, class-method enqueueing, and current `Workflow.start` / `Workflow.handle` aliases.
- First-class cooperative workflow cancellation through `Workflow.handle(...).cancel(reason:)`, persisted cancellation requests, `canceling` / `canceled` states, and replay-safe cleanup steps.
- Class-oriented durable object API with `ref`, `expose`, `expose_command`, command idempotency keys, and explicit state updates.
- PostgreSQL/YSQL and MySQL/MariaDB store implementations.
- Durable workflow, step, wait, attempt, fence, outbox, durable-object, and durable-object-command persistence.
- Worker polling with leased workflow claims.
- Heartbeats, stale lease recovery, and lease-aware resume.
- Timer waits, external event waits, side-effect fences, and durable outbox primitives.
- Retry due-time claims distinguish retryable failures from terminal failed workflows.
- Lease-routed workflow RPC helpers.
- Deterministic simulation tests for workflow safety and crash-recovery scenarios.

- `DurableObject.at` and `DurableObject.tell` are the preferred future durable-object spellings. The current durable-object implementation still uses `DurableObject.ref`; workflow code supports both `Workflow.start` / `Workflow.handle` and lower-level `Workflow.enqueue` / `Workflow.ref` / `Engine#run`.
- Workflow command methods currently persist command events; executing command bodies through the workflow owner and returning command results is target runtime work.
- Full durable workflow signals (`signal def`, `wait_condition`) are target work. Implemented today are lower-level timer waits, event waits, and event signaling.
- Durable-object commands persist command rows and execute inline in the current prototype. Per-object FIFO mailbox leasing, async `tell`, sleeps, and worker-driven object execution are target work.
- Fences deduplicate side effects after a fence row is inserted, but fence-owner crash recovery is not complete.
- The gRPC transport and workflow RPC routing are implemented for the prototype test matrix, but production mTLS/Spiffe policy, admin surfaces, metrics, tracing, and operator tooling are not yet implemented.
- There is no compatibility promise for production workloads yet. Treat the SQL schema, public names, and operational knobs as prototype surfaces unless the spec states otherwise.

## Documentation Gateway

- [docs/spec.md](docs/spec.md): source of truth for implemented, partial, target, and future-scope guarantees.
- [docs/architecture.md](docs/architecture.md): component overview, storage model, worker lifecycle, durability semantics, and benchmark/query-shape strategy.
- [docs/deterministic-testing.md](docs/deterministic-testing.md): deterministic simulation harness, recovery scenarios, seed search, and bugs found by the harness.
- [bench/README.md](bench/README.md) and [bench/run.rb](bench/run.rb): benchmark operation coverage and local benchmark commands.
- [examples/counter.rb](examples/counter.rb): minimal runnable workflow example.
- [docs/huginn-integration-report.md](docs/huginn-integration-report.md): integration notes from a real Rails/MySQL application experiment.
- [sig/durababble.rbs](sig/durababble.rbs): static-only RBS declarations for the public class API.

Historical comparison and review notes remain in `docs/`, but the current API and guarantees are defined by this README, the spec, and the architecture doc.
