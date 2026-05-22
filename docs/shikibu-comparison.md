# Durababble vs. Shikibu comparison

> Historical note: this comparison was written before Durababble replaced the old `Workflow.define` context-step DSL with the class-oriented `Durababble::Workflow` / `Durababble::DurableObject` API. Treat old API examples in this document as historical context, not current public API documentation. Current API docs live in `README.md`, `docs/spec.md`, and `docs/architecture.md`.

Date: 2026-05-21

## Repository snapshots inspected

- Durababble: `/home/airhorns/code/durababble`, branch `compare-shikibu`, commit `1e9febae1e8f0fabe6d993165dc55459a676b2a9` plus the pre-existing uncommitted working tree changes present when the branch was created.
- Shikibu: `/home/airhorns/code/shikibu`, branch `main`, commit `8afa276b2fc80d3d7133b7411c032e0fc934af99`.
- Shikibu schema submodule: `/home/airhorns/code/shikibu/schema`, commit `bc58e303a96b413a2ca7387815d566c9b69f221d`.

I initialized Shikibu's schema submodule because its storage layer depends on `schema/db/migrations/*`.

## Executive answer

Shikibu is much broader, more productized, and closer to a usable Ruby durable-execution library today. It has a better application-facing API, richer docs, multi-database support, saga compensation, typed payload support, channel messaging, Rack/CloudEvents integration, Sidekiq/ActiveJob integration, a background worker runtime, leader election, PostgreSQL LISTEN/NOTIFY, shared cross-language schema, and a real outbox relayer.

Durababble should still exist if the goal is to explore a stricter YugabyteDB-first durable-execution core with explicit crash/guarantee matrices, Paquito/binary runtime storage, deterministic simulation testing, lease-routed workflow RPC, query-shape benchmarking, and a deliberately small state machine whose invariants are easy to reason about. It should probably not try to compete as a general Ruby durable-execution framework without absorbing many of Shikibu's product/API ideas.

My recommendation: keep Durababble as the correctness/coordination laboratory and either (a) narrow its identity around Yugabyte-specific durability and distributed-owner semantics, or (b) mine Shikibu aggressively for API/runtime features before presenting Durababble as an application framework. Do not delete it just because Shikibu exists; Shikibu leaves several correctness/research questions less explicitly specified than Durababble.

## High-level positioning

| Dimension | Durababble | Shikibu | Winner / implication |
| --- | --- | --- | --- |
| Primary identity | Ruby 4 prototype durable engine backed specifically by YugabyteDB/YSQL | Ruby 3.3+ lightweight durable-execution framework, port of Edda, no separate server | Shikibu for users; Durababble for Yugabyte/correctness research |
| API shape | `Workflow.define` with ordered named `step`s receiving/returning context hashes | subclass `Shikibu::Workflow`, `execute`, inline `activity`, typed input/output, compensation, sleep/wait/channel helpers | Shikibu is much more idiomatic and ergonomic |
| Storage | hand-written `pg` SQL, one schema, Paquito `bytea` runtime columns | Sequel over SQLite/PostgreSQL/MySQL, shared schema submodule, JSON text plus optional binary columns | Shikibu for portability; Durababble for binary storage/YSQL-specific control |
| Recovery model | step table + append-only attempts; skip completed steps; retry incomplete rows | deterministic replay from `workflow_history`; cached activity results by activity id | Shikibu has the more conventional Temporal/Edda model; Durababble has explicit step-state semantics |
| Multi-worker coordination | workflow leases, `FOR UPDATE SKIP LOCKED`, heartbeats, stale lease recovery | distributed locks on workflow instances, stale lock cleanup, leader election for background tasks | Shikibu has fuller runtime orchestration; Durababble has clearer lease/RPC invariants |
| Waiting | timer waits and external event waits | timer sleep, wait_event, channel messaging, direct/competing/broadcast delivery | Shikibu substantially better |
| Side effects | idempotency fences and durable outbox, but no crash recovery for running fences yet | activity history caching, saga compensation, transactional outbox relayer | Different emphasis; Shikibu better for app patterns, Durababble more explicit about fence semantics |
| Messaging | lease-routed workflow RPC through current owner | channel messaging + CloudEvents/Rack + PostgreSQL NOTIFY | Shikibu better external/app messaging; Durababble has a unique owner-routed RPC idea |
| Testing | real Yugabyte integration suite, crash harness, DST-style deterministic simulator, SimpleCov thresholds, benchmark workflow | unit tests, PostgreSQL/MySQL integration tests in CI, examples, RuboCop | Durababble stronger on explicit failure-model proof; Shikibu broader coverage/examples |
| Production packaging | prototype CLI only; no daemon supervisor/metrics/tracing | app/worker runtime, Rack, Sidekiq/ActiveJob, docs site structure, release workflow | Shikibu much more product-ready |

## Durababble architecture observed

Important files read:

- `README.md`
- `docs/spec.md`
- `docs/architecture.md`
- `docs/deterministic-testing.md`
- `lib/durababble/workflow.rb`
- `lib/durababble/engine.rb`
- `lib/durababble/store.rb`
- `lib/durababble/worker.rb`
- `lib/durababble/workflow_rpc.rb`
- `lib/durababble/deterministic.rb`
- `durababble.gemspec`

Durababble is a compact engine:

- `Durababble::Workflow` is a simple ordered-step DSL: `Workflow.define("counter") { step("increment") { |ctx| ... } }`.
- `Durababble::Engine` enqueues and resumes workflows, claims leases, records step starts/completions/failures, records waits, and reconstructs context from completed steps.
- `Durababble::Worker` is intentionally minimal: `tick` claims one runnable workflow and resumes it; `run_until_idle` loops until idle.
- `Durababble::Store` owns all SQL directly through the `pg` gem. It creates `workflows`, `steps`, `step_attempts`, `waits`, `fences`, and `outbox` tables. Runtime values are serialized via Paquito into `bytea` columns.
- `Durababble::WorkflowRpc` routes commands to the worker that currently owns a workflow lease, validates the lease at the receiver before and after handling, translates stale/no-owner errors, and can start/await a new lease before retrying.
- `Durababble::Deterministic` provides a virtual scheduler, network, and Yugabyte-like store for seed-searching distributed schedules and fault timing.

Durababble's strongest architectural choices:

1. **Explicit guarantee and crash matrix.** `docs/spec.md` lists concrete guarantees and maps them to tests. This is a very good habit and more rigorous than Shikibu's marketing-style guarantee language.
2. **Attempt history is first-class.** `step_attempts` records retries and stale attempts. The docs say DST found and pinned a stale-running-attempt bug.
3. **Direct YSQL control.** Hand-written SQL makes lease, wait, outbox, and index behavior visible and tunable for Yugabyte.
4. **Binary runtime payloads.** Paquito `bytea` storage avoids accidentally treating Ruby runtime values as JSON/JSONB. This may matter if preserving Ruby object fidelity is important.
5. **Lease-routed owner RPC.** Shikibu has messaging, but Durababble's current-owner RPC is a distinct distributed-systems primitive useful for actor-like workflow ownership.
6. **Deterministic simulation.** Even if purpose-built, the virtual scheduler/network/store are a serious differentiator for discovering race bugs.
7. **Benchmark/query-shape focus.** `docs/architecture.md` and `.github/workflows/benchmarks.yml` show a deliberate benchmark harness for queue claims, waits, outbox, large fixtures, and longitudinal artifacts.

Durababble's biggest weaknesses relative to Shikibu:

1. The public workflow API is too low-level: ordered context-transforming steps are simple but not what app developers expect from durable execution.
2. No clear Activity abstraction; side effects are just step code plus optional fences/outbox.
3. No saga compensation API.
4. No typed input/output story.
5. No background application lifecycle comparable to `Shikibu::App#start/#shutdown`.
6. No Rack/Rails/Sidekiq/ActiveJob integrations.
7. No PostgreSQL LISTEN/NOTIFY or wakeup mechanism; waiting/resumption is poll/tick oriented.
8. No SQLite dev mode yet; the real SQL backends are now Yugabyte/PostgreSQL wire support and MySQL/MariaDB.
9. CLI is a prototype counter harness, not an operational UI.
10. Docs correctly admit missing workflow versioning, cron, metrics, tracing, daemon supervision, fence owner crash recovery, automatic long-step heartbeat, and graceful missing-registry handling.

## Shikibu architecture observed

Important files read:

- `README.md`
- `docs/getting-started/*`
- `docs/core-features/*`
- `docs/integrations/*`
- `lib/shikibu/workflow.rb`
- `lib/shikibu/activity.rb`
- `lib/shikibu/replay.rb`
- `lib/shikibu/app.rb`
- `lib/shikibu/worker.rb`
- `lib/shikibu/storage/sequel_storage.rb`
- `lib/shikibu/storage/migrations.rb`
- `lib/shikibu/channels.rb`
- `lib/shikibu/outbox/relayer.rb`
- `lib/shikibu/notify/pg_notify.rb`
- `lib/shikibu/middleware/rack_app.rb`
- `schema/db/migrations/postgresql/20251217000000_initial_schema.sql`
- `shikibu.gemspec`
- `.github/workflows/ci.yml`

Shikibu is much larger. Rough local counts, excluding `.git`/build artifacts:

- Durababble: ~30 Ruby files, ~3,960 nonblank/noncomment Ruby LOC, ~15 spec files.
- Shikibu: ~44 Ruby files, ~6,998 nonblank/noncomment Ruby LOC, ~12 spec files plus far more docs/examples.

Shikibu's core model:

- Users subclass `Shikibu::Workflow` and implement `execute`.
- Activities are called inline with `activity :name do ... end`; during replay, cached activity results are returned from history.
- `ReplayEngine` starts/resumes workflows, saves workflow definitions/source hashes, creates workflow instances, loads history, builds a cache, runs the workflow, handles timer/channel suspension, recurrence, cancellation, compensation, and lock release.
- `SequelStorage` abstracts SQLite/PostgreSQL/MySQL. It handles instances, history, distributed locks, timers, subscriptions, outbox events, system locks, and database-specific behavior such as `SKIP LOCKED` vs SQLite atomic update.
- `Worker` starts background threads for workflow resumption, message delivery, leader election, timer checks, stale lock cleanup, timeout checks, message cleanup, and optional outbox relay.
- `Channels` implements broadcast, competing-consumer, and direct messaging patterns.
- `RackApp` exposes CloudEvents ingestion, health endpoints, status/result/cancel endpoints.
- `PostgresNotifyListener` uses `LISTEN/NOTIFY` to wake loops on PostgreSQL.
- Schema is shared with Python Edda and Go Romancy through a submodule and a `framework` column.

Shikibu's strongest choices:

1. **Much better developer API.** Subclassing a workflow and writing normal-looking `execute` code with `activity`, `sleep`, `wait_event`, `publish`, and `receive` is more likely to be adopted than Durababble's context-step DSL.
2. **Activity/result history replay.** The `activity` abstraction is the key API primitive Durababble lacks. It maps directly to durable execution mental models.
3. **Saga compensation.** `on_failure` registers named compensations; `ReplayEngine` can resume compensating workflows from DB state.
4. **Typed workflows.** Optional `dry-struct`/Data-like input/output validation is a strong ergonomics and correctness feature.
5. **Multiple databases.** SQLite for dev/test, PostgreSQL and MySQL for multi-process production makes adoption much easier.
6. **Integrations and ops surface.** Rack, Rails docs, Sidekiq/ActiveJob examples, CloudEvents endpoints, status/result/cancel endpoints, health checks, CI, release workflow, and docs structure make this feel like a product.
7. **Background runtime.** A real app object and worker threads solve things Durababble still leaves as `tick` loops.
8. **PostgreSQL wakeups.** LISTEN/NOTIFY reduces polling latency/load for PostgreSQL deployments.
9. **Channel messaging.** Broadcast, competing, and direct modes are a bigger concept than Durababble's event waits.
10. **Shared schema/ecosystem.** Compatibility with Edda/Romancy is strategically valuable.

Shikibu weaknesses / caution areas:

1. **Guarantees are less formally specified.** README claims activities execute exactly once and workflows survive arbitrary crashes, but I did not find a Durababble-style crash matrix that enumerates every failure point and test. The code's comments around `Workflow#execute_activity` say executing the activity and appending history are "atomic" in a DB transaction, but arbitrary external side effects inside the block are not actually atomic with DB commit unless the side effect itself participates in that transaction.
2. **Activity id determinism is fragile by nature.** `activity_id = ctx.generate_activity_id(name.to_s)` implies stable order/name dependence. This is conventional for replay engines, but it creates workflow-versioning hazards that need explicit user guidance.
3. **JSON-first storage.** The shared schema stores `input_data`, `output_data`, and most event data as text JSON with optional binary columns. That is portable, but less faithful to arbitrary Ruby values than Durababble's Paquito approach.
4. **Schema/migrations are submodule-dependent.** Cloning without submodules leaves migrations unavailable. CI uses `actions/checkout` with `submodules: true`; local users can miss this.
5. **More moving parts.** Threads, leader election, notify listener, relayer, channels, shared schema, and multi-DB support increase operational and correctness surface area.
6. **Tests could not be run in this host as-is.** The repo requires Ruby 3.3.6; this host's mise currently has Ruby 4.0.5 installed. `mise exec -- ruby ...` failed for Shikibu before installing Ruby 3.3.6. I therefore treated source/docs/tests as evidence, not passing local execution.

## Feature-by-feature comparison

### 1. Workflow definition

Durababble:

```ruby
workflow = Durababble::Workflow.define("counter") do
  step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
  step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
end
```

Shikibu:

```ruby
class OrderSaga < Shikibu::Workflow
  workflow_name 'order_saga'

  def execute(order_id:, amount:)
    result = activity :process_payment do
      PaymentService.charge(order_id, amount)
    end
    on_failure :refund_payment, order_id: order_id
    { status: 'completed', order_id: order_id, payment: result }
  end
end
```

Shikibu is much better for application authors. Durababble's DSL is easier to reason about internally but feels like a storage-engine test harness. Steal Shikibu's base-class API or at least add a higher-level facade over Durababble's engine.

### 2. Activity model and replay

Durababble persists ordered step completion and skips completed steps. That is simple and robust for linear workflows, but the durable unit is the whole step, not a nested activity. If a step has multiple side effects, Durababble needs users to manually split steps or use fences/outbox correctly.

Shikibu persists workflow history by `activity_id`; during replay, already completed activities return cached results. This allows a single `execute` method to contain normal orchestration code while still memoizing side effects. This is a must-steal idea.

Caveat: Shikibu's comments say activity execution and recording are atomic, but if the activity is an external payment/email/API call, the external side effect is not atomically committed with the DB history row. Durababble's fence/outbox design is more honest about the gap. A better Durababble API could combine Shikibu-style `activity` ergonomics with explicit side-effect policies: pure activity, idempotency-key activity, outbox activity, local-transaction activity.

### 3. Storage and database scope

Durababble is Yugabyte/YSQL-specific, hand-written SQL, Paquito `bytea`. It creates performance indexes explicitly for queue claims, expired leases, waits, attempts, and outbox scans. This is good for learning and performance work.

Shikibu uses Sequel and a shared schema supporting SQLite/PostgreSQL/MySQL. This is much better for adoption. SQLite default support is especially useful for tutorials and CI. PostgreSQL/MySQL integration tests exist in GitHub Actions.

Decision point: if Durababble wants to be a product, add SQLite dev support. If it wants to be a Yugabyte lab, keep YSQL-specific SQL and lean harder into correctness/perf claims Shikibu cannot make.

### 4. Waiting and messaging

Durababble supports timer waits and external event waits with transactional wakeup and concurrent signal protection. It also has lease-routed workflow RPC, which is unique and useful for sending commands to the current owner.

Shikibu has a much richer messaging story:

- `sleep` / `sleep_until`
- `wait_event`
- `subscribe` / `receive` / `try_receive`
- `publish`
- `send_to`
- channel modes: broadcast, competing, direct
- PostgreSQL LISTEN/NOTIFY for waking workers
- Rack CloudEvents ingress

Steal: channels, direct messages, LISTEN/NOTIFY wakeups, CloudEvents ingress, and status/result/cancel HTTP endpoints. Preserve: Durababble's strict lease-owner RPC as a separate primitive.

### 5. Compensation and side effects

Durababble has:

- side-effect fences via `Store#with_fence`
- durable outbox with unique keys, leasing, expiry recovery, ack
- explicit known gap: fence owner crash can leave a running fence forever until waiters time out

Shikibu has:

- `on_failure` saga compensation, persisted in `workflow_compensations`
- compensation idempotency via history rows
- crash recovery path for `compensating` workflows
- outbox relayer that publishes CloudEvents to HTTP broker and marks published/failed/invalid/expired

Shikibu has done app-level side-effect patterns better. Durababble has stronger low-level fence/outbox lease modeling but lacks the user-facing saga abstraction and actual relayer.

Steal: named compensation registry, LIFO saga API, outbox relayer with retry/backoff/max-age/permanent-vs-retryable error handling.

Improve beyond Shikibu: make compensation/outbox/fence semantics explicit in the guarantee matrix, including crash between external side effect and history append.

### 6. Worker/runtime lifecycle

Durababble worker is intentionally small: one `tick` claims and runs one workflow. This is great for tests and proof, but not a complete runtime.

Shikibu has `App#start`/`#shutdown`, background worker threads, leader election through `system_locks`, timer checks, stale lock cleanup, timeout checks, message cleanup, optional outbox relayer, and wake events.

Shikibu is better here. Durababble should steal the App lifecycle and leader-only background jobs, while retaining a deterministic/single-tick mode for testing.

### 7. Tests and verification

Durababble's verification story is unusually good for a prototype:

- real Yugabyte/YSQL RSpec integration suite
- subprocess crash harness
- hardening specs for concurrency
- deterministic simulation seed search over safety rows and crash rows
- SimpleCov thresholds
- benchmark artifacts

Shikibu has:

- unit specs
- PostgreSQL/MySQL integration specs in CI
- examples and docs
- RuboCop
- release workflow

Shikibu probably has more surface tested by ordinary unit examples, but Durababble's failure-model testing is more compelling. This is a major reason Durababble should continue existing.

## Should Durababble even exist?

Yes, if at least one of these is true:

1. You care about YugabyteDB/YSQL-specific durable-execution correctness and performance.
2. You want a small engine whose crash and lease semantics are fully enumerated.
3. You want deterministic simulation / DST-style race hunting in Ruby durable execution.
4. You want to experiment with current-owner workflow RPC and strict lease-routed commands.
5. You want binary runtime-value storage rather than JSON-first cross-language schema.

No, or not as a separate product, if the goal is simply "a Ruby durable execution framework people can use in Rails/service apps soon." Shikibu is already much further ahead on that axis.

Best framing: Durababble is not a worse Shikibu; it is a stricter experimental core. But if Durababble is marketed as an application framework without absorbing Shikibu's APIs/integrations, Shikibu makes it look redundant.

## Ideas to steal from Shikibu, prioritized

### P0: API and product basics

1. **Workflow base class API**
   - Add `class MyWorkflow < Durababble::Workflow` with `execute` rather than only `Workflow.define` ordered steps.
   - Keep the existing step DSL internally or as a low-level mode.

2. **Activity abstraction**
   - Add `activity :name do ... end` with persisted history/memoization.
   - Expose activity-level retry policy.
   - Make activity IDs explicit/stable and document versioning pitfalls.

3. **App runtime**
   - Add `Durababble::App.new(database_url:, service_name:, worker_id:, ...)`.
   - Provide `start`, `shutdown`, `register`, `start_workflow`, `resume_workflow`, `status`, `result`, `cancel`.

4. **Background worker loops**
   - Keep `Worker#tick` for deterministic tests, but add production-ish loops for queue draining, timers, waits, stale leases, outbox.

5. **Docs shape**
   - Shikibu's README and `docs/getting-started`, `core-features`, `integrations`, `examples` structure is much better.

### P1: Workflow capabilities

6. **Saga compensation**
   - Add named compensation registry and `on_failure` blocks.
   - Persist compensation stack and prove crash recovery.

7. **Typed payloads**
   - Support optional Data/dry-struct input/output validation.
   - Keep Paquito for storage, but provide typed ergonomic boundaries.

8. **Channel messaging**
   - Broadcast, competing-consumer, direct message modes.
   - Integrate with waits and owner routing.

9. **Recur/continue-as-new**
   - Shikibu's `recur` archives history and creates a continued instance. Durababble needs a comparable story before long-running loops are ergonomic.

10. **Cancellation**
    - First-class cancel API, state, and tests.

### P2: Integrations and operations

11. **Rack/CloudEvents ingress**
    - `/events`, `/health`, `/workflows/:id/status`, `/result`, `/cancel` endpoints.

12. **Rails / Sidekiq / ActiveJob docs or adapters**
    - Even thin examples would dramatically improve perceived usefulness.

13. **PostgreSQL LISTEN/NOTIFY**
    - Useful if running against PostgreSQL-compatible endpoints that support it; evaluate Yugabyte compatibility and behavior carefully.

14. **Outbox relayer**
    - Durababble has storage primitives; Shikibu has a real relayer with HTTP CloudEvents, retry, max retry, max age.

15. **Migration ergonomics**
    - Multi-worker-safe migrations with advisory locks or equivalent YSQL-safe lock.

### P3: Strategic choices to evaluate, not blindly copy

16. **Multi-DB support**
    - Great for product adoption. But it may dilute Durababble's Yugabyte correctness focus.

17. **Shared cross-language schema**
    - Strategic but constraining. Durababble's Paquito/Ruby fidelity and explicit step attempts may not fit Shikibu/Edda/Romancy schema.

18. **Sequel abstraction**
    - Better portability; worse direct control over YSQL query plans. Use only if product scope wins over engine-research scope.

## What Durababble has done better

1. **Correctness spec discipline.** Shikibu has feature docs; Durababble has guarantee and crash matrices.
2. **DST-style bug hunting.** Durababble's virtual scheduler/network/store is a real differentiator.
3. **Yugabyte-focused query/performance work.** Shikibu targets multiple DBs; Durababble can be sharper about YSQL behavior.
4. **Lease-routed workflow RPC.** Shikibu has channel messaging but not the same current-owner command routing semantics.
5. **Runtime value fidelity.** Paquito `bytea` storage avoids JSON coercion limits.
6. **Explicit known gaps.** Durababble docs name shortcomings like fence owner crash recovery; Shikibu docs are more aspirational in places.
7. **Attempt history and stale retry cleanup.** Durababble's append-only attempts and retry cleanup are explicit and tested.

## What Shikibu has done better

1. **User-facing API.** Subclassed workflows plus activities are far better than ordered context steps.
2. **Application features.** Compensation, typed workflows, channels, recurrence, cancellation.
3. **Integrations.** Rack, Rails, Sidekiq/ActiveJob, CloudEvents, health/status/result endpoints.
4. **Runtime.** `App`, background worker, leader election, cleanup loops, notification wakeups.
5. **Portability.** SQLite/PostgreSQL/MySQL.
6. **Documentation and examples.** Much more complete onboarding and feature docs.
7. **Release/project polish.** CI with lint/tests/integration, release workflow, gemspec metadata, docs site config.
8. **Outbox relayer.** Not just storage; actually publishes and retries CloudEvents.
9. **Ecosystem.** Shared schema with Edda/Romancy gives it a story beyond one Ruby gem.

## Concrete next steps for Durababble

If we want Durababble to remain independent and compelling, I would do this sequence:

1. **Write a crisp positioning doc**: "Yugabyte-first durable execution core with explicit crash guarantees" rather than generic Ruby Temporal clone.
2. **Add a high-level workflow/activity API** inspired by Shikibu while keeping the existing engine/test core.
3. **Design an activity side-effect policy model** that is more honest than Shikibu's blanket exactly-once wording:
   - replay-cached pure activity
   - fenced idempotent activity
   - outbox activity
   - externally idempotent activity with key
4. **Add saga compensation** and a crash matrix for compensation failure/recovery.
5. **Add `Durababble::App` and background loops**, but keep deterministic tickable components for tests.
6. **Add channels/direct messages** and consider unifying them with existing workflow RPC.
7. **Add Rack/CloudEvents/status endpoints** for product usefulness.
8. **Fix fence owner crash recovery** before making strong side-effect claims.
9. **Document workflow versioning/activity ID rules** early.
10. **Optionally add SQLite dev mode** only if product ergonomics become a goal; otherwise stay Yugabyte-specific and deepen benchmarks.

## Bottom line

Shikibu makes Durababble look immature as a general-purpose Ruby durable-execution framework. It does not make Durababble intellectually or technically obsolete. Durababble has a better correctness-research posture, better explicit crash semantics, more direct YSQL control, and a unique owner-routed RPC experiment. The right move is to steal Shikibu's API/runtime/integration ideas while preserving Durababble's stricter specification, deterministic testing, and Yugabyte focus.
