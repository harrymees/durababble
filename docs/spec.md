# Durababble specification

This is the reconciled in-repo Durababble spec. It merges the requirements from `/home/airhorns/durababble_plan.md` into the current standalone repository spec while preserving decisions already made in this repo: SQL backend abstraction, PostgreSQL/YSQL **and** MySQL/MariaDB support, Paquito binary storage, the class-oriented Ruby API, current tested workflow leases/recovery, and the high-level worker runtime.

Status labels used below:

- **Implemented**: present in the repo and covered by tests unless stated otherwise.
- **Partially implemented**: API or storage exists, but a runtime path, distributed behavior, or test coverage is incomplete.
- **Target**: desired behavior imported from the home-directory design spec and reconciled with repo decisions.
- **Future scope**: desired eventually, but explicitly not a near-term implementation constraint.
- **Question**: still needs a product/runtime decision before implementation.

## Settled reconciliation decisions

These decisions resolve the earlier spec-vs-implementation conflicts:

1. **Public API direction:** move toward `Workflow.start` / `Workflow.handle` and `DurableObject.at` / `DurableObject.tell`. Current `Workflow.enqueue` / `Workflow.ref` and `DurableObject.ref` are implementation facts today and may remain as compatibility aliases while the public surface migrates.
2. **Worker-pool keying:** persist `worker_pool` and include it in primary keys/indexes where query patterns route, claim, schedule, list, or recover by worker pool. Do **not** add it gratuitously to keys for tables whose query patterns do not care about worker pool.
3. **Ruby::Box:** workflow execution boxes are **future scope**, not a near-term requirement for this prototype.
4. **Step identity:** method/order-based step identity is the current and preferred approach. Do not revert to the old explicit `step(:name) { ... }` API as the primary model.
5. **Inbox:** object commands, object wakes, workflow signals, and workflow command events should converge on one unified durable inbox/mailbox model.
6. **Remote RPC:** implement the full four-method gRPC service (`AwakenBatch`, `EvictLease`, `CallTransient`, `DeliverMessage`) for real inter-node communication, not a minimal workflow-only subset.
7. **Serialized state migration/capability routing:** defer class-level serialized data migration and node capability routing for now. `schema_version` / `on_load` for object state and workflow args/results/errors remain documented future scope, but they are not part of the first gRPC/inbox implementation.
8. **Workflow patch markers:** add a Temporal-style `patched(...)` event-log checker as target workflow code-evolution machinery. This is separate from serialized state migration: it protects deterministic workflow control-flow changes by recording/checking patch markers in workflow history.

The current prototype is not a production Temporal replacement. It is a correctness-oriented Ruby 4 durable-execution prototype with explicit durability, recovery, lease, RPC, storage, and testing requirements.

## Functional spec

- **Ruby library and gem shape.** Durababble is a Ruby 4 gem scaffold managed by mise. The original proposal described incubation inside `agent-server`; the repo is now a standalone prototype library.
- **Two durable primitives.** Durababble exposes durable workflows for one-off, start-to-finish executions and durable objects for long-lived id-addressed stateful entities.
- **Class-oriented public API.** The implemented API uses `Durababble::Workflow` subclasses with `#execute`, `step`, `expose`, and `expose_command`; and `Durababble::DurableObject` subclasses with `expose`, `expose_command`, and explicit `update_state`. Public direction is `Workflow.start/handle` and `DurableObject.at/tell`; current `enqueue/ref` names are transitional implementation facts.
- **SQL-backed storage.** Storage works through either the PostgreSQL wire protocol (`postgresql://` / `postgres://`, including YugabyteDB/YSQL) or MySQL/MariaDB (`mysql2://` / `mysql://`). The home spec was Yugabyte/Postgres-only and listed MySQL as an anti-goal; the repo has intentionally moved beyond that. The default storage namespace is `DURABABBLE_SCHEMA` when set, otherwise a deterministic `Durababble.workspace_schema(DURABABBLE_WORKSPACE_ROOT || Dir.pwd)` value, so simultaneous worktrees and Symphony workers do not share internal tables by accident.
- **Backend conformance.** Both SQL adapters must implement the same durable state-machine semantics. Backend-specific SQL is allowed, but behavior must be proven through shared conformance tests plus backend-specific plan/locking tests when query shape matters.
- **Binary runtime payloads.** Runtime values (`input`, `result`, `context`, `payload`, state, arguments, kwargs, and heartbeat cursors) are serialized with Paquito into binary columns, not JSON/JSONB. PostgreSQL/YSQL stores these columns as `bytea`; MySQL/MariaDB stores them as `LONGBLOB`. The PostgreSQL/YSQL migration can convert the earlier prototype's JSONB runtime columns into Paquito bytea columns.
- **Durable workflow rows and step rows.** Workflows are durable before execution. Step identity is assigned by deterministic method execution order; method names are recorded as metadata and users do not pass step names at call sites.
- **Append-only attempts.** Step attempts are append-only, including waits that transition to completed attempts. Retries and stale attempts remain inspectable.
- **Runnable workflow queue.** Runnable workflows are represented by `pending` rows, retryable `failed` rows whose non-null `next_run_at` is due, or expired `running` leases that are recoverable. Terminal `failed` rows with no retry deadline are not claimable.
- **Distributed workflow leases.** Workflow ownership is represented by `locked_by` and `locked_until` on the current schema's `workflows` table. Lease holders must re-check ownership before mutating durable workflow state.
- **Lease-aware resume.** `Engine#resume` refuses to execute work owned by another live worker.
- **Heartbeat extension.** Active workflow leases can be extended, including explicit step heartbeats with opaque cursor storage. Long-running steps do not heartbeat automatically; user code must call the provided heartbeat before the lease deadline or choose a long enough lease.
- **Expired lease stealing.** Crashed workers are recovered by returning expired `running` workflows to `pending`.
- **Resume semantics.** Completed steps are skipped; incomplete/running/failed/waiting steps are retried or continued according to durable state.
- **Replay shape checks.** A completed step is only replayed when the current workflow reaches the same durable position with the same recorded step method name. A mismatch fails the workflow with `Durababble::NonDeterminismError` instead of silently returning a stale result for different code. Replay also fails if `#execute` returns before consuming all completed step positions, so removing a durable suffix or skipping an already-recorded branch cannot silently complete with partial history.
- **Timer waits.** `Durababble.wait_until` persists timer waits and resumes workflows after the wake time.
- **External event waits.** `Durababble.wait_event` and `Store#signal_event` persist event waits and wake matching workflows.
- **Durable workflow signals.** Target: workflow signals are durable inbox/history messages delivered to declared signal handlers at deterministic workflow yield points. Current implementation has lower-level `wait_event` / `signal_event` and workflow `expose_command` command events, but not the full `signal def` / `wait_condition` inbox-history model.
- **Object sleeps.** Target: durable objects support one pending `sleep_until`/`cancel_sleep` wakeup per object id, converted atomically into a durable mailbox wake message. Not implemented in the current repo.
- **Idempotency fences.** `Store#with_fence` acquires a fence before the side-effect block executes so concurrent callers do not duplicate the side effect. Fence owner crash recovery is not implemented.
- **Durable outbox.** Outbox rows have unique keys, leasing, expiry recovery, and acknowledgement. The public workflow/object API does not expose outbox as a first-class concept yet.
- **Durable object commands.** `expose_command` on durable objects records command rows, creates a stable library-generated idempotency key, and executes inline in the current prototype. Target runtime requires per-object durable mailbox ordering, lease ownership, recovery, and worker-driven execution through the unified inbox.
- **Workflow exposed commands.** `expose_command` on workflows currently persists durable command events via `Store#signal_event`. Target behavior is durable inbox command/signal delivery to the workflow owner with return values where the API is synchronous.
- **Cooperative workflow cancellation.** Implemented. `Workflow.handle(workflow_id).cancel(reason:)` durably records the first cancellation request and requests cooperative delivery. Pending, waiting, and retry-backoff runs move to `canceling`; running runs keep their active lease and observe cancellation at deterministic yield points.
- **Exposed transient methods.** `expose` declares public query/transient methods. Target behavior is owner-local non-durable RPC via gRPC for live objects/workflows; current behavior is local ref/query execution without remote owner routing.
- **Library-generated operation keys.** Workflow steps and durable object commands receive library-generated idempotency keys (`step_context.idempotency_key`, `command_context.idempotency_key`). Target public APIs also accept caller idempotency keys for starts, asks, tells, and signals.
- **Worker pools.** Worker pools are runtime groupings for workers that can execute a workflow/object family. Current `WorkerRuntime` accepts a `worker_pool` name and filters claims by registered workflow class names. Target persistence should add `worker_pool` to durable targets and to keys/indexes where query patterns need pool scoping.
- **Inter-node RPC.** Actual remote intranode/inter-pod communication must be gRPC over mTLS/Spiffe, not the current local JSON-line subprocess command RPC. The repo's `Durababble::RpcClient` is only a local command-process protocol used by tests/benchmarks.
- **Sticky placement.** Target runtime requires pool-local sticky placement for hot durable objects and running workflows, with leases, node registration, owner lookup, in-memory caches, and route-to-owner behavior. Only workflow lease-routing primitives exist today.
- **Determinism and code evolution.** Current workflows are deterministic by method/order step sequence and persisted outputs. Target code evolution includes Temporal-style `patched` / `deprecate_patch` marker checks for safe workflow control-flow changes. Future scope includes `Ruby::Box` workflow realms, deterministic time/sleep/random shims, workflow-local deterministic fibers, and class-level schema-versioned object/workflow state.
- **RBS typing.** The runtime does not load or validate user RBS. The gem ships `sig/durababble.rbs` with `Durababble::Workflow[Input, Output]` and `Durababble::DurableObject[Id, State]` generics for static tooling only.
- **High-level worker lifecycle.** `Durababble::WorkerRuntime` is the app/process integration point. It loops `Worker#tick` for one worker pool, stops taking new claims on shutdown, waits for in-flight work up to a timeout, and revokes still-held workflow/outbox leases if the timeout is exceeded.
- **Operator UI.** `Durababble::OperatorApp` is a read-only Rack-compatible operator UI prototype that can be mounted by a host Rails/Rack app behind its own authentication middleware. `docs/operator-web-ui.md` specifies the broader target operator-facing web UI for workflow/object inspection, progress views, lease/outbox/wait diagnostics, and management actions such as cancel, terminate, retry/resume, and pause/resume. Most mutating management APIs remain follow-up work.
- **Coverage thresholds.** The suite uses SimpleCov line and branch coverage thresholds for the library.

## Programming model

Durababble's user-facing model is two Ruby base classes over shared durable coordination machinery:

| Primitive        | Current class               | Public direction                          | Best for                                                                              | Mental model                                         |
| ---------------- | --------------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Durable workflow | `Durababble::Workflow`      | `Workflow.start` / `Workflow.handle`      | one-off processes with a start, result, steps, waits, and recovery                    | function/object that survives restarts               |
| Durable object   | `Durababble::DurableObject` | `DurableObject.at` / `DurableObject.tell` | sessions, carts, conversations, agents, per-shop workers, or other id-addressed state | SQL-backed actor/mailbox object with a current owner |

The home spec called the workflow class `Durababble::DurableWorkflow`; the repo uses `Durababble::Workflow`. Unless renamed later, `Durababble::Workflow` is the in-repo class name.

Workflow and object calls compose. A workflow can call a durable object. A durable object command can start or signal workflows once the public start/signal APIs exist. Target worker-pool semantics say child calls inherit the caller's worker pool unless explicitly overridden.

### Current workflow API

A workflow class subclasses `Durababble::Workflow` and implements `#execute(input)`. `#execute` should be deterministic orchestration code. Any method that performs durable side effects must be declared as a workflow step:

```ruby
class FulfillOrder < Durababble::Workflow
  def execute(order)
    payment = charge_card(order)
    ship(order, payment)
  end

  step retry: { maximum_attempts: 5, schedule: [1, 5, 30] }
  def charge_card(order)
    Payments.charge(order, idempotency_key: step_context.idempotency_key)
  end

  step def ship(order, payment)
    Shipping.buy_label(order, idempotency_key: step_context.idempotency_key)
  end

  expose_command def note(message:)
    # Transitional implementation: Workflow.ref(run_id).note(message:)
    # records a durable command event today.
    message
  end

  expose def description
    self.class.workflow_name
  end
end
```

`Workflow.enqueue(input, store:)` creates a durable pending execution today. `Workflow.start(input, store:)` creates a durable execution and returns a handle; `Workflow.handle(workflow_id, store:)` / `Workflow.ref(workflow_id, store:)` returns a handle for management calls. `Engine#run(WorkflowClass, input:)` remains the inline convenience path for tests and scripts. `Worker` accepts workflow classes, not old DSL workflow instances. The legacy block DSL is intentionally removed.

`step_context` is available only while a workflow step is executing. It contains `workflow_id`, `step_index`, `attempt_number`, `idempotency_key`, and `heartbeat`. Idempotency keys are generated from durable coordinates and are stable across retries of the same logical step.

Workflow exposed commands are currently represented as durable event signals named `workflow:<workflow_id>:command:<method>`. That makes command delivery durable and replay-friendly, but does not yet execute the command method body on the workflow lease owner or return a command result to the caller.

### Cooperative workflow cancellation

Temporal's cancellation model separates a cancellation request from hard termination: cancellation is delivered cooperatively into workflow execution, workflow code may catch the cancellation exception and run cleanup/compensation, child work follows explicit cancellation policy, and cleanup errors can cause the workflow to fail instead of becoming canceled. Durababble adopts the same shape while keeping the prototype's method/order step model and SQL persistence.

Implemented Durababble semantics:

- `Workflow.handle(workflow_id, store:).cancel(reason:)` is the first-class cancellation API. It stores the first reason plus request/delivery timestamps on the workflow row; duplicate requests return the current run and preserve the first recorded reason.
- Canceling a terminal `completed`, terminal `failed`, or already `canceled` workflow is idempotent and does not pretend cleanup ran. Already `canceled` workflows retain their original cancellation reason.
- Pending, waiting, and retry-backoff workflows move to `canceling`, clear `next_run_at`, and become runnable immediately. Pending waits for the workflow are marked `canceled`, so late timer/event signals cannot resume the old wait.
- Running workflows keep their live lease. Cancellation is observed at deterministic yield points: before starting a new step, when replay reaches a completed step boundary, after a step completes, and when a running step heartbeats.
- Delivery raises `Durababble::CancellationError` with the durable reason and workflow id. Once the error has been raised in an execution attempt, cleanup steps can run normally; the same durable request is re-delivered after crash/recovery so cleanup code can resume from already-completed steps.
- If workflow code catches cancellation and returns after cleanup, the engine records the workflow as `canceled` and stores the cleanup result. Re-raising `CancellationError` also records `canceled`. If cleanup raises an unrelated error, ordinary step retry policy applies; exhausted or non-retryable cleanup failures mark the workflow `failed`.
- Durababble does not yet have first-class child workflows. When child workflow APIs are added, cancellation must use explicit child-cancellation policy instead of implicitly terminating child work, and parent cleanup must not report `canceled` until the chosen child policy has reached a durable outcome.
- Cooperative cancellation is not operator termination. A future termination API may stop a run without executing cleanup, but it must use a distinct state/surface and must not report that cooperative cleanup completed.

### Target workflow API requirements

The target workflow surface is:

- `Workflow.start(...)` starts a workflow and returns a handle.
- Idempotent start accepts caller-provided `id:` or `idempotency_key:` and returns the same handle for the same worker pool, class, and arguments; same key with different shape raises `IdempotencyKeyConflict`.
- `Workflow.handle(workflow_id)` returns a query/management handle supporting status/result/cancel/resume, signals, and exposed transient methods.
- `handle.signal(:name, **args, idempotency_key:)` commits a durable inbox message before returning and fails for terminal workflows.
- `signal def handler` declares deterministic workflow signal handlers.
- `Durababble::Workflow.wait_condition(timeout: nil) { ... }` blocks a workflow fiber until the condition is true or a durable timeout fires.
- `Durababble::Workflow.sleep(duration)` and `sleep_until(time)` are durable workflow sleeps.
- Workflow-local deterministic futures are future scope, but if added they must preserve method/order step identity and stable replay order.
- `patched(patch_id)` is the target near-term API for cross-deploy workflow control-flow compatibility. It records/checks a durable patch marker before the new branch emits steps, waits, or signals.
- `deprecate_patch(patch_id)` is the cleanup API after no live workflows still need the old branch. It keeps replay compatibility while allowing the old branch to be removed before final marker removal.
- Numeric `version(change_id, default:, max:)` can remain future scope if a concrete need appears, but the preferred target is the simpler Temporal-style boolean patch marker.

### Current durable object API

A durable object class subclasses `Durababble::DurableObject`. It is currently addressed by `Class.ref(id, store:)`; public direction is `Class.at(id, worker_pool: nil, idempotency_key: nil)` for proxy calls and `Class.tell(...)` for async durable commands. Durable object methods are not workflow steps. Public query methods are declared with `expose`; public serialized mutating commands are declared with `expose_command`:

```ruby
class Account < Durababble::DurableObject
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
```

The durable-object contract is that `expose_command` commands serialize through the durable object's identity, run with `command_context`, and may update state with `update_state(new_state)`. `expose` queries read latest persisted state and should not mutate state. `command_context.idempotency_key` is generated by Durababble and is stable for the durable command. The current inline prototype records command rows and executes the command immediately; enforcing one-at-a-time object command leasing through the unified inbox is required before calling the durable-object runtime complete.

### Target durable object API requirements

- Address objects through `DurableObject.at(id, worker_pool: nil, idempotency_key: nil)`. Existing `ref` can be kept as a compatibility alias or lower-level API, but `at` is the intended public spelling.
- `expose def` registers transient non-durable owner-local methods. They must not enqueue inbox messages, call `step`, mutate durable state, schedule sleeps, or write Durababble tables. Passing `idempotency_key:` to an exposed transient method raises.
- `expose_command def` registers durable mailbox commands. Commands enter the unified inbox and execute in strict per-target FIFO order. Synchronous asks and asynchronous tells share the same mailbox, so asks cannot overtake earlier tells.
- `Object.tell(id, :method, **args, idempotency_key: nil)` enqueues a durable fire-and-forget command. It validates that the target method is an `expose_command`.
- Commands, scheduled wakeups, and other mailbox work acquire an exclusive writer slot. Exposed transient methods acquire shared read access, can run concurrently with each other, and stop entering once mailbox work is waiting.
- If the mailbox head is waiting for backoff, paused, dead-lettered, or otherwise blocked, later messages for the same target must not run.
- A command's inbox row owns a stable `operation_id`; any `step` checkpoints inside that command use the operation id so retries skip completed side-effect steps.
- `on_create`, `on_load(prev_schema_version:, prev_dump:)`, `on_wake(payload: nil)`, and `on_destroy` are lifecycle callbacks. They are not remotely callable public methods.
- Class-level `schema_version` for durable object state is deferred future scope, clarified below. If implemented later, `on_load` handles forward migrations after deserialization and before exposed user code runs.
- Optional `attribute :name, Type, default:, null:` accessors should map to an opaque Paquito-serialized object state blob by default, with future optional indexed/generated-column support.
- Object `sleep_until(at:, payload: nil)` atomically replaces the pending sleep row for that object in the same transaction as the command state write. `cancel_sleep` removes it. Matured sleeps convert to mailbox `wake` messages before the sleep row is removed.
- Management operations should exist for operator use: `list`, `find`, `pause`, `resume`, `cancel`, `destroy!`, `evict`, and explicit `relocate_worker_pool`.

No `.with(id) { ... }` block API is part of v1. Multi-method atomicity should be expressed by writing one command method that does the whole operation.

### Typing

Durababble's runtime does not load or validate user RBS. The gem ships `sig/durababble.rbs` with `Durababble::Workflow[Input, Output]` and `Durababble::DurableObject[Id, State]` generics for static tooling only; runtime serialization remains Paquito-based.

## Idempotency contract

Current implemented idempotency surfaces:

- Step idempotency keys are generated from workflow id + logical step position and are available through `step_context.idempotency_key`.
- Durable object command idempotency keys are generated from object type + object id + command id and are available through `command_context.idempotency_key`.
- `Store#with_fence` deduplicates side-effect blocks by a workflow-local fence key while the owner completes or fails normally.
- Outbox uniqueness uses message keys.

Target public-entry idempotency requirements:

- Every public durable entry point accepts `idempotency_key:` where durability is implied: workflow starts, object asks, object tells, workflow signals, and operator APIs exposed externally.
- Idempotency keys are scoped to worker pool, target, operation kind, method, and argument fingerprint, not just target id.
- Same key + same operation shape returns the existing handle/result or re-raises the saved error. Same key + different operation shape raises `Durababble::IdempotencyKeyConflict`.
- Transient `expose` methods are not durable and must not accept idempotency keys.
- Caller timeout after a durable command/signal/message commits does not cancel the durable work. Retrying with the same idempotency key reattaches to the same row.
- Retention for completed idempotency/inbox rows must be specified. The home spec proposed daily sweepers with default 30-day retention but left per-class retention as a question.

## Storage and schema requirements

### Current in-repo schema

The implemented schema is intentionally smaller than the target schema. Current tables are:

| Table                     | Purpose                                                                            | Current key shape          |
| ------------------------- | ---------------------------------------------------------------------------------- | -------------------------- |
| `workflows`               | one workflow execution; status, input/result/error, workflow lease, retry due time, and cooperative cancellation metadata | `id`                       |
| `steps`                   | latest logical workflow step state by deterministic position                       | `(workflow_id, position)`  |
| `step_attempts`           | append-only attempt history for steps and waits                                    | `id`                       |
| `waits`                   | durable timer/event waits                                                          | `id`                       |
| `fences`                  | workflow-local side-effect idempotency fences                                      | `(workflow_id, key)`       |
| `outbox`                  | durable outgoing messages with processing leases                                   | `id`, unique `key`         |
| `durable_objects`         | latest object state by object type/id                                              | `(object_type, object_id)` |
| `durable_object_commands` | transitional persisted durable object command rows                                 | `id`                       |

The PostgreSQL/YSQL adapter uses the selected namespace as a schema. The MySQL/MariaDB adapter prefixes table names with the selected namespace because MySQL has database/table namespace differences. If callers do not pass a `schema:` argument, the selected namespace is `DURABABBLE_SCHEMA` when set, otherwise `Durababble.workspace_schema(DURABABBLE_WORKSPACE_ROOT || Dir.pwd)`. MySQL columns use `VARCHAR`, `DATETIME(6)`, and `LONGBLOB`; PostgreSQL/YSQL columns use `text`, `timestamptz`, and `bytea`.

Current query-shape requirements:

- Claim paths use `FOR UPDATE SKIP LOCKED` where supported.
- Queue/recovery indexes cover workflow claims, due retries, expired workflow leases, event waits, timer waits, step-attempt lookup, outbox claims, and object-command status scans.
- Runtime value decoding must only decode known serialized binary columns.
- PostgreSQL/YSQL migrations must preserve legacy JSONB values when converting runtime columns to Paquito bytes.
- MySQL/MariaDB and PostgreSQL/YSQL must pass the same store backend conformance suite.

### Target schema requirements

These requirements are not yet reflected in the current schema, but they are the direction unless later implementation evidence changes them:

- Persist immutable `worker_pool` on durable targets whose routing/claiming/scheduling semantics are pool-scoped.
- Include `worker_pool` in primary keys and indexes when query patterns need to filter, route, claim, recover, or list by worker pool. Do not add it to keys where query patterns do not care about worker pool.
- Split workflow/object ownership into a unified leases table keyed by the pool-scoped target identity, with `node_id`, `rpc_address`, `lease_token`, and `lease_until`.
- Add a node registry table keyed by the pool/node identity with `rpc_address`, advertised pools, draining flag, and heartbeat time. Durable class data-version capabilities are deferred future scope.
- Replace/evolve `durable_object_commands` and `signal_event` command-event rows into a unified inbox for object asks/tells/wakes and workflow signals/commands.
- Add per-target mailbox sequence state so `enqueue_message` allocates a monotonic sequence and target executors drain only a contiguous ready prefix from the head.
- Add object sleep rows keyed by object identity plus worker pool when sleep dispatch is pool-scoped, with `sleep_id`, `wake_at`, and Paquito payload.
- Add append-only workflow history marker rows for `patched` / `deprecate_patch`. This can start as a narrow patch-marker event log and later fold into a full workflow history/inbox table; it must be ordered per workflow and portable to both SQL adapters.
- Add explicit idempotency rows for workflow starts and any public durable operation not deduped by the inbox itself.
- Consider whether high-risk transactional pieces (`enqueue_message`, target-head drain/advance, sleep-to-inbox conversion, object state + message completion) should become database functions to reduce lock-order drift.
- Plan retention/partitioning for high-volume history (`steps`, `step_attempts`, `inbox`, idempotency) before production scale.

### SQL portability requirement

The home spec used Postgres/Yugabyte features such as partial indexes, `RETURNING`, `ON CONFLICT`, `BYTEA`, and `gen_random_uuid()`. The repo now supports MySQL/MariaDB, so target schema work must either:

1. define equivalent behavior in the backend abstraction and conformance tests, or
2. explicitly mark a target feature as YSQL-only and keep it out of the common public contract.

Do not silently drop MySQL support when importing home-spec schema ideas.

## Worker pools, leases, routing, and scheduling

### Current worker and recovery behavior

Worker polling is intentionally simple. A worker pool is currently a set of processes repeatedly calling `Durababble::Worker#tick` or `#run_until_idle`; each tick claims at most one runnable workflow whose workflow name is present in the worker registry, then resumes it through `Engine#resume`.

`Durababble::WorkerRuntime` is the preferred app/process lifecycle entrypoint. A Rails initializer can create one runtime per desired pool during boot, keep the returned object, and call `shutdown(timeout: ...)` from the process shutdown hook. Shutdown stops new claims, waits for the active tick, and releases this worker's workflow/outbox leases if the timeout expires. Late/zombie state writes are guarded by lease ownership checks in `Engine`.

Expired leases are either reclaimed directly by `claim_runnable_workflow`/`claim_workflow` or moved back to `pending` by `Store#steal_expired_leases!`, so the current prototype does not require a separate coordinator.

### Target worker-pool requirements

- Every durable target whose execution/routing is pool-scoped has an immutable persisted `worker_pool` selected at first materialization.
- If a class declares a default pool, that pool wins unless the caller overrides while creating a new durable unit. Once the row exists, the persisted pool wins.
- Worker pools are the routing and multiregion boundary. A pod in another pool cannot claim, route, or wake a target unless the target is explicitly relocated.
- `nodes` records which pools each pod serves, its RPC address, draining state, and heartbeat. Advertising durable class data-version capabilities is explicitly deferred future scope.
- Scheduler scans filter by pools served by the current pod.
- `AwakenBatch`, `DeliverMessage`, and `CallTransient` are sent only to nodes in the target pool.
- Automatic cross-pool stealing is forbidden. Regional failover is explicit `relocate_worker_pool` operator/runtime work that quiesces the target, releases the old lease, updates the row, and wakes it in the new pool.
- Tables/indexes only include `worker_pool` in keys when these query patterns actually need it.

### Target sticky placement requirements

Routing should keep hot ids on the pod that already has them in memory:

- Every pod loads or generates a stable `node_id` and advertises `rpc_address = "#{POD_IP}:#{DURABABBLE_RPC_PORT}"`.
- Every acquired lease writes `node_id`, `rpc_address`, and a fresh `lease_token` into the lease row.
- Lease acquisition uses a hot in-memory cache when the owner has a fresh lease and falls back to atomic SQL acquisition/lookup when cold, near expiry, or routed remotely.
- A `LeaseRenewer` refreshes in-flight leases every `lease_ttl_ms / 3` and only when the `lease_token` still matches.
- Cache entries are evicted on near-expiry, `EvictLease`, object CAS conflict, lease-renew failure, idle timeout, or LRU capacity pressure.
- Durable object cache entries include `{instance, lock_version, lease_token, last_used_at, gate}` and must never invoke user code when the lease is expired or near refresh threshold.
- Idle owners stop renewing after `idle_eviction_ms` and release leases so the next pool-local caller can acquire ownership.

## Inter-node RPC protocol

### Current implementation

Current code has two different RPC-ish pieces:

1. `Durababble::RpcClient`: a JSON-line command protocol to a child process. This is useful for local subprocess tests and benchmarks, but it is **not** the target intranode/inter-pod RPC protocol.
2. `Durababble::Rpc::Server` / `Durababble::Rpc::Client`: the real four-method gRPC transport for `AwakenBatch`, `EvictLease`, `CallTransient`, and `DeliverMessage`, using protobuf messages and Paquito-encoded transient args/results. Tests use insecure localhost credentials; production callers provide gRPC credentials from the hosting environment.
3. `Durababble::WorkflowRpc`: lease-aware workflow RPC routing. It can still be tested with injected clients, but cross-node tests now run through `Durababble::Rpc::WorkflowClient`, which calls the gRPC `CallTransient` method on a real `GRPC::RpcServer`.

The current gRPC transport still does not provide the durable inbox implementation or persistent node registry tables. In tests and the prototype, the node directory is an in-memory mapping from node id to `rpc_address`.

### Target gRPC requirement

Actual remote intranode/inter-pod communication must use the full four-method gRPC service over Shopify-standard mTLS/Spiffe. Each pod runs a dedicated `Durababble::RpcServer` using the `grpc` gem's `GRPC::RpcServer`, bound to `rpc_host:rpc_port` (default port `50051`) with its own thread pool. No shared bearer secret is used. Peer identity comes from Spiffe; Durababble additionally authorizes peers through an allowed service-account list.

The service shape is part of the merged target contract:

```proto
syntax = "proto3";
package durababble.v1;

service Durababble {
  rpc AwakenBatch(AwakenBatchRequest) returns (AwakenBatchResponse);
  rpc EvictLease(EvictLeaseRequest) returns (EvictLeaseResponse);
  rpc CallTransient(TransientRequest) returns (TransientResponse);
  rpc DeliverMessage(DeliverMessageRequest) returns (DeliverMessageResponse);
}

message RemoteError {
  string klass = 1;
  string message = 2;
  repeated string backtrace = 3;
}

message LeaseMoved {
  string new_rpc_address = 1;
  string new_node_id = 2;
}

message AwakenBatchRequest {
  string worker_pool = 1;
  repeated string workflow_ids = 2;
}
message AwakenBatchResponse {}

message EvictLeaseRequest {
  string worker_pool = 1;
  string target_kind = 2;  // object | workflow
  string target_class = 3; // empty for workflows
  string target_id = 4;
}
message EvictLeaseResponse {}

message TransientRequest {
  string worker_pool = 1;
  string class_name = 2;
  string object_id = 3;
  string workflow_id = 4;
  string method = 5;
  bytes args = 6;          // paquito-encoded
  int64 deadline_ms = 7;
}

message TransientResponse {
  oneof result {
    bytes ok = 1;          // paquito-encoded
    RemoteError err = 2;
    bool not_running = 3;
    LeaseMoved moved = 4;
  }
}

message DeliverMessageRequest {
  string worker_pool = 1;
  string target_kind = 2;  // object | workflow
  string target_class = 3; // empty for workflows
  string target_id = 4;
}
message DeliverMessageResponse {}
```

Semantics:

- **AwakenBatch** is a latency optimization after workflow starts or matured scheduler rows. It never replaces durable DB state or the scheduler correctness path.
- **DeliverMessage** is a wakeup for already-committed inbox rows. It carries no user payload; the receiver queries durable inbox rows itself. If the receiver no longer owns the lease, it returns success without work and the sender/scheduler re-checks the lease.
- **CallTransient** is non-durable RPC for exposed methods against the current owner. It returns a Paquito result, a remote error, `not_running`, or `LeaseMoved`.
- **EvictLease** asks a pod to drop a cached lease it may no longer own.
- Connection failure to an owner should cause a short retry, lease re-check, and reroute. If wakeup still fails after the retry budget, durable inbox/scheduler recovery remains the correctness path.
- gRPC is required for cross-pod calls because strongly typed protos catch shape drift and mTLS is already available. JSON-line RPC is not acceptable for production intranode communication.

## Durable inbox, signals, and mailbox ordering

The target is a single unified durable inbox:

- Every durable target (object instance or workflow execution) has an inbox.
- Object inboxes are push-driven: the owner pod drains commands/wakes/internal work from the mailbox and invokes registered command/lifecycle methods against the cached instance.
- Workflow inboxes are history-driven: signal rows are accepted into durable workflow history and delivered to `signal` handlers at deterministic yield points so replay sees the same event order.
- Inbox enqueue commits before any gRPC wakeup.
- Sequence allocation and inbox insert are one transaction.
- Target executors drain only the contiguous ready prefix from the mailbox head. `SKIP LOCKED` must not let later messages overtake a blocked head for the same target.
- Object message completion, state write, sleep updates, and mailbox head advancement must be one fenced transaction.
- Workflow signal acceptance and history retention must be idempotent by message id.
- For object targets, `consumed_at` means command completion. For workflow targets, `consumed_at` means accepted into workflow history, not safe to delete until retention expires.
- Ask rows store serialized result or error. Tell/wake rows retry with backoff and move to dead-letter after `max_message_attempts`.
- A dead-lettered or backed-off head blocks later messages until an operator retries, skips, cancels, destroys, or repairs the target.

Current repo status:

- Implemented: `waits`, `Store#signal_event`, workflow event waits, object command rows.
- Partially implemented: workflow `expose_command` durable event recording; object command persistence and inline execution.
- Transitional: `durable_object_commands` and `signal_event` can remain as compatibility/lower-level implementation details during migration, but the target durable message model is one inbox.
- Missing: unified `inbox`, mailbox sequence state, object ask/tell split, `signal def`, `wait_condition`, `DeliverMessage`, per-target FIFO mailbox runtime, dead-letter/repair UX, and retention sweepers.

## Determinism, steps, and code evolution

### Current step semantics

Workflow steps are method-level durable side-effect boundaries. On first execution, the engine records a running step/attempt, runs the method, stores the serialized result, and marks the step completed. On resume, completed rows return cached results and do not re-run the method. If the process crashes after an external side effect but before the checkpoint commits, the step may run again; external systems must use `step_context.idempotency_key`.

No workflow row lock is held while user step code runs. The executor holds a renewable lease and fences durable writes with current lease ownership. If the lease is lost while activity code is running, the external activity may still finish, but the checkpoint/status write fails and recovery follows the normal idempotent retry path.

### Step retry policy details

Workflow steps are retriable at the step boundary rather than by blindly rerunning whole workflows. The DSL is intentionally close to Temporal's Activity Retry Policy, but shaped as Ruby keyword arguments at the step method definition site:

```ruby
class ImportWorkflow < Durababble::Workflow
  def execute(input)
    download(input)
  end

  step retry: {
    initial_interval: 1,
    backoff_coefficient: 2.0,
    maximum_interval: 100,
    maximum_attempts: 5,
    non_retryable_errors: [ArgumentError]
  }
  def download(input)
    # use step_context.idempotency_key and step_context.heartbeat here
  end
end
```

`schedule: [1, 5, 30]` may be supplied for an explicit per-retry schedule; after the explicit array is exhausted, Durababble falls back to capped exponential backoff. Intervals are numeric seconds. `maximum_attempts:` counts the first execution plus retries. `non_retryable_errors:` accepts Ruby exception classes or class-name strings.

On a retryable failure, `Engine` records the current step attempt as failed, sets the workflow back to `pending`, clears `locked_by`/`locked_until`, and stores `next_run_at`. `claim_runnable_workflow` ignores pending/failed workflows whose `next_run_at` is still in the future, and only treats `failed` rows as retryable when `next_run_at` is non-null and due, so retry delay survives process restarts and terminal failures remain terminal. On the final failure, or for a non-retryable error, the workflow itself is marked `failed` with `next_run_at` cleared and the error bubbles to workflow state.

### Method/order step identity

Method/order-based step identity is the preferred in-repo approach:

- Step positions are assigned by deterministic method execution order.
- Method names are metadata and guardrails; users do not name steps at call sites.
- Replay skips completed positions and uses method-name metadata to detect surprising code drift where practical.
- Future workflow concurrency/futures must preserve deterministic method/order semantics with explicit branch keys only where needed for stable fan-out, not by changing the basic step API to `step(:name) { ... }`.

### Patched workflow event-log checker

Status: **Target**. This is not implemented yet.

Durababble should add a Temporal-inspired patch marker API for safe workflow code evolution before adding heavier class-level state migration or capability routing. It solves a different problem from serialized data migrations: a workflow code deploy can change deterministic orchestration control flow, step order, waits, or signal handling. Existing executions must keep following the history they already produced, while new executions and executions that have not yet reached the change point can take the new branch.

Target API:

```ruby
class FulfillOrder < Durababble::Workflow
  def execute(order)
    if patched("2026-05-ship-after-tax")
      quote_tax(order)
      ship(order)
    else
      ship(order)
      quote_tax(order)
    end
  end
end
```

The canonical public spelling is `patched(patch_id)`. A Ruby-idiomatic `patched?(patch_id)` alias is acceptable, but docs should use `patched` to match Temporal terminology. `deprecate_patch(patch_id)` is the cleanup helper after the old branch no longer has any live histories.

Rules:

- `patch_id` is a stable, non-empty string unique to one logical code change. Do not reuse a removed id for a later unrelated change.
- `patched` is only valid in deterministic workflow orchestration code: `execute`, future signal handlers, and future `wait_condition` predicates/continuations. It must not be called inside `step` bodies, durable object commands, exposed transient methods, or arbitrary library code outside an active workflow execution. Calling it inside a step should raise a typed deterministic/runtime error because completed step bodies are not replayed.
- The first `patched(patch_id)` call for a workflow execution is event-bearing. Later calls with the same id in the same execution return the memoized decision and must not append duplicate marker rows.
- When executing live code at a history point that has no persisted event yet, `patched(patch_id)` appends a normal patch marker and returns `true`. The marker commit must happen before the new branch produces steps, waits, signals, or other durable workflow events.
- When replaying/checking a history that already contains a normal marker for `patch_id`, `patched(patch_id)` consumes that marker and returns `true`.
- When replaying/checking history produced by old code that reached the change point without a marker, `patched(patch_id)` returns `false` and appends nothing, so code runs the old branch and matches the existing step/wait history.
- If persisted history contains a normal patch marker that current code does not consume with `patched` or `deprecate_patch`, the checker raises a nondeterminism error before any further durable writes. The same applies to patch-id mismatches and out-of-order marker consumption.
- Patch markers are workflow-history markers, not user-visible inbox messages and not serialized state schema versions. They must not route work to different nodes by capability.

Target event-log/checker model:

- Add a per-workflow ordered history checker around deterministic durable workflow boundaries. Existing `steps` rows remain the source of completed step results; the event log is the deterministic skeleton that says which branch/checkpoint sequence the workflow produced.
- Initial storage can be a narrow `workflow_patch_events` / `workflow_history_events` table with `workflow_id`, monotonic `event_index`, `kind` (`patch`, `patch_deprecated`, later `step`, `wait`, `signal`), `patch_id` or event key, optional metadata bytes, and timestamps. The design must work on PostgreSQL/YSQL and MySQL/MariaDB without relying on partial-index-only behavior.
- A `WorkflowHistoryChecker` cursor compares calls made by current code with persisted history. Step calls and waits should eventually participate in the same cursor so removing a `patched` call in front of an existing marker fails immediately rather than drifting into method/order mismatch later.
- Marker append and checker reads must be fenced by the current workflow lease. A worker that lost the lease must not append or consume patch markers while committing later workflow state.
- Crash after marker commit but before the first new-branch step is safe: replay sees the marker, `patched` returns `true`, and the new branch resumes. Crash before marker commit is also safe: no branch output was committed, so the retry can append the marker and take the new branch unless old history already forces `false`.
- Admin/observability surfaces should expose patch usage by workflow type/id: normal marker, deprecated marker, and open workflows with no marker for a given patch id. A conservative cleanup gate is “no open workflow of this type lacks the patch marker” before deleting the old branch.

Patch lifecycle:

1. **Introduce patch:** deploy `if patched("id") { new } else { old }`. New or not-yet-reached executions record a marker and run the new branch; already-past executions without a marker run the old branch.
2. **Deprecate patch:** after no live workflows still need the old branch, deploy `deprecate_patch("id")` plus the new code only. This keeps marker-aware histories replayable while removing the old branch.
3. **Remove marker call:** after all relevant histories have completed and aged out of retention, remove `deprecate_patch("id")`. Never reuse `id`.

Required tests for the eventual implementation:

- First execution records a marker before the first new-branch step and returns `true`.
- Replay/resume with a marker returns `true` and skips/replays the new branch deterministically.
- Replay/resume of old history with no marker returns `false` and follows the old branch.
- Removing a required `patched` call while normal marker histories still exist raises a nondeterminism error before durable writes.
- `deprecate_patch` allows the old branch to be removed and later allows the marker call to be removed after retention.
- Duplicate calls with the same id are memoized; accidental id reuse across unrelated code points is rejected or at least surfaced by checker/test tooling.
- The same behavior passes shared backend conformance on PostgreSQL/YSQL and MySQL/MariaDB, plus a subprocess crash test around marker commit.

### Future determinism scope

The home spec's stricter deterministic workflow runtime is future scope, not a near-term gate:

- Per-execution `Ruby::Box` realms derived from a template box.
- Deterministic definitions for common nondeterministic Ruby entry points (`Time.now`, `Date.today`, `Kernel#sleep`, randomness, UUIDs) inside the box.
- Illegal external I/O outside `step` raising `Durababble::DeterminismError`.
- A safe host-realm activity trampoline so side-effecting `step` code runs with normal host Ruby semantics.
- Paquito-only values crossing host/box boundaries.
- Workflow-local deterministic futures/fibers that do not depend on host scheduling.
- Numeric workflow version markers for cross-deploy control-flow compatibility beyond the boolean `patched` model, if a concrete need appears.

Do not make `RUBY_BOX=1` or `Ruby::Box.enabled?` a startup requirement for the current prototype.

### Class-level data schema versioning

Status: **Future scope / explicitly deferred**. This refers to serialized durable data shape, not SQL DDL. Do not implement this as part of the first gRPC/inbox work:

- Durable object state may have a class-level `schema_version` and `on_load(prev_schema_version:, prev_dump:)` migration hook.
- Workflow args/results/errors may similarly carry a class/code data version so long-lived executions can be loaded by compatible code.
- Node capability routing can advertise which durable classes and data versions a pod can serve, allowing callers to avoid routing future-version state to old pods.
- A future-schema read should fail closed with a typed error such as `FutureSchemaVersionError` rather than corrupting state.

This is intentionally later than the `patched` event-log checker. Patch markers handle workflow control-flow compatibility across deploys; they do not migrate serialized object/workflow data or route by data-version capability.

## Configuration requirements

Current configuration is minimal and mostly constructed directly through `Store.connect`, `Engine`, `Worker`, and `WorkerRuntime` arguments.

Target configuration imported from the home spec:

```ruby
Durababble.configure do |c|
  c.connection = :durababble
  c.node_id = SecureRandom.uuid
  c.worker_pools = ENV.fetch("DURABABBLE_WORKER_POOLS", "default").split(",")
  c.default_worker_pool = "default"
  c.allowed_peer_service_accounts = ["agent-server"]
  c.rpc_host = ENV.fetch("POD_IP")
  c.rpc_port = ENV.fetch("DURABABBLE_RPC_PORT", "50051").to_i
  c.rpc_thread_pool_size = 32
  c.scheduler_tick_ms = 3_000
  c.scheduler_tick_jitter_pct = 25
  c.scheduler_batch = 50
  c.workflow_recovery_attempts = 50
  c.lease_ttl_ms = 30_000
  c.lease_refresh_threshold_ms = 10_000
  c.idempotency_wait_timeout_ms = 5_000
  c.idle_eviction_ms = 5 * 60_000
  c.cache_capacity = 5_000
  c.max_cached_workflows = 1_000
  c.shutdown_grace_s = 30
  c.max_workflow_args_bytes = 4 * 1024 * 1024
  c.warn_workflow_args_bytes = 1 * 1024 * 1024
  c.max_step_output_bytes = 4 * 1024 * 1024
  c.warn_step_output_bytes = 1 * 1024 * 1024
  c.max_object_state_bytes = 4 * 1024 * 1024
  c.warn_object_state_bytes = 1 * 1024 * 1024
  c.observability = MyApp::Observability::Durababble
end
```

Size guards are hard requirements for production: warn at configured thresholds and raise `Durababble::PayloadTooLarge` when serialized workflow args, step outputs, or object state exceed max bytes. v1 does not spill to blob storage.

## Observability and operations

Current repo status:

- Prototype CLI supports migration, counter workflow run/resume, inspection, and version output.
- Benchmarks record operation latency/throughput/allocation reports for workflow queueing, leases, waits, outbox, fences, and local command RPC.
- Production metrics/tracing/admin UI are not implemented.

Target requirements:

- Built-in admin/status surfaces for listing/finding workflows and objects, with bounded pagination.
- StatsD counters/timers for command ask latency, exposed method latency, mailbox queue/execution latency, `CallTransient`, step execution, replay frequency, recovery sweeps, sleep dispatch, lease acquisitions/forwardings/takeovers, lease-cache hit ratio, and object-cache hit ratio.
- OpenTelemetry spans around public calls, workflow executions, steps, scheduler ticks, and inbound gRPC requests. Spans must include worker pool, class, target id, and lease owner.
- Bugsnag/error integration for unhandled exceptions inside commands, exposed methods, steps, and gRPC handlers.
- Slow-step warnings.
- Routing health metrics for wakeup error rate, wakeup latency, and lease takeover frequency.
- Circuit breakers around database connections; public methods should raise a typed error such as `Durababble::CircuitBreakerOpen` when the durable store is unavailable before commit.
- gRPC server health metrics for in-flight requests, handler-thread saturation, and dropped requests.
- Operator actions for dead-lettered mailbox heads: retry now, skip, cancel target, destroy target, and repair/decode failed payloads.

## Guarantee matrix

| Guarantee                                               | Status                                             | Implementation / target                                                                                                                                                    | Explicit test expectation                                 |
| ------------------------------------------------------- | -------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| Workflows are durable before execution                  | Implemented                                        | `Store#enqueue_workflow` inserts `pending` rows with Paquito-serialized input                                                                                              | complete spec guarantee + crash matrix                    |
| Runnable work is claimable by one worker at a time      | Implemented                                        | `Store#claim_runnable_workflow` atomically updates one row and uses `FOR UPDATE SKIP LOCKED` where available                                                               | backend conformance + hardening concurrency specs         |
| Resume honors lease ownership                           | Implemented                                        | `Engine#resume` uses `Store#claim_workflow` and raises `LeaseConflict` for another live owner                                                                              | hardening lease spec                                      |
| Active leases can be heartbeated                        | Implemented                                        | `Store#heartbeat` extends `locked_until` only for the owning worker                                                                                                        | complete spec guarantee matrix                            |
| Running steps can explicitly heartbeat progress         | Implemented                                        | `Heartbeat#record(cursor)` extends the workflow lease and stores an opaque Paquito cursor on the step/attempt                                                              | heartbeat spec + DST cursor recovery scenario             |
| Heartbeat cursors survive recovery                      | Implemented                                        | `Engine#resume` passes the previous incomplete attempt's heartbeat cursor into the next invocation                                                                         | heartbeat spec + DST cursor recovery scenario             |
| Zombie workers cannot renew expired leases              | Implemented                                        | heartbeat updates only when owner/deadline still match                                                                                                                     | heartbeat spec                                            |
| Zombie workers cannot complete after lease revocation   | Implemented                                        | `Engine` re-checks workflow lease ownership before terminal durable writes                                                                                                 | worker lifecycle spec                                     |
| Step retries are durably scheduled                      | Implemented                                        | failed retryable steps store `next_run_at` and release the lease                                                                                                           | step retry spec + DST retry scenario                      |
| Cancellation requests are durable and idempotent        | Implemented                                        | `workflows.cancel_requested_at` / `cancel_reason` record the first request on the workflow row; duplicate `handle.cancel` calls preserve it                                | workflow cancellation spec                                |
| Cancellation cleanup is replay-safe                     | Implemented                                        | cancellation is delivered at yield points; cleanup runs as ordinary steps and completed cleanup steps are skipped after crash/recovery                                      | workflow cancellation spec + DST cancellation scenario    |
| Retry options are Temporal-like but Ruby-shaped         | Implemented                                        | `initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`, `schedule`, `non_retryable_errors`                                                      | retry policy specs                                        |
| Final retry failure bubbles to workflow                 | Implemented                                        | exhausted/non-retryable step failure marks workflow `failed`                                                                                                               | step retry spec                                           |
| Expired leases can be recovered                         | Implemented                                        | `Store#steal_expired_leases!` returns expired `running` workflows to `pending`                                                                                             | complete spec guarantee + crash matrix                    |
| Completed steps are not re-executed on resume           | Implemented                                        | `Engine#resume` reconstructs context from completed step rows and skips them                                                                                               | complete spec guarantee + subprocess crash harness        |
| Incomplete steps are retried                            | Implemented                                        | non-`completed` step rows are not skipped                                                                                                                                  | crash matrix                                              |
| Step attempts are append-only                           | Implemented                                        | `step_attempts` records every started attempt and terminal status                                                                                                          | guarantee matrix                                          |
| Waiting attempts complete when signaled                 | Implemented                                        | wait completion updates attempts from `waiting` to `completed`                                                                                                             | wait-attempt spec                                         |
| Timer waits survive process exit                        | Implemented                                        | `waits` rows store timer wake time and context                                                                                                                             | timer/event tests                                         |
| Event waits survive process exit                        | Implemented                                        | `waits` rows store event key and context                                                                                                                                   | timer/event + crash matrix                                |
| Signaled waits resume with payload                      | Implemented                                        | `signal_event` completes waiting step with payload                                                                                                                         | timer/event test                                          |
| Concurrent signalers wake a wait once                   | Implemented                                        | `signal_event` completes pending waits via a locked update                                                                                                                 | event concurrency spec                                    |
| Side effects can be fenced by key                       | Implemented with boundary                          | `with_fence` inserts `running` before yield; owner crash recovery is future work                                                                                           | fence concurrency spec; missing owner-crash spec          |
| Outbox delivery is durable and leased                   | Implemented                                        | outbox rows are unique by key, claimable, acknowledgeable, and reclaimable after expiry                                                                                    | outbox specs                                              |
| Workflow RPCs route to current lease holder             | Implemented for workflow transient RPC             | `WorkflowRpc::Router`/`Handler` validate owner before/after handling, refresh ownership after transport failures, and reroute; `Rpc::WorkflowClient` routes over real gRPC | workflow RPC spec + gRPC transport spec + DST scenarios   |
| Actual inter-pod RPC uses full four-method gRPC service | Implemented transport; production security pending | `Durababble::Rpc::Server` serves all four proto methods with injectable credentials/auth callbacks                                                                         | gRPC integration/contract tests + DST response scenarios  |
| Multi-row state transitions are transactional           | Implemented for current workflow/wait/outbox paths | step start/finish/failure and wait transitions run in DB transactions                                                                                                      | implementation + regression suite                         |
| Runtime values are not stored as JSONB                  | Implemented                                        | Paquito bytes in `bytea` / `LONGBLOB`                                                                                                                                      | store storage + legacy migration specs                    |
| MySQL/MariaDB honors common store semantics             | Implemented                                        | `MysqlStore` with shared conformance tests                                                                                                                                 | backend conformance spec                                  |
| Durable object query/command API exists                 | Partially implemented                              | current `ref`, `expose`, `expose_command`, command rows, inline command execution; public target is `at/tell`                                                              | durable object specs                                      |
| Object commands are per-id FIFO and worker-driven       | Target                                             | unified inbox/mailbox + object lease owner + writer gate                                                                                                                   | missing                                                   |
| Object sleeps convert to durable wake messages          | Target                                             | sleeps table + sleep-to-inbox conversion                                                                                                                                   | missing                                                   |
| Workflow signals are durable ordered history            | Target                                             | inbox rows accepted into workflow history and replayed at yield points                                                                                                     | missing                                                   |
| Workflow patch markers guard code evolution             | Target                                             | `patched` / `deprecate_patch` append and check ordered workflow history markers before branch side effects                                                                 | missing; needs unit, backend-conformance, and crash tests |
| Transient exposed methods route to owner                | Target                                             | `CallTransient` gRPC against object/workflow owner                                                                                                                         | missing                                                   |
| Worker pool scopes persisted targets and relevant keys  | Target                                             | persist/include `worker_pool` where query patterns require it                                                                                                              | missing                                                   |
| Workflow boxes isolate deterministic shims              | Future scope                                       | `Ruby::Box` template + execution boxes                                                                                                                                     | missing, not near-term gate                               |
| Method/order-based step identity is preferred           | Implemented / target                               | method declaration + deterministic execution position, not `step(:name)` call-site API                                                                                     | existing workflow specs + future concurrency tests        |
| Unified inbox is the durable message model              | Target                                             | converge object commands, object wakes, workflow signals/commands                                                                                                          | missing                                                   |
| CLI can operate the prototype                           | Implemented                                        | executable supports migrate/run/inspect/resume/version                                                                                                                     | CLI spec                                                  |

## Crash matrix

| Crash point                                                                | Expected recovery                                                                               | Status / explicit test                                                 |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| After enqueue, before claim                                                | Later engine/worker can run the pending workflow                                                | implemented; crash matrix                                              |
| After lease claim, before step start                                       | Lease expiry returns workflow to pending; another worker completes it                           | implemented; crash matrix                                              |
| After step start, before step completion                                   | Step remains incomplete/running; recovery retries it                                            | implemented; crash matrix                                              |
| After step heartbeat, before step completion                               | Latest heartbeat cursor is available to next invocation                                         | implemented; heartbeat spec + DST                                      |
| After step failure, before retry due time                                  | Retry schedule persists; workflow is not claimable early                                        | implemented; retry spec + DST                                          |
| After step completion, before workflow completion                          | Completed step is skipped and remaining work continues                                          | implemented; subprocess crash harness                                  |
| After cancellation cleanup step completes, before canceled terminal write  | Completed cleanup step is skipped and workflow finishes `canceled` on recovery                  | implemented; workflow cancellation spec                                |
| While waiting for an event                                                 | Wait row survives; signal wakes workflow and execution continues                                | implemented; crash matrix                                              |
| While waiting when cancellation is requested                               | Wait row/attempt are marked canceled; cleanup runs on next claim and late signals are ignored   | implemented; workflow cancellation spec + DST cancellation scenario     |
| After outbox insert, before delivery                                       | Outbox message remains claimable exactly once at a time                                         | implemented; crash matrix                                              |
| After outbox claim, before ack                                             | Expired outbox lease can be reclaimed by another sender                                         | implemented; outbox recovery spec                                      |
| During lease-routed workflow RPC                                           | Receiver rejects stale/moved/shutdown/no-owner states; caller refreshes or fails by policy      | partially implemented; workflow RPC spec + DST                         |
| During app shutdown with in-flight step                                    | Runtime stops new claims; timeout releases leases; later worker retries                         | implemented; worker lifecycle spec                                     |
| Crash after inbox row commits before `DeliverMessage`                      | Inbox row remains; scheduler safety-net wakes target later                                      | target; missing unified inbox                                          |
| Crash before inbox row commits                                             | No message row; caller retry decides whether to enqueue                                         | target; missing unified inbox                                          |
| Crash while allocating mailbox sequence                                    | Transaction rolls back or commits both sequence advance and inbox row                           | target; missing mailbox sequence                                       |
| Crash while object command runs before first step                          | Inbox head remains unconsumed; new owner reruns command after lease expiry                      | target; current inline command path not sufficient                     |
| Crash after object command step completion before state/message completion | Step output is cached; command replays and persists state/result once                           | target; object command steps/mailbox missing                           |
| Crash after object state persists before ask result completion             | State persist and message completion must be atomic so this split state is impossible           | target; missing mailbox transaction                                    |
| Crash while head message is in backoff/dead-letter                         | Later messages remain blocked until operator action                                             | target; missing dead-letter runtime                                    |
| Crash during object sleep-to-inbox conversion                              | Either sleep row remains or wake inbox row exists; no wake lost                                 | target; missing object sleeps                                          |
| Crash while workflow signal is accepted into history                       | Accepted signal is replayed in sequence                                                         | target; missing signal history                                         |
| Crash after patch marker commit before first new-branch step               | Replay sees marker, `patched` returns true, and the new branch continues                        | target; missing patch event-log checker                                |
| Code removes `patched` while normal marker history still exists            | Checker raises nondeterminism before any later durable write                                    | target; missing patch event-log checker                                |
| Old pod reads future-version durable data                                  | Old pod releases lease; caller routes to compatible node or receives typed future-version error | deferred future scope; class-level schema capabilities not implemented |
| Owner pod loses lease while activity continues                             | External effect may finish; checkpoint/status write fails; retry uses idempotency               | implemented for workflow lease checks; object target missing           |
| Caller times out waiting for durable ask result                            | Row may keep running; same idempotency key reattaches later                                     | target; missing asks/inbox idempotency                                 |
| Database circuit breaker opens before commit                               | No durable change unless transaction committed; caller receives typed breaker error             | target; missing breaker integration                                    |
| Paquito decode fails for persisted payload                                 | Target/message/workflow moves to operator-visible error/repair state                            | target; repair UX missing                                              |

## Testing and coverage standard

The test suite must keep correctness claims evidence-backed:

- Real PostgreSQL/YSQL integration tests cover storage semantics, migration, leases, waits, outbox, fences, crash/recovery, and query-shape behavior.
- Shared backend conformance tests cover MySQL/MariaDB and PostgreSQL/YSQL behavior equivalence.
- Backend-specific tests must pin SQL behavior that differs by adapter, including lock/claim semantics and EXPLAIN-backed query-plan assertions for hot paths when practical.
- Deterministic simulation tests (DST) are useful for exploring lease/race schedules, but any DST-found storage bug should be pinned by a real backend regression test.
- Subprocess crash harnesses cover real process death around durable boundaries.
- RPC tests must cover stale lease, lease moved, no-active-owner, shutdown/non-running workflow, retry/reroute, gRPC serialization, unavailable-node, timeout, deadline, RST, EOF, lost-response, duplicate-response, auth-failure, wakeup drops/duplicates, and all four service methods.
- Object mailbox tests must cover strict FIFO, blocked head behavior, ask/tell ordering, wake ordering, idempotency conflicts, owner crash, lease takeover, dead-letter, and operator repair paths.
- Workflow signal tests must cover history acceptance, deterministic replay order, timeout behavior, terminal-workflow rejection, and idempotency dedup.
- Workflow patch-marker tests must cover first-run marker recording, old-history `false` branches, marker-history `true` branches, missing-marker nondeterminism failures, `deprecate_patch` cleanup, duplicate-id handling, backend conformance, and crash after marker commit.
- Exposed transient method tests must verify no durable state mutation and deadline/crash semantics. The home spec proposed an `expose_safety_check` helper.
- Observability tests should verify required span/metric labels without depending on a particular vendor backend.

SimpleCov thresholds required by the CI coverage gate:

- global line coverage: 90% minimum
- global branch coverage: 85% minimum
- per-file line coverage: 59% minimum
- per-file branch coverage: 41% minimum

These ratchet thresholds are based on the current CI MySQL suite. They remain below the target of 95% line coverage and 90% branch coverage, but the global line ratchet now enforces the 90% milestone and the global branch ratchet enforces the 85% milestone; meaningful tests should raise the configured minimums as coverage improves, and the minimums must not be lowered without an explicit spec update. The gate is `mise exec -- bundle exec rake test:coverage`. It enables branch coverage, measures library files under `lib/**/*.rb`, excludes tests and non-library support surfaces from the metric, excludes `lib/durababble/version.rb` because Bundler loads that gem metadata before SimpleCov starts, prints the SimpleCov summary in CI logs, and writes the HTML report plus SimpleCov result JSON to `coverage/` for CI artifact upload.

## Prototype boundaries and anti-goals

Current explicit boundaries:

- Fence owner crash recovery is not implemented. A crash after fence acquisition and before fence completion leaves a `running` fence that waiters eventually time out on.
- Worker registry misses are avoided for normal worker polling by claiming only workflow names present in the supplied registry. Enqueuing a workflow name with no corresponding worker pool leaves it pending until an appropriate pool starts.
- Long-running steps do not heartbeat automatically while user code runs.
- CLI coverage is happy-path oriented and not a complete UX/error-contract specification.
- Workflow `expose_command` currently records durable command events; full inbox-routed workflow command execution with return values is future work.
- Durable object command rows and inline execution exist, but per-object queue serialization, lease recovery, and worker-driven object command execution are target behavior.
- Unified inbox, persistent node registry, sticky object placement, object sleeps, workflow signals, patch-marker event checking, metrics/tracing, admin UI, and circuit breakers are not implemented.
- Ruby::Box execution and deterministic fibers are future scope rather than current prototype gates. Numeric workflow version APIs beyond `patched` remain future scope unless a concrete need appears.
- Class-level data schema versioning/capability routing is explicitly deferred future scope.

Carried-forward anti-goals, adjusted for repo decisions:

- No block-form durable object `.with` API in v1.
- No process-wide monkeypatching for determinism.
- Durable objects are not boxed by default.
- No cross-object transactions or full distributed-actor semantics.
- No split-brain tolerance beyond database invariants.
- No durable queue/cron replacement; integrate with an adjacent scheduler/queue if product needs exceed simple durable sleeps/waits.
- No streams API until a real consumer requires it.
- No automatic cross-pool routing; relocation/failover is explicit.
- No silent payload spill to blob storage; oversized values fail loudly.
- MySQL is **not** an anti-goal in this repo despite the original proposal. Any target feature imported from the proposal must respect the common backend contract or explicitly narrow itself.

## Remaining reconciliation questions

No open reconciliation question remains from the previous list. Class-level serialized state migration and node capability routing are deferred future scope. The next target code-evolution mechanism to spec/implement is the Temporal-style `patched` event-log checker described above.
