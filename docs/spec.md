# Durababble specification

This document specifies Durababble's intended durable-execution behavior. It is the contract an implementation must satisfy, not a progress report about intermediate versions.

## Design decisions

These decisions define the intended runtime, storage, and API model:

1. **Public API:** workflows use `Workflow.start` / `Workflow.handle`; durable objects use `DurableObject.at` / `DurableObject.tell`.
2. **Worker-pool keying:** persist `worker_pool` and include it in primary keys/indexes where query patterns route, claim, schedule, list, or recover by worker pool. Do **not** add it gratuitously to keys for tables whose query patterns do not care about worker pool.
3. **Ruby::Box:** workflow execution boxes are outside the v1 contract. The required determinism contract is specified here without making Ruby::Box a startup requirement.
4. **Step identity:** workflow execution uses one Temporal-style ordered command replay model for all workflows. The deterministic workflow executor assigns monotonic command ids as workflow fibers schedule durable operations, and replay validates the same ordered command stream. A workflow with no `async` calls is just the single-fiber, one-command-at-a-time case of this same model. User-provided semantic step keys are optional ergonomics, not the primary identity model. Do not use explicit `step(:name) { ... }` call-site keys as the primary model.
5. **Inbox:** object commands, object wakes, workflow signals, and workflow command events share one unified durable inbox/mailbox model. Inbox rows are durable message/result records, not a second globally polled work queue; ready messages must create or wake a coalesced target activation.
6. **Remote RPC:** real inter-node communication uses the full four-method gRPC service (`AwakenBatch`, `EvictLease`, `CallTransient`, `DeliverMessage`), not a minimal workflow-only subset.
7. **Serialized state migration/capability routing:** class-level serialized data migration and node capability routing are outside the v1 contract. `schema_version` / `on_load` for object state and workflow args/results/errors remain separate from gRPC/inbox semantics.
8. **Workflow patch markers:** Temporal-style `patched(...)` event-log checks are the workflow code-evolution machinery. This is separate from serialized state migration: it protects deterministic workflow control-flow changes by recording/checking patch markers in workflow history.

## Functional spec

- **Ruby library and gem shape.** Durababble is a Ruby 4 gem.
- **Two durable primitives.** Durababble exposes durable workflows for one-off, start-to-finish executions and durable objects for long-lived id-addressed stateful entities.
- **Class-oriented public API.** Durababble uses `Durababble::Workflow` subclasses with `#execute`, `step`, `expose`, and `expose_command`; and `Durababble::DurableObject` subclasses with `expose`, `expose_command`, and explicit `update_state`. Public handles are obtained through `Workflow.start` / `Workflow.handle` and `DurableObject.at` / `DurableObject.tell`.
- **SQL-backed storage.** Storage works through either the PostgreSQL wire protocol (`postgresql://` / `postgres://`, including YugabyteDB/YSQL) or MySQL/MariaDB (`mysql2://` / `mysql://`). The default storage namespace is `DURABABBLE_SCHEMA` when set, otherwise a deterministic `Durababble.workspace_schema(DURABABBLE_WORKSPACE_ROOT || Dir.pwd)` value, so simultaneous worktrees and workers do not share internal tables by accident.
- **Backend conformance.** Both SQL adapters must implement the same durable state-machine semantics. Backend-specific SQL is allowed, but behavior must be proven through shared conformance tests plus backend-specific plan/locking tests when query shape matters.
- **Binary runtime payloads.** Runtime values (`input`, `result`, `context`, `payload`, state, arguments, kwargs, and heartbeat cursors) are serialized with Paquito into binary columns, not JSON/JSONB. PostgreSQL/YSQL stores these columns as `bytea`; MySQL/MariaDB stores them as `LONGBLOB`.
- **Durable workflow rows and step rows.** Workflows are durable before execution. The runtime emits ordered workflow commands for every durable workflow operation. Latest step rows are query state derived from or updated alongside history, not the source of replay truth.
- **Workflow execution model.** All workflow orchestration code runs on a Durababble-managed deterministic scheduler. Raw `Async { ... }` and `Async::Task#async` create additional deterministic workflow fibers when they are called from workflow orchestration code. Code without async fanout runs as the root fiber only. The runtime and replay system must not assume that a workflow will stay single-fiber across code changes. Durable steps called from any workflow fiber emit ordered workflow commands before their side-effecting implementation runs. The default executor is process-local, but step bodies run outside the deterministic workflow scheduler so orchestration fibers can schedule and wait for concurrent work. Durababble must integrate with Async's task lifecycle so workflow execution context propagates to non-transient child tasks; workflow authors must not be forced to use Durababble-specific async helpers to call durable steps from ordinary Async task trees.
- **Workflow command history.** Replay requires an append-only per-workflow command/event history. Step scheduling, step execution starts, and step completions are distinct facts. A scheduled step command records the deterministic command id plus the full replay-relevant command shape before any local or remote executor starts the side effect: step method name, serialized args/kwargs or a stable payload digest, retry/executor attributes, and any semantic key if one is present. A start event records that an executor began a concrete attempt. A completion/failure/wait event resolves the command's workflow future. Replay must validate scheduled command shape even when no completion exists, so a step that started before a crash cannot disappear silently.
- **Deterministic workflow activations.** Workflow replay needs deterministic delivery of future resolutions. Step completions, step failures, timer fires, signal deliveries, and child-workflow completions are external events that make workflow tasks runnable; they must be appended to history and delivered to the deterministic workflow scheduler in history order. This matters even without racing tasks: two branches may each schedule a second step after their first step resolves, and the second-step command order must be driven by recorded completion order rather than by wall-clock timing or a local scheduler's ready-queue accident.
- **Concurrent history writes.** Process-local step execution may run multiple step attempts concurrently, but those attempts must not concurrently mutate workflow history through one non-threadsafe store connection. History mutations are serialized per workflow or protected by executor-local connections/transactions with durable command ids and optimistic checks. Sharing the workflow executor's live connection across Async child tasks is not a valid implementation strategy.
- **Wait suspension and in-flight work.** A step wait is a terminal command-resolution event, but releasing the workflow lease to `waiting` happens only after the workflow activation has reached a safe suspension point. If one branch records a wait while sibling workflow fibers have already scheduled process-local steps, those sibling steps may finish and commit before the workflow row is released. An activation with only the root fiber is the single-fiber degenerate case, so it can usually persist the wait and suspend the workflow in one activation.
- **Append-only attempts.** Step attempts are append-only, including waits that transition to completed attempts. Retries and stale attempts remain inspectable.
- **Runnable workflow queue.** Runnable workflows are represented by `pending` rows, retryable `failed` rows whose non-null `next_run_at` is due, or expired `running` leases that are recoverable. Terminal `failed` rows with no retry deadline are not claimable.
- **Distributed workflow leases.** Workflow and object ownership are represented by persisted leases keyed by pool-scoped durable target identity. Lease holders must re-check ownership before mutating durable state.
- **Lease-aware resume.** `Engine#resume` refuses to execute work owned by another live worker.
- **Heartbeat extension.** Active workflow leases can be extended, including explicit step heartbeats with opaque cursor storage. Long-running steps do not heartbeat automatically; user code must call the provided heartbeat before the lease deadline or choose a long enough lease.
- **Expired lease stealing.** Crashed workers are recovered by returning expired `running` workflows to `pending`.
- **Resume semantics.** Completed steps are skipped; incomplete/running/failed/waiting steps are retried or continued according to durable state.
- **Replay shape checks.** Replay shape checks apply to all scheduled durable commands, not only completed steps. If a prior run scheduled command `17` as `fetch_profile(user_id: 1)` and a replay schedules command `17` as `fetch_profile(user_id: 2)` or `send_email`, the workflow is nondeterministic even if the original command only reached `started` before a crash. This is required for scatter/gather fanout where many branches call the same step method with different inputs. Completion events are used to resolve futures; start events are used for execution observability and retry/recovery sanity; schedule events are the replay contract.
- **Timer waits.** `Durababble.wait_until` persists timer waits and resumes workflows after the wake time.
- **External event waits.** `Durababble.wait_event` and `Store#signal_event` persist event waits and wake matching workflows.
- **Durable workflow signals.** Workflow signals are durable inbox/history messages delivered to declared signal handlers at deterministic workflow yield points.
- **Object sleeps.** Durable objects support one pending `sleep_until`/`cancel_sleep` wakeup per object id, converted atomically into a durable mailbox wake message.
- **Idempotency fences.** `Store#with_fence` acquires a fence before the side-effect block executes so concurrent callers do not duplicate the side effect. Fence owner crash recovery must be explicit and operator-visible.
- **Durable outbox.** Outbox rows have unique keys, leasing, expiry recovery, and acknowledgement. The public workflow/object API does not expose outbox as a first-class concept yet.
- **Durable object commands.** `expose_command` on durable objects records command rows, creates a stable library-generated idempotency key, and executes through per-object durable mailbox ordering, lease ownership, recovery, and worker-driven execution via the unified inbox.
- **Workflow exposed commands.** `expose_command` on workflows commits a durable inbox ask/tell row, wakes the active workflow owner or a durable target activation immediately after commit, executes the command against the workflow as soon as the workflow reaches a safe deterministic yield point, and stores serialized result/error on synchronous ask rows so callers can return or reattach.
- **Cooperative workflow cancellation.** `Workflow.handle(workflow_id).cancel(reason:)` durably records the first cancellation request and makes the workflow runnable. Cancellation is delivered into deterministic workflow execution at durable yield points. Workflow code may catch `Durababble::CancellationError` and run cleanup steps; returning from cleanup or re-raising the cancellation error records the workflow as `canceled`, while unrelated cleanup failures follow ordinary retry/failure semantics.
- **Exposed transient methods.** `expose` declares public query/transient methods. Exposed methods use owner-local non-durable RPC via gRPC for live objects/workflows.
- **Library-generated operation keys.** Workflow steps and durable object commands receive library-generated idempotency keys (`step_context.idempotency_key`, `command_context.idempotency_key`). Public APIs also accept caller idempotency keys for starts, asks, tells, and signals.
- **Worker pools.** Worker pools are runtime groupings for workers that can execute a workflow/object family. Persistence includes `worker_pool` on durable targets and on keys/indexes where query patterns need pool scoping.
- **Inter-node RPC.** Remote intranode/inter-pod communication uses gRPC over mTLS/Spiffe.
- **Sticky placement.** Runtime requires pool-local sticky placement for hot durable objects and running workflows, with leases, node registration, owner lookup, in-memory caches, and route-to-owner behavior.
- **Determinism and code evolution.** Workflow code evolution includes Temporal-style `patched` / `deprecate_patch` marker checks for safe workflow control-flow changes. The workflow runtime requires workflow-local deterministic fibers, deterministic time/sleep/random shims for workflow orchestration code, and a command-history-driven replay loop. `Ruby::Box` workflow realms and class-level schema-versioned object/workflow state are outside the v1 contract.
- **RBS typing.** The runtime does not load or validate user RBS. The gem ships `sig/durababble.rbs` with `Durababble::Workflow[Input, Output]` and `Durababble::DurableObject[Id, State]` generics for static tooling only.
- **High-level worker lifecycle.** `Durababble::WorkerRuntime` is the app/process integration point. It loops `Worker#tick` for one worker pool, stops taking new claims on shutdown, waits for in-flight work up to a timeout, and revokes still-held workflow/outbox leases if the timeout is exceeded.
- **Coverage thresholds.** The suite uses SimpleCov line and branch coverage thresholds for the library.

## Programming model

Durababble's user-facing model is two Ruby base classes over shared durable coordination machinery:

| Primitive        | Class                       | Public handle API                         | Best for                                                                              | Mental model                                         |
| ---------------- | --------------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Durable workflow | `Durababble::Workflow`      | `Workflow.start` / `Workflow.handle`      | one-off processes with a start, result, steps, waits, and recovery                    | function/object that survives restarts               |
| Durable object   | `Durababble::DurableObject` | `DurableObject.at` / `DurableObject.tell` | sessions, carts, conversations, agents, per-shop workers, or other id-addressed state | SQL-backed actor/mailbox object with a lease owner   |

`Durababble::Workflow` is the workflow base class.

Workflow and object calls compose. A workflow can call a durable object. A durable object command can start or signal workflows. Worker-pool semantics say child calls inherit the caller's worker pool unless explicitly overridden.

### Workflow API

A workflow class subclasses `Durababble::Workflow` and implements `#execute(input)`. `#execute` must be deterministic orchestration code. Any method that performs durable side effects must be declared as a workflow step:

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

  expose_command def cancel(reason:) = reason

  expose def description
    self.class.workflow_name
  end
end
```

`Workflow.start(input, idempotency_key: nil, worker_pool: nil)` creates a durable pending execution and returns a workflow handle. `Workflow.handle(workflow_id)` returns a query/management handle. Worker runtimes accept workflow classes.

`step_context` is available only while a workflow step is executing. It contains `workflow_id`, `step_index`, `attempt_number`, `idempotency_key`, and `heartbeat`. Idempotency keys are generated from durable coordinates and are stable across retries of the same logical step.

The workflow runtime supports deterministic workflow fibers through raw Async programming. Workflow authors use the documented `Async` API directly; Durababble does not expose its own workflow `async`, `await`, or `await_all` helpers. The following raw Async scatter/gather workflow must be valid:

```ruby
class FanoutProfiles < Durababble::Workflow
  def execute(user_ids)
    Async do |task|
      tasks = user_ids.map { |id| task.async { fetch_profile(id) } }
      tasks.map(&:wait)
    end.wait
  end

  step def fetch_profile(user_id)
    Profiles.fetch(user_id, idempotency_key: step_context.idempotency_key)
  end
end
```

Durababble must record a `step_scheduled` history event for each `fetch_profile(id)` command before dispatching that step body. The recorded command shape must distinguish `fetch_profile(1)` from `fetch_profile(2)`, even though both call the same Ruby method.

Continuation fanout must also replay safely:

```ruby
class EnrichProfiles < Durababble::Workflow
  def execute(user_ids)
    Async do |task|
      tasks = user_ids.map do |id|
        task.async do
          profile = fetch_profile(id)
          score_profile(profile)
        end
      end

      tasks.map(&:wait)
    end.wait
  end

  step def fetch_profile(user_id)
    Profiles.fetch(user_id, idempotency_key: step_context.idempotency_key)
  end

  step def score_profile(profile)
    ProfileScoring.score(profile, idempotency_key: step_context.idempotency_key)
  end
end
```

If `fetch_profile(2)` completes before `fetch_profile(1)` in the original execution and schedules `score_profile(...)` first, replay must resume workflow fibers in the same history-recorded completion order. It must not resume fibers based on Ruby scheduler timing, database query order, or the fact that completed first-step results can be returned synchronously during replay.

Workflow exposed commands are durable workflow inbox messages delivered to the workflow lease owner. A command call that commits must make the target runnable immediately: if a live owner holds the workflow lease, the runtime sends `DeliverMessage` after commit; if there is no live owner or advisory delivery fails, the durable target activation remains claimable by the worker pool. Workflow authors must not have to park on a matching `wait_event` or poll their inbox manually. The command executes at the next safe deterministic yield point: before a new durable workflow command starts, after a step completes/fails/waits, when a running step heartbeats or otherwise yields to the workflow engine, or when a sleep/wait activation is interrupted by message work. A non-heartbeating user step body is not preempted mid-Ruby-frame; the command runs when the active owner reaches a safe point or when lease expiry/recovery gives the target to another worker.

Synchronous workflow command APIs are durable asks. The caller waits for the inbox row to store a serialized result or typed error, then returns/re-raises it. If the caller times out or loses its connection after the row commits, the durable command is not canceled; retrying with the same idempotency key reattaches to the same inbox row and observes the stored result/error when available. Command responses do not require the general outbox table because the response is a one-to-one property of the ask row; workflow/object code may still write ordinary outbox messages when a command handler needs durable delivery to an external system.

### Cooperative workflow cancellation

Cancellation is cooperative execution, not hard termination:

- `Workflow.handle(workflow_id).cancel(reason:)` records the first durable cancellation reason and request timestamp. Duplicate requests return the current run and preserve the first reason.
- Pending, waiting, and retry-backoff workflows move to `canceling`, clear `next_run_at`, and become claimable immediately. Pending waits are marked canceled so late timer/event signals cannot resume the canceled wait.
- Running workflows keep their active lease. Cancellation is observed at deterministic yield points: before a new durable command starts, when replay reaches a completed command boundary, after a step completes, and when a running step heartbeats.
- Delivery raises `Durababble::CancellationError` with the durable reason and workflow id. Once raised, cleanup steps run as ordinary durable steps under the same command-history replay model as all other workflow work.
- If workflow code catches cancellation and returns after cleanup, the engine records the workflow as `canceled` and stores the cleanup result. Re-raising `CancellationError` also records `canceled`. If cleanup raises an unrelated error, ordinary step retry policy applies; exhausted or non-retryable cleanup failures mark the workflow `failed`.
- Child workflow APIs must use explicit child-cancellation policy. Parent cancellation must not silently terminate child work or report `canceled` before the selected child policy has reached a durable outcome.
- Operator termination is a distinct hard-stop operation. It may stop work without running cleanup, but it must use a separate state/API and must not report cooperative cleanup as completed.

### Workflow API requirements

The workflow surface is:

- `Workflow.start(...)` starts a workflow and returns a handle.
- Idempotent start accepts caller-provided `id:` or `idempotency_key:` and returns the same handle for the same worker pool, class, and arguments; same key with different shape raises `IdempotencyKeyConflict`.
- `Workflow.handle(workflow_id)` returns a query/management handle supporting status/result/cancel/resume, signals, and exposed transient methods.
- `handle.signal(:name, **args, idempotency_key:)` commits a durable inbox message before returning and fails for terminal workflows.
- Calling an `expose_command` method on a workflow handle commits a durable ask row, wakes the target through `DeliverMessage` or target activation, waits for serialized result/error unless the API is explicitly fire-and-forget, and fails for terminal workflows.
- `signal def handler` declares deterministic workflow signal handlers.
- `Durababble::Workflow.wait_condition(timeout: nil) { ... }` blocks a workflow fiber until the condition is true or a durable timeout fires.
- `Durababble::Workflow.sleep(duration)` and `sleep_until(time)` are durable workflow sleeps.
- Raw `Async { ... }` and `Async::Task#async` are supported workflow authoring APIs. Non-transient child tasks created inside workflow orchestration inherit the active workflow execution context and may call durable steps. `transient: true` Async tasks do not inherit workflow execution context and must not call durable steps.
- Durababble must not require, document, or expose library-specific async helpers for workflow authors. Context propagation and deterministic replay are provided by integrating with Async task creation and waiting.
- Workflow-local futures must preserve ordered command replay. Task creation and first execution order must be deterministic, and a durable step command is assigned when the workflow fiber reaches the step call, not when the local step implementation happens to finish. The scheduled command shape must include enough data to reject same-method input reordering during replay.
- Awaiting a durable command parks only the workflow fiber. The step implementation runs on a local process executor by default, and later on a remote executor for remote steps. The workflow scheduler resumes fibers only from deterministic history activations.
- A wait returned by one workflow fiber must not invalidate the lease for sibling fibers that are still committing already-started local step results. The executor finalizes workflow suspension after the activation quiesces or when no sibling workflow fibers can still schedule/commit work.
- Workflow-visible races over Async tasks are not just scheduler operations. The winning resolution order must be recorded in workflow history, because real completion order is external input to deterministic replay.
- Workflow orchestration code must not perform direct blocking or nondeterministic I/O. It may schedule durable steps, sleeps, waits, signals, child workflows, and deterministic local computation. Step implementations may perform process-local side effects using the local executor and are expected to pass `step_context.idempotency_key` to external systems that support idempotency.
- `patched(patch_id)` is the API for cross-deploy workflow control-flow compatibility. It records/checks a durable patch marker before the new branch emits steps, waits, or signals.
- `deprecate_patch(patch_id)` is the cleanup API after no live workflows still need the old branch. It keeps replay compatibility while allowing the old branch to be removed before final marker removal.
- Numeric `version(change_id, default:, max:)` is outside the v1 API unless a concrete need appears; the preferred model is the simpler Temporal-style boolean patch marker.

### Durable object API

A durable object class subclasses `Durababble::DurableObject`. It is addressed by `Class.at(id, worker_pool: nil, idempotency_key: nil)` for proxy calls and `Class.tell(...)` for async durable commands. Durable object methods are not workflow steps. Public query methods are declared with `expose`; public serialized mutating commands are declared with `expose_command`:

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

The durable-object contract is that `expose_command` commands serialize through the durable object's identity, run with `command_context`, and may update state with `update_state(new_state)`. `expose` queries read latest persisted state and must not mutate state. `command_context.idempotency_key` is generated by Durababble and is stable for the durable command. Object commands execute one at a time through object command leasing and the unified inbox.

### Durable object API requirements

- Address objects through `DurableObject.at(id, worker_pool: nil, idempotency_key: nil)`. Existing `ref` can be kept as a compatibility alias or lower-level API, but `at` is the intended public spelling.
- `expose def` registers transient non-durable owner-local methods. They must not enqueue inbox messages, call `step`, mutate durable state, schedule sleeps, or write Durababble tables. Passing `idempotency_key:` to an exposed transient method raises.
- `expose_command def` registers durable mailbox commands. Commands enter the unified inbox and execute in strict per-target FIFO order. Synchronous asks and asynchronous tells share the same mailbox, so asks cannot overtake earlier tells.
- Enqueuing a ready object command wakes the active owner through `DeliverMessage` or leaves a durable target activation for pool-local recovery. Object authors do not poll for commands; the owner drains the mailbox when woken or claimed.
- `Object.tell(id, :method, **args, idempotency_key: nil)` enqueues a durable fire-and-forget command. It validates that the target method is an `expose_command`.
- Commands, scheduled wakeups, and other mailbox work acquire an exclusive writer slot. Exposed transient methods acquire shared read access, can run concurrently with each other, and stop entering once mailbox work is waiting.
- If the mailbox head is waiting for backoff, paused, dead-lettered, or otherwise blocked, later messages for the same target must not run.
- A command's inbox row owns a stable `operation_id`; any `step` checkpoints inside that command use the operation id so retries skip completed side-effect steps.
- `on_create`, `on_load(prev_schema_version:, prev_dump:)`, `on_wake(payload: nil)`, and `on_destroy` are lifecycle callbacks. They are not remotely callable public methods.
- Class-level `schema_version` for durable object state is outside the v1 API, clarified below. If it is introduced, `on_load` handles forward migrations after deserialization and before exposed user code runs.
- Optional `attribute :name, Type, default:, null:` accessors map to an opaque Paquito-serialized object state blob by default, with optional indexed/generated-column support behind explicit schema configuration.
- Object `sleep_until(at:, payload: nil)` atomically replaces the pending sleep row for that object in the same transaction as the command state write. `cancel_sleep` removes it. Matured sleeps convert to mailbox `wake` messages before the sleep row is removed.
- Management operations exist for operator use: `list`, `find`, `pause`, `resume`, `cancel`, `destroy!`, `evict`, and explicit `relocate_worker_pool`.

No `.with(id) { ... }` block API is part of v1. Multi-method atomicity is expressed by writing one command method that does the whole operation.

### Typing

Durababble's runtime does not load or validate user RBS. The gem ships `sig/durababble.rbs` with `Durababble::Workflow[Input, Output]` and `Durababble::DurableObject[Id, State]` generics for static tooling only; runtime serialization remains Paquito-based.

## Idempotency contract

Durable operations use both library-generated keys and caller-provided keys:

- Step idempotency keys are generated from workflow id + deterministic command id and are available through `step_context.idempotency_key`.
- Durable object command idempotency keys are generated from object type + object id + mailbox message id and are available through `command_context.idempotency_key`.
- `Store#with_fence` deduplicates side-effect blocks by a workflow-local fence key while the owner completes or fails normally.
- Outbox uniqueness uses message keys.
- Every public durable entry point accepts `idempotency_key:` where durability is implied: workflow starts, object asks, object tells, workflow signals, and operator APIs exposed externally.
- Idempotency keys are scoped to worker pool, target, operation kind, method, and argument fingerprint, not just target id.
- Same key + same operation shape returns the existing handle/result or re-raises the saved error. Same key + different operation shape raises `Durababble::IdempotencyKeyConflict`.
- Transient `expose` methods are not durable and must not accept idempotency keys.
- Caller timeout after a durable command/signal/message commits does not cancel the durable work. Retrying with the same idempotency key reattaches to the same row.
- Completed idempotency and inbox rows have configurable retention. The default retention is 30 days unless a class or operation specifies a longer retention window.

## Storage and schema requirements

### Schema

Durababble persists the following logical entities. Physical table names may be namespaced or adapter-specific, but the durable semantics are common across supported SQL backends.

| Entity                | Purpose                                                                                       | Required key/query shape                              |
| --------------------- | --------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| `workflows`           | workflow execution status, input/result/error, retry due time, and workflow metadata           | workflow id, workflow class, status, due time         |
| `workflow_history`    | append-only ordered replay events for workflow commands, completions, signals, timers, markers | `(workflow_id, event_index)`, command id lookup       |
| `steps`               | latest logical workflow step state for query/result caching                                    | `(workflow_id, command_id)`                           |
| `step_attempts`       | append-only attempt history for step execution and waits                                       | attempt id, workflow id, command id, status           |
| `waits`               | durable timer/event waits                                                                     | wait id, workflow id, due time/event key/status       |
| `leases`              | pool-scoped ownership for workflow and object targets                                          | `(worker_pool, target_kind, target_class, target_id)` |
| `nodes`               | worker node registry with RPC address, advertised pools, draining flag, and heartbeat          | `(worker_pool, node_id)` and fresh heartbeat lookups  |
| `inbox`               | durable asks, tells, wakes, workflow signals, and workflow command messages                    | target identity, sequence, status, message id         |
| `mailbox_sequences`   | per-target monotonic mailbox sequence allocation                                               | target identity                                       |
| `target_activations`  | coalesced runnable target wakeups for inbox/scheduler work                                     | worker pool, ready time, target identity, status      |
| `idempotency_keys`    | public durable operation deduplication and shape conflict detection                            | operation scope + caller key                          |
| `fences`              | workflow-local side-effect idempotency fences                                                  | `(workflow_id, key)`                                  |
| `outbox`              | durable outgoing messages with processing leases                                               | id, unique message key, lease expiry                  |
| `durable_objects`     | latest object state by object type/id                                                         | `(worker_pool, object_type, object_id)`               |
| `object_sleeps`       | pending durable object wakeups                                                                | object identity and sleep id                          |

The PostgreSQL/YSQL adapter uses the selected namespace as a schema. The MySQL/MariaDB adapter prefixes table names with the selected namespace because MySQL has database/table namespace differences. If callers do not pass a `schema:` argument, the selected namespace is `DURABABBLE_SCHEMA` when set, otherwise `Durababble.workspace_schema(DURABABBLE_WORKSPACE_ROOT || Dir.pwd)`. MySQL columns use `VARCHAR`, `DATETIME(6)`, and `LONGBLOB`; PostgreSQL/YSQL columns use `text`, `timestamptz`, and `bytea`.

Query-shape and transaction requirements:

- Claim paths use `FOR UPDATE SKIP LOCKED` where supported.
- Persist immutable `worker_pool` on durable targets whose routing/claiming/scheduling semantics are pool-scoped.
- Include `worker_pool` in primary keys and indexes when query patterns need to filter, route, claim, recover, or list by worker pool. Do not add it to keys where query patterns do not care about worker pool.
- Workflow/object ownership uses a unified leases table keyed by pool-scoped target identity, with `node_id`, `rpc_address`, `lease_token`, and `lease_until`.
- The node registry is keyed by pool/node identity and records `rpc_address`, advertised pools, draining flag, and heartbeat time.
- Object asks/tells/wakes and workflow signals/commands use the unified inbox.
- Per-target mailbox sequence state lets `enqueue_message` allocate a monotonic sequence and lets target executors drain only a contiguous ready prefix from the head.
- Enqueueing a ready inbox message must atomically upsert a target activation row, or an equivalent scheduler row, keyed by worker pool and durable target identity. Workers globally poll target activations, not the inbox table; one activation can cover many pending inbox rows for the same target.
- Target activation completion must be conditional on the target mailbox/head state: after draining, the executor clears the activation only if no ready inbox row remains, otherwise it keeps or re-arms the activation with the next due time.
- If a worker claims a target activation but finds the target is still owned by another fresh lease, it must not hot-loop the activation; it should rely on advisory `DeliverMessage` for the live owner and re-arm the durable fallback no earlier than the observed lease deadline.
- Object sleep rows are keyed by object identity plus worker pool when sleep dispatch is pool-scoped, with `sleep_id`, `wake_at`, and Paquito payload.
- Append-only workflow history rows are ordered per workflow. Required event families include `step_scheduled`, `step_started`, `step_completed`, `step_failed`, `step_waiting`, timer/wait events, signal delivery events, child-workflow events, and `patched` / `deprecate_patch` marker rows.
- Store deterministic command ids and replay-relevant command shape on schedule events, and store concrete attempt ids on start/completion/failure/wait events. The command id is the replay identity; the attempt id is the execution/retry identity.
- Latest-state tables such as `steps` exist for query convenience, but mutable latest-state rows are not the replay source. Replay uses ordered schedule history; deterministic scheduling uses history-ordered future resolution events; execution recovery uses distinct attempt start/completion events.
- Wait rows and `step_waiting` history can be committed before the workflow row is released to `waiting` when an activation still has sibling workflow fibers to drain. Event/timer wake queries only make externally visible progress once the workflow is durably suspended or otherwise ready for that activation.
- Explicit idempotency rows cover workflow starts and any public durable operation not deduped by the inbox itself.
- Queue/recovery indexes cover workflow claims, due retries, expired workflow leases, event waits, timer waits, step-attempt lookup, outbox claims, and mailbox status scans.
- High-risk transactional pieces (`enqueue_message`, target-head drain/advance, sleep-to-inbox conversion, object state + message completion) may be provided as database functions to reduce lock-order drift, provided the common backend contract is preserved.
- Plan retention/partitioning for high-volume history (`steps`, `step_attempts`, `inbox`, idempotency) before production scale.
- Runtime value decoding only decodes known serialized binary columns.
- Store migrations that convert older runtime payload encodings to Paquito bytes must preserve existing values.
- MySQL/MariaDB and PostgreSQL/YSQL must pass the same store backend conformance suite.

### SQL portability requirement

All public durable semantics must work on PostgreSQL/YSQL and MySQL/MariaDB. Schema work that would otherwise rely on backend-specific features such as partial indexes, `RETURNING`, `ON CONFLICT`, `BYTEA`, or `gen_random_uuid()` must either:

1. define equivalent behavior in the backend abstraction and conformance tests, or
2. explicitly mark a feature as YSQL-only and keep it out of the common public contract.

Do not silently drop MySQL/MariaDB support.

## Worker pools, leases, routing, and scheduling

### Worker and recovery behavior

A worker pool is a set of processes repeatedly calling `Durababble::Worker#tick` or `#run_until_idle`. Each tick claims runnable work whose workflow or object class is present in the worker registry, including workflow rows and coalesced target activations, then resumes it through the deterministic workflow executor or durable object mailbox executor.

`Durababble::WorkerRuntime` is the preferred app/process lifecycle entrypoint. A Rails initializer can create one runtime per desired pool during boot, keep the returned object, and call `shutdown(timeout: ...)` from the process shutdown hook. Shutdown stops new claims, waits for the active tick, and releases this worker's workflow/outbox leases if the timeout expires. Late/zombie state writes are guarded by lease ownership checks in `Engine`.

Expired leases are reclaimed by claim paths or recovery sweeps. Recovery does not require a separate coordinator for correctness; any worker serving the pool may move expired work back to a claimable state.

### Worker-pool requirements

- Every durable target whose execution/routing is pool-scoped has an immutable persisted `worker_pool` selected at first materialization.
- If a class declares a default pool, that pool wins unless the caller overrides while creating a new durable unit. Once the row exists, the persisted pool wins.
- Worker pools are the routing and multiregion boundary. A pod in another pool cannot claim, route, or wake a target unless the target is explicitly relocated.
- `nodes` records which pools each pod serves, its RPC address, draining state, and heartbeat. Durable class data-version capabilities are outside the v1 routing contract.
- Scheduler scans filter by pools served by the local pod.
- `AwakenBatch`, `DeliverMessage`, and `CallTransient` are sent only to nodes in the target pool.
- Automatic cross-pool stealing is forbidden. Regional failover is explicit `relocate_worker_pool` operator/runtime work that quiesces the target, releases the old lease, updates the row, and wakes it in the new pool.
- Tables/indexes only include `worker_pool` in keys when these query patterns actually need it.

### Sticky placement requirements

Routing keeps hot ids on the pod that already has them in memory:

- Every pod loads or generates a stable `node_id` and advertises `rpc_address = "#{POD_IP}:#{DURABABBLE_RPC_PORT}"`.
- Every acquired lease writes `node_id`, `rpc_address`, and a fresh `lease_token` into the lease row.
- Lease acquisition uses a hot in-memory cache when the owner has a fresh lease and falls back to atomic SQL acquisition/lookup when cold, near expiry, or routed remotely.
- A `LeaseRenewer` refreshes in-flight leases every `lease_ttl_ms / 3` and only when the `lease_token` still matches.
- Cache entries are evicted on near-expiry, `EvictLease`, object CAS conflict, lease-renew failure, idle timeout, or LRU capacity pressure.
- Durable object cache entries include `{instance, lock_version, lease_token, last_used_at, gate}` and must never invoke user code when the lease is expired or near refresh threshold.
- Idle owners stop renewing after `idle_eviction_ms` and release leases so the next pool-local caller can acquire ownership.

## Inter-node RPC protocol

### gRPC requirement

Actual remote intranode/inter-pod communication must use the full four-method gRPC service over Shopify-standard mTLS/Spiffe. Each pod runs a dedicated `Durababble::RpcServer` using the `grpc` gem's `GRPC::RpcServer`, bound to `rpc_host:rpc_port` (default port `50051`) with its own thread pool. No shared bearer secret is used. Peer identity comes from Spiffe; Durababble additionally authorizes peers through an allowed service-account list.

The service shape is part of the public runtime contract:

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
- **DeliverMessage** is a wakeup for already-committed inbox rows. It carries no user payload; the receiver queries durable inbox rows itself and schedules an owner-local target activation without blocking the gRPC handler on user code. If the receiver no longer owns the lease, it returns success without work and the sender/scheduler re-checks the lease.
- **CallTransient** is non-durable RPC for exposed methods against the active owner. It returns a Paquito result, a remote error, `not_running`, or `LeaseMoved`.
- **EvictLease** asks a pod to drop a cached lease it may no longer own.
- Connection failure to an owner causes a short retry, lease re-check, and reroute. If wakeup still fails after the retry budget, the already-committed target activation remains the correctness path and is eventually claimed by a worker in the target pool.
- gRPC is required for cross-pod calls because strongly typed protos catch shape drift and mTLS is already available. Auxiliary test transports must not be used for production intranode communication.

## Durable inbox, signals, and mailbox ordering

Durababble uses a single unified durable inbox:

- Every durable target (object instance or workflow execution) has an inbox.
- Inbox rows are not globally polled as independent work items. Ready inbox rows make their target runnable by creating/updating one target activation keyed by target identity; the active owner or the activation claimant drains the target mailbox.
- Object inboxes are push-driven: the owner pod drains commands/wakes/internal work from the mailbox and invokes registered command/lifecycle methods against the cached instance.
- Workflow inboxes are history-driven: signal and command rows are accepted into durable workflow history and delivered to handlers at deterministic yield points so replay sees the same event order.
- Inbox enqueue and target activation upsert commit before any gRPC wakeup.
- Sequence allocation, inbox insert, and ready-target activation upsert are one transaction.
- Target executors drain only the contiguous ready prefix from the mailbox head. `SKIP LOCKED` must not let later messages overtake a blocked head for the same target.
- Object message completion, state write, sleep updates, and mailbox head advancement must be one fenced transaction.
- Workflow signal/command acceptance and history retention must be idempotent by message id.
- Workflow commands and signals must not require user-authored `wait_event` compatibility shims. The workflow executor must inspect the target inbox when woken by `DeliverMessage`, when claiming a target activation, and at safe deterministic yield points during active execution.
- For object targets, `consumed_at` means command completion. For workflow signal targets, `consumed_at` means accepted into workflow history; for workflow command asks, `consumed_at` means the handler result/error was durably committed after the delivery was recorded. Rows remain retained until retention expires.
- Ask rows store serialized result or error. Tell/wake rows retry with backoff and move to dead-letter after `max_message_attempts`.
- A dead-lettered or backed-off head blocks later messages until an operator retries, skips, cancels, destroys, or repairs the target.

## Determinism, steps, and code evolution

### Step semantics

Workflow steps are method-level durable side-effect boundaries. On first execution, the runtime records a scheduled command, records a running attempt, runs the method outside the deterministic workflow scheduler, stores the serialized result, and marks the command completed. On resume, completed command rows return cached results and do not re-run the method. If the process crashes after an external side effect but before the checkpoint commits, the step may run again; external systems must use `step_context.idempotency_key`.

No workflow row lock is held while user step code runs. The executor holds a renewable lease and fences durable writes with active lease ownership. If the lease is lost while activity code is running, the external activity may still finish, but the checkpoint/status write fails and recovery follows the normal idempotent retry path.

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

On a retryable failure, the runtime records the step attempt as failed, returns the workflow to a claimable retry state, releases the workflow lease, and stores `next_run_at`. Claim paths ignore pending/failed workflows whose `next_run_at` is still in the future, and only treat `failed` rows as retryable when `next_run_at` is non-null and due, so retry delay survives process restarts and terminal failures remain terminal. On the final failure, or for a non-retryable error, the workflow itself is marked `failed` with `next_run_at` cleared and the error bubbles to workflow state.

### Method/order step identity

Method/order-based step identity is the workflow replay model:

- Command ids are assigned by deterministic workflow execution order.
- Method names are metadata and guardrails; users do not name steps at call sites.
- Replay skips completed commands and uses command-shape metadata to detect code drift.
- Workflow concurrency/futures preserve deterministic method/order semantics. User-provided semantic keys are optional guardrails for readability and operator lookup; they are not required for replay identity.

### Patched workflow event-log checker

Durababble provides a Temporal-inspired patch marker API for safe workflow code evolution. It solves a different problem from serialized data migrations: a workflow code deploy can change deterministic orchestration control flow, step order, waits, or signal handling. Existing executions must keep following the history they already produced, while new executions and executions that have not yet reached the change point can take the new branch.

API:

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

The canonical public spelling is `patched(patch_id)`. A Ruby-idiomatic `patched?(patch_id)` alias is acceptable, but docs use `patched` to match Temporal terminology. `deprecate_patch(patch_id)` is the cleanup helper after the old branch no longer has any live histories.

Rules:

- `patch_id` is a stable, non-empty string unique to one logical code change. Do not reuse a removed id for a later unrelated change.
- `patched` is only valid in deterministic workflow orchestration code: `execute`, signal handlers, and `wait_condition` predicates/continuations. It must not be called inside `step` bodies, durable object commands, exposed transient methods, or arbitrary library code outside an active workflow execution. Calling it inside a step raises a typed deterministic/runtime error because completed step bodies are not replayed.
- The first `patched(patch_id)` call for a workflow execution is event-bearing. Later calls with the same id in the same execution return the memoized decision and must not append duplicate marker rows.
- When executing live code at a history point that has no persisted event yet, `patched(patch_id)` appends a normal patch marker and returns `true`. The marker commit must happen before the new branch produces steps, waits, signals, or other durable workflow events.
- When replaying/checking a history that already contains a normal marker for `patch_id`, `patched(patch_id)` consumes that marker and returns `true`.
- When replaying/checking history produced by old code that reached the change point without a marker, `patched(patch_id)` returns `false` and appends nothing, so code runs the old branch and matches the existing step/wait history.
- If persisted history contains a normal patch marker that workflow code does not consume with `patched` or `deprecate_patch`, the checker raises a nondeterminism error before any further durable writes. The same applies to patch-id mismatches and out-of-order marker consumption.
- Patch markers are workflow-history markers, not user-visible inbox messages and not serialized state schema versions. They must not route work to different nodes by capability.

Event-log/checker model:

- A per-workflow ordered history checker wraps deterministic durable workflow boundaries. Completed step result state stores cached outputs; the event log is the deterministic skeleton that says which branch/checkpoint sequence the workflow produced.
- Workflow history storage has `workflow_id`, monotonic `event_index`, `kind` (`patch`, `patch_deprecated`, `step_scheduled`, `step_started`, `step_completed`, `wait`, `signal`), event key/command id, optional metadata bytes, and timestamps. The design must work on PostgreSQL/YSQL and MySQL/MariaDB without relying on partial-index-only behavior.
- A `WorkflowHistoryChecker` cursor compares calls made by workflow code with persisted history. Step calls and waits participate in the same cursor so removing a `patched` call in front of an existing marker fails immediately rather than drifting into method/order mismatch later.
- Marker append and checker reads must be fenced by the active workflow lease. A worker that lost the lease must not append or consume patch markers while committing later workflow state.
- Crash after marker commit but before the first new-branch step is safe: replay sees the marker, `patched` returns `true`, and the new branch resumes. Crash before marker commit is also safe: no branch output was committed, so the retry can append the marker and take the new branch unless old history already forces `false`.
- Admin/observability surfaces expose patch usage by workflow type/id: normal marker, deprecated marker, and open workflows with no marker for a given patch id. A conservative cleanup gate is “no open workflow of this type lacks the patch marker” before deleting the old branch.

Patch lifecycle:

1. **Introduce patch:** deploy `if patched("id") { new } else { old }`. New or not-yet-reached executions record a marker and run the new branch; already-past executions without a marker run the old branch.
2. **Deprecate patch:** after no live workflows still need the old branch, deploy `deprecate_patch("id")` plus the new code only. This keeps marker-aware histories replayable while removing the old branch.
3. **Remove marker call:** after all relevant histories have completed and aged out of retention, remove `deprecate_patch("id")`. Never reuse `id`.

Required tests:

- First execution records a marker before the first new-branch step and returns `true`.
- Replay/resume with a marker returns `true` and skips/replays the new branch deterministically.
- Replay/resume of old history with no marker returns `false` and follows the old branch.
- Removing a required `patched` call while normal marker histories still exist raises a nondeterminism error before durable writes.
- `deprecate_patch` allows the old branch to be removed and later allows the marker call to be removed after retention.
- Duplicate calls with the same id are memoized; accidental id reuse across unrelated code points is rejected or at least surfaced by checker/test tooling.
- The same behavior passes shared backend conformance on PostgreSQL/YSQL and MySQL/MariaDB, plus a subprocess crash test around marker commit.

### Determinism boundaries

The deterministic workflow runtime follows these boundaries:

- Per-execution `Ruby::Box` realms derived from a template box.
- Deterministic definitions for common nondeterministic Ruby entry points (`Time.now`, `Date.today`, `Kernel#sleep`, randomness, UUIDs) inside the box.
- Illegal external I/O outside `step` raising `Durababble::DeterminismError`.
- A safe host-realm activity trampoline so side-effecting `step` code runs with normal host Ruby semantics.
- Paquito-only values crossing host/box boundaries.
- Workflow-local deterministic futures/fibers that do not depend on host scheduling.
- Numeric workflow version markers for cross-deploy control-flow compatibility beyond the boolean `patched` model, if a concrete need appears.

The v1 runtime must not depend on process-wide monkeypatching. Deterministic shims belong to workflow-local execution realms.

### Class-level data schema versioning

Class-level serialized data schema versioning is outside the v1 contract. This refers to serialized durable data shape, not SQL DDL:

- Durable object state may have a class-level `schema_version` and `on_load(prev_schema_version:, prev_dump:)` migration hook.
- Workflow args/results/errors may similarly carry a class/code data version so long-lived executions can be loaded by compatible code.
- Node capability routing can advertise which durable classes and data versions a pod can serve, allowing callers to avoid routing newer-version state to older pods.
- A read of newer serialized data by incompatible code fails closed with a typed error such as `FutureSchemaVersionError` rather than corrupting state.

This is intentionally later than the `patched` event-log checker. Patch markers handle workflow control-flow compatibility across deploys; they do not migrate serialized object/workflow data or route by data-version capability.

## Configuration requirements

Durababble exposes a process-wide configuration object for storage, worker pools, RPC, leases, scheduling, payload limits, and observability:

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

- The CLI supports migration, workflow run/resume, inspection, and version output.
- Benchmarks record operation latency/throughput/allocation reports for workflow queueing, leases, waits, outbox, fences, inbox, deterministic workflow replay, and local step execution.
- Built-in admin/status surfaces for listing/finding workflows and objects, with bounded pagination.
- StatsD counters/timers for command ask latency, exposed method latency, mailbox queue/execution latency, `CallTransient`, step execution, replay frequency, recovery sweeps, sleep dispatch, lease acquisitions/forwardings/takeovers, lease-cache hit ratio, and object-cache hit ratio.
- OpenTelemetry spans around public calls, workflow executions, steps, scheduler ticks, and inbound gRPC requests. Spans must include worker pool, class, target id, and lease owner.
- Bugsnag/error integration for unhandled exceptions inside commands, exposed methods, steps, and gRPC handlers.
- Slow-step warnings.
- Routing health metrics for wakeup error rate, wakeup latency, and lease takeover frequency.
- Circuit breakers around database connections; public methods raise a typed error such as `Durababble::CircuitBreakerOpen` when the durable store is unavailable before commit.
- gRPC server health metrics for in-flight requests, handler-thread saturation, and dropped requests.
- Operator actions for dead-lettered mailbox heads: retry now, skip, cancel target, destroy target, and repair/decode failed payloads.

## Guarantee matrix

| Guarantee                                               | Required behavior                                                                                         | Validation expectation                                      |
| ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| Workflows are durable before execution                  | Starting a workflow commits a pending row with Paquito-serialized input before any worker can execute it. | complete spec guarantee + crash matrix                      |
| Runnable work is claimable by one worker at a time      | Claim paths atomically assign one live owner using adapter-appropriate row locking.                       | backend conformance + hardening concurrency specs           |
| Resume honors lease ownership                           | A worker may execute only work it owns; another live owner causes `LeaseConflict` or equivalent refusal.  | hardening lease spec                                        |
| Active leases can be heartbeated                        | Lease heartbeats extend ownership only for the owning worker and matching lease token/deadline.           | complete spec guarantee matrix                              |
| Running steps can explicitly heartbeat progress         | Step heartbeats extend the workflow lease and store an opaque Paquito cursor on the attempt.              | heartbeat spec + DST cursor recovery scenario               |
| Heartbeat cursors survive recovery                      | A retried incomplete attempt receives the latest stored heartbeat cursor.                                 | heartbeat spec + DST cursor recovery scenario               |
| Zombie workers cannot renew expired leases              | Heartbeat writes fail after lease owner/deadline mismatch.                                                | heartbeat spec                                              |
| Zombie workers cannot complete after lease revocation   | Terminal workflow/step writes re-check lease ownership before commit.                                     | worker lifecycle spec                                       |
| Step retries are durably scheduled                      | Retryable failures store `next_run_at`, release the lease, and are not claimable early.                   | step retry spec + DST retry scenario                        |
| Retry options are Temporal-like but Ruby-shaped         | `initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`, `schedule`, and `non_retryable_errors` define retry policy. | retry policy specs                         |
| Final retry failure bubbles to workflow                 | Exhausted or non-retryable step failure marks the workflow failed.                                        | step retry spec                                             |
| Expired leases can be recovered                         | Expired running work returns to a claimable state.                                                        | complete spec guarantee + crash matrix                      |
| Completed steps are not re-executed on resume           | Completed command results are returned from durable state.                                                | complete spec guarantee + subprocess crash harness          |
| Incomplete steps are retried                            | Incomplete/running/failed/waiting command state is retried or continued according to durable state.       | crash matrix                                                |
| Workflow command history is replay truth                | Schedule events, start events, and completion/failure/wait events are distinct append-only history facts. | async workflow replay specs + backend conformance           |
| Parallel schedule shape is validated                    | Replay validates method, args/kwargs digest, retry/executor attributes, and semantic key for every scheduled command, including incomplete commands. | fanout replay/nondeterminism specs          |
| Workflow future resolution is deterministic             | Step completions, failures, timer fires, signals, and child completions resume workflow fibers in history order. | continuation fanout replay specs              |
| Step attempts are append-only                           | Every started attempt and terminal attempt state remains inspectable.                                     | guarantee matrix                                            |
| Waiting attempts complete when signaled                 | Wait completion updates attempts from `waiting` to `completed` without losing payload.                    | wait-attempt spec                                           |
| Timer waits survive process exit                        | Timer wait rows store wake time and serialized context.                                                   | timer/event tests                                           |
| Event waits survive process exit                        | Event wait rows store event key and serialized context.                                                   | timer/event + crash matrix                                  |
| Signaled waits resume with payload                      | Matching event delivery completes waiting workflow fibers with Paquito payloads.                          | timer/event test                                            |
| Concurrent signalers wake a wait once                   | Concurrent signal delivery uses locked/idempotent updates.                                                | event concurrency spec                                      |
| Side effects can be fenced by key                       | A fence records `running` before yielding and exposes operator-visible recovery for abandoned owners.     | fence concurrency spec + owner-crash spec                   |
| Outbox delivery is durable and leased                   | Outbox rows are unique by key, claimable, acknowledgeable, and reclaimable after expiry.                  | outbox specs                                                |
| Workflow commands wake and run promptly                 | Command enqueue wakes the active owner or leaves a durable target activation; no workflow-side `wait_event` is required. | workflow command mailbox specs + gRPC wakeup specs |
| Synchronous durable commands return results             | Ask rows store serialized result/error and caller retries with the same idempotency key reattach.         | workflow/object ask specs                                   |
| Inbox is not a second global polling queue              | Workers poll coalesced target activations and target owners drain inbox rows for their own target.        | query-plan and mailbox specs                                |
| Workflow RPCs route to active lease holder              | RPC routing validates owner before/after handling, refreshes ownership after transport failures, and reroutes. | workflow RPC spec + gRPC transport spec + DST scenarios |
| Inter-pod RPC uses full four-method gRPC service        | Runtime RPC serves `AwakenBatch`, `EvictLease`, `CallTransient`, and `DeliverMessage` with production credentials/auth callbacks. | gRPC integration/contract tests + DST response scenarios |
| Multi-row state transitions are transactional           | Step start/finish/failure, wait transitions, inbox enqueue, mailbox advancement, and state/result writes commit atomically where required. | implementation + regression suite          |
| Runtime values are Paquito bytes                        | Runtime payloads use Paquito bytes in `bytea` / `LONGBLOB`, not JSONB.                                  | store storage + legacy migration specs                      |
| MySQL/MariaDB honors common store semantics             | MySQL/MariaDB and PostgreSQL/YSQL satisfy the same store behavior contract.                              | backend conformance spec                                    |
| Durable object API uses durable mailboxing              | `at`, `tell`, `expose`, and `expose_command` execute through per-object mailbox ordering and lease ownership. | durable object specs                                  |
| Object commands are per-id FIFO and worker-driven       | Inbox/mailbox execution enforces one writer, blocked-head behavior, and worker-driven retries.           | object mailbox specs                                        |
| Object sleeps convert to durable wake messages          | Sleep rows atomically convert to wake inbox rows without losing wakes.                                   | object sleep specs                                          |
| Workflow signals are durable ordered history            | Inbox rows are accepted into workflow history and replayed at deterministic yield points.                | workflow signal specs                                       |
| Workflow patch markers guard code evolution             | `patched` / `deprecate_patch` append and check ordered workflow history markers before branch side effects. | patch-marker unit, backend-conformance, and crash tests |
| Transient exposed methods route to owner                | `CallTransient` invokes live object/workflow owner without durable mutation.                             | transient RPC specs                                         |
| Worker pool scopes persisted targets and relevant keys  | Persisted targets and query-critical keys include `worker_pool` where routing/claiming requires it.      | worker-pool backend specs                                   |
| Unified inbox is the durable message model              | Object commands, object wakes, workflow signals, and workflow commands share one inbox contract.         | inbox/mailbox specs                                         |
| CLI supports operational basics                         | CLI supports migration, workflow run/resume, inspection, and version output.                             | CLI spec                                                    |

## Crash matrix

| Crash point                                                                | Expected recovery                                                                               |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| After enqueue, before claim                                                | Later engine/worker can run the pending workflow.                                               |
| After lease claim, before step schedule                                    | Lease expiry returns workflow to pending; another worker completes it.                          |
| After step schedule, before step start                                     | Replay validates the scheduled command shape and recovery dispatches the command.               |
| After step start, before step completion                                   | Step remains incomplete/running; recovery retries it with a new attempt.                        |
| After step heartbeat, before step completion                               | Latest heartbeat cursor is available to the next attempt.                                       |
| After step failure, before retry due time                                  | Retry schedule persists; workflow is not claimable early.                                       |
| After step completion, before workflow completion                          | Completed step is skipped and remaining work continues.                                         |
| After cancellation cleanup step completes, before canceled terminal write  | Completed cleanup step is skipped and workflow finishes `canceled` on recovery.                 |
| While waiting for an event                                                 | Wait row survives; signal wakes workflow and execution continues.                               |
| While waiting when cancellation is requested                               | Wait row/attempt are marked canceled; cleanup runs on next claim and late signals are ignored.  |
| After outbox insert, before delivery                                       | Outbox message remains claimable exactly once at a time.                                        |
| After outbox claim, before ack                                             | Expired outbox lease can be reclaimed by another sender.                                        |
| During lease-routed workflow RPC                                           | Receiver rejects stale/moved/shutdown/no-owner states; caller refreshes or fails by policy.     |
| During app shutdown with in-flight step                                    | Runtime stops new claims; timeout releases leases; later worker retries.                        |
| Crash after inbox row and target activation commit before `DeliverMessage` | Activation remains claimable; a later worker/owner drains the target inbox.                      |
| Crash before inbox row commits                                             | No message row exists; caller retry decides whether to enqueue.                                 |
| Crash while allocating mailbox sequence                                    | Transaction rolls back or commits both sequence advance and inbox row.                          |
| `DeliverMessage` reaches a stale owner                                     | Receiver no-ops after lease check; activation/lease re-check routes work to the current owner.  |
| Crash while object command runs before first step                          | Inbox head remains unconsumed; new owner reruns command after lease expiry.                     |
| Crash after object command step completion before state/message completion | Step output is cached; command replays and persists state/result once.                          |
| Crash after object state persists before ask result completion             | State persist and message completion must be atomic so this split state is impossible.          |
| Crash while head message is in backoff/dead-letter                         | Later messages remain blocked until operator action.                                            |
| Crash during object sleep-to-inbox conversion                              | Either sleep row remains or wake inbox row exists; no wake is lost.                             |
| Crash while workflow signal is accepted into history                       | Accepted signal is replayed in sequence.                                                        |
| Crash after patch marker commit before first new-branch step               | Replay sees marker, `patched` returns true, and the new branch continues.                       |
| Code removes `patched` while normal marker history still exists            | Checker raises nondeterminism before any later durable write.                                   |
| Incompatible code reads newer-version durable data                         | Worker releases lease; caller routes to compatible node or receives typed newer-version error. |
| Owner pod loses lease while activity continues                             | External effect may finish; checkpoint/status write fails; retry uses idempotency.              |
| Caller times out waiting for durable ask result                            | Row may keep running; same idempotency key reattaches later.                                    |
| Database circuit breaker opens before commit                               | No durable change exists unless transaction committed; caller receives typed breaker error.     |
| Paquito decode fails for persisted payload                                 | Target/message/workflow moves to operator-visible error/repair state.                           |

## Testing and coverage standard

The test suite must keep correctness claims evidence-backed:

- Real PostgreSQL/YSQL integration tests cover storage semantics, migration, leases, waits, outbox, fences, crash/recovery, and query-shape behavior.
- Shared backend conformance tests cover MySQL/MariaDB and PostgreSQL/YSQL behavior equivalence.
- Backend-specific tests must pin SQL behavior that differs by adapter, including lock/claim semantics and EXPLAIN-backed query-plan assertions for hot paths when practical.
- Deterministic simulation tests (DST) are useful for exploring lease/race schedules, but any DST-found storage bug must be pinned by a real backend regression test.
- Subprocess crash harnesses cover real process death around durable boundaries.
- RPC tests must cover stale lease, lease moved, no-active-owner, shutdown/non-running workflow, retry/reroute, gRPC serialization, unavailable-node, timeout, deadline, RST, EOF, lost-response, duplicate-response, auth-failure, wakeup drops/duplicates, and all four service methods.
- Object mailbox tests must cover strict FIFO, blocked head behavior, ask/tell ordering, wake ordering, idempotency conflicts, owner crash, lease takeover, dead-letter, and operator repair paths.
- Workflow signal tests must cover history acceptance, deterministic replay order, timeout behavior, terminal-workflow rejection, and idempotency dedup.
- Workflow patch-marker tests must cover first-run marker recording, old-history `false` branches, marker-history `true` branches, missing-marker nondeterminism failures, `deprecate_patch` cleanup, duplicate-id handling, backend conformance, and crash after marker commit.
- Exposed transient method tests must verify no durable state mutation and deadline/crash semantics.
- Observability tests verify required span/metric labels without depending on a particular vendor backend.

SimpleCov thresholds required by the CI coverage gate:

- global line coverage: 90% minimum
- global branch coverage: 85% minimum
- per-file line coverage: 59% minimum
- per-file branch coverage: 41% minimum

The gate is `mise exec -- bundle exec rake test:coverage`. It enables branch coverage, measures library files under `lib/**/*.rb`, excludes tests and non-library support surfaces from the metric, excludes `lib/durababble/version.rb` because Bundler loads that gem metadata before SimpleCov starts, prints the SimpleCov summary in CI logs, and writes the HTML report plus SimpleCov result JSON to `coverage/` for CI artifact upload. Meaningful tests should raise the configured minimums as coverage improves, and the minimums must not be lowered without an explicit spec update.

## Boundaries and anti-goals

These constraints are part of the v1 contract:

- No block-form durable object `.with` API in v1.
- No process-wide monkeypatching for determinism.
- Durable objects are not boxed by default.
- No cross-object transactions or full distributed-actor semantics.
- No split-brain tolerance beyond database invariants.
- No durable queue/cron replacement; integrate with an adjacent scheduler/queue if product needs exceed simple durable sleeps/waits.
- No streams API until a real consumer requires it.
- No automatic cross-pool routing; relocation/failover is explicit.
- No silent payload spill to blob storage; oversized values fail loudly.
- MySQL/MariaDB support is required for the common public contract.
- Worker registry misses are avoided by claiming only workflow/object classes present in the supplied registry. Enqueuing a workflow name with no corresponding worker pool leaves it pending until an appropriate pool starts.
- Long-running steps do not heartbeat automatically while user code runs.
- Class-level serialized state migration and node capability routing are outside the v1 contract.
