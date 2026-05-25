# Durababble specification

This document is Durababble's durable-execution contract. It describes the behavior the implementation must provide and the evidence expected from tests, so a reviewer can compare the running system to the intended system without reconstructing history from implementation notes.

## Scope

Durababble is a Ruby library packaged as a gem. Ruby owns workflow and durable-object definitions; SQL owns durable coordination, replay, recovery, leases, inboxes, and retained state.

Durababble exposes two durable primitives:

| Primitive | Class | Public handle API | Best for | Mental model |
| --- | --- | --- | --- | --- |
| Durable workflow | `Durababble::Workflow` | `Workflow.start` / `Workflow.handle` | Finite executions with a start, result, steps, waits, retries, cancellation, and recovery | A function or process that survives restarts |
| Durable object | `Durababble::DurableObject` | `DurableObject.at` / `DurableObject.tell` | Sessions, carts, conversations, agents, per-shop workers, or other id-addressed state | A SQL-backed actor/mailbox object with a lease owner |

Workflow and object calls compose. A workflow can call a durable object, and a durable object command can start or signal workflows. Child durable calls inherit the caller's worker pool unless explicitly overridden.

Storage works through PostgreSQL/YSQL (`postgresql://` / `postgres://`) and MySQL/MariaDB (`mysql://` / `mysql2://`). Both adapters must provide the same public durable semantics, with backend-specific SQL hidden behind shared conformance tests and backend-specific locking/query-plan tests where query shape matters.

Runtime values are serialized with Paquito into binary columns. PostgreSQL/YSQL stores runtime payloads as `bytea`; MySQL/MariaDB stores them as `LONGBLOB`. Runtime payloads include workflow inputs/results/errors, step args/results, wait contexts, inbox payloads, durable object state, command args/results/errors, idempotency fingerprints, and heartbeat cursors.

The default storage namespace is `DURABABBLE_SCHEMA` when set. Otherwise it is derived from `Durababble.workspace_schema(DURABABBLE_WORKSPACE_ROOT || Dir.pwd)` so concurrent worktrees do not share internal tables by accident. PostgreSQL/YSQL uses the selected namespace as a SQL schema; MySQL/MariaDB uses it as the durable table prefix inside the configured database.

The contract specifies behavior, not a mandatory internal isolation technology. Workflow execution must be deterministic, workflow-local, and free of process-wide monkeypatching, but the implementation may choose the isolation mechanism that provides those guarantees.

## Terminology

- **Durable target:** a workflow execution or durable object instance addressed by class/type, id, and worker pool.
- **Worker pool:** a persisted routing and execution boundary. Workers in a pool may claim, route, wake, and execute only targets assigned to that pool.
- **Lease:** a persisted ownership record for a durable target, with the owner gRPC address as the routable node identity, a lease token, and a deadline.
- **Workflow command:** a deterministic workflow operation such as a step schedule, wait, signal delivery, child workflow command, or patch marker.
- **Command id:** the replay identity assigned by deterministic workflow execution order.
- **Attempt id:** the execution identity for one concrete try of a command. Retries create new attempts for the same command id.
- **Activation:** one deterministic workflow run/replay slice that processes runnable fibers until they finish, block on durable work, or reach a safe suspension point.
- **Inbox/mailbox:** the durable per-target message stream used for object asks/tells/wakes and workflow signals/commands.

## Public programming model

### Workflows

A workflow subclasses `Durababble::Workflow` and implements `#execute(input)`. `#execute` is deterministic orchestration code. Side-effecting work must be expressed as durable step methods declared with `step`.

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

`Workflow.start(input, id: nil, idempotency_key: nil, worker_pool: nil)` creates a durable pending execution before any worker can run it and returns a workflow handle. `Workflow.handle(workflow_id)` returns a query/management handle for status, result, cancellation, resume, signals, and exposed methods.

Idempotent start scopes caller keys to worker pool, workflow class, operation kind, and argument fingerprint. The same key with the same shape returns the same handle; the same key with a different shape raises `Durababble::IdempotencyKeyConflict`.

`step_context` is available only while a workflow step is executing. It exposes `workflow_id`, `command_id`, `attempt_number`, `idempotency_key`, and `heartbeat`. Idempotency keys are generated from durable coordinates and remain stable across retries of the same logical command.

`expose` declares non-durable transient methods. Transient methods are invoked through owner-local RPC against the live workflow owner, must not mutate durable state, and must not accept `idempotency_key:`.

`expose_command` declares durable workflow command methods. A command call commits a durable inbox ask/tell row before returning to the caller, wakes the active workflow owner through `DeliverMessage` or leaves a durable target activation, and executes against the workflow at the next safe deterministic yield point. Workflow authors do not park on a matching `wait_event` or poll an inbox manually. Synchronous command APIs wait for the ask row to store a serialized result or typed error; retrying with the same idempotency key reattaches to the same row.

`signal def handler` declares deterministic workflow signal handlers. `handle.signal(:name, **args, idempotency_key:)` commits a durable signal message before returning and fails for terminal workflows.

Workflow code may use durable timer and event waits through workflow helper methods or the module-level helpers: `wait_until(time, context)`, `wait_event(event_key, context)`, `Durababble.wait_until(time, context)`, and `Durababble.wait_event(event_key, context)`. `Store#signal_event(event_key, payload:)` wakes matching event waits.

Durable sleep helpers such as `Durababble::Workflow.sleep(duration)` and `sleep_until(time)` are timer waits with workflow-friendly API shape.

`Durababble::Workflow.wait_condition(timeout: nil) { ... }` blocks a workflow fiber until the predicate is true or a durable timeout fires. Durable sleeps are implemented as timer waits and must survive process exit.

### Durable objects

A durable object subclasses `Durababble::DurableObject`. It is addressed by `Class.at(id, worker_pool: nil, idempotency_key: nil)` for proxy calls and by `Class.tell(id, :method, **args, idempotency_key: nil)` for asynchronous durable commands. Durable object methods are not workflow steps.

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

`expose` registers transient non-durable owner-local methods. Transient methods read latest persisted state, may run concurrently with other transient methods, must not enqueue inbox messages, call workflow steps, mutate durable state, schedule sleeps, or write Durababble tables, and reject `idempotency_key:`.

`expose_command` registers durable mailbox commands. Commands execute through the durable object's identity, receive `command_context`, and update state only through `update_state(new_state)`. `command_context.idempotency_key` is generated from object type, object id, and mailbox message id and is stable for the durable command.

Synchronous asks and asynchronous tells share the same mailbox, so asks cannot overtake earlier tells. `tell` validates that the target method is an `expose_command`. Enqueuing a ready object command wakes the active owner through `DeliverMessage` or leaves a durable target activation for pool-local recovery. Object authors do not poll for commands; the owner drains the mailbox when woken or claimed.

Lifecycle callbacks are `on_create`, `on_load`, `on_wake(payload: nil)`, and `on_destroy`. They are lifecycle hooks, not remotely callable public methods.

Durable objects support one pending `sleep_until(at:, payload: nil)` wakeup per object id. `sleep_until` atomically replaces the pending sleep row in the same transaction as the command state write. `cancel_sleep` removes it. Matured sleeps convert atomically into durable mailbox wake messages.

Management operations exist for operator use: `list`, `find`, `pause`, `resume`, `cancel`, `destroy!`, `evict`, and explicit `relocate_worker_pool`.

Durababble does not provide a block-form durable-object `.with(id) { ... }` API. Multi-method atomicity is expressed by writing one command method that performs the full operation.

### Typing

The runtime does not load or validate user RBS. The gem ships `sig/durababble.rbs` with `Durababble::Workflow[Input, Output]` and `Durababble::DurableObject[Id, State]` generics for static tooling only; runtime serialization remains Paquito-based.

## Workflow execution semantics

### Deterministic orchestration

Workflow orchestration runs on a Durababble-managed deterministic scheduler. Workflow code without async fanout runs as the root workflow fiber. Raw `Async { ... }` and `Async::Task#async` create additional deterministic workflow fibers when called from workflow orchestration code.

Durababble integrates with Async task creation and waiting so ordinary non-transient Async child tasks inherit workflow execution context and may call durable steps. Workflow authors must not need Durababble-specific async helpers. `transient: true` Async tasks do not inherit workflow execution context and must not call durable steps.

Workflow orchestration code must not perform direct blocking or nondeterministic I/O. It may schedule durable steps, sleeps, waits, signals, child workflows, and deterministic local computation. Step bodies run outside the deterministic scheduler and may perform process-local side effects.

The workflow runtime must provide workflow-local deterministic behavior for time, sleep, randomness, UUID generation, and workflow futures/fibers. The implementation must not rely on process-wide monkeypatching to create determinism.

Durable commands called from any workflow fiber are assigned command ids when the workflow fiber reaches the call, before the side-effecting implementation runs. The runtime and replay system must not assume that a workflow will stay single-fiber across code changes.

### Workflow command history

Workflow replay is driven by an append-only per-workflow command/event history. Latest-state tables such as `steps` are query caches and recovery aids, not the replay source of truth.

Step scheduling, step execution starts, and step completions are distinct durable facts. A schedule event records command id and full replay-relevant command shape before any local or remote executor starts the side effect. A start event records that an executor began a concrete attempt. A completion or failure event resolves the command's workflow future. Workflow-level waits use separate wait history.

Schedule event shape includes step method name, serialized args/kwargs or a stable payload digest, retry/executor attributes, and any semantic key if one is present. Replay validates the scheduled command shape even when no completion exists, so a step that started before a crash cannot disappear silently.

Replay shape checks apply to every scheduled durable command, not only completed steps. If a previous run scheduled command `17` as `fetch_profile(user_id: 1)` and replay schedules command `17` as `fetch_profile(user_id: 2)` or `send_email`, the workflow is nondeterministic even if the original command only reached `started`.

Step completions, step failures, timer fires, signal deliveries, and child-workflow completions are external events that make workflow fibers runnable. They must be appended to history and delivered to the deterministic scheduler in history order, because completion order can affect later command order.

Process-local step execution may run multiple step attempts concurrently, but attempts must not concurrently mutate workflow history through one non-threadsafe store connection. History mutations are serialized per workflow or protected by executor-local connections/transactions with durable command ids and optimistic checks.

### Steps, retries, and heartbeats

Workflow steps are method-level durable side-effect boundaries. On first execution, the runtime records a scheduled command, records a running attempt, runs the method outside the deterministic workflow scheduler, stores the serialized result, and marks the command completed.

On resume, completed command results are returned from durable state and the step body is not re-run. Incomplete, running, and failed commands are retried or continued according to durable state.

If the process crashes after an external side effect but before the checkpoint commits, the step may run again. Step implementations are expected to pass `step_context.idempotency_key` to external systems that support idempotency.

No workflow row lock is held while user step code runs. The executor holds a renewable lease and fences durable writes with active lease ownership. If the lease is lost while step code is running, the external side effect may still finish, but checkpoint/status writes fail and recovery follows the normal idempotent retry path.

Step attempts are append-only. Retries and stale attempts remain inspectable.

Step retry policy is declared at the step method definition site:

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

`schedule: [1, 5, 30]` supplies an explicit per-retry schedule. After the explicit array is exhausted, Durababble falls back to capped exponential backoff. Intervals are numeric seconds. `maximum_attempts:` counts the first execution plus retries. `non_retryable_errors:` accepts Ruby exception classes or class-name strings.

On a retryable failure, the runtime records the step attempt as failed, releases the workflow lease, stores `next_run_at`, and returns the workflow to a claimable retry state after the retry deadline. Pending or failed workflows whose `next_run_at` is in the future are not claimable. Terminal failed workflows clear `next_run_at` and are not claimable.

Active workflow leases can be extended. Step heartbeats (`step_context.heartbeat.record(cursor)`) compare-and-swap against current workflow lease owner/deadline, extend `locked_until`, and store an opaque Paquito-serialized cursor on the current step/attempt. Long-running steps do not heartbeat automatically; user code must heartbeat before the lease deadline or choose a long enough lease.

On retry, `step_context.heartbeat.cursor` exposes the latest cursor from the previous incomplete invocation.

### Async fanout

Raw Async scatter/gather must be durable and replayable:

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

Durababble records a `step_scheduled` history event for each `fetch_profile(id)` command before dispatching that step body. The recorded command shape must distinguish `fetch_profile(1)` from `fetch_profile(2)`.

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

If `fetch_profile(2)` completes before `fetch_profile(1)` and schedules `score_profile(...)` first, replay must resume workflow fibers in the same history-recorded completion order. It must not resume fibers based on Ruby scheduler timing, database query order, or synchronous return of already-completed step results.

### Waits and signals

Timer waits persist a wake time and Paquito context, suspend the root workflow, and resume when the wake time is due. `wait_until` must run from workflow execution's root task, not from durable steps, non-workflow callers, or child Async branches.

External event waits persist an event key and Paquito context. `Store#signal_event` completes matching workflow-level waits with Paquito payloads and wakes workflows. Concurrent signalers must wake a wait once. `wait_event` must run from workflow execution's root task, not from durable steps, non-workflow callers, or child Async branches.

Step methods must not wait, sleep, or return `WaitRequest` values. A wait can last forever, so suspending from inside a side-effecting step would leave step attempts in an unsafe state.

Workflow signals are durable inbox/history messages delivered to declared signal handlers at deterministic workflow yield points. Signal acceptance is idempotent by message id and fails for terminal workflows.

Workflow commands are durable workflow inbox messages delivered to the workflow lease owner. A committed command must make the target runnable immediately. If a live owner holds the workflow lease, the runtime sends `DeliverMessage` after commit; if there is no live owner or advisory delivery fails, the durable target activation remains claimable by the worker pool. The command executes before a new durable workflow command starts, after a step completes/fails/waits, when a running step heartbeats or otherwise yields to the workflow engine, or when a sleep/wait activation is interrupted by message work. A non-heartbeating user step body is not preempted mid-Ruby-frame; the command runs when the active owner reaches a safe point or lease expiry/recovery gives the target to another worker.

Synchronous workflow command APIs are durable asks. The caller waits for the inbox row to store a serialized result or typed error, then returns/re-raises it. If the caller times out or loses its connection after the row commits, the durable command is not canceled; retrying with the same idempotency key reattaches to the same inbox row. Command responses do not require the general outbox table because the response is a one-to-one property of the ask row; workflow/object code may still write ordinary outbox messages when a command handler needs durable delivery to an external system.

### Cooperative cancellation

Cancellation is cooperative execution, not hard termination.

- `Workflow.handle(workflow_id).cancel(reason:)` records the first durable cancellation reason and request timestamp. Duplicate requests return the current run and preserve the first reason.
- Pending, waiting, and retry-backoff workflows move to `canceling`, clear `next_run_at`, and become claimable immediately. Pending waits are marked canceled so late timer/event signals cannot resume the canceled wait.
- Running workflows keep their active lease. Cancellation is observed at deterministic yield points: before a new durable command starts, when replay reaches a completed command boundary, after a step completes, and when a running step heartbeats.
- Delivery raises `Durababble::CancellationError` with the durable reason and workflow id. Cleanup steps run as ordinary durable steps under the same command-history replay model as all other workflow work.
- If workflow code catches cancellation and returns after cleanup, the engine records the workflow as `canceled` and stores the cleanup result. Re-raising `CancellationError` also records `canceled`.
- If cleanup raises an unrelated error, ordinary step retry policy applies. Retryable cleanup failures remain `canceling` with `next_run_at` set and become claimable again only when due. Exhausted or non-retryable cleanup failures mark the workflow `failed`.
- Child workflow APIs must require an explicit child-cancellation policy. Parent cancellation must not silently terminate child work or report `canceled` before the selected child policy reaches a durable outcome.
- Operator termination is a distinct hard-stop operation. It may stop work without running cleanup, but it uses a separate state/API and must not report cooperative cleanup as completed.

### Workflow code evolution

`patched(patch_id)` and `deprecate_patch(patch_id)` are the workflow control-flow compatibility APIs. They protect deterministic changes to step order, waits, signal handling, and other durable workflow branches by recording/checking patch markers in workflow history before new-branch durable events are emitted.

```ruby
class FulfillOrder < Durababble::Workflow
  def execute(order)
    if patched("ship-after-tax")
      quote_tax(order)
      ship(order)
    else
      ship(order)
      quote_tax(order)
    end
  end
end
```

Rules:

- `patch_id` is a stable, non-empty string unique to one logical code change. Do not reuse a removed id for an unrelated change.
- `patched` is valid only in deterministic workflow orchestration code: `execute`, signal handlers, and `wait_condition` predicates/continuations. Calling it inside a step, durable object command, exposed transient method, or arbitrary library code outside active workflow execution raises a typed error.
- The first `patched(patch_id)` call for a workflow execution is event-bearing. Later calls with the same id in the same execution return the memoized decision and append no duplicate marker rows.
- When live execution reaches a history point with no persisted event, `patched(patch_id)` appends a normal patch marker and returns `true`. The marker commit must happen before the new branch produces steps, waits, signals, or other durable workflow events.
- When replaying history that already contains a normal marker for `patch_id`, `patched(patch_id)` consumes that marker and returns `true`.
- When replaying history that reached the change point without a marker, `patched(patch_id)` returns `false` and appends nothing, so workflow code follows the branch matching existing history.
- If persisted history contains a normal patch marker that workflow code does not consume with `patched` or `deprecate_patch`, the checker raises nondeterminism before any further durable writes. Patch-id mismatches and out-of-order marker consumption also raise nondeterminism.
- Patch markers are workflow-history markers, not inbox messages, state schema versions, or node-capability routing signals.

Patch lifecycle:

1. Introduce a patch by deploying `if patched("id") { new } else { old }`. New or not-yet-reached executions record a marker and run the new branch; executions already past the change point without a marker run the old branch.
2. Deprecate the patch after no live workflows need the old branch by deploying `deprecate_patch("id")` plus the new code only. This keeps marker-aware histories replayable while removing the old branch.
3. Remove the marker call after all relevant histories have completed and aged out of retention. Never reuse the id.

Admin and observability surfaces expose patch usage by workflow type/id: normal marker, deprecated marker, and open workflows with no marker for a given patch id.

## Durable object execution semantics

Object commands, scheduled wakeups, and other mailbox work acquire an exclusive writer slot for the target. Exposed transient methods acquire shared read access, can run concurrently with each other, and stop entering once mailbox work is waiting.

Object inboxes are push-driven. The owner pod drains commands, wakes, and internal work from the mailbox and invokes registered command/lifecycle methods against the cached instance. Ready object commands wake the active owner through `DeliverMessage` or leave a durable target activation for pool-local recovery.

Commands execute one at a time in strict FIFO order per durable target. Target executors drain only the contiguous ready prefix from the mailbox head. `SKIP LOCKED` must not let later messages overtake a blocked head for the same target.

If the mailbox head is waiting for backoff, paused, dead-lettered, or otherwise blocked, later messages for the same target must not run. A dead-lettered or backed-off head remains blocking until an operator retries, skips, cancels, destroys, or repairs the target.

Object message completion, state write, sleep updates, and mailbox head advancement are one fenced transaction. A crash cannot leave state persisted without the corresponding ask result/message completion, or message completion without the corresponding state write.

Ask rows store serialized result or error. Tell/wake rows retry with backoff and move to dead-letter after `max_message_attempts`.

A command's inbox row owns a stable `operation_id`. Any durable checkpoints inside that command use the operation id so retries skip completed side-effect checkpoints.

Durable object cache entries include `{instance, lock_version, lease_token, last_used_at, gate}` and must never invoke user code when the lease is expired or near its refresh threshold.

## Shared durable primitives

### Idempotency

Durable operations use both library-generated keys and caller-provided keys:

- Step idempotency keys are generated from workflow id plus deterministic command id and are available through `step_context.idempotency_key`.
- Durable object command idempotency keys are generated from object type, object id, and mailbox message id and are available through `command_context.idempotency_key`.
- Every public durable entry point accepts `idempotency_key:` where durability is implied: workflow starts, object asks, object tells, workflow signals, and externally exposed operator APIs.
- Idempotency keys are scoped to worker pool, target, operation kind, method, and argument fingerprint, not just target id.
- Same key plus same operation shape returns the existing handle/result or re-raises the saved error. Same key plus different operation shape raises `Durababble::IdempotencyKeyConflict`.
- Caller timeout after a durable command, signal, or message commits does not cancel durable work. Retrying with the same idempotency key reattaches to the same row.
- Transient `expose` methods are not durable and must not accept idempotency keys.

### Fences

`Store#with_fence` deduplicates workflow-local side-effect blocks. It inserts or acquires a fence before the side-effect block executes so concurrent callers do not duplicate the side effect.

Same key plus same shape waits for or returns the first completed result. Same key plus different shape raises a conflict. Fence owner crash recovery must be explicit and operator-visible; Durababble must not silently rerun an abandoned fenced side effect without a defined recovery action.

### Outbox

Outbox rows have unique message keys, leasing, expiry recovery, and acknowledgement. Outbox rows are claimable by one sender at a time, reclaimable after lease expiry, and acknowledged only after external delivery.

The public workflow/object API does not expose outbox as a first-class user concept.

## Storage and schema requirements

Durababble persists the following logical entities. Physical table names may be namespaced or adapter-specific, but durable semantics are common across supported SQL backends.

| Entity | Purpose | Required key/query shape |
| --- | --- | --- |
| `workflows` | Workflow execution status, input/result/error, retry due time, cancellation metadata, and workflow metadata | Workflow id, workflow class, worker pool, status, due time |
| `workflow_history` | Append-only ordered replay events for workflow commands, completions, signals, timers, and markers | `(workflow_id, event_index)`, command id lookup |
| `steps` | Latest logical workflow command state for query/result caching | `(workflow_id, command_id)` |
| `step_attempts` | Append-only attempt history for step execution | Attempt id, workflow id, command id, status |
| `waits` | Durable workflow-level timer and external-event waits | Wait id, workflow id, scope, position, due time/event key/status |
| `leases` | Pool-scoped ownership for workflow and object targets, including the owner gRPC address | `(worker_pool, target_kind, target_class, target_id)` |
| `inbox` | Durable asks, tells, wakes, workflow signals, and workflow command messages | Target identity, sequence, status, message id |
| `mailbox_sequences` | Per-target monotonic mailbox sequence allocation | Target identity |
| `target_activations` | Coalesced runnable target wakeups for inbox/scheduler work | Worker pool, ready time, target identity, status |
| `idempotency_keys` | Public durable operation deduplication and shape conflict detection | Operation scope plus caller key |
| `fences` | Workflow-local side-effect idempotency fences | `(workflow_id, key)` |
| `outbox` | Durable outgoing messages with processing leases | Id, unique message key, lease expiry |
| `durable_objects` | Latest object state by object type/id | `(worker_pool, object_type, object_id)` |
| `object_sleeps` | Pending durable object wakeups | Object identity and sleep id |

Query-shape and transaction requirements:

- Claim paths use `FOR UPDATE SKIP LOCKED` where supported.
- Persist immutable `worker_pool` on durable targets whose routing, claiming, scheduling, listing, or recovery semantics are pool-scoped.
- Include `worker_pool` in primary keys and indexes when query patterns need to filter, route, claim, recover, or list by worker pool. Do not add it to keys whose query patterns do not care about worker pool.
- Workflow/object ownership uses the unified leases table keyed by pool-scoped target identity, with the owner gRPC address as the node identity plus `lease_token` and `lease_until`.
- Durababble does not require a separate nodes table for direct target routing. A caller that needs to wake or RPC a target reads the target's fresh lease row and dials the gRPC address stored there; when no fresh lease exists, the durable target activation remains the correctness path until a worker claims it.
- A worker that claims a target activation for a target currently leased by another fresh owner must forward `DeliverMessage` to the gRPC address in the lease row and re-arm the activation on a bounded retry, rather than parking it until lease expiry.
- Object asks/tells/wakes and workflow signals/commands use the unified inbox. Inbox rows are durable message/result records, not a second globally polled work queue.
- Sequence allocation, inbox insert, and ready-target activation upsert commit in one transaction.
- Per-target mailbox sequence state lets `enqueue_message` allocate a monotonic sequence and lets target executors drain only a contiguous ready prefix from the head.
- Enqueueing a ready inbox message must atomically upsert a target activation row, or an equivalent scheduler row, keyed by worker pool and durable target identity. Workers globally poll target activations, not the inbox table; one activation can cover many pending inbox rows for the same target.
- Target activation completion is conditional on the target mailbox/head state: after draining, the executor clears the activation only if no ready inbox row remains, otherwise it keeps or re-arms the activation with the next due time.
- If a worker claims a target activation but finds the target is still owned by another fresh lease, it must not hot-loop the activation; it relies on advisory `DeliverMessage` for the live owner and re-arms the durable fallback no earlier than the observed lease deadline.
- Object sleep rows are keyed by object identity plus worker pool when sleep dispatch is pool-scoped, with `sleep_id`, `wake_at`, and Paquito payload.
- Append-only workflow history rows are ordered per workflow. Required event families include `step_scheduled`, `step_started`, `step_completed`, `step_failed`, workflow-level timer/wait events, signal delivery events, child-workflow events, and patch marker events.
- Store deterministic command ids and replay-relevant command shape on schedule events. Store concrete attempt ids on start/completion/failure events. The command id is the replay identity; the attempt id is the execution/retry identity.
- Mutable latest-state rows are not the replay source. Replay uses ordered schedule history; deterministic scheduling uses history-ordered future resolution events; execution recovery uses distinct attempt start/completion events.
- Workflow wait rows are committed before the workflow row is released to `waiting`. Event/timer wake queries only make externally visible progress once the workflow is durably suspended.
- Explicit idempotency rows cover workflow starts and any public durable operation not deduped by the inbox itself.
- Queue/recovery indexes cover workflow claims, due retries, expired workflow leases, event waits, timer waits, step-attempt lookup, outbox claims, and mailbox status scans.
- High-risk transactional pieces such as `enqueue_message`, target-head drain/advance, sleep-to-inbox conversion, and object state plus message completion may be implemented with database functions to reduce lock-order drift, provided the common backend contract is preserved.
- Retention and partitioning must be planned for high-volume history, step attempts, inbox, and idempotency rows before production scale.
- Runtime value decoding only decodes known serialized binary columns.
- MySQL/MariaDB and PostgreSQL/YSQL must pass the same store backend conformance suite.

All public durable semantics must work on PostgreSQL/YSQL and MySQL/MariaDB. Schema work that would otherwise rely on backend-specific features such as partial indexes, `RETURNING`, `ON CONFLICT`, `BYTEA`, or `gen_random_uuid()` must either define equivalent behavior in the backend abstraction and conformance tests or explicitly keep the feature out of the common public contract.

## Workers, leases, routing, and scheduling

### Worker lifecycle and recovery

A worker pool is served by processes repeatedly calling `Durababble::Worker#tick` or `#run_until_idle`. Each tick claims runnable work whose workflow or object class is present in the worker registry, including workflow rows and coalesced target activations, then resumes it through the deterministic workflow executor or durable object mailbox executor.

Runnable workflows are pending rows, retryable failed rows whose non-null `next_run_at` is due, canceling rows with no live lease whose `next_run_at` is null or due, and expired running leases that are recoverable. Terminal failed rows with no retry deadline are not claimable.

`Durababble::WorkerRuntime` is the preferred app/process lifecycle entrypoint. It loops `Worker#tick` for one worker pool, stops taking new claims on shutdown, waits for in-flight work up to a timeout, and releases still-held workflow/outbox leases if the timeout is exceeded.

`Engine#resume` refuses to execute work owned by another live worker. Lease holders must re-check ownership before mutating durable state.

Expired leases are reclaimed by claim paths or recovery sweeps. Recovery does not require a separate coordinator for correctness; any worker serving the pool may move expired work back to a claimable state.

### Worker-pool requirements

- Every durable target whose execution/routing is pool-scoped has an immutable persisted `worker_pool` selected at first materialization.
- If a class declares a default pool, that pool wins unless the caller overrides while creating a new durable unit. Once the row exists, the persisted pool wins.
- Worker pools are the routing and multiregion boundary. A pod in another pool cannot claim, route, or wake a target unless the target is explicitly relocated.
- Scheduler scans filter by pools served by the local pod.
- `DeliverMessage` and `CallTransient` route to the gRPC address in the target's fresh lease row. `AwakenBatch` is sent only to explicitly configured peers in the target pool.
- Automatic cross-pool stealing is forbidden. Regional failover is explicit `relocate_worker_pool` operator/runtime work that quiesces the target, releases the old lease, updates the row, and wakes it in the new pool.

### Sticky placement

Routing keeps hot ids on the pod that already has them in memory:

- Every pod exposes `rpc_address = "#{POD_IP}:#{DURABABBLE_RPC_PORT}"`, and worker runtimes use that address as the lease owner identity.
- Every acquired lease writes the owner gRPC address and a fresh `lease_token` into the lease row.
- Lease acquisition uses a hot in-memory cache when the owner has a fresh lease and falls back to atomic SQL acquisition/lookup when cold, near expiry, or routed remotely.
- A `LeaseRenewer` refreshes in-flight leases every `lease_ttl_ms / 3` and only when the `lease_token` still matches.
- Cache entries are evicted on near-expiry, `EvictLease`, object CAS conflict, lease-renew failure, idle timeout, or LRU capacity pressure.
- Idle owners stop renewing after `idle_eviction_ms` and release leases so the next pool-local caller can acquire ownership.

## Inter-node RPC

Remote intranode/inter-pod communication uses gRPC over mTLS/Spiffe. Each pod runs a dedicated `Durababble::RpcServer` using the `grpc` gem's `GRPC::RpcServer`, bound to `rpc_host:rpc_port` with its own thread pool. Peer identity comes from Spiffe, and Durababble additionally authorizes peers through an allowed service-account list.

The service shape is part of the runtime contract:

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

RPC semantics:

- **AwakenBatch** is a latency optimization after workflow starts or matured scheduler rows. It never replaces durable DB state or scheduler correctness.
- **DeliverMessage** wakes an owner for already-committed inbox rows. It carries no user payload; the receiver queries durable inbox rows and schedules an owner-local target activation without blocking the gRPC handler on user code. If the receiver no longer owns the lease, it returns success without work and scheduler recovery remains the correctness path.
- **CallTransient** is non-durable RPC for exposed methods against the active owner. It returns a Paquito result, a remote error, `not_running`, or `LeaseMoved`.
- **EvictLease** asks a pod to drop a cached lease it may no longer own.
- Connection failure to an owner causes short retry, lease re-check, and reroute. If wakeup still fails after the retry budget, the already-committed target activation remains the correctness path and is eventually claimed by a worker in the target pool.
- Receivers reject stale in-flight messages unless they still own the target before and after handler execution.
- gRPC is required for production cross-pod calls because strongly typed protos catch shape drift and mTLS is available. Auxiliary test transports must not be used for production intranode communication.

## Configuration, limits, and operations

Durababble exposes process-wide configuration for storage, worker pools, RPC, leases, scheduling, payload limits, and observability:

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

Size guards are production requirements. Durababble warns at configured thresholds and raises `Durababble::PayloadTooLarge` when serialized workflow args, step outputs, or object state exceed max bytes. The runtime does not silently spill oversized values to blob storage.

Operational surfaces include CLI migration, workflow run/resume, inspection, version output, built-in bounded listing/finding of workflows and objects, and operator actions for dead-lettered mailbox heads: retry now, skip, cancel target, destroy target, and repair/decode failed payloads.

Observability requirements:

- Benchmarks record operation latency, throughput, and allocation reports for workflow queueing, leases, waits, outbox, fences, inbox, deterministic workflow replay, and local step execution.
- OpenTelemetry support is an optional integration over the official API gems. `Durababble.configure_observability(enabled: true, attributes:)` uses `OpenTelemetry.tracer_provider` and `OpenTelemetry.meter_provider`; Durababble never configures an SDK, collector, or exporter for the application.
- With observability disabled, instrumentation exits through cheap no-op checks before dynamic attributes are built. Durababble callsites construct valid, low-cardinality OpenTelemetry attribute hashes directly; application-provided static attributes should use string keys and OpenTelemetry-compatible scalar values.
- Stable span names include `durababble.workflow.start`, `durababble.workflow.resume`, `durababble.workflow.execute`, `durababble.workflow.step`, `durababble.object.query`, `durababble.object.command.enqueue`, `durababble.object.command`, `durababble.workflow_rpc.*`, `durababble.rpc.client.*`, and `durababble.rpc.server.*`. Durababble does not wrap ActiveRecord SQL execution in its own spans; applications should use standard ActiveRecord/database OpenTelemetry instrumentation for SQL visibility.
- Stable span and metric attributes use the `durababble.*` namespace and may include workflow id/name/status, step name/index/attempt, object type/id/method, worker pool/id, lease owner, wait kind/event key, retry delay, store backend for higher-level queue metrics, RPC method/target shape, and `error.type`. Instrumentation must not attach SQL text, serialized payload bytes, raw user arguments, secret-bearing values, or unbounded free-form payload data.
- Metrics include workflow start/completion/failure/cancellation counters, step attempt/success/failure/retry counters, wait start/completion counters and wait latency histograms, queue claim latency, lease heartbeat/conflict/expired-recovery counters, outbox pending/processed/failure counters, worker tick duration/counts, and workflow replay/history size measurements. Cancellation/termination, explicit outbox delivery failure, richer replay cost, and object mailbox queue-depth metrics remain reserved where the runtime feature is not implemented yet. Applications should use ActiveRecord/database OpenTelemetry instrumentation for SQL operation latency/error metrics.
- StatsD counters/timers cover command ask latency, exposed method latency, mailbox queue/execution latency, `CallTransient`, step execution, replay frequency, recovery sweeps, sleep dispatch, lease acquisitions/forwardings/takeovers, lease-cache hit ratio, and object-cache hit ratio.
- OpenTelemetry spans wrap public calls, workflow executions, steps, durable-object commands/queries, workflow RPC routing, and inbound gRPC requests. Spans include worker pool, class, target id, and lease owner where relevant.
- Bugsnag/error integration reports unhandled exceptions inside commands, exposed methods, steps, and gRPC handlers.
- Slow-step warnings are emitted.
- Routing health metrics cover wakeup error rate, wakeup latency, and lease takeover frequency.
- Circuit breakers around database connections cause public methods to raise a typed error such as `Durababble::CircuitBreakerOpen` when the durable store is unavailable before commit.
- gRPC server health metrics cover in-flight requests, handler-thread saturation, and dropped requests.

## Guarantee matrix

| Guarantee | Required behavior | Validation expectation |
| --- | --- | --- |
| Workflows are durable before execution | Starting a workflow commits a pending row with Paquito-serialized input before any worker can execute it. | Complete spec guarantee plus crash matrix |
| Runnable work is claimable by one worker at a time | Claim paths atomically assign one live owner using adapter-appropriate row locking. | Backend conformance plus hardening concurrency specs |
| Resume honors lease ownership | A worker may execute only work it owns; another live owner causes `LeaseConflict` or equivalent refusal. | Hardening lease spec |
| Active leases can be heartbeated | Lease heartbeats extend ownership only for the owning worker and matching lease token/deadline. | Complete spec guarantee matrix |
| Running steps can explicitly heartbeat progress | Step heartbeats extend the workflow lease and store an opaque Paquito cursor on the attempt. | Heartbeat spec plus DST cursor recovery scenario |
| Heartbeat cursors survive recovery | A retried incomplete attempt receives the latest stored heartbeat cursor. | Heartbeat spec plus DST cursor recovery scenario |
| Zombie workers cannot renew expired leases | Heartbeat writes fail after lease owner/deadline mismatch. | Heartbeat spec |
| Zombie workers cannot complete after lease revocation | Terminal workflow/step writes re-check lease ownership before commit. | Worker lifecycle spec |
| Step retries are durably scheduled | Retryable failures store `next_run_at`, release the lease, and are not claimable early. | Step retry spec plus DST retry scenario |
| Retry options are Temporal-like but Ruby-shaped | `initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`, `schedule`, and `non_retryable_errors` define retry policy. | Retry policy specs |
| Final retry failure bubbles to workflow | Exhausted or non-retryable step failure marks the workflow failed. | Step retry spec |
| Expired leases can be recovered | Expired running work returns to a claimable state. | Complete spec guarantee plus crash matrix |
| Completed steps are not re-executed on resume | Completed command results are returned from durable state. | Complete spec guarantee plus subprocess crash harness |
| Incomplete steps are retried | Incomplete/running/failed command state is retried or continued according to durable state. | Crash matrix |
| Workflow command history is replay truth | Schedule events, start events, completion/failure events, and workflow-level wait history are distinct append-only history facts. | Async workflow replay specs plus backend conformance |
| Parallel schedule shape is validated | Replay validates method, args/kwargs digest, retry/executor attributes, and semantic key for every scheduled command, including incomplete commands. | Fanout replay/nondeterminism specs |
| Workflow future resolution is deterministic | Step completions, failures, timer fires, signals, and child completions resume workflow fibers in history order. | Continuation fanout replay specs |
| Step attempts are append-only | Every started attempt and terminal attempt state remains inspectable. | Guarantee matrix |
| Timer waits survive process exit | Timer wait rows store wake time and serialized context. | Timer/event tests |
| Event waits survive process exit | Event wait rows store event key and serialized context. | Timer/event plus crash matrix |
| Signaled waits resume with payload | Matching event delivery completes waiting workflow fibers with Paquito payloads. | Timer/event test |
| Concurrent signalers wake a wait once | Concurrent signal delivery uses locked/idempotent updates. | Event concurrency spec |
| Side effects can be fenced by key | A fence records `running` before yielding and exposes operator-visible recovery for abandoned owners. | Fence concurrency spec plus owner-crash spec |
| Outbox delivery is durable and leased | Outbox rows are unique by key, claimable, acknowledgeable, and reclaimable after expiry. | Outbox specs |
| Workflow commands wake and run promptly | Command enqueue wakes the active owner or leaves a durable target activation; no workflow-side `wait_event` is required. | Workflow command mailbox specs plus gRPC wakeup specs |
| Synchronous durable commands return results | Ask rows store serialized result/error and caller retries with the same idempotency key reattach. | Workflow/object ask specs |
| Inbox is not a second global polling queue | Workers poll coalesced target activations and target owners drain inbox rows for their own target. | Query-plan and mailbox specs |
| Workflow RPCs route to active lease holder | RPC routing validates owner before/after handling, refreshes ownership after transport failures, and reroutes. | Workflow RPC spec plus gRPC transport spec plus DST scenarios |
| Inter-pod RPC uses full four-method gRPC service | Runtime RPC serves `AwakenBatch`, `EvictLease`, `CallTransient`, and `DeliverMessage` with production credentials/auth callbacks. | gRPC integration/contract tests plus DST response scenarios |
| Multi-row state transitions are transactional | Step start/finish/failure, wait transitions, inbox enqueue, mailbox advancement, and state/result writes commit atomically where required. | Implementation plus regression suite |
| Runtime values are Paquito bytes | Runtime payloads use Paquito bytes in `bytea` / `LONGBLOB`, not JSONB. | Payload storage specs |
| MySQL/MariaDB honors common store semantics | MySQL/MariaDB and PostgreSQL/YSQL satisfy the same store behavior contract. | Backend conformance spec |
| Durable object API uses durable mailboxing | `at`, `tell`, `expose`, and `expose_command` execute through per-object mailbox ordering and lease ownership. | Durable object specs |
| Object commands are per-id FIFO and worker-driven | Inbox/mailbox execution enforces one writer, blocked-head behavior, and worker-driven retries. | Object mailbox specs |
| Object sleeps convert to durable wake messages | Sleep rows atomically convert to wake inbox rows without losing wakes. | Object sleep specs |
| Workflow signals are durable ordered history | Inbox rows are accepted into workflow history and replayed at deterministic yield points. | Workflow signal specs |
| Workflow patch markers guard code evolution | `patched` / `deprecate_patch` append and check ordered workflow history markers before branch side effects. | Patch-marker unit, backend-conformance, and crash tests |
| Transient exposed methods route to owner | `CallTransient` invokes live object/workflow owner without durable mutation. | Transient RPC specs |
| Worker pool scopes persisted targets and relevant keys | Persisted targets and query-critical keys include `worker_pool` where routing/claiming requires it. | Worker-pool backend specs |
| Unified inbox is the durable message model | Object commands, object wakes, workflow signals, and workflow commands share one inbox contract. | Inbox/mailbox specs |
| CLI supports operational basics | CLI supports migration, workflow run/resume, inspection, and version output. | CLI spec |

## Crash matrix

| Crash point | Expected recovery |
| --- | --- |
| After enqueue, before claim | Later engine/worker can run the pending workflow. |
| After lease claim, before step schedule | Lease expiry returns workflow to pending; another worker completes it. |
| After step schedule, before step start | Replay validates the scheduled command shape and recovery dispatches the command. |
| After step start, before step completion | Step remains incomplete/running; recovery retries it with a new attempt. |
| After step heartbeat, before step completion | Latest heartbeat cursor is available to the next attempt. |
| After step failure, before retry due time | Retry schedule persists; workflow is not claimable early. |
| After step completion, before workflow completion | Completed step is skipped and remaining work continues. |
| After cancellation cleanup step completes, before canceled terminal write | Completed cleanup step is skipped and workflow finishes `canceled` on recovery. |
| While waiting for an event | Wait row survives; signal wakes workflow and execution continues. |
| While waiting when cancellation is requested | Wait row is marked canceled; cleanup runs on next claim and late signals are ignored. |
| After outbox insert, before delivery | Outbox message remains claimable exactly once at a time. |
| After outbox claim, before ack | Expired outbox lease can be reclaimed by another sender. |
| During lease-routed workflow RPC | Receiver rejects stale/moved/shutdown/no-owner states; caller refreshes or fails by policy. |
| During app shutdown with in-flight step | Runtime stops new claims; timeout releases leases; later worker retries. |
| Crash after inbox row and target activation commit before `DeliverMessage` | Activation remains claimable; a later worker/owner drains the target inbox. |
| Crash before inbox row commits | No message row exists; caller retry decides whether to enqueue. |
| Crash while allocating mailbox sequence | Transaction rolls back or commits both sequence advance and inbox row. |
| `DeliverMessage` reaches a stale owner | Receiver no-ops after lease check; activation/lease re-check routes work to the current owner. |
| Crash while object command runs before first checkpoint | Inbox head remains unconsumed; new owner reruns command after lease expiry. |
| Crash after object command checkpoint completion before state/message completion | Checkpoint output is cached; command replays and persists state/result once. |
| Crash after object state persists before ask result completion | State persist and message completion must be atomic so this split state is impossible. |
| Crash while head message is in backoff/dead-letter | Later messages remain blocked until operator action. |
| Crash during object sleep-to-inbox conversion | Either sleep row remains or wake inbox row exists; no wake is lost. |
| Crash while workflow signal is accepted into history | Accepted signal is replayed in sequence. |
| Crash after patch marker commit before first new-branch step | Replay sees marker, `patched` returns true, and the new branch continues. |
| Code removes `patched` while normal marker history still exists | Checker raises nondeterminism before any later durable write. |
| Owner pod loses lease while activity continues | External effect may finish; checkpoint/status write fails; retry uses idempotency. |
| Caller times out waiting for durable ask result | Row may keep running; same idempotency key reattaches later. |
| Database circuit breaker opens before commit | No durable change exists unless transaction committed; caller receives typed breaker error. |
| Paquito decode fails for persisted payload | Target/message/workflow moves to operator-visible error/repair state. |

## Testing and coverage standard

Correctness claims must be backed by tests:

- Real PostgreSQL/YSQL integration tests cover storage semantics, migration, leases, waits, outbox, fences, crash/recovery, and query-shape behavior.
- Shared backend conformance tests cover MySQL/MariaDB and PostgreSQL/YSQL behavior equivalence.
- Backend-specific tests pin SQL behavior that differs by adapter, including lock/claim semantics and EXPLAIN-backed query-plan assertions for hot paths when practical.
- Deterministic simulation tests are useful for exploring lease/race schedules, but any storage bug found through simulation must be pinned by a real backend regression test.
- Subprocess crash harnesses cover real process death around durable boundaries.
- RPC tests cover stale lease, lease moved, no-active-owner, shutdown/non-running workflow, retry/reroute, gRPC serialization, unavailable-node, timeout, deadline, RST, EOF, lost-response, duplicate-response, auth-failure, wakeup drops/duplicates, and all four service methods.
- Object mailbox tests cover strict FIFO, blocked head behavior, ask/tell ordering, wake ordering, idempotency conflicts, owner crash, lease takeover, dead-letter, and operator repair paths.
- Workflow signal tests cover history acceptance, deterministic replay order, timeout behavior, terminal-workflow rejection, and idempotency dedup.
- Workflow patch-marker tests cover first-run marker recording, no-marker `false` branches, marker-history `true` branches, missing-marker nondeterminism failures, `deprecate_patch` cleanup, duplicate-id handling, backend conformance, and crash after marker commit.
- Exposed transient method tests verify no durable state mutation and deadline/crash semantics.
- Observability tests verify required span/metric labels without depending on a particular vendor backend.

SimpleCov thresholds required by the CI coverage gate:

- global line coverage: 90% minimum
- global branch coverage: 85% minimum
- per-file line coverage: 59% minimum
- per-file branch coverage: 41% minimum

The gate is `mise exec -- bundle exec rake test:coverage`. It enables branch coverage, measures library files under `lib/**/*.rb`, excludes tests and non-library support surfaces from the metric, excludes `lib/durababble/version.rb` because Bundler loads that gem metadata before SimpleCov starts, prints the SimpleCov summary in CI logs, and writes the HTML report plus SimpleCov result JSON to `coverage/` for CI artifact upload. Meaningful tests should raise the configured minimums as coverage improves, and the minimums must not be lowered without an explicit spec update.

## Boundaries and anti-goals

These constraints are part of the contract:

- No block-form durable object `.with` API.
- No process-wide monkeypatching for determinism.
- No cross-object transactions or full distributed-actor semantics.
- No split-brain tolerance beyond database invariants.
- No durable queue/cron replacement; integrate with an adjacent scheduler/queue if product needs exceed simple durable sleeps/waits.
- No streams API without a concrete consumer requirement.
- No automatic cross-pool routing; relocation/failover is explicit.
- No silent payload spill to blob storage; oversized values fail loudly.
- No production RPC transport other than the four-method gRPC service.
- No runtime loading or validation of user RBS.
- MySQL/MariaDB support is required for the common public contract.
- Worker registry misses are avoided by claiming only workflow/object classes present in the supplied registry. Enqueuing a workflow name with no corresponding worker pool leaves it pending until an appropriate pool starts.
- Long-running steps do not heartbeat automatically while user code runs.
- Class-level serialized state migrations and node capability routing are not part of this contract.
