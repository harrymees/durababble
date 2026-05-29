# Durababble specification

This document is Durababble's durable-execution contract. It describes the behavior the implementation must provide and the evidence expected from tests, so a reviewer can compare the running system to the intended system without reconstructing history from implementation notes.

## Scope

Durababble is a Ruby library packaged as a gem. Ruby owns workflow and durable-object definitions; SQL owns durable coordination, replay, recovery, leases, inboxes, and retained state.

Durababble exposes two durable primitives:

| Primitive | Class | Public handle API | Best for | Mental model |
| --- | --- | --- | --- | --- |
| Durable workflow | `Durababble::Workflow` | `Workflow.start` / `Workflow.handle` | Finite executions with a start, result, steps, waits, retries, cancellation, termination, and recovery | A function or process that survives restarts |
| Durable object | `Durababble::DurableObject` | `DurableObject.at` / `DurableObject.handle` typed handle calls | Sessions, carts, conversations, agents, per-shop workers, or other id-addressed state | A SQL-backed actor/mailbox object with a lease owner |

Workflow and object calls compose. A workflow can call a durable object, and a durable object command can start or command workflows through their exposed RPC surface. Child durable calls inherit the caller's worker pool unless explicitly overridden.

The repo includes an Alloy model under `formal/` for workflow state, leases, storage rows, waits/signals, fences, outbox, target activations, and FIFO inbox command rows for object and workflow targets. `mise exec -- bundle exec rake formal` verifies all Alloy `run`/`check` commands. `[DURABABBLE-*]` sigils between the model and Ruby implementation/tests are checked on every PR by `test/durababble/formal_sigil_drift_test.rb` in the fast `test` suite.

Storage works through PostgreSQL/YSQL (`postgresql://` / `postgres://`) and MySQL/MariaDB (`mysql://` / `mysql2://`). Both adapters must provide the same public durable semantics, with backend-specific SQL hidden behind shared conformance tests and backend-specific locking/query-plan tests where query shape matters.

Runtime values are serialized with Paquito into binary columns. PostgreSQL/YSQL stores runtime payloads as `bytea`; MySQL/MariaDB stores them as `LONGBLOB`. Runtime payloads include workflow inputs/results/errors, step args/results, wait contexts, inbox payloads, durable object state, command args/results/errors, idempotency fingerprints, and heartbeat cursors.

The default storage namespace is `DURABABBLE_SCHEMA` when set. Otherwise it is derived from `Durababble.workspace_schema(DURABABBLE_WORKSPACE_ROOT || Dir.pwd)` so concurrent worktrees do not share internal tables by accident. PostgreSQL/YSQL uses the selected namespace as a SQL schema; MySQL/MariaDB uses it as the durable table prefix inside the configured database.

Store migrations are an explicit setup/deploy responsibility. Runtime workflow engines, class helpers, and object/workflow handles do not run `migrate!` on demand before enqueueing, querying, or dispatching work; callers must migrate the selected namespace before handing the store to runtime paths.

The contract specifies behavior, not a mandatory internal isolation technology. Workflow execution must be deterministic, workflow-local, and free of process-wide monkeypatching, but the implementation may choose the isolation mechanism that provides those guarantees.

## Terminology

- **Durable target:** a workflow execution or durable object instance addressed by class/type, id, and worker pool.
- **Worker pool:** a persisted routing and execution boundary. Workers in a pool may claim, route, wake, and execute only targets assigned to that pool.
- **Lease:** a persisted ownership record for a durable target, with the owner worker identity, a lease token, and a deadline. Production `WorkerRuntime` identities are compact parseable strings in the form `worker-id@host:port`, where the random worker id distinguishes a process incarnation and the address suffix is the routable HTTP/2 RPC endpoint.
- **Workflow command:** a deterministic workflow operation such as a step schedule, timer wait, workflow command delivery, child workflow command, or patch marker.
- **Command id:** the replay identity assigned by deterministic workflow execution order.
- **Attempt id:** the execution identity for one concrete try of a command. Retries create new attempts for the same command id.
- **Activation:** one deterministic workflow run/replay slice that processes runnable fibers until they finish, block on durable work, or reach a safe suspension point.
- **Inbox/mailbox:** the durable per-target message stream used for object asks/tells/wakes and workflow commands.

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

Steps may also be defined as named reusable constants outside the workflow class with `Durababble.step`. Calling that object from workflow orchestration schedules the same durable command records as a method step, using the step object's explicit name for replay shape validation and stored step metadata. Reusable steps support the same retry options as method steps, and their body runs with the workflow instance as `self`, so `step_context` is available; `Durababble.step_context` exposes the same context when a step body is factored into plain functions.

```ruby
ChargeCardStep = Durababble.step("charge_card", retry: { maximum_attempts: 5, schedule: [1, 5, 30] }) do |order|
  Payments.charge(order, idempotency_key: step_context.idempotency_key)
end

class FulfillOrder < Durababble::Workflow
  def execute(order)
    ChargeCardStep.call(order)
  end
end
```

Outside workflow execution, `Workflow.enqueue(input, id: nil, engine: nil, store: nil, worker_pool: nil)` creates a durable pending execution before any worker can run it and returns the workflow id. `Workflow.start(input, id: nil, engine: nil, store: nil, worker_pool: nil)` enqueues the same durable execution and returns a workflow handle. When `id:` is omitted, Durababble generates a new workflow id above the store boundary; when `id:` is provided, the enqueue path persists that exact id atomically in the selected worker pool and raises `Durababble::WorkflowAlreadyExists` if a workflow row already owns it, without appending workflow history, inbox messages, waits, or target activations. Completed, failed, canceled, and terminated workflow rows still own their ids, so deterministic ids are deduplication keys for the full lifetime of stored workflow state. `Workflow.at(workflow_id, engine: nil)` and `Workflow.handle(workflow_id, engine: nil)` return query/management handles for status, result, cancellation, termination, resume, and exposed methods. When `engine:` is omitted, these helpers use Durababble's configured default engine. Inside workflow execution, `Workflow.enqueue` and `Workflow.start` are child-workflow starts as specified below and do not accept `store:` or `engine:`.

Idempotent start scopes caller keys to worker pool, workflow class, operation kind, and argument fingerprint. The same key with the same shape returns the same handle; the same key with a different shape raises `Durababble::IdempotencyKeyConflict`.

`step_context` and `Durababble.step_context` are available only while a workflow step is executing. They expose `workflow_id`, `command_id`, `attempt_number`, `idempotency_key`, and `heartbeat`. Idempotency keys are generated from durable coordinates and remain stable across retries of the same logical command.

`expose` declares non-durable transient methods. Transient methods are invoked through `CallTransient` against the live workflow owner, take an owner-local fast path only when the current runtime owns the workflow lease, must not mutate durable state, must not call workflow steps or waits, and must not accept `idempotency_key:`. A transient workflow query requires an active lease owner; it does not enqueue inbox rows, create workflow rows, warm inactive workflows, or append workflow history, and stale/missing owner, lease handoff, and unavailable-owner failures follow `WorkflowRpc`/gRPC transient error mapping.

`expose_command` declares durable workflow command methods. A command call commits a durable inbox ask/tell row before returning to the caller, wakes the active workflow owner through `DeliverMessage` or leaves a durable target activation, and executes against the workflow at the next safe deterministic yield point. If the workflow row exists but is not actively owned, the target activation lets a worker warm it up before delivering the command; if the workflow does not exist or is terminal, the command fails instead of being buffered for a future target. Workflow authors do not park on a matching broadcast or poll an inbox manually. Synchronous command APIs wait for the ask row to store a serialized result or typed error; retrying with the same idempotency key reattaches to the same row.

`expose_stream` declares non-durable server-streaming transient methods. The method body yields successive values to its block; callers receive an enumerable stream and iterate it. Stream calls are routed through `CallTransientStream` to the live workflow owner and take an owner-local fast path only when the current runtime owns the workflow lease; like transient queries they read latest state, must not mutate durable state, must not call workflow steps or waits, and must not accept `idempotency_key:`. The dispatcher verifies lease ownership before the first value and re-checks it on a short interval as values are emitted, so an explicit lease hand-off mid-stream ends the stream with a terminal `StaleLease` frame that the consumer re-raises rather than seeing a clean end. When no active owner holds the lease, the stream runs against a latest-state snapshot, matching transient-query semantics. Indefinite producers poll `Durababble.stream_cancelled?` (or `Workflow.stream_cancelled?`) and return when the consumer disconnects or the lease is lost.

Workflow orchestration code may obtain workflow handles with `Workflow.handle(id)` / `Workflow.at(id)` and call the same handle methods available to external callers. Calls made from orchestration code outside an explicit step are not transient Ruby shortcuts: Durababble schedules a normal workflow command-history entry for the handle call before reading workflow status/result/error, requesting cancellation, invoking exposed queries, or enqueuing exposed workflow commands. Replay validates the recorded handle target, method, args, kwargs, and idempotency key and reuses the recorded result or error instead of sending the outbound RPC again.

#### Child workflows

Child-workflow orchestration was designed against Temporal Ruby child workflows ([docs.temporal.io/develop/ruby/workflows/child-workflows](https://docs.temporal.io/develop/ruby/workflows/child-workflows), [ruby.temporal.io/Temporalio/Workflow.html](https://ruby.temporal.io/Temporalio/Workflow.html), observed SDK HEAD `205e9a153751afae5a4dcf0e39a0a1a95e6afc91`), Hatchet child spawning ([docs.hatchet.run/v1/child-spawning](https://docs.hatchet.run/v1/child-spawning), observed repo HEAD `afbde7cf8f9f4e5bdf31b015569909693f8c5949`), and Absurd's Postgres-native checkpoint/await/retry model ([earendil-works.github.io/absurd](https://earendil-works.github.io/absurd/), observed repo HEAD `f2fcc45db4dfa46cd44cab36a4aa1f5d9e393bbd`). Temporal records child lifecycle in parent history and exposes start-plus-handle vs execute-and-await APIs with parent close policy; Hatchet treats child spawning as durable task/workflow fanout across the worker fleet; Absurd emphasizes checkpointed tasks, sleeps, event awaits, and retry from the last checkpoint rather than a named child-workflow surface. Durababble follows the smaller Ruby-friendly model: an explicit child handle, parent/origin metadata on the child workflow row, replay-safe starts/observes, independent child retry, and an explicit cancellation policy instead of implicit parent-close cleanup.

Workflow code starts a child by calling the child workflow class helper: `ChildWorkflow.enqueue(input, id: nil, worker_pool: nil, idempotency_key: nil, cancellation: :request_cancel, colocate: false)` or `ChildWorkflow.start(...)`. Outside workflow execution, `enqueue` still returns the workflow id and `start` still returns a normal workflow handle; inside workflow execution, both calls schedule a durable child start and return `Durababble::ChildWorkflowHandle`. Durable object commands use the same class helpers with default `cancellation: :abandon`. The child handle's public surface is `workflow_id`, `worker_pool`, `cancellation_policy`, `status`, `result`, `error`, `await(poll_interval: 1, timeout: nil)`, `cancel(reason: nil)`, `terminate(reason: nil)`, and `ref`. In workflow execution, `result` durably waits for a terminal child outcome and returns the completed result; it raises `Durababble::ChildWorkflowFailed`, `Durababble::ChildWorkflowCanceled`, or `Durababble::ChildWorkflowTerminated` for terminal non-success outcomes. `await` has the same terminal behavior and additionally accepts an explicit poll interval and timeout, raising `Durababble::CommandTimeout` if that timeout expires. Outside workflow execution, child-handle `result` is a nonblocking latest-state read.

Child start is a durable workflow command. Durababble records the parent command schedule before inserting the child workflow row with parent/origin metadata in one store transaction; if the parent crashes after the child is created, replay reuses the recorded command result and reattaches to the existing child instead of inserting another child. When `id:` is omitted, the child id is generated from the durable parent command coordinates and resolved idempotency key, not from retry-time shape fields such as child input or worker pool. When `idempotency_key:` is omitted, it is generated from parent workflow id plus command id before the child id is generated. Reusing the same durable parent command or same object command reattaches through the ordinary workflow id uniqueness constraint; a workflow id collision with a different child origin, child name, input, worker pool, or cancellation policy raises `Durababble::IdempotencyKeyConflict`.

Awaiting or observing a child is also a durable workflow command. `status`, `error`, `result`, and each `await` poll record observe commands whose completed result is reused on replay. In workflow execution, `result` and `await` record timer waits and suspend the parent at normal workflow safe points while the child is pending/running/canceling; a crash while waiting replays the existing child handle and continues polling until the child reaches a terminal state. The child workflow row remains the source of truth for status, result, and error; completed observe results are also present in the parent's command history.

Child workflows run independently in the target worker pool. If `worker_pool:` is omitted, the child inherits the parent's worker pool; if it is provided, workers in the parent pool do not claim the child. A child whose workflow class is not registered in any worker for its pool remains pending and inspectable until a matching worker is deployed or an operator cancels/terminates it through a workflow handle. This first slice does not auto-start a missing worker pool or resolve unknown workflow names through a global registry.

`colocate: true` (default `false`) asks Durababble to keep the child on the same worker as the durable object that started it. Colocation is supported only from object commands — an object may colocate the child workflows and child objects it starts, but workflow-to-workflow colocation is not supported and passing `colocate: true` from workflow execution (or any non-object context) raises `ArgumentError`. At start time the child records the originating object as its colocation owner, so that object's own lease is the single lease that gates claiming the child — see [Colocation](#colocation). Colocation does not override worker pools; a colocated child still runs in its resolved pool, so `colocate: true` is meaningful only when the child and owner object share a pool that some worker serves. `colocate` is part of the child-start shape: reusing the same durable parent command with a different `colocate` value raises `Durababble::IdempotencyKeyConflict`, exactly like a child name or input mismatch.

Parent cancellation is cooperative and policy driven. The default workflow-origin policy is `:request_cancel`: when the parent observes a cancellation request, Durababble durably requests cancellation of each non-terminal child workflow row whose child cancellation policy is `request_cancel`, then raises `Durababble::CancellationError` into parent workflow code. `cancellation: :abandon` leaves the child running/pending when the parent cancels. Parent hard termination does not deliver cancellation, does not run parent cleanup, and does not request child cancellation; operators can cancel or terminate the child separately through its handle. Child retry policy remains whatever the child workflow declares; parent step/replay retries never duplicate child start and do not reset child retry attempts.

Workflow code may use durable timer waits directly from orchestration code through workflow helper methods or the module-level helpers: `sleep(duration)`, `wait_until(time, context)`, `Durababble.sleep(duration)`, and `Durababble.wait_until(time, context)`.

Durable sleep helpers such as `Durababble::Workflow.sleep(duration)` and `wait_until(time)` are timer waits with workflow-friendly API shape.

`wait_condition(timeout: nil) { ... }` and `Durababble.wait_condition(timeout: nil) { ... }` block a workflow fiber until the predicate is true or a durable timeout fires. Direct waits append replayable workflow command history, park the workflow with the earliest unresolved timer in `workflows.next_run_at`, and resume under the normal workflow claim path when the timer is due. Durable sleeps are implemented as timer waits and must survive process exit.

### Durable objects

A durable object subclasses `Durababble::DurableObject`. It is addressed by `Class.at(id, engine: nil, worker_pool: nil, idempotency_key: nil)` or `Class.handle(id, engine: nil, worker_pool: nil, idempotency_key: nil)` for typed handle calls such as `account.credit(1_000)`. When `engine:` is omitted, these helpers use Durababble's configured default engine. Durable object methods are not workflow steps.

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

`expose` registers transient non-durable owner-local methods. Transient methods are dispatched to the active object owner through `CallTransient` when one exists, with an in-process fast path only inside the runtime that currently owns the object lease. A caller must not satisfy an exposed read by loading persisted object state in its own process while another live owner exists. There is at most one materialized instance of an object in the cluster, and every read, command, and stream is served from that single resident instance, so a read always observes the same live object that commands mutate.

When no node owns the object, a read claims the object lease and becomes the resident owner: it materializes the instance (running `on_load`, or `on_create` when no durable state exists yet) and keeps it warm so the next operation reuses it. A pure-client process with no residency host instead falls back to a transient claim-read-release. Reads no longer wait on a mailbox read gate; because they run against the live resident instance on the owning node's single cooperative reactor — where commands for one object are FIFO-serialized — a read reflects every command that has already committed and never observes a stale snapshot that skipped unresolved commands.

Transient object methods may run concurrently with other admitted transient methods, must not enqueue inbox messages, call workflow steps, mutate durable state, schedule sleeps, or write Durababble tables, and reject `idempotency_key:`. `update_state` from an exposed read raises `Durababble::Error`.

`expose_command` registers durable mailbox commands. Commands execute through the durable object's identity, receive `command_context`, and update state only through `update_state(new_state)`. `command_context.idempotency_key` is generated from object type, object id, and mailbox message id and is stable for the durable command.

Synchronous asks and asynchronous tells share the same mailbox, so asks cannot overtake earlier tells. `tell` validates that the target method is an `expose_command`. Enqueuing a ready object command wakes the active owner through `DeliverMessage` or leaves a durable target activation for pool-local recovery. Object authors do not poll for commands; the owner drains the mailbox when woken or claimed.

`expose_stream` registers non-durable server-streaming owner-local methods. The method body yields successive values to its block; callers receive an enumerable stream and iterate it. Object streams are routed through `CallTransientStream` to the holder of the unified `durable_objects` lease so every producer (and any commands it observes) runs on the single resident owner node. An in-flight stream is a refcounted holder of the residency lease, extending the object's resident lifetime the same way an in-flight command does: a streamed object stays resident and leased until its last stream finishes, then through the normal idle window. If the node loses the lease while emitting, the stream ends with a terminal `StaleLease` frame and the consumer reconnects onto the new owner; a consumer with no live owner but its own residency host self-routes and becomes the resident owner, while a plain consumer with neither falls back to a latest-state snapshot. Like exposed reads, stream methods read latest state, must not mutate durable state or write Durababble tables, and reject `idempotency_key:`; indefinite producers poll `Durababble.stream_cancelled?` and return when the consumer disconnects or the lease is lost.

Workflow orchestration code may obtain durable-object handles with `DurableObject.at(id)` / `DurableObject.handle(id)` and call exposed queries or commands directly, and may call `DurableObject.tell(id, method, ...)` for asynchronous commands. When these APIs are called from orchestration code outside an explicit step, Durababble records the handle RPC as a workflow command-history entry before the object query, ask, or tell is dispatched. Completed history is replayed as the original return value, including the object command result or tell message id, so crash recovery does not enqueue duplicate object inbox rows.

Lifecycle callbacks are `on_create`, `on_load`, `on_wake(name:, payload: nil)`, and `on_destroy`. They are lifecycle hooks, not remotely callable public methods.

Durable objects support multiple named wakes per object id. `schedule_wake(name:, at:, payload: nil)` upserts a wake row by `(worker_pool, object_type, object_id, name)` in the same transaction as the command state write, replacing the time and payload when the same `name` is re-scheduled. `cancel_wake(name:)` removes one named wake and `cancel_all_wakes` removes all of them. Matured wakes convert atomically into durable mailbox `wake` messages that carry the wake name to `on_wake`.

Durable object commands may start workflows with `WorkflowClass.enqueue(input, id: nil, worker_pool: nil, idempotency_key: nil, cancellation: :abandon)` or `WorkflowClass.start(...)`. This API is command-only: exposed queries and code outside an object command cannot start workflows because that would mutate durable state without a mailbox command boundary. The start persists immediately on the child workflow row using `child_origin_kind = "object"`, the object type/id, and the durable object command id. A retry of the same object command reattaches to the existing child through the workflow id uniqueness constraint. Durable object commands do not synchronously `await` child workflows; they observe child outcomes safely by storing the child id in object state, scheduling a named wake, receiving a later workflow command/signal, or exposing a query that reads `Workflow.handle(child_id).status/result` after the command has committed.

Durable object management APIs in the current contract are limited to `list`, `find`, `cancel`, `destroy!`, `evict`, and explicit `relocate_worker_pool`. Durable-object `pause` and `resume` control APIs are not part of this contract.

Durababble does not provide a block-form durable-object `.with(id) { ... }` API. Multi-method atomicity is expressed by writing one command method that performs the full operation.

### Typing

The runtime does not load or validate user RBS. The gem ships `sig/durababble.rbs` with `Durababble::Workflow[Input, Output, Dispatch = Object]` and `Durababble::DurableObject[Id, State, Dispatch = Object]` generics for static tooling only; the third generic is the type-level RPC dispatch surface exposed on handles returned by `start`, `at`, and `handle`, while runtime serialization remains Paquito-based.

## Workflow execution semantics

### Deterministic orchestration

Workflow orchestration runs on a Durababble-managed deterministic scheduler. Workflow code without async fanout runs as the root workflow fiber. Raw `Async { ... }` and `Async::Task#async` create additional deterministic workflow fibers when called from workflow orchestration code.

Durababble integrates with Async task creation and waiting so ordinary non-transient Async child tasks inherit workflow execution context and may call durable steps. Workflow authors must not need Durababble-specific async helpers. `transient: true` Async tasks do not inherit workflow execution context and must not call durable steps.

Workflow orchestration code must not perform direct blocking or nondeterministic I/O. It may schedule durable steps, sleeps, timer waits, durable commands, child workflows, and deterministic local computation. The runtime rejects unsafe direct host calls such as wall-clock time, randomness, blocking sleeps, process calls, and blocking file/IO operations with `Durababble::DeterminismError`. Step bodies run outside the deterministic scheduler and may perform process-local side effects.

The workflow runtime must provide workflow-local deterministic behavior for time, sleep, randomness, UUID generation, and workflow futures/fibers, or reject unsafe host APIs with `Durababble::DeterminismError` until a durable deterministic API exists. The implementation must not rely on process-wide monkeypatching to create determinism.

Durable commands called from any workflow fiber are assigned command ids when the workflow fiber reaches the call, before the side-effecting implementation runs. The runtime and replay system must not assume that a workflow will stay single-fiber across code changes.

In-workflow handle RPCs are durable commands for replay purposes. The command shape records the target kind, target type, target id, RPC kind, method, args, kwargs, and caller-provided idempotency key if present; the schedule is persisted before the handle implementation dispatches a workflow inbox command, object inbox command, object tell, object query, or workflow status/result/cancel operation. If the process crashes after dispatch and completion are recorded, recovery delivers the recorded command resolution and skips the outbound dispatch block. If dispatch fails before a terminal resolution, the command follows the same step failure, retry scheduling, cancellation, and lease fencing rules as other workflow commands.

### Workflow command history

Workflow replay is driven by an append-only per-workflow command history. Latest-state tables such as `steps` are query caches and recovery aids, not the replay source of truth.

Replay is bounded by a configurable maximum number of per-workflow `workflow_history` rows. The count includes every replay/history fact for that workflow: step schedules, starts, completions, failures, wait records, timer completions, workflow command completion/failure records, child-workflow records, and patch markers. It does not count latest-state or recovery helper rows in `steps`, `step_attempts`, `inbox`, or `outbox`, except where those operations append a workflow-history fact. The engine checks the count before loading full replay payloads and checks projected growth before scheduling a new durable command. Durababble also has a lower warning threshold, defaulting to `8_000` events, that logs through `Durababble.logger` without stopping the run. If an open workflow exceeds the hard bound, the engine raises `Durababble::WorkflowHistoryLimitExceeded`, records that typed error on the workflow row, clears the lease and retry deadline, and returns a terminal failed run so workers do not repeatedly replay the same oversized history. Terminal workflow target activations dead-letter pending workflow-command inbox work instead of re-arming the target, so an oversized terminal workflow does not leave runnable task rows behind. Terminal workflows are returned without applying the replay bound so completed results and terminal errors remain inspectable.

Step scheduling, step execution starts, and terminal outcomes are distinct durable facts. A schedule record stores command id and full replay-relevant command shape before any local or remote executor starts the side effect. A start record stores that an executor began a concrete attempt. Success, wait, cancellation, and non-retrying failure records resolve the command's workflow future. Retryable failure records are diagnostic history for the failed attempt and must not be treated as terminal replay events.

Schedule record shape includes step method name, serialized args/kwargs or a stable payload digest, retry/executor attributes, and any semantic key if one is present. Replay validates the scheduled command shape even when no completion exists, so a step that started before a crash cannot disappear silently.

Replay shape checks apply to every scheduled durable command, not only completed steps. If a previous run scheduled command `17` as `fetch_profile(user_id: 1)` and replay schedules command `17` as `fetch_profile(user_id: 2)` or `send_email`, the workflow is nondeterministic even if the original command only reached `started`.

Step completions, step failures, timer fires, workflow command deliveries, and child-workflow completions are external inputs that make workflow fibers runnable. They must be appended to history and delivered to the deterministic scheduler in history order, because completion order can affect later command order.

Process-local step execution may run multiple step attempts concurrently, but attempts must not concurrently mutate workflow history through one non-threadsafe store connection. History mutations are serialized per workflow or protected by executor-local connections/transactions with durable command ids and optimistic checks.

### Steps, retries, and heartbeats

Workflow steps are method-level durable side-effect boundaries. On first execution, the runtime records a scheduled command, records a running attempt, runs the method outside the deterministic workflow scheduler, stores the serialized result, and marks the command completed.

On resume, completed command results are returned from durable state and the step body is not re-run. Incomplete, running, failed, and waiting commands are retried or continued according to durable state.

If the process crashes after an external side effect but before the checkpoint commits, the step may run again. Step implementations are expected to pass `step_context.idempotency_key` to external systems that support idempotency.

No workflow row lock is held while user step code runs. The executor holds a renewable lease and fences durable writes with active lease ownership. If the lease is lost while step code is running, the external side effect may still finish, but checkpoint/status writes fail and recovery follows the normal idempotent retry path.

Step attempts are append-only. Retries and stale attempts remain inspectable, including waits that transition to completed attempts.

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

`schedule: [1, 5, 30]` supplies an explicit per-retry schedule. After the explicit array is exhausted, Durababble falls back to capped exponential backoff. Intervals are numeric seconds. `maximum_attempts:` counts the first execution plus retries. `non_retryable_errors:` accepts Ruby exception classes or class-name strings. A retryable failure must commit the failed attempt record, diagnostic failure history, lease release, and retry due time in one transaction so replay and claiming cannot observe a half-scheduled retry.

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

Durababble records a `step_scheduled` history record for each `fetch_profile(id)` command before dispatching that step body. The recorded command shape must distinguish `fetch_profile(1)` from `fetch_profile(2)`.

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

### Waits and commands

Timer waits persist their wake time and Paquito context in workflow history, set `workflows.next_run_at` to the earliest unresolved timer when the workflow parks, and resume when the normal workflow claim path finds that due row.

A step wait is a terminal command-resolution record, but releasing the workflow lease to `waiting` happens only after the workflow activation reaches a safe suspension point. If one branch records a wait while sibling workflow fibers have already scheduled process-local steps, those sibling steps may finish and commit before the workflow row is released.

Workflow commands are durable workflow inbox messages delivered to the workflow lease owner. A committed command must make the target runnable immediately. If a live owner holds the workflow lease, the runtime sends `DeliverMessage` after commit; if there is no live owner or advisory delivery fails, the durable target activation remains claimable by the worker pool. The command executes inside the active `WorkflowExecution` and same workflow object instance that is running or replaying `#execute` after a step completes/fails/waits, when a running step heartbeats or otherwise yields to the workflow engine, or when a condition wait is interrupted by message work. A non-heartbeating user step body is not preempted mid-Ruby-frame; the command runs when the active owner reaches a safe point or lease expiry/recovery gives the target to another worker.

Workflow command delivery appends a `workflow_command_completed` or `workflow_command_failed` history record with the inbox message id, method, args, kwargs, sequence, shape hash, and result or error. Replay delivers those command records in `workflow_history` order only after preceding scheduled/resolved workflow command history has been consumed, invokes the command handler against workflow-local state, and raises replay divergence if the exposed method is missing or the replayed result/error diverges from the recorded command record.

Synchronous workflow command APIs are durable asks. The caller waits for the inbox row to store a serialized result or typed error, then returns/re-raises it. If the caller times out or loses its connection after the row commits, the durable command is not canceled; retrying with the same idempotency key reattaches to the same inbox row. Command responses do not require the general outbox table because the response is a one-to-one property of the ask row; workflow/object code may still write ordinary outbox messages when a command handler needs durable delivery to an external system.

### Cooperative cancellation

Cancellation is cooperative execution, not hard termination.

- `Workflow.handle(workflow_id).cancel(reason:)` records the first durable cancellation reason and request timestamp. Duplicate requests return the current run and preserve the first reason.
- Pending, waiting, and retry-backoff workflows move to `canceling`, clear `next_run_at`, and become claimable immediately. Waiting step/attempt state is marked canceled so a later timer claim cannot resume canceled work.
- Running workflows keep their active lease. Cancellation is observed at deterministic yield points: before a new durable command starts, when replay reaches a completed command boundary, after a step completes, and when a running step heartbeats.
- Delivery raises `Durababble::CancellationError` with the durable reason and workflow id. Cleanup steps run as ordinary durable steps under the same command-history replay model as all other workflow work.
- If workflow code catches cancellation and returns after cleanup, the engine records the workflow as `canceled` and stores the cleanup result. Re-raising `CancellationError` also records `canceled`.
- If cleanup raises an unrelated error, ordinary step retry policy applies. Retryable cleanup failures remain `canceling` with `next_run_at` set and become claimable again only when due. Exhausted or non-retryable cleanup failures mark the workflow `failed`.
- Child workflow starts require an explicit child-cancellation policy in durable state. Parent cancellation requests child cancellation only for non-terminal child workflow rows whose policy is `request_cancel`; children with `abandon` keep running independently. Parent cancellation never hard-terminates children and never claims child cleanup completed.

### Hard termination

Termination is an operator hard stop, not cancellation with stronger wording.

- `Workflow.handle(workflow_id).terminate(reason: nil)` durably marks a non-terminal workflow `terminated` as soon as the store can commit that terminal state. The terminal run has `status == "terminated"`, `result == nil`, and `error` set to the supplied reason or `"workflow terminated"` when no reason is supplied.
- Duplicate termination calls are idempotent. If the workflow is already `terminated`, later calls return the same terminal run and preserve the first durable reason. If the workflow is already `completed`, `failed`, or `canceled`, termination is a no-op that returns the existing terminal run without changing result, error, cancellation metadata, history, wait state, or commands.
- Termination does not deliver `Durababble::CancellationError`, does not set cancellation metadata, and does not run workflow cleanup. A workflow already in `canceling` can still be terminated; if termination commits first, cleanup is skipped and the final state is `terminated`.
- When termination wins, the store clears the workflow lease and retry deadline, records a termination history event, marks live scheduled/running/waiting step and attempt rows as canceled with a termination error, dead-letters queued or running workflow-command inbox rows, and removes pending workflow target activations.
- A running Ruby step body is not asynchronously interrupted mid-frame. After termination commits, later step heartbeats, waits, step completions/failures, workflow completion/failure/cancellation writes, and workflow command completions are fenced by the missing running lease or the terminal workflow row and cannot revive the workflow.
- Late timer claims for terminated workflows are ignored because only non-terminal waiting workflows with due `next_run_at` can resume. New workflow commands after termination fail instead of being buffered for a future run, and already queued commands complete with a terminal error rather than executing user code.
- Race rule: the first durable terminal write wins. Completion, failure, cancellation, and termination are mutually exclusive terminal outcomes; once any one commits, the others return or observe the persisted terminal run without changing it. Recovery and replay treat `terminated` as terminal and never claim, replay, or resume it.

### Workflow code evolution

`patched(patch_id)` and `deprecate_patch(patch_id)` are the workflow control-flow compatibility APIs. They protect deterministic changes to step order, waits, command handling, and other durable workflow branches by recording/checking patch markers in workflow history before new-branch durable records are emitted.

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
- `patched` is valid only in deterministic workflow orchestration code: `execute` and `wait_condition` predicates/continuations. Calling it inside a step, durable object command, exposed transient method, or arbitrary library code outside active workflow execution raises a typed error.
- The first `patched(patch_id)` call for a workflow execution writes history. Later calls with the same id in the same execution return the memoized decision and append no duplicate marker rows.
- When live execution reaches a history point with no persisted marker, `patched(patch_id)` appends a normal patch marker and returns `true`. The marker commit must happen before the new branch produces steps, waits, commands, or other durable workflow records.
- When replaying history that already contains a normal marker for `patch_id`, `patched(patch_id)` consumes that marker and returns `true`.
- When replaying history that reached the change point without a marker, `patched(patch_id)` returns `false` and appends nothing, so workflow code follows the branch matching existing history.
- If persisted history contains a normal patch marker that workflow code does not consume with `patched` or `deprecate_patch`, the checker raises replay divergence before any further durable writes. Patch-id mismatches and out-of-order marker consumption also raise replay divergence.
- Patch markers are workflow-history markers, not inbox messages, state schema versions, or node-capability routing indicators.

Patch lifecycle:

1. Introduce a patch by deploying `if patched("id") { new } else { old }`. New or not-yet-reached executions record a marker and run the new branch; executions already past the change point without a marker run the old branch.
2. Deprecate the patch after no live workflows need the old branch by deploying `deprecate_patch("id")` plus the new code only. This keeps marker-aware histories replayable while removing the old branch.
3. Remove the marker call after all relevant histories have completed and aged out of retention. Never reuse the id.

Admin and observability surfaces expose patch usage by workflow type/id: normal marker, deprecated marker, and open workflows with no marker for a given patch id.

## Durable object execution semantics

The owning node keeps a single resident instance per `(object_type, object_id)` and serves commands, wakes, queries, and streams from it. Commands and wakes for one target are FIFO-serialized by the owner's single drain loop, so they never overlap each other. Queries and streams observe the live resident instance on the same cooperative reactor; `update_state` replaces the current-state reference atomically, so a read never sees a torn write. Because durable state round-trips the store on every command (each operation re-reads it and persists on completion), crash-safety does not depend on resident memory — only the expensive user resource set up in `on_create`/`on_load` stays warm in the instance. (A per-object cooperative gate remains a documented future option should a concrete read/write race surface.)

The per-object lease is the residency token: the owner holds it continuously while the object is resident, rather than releasing it after each command drain. Because `DeliverMessage` is owner-routed (it delivers to the lease holder), a held lease naturally routes all of an object's work back to its resident owner, guaranteeing at most one materialized instance in the cluster. The owner retains the lease and instance for a configurable idle window (`object_idle_ttl`, default `lease_seconds`) after the last operation; on expiry it runs `on_destroy` and releases the lease so ownership can rebalance. Lease takeover, graceful shutdown, and renewal failure tear the instance down through `on_destroy` the same way.

Object inboxes are push-driven. The owner pod drains commands, wakes, and internal work from the mailbox and invokes registered command/lifecycle methods against the resident instance. Ready object commands wake the active owner through `DeliverMessage` or leave a durable target activation for pool-local recovery. Exposed object reads use the same `CallTransient` method as workflow transient calls: the caller looks up the current object lease, calls the owner node when the lease belongs to another runtime, and uses the registered local transient handler when the current runtime owns the lease. When no node owns the object, a caller with a local residency host claims the lease and becomes the resident owner before reading; a caller without one performs a transient claim-read-release. Stale-owner or no-active-owner responses retry once after refreshing the lease. Transport-unavailable owner failures are returned to the caller and do not fall back to caller-local persisted-state inspection. The owner validates its lease before and after executing the transient method.

Commands execute one at a time in strict FIFO order per durable target. Target executors drain only the contiguous ready prefix from the mailbox head. `SKIP LOCKED` must not let later messages overtake a blocked head for the same target.

If the mailbox head is waiting for backoff, dead-lettered, or otherwise blocked, later messages for the same target must not run. A dead-lettered or backed-off head remains blocking until an operator retries, skips, cancels, destroys, or repairs the target.

Object message completion, state write, sleep updates, and mailbox head advancement are one fenced transaction. A crash cannot leave state persisted without the corresponding ask result/message completion, or message completion without the corresponding state write.

Ask rows store serialized result or error. Tell/wake rows retry with backoff and move to dead-letter after `max_message_attempts`.

A command's inbox row owns a stable `operation_id`. Any durable checkpoints inside that command use the operation id so retries skip completed side-effect checkpoints.

The owner's residency host tracks each held key as `{instance, last_used_at, refcount, lost}`: the resident user instance (nil until first materialized), the monotonic idle stamp set when refcount drops to zero, the count of in-flight operations holding the lease, and a flag set when renewal fails or the lease is evicted. A renewal that fails flips `lost`, which surfaces to in-flight streams as `StaleLease`; the host must never invoke user code under a lease that is expired or has been lost.

## Shared durable primitives

### Idempotency

Durable operations use both library-generated keys and caller-provided keys:

- Step idempotency keys are generated from workflow id plus deterministic command id and are available through `step_context.idempotency_key`.
- Durable object command idempotency keys are generated from object type, object id, and mailbox message id and are available through `command_context.idempotency_key`.
- Every public durable entry point accepts `idempotency_key:` where durability is implied: workflow starts, workflow command asks/tells, object asks, object tells, and externally exposed operator APIs.
- Idempotency keys are scoped to worker pool, target, operation kind, method, and argument fingerprint, not just target id.
- Same key plus same operation shape returns the existing handle/result or re-raises the saved error. Same key plus different operation shape raises `Durababble::IdempotencyKeyConflict`.
- Caller timeout after a durable command or message commits does not cancel durable work. Retrying with the same idempotency key reattaches to the same row.
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
| `workflows` | Workflow execution status, input/result/error, retry due time, cancellation metadata, workflow metadata, and nullable child parent/origin/cancellation metadata | Workflow id, workflow class, worker pool, status, due time, parent workflow/object listing |
| `workflow_history` | Append-only ordered replay records for workflow commands, completions, timers, and markers | `(workflow_id, record_index)`, command id lookup |
| `steps` | Latest logical workflow command state for query/result caching | `(workflow_id, command_id)` |
| `step_attempts` | Append-only attempt history for step execution and waits | Attempt id, workflow id, command id, status |
| Lease ownership (inline) | Pool-scoped ownership for workflow and object targets, stored as `locked_by`/`locked_until` columns on each leasable row rather than a standalone table | Owner worker identity + deadline columns on `workflows`, `durable_objects`, `inbox`, `target_activations`, `fences`, and `outbox` |
| `inbox` | Durable asks, tells, wakes, and workflow command messages | Target identity, sequence, status, message id |
| `mailbox_sequences` | Per-target monotonic mailbox sequence allocation | Target identity |
| `target_activations` | Coalesced runnable target wakeups for inbox/scheduler work | Worker pool, ready time, target identity, status |
| Idempotency (inline) | Public durable operation deduplication and shape conflict detection, stored on `inbox` columns (`idempotency_key`/`idempotency_hash`/`shape_hash`) plus the `workflows.id` primary key rather than a standalone table | Inbox idempotency-hash uniqueness; `workflows.id` uniqueness |
| `fences` | Workflow-local side-effect idempotency fences | `(workflow_id, key)` |
| `outbox` | Durable outgoing messages with processing leases | Id, unique message key, lease expiry |
| `durable_objects` | Latest object state by object type/id | `(worker_pool, object_type, object_id)` |
| `object_wakeups` | Pending named durable object wakes, each with Paquito payload and wake time | `(worker_pool, object_type, object_id, name)` |

Query-shape and transaction requirements:

- Claim paths use `FOR UPDATE SKIP LOCKED` where supported.
- Persist immutable `worker_pool` on durable targets whose routing, claiming, scheduling, listing, or recovery semantics are pool-scoped.
- Include `worker_pool` in primary keys and indexes when query patterns need to filter, route, claim, recover, or list by worker pool. Do not add it to keys whose query patterns do not care about worker pool.
- Workflow/object ownership is recorded with inline `locked_by`/`locked_until` columns on each leasable row (`workflows`, `durable_objects`, `inbox`, `target_activations`, `fences`, `outbox`), not a separate leases table. `locked_by` stores the full owner worker identity (`worker-id@host:port`) and doubles as the fencing token because the random worker-id distinguishes each process incarnation; `locked_until` is the deadline.
- Durababble does not require a separate nodes table for direct target routing. A caller that needs to wake or RPC a target reads the target's fresh lease row, dials the HTTP/2 RPC address suffix from the stored worker identity, and sends the full identity as `expected_worker_id`; when no fresh lease exists, the durable target activation remains the correctness path until a worker claims it.
- A worker that claims a target activation for a target currently leased by another fresh owner must forward `DeliverMessage` to the HTTP/2 RPC address suffix in the lease row and re-arm the activation on a bounded retry, rather than parking it until lease expiry.
- Object asks/tells/wakes and workflow commands use the unified inbox. Inbox rows are durable message/result records, not a second globally polled work queue.
- Sequence allocation, inbox insert, and ready-target activation upsert commit in one transaction.
- Per-target mailbox sequence state lets `enqueue_message` allocate a monotonic sequence and lets target executors drain only a contiguous ready prefix from the head.
- Enqueueing a ready inbox message must atomically upsert a target activation row, or an equivalent scheduler row, keyed by worker pool and durable target identity. Workers globally poll target activations, not the inbox table; one activation can cover many pending inbox rows for the same target.
- Target activation completion is conditional on the target mailbox/head state: after draining, the executor clears the activation only if no ready inbox row remains, otherwise it keeps or re-arms the activation with the next due time.
- If a worker claims a target activation but finds the target is still owned by another fresh lease, it must not hot-loop the activation; it relies on advisory `DeliverMessage` for the live owner and re-arms the durable fallback no earlier than the observed lease deadline.
- Object wake rows are keyed by object identity and worker pool plus the wake `name`, so several named wakes can be pending for one object id, each with its own `wake_at` and Paquito payload.
- Child workflow rows are ordinary `workflows` rows with nullable child-origin metadata. Child start deduplication uses the same `workflows.id` primary-key uniqueness as non-child workflow starts: generated child ids are deterministic from parent/object command coordinates and the resolved idempotency key, while explicit child ids behave like ordinary workflow ids. Parent/object origin columns are for inspection, cancellation, and listing; those listing queries are not worker polling paths and do not add child-origin indexes to the main workflow table.
- Colocation adds nullable owner columns (`colocated_owner_object_type`, `colocated_owner_object_id`) to `workflows` and `durable_objects` and no separate table: the owner object's own lease is the single mutex for the colocated set. The columns are `NULL` for the overwhelmingly common non-colocated case, and every claim/heartbeat path short-circuits the owner check on `NULL` so non-colocated work pays nothing. Only object commands may create colocated children (workflow-to-workflow colocation is unsupported), and the recorded owner is always flattened to the root object so no transitive resolution is needed at claim time. A colocated claim acquires the owner object's lease in the same atomic statement that acquires the child lease (a data-modifying CTE on PostgreSQL/YSQL; a conditional owner `UPDATE` between candidate select and child update on MySQL/MariaDB), gated so an owner held live by another worker blocks the claim. The owner lease is released only by a guarded release that clears it when no colocated child is still live (an indexed `NOT EXISTS` probe over non-terminal child workflows and still-leased child objects), so colocated work re-homes as a unit without forcing a permanent worker pinning. No group-row upsert or group steal/release sweep exists, so colocation is strictly cheaper than a separate group lease.
- Append-only workflow history rows are ordered per workflow. Required record families include `step_scheduled`, `step_started`, `step_completed`, `step_failed`, `step_waiting`, timer/wait records, workflow command delivery records, child-workflow records, and patch marker records.
- Store deterministic command ids and replay-relevant command shape on schedule records. Store concrete attempt ids on start/completion/failure/wait records. The command id is the replay identity; the attempt id is the execution/retry identity.
- Mutable latest-state rows are not the replay source. Replay uses ordered schedule history; deterministic scheduling uses history-ordered future resolution records; execution recovery uses distinct attempt start/completion records.
- Wait rows and `step_waiting` history can be committed before the workflow row is released to `waiting` when an activation still has sibling workflow fibers to drain. Timer wake queries only make externally visible progress once the workflow is durably suspended or otherwise ready for that activation.
- Idempotency dedup is inline rather than a separate `idempotency_keys` table: top-level workflow starts dedup on the `workflows.id` primary key (a colliding id raises `Durababble::WorkflowAlreadyExists`), while durable messages and child/object-command workflow starts dedup through the `inbox` `idempotency_key`/`idempotency_hash`/`shape_hash` columns and their unique index.
- Queue/recovery indexes cover workflow claims, due retries/timers through `workflows.next_run_at`, expired workflow leases, step-attempt lookup, outbox claims, and mailbox status scans.
- Worker lease release, cancellation wait cleanup, and durable-object command paths have explicit indexes and query-plan coverage where they can become hot at scale.
- New production Store SQL must be added to `Durababble::StoreQueries`; each new registered query must be covered by plan assertions, benchmark coverage, backend conformance coverage, or an explicit uncovered-query list entry reviewed in the query-plan suite.
- High-risk transactional pieces such as `enqueue_message`, target-head drain/advance, sleep-to-inbox conversion, and object state plus message completion may be implemented with database functions to reduce lock-order drift, provided the common backend contract is preserved.
- Retention and partitioning must be planned for high-volume history, step attempts, inbox, and idempotency rows before production scale.
- Runtime value decoding only decodes known serialized binary columns.
- MySQL/MariaDB and PostgreSQL/YSQL must pass the same store backend conformance suite.

All public durable semantics must work on PostgreSQL/YSQL and MySQL/MariaDB. Schema work that would otherwise rely on backend-specific features such as partial indexes, `RETURNING`, `ON CONFLICT`, `BYTEA`, or `gen_random_uuid()` must either define equivalent behavior in the backend abstraction and conformance tests or explicitly keep the feature out of the common public contract.

## Workers, leases, routing, and scheduling

### Worker lifecycle and recovery

A worker pool is served by processes repeatedly calling `Durababble::Worker#tick` or `#run_until_idle`, or by an embedded `Durababble::WorkerRuntime`. Each raw worker tick claims one runnable work item whose workflow or object class is present in the worker registry, including workflow rows and coalesced target activations, then resumes it through the deterministic workflow executor or durable object mailbox executor.

Runnable workflows are pending rows, retryable failed rows whose non-null `next_run_at` is due, canceling rows with no live lease whose `next_run_at` is null or due, and expired running leases that are recoverable. Terminal failed rows with no retry deadline are not claimable.

`Durababble::WorkerRuntime` is the preferred app/process lifecycle entrypoint. It must be started from a caller-owned `Async` task (or with an explicit parent passed to `start_async`), runs one worker pool with configurable `concurrency:`, schedules up to that many worker work items concurrently with `async` fibers, avoids running the same durable target identity twice inside the process, stops taking new claims on shutdown, waits for in-flight work up to a timeout, and releases still-held workflow, inbox, target-activation, and outbox leases if the timeout is exceeded.

`Engine#resume` refuses to execute work owned by another live worker. Lease holders must re-check ownership before mutating durable state.

Expired leases are reclaimed by claim paths or recovery sweeps. Recovery does not require a separate coordinator for correctness; any worker serving the pool may move expired work back to a claimable state.

### Worker-pool requirements

- Every durable target whose execution/routing is pool-scoped has an immutable persisted `worker_pool` selected at first materialization.
- If a class declares a default pool, that pool wins unless the caller overrides while creating a new durable unit. Once the row exists, the persisted pool wins.
- Worker pools are the routing and multiregion boundary. A pod in another pool cannot claim, route, or wake a target unless the target is explicitly relocated.
- Scheduler scans filter by pools served by the local pod.
- `DeliverMessage` and `CallTransient` route to the RPC address suffix in the target's fresh lease row and include the full lease owner as `expected_worker_id`. `AwakenBatch` is sent only to explicitly configured peers in the target pool.
- Automatic cross-pool stealing is forbidden. Regional failover is explicit `relocate_worker_pool` operator/runtime work that quiesces the target, releases the old lease, updates the row, and wakes it in the new pool.

### Sticky placement

Routing keeps hot ids on the pod that already has them in memory:

- Every pod exposes `rpc_address = "#{POD_IP}:#{DURABABBLE_RPC_PORT}"`, and worker runtimes combine a random worker id with that address as `worker-id@rpc_address` for the lease owner identity.
- Every acquired lease writes the full owner worker identity and a fresh `lease_token` into the lease row.
- Lease acquisition uses a hot in-memory cache when the owner has a fresh lease and falls back to atomic SQL acquisition/lookup when cold, near expiry, or routed remotely.
- A `LeaseRenewer` refreshes in-flight leases every `lease_ttl_ms / 3` and only when the `lease_token` still matches.
- Cache entries are evicted on near-expiry, `EvictLease`, object CAS conflict, lease-renew failure, idle timeout, or LRU capacity pressure.
- Idle owners stop renewing after `idle_eviction_ms` and release leases so the next pool-local caller can acquire ownership.

### Colocation

Sticky placement is a best-effort routing optimization. Colocation is a durable correctness constraint: when a durable object command starts a child workflow or child object with `colocate: true`, the child and its owner object must never be actively leased by two different workers at the same time. Colocation is an object-parent-only capability — only an object command may create colocated children, and workflow-to-workflow colocation is unsupported. The runtime models the constraint with the owner object's own lease; there is no separate group mutex.

- **Object-parent-only owner.** A colocated child records its owner in `colocated_owner_object_type`/`colocated_owner_object_id`. The owner is always an object, never a workflow. Recording it (rather than allocating a group) makes binding replay-safe and idempotent: a command that retries a child-start recomputes the same owner and re-stamps the same value.
- **Flattened root owner.** When an object command starts a colocated child, the child's owner is the starting object's _own_ owner if that object is itself colocated, otherwise the starting object itself. Every colocated child therefore points directly at the root owner object, so an entire colocated tree shares one owner with no transitive resolution at claim time.
- **Owner-lease co-tenancy invariant.** The owner object's lease is the single gate. A worker may claim a colocated child (workflow or object) only if it already holds, or can atomically acquire, the owner object's lease (the lease row is unleased, expired, or already owned by that worker); acquiring the child lease and the owner lease is one atomic step. While the owner object is live on worker A, no other worker can claim any colocated child of it — this is what prevents a second worker from leasing a child while the owner runs on A, and vice versa.
- **No separate binding step.** The owner columns are stamped into the child's insert, so a colocated start adds no statement beyond the child row that is created anyway. The owner object is the live holder at start time (the command that starts the child holds the object's lease), so the child is born under the worker already running the object.
- **Owner keepalive on child heartbeat.** A colocated child workflow's step heartbeat also renews the owner object's lease, so a long-running colocated child keeps the owner — whose command finished long ago — protected from takeover. Object commands are short and re-acquire the owner lease at claim, so child objects need no separate heartbeat. Non-colocated children (`NULL` owner) never touch the owner lease.
- **Guarded release and crash-safe re-homing.** The owner object's lease is released only when no colocated child of it is still live: a guarded `release_object_lease` checks a `NOT EXISTS` over non-terminal colocated child workflows and still-leased colocated child objects. Because a child cannot be claimed without the owner, and the owner cannot be released while a child is live, the owner and its children can never be actively leased by two workers. Colocation never pins work forever: if a worker dies, the owner lease and the children's leases all expire; once everything is idle the guard passes and the next claim re-homes the owner and its children together onto a new worker.
- **Hot-path cheapness.** Non-colocated children have `NULL` owner columns and short-circuit every owner check, paying nothing on the claim, poll, heartbeat, or task-write paths. Colocated claims fold owner-lease acquisition into the existing single claim statement (a CTE on PostgreSQL/YSQL, a conditional owner update wedged between the candidate select and the row update on MySQL/MariaDB) and so add no extra round trip; the candidate filter skips children whose owner is held live by another worker, so blocked work never stalls a polling worker. The only added cost is one indexed `NOT EXISTS` probe on the cold release path, empty for non-colocated objects. With no group table, group-row upsert, or group steal/release sweep, this is strictly cheaper than a separate group lease.

## Inter-node RPC

Remote intranode/inter-pod communication uses gRPC over HTTP/2 (via `async-grpc`), with Paquito/Marshal value-object payloads inside gRPC messages (Ruby-to-Ruby — durababble does not carry cross-language interop, so it does not pay the protobuf schema tax). Each pod runs a dedicated `Durababble::Rpc::Server` (an `Async::HTTP::Server` with an `Async::GRPC::Dispatcher` on a reactor thread), bound to `rpc_host:rpc_port`. The transport is currently cleartext h2c with no built-in peer authentication; an optional `authorize:` hook on `Rpc::Server` runs at the application layer. Production hardening (mTLS / SPIFFE identity, ideally provided by the deployment's service mesh) is target work — see [Cluster RPC § Transport Security](content/cluster-rpc.md#transport-security).

The service shape is part of the runtime contract. Four unary methods plus one server-streaming method:

| Method | Request | Response |
| --- | --- | --- |
| `AwakenBatch` | `Messages::AwakenBatchRequest` | `Messages::AwakenBatchResponse` |
| `EvictLease` | `Messages::EvictLeaseRequest` | `Messages::EvictLeaseResponse` |
| `DeliverMessage` | `Messages::DeliverMessageRequest` | `Messages::DeliverMessageResponse` |
| `CallTransient` | `Messages::TransientRequest` | `Messages::TransientResponse` |
| `CallTransientStream` | `Messages::TransientRequest` | stream of `Messages::StreamFrame` |

All message bodies are `Durababble::Rpc.dump`/`Rpc.load` (Paquito with a single-byte version prefix wrapping Marshal). The `Messages` value-object fields (see `lib/durababble/rpc_messages.rb`):

- `AwakenBatchRequest { worker_pool, workflow_ids }`
- `EvictLeaseRequest { worker_pool, target_kind, target_class, target_id, expected_worker_id }` (target_kind: `"object" | "workflow"`; target_class empty for workflows)
- `DeliverMessageRequest` — same shape as `EvictLeaseRequest`
- `TransientRequest { worker_pool, class_name, durable_object_id, workflow_id, method, args, deadline_ms, expected_worker_id }` (`args` is a nested Paquito blob; `durable_object_id` is the prefix to avoid shadowing `Object#object_id` on the value class)
- `TransientResponse` — discriminated, exactly one of `{ ok: <paquito_bytes>, err: RemoteError, not_running: true, moved: LeaseMoved }`; `#result` reports which (mirrors the former protobuf oneof accessor)
- `RemoteError { klass, message, backtrace }`
- `LeaseMoved { new_rpc_address, new_node_id }`
- `StreamFrame { kind, value, error }` (`kind: :value | :error`; a `:value` frame carries one Paquito application value, an `:error` frame carries a `RemoteError` and terminates the stream)

RPC semantics:

- **AwakenBatch** is a latency optimization after workflow starts or matured scheduler rows. It never replaces durable DB state or scheduler correctness.
- **DeliverMessage** wakes an owner for already-committed inbox rows. It carries no user payload, but it does carry `expected_worker_id` so a fresh worker at a recycled address can ignore a wake intended for the previous process incarnation. The receiver queries durable inbox rows and schedules an owner-local target activation without blocking the RPC handler on user code. If the receiver no longer owns the lease, it returns success without work and scheduler recovery remains the correctness path.
- **CallTransient** is non-durable RPC for exposed methods against the active owner. It carries `expected_worker_id`; the receiver rejects the call as stale before invoking user code if its local worker identity does not match. It returns a Paquito result, a remote error, `not_running`, or `LeaseMoved`.
- **CallTransientStream** is non-durable server-streaming RPC for `expose_stream` methods against the active owner. The owner-local instance runs the `expose_stream` method and forwards each yielded value as a `:value` `StreamFrame`; an unhandled producer error becomes a terminal `:error` frame carrying a `RemoteError`. Only methods registered with `expose_stream` are callable (mirroring the `expose_command` guard); a request for any other method ends the stream with `UnknownCommand`. Unlike the unary methods it does not carry `expected_worker_id`: ownership is enforced by lease re-checks up front, before each emit (workflow streams re-read `current_workflow_lease`, throttled to ~1s; object streams hold the unified `durable_objects` lease through a per-node refcounted `ObjectStreamHost` renewed by an Async task), and after the producer returns, so a lost or moved lease ends the stream with a terminal `StaleLease` frame and the consumer reconnects onto the new owner. Producers of indefinite streams poll `Durababble.stream_cancelled?` to exit when the consumer disconnects or the lease is lost.
- **EvictLease** asks a pod to drop a cached lease it may no longer own. It also carries `expected_worker_id` and is ignored by workers whose local identity does not match the intended lease owner.
- Connection failure to an owner causes short retry, lease re-check, and reroute. If wakeup still fails after the retry budget, the already-committed target activation remains the correctness path and is eventually claimed by a worker in the target pool.
- Receivers reject stale in-flight messages unless they still own the target before and after handler execution.
- Auxiliary test transports must not be used for production intranode communication.

**Retry policy (only known errors are retried).** Routing layers retry only typed routing failures (`NodeUnavailable` for transport-level unavailability — connection refused / timeout / HTTP/2 stream reset / gRPC `Unavailable` / gRPC `DeadlineExceeded` / gRPC `Cancelled`; `StaleLease` for a moved lease; `NoActiveLease` for an unowned workflow). An unexpected raise inside a handler is returned as gRPC `Internal` and surfaces on the client as `Rpc::Error` (deliberately not a subclass of `Rpc::Unavailable`); the router does **not** retry. The rationale is to avoid amplifying load against a peer that has just hit an unforeseen bug — a stampede caused by automatic retries can compound a single bad node into a cluster-wide failure.

## Configuration, limits, and operations

Configuration splits into process-wide module state and per-worker-pool runtime arguments; there is no single block-form config object.

`Durababble.configure(database_url:, schema: default_schema)` connects and installs the process-wide default store (closing any previous one) and returns it. `database_url` defaults to `ENV["DURABABBLE_DATABASE_URL"]` at call sites that read `default_database_url`; `schema` defaults to `ENV["DURABABBLE_SCHEMA"]` or a schema name derived from the workspace path. Other process-wide knobs are plain module setters rather than block fields:

```ruby
Durababble.configure(database_url: ENV.fetch("DURABABBLE_DATABASE_URL"))

Durababble.logger = Rails.logger
Durababble.max_workflow_history_events = ENV.fetch("DURABABBLE_MAX_WORKFLOW_HISTORY_EVENTS", "10000").to_i
Durababble.workflow_history_warning_events = ENV.fetch("DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS", "8000").to_i
Durababble.payload_limits = {
  workflow_input: 4 * 1024 * 1024,
  workflow_result: 4 * 1024 * 1024,
  step_output: 4 * 1024 * 1024,
  object_state: 4 * 1024 * 1024,
  inbox_payload: 4 * 1024 * 1024,
  rpc_argument: 4 * 1024 * 1024,
}
Durababble.configure_observability(enabled: true, attributes: { "service.name" => "billing" })

# Alternatively, install a pre-built store or engine directly.
Durababble.default_store = my_store
```

Worker pools, RPC binding, leases, scheduler cadence, and concurrency are per-runtime arguments to `Durababble::WorkerRuntime.new` (one runtime per pool), not module-global config:

```ruby
Durababble::WorkerRuntime.start(
  workflows: [ChargeOrder, FulfillOrder],
  objects: [Account],
  worker_pool: "default",
  store: Durababble.store,        # or database_url:/schema: to own a connection
  worker_id: nil,                 # defaults to "<pool>-<random>"
  lease_seconds: 30,
  poll_interval: 0.1,
  concurrency: 4,
  migrate: true,
  rpc_host: ENV.fetch("POD_IP", "127.0.0.1"),
  rpc_port: ENV.fetch("DURABABBLE_RPC_PORT", "0").to_i,
  rpc_credentials: :this_port_is_insecure,
  rpc_pool_size: 4,
)
```

Worker runtimes and workflows run inside an Async fiber reactor where each fiber needs its own ActiveRecord connection, so Durababble requires `ActiveSupport::IsolatedExecutionState.isolation_level = :fiber`. It does not set this process-global itself; it asserts it lazily at workflow-execution time via `assert_fiber_isolation!` and raises `Durababble::ConfigurationError` if the host left the default `:thread` isolation in place. Falcon's Railtie sets `:fiber` defensively even under Puma.

The current Ruby API exposes payload limits as one process-wide override hash: `Durababble.payload_limits = { workflow_input: bytes, workflow_result: bytes, step_output: bytes, object_state: bytes, inbox_payload: bytes, rpc_argument: bytes }`. Matching environment variables are `DURABABBLE_MAX_WORKFLOW_INPUT_BYTES`, `DURABABBLE_MAX_WORKFLOW_RESULT_BYTES`, `DURABABBLE_MAX_STEP_OUTPUT_BYTES`, `DURABABBLE_MAX_OBJECT_STATE_BYTES`, `DURABABBLE_MAX_INBOX_PAYLOAD_BYTES`, and `DURABABBLE_MAX_RPC_ARGUMENT_BYTES`; `workflow_args` and `DURABABBLE_MAX_WORKFLOW_ARGS_BYTES` remain compatibility aliases for workflow input bytes. Every default is `4 * 1024 * 1024` serialized bytes, and every configured value must be positive.

Size guards are production requirements. Durababble raises `Durababble::PayloadTooLarge` when serialized workflow inputs, workflow results, step outputs, durable object state, inbox payload/result bytes, or `CallTransient` RPC arguments exceed the configured maximum. Checks run after Paquito serialization against the exact byte string that will be written to MySQL/MariaDB `LONGBLOB`, PostgreSQL/YSQL `bytea`, or sent over the inter-node RPC channel as the HTTP/2 request body; PostgreSQL's client-side hex literal text is not counted as payload size. The error message identifies the surface and context such as workflow id, object id, inbox message id, or RPC method, but it must not include the serialized payload or original Ruby value. Rejected writes happen before the durable mutation or inside a rolled-back SQL transaction, so oversized workflow rows, history completions, inbox rows, object-state updates, and RPC handler side effects are not partially persisted. RPC servers re-check the `rpc_argument` limit on the inbound leg before deserializing, so a peer that skipped the client-side guard still cannot push oversized payloads through; the server surfaces the rejection as an `err` frame in the `TransientResponse`. The runtime does not silently spill oversized values to blob storage.

The workflow-history guard is a replay-cost and correctness guard, not a retention system. Applications should alert on the warning log at `DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS`, alert on `Durababble::WorkflowHistoryLimitExceeded`, inspect the workflow type/id and history count, and remediate by splitting the workflow into smaller durable runs/objects, pruning or compacting old terminal histories with an explicit operator tool, or raising `DURABABBLE_MAX_WORKFLOW_HISTORY_EVENTS` only after the long-history benchmark shows acceptable replay latency and allocation cost.

CLI tooling is not part of the current Durababble contract. Store migrations remain an explicit setup/deploy responsibility through the store API, and runtime operation is embedded through `Durababble::WorkerRuntime`, `Durababble::Worker`, workflow handles, durable-object handles, and application-owned process supervision. Future non-contract operator or web tooling may add version and inspection views, bounded workflow/object listing, history pruning, dead-letter retry/skip/cancel/destroy/repair flows, and similar operational controls, but this specification does not require a CLI for migration, workflow run/resume, inspection, version output, or operator repair.

Observability requirements:

- Benchmarks record operation latency, throughput, and allocation reports for workflow queueing, leases, waits, outbox, fences, inbox, deterministic workflow replay, and local step execution.
- OpenTelemetry support is an optional integration over the official API gems. `Durababble.configure_observability(enabled: true, attributes:)` uses `OpenTelemetry.tracer_provider` and `OpenTelemetry.meter_provider`; Durababble never configures an SDK, collector, or exporter for the application.
- With observability disabled, instrumentation exits through cheap no-op checks before dynamic attributes are built. Durababble callsites construct valid, low-cardinality OpenTelemetry attribute hashes directly; application-provided static attributes should use string keys and OpenTelemetry-compatible scalar values.
- Stable span names include `durababble.workflow.start`, `durababble.workflow.resume`, `durababble.workflow.execute`, `durababble.workflow.step`, `durababble.object.query`, `durababble.object.command.enqueue`, `durababble.object.command`, `durababble.workflow_rpc.*`, `durababble.rpc.client.*`, and `durababble.rpc.server.*`. Durababble does not wrap ActiveRecord SQL execution in its own spans; applications should use standard ActiveRecord/database OpenTelemetry instrumentation for SQL visibility.
- Stable span and metric attributes use the `durababble.*` namespace and may include workflow id/name/status, step name/index/attempt, object type/id/method, worker pool/id, lease owner, wait kind, retry delay, store backend for higher-level queue metrics, RPC method/target shape, and `error.type`. Instrumentation must not attach SQL text, serialized payload bytes, raw user arguments, secret-bearing values, or unbounded free-form payload data.
- Metrics include workflow start/completion/failure/cancellation counters, step attempt/success/failure/retry counters, wait start/completion counters and wait latency histograms, queue claim latency, lease heartbeat/conflict/expired-recovery counters, outbox pending/processed/failure counters, worker tick duration/counts, and workflow replay/history size measurements. Cancellation/termination, explicit outbox delivery failure, richer replay cost, and object mailbox queue-depth metrics remain reserved where the runtime feature is not implemented yet. Applications should use ActiveRecord/database OpenTelemetry instrumentation for SQL operation latency/error metrics.
- OpenTelemetry spans wrap public calls, workflow executions, steps, durable-object commands/queries, workflow RPC routing, and inbound RPC requests. Spans include worker pool, class, target id, and lease owner where relevant.
- Bugsnag/error integration reports unhandled exceptions inside commands, exposed methods, steps, and RPC handlers.
- Slow-step warnings are emitted.
- Routing health metrics cover wakeup error rate, wakeup latency, and lease takeover frequency.
- Circuit breakers around database connections cause public methods to raise a typed error such as `Durababble::CircuitBreakerOpen` when the durable store is unavailable before commit.
- RPC server health metrics cover in-flight requests, reactor saturation, and dropped requests.

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
| Incomplete steps are retried | Incomplete/running/failed/waiting command state is retried or continued according to durable state. | Crash matrix |
| Workflow command history is replay truth | Schedule, start, and completion/failure/wait records are distinct append-only history facts. | Async workflow replay specs plus backend conformance |
| Child workflow starts are durable and idempotent | Parent/object origins persist the child workflow row with origin metadata before returning a handle; replay or command retry reattaches to the same child and rejects shape conflicts. | Child workflow regression specs plus backend conformance |
| Child workflow awaits are replay-safe | Parent workflow observes child status through recorded commands, suspends on durable waits while pending, reuses completed results, and propagates child failure/cancel/termination as typed child errors. | Child workflow wait/crash/failure specs |
| Child cancellation is policy driven | Parent cooperative cancellation requests child cancellation only for child rows whose policy is `request_cancel`; `abandon` and parent hard termination do not mutate child state. | Child workflow cancellation and termination specs |
| Colocated children share one live worker | The owner object's own lease is the single gate; a worker claims a colocated child (workflow or object) only while acquiring/holding the owner object's lease, and the owner lease is released only when no colocated child is live, so owner and colocated child are never actively leased by two workers at once. | Colocation claim-gate and owner-fence backend conformance specs plus child-start reuse conflict specs |
| Colocation is crash-safe, not permanent pinning | When the holder dies, the owner lease and the children's leases expire; once idle the guarded release passes and the next claim re-homes the owner and its children together onto the next worker; colocation never strands work on a dead worker. | Colocation crash/recovery specs plus crash matrix |
| Parallel schedule shape is validated | Replay validates method, args/kwargs digest, retry/executor attributes, and semantic key for every scheduled command, including incomplete commands. | Fanout replay divergence specs |
| Workflow future resolution is deterministic | Step completions, failures, timer fires, workflow command deliveries, and child completions resume workflow fibers in history order. | Continuation fanout replay specs |
| Step attempts are append-only | Every started attempt and terminal attempt state remains inspectable. | Guarantee matrix |
| Timer wait attempts complete once | Wait completion updates attempts from `waiting` to `completed` without losing payload. | Wait-attempt spec |
| Timer waits survive process exit | Workflow history stores the timer wake time/context, and `workflows.next_run_at` stores the earliest unresolved wake for queue claims. | Timer wait tests |
| Side effects can be fenced by key | A fence records `running` before yielding and exposes operator-visible recovery for abandoned owners. | Fence concurrency spec plus owner-crash spec |
| Outbox delivery is durable and leased | Outbox rows are unique by key, claimable, acknowledgeable, and reclaimable after expiry. | Outbox specs |
| Workflow commands wake and run promptly | Command enqueue wakes the active owner or leaves a durable target activation; no workflow-side broadcast wait is required. | Workflow command mailbox specs plus RPC wakeup specs |
| Synchronous durable commands return results | Ask rows store serialized result/error and caller retries with the same idempotency key reattach. | Workflow/object ask specs |
| Inbox is not a second global polling queue | Workers poll coalesced target activations and target owners drain inbox rows for their own target. | Query-plan and mailbox specs |
| Workflow RPCs route to active lease holder | RPC routing validates owner before/after handling, refreshes ownership after transport failures, and reroutes. | Workflow RPC spec plus gRPC transport spec plus DST scenarios |
| Inter-pod RPC uses the fixed gRPC service | Runtime RPC serves `AwakenBatch`, `EvictLease`, `DeliverMessage`, `CallTransient`, and the server-streaming `CallTransientStream` over async-grpc with an optional application-layer authorize hook. | RPC transport integration/contract tests plus DST response scenarios |
| Multi-row state transitions are transactional | Step start/finish/failure, wait transitions, inbox enqueue, mailbox advancement, and state/result writes commit atomically where required. | Implementation plus regression suite |
| Runtime values are Paquito bytes | Runtime payloads use Paquito bytes in `bytea` / `LONGBLOB`, not JSONB. | Payload storage specs |
| MySQL/MariaDB honors common store semantics | MySQL/MariaDB and PostgreSQL/YSQL satisfy the same store behavior contract. | Backend conformance spec |
| Durable object API uses durable mailboxing | `at`, `tell`, `expose`, and `expose_command` execute through per-object mailbox ordering and lease ownership. | Durable object specs |
| Durable objects start workflows only from commands | Object-origin workflow starts persist a child link under the object command id and observe child outcomes later through persisted state, wakes, signals, or handle reads instead of blocking the command on a child await. | Child workflow durable-object specs |
| Object commands are per-id FIFO and worker-driven | Inbox/mailbox execution enforces one writer, blocked-head behavior, and worker-driven retries. | Object mailbox specs |
| Named object wakes convert to durable wake messages | Each named wake row atomically converts to a wake inbox row carrying its name, without losing wakes. | Object wake specs |
| Workflow patch markers guard code evolution | `patched` / `deprecate_patch` append and check ordered workflow history markers before branch side effects. | Patch-marker unit, backend-conformance, and crash tests |
| Transient exposed methods route to owner | `CallTransient` invokes live object/workflow owner without durable mutation. | Transient RPC specs |
| Worker pool scopes persisted targets and relevant keys | Persisted targets and query-critical keys include `worker_pool` where routing/claiming requires it. | Worker-pool backend specs |
| Unified inbox is the durable message model | Object commands, object wakes, and workflow commands share one inbox contract. | Inbox/mailbox specs |
| Alloy model tracks storage/lease/concurrency invariants | `formal/workflow_storage.als` checks model obligations; `[DURABABBLE-*]` sigils tie each obligation to a Ruby implementation/test callsite. | `rake formal` (Alloy verifier) plus `FormalSigilDriftTest` in CI |

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
| After child workflow start commits, before parent awaits | Parent replay reattaches to the existing child workflow row and does not create a duplicate child workflow. |
| While parent is waiting on a child | The parent remains suspended through a durable wait, the child continues in its worker pool, and parent replay observes the child terminal result/error when it wakes. |
| Owner object's command finishes while a colocated child still runs | The colocated child workflow's heartbeat keeps the owner object's lease alive on its worker; no other worker can claim the owner object until that lease lapses, and the owner lease cannot be released while the child is still live. |
| Worker holding a colocated owner object dies | The owner object's lease and the children's leases all expire; once idle the guarded release passes and the next claim re-homes the owner object and its colocated children together onto a new worker. |
| After cancellation cleanup step completes, before canceled terminal write | Completed cleanup step is skipped and workflow finishes `canceled` on recovery. |
| While waiting when cancellation is requested | Waiting step/attempt state is marked canceled, cleanup runs on next claim, and late timer claims are ignored. |
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
| Crash during object wake-to-inbox conversion | Either the named wake row remains or its wake inbox row exists; no wake is lost. |
| Crash after patch marker commit before first new-branch step | Replay sees marker, `patched` returns true, and the new branch continues. |
| Code removes `patched` while normal marker history still exists | Checker raises replay divergence before any later durable write. |
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
- RPC tests cover stale lease, lease moved, no-active-owner, shutdown/non-running workflow, retry/reroute, Paquito serialization, unavailable-node, timeout, deadline, RST, EOF, lost-response, duplicate-response, auth-failure, wakeup drops/duplicates, and all four service methods.
- Object mailbox tests cover strict FIFO, blocked head behavior, ask/tell ordering, wake ordering, idempotency conflicts, owner crash, lease takeover, dead-letter, and operator repair paths.
- Workflow command tests cover history acceptance, deterministic replay order, timeout behavior, terminal-workflow rejection, and idempotency dedup.
- Workflow patch-marker tests cover first-run marker recording, no-marker `false` branches, marker-history `true` branches, missing-marker replay divergence failures, `deprecate_patch` cleanup, duplicate-id handling, backend conformance, and crash after marker commit.
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
- Streaming is limited to the `expose_stream` / `CallTransientStream` server-streaming surface against a live lease owner. No broader streams API (durable stream storage, pub/sub fan-out, replayable stream history) without a concrete consumer requirement.
- No automatic cross-pool routing; relocation/failover is explicit.
- No silent payload spill to blob storage; oversized values fail loudly.
- No production RPC transport other than the fixed gRPC service over `async-grpc` (the four unary methods plus the `CallTransientStream` server-streaming method).
- No runtime loading or validation of user RBS.
- MySQL/MariaDB support is required for the common public contract.
- Worker registry misses are avoided by claiming only workflow/object classes present in the supplied registry. Enqueuing a workflow name with no corresponding worker pool leaves it pending until an appropriate pool starts.
- Long-running steps do not heartbeat automatically while user code runs.
- Class-level serialized state migrations and node capability routing are not part of this contract.
