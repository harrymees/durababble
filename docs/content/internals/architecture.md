---
title: "Architecture"
weight: 5
---

# Architecture

Durababble is a Ruby 4 durable execution prototype. Ruby owns workflow and durable-object definitions; a SQL store owns durable coordination and recovery state. The primary tested stores are YugabyteDB/YSQL through the PostgreSQL wire protocol and MySQL/MariaDB through Trilogy.

## Components

- `Durababble::Workflow`: class-oriented workflow base. A subclass implements `#execute(input)` for deterministic orchestration and marks side-effect boundaries with `step def ...`, `step retry: ...` followed by `def ...`, or imported reusable step objects created with `Durababble.step("name") { ... }`. Steps are called as ordinary workflow methods or as callable constants from orchestration code; the engine assigns durable positions by deterministic execution order.
- `Durababble::ChildWorkflowHandle`: explicit handle returned by workflow-context `Workflow.enqueue` / `Workflow.start` and durable-object command `Workflow.enqueue` / `Workflow.start`. It exposes child workflow id, worker pool, cancellation policy, status/result/error, `await`, `cancel`, `terminate`, and `ref`, while the store owns replay-safe origin metadata on the child workflow row. In workflow execution, `result` durably awaits the child; outside workflow execution, it reads the latest workflow result without blocking.
- `Durababble::DurableObject`: class-oriented durable object base. A subclass is addressed by `Class.at(id)` / `Class.handle(id)` for typed handle calls; each helper also accepts `engine:` or `store:` for explicit routing. Durable objects expose public read methods with `expose`, expose public mutating commands with `expose_command`, and mutate state explicitly with `update_state(new_state)`. Durable object methods are not workflow steps.
- `Durababble::RetryPolicy`: normalizes retry options (`initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`, explicit `schedule`, and `non_retryable_errors`) and computes durable retry delays for workflow steps and durable-object commands.
- `Durababble::Engine`: enqueues and resumes workflow runs, enforces workflow lease ownership, records workflow step transitions, handles explicit step heartbeats, handles retryable step failures, handles waits, handles terminal cancellation/termination outcomes, and skips completed steps during recovery. `Durababble.configure` installs a default engine over the configured default store for top-level class helpers.
- `Durababble::Worker`: polls for one runnable workflow or target activation at a time. Workflow rows execute through the deterministic engine; workflow and object target activations drain the target inbox under the worker's lease identity. The worker registry contains workflow classes and durable object classes.
- `Durababble::WorkerRuntime`: high-level app/process entrypoint for a named worker pool. It starts a background scheduler, serves RPC wakeups, advertises a per-process identity in the form `worker-id@host:port`, runs up to a configurable number of target executions concurrently with `async` fibers, stops taking new work on shutdown, waits for in-flight work up to a timeout, and releases this worker's leases if the timeout expires.
- `Durababble::WorkflowRpc`: routes node-to-node workflow RPCs through the current workflow lease holder and rejects stale in-flight messages when ownership changes or the workflow stops running. This is the lower-level routing primitive; public `Workflow.handle(...).expose` calls use `CallTransient` against the active owner without durable mutation, while `Workflow.handle(...).expose_command` records durable workflow command inbox rows and wakes or warms the workflow target before execution.
- `Durababble::Rpc::Server` / `Durababble::Rpc::Client`: gRPC-over-HTTP/2 transport (via `async-grpc`) for cross-node wakeups, evictions, transient calls, and durable-message wakeups. Wire payloads are Paquito/Marshal value objects defined in `Durababble::Rpc::Messages` (Ruby-to-Ruby — durababble does not carry cross-language interop). Workflow transient calls use `Durababble::Rpc::WorkflowClient` to bridge `WorkflowRpc::Router` onto the `CallTransient` method. The transport is currently cleartext h2c with no built-in peer authentication; see [Cluster RPC § Transport Security](/cluster-rpc/#transport-security).
- `Durababble::Store`: backend-selecting durable store facade. `postgresql://`/`postgres://` URLs use the PostgreSQL/YSQL adapter with the `pg` gem; `mysql://`/`mysql2://`/`trilogy://` URLs use the MySQL/MariaDB adapter with the `trilogy` gem. It owns schema migration and all durable state transitions. Runtime Ruby values are serialized through Paquito and stored in binary columns (`bytea` on PostgreSQL/YSQL, `LONGBLOB` on MySQL/MariaDB). Configurable serialized-byte limits reject oversized workflow inputs, workflow results, step outputs, durable-object state, inbox payload/result bytes, and RPC arguments before durable mutations can partially commit. If callers do not pass `schema:`, the default namespace comes from `DURABABBLE_SCHEMA` or from deterministic `Durababble.workspace_schema(DURABABBLE_WORKSPACE_ROOT || Dir.pwd)`; PostgreSQL/YSQL uses that namespace as a schema, while MySQL/MariaDB uses it as the durable table prefix inside the configured database.
- `sig/durababble.rbs`: static-only RBS declarations for the public class API, including the optional handle dispatch generic used by workflow and durable object RPC handles. Runtime execution does not load or validate RBS.
- `formal/workflow_storage.als`: Alloy model for workflow state, ordered workflow command history, leases, timer wait facts, fences, outbox rows, target activations, and FIFO inbox command serialization for object and workflow targets. `scripts/verify-alloy.sh` verifies the model, and `test/durababble/formal_sigil_drift_test.rb` keeps `[DURABABBLE-*]` obligations synchronized with Ruby implementation/tests on every PR.

Application setup owns migrations. `Store#migrate!` should run from deploy/setup orchestration before workflow engines, class helpers, or durable-object handles enqueue or query work; those runtime paths expect the selected namespace to already exist rather than migrating on demand.

## Public API model

### Workflows

Workflow classes look like ordinary Ruby objects:

```ruby
class CounterWorkflow < Durababble::Workflow
  workflow_name "counter"

  def execute(input)
    double(increment(input))
  end

  step def increment(input)
    { "count" => input.fetch("count") + 1 }
  end

  step def double(input)
    { "count" => input.fetch("count") * 2 }
  end
end
```

When `#execute` calls a step method, the wrapper delegates to `WorkflowExecution#call_step`; when it calls a reusable step constant, the step object delegates to `WorkflowExecution#call_step_object`. Direct orchestration waits (`sleep`, `wait_until`, and `wait_condition`) and in-workflow handle RPCs (`Workflow.handle(...).approve(...)`, `DurableObject.at(...).credit(...)`, and `DurableObject.tell(...)`) delegate to the same workflow command scheduler without wrapping the operation in a user step. The execution object:

1. assigns the next step position;
2. returns a persisted result immediately if that position already completed and the recorded step name matches the current method;
3. records the step start and attempt;
4. builds `step_context` with a generated idempotency key and heartbeat;
5. invokes the original Ruby method body;
6. persists success, wait, retryable failure, or final failure.

This means step identity is based on deterministic call order. The method step name or reusable step object's explicit name is recorded as metadata; callers do not pass per-call step names. If replay reaches a completed position with a different current step name, or if workflow execution returns before all completed positions have been consumed, the engine fails the run with `Durababble::ReplayDivergenceError` so code changes cannot silently reuse stale results or drop a recorded durable suffix.

Before loading replay payloads, `Engine#execute` asks the store for the workflow's `workflow_history` row count and compares it with `Durababble.max_workflow_history_events` (`DURABABBLE_MAX_WORKFLOW_HISTORY_EVENTS`, default `10_000`). It also logs through `Durababble.logger` once a run reaches `Durababble.workflow_history_warning_events` (`DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS`, default `8_000`). `WorkflowExecution` checks projected history growth before scheduling a new durable command, so a run that would cross either threshold is observed before more history rows are appended. The counted rows are the replay facts in `workflow_history`; latest-state tables such as `steps`, `step_attempts`, `inbox`, and `outbox` are audited separately but are not replay input unless the operation appends a workflow-history row. If an open workflow is already over the bound, or a new command would push it over the bound, the engine records `Durababble::WorkflowHistoryLimitExceeded` as the workflow's terminal failed error instead of repeatedly claiming and replaying the oversized run. Terminal workflow target activations dead-letter pending workflow-command inbox rows and delete the coalesced activation, so terminal oversized workflows do not leave runnable activation tasks that workers claim forever. Terminal rows bypass the guard so prior completed/canceled/failed results are still readable.

Direct waits, child workflow operations, and in-workflow handle RPCs use the same monotonic command ids and replay validation as steps, but their command names are the wait helper, `child_workflow:<workflow_name>:start`, `child_workflow:workflow:observe`, or `handle_rpc:<target_kind>:<target_type>:<method>` rather than user step method names. A direct wait records a scheduled command, records a waiting history entry with the timer metadata, and stores the earliest unresolved timer on `workflows.next_run_at` when the activation parks. When a worker later claims the due workflow, replay completes the due timer under the workflow lease and resumes the blocked workflow fiber. A child start records a scheduled command before inserting the child workflow row with parent/origin metadata; a child observe records the child's current status/result/error as the command resolution. A handle RPC records a scheduled command before dispatching the workflow/object handle call and records the returned result or error as the command resolution, so replay can reuse the recorded result without sending the outbound inbox message or query again. Replay indexes terminal history by command id and uses the latest terminal event, so a completed timer event overrides the earlier waiting event for the same command.

Workflow code starts children with `ChildWorkflow.enqueue(input, id: nil, worker_pool: nil, idempotency_key: nil, cancellation: :request_cancel)` or `ChildWorkflow.start(...)`. The returned `ChildWorkflowHandle#result` polls through recorded observe commands and durable timer waits when called from workflow execution, so a parent crash after child creation or while waiting does not duplicate the child and does not lose the wait. `ChildWorkflowHandle#await` remains available for explicit poll intervals or timeouts. Child workflows run in their persisted worker pool; a child in another pool or without a registered worker remains pending and inspectable until a matching worker appears or an operator acts on the handle. Child retry is independent from parent retry.

While `#execute` is running, Durababble enables a workflow-local determinism guard for the current execution thread/fibers. Direct user orchestration calls to host wall-clock time, randomness, blocking sleeps, process APIs, and blocking file/IO operations raise `Durababble::DeterminismError`; Durababble's own persistence calls are scoped out of the guard, and transient step fibers clear workflow context so step bodies keep normal Ruby host semantics.

Workflow `expose` and `expose_command` define the public handle surface:

```ruby
workflow = CounterWorkflow.at(run_id)
workflow.description
workflow.cancel(reason: "user request")
workflow.terminate(reason: "operator hard stop")
```

Exposed workflow queries route through `WorkflowRpc::Router` to the workflow's current active lease owner and then through `CallTransient`; when the owner is the same `WorkerRuntime`, Durababble takes an owner-local fast path into the same transient handler while still checking the active lease. Query calls reject `idempotency_key:`, do not enqueue inbox rows, do not start or warm workflows without an active owner, and do not append workflow history when called externally. Exposed workflow commands persist `workflow_command` inbox rows for the workflow target, wake the active leaseholder through `DeliverMessage` when one exists, and wait for the ask row to store the command result or error. The owner delivers those rows through the active `WorkflowExecution` at deterministic safe points, invoking the command method on the same workflow instance that is running or replaying `#execute`; workers poll coalesced target activations as the durable fallback rather than constructing a separate workflow object to drain the inbox. When workflow orchestration code calls workflow handle APIs, `WorkflowExecution#call_handle_rpc` wraps the same public query or command surface in workflow history, so replay returns the recorded result without re-routing a query or re-enqueuing a command.

### Durable objects

Durable objects are identity-addressed classes with explicit state updates:

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

The durable-object contract is actor-like: commands for the same `(object_type, object_id)` serialize through that identity, each command receives `command_context`, and retries/recovery are recorded in the unified inbox. Synchronous asks and asynchronous tells enqueue inbox rows and wake or activate the object target; workers drain the object's contiguous ready mailbox prefix, invoke exposed command methods, and commit state plus message completion in one store transaction. Generated command idempotency keys remain stable across retries because they derive from the durable mailbox row.

Exposed object queries are transient owner-local reads served from the single resident instance the owning node keeps for each object. If an active object lease exists, callers route the query through `CallTransient` to that owner, or take an in-process fast path only when the current runtime owns the same lease. If no node owns the object, a caller with a local residency host claims the lease and becomes the resident owner — materializing the instance via `on_load` (or `on_create` when no durable state exists) and keeping it warm — before serving the read; a caller with no host falls back to a transient claim-read-release. There is no mailbox read gate: queries run against the live resident instance on the owner's single cooperative reactor, where commands for one object are FIFO-serialized, so a read reflects every committed command and never observes a stale snapshot that skipped unresolved work. The owner holds the lease continuously as a residency token and evicts the instance (running `on_destroy`) after an idle window or on lease loss.

Workflow orchestration can call durable-object handles directly. The handle acquisition uses the workflow execution store when no explicit `store:` or `engine:` is passed, and the query/ask/tell call is recorded as a workflow history command before object dispatch. Recorded command results replay as the original query result, ask result, or tell message id, so recovery does not duplicate object inbox rows after a crash.

Durable object commands can start workflows with `WorkflowClass.enqueue(input, id: nil, worker_pool: nil, idempotency_key: nil, cancellation: :abandon)` or `WorkflowClass.start(...)`. The object command persists object-origin metadata on the child workflow row immediately using the durable command id, so a retry of the same command reattaches to the child instead of starting another one. Object commands do not synchronously wait for children; they observe later by storing the child id in object state, scheduling a named wake, receiving a later signal/command, or reading a workflow handle after the command commits.

Object commands may schedule any number of independent, named wakeups with `schedule_wake(name:, at:, payload: nil)`, replace one by re-scheduling the same `name`, remove one with `cancel_wake(name:)`, or drop them all with `cancel_all_wakes`. Pending wakeups are stored outside the mailbox so future commands are not blocked behind an alarm time, but scheduling/cancellation commits in the same transaction as command state and command completion. When a wake time matures, the store converts that row into a normal object `wake` inbox message in the same worker pool, carrying the wake's name, and the worker delivers it through `on_wake(name:, payload:)`.

## Storage model

- `workflows`: one row per run; stores status, input, result, errors, workflow lease owner/deadline, `next_run_at` for durably scheduled step retries and earliest unresolved workflow timers, first cooperative cancellation request metadata (`cancel_reason`, `cancel_requested_at`, `cancel_delivered_at`), nullable child parent/origin/cancellation metadata, and the terminal `terminated` state for operator hard stops.
- `steps`: one row per workflow step position; stores latest state, result used for resume, and latest opaque heartbeat cursor for incomplete-step recovery.
- `step_attempts`: append-only attempt history for every started step, including the latest heartbeat cursor observed for each attempt.
- `fences`: idempotency fence state. A row is inserted as `running` before the side effect block executes; waiters read the completed result instead of running the block.
- `outbox`: durable outgoing messages with unique keys, processing leases, expiry recovery, and acknowledgements.
- `durable_objects`: latest durable-object state by `(worker_pool, object_type, object_id)`.
- `object_wakeups`: pending durable-object wakeups keyed by `(worker_pool, object_type, object_id, name)`, so an object can hold several named wakes at once; each row includes the wake time and Paquito payload that will be delivered to `on_wake`.
- `mailbox_sequences`: per-target sequence allocation state for workflow and object inbox messages.
- `inbox`: persisted object asks/tells/wakes plus workflow command messages, including target identity, mailbox sequence, idempotency key, shape hash, retry/dead-letter fields, result/error, and retention deadline.

Durababble enforces its payload limits after Paquito serialization and before writing or sending the serialized bytes. The default for `workflow_input`, `workflow_result`, `step_output`, `object_state`, `inbox_payload`, and `rpc_argument` is 4 MiB each, with `workflow_args` retained as a compatibility alias for workflow inputs in `Durababble.payload_limits`. These checks are deliberately below the storage column capacities: MySQL/MariaDB `LONGBLOB` can store far larger values than the Durababble default, and PostgreSQL/YSQL `bytea` is likewise not the intended application-level bound. The Durababble limits are operational guardrails for replay cost, RPC pressure, and accidental payload capture, not a substitute for backend column definitions.

## Durability semantics

- Enqueue persists the workflow before work is claimed. Callers may provide an explicit workflow id; Durababble generates missing ids above the store boundary, inserts the final id through the workflow row primary key, and raises `Durababble::WorkflowAlreadyExists` on duplicates before any workflow history, wait, inbox, or target activation side effects are written. Terminal workflow rows still own their ids for deduplication.
- Claiming work atomically marks one workflow `running` and writes `locked_by`/`locked_until` using locked queue selection.
- A workflow heartbeat extends an owned running lease.
- A step heartbeat (`step_context.heartbeat.record(cursor)`) compare-and-swaps against the current workflow lease owner/deadline, extends `locked_until`, and stores an opaque Paquito-serialized cursor on the current step/attempt. If the workflow lease expired or moved, the heartbeat raises `LeaseConflict` instead of reviving a zombie owner.
- Expired leases are returned to `pending` for recovery.
- `Engine#resume` refuses to execute a workflow leased by another live worker.
- Before a step runs, its current step row and a new attempt row are persisted transactionally.
- After success/failure/wait, the related step and attempt rows are updated transactionally.
- Before an in-workflow handle RPC dispatches to a workflow or durable-object handle, the workflow records a scheduled command row/history event containing the handle target and call shape. After the handle call returns or raises, the same step result/error machinery records the terminal command resolution.
- Success/failure/wait suspension and final workflow status writes are fenced by SQL conditions on the active workflow lease. If the conditional update does not affect the expected row, the store raises `LeaseConflict` so a timed-out worker whose lease was explicitly released during process shutdown cannot commit stale output after another process has been allowed to retry.
- After success, the current step and attempt are marked `completed` with a Paquito-serialized bytea result.
- After retryable step failure, the current step/attempt record the error, a diagnostic `step_failed` history row is written with retry metadata, the workflow lease is cleared, and `next_run_at` delays the next claim. Those writes happen in one SQL transaction. Replay treats retrying `step_failed` rows as diagnostics, not terminal command resolutions. After attempts are exhausted, or the error class is non-retryable, the workflow records a terminal `step_failed` event for that command and workflow code can rescue it from history.
- Terminal `failed` workflows clear `next_run_at` and are not returned by claim paths. Only failed rows with a non-null due `next_run_at` are treated as retryable queue work.
- On resume, only `completed` steps are skipped; incomplete/running/failed/waiting work is retried or continued. For a retried step, `step_context.heartbeat.cursor` exposes the latest cursor from the previous incomplete invocation.
- Timer wait requests append waiting workflow history, update the latest step/attempt state, and put the workflow in `waiting` with `next_run_at` set to the earliest unresolved wake. When that row is due, the normal workflow claim/replay path completes the timer command under the workflow lease.
- Workflow cancellation is cooperative, not termination. `Workflow.handle(id).cancel(reason:)` records durable cancellation metadata on the workflow row. Pending, waiting, and retry-backoff workflows move to `canceling` and become claimable immediately; running workflows keep their current lease and observe the request at the next step boundary, completed-step replay boundary, or step heartbeat. The engine raises `Durababble::CancellationError` into workflow code. Once raised, cleanup steps run as ordinary durable steps; completed cleanup steps are skipped after crash/recovery. If cleanup returns or re-raises the cancellation error, the workflow becomes `canceled`. If cleanup raises an unrelated non-retryable error, the workflow becomes `failed`; retryable cleanup failures keep `next_run_at` and retry under `canceling`.
- Workflow termination is a separate hard terminal transition. `Workflow.handle(id).terminate(reason:)` stores `status = 'terminated'`, clears leases and retry deadlines, cancels live step attempts, dead-letters queued workflow commands, removes pending workflow target activations, and records a termination history event without delivering `CancellationError` or running cleanup. Stale owners that finish step or workflow code after termination hit the same lease/status fences as any lost owner and cannot change the terminal row.
- Child workflow starts commit the child workflow row with parent/origin metadata atomically before returning a handle. Parent replay reuses the recorded start result, child observe commands reuse recorded status/result/error, and `await` parks through normal durable waits while the child is non-terminal. Parent cancellation requests child cancellation only for child rows with `child_cancellation_policy = 'request_cancel'`; parent hard termination does not run cleanup and does not mutate linked children.
- Operator termination remains a separate hard-stop concept: it may mark or remove work without running user cleanup and must not report the workflow as cooperatively `canceled`.
- Timer completion happens during claimed workflow replay, under the workflow lease, so concurrent workers still have one winner and a due timer command completes once.
- Fences persist a running row before side effects and persist the first completed result for all repeated callers.
- Outbox rows are unique by key, leased for delivery, reclaimable after lease expiry, and acknowledged after external delivery.
- Workflow RPC routing is lease-validated at both ends: callers look up the current active lease holder, dial the address suffix from the persisted `worker-id@host:port` identity, send that full identity as `expected_worker_id`, receivers reject messages unless they are the intended worker incarnation and still own the workflow before and after handler execution, and callers retry stale ownership, no-active-owner, and transport-unavailable failures only after a fresh owner lookup. If the fresh lookup finds no active owner for a recoverable workflow, `WorkflowRpc::Router` starts and awaits a new lease through `WorkflowRpc::LeaseStarter`, then reroutes the original RPC opaquely to the caller; terminal/shutdown states are still surfaced as non-routable.
- Object transient routing follows the same active-owner shape: callers use the object lease row to route `CallTransient` to the owner, local fast paths are allowed only for the runtime that currently owns the object lease, and unavailable-owner failures do not fall back to caller-local state reads. The object transient handler validates lease ownership before and after user code, and object streams run under `ObjectStreamHost`'s claimed/renewed object lease rather than a lease-free snapshot.
- Cross-node workflow RPC uses the same lease validation over gRPC: `Rpc::Server#CallTransient` returns `Messages::LeaseMoved`, `not_running`, or `Messages::RemoteError` responses (Paquito-serialized value objects), and `Rpc::WorkflowClient` decodes those into the typed `WorkflowRpc` errors the router already understands. `DeliverMessage` and `EvictLease` also carry `expected_worker_id` and are ignored by a fresh process that inherited a previous worker's address. The router only retries known transient errors (`NodeUnavailable`, `StaleLease`, `NoActiveLease`); an unexpected raise on the peer surfaces as `Rpc::Error` (gRPC `Internal`) and is **not** retried — see [Cluster RPC § Retry Semantics](/cluster-rpc/#retry-semantics).

The high-risk durability and lease claims above are also modeled in Alloy. The CI-gated command is `mise exec -- bundle exec rake formal`, which runs the Alloy verifier; sigil drift between the model and the Ruby tree is caught separately by `test/durababble/formal_sigil_drift_test.rb` on every PR.

## Application worker lifecycle

`WorkerRuntime` is the intended entrypoint for embedding Durababble in a Rails or similar long-lived app process. The runtime is async-native, so boot it from a caller-owned `Async` reactor and shut it down from that same lifecycle:

```ruby
Async do
  worker = Durababble::WorkerRuntime.start(
    database_url: ENV.fetch("DATABASE_URL"),
    workflows: MyApp::DurableWorkflows.for_pool("default"),
    worker_pool: "default",
    concurrency: Integer(ENV.fetch("DURABABBLE_WORKER_CONCURRENCY", "8")),
    rpc_host: ENV.fetch("POD_IP", "127.0.0.1"),
    rpc_port: ENV.fetch("DURABABBLE_RPC_PORT", "50051").to_i
  )
  sleep
ensure
  worker&.shutdown(timeout: 10)
end
```

The runtime only claims workflow names and object types present in its registries, so separate pools can run different durable families without claiming work they cannot execute. `concurrency:` defaults to `1`; larger values let one Ruby process schedule that many workflow tasks, target activations, or object command drains at once in `async` fibers. The scheduler still keeps one in-flight work item per durable target identity inside the process, so duplicate workflow activations are deferred and commands for one object id remain serialized while unrelated targets use other slots. Host applications should size their ActiveRecord pool for the runtime concurrency plus RPC handlers and ordinary application traffic, and ActiveRecord apps are expected to run with fiber-isolated execution state.

Shutdown is cooperative: the scheduler stops claiming new work and returns `:stopped` if all active work finishes before the deadline. If user step code exceeds the deadline, `shutdown` releases this worker's workflow, inbox, target-activation, and outbox leases and returns `:timeout`; the still-running fibers may later observe `LeaseConflict`, but they cannot commit stale output because state updates are lease-checked.

## Observability

`Durababble::Observability` is a thin OpenTelemetry integration used by the workflow engine, durable-object handles, worker/runtime loop, workflow RPC, gRPC transport, and higher-level store lifecycle transitions. It is disabled by default and only executes cheap no-op checks in that mode. When `Durababble.configure_observability(enabled: true, attributes:)` is called, Durababble uses the official OpenTelemetry API globals (`OpenTelemetry.tracer_provider` and `OpenTelemetry.meter_provider`) and leaves SDK/exporter/collector setup to the host application.

The instrumentation boundary is intentionally outside durable state semantics. Spans and metrics describe already-durable transitions; they do not decide leases, retries, wakeups, or command completion. Durababble does not wrap raw ActiveRecord SQL calls in its own spans or metrics; applications should enable standard ActiveRecord/database OpenTelemetry instrumentation for SQL visibility, while Durababble emits higher-level telemetry for workflows, steps, waits, leases, outbox rows, queues, workers, and RPCs.

Primary spans:

| Span name | Emitted by |
| --- | --- |
| `durababble.workflow.start`, `.resume`, `.execute`, `.step` | `Engine` / `WorkflowExecution` |
| `durababble.object.query`, `.command.enqueue`, `.command` | durable object handle |
| `durababble.workflow_rpc.*` | workflow RPC lease start, route, and handler paths |
| `durababble.rpc.client.*`, `durababble.rpc.server.*` | gRPC transport methods |

Primary metrics:

| Metric | Purpose |
| --- | --- |
| `durababble.workflow.starts/completions/failures/cancellations` | workflow lifecycle; cancellations are reserved until the cancel API lands |
| `durababble.workflow.step.attempts/successes/failures/retries` | step health and retry scheduling |
| `durababble.waits.started/completed`, `durababble.wait.latency` | wait persistence and wake latency |
| `durababble.queue.claim_latency` | workflow/outbox claim delay where creation time is available |
| `durababble.leases.heartbeats/conflicts/expired_recovery` | lease health and recovery |
| `durababble.outbox.pending/processed/failures` | outbox backlog and delivery result surface; explicit delivery failure is future work |
| `durababble.worker.ticks`, `durababble.worker.tick.duration` | worker loop health |
| `durababble.workflow.history.steps`, `durababble.workflow.replay.steps` | replay/history size and replay cost proxy |

## Benchmarking and query-shape validation

- `bench/run.rb` is a macro benchmark harness for the storage and coordination operations that dominate durable execution performance.
- The suite records environment metadata, per-operation latency percentiles, throughput, and allocation counts as JSON/CSV/Markdown.
- The history-specific profile `mise exec -- ruby bench/run.rb --profile history-smoke` measures replay/resume latency and allocations for small, medium, and intentionally large completed histories.
- The large-fixture benchmarks load historical workflow, step, wait-history, and outbox rows into the selected SQL backend before measuring queue claims, due workflow timers, and missed workflow-command wakeups against large tables.
- The benchmark operation set intentionally covers the main prototype lifecycle: enqueue, claim, heartbeat, lease conflict/recovery, worker tick/drain, end-to-end timer waits, resume-with-completed-steps, failed-workflow retry, observability reads, idempotency fences, outbox claim/ack/reclaim, durable-object command claim/complete/read, large-table query shapes, and cross-process command RPC.
- GitHub Actions runs the benchmark suite on demand and weekly, then stores timestamped benchmark reports as workflow artifacts for longitudinal comparison.
- Store migrations create queue/recovery indexes for workflow claims, expired leases, worker lease release, due workflow timers through `workflows.next_run_at`, outbox delivery scans, and durable-object/inbox command scans so the benchmark suite exercises production-intended query plans rather than relying on tiny-table behavior.
- `Durababble::StoreQueries` is the executable query registry for Store SQL. Hot store paths call registered query builders by id, and query-plan tests record the ids exercised by large-fixture operations, so adding production SQL requires plan coverage, benchmark coverage, backend conformance coverage, or an explicit uncovered-query list entry reviewed in the query-plan suite.

## Coverage gate

GitHub Actions runs `bundle exec rake test:coverage`, the same gate developers can run locally through `mise exec -- bundle exec rake test:coverage`. That task enables SimpleCov branch coverage, measures library files under `lib/**/*.rb`, and fails when global line coverage drops below 90%, global branch coverage drops below 85%, per-file line coverage drops below 59%, or per-file branch coverage drops below 41%. These ratchet thresholds are based on the current MySQL-backed CI suite, with a documented target of 95% line coverage and 90% branch coverage as meaningful tests improve the baseline. CI uploads the generated `coverage/` report so regressions can be diagnosed from the per-file HTML output and result JSON.

See [the spec](../../spec.md) for the guarantee and crash matrices implemented by tests.
