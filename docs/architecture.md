# Durababble architecture

Durababble is a Ruby 4 durable execution prototype. Ruby owns workflow and durable-object definitions; a SQL store owns durable coordination and recovery state. The primary tested stores are YugabyteDB/YSQL through the PostgreSQL wire protocol and MySQL/MariaDB through Trilogy.

## Components

- `Durababble::Workflow`: class-oriented workflow base. A subclass implements `#execute(input)` for deterministic orchestration and marks side-effect boundaries with `step def ...` or `step retry: ...` followed by `def ...`. Steps are called as ordinary methods on `self`; the engine assigns durable positions by deterministic execution order.
- `Durababble::DurableObject`: class-oriented durable object base. A subclass is addressed by `Class.ref(id, store:)`, exposes public read methods with `expose`, exposes public mutating commands with `expose_command`, and mutates state explicitly with `update_state(new_state)`. Durable object methods are not workflow steps.
- `Durababble::RetryPolicy`: normalizes retry options (`initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`, explicit `schedule`, and `non_retryable_errors`) and computes durable retry delays for workflow steps and durable-object commands.
- `Durababble::Engine`: creates/resumes workflow runs, enforces workflow lease ownership, records workflow step transitions, handles explicit step heartbeats, handles retryable step failures, handles waits, and skips completed steps during recovery.
- `Durababble::Worker`: polls for one runnable workflow at a time and executes it under a lease. The worker registry contains workflow classes.
- `Durababble::WorkerRuntime`: high-level app/process entrypoint for a named worker pool. It starts a background polling loop, stops taking new work on shutdown, waits for in-flight work up to a timeout, and releases this worker's leases if the timeout expires.
- `Durababble::WorkflowRpc`: routes node-to-node workflow RPCs through the current workflow lease holder and rejects stale in-flight messages when ownership changes or the workflow stops running. This is the lower-level routing primitive; public `Workflow.ref(...).expose_command` currently records durable command events rather than executing through this router.
- `Durababble::Rpc::Server` / `Durababble::Rpc::Client`: protobuf/gRPC transport for cross-node wakeups, evictions, transient calls, and durable-message wakeups. Workflow transient calls use `Durababble::Rpc::WorkflowClient` to bridge `WorkflowRpc::Router` onto the `CallTransient` gRPC method.
- `Durababble::Store`: backend-selecting durable store facade. `postgresql://`/`postgres://` URLs use the PostgreSQL/YSQL adapter with the `pg` gem; `mysql://`/`mysql2://`/`trilogy://` URLs use the MySQL/MariaDB adapter with the `trilogy` gem. It owns schema migration and all durable state transitions. Runtime Ruby values are serialized through Paquito and stored in binary columns (`bytea` on PostgreSQL/YSQL, `LONGBLOB` on MySQL/MariaDB). If callers do not pass `schema:`, the default namespace comes from `DURABABBLE_SCHEMA` or from deterministic `Durababble.workspace_schema(DURABABBLE_WORKSPACE_ROOT || Dir.pwd)`; PostgreSQL/YSQL uses that namespace as a schema, while MySQL/MariaDB uses it as the durable table prefix inside the configured database.
- `sig/durababble.rbs`: static-only RBS declarations for the public class API. Runtime execution does not load or validate RBS.

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

When `#execute` calls a step method, the wrapper delegates to `WorkflowExecution#call_step`. The execution object:

1. assigns the next step position;
2. returns a persisted result immediately if that position already completed and the recorded step name matches the current method;
3. records the step start and attempt;
4. builds `step_context` with a generated idempotency key and heartbeat;
5. invokes the original Ruby method body;
6. persists success, wait, retryable failure, or final failure.

This means step identity is based on deterministic call order. The method name is recorded as metadata, but callers do not pass step names at call sites.
If replay reaches a completed position with a different current method name, or if workflow execution returns before all completed positions have been consumed, the engine fails the run with `Durababble::NonDeterminismError` so code changes cannot silently reuse stale results or drop a recorded durable suffix.

Workflow `expose` and `expose_command` define the public ref surface:

```ruby
workflow = CounterWorkflow.ref(run_id, store:)
workflow.description
workflow.cancel(reason: "user request")
```

In the current prototype, exposed workflow queries execute against a lightweight ref instance. Exposed workflow commands persist command events using `Store#signal_event`. A full command executor that routes to the current lease owner, executes the method body, and returns command results is future work.

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

The desired durable-object contract is actor-like: commands for the same `(object_type, object_id)` serialize through that identity, each command receives `command_context`, and retries/recovery are recorded in `durable_object_commands`. The current prototype has the class/ref API, command rows, inline command execution, generated command idempotency keys, and explicit state persistence; the dedicated object-command worker/lease path is still to be hardened.

## Storage model

- `workflows`: one row per run; stores status, input, result, errors, workflow lease owner/deadline, `next_run_at` for durably scheduled step retries, and first cooperative cancellation request metadata (`cancel_reason`, `cancel_requested_at`, `cancel_delivered_at`).
- `steps`: one row per workflow step position; stores latest state, result used for resume, and latest opaque heartbeat cursor for incomplete-step recovery.
- `step_attempts`: append-only attempt history for every started step, including the latest heartbeat cursor observed for each attempt.
- `waits`: durable timer and external-event waits, including context needed to resume.
- `event_keys`: per-event-key lock rows that serialize event emission with matching wait creation, so a signal and a wait cannot miss each other when they race.
- `event_signals`: cached event emissions created when `Store#signal_event` runs before a matching wait exists. Rows store the Paquito payload plus pending/delivered status and the wait id that consumed them.
- `fences`: idempotency fence state. A row is inserted as `running` before the side effect block executes; waiters read the completed result instead of running the block.
- `outbox`: durable outgoing messages with unique keys, processing leases, expiry recovery, and acknowledgements.
- `durable_objects`: latest durable-object state by `(object_type, object_id)`.
- `durable_object_commands`: persisted object command calls, arguments, result/error, status, and command lease columns.

## Durability semantics

- Enqueue persists the workflow before work is claimed.
- Claiming work atomically marks one workflow `running` and writes `locked_by`/`locked_until` using locked queue selection.
- A workflow heartbeat extends an owned running lease.
- A step heartbeat (`step_context.heartbeat.record(cursor)`) compare-and-swaps against the current workflow lease owner/deadline, extends `locked_until`, and stores an opaque Paquito-serialized cursor on the current step/attempt. If the workflow lease expired or moved, the heartbeat raises `LeaseConflict` instead of reviving a zombie owner.
- Expired leases are returned to `pending` for recovery.
- `Engine#resume` refuses to execute a workflow leased by another live worker.
- Before a step runs, its current step row and a new attempt row are persisted transactionally.
- After success/failure/wait, the related step and attempt rows are updated transactionally.
- Before recording success/failure/wait or final workflow completion, the engine confirms the workflow lease is still owned by the current worker. This prevents a timed-out worker whose lease was explicitly released during process shutdown from committing stale output after another process has been allowed to retry.
- After success, the current step and attempt are marked `completed` with a Paquito-serialized bytea result.
- After retryable step failure, the current step/attempt record the error, the workflow lease is cleared, and `next_run_at` delays the next claim. After attempts are exhausted, or the error class is non-retryable, the workflow records the final error and becomes `failed`.
- Terminal `failed` workflows clear `next_run_at` and are not returned by claim paths. Only failed rows with a non-null due `next_run_at` are treated as retryable queue work.
- On resume, only `completed` steps are skipped; incomplete/running/failed/waiting work is retried or continued. For a retried step, `step_context.heartbeat.cursor` exposes the latest cursor from the previous incomplete invocation.
- Wait requests persist a `waits` row and put the workflow in `waiting` until timer wake or event signal completes the waiting step.
- Event signals still fan out to all already-pending waits for the matching key. If no matching wait is pending, the signal is persisted in `event_signals`; the first later wait for the key consumes the oldest cached row, completes its step with the cached payload, and leaves the workflow `pending` for the next worker tick.
- Event/timer completion uses locked updates, and event signal/wait races additionally lock the `event_keys` row for that key, so concurrent signalers wake a wait once and cached emissions are delivered once.
- Workflow cancellation is cooperative, not termination. `Workflow.handle(id).cancel(reason:)` records durable cancellation metadata on the workflow row. Pending, waiting, and retry-backoff workflows move to `canceling` and become claimable immediately; running workflows keep their current lease and observe the request at the next step boundary, completed-step replay boundary, or step heartbeat. The engine raises `Durababble::CancellationError` into workflow code. Once raised, cleanup steps run as ordinary durable steps; completed cleanup steps are skipped after crash/recovery. If cleanup returns or re-raises the cancellation error, the workflow becomes `canceled`. If cleanup raises an unrelated non-retryable error, the workflow becomes `failed`; retryable cleanup failures keep `next_run_at` and retry under `canceling`.
- First-class child workflows are still future scope. Cancellation semantics for that future surface must require an explicit durable child-cancellation policy; cooperative parent cancellation must not silently terminate child work or claim that child cleanup completed.
- Operator termination remains a separate hard-stop concept: it may mark or remove work without running user cleanup and must not report the workflow as cooperatively `canceled`.
- Fences persist a running row before side effects and persist the first completed result for all repeated callers.
- Outbox rows are unique by key, leased for delivery, reclaimable after lease expiry, and acknowledged after external delivery.
- Workflow RPC routing is lease-validated at both ends: callers look up the current active lease holder, receivers reject messages unless they still own the workflow before and after handler execution, and callers retry stale ownership, no-active-owner, and transport-unavailable failures only after a fresh owner lookup. If the fresh lookup finds no active owner for a recoverable workflow, `WorkflowRpc::Router` starts and awaits a new lease through `WorkflowRpc::LeaseStarter`, then reroutes the original RPC opaquely to the caller; terminal/shutdown states are still surfaced as non-routable.
- Cross-node workflow RPC uses the same lease validation over gRPC: `Rpc::Server#CallTransient` returns protobuf `LeaseMoved`, `not_running`, or `RemoteError` responses, and `Rpc::WorkflowClient` decodes those into the typed `WorkflowRpc` errors the router already understands.

## Application worker lifecycle

`WorkerRuntime` is the intended entrypoint for embedding Durababble in a Rails or similar long-lived app process:

```ruby
WORKER = Durababble::WorkerRuntime.start(
  database_url: ENV.fetch("DATABASE_URL"),
  workflows: MyApp::DurableWorkflows.for_pool("default"),
  worker_pool: "default",
  worker_id: "#{Socket.gethostname}-#{Process.pid}"
)

at_exit { WORKER.shutdown(timeout: 10) }
```

The runtime only claims workflow names present in its `workflows` registry, so separate pools can run different workflow families without claiming work they cannot execute. Shutdown is cooperative: the loop stops after the current tick and returns `:stopped` if the tick completes before the deadline. If user step code exceeds the deadline, `shutdown` releases this worker's workflow and outbox leases and returns `:timeout`; the still-running thread may later observe `LeaseConflict`, but it cannot commit stale step output because state updates are lease-checked.

## Benchmarking and query-shape validation

- `bench/run.rb` is a macro benchmark harness for the storage and coordination operations that dominate durable execution performance.
- The suite records environment metadata, per-operation latency percentiles, throughput, and allocation counts as JSON/CSV/Markdown.
- The large-fixture benchmarks load historical workflow, step, wait, and outbox rows into the selected SQL backend before measuring queue claims, due-timer scans, and event-signal misses against large tables.
- The benchmark operation set intentionally covers the main prototype lifecycle: enqueue, claim, heartbeat, lease conflict/recovery, worker tick/drain, end-to-end event and timer waits, resume-with-completed-steps, failed-workflow retry, observability reads, idempotency fences, outbox claim/ack/reclaim, large-table query shapes, and cross-process command RPC.
- GitHub Actions runs the benchmark suite on demand and weekly, then stores timestamped benchmark reports as workflow artifacts for longitudinal comparison.
- Store migrations create queue/recovery indexes for workflow claims, expired leases, pending event waits, due timers, and outbox delivery scans so the benchmark suite exercises production-intended query plans rather than relying on tiny-table behavior.

## Coverage gate

GitHub Actions runs `bundle exec rake test:coverage`, the same gate developers can run locally through `mise exec -- bundle exec rake test:coverage`. That task enables SimpleCov branch coverage, measures library files under `lib/**/*.rb`, and fails when global line coverage drops below 90%, global branch coverage drops below 85%, per-file line coverage drops below 59%, or per-file branch coverage drops below 41%. These ratchet thresholds are based on the current MySQL-backed CI suite, with a documented target of 95% line coverage and 90% branch coverage as meaningful tests improve the baseline. CI uploads the generated `coverage/` report so regressions can be diagnosed from the per-file HTML output and result JSON.

See `docs/spec.md` for the guarantee and crash matrices implemented by tests.
