# Durababble specification

This document is the implemented prototype spec. Every item below is covered by explicit integration tests against real local Yugabyte/YSQL. The hardening suite includes multi-connection concurrency tests and a subprocess crash harness. The deterministic simulation suite maps each safety/guarantee row and crash row to one or more virtual scenarios and searches many deterministic seeds.

## Functional spec

- Ruby 4 gem scaffold managed by mise.
- Yugabyte-backed storage through the PostgreSQL wire protocol.
- Runtime values (`input`, `result`, `context`, `payload`, and heartbeat cursors) are serialized with Paquito into `bytea` columns, not stored as JSON/JSONB. `migrate!` can convert the earlier prototype's JSONB runtime columns into Paquito bytea columns.
- Workflow DSL with ordered named steps and step-level retry policies modeled after Temporal Activity retry options, Ruby-ified as `retry_policy:` keyword arguments.
- Durable workflow rows and durable step rows.
- Append-only step attempt history, including waits that transition to completed attempts.
- Runnable workflow queue via `pending` rows.
- Distributed workflow leases with `locked_by` and `locked_until`.
- Lease-aware `Engine#resume` that refuses to execute work owned by another worker.
- Heartbeat extension for active leases, including explicit step heartbeats with opaque cursor storage.
- Expired lease stealing for crashed workers.
- Resume semantics that skip completed steps and retry incomplete work.
- Timer waits via `Durababble.wait_until`.
- External event waits via `Durababble.wait_event` and `Store#signal_event`.
- Side-effect idempotency fences via `Store#with_fence`; the fence is acquired before the block executes so concurrent callers do not duplicate the side effect.
- Durable outbox with unique keys, leasing, expiry recovery, and acknowledgement.
- Lease-routed workflow RPC for node-to-node workflow messages: callers route through the current workflow lease holder, receivers re-check lease ownership before and after handling, stale holders reject in-flight RPCs, callers refresh/retry after lease-holder changes, no-active-owner races are handled internally by starting and awaiting a workflow lease before rerouting, and shutdown/non-running workflows are not retried.
- High-level worker lifecycle entrypoint via `Durababble::WorkerRuntime`, intended for app boot/shutdown integration. A runtime loops `Worker#tick` for one worker pool, stops taking new claims on shutdown, waits for in-flight work up to a timeout, and revokes its still-held workflow/outbox leases if the timeout is exceeded.
- Prototype CLI commands: `migrate`, `run-counter`, `inspect`, `resume-counter`, and `version`.
- SimpleCov line and branch coverage thresholds for the library.

## Guarantee matrix

| Guarantee | Implementation | Explicit test |
| --- | --- | --- |
| Workflows are durable before execution | `Store#enqueue_workflow` inserts `pending` rows with Paquito-serialized bytea input | complete spec guarantee + crash matrix |
| Runnable work is claimable by one worker at a time | `Store#claim_runnable_workflow` atomically updates one row and uses `FOR UPDATE SKIP LOCKED` | hardening concurrency spec |
| Resume honors lease ownership | `Engine#resume` uses `Store#claim_workflow` and raises `LeaseConflict` for another live owner | hardening lease spec |
| Active leases can be heartbeated | `Store#heartbeat` extends `locked_until` only for the owning worker | complete spec guarantee matrix |
| Running steps can explicitly heartbeat progress | Step handlers receive a `Durababble::Heartbeat`; `Heartbeat#record(cursor)` extends the workflow lease and stores an opaque Paquito-serialized cursor on the step/attempt | heartbeat spec + DST cursor recovery scenario |
| Heartbeat cursors survive recovery | `Engine#resume` passes the previous incomplete attempt's heartbeat cursor into the next invocation of the same step | heartbeat spec + DST cursor recovery scenario |
| Zombie workers cannot renew expired leases | `Store#heartbeat_step` updates `locked_until` and cursor only when `locked_by` still matches and `locked_until >= now()`; expired/moved leases raise `LeaseConflict` through the engine heartbeat object | heartbeat spec |
| Zombie workers cannot complete after lease revocation | `Engine` re-checks workflow lease ownership before recording step/wait/failure/workflow terminal state, so a timed-out shutdown that revokes leases makes a late owner raise `LeaseConflict` instead of committing stale output | worker lifecycle spec |
| Step retries are durably scheduled | A failing step with `retry_policy:` records a failed attempt, releases the workflow lease, stores `workflows.next_run_at`, and is not claimable again until the retry time is due | step retry spec + DST retry recovery scenario |
| Retry options are Temporal-like but Ruby-shaped | Supported options are `initial_interval:`, `backoff_coefficient:`, `maximum_interval:`, `maximum_attempts:`, `schedule:`, and `non_retryable_errors:` | retry policy specs |
| Final retry failure bubbles to the workflow | When attempts are exhausted, or the error class is non-retryable, the current step attempt is failed and the workflow row becomes `failed` with the step error | step retry spec |
| Expired leases can be recovered | `Store#steal_expired_leases!` returns expired `running` workflows to `pending` | complete spec guarantee + crash matrix |
| Completed steps are not re-executed on resume | `Engine#resume` reconstructs context from completed step rows and skips them | complete spec guarantee + subprocess crash harness |
| Incomplete steps are retried | non-`completed` step rows are not skipped | complete spec crash matrix |
| Step attempts are append-only | `step_attempts` records every started attempt and terminal status | complete spec guarantee matrix |
| Waiting attempts complete when signaled | wait completion updates attempts from `waiting` to `completed` | hardening wait-attempt spec |
| Timer waits survive process exit | `waits` rows store timer wake time and context | complete spec timer/event test |
| Event waits survive process exit | `waits` rows store event key and context | complete spec timer/event and crash matrix |
| Signaled waits resume with payload | `signal_event` completes the waiting step with context merged with payload | complete spec timer/event test |
| Concurrent signalers wake a wait once | `signal_event` completes pending waits via one locked update | hardening event concurrency spec |
| Side effects can be fenced by key | `with_fence` inserts a running fence before yield; other callers wait for result | hardening fence concurrency spec |
| Outbox delivery is durable and leased | `outbox` rows are unique by key, claimable once, acknowledgeable, and reclaimable after lease expiry | complete + hardening outbox specs |
| Workflow RPCs are routed to the current lease holder | `WorkflowRpc::Router` looks up `Store#current_workflow_lease`; `WorkflowRpc::Handler` validates ownership before and after handling; stale in-flight RPCs are rejected and optionally retried after a fresh lookup | workflow RPC spec + DST lease-change/shutdown scenarios |
| Multi-row state transitions are transactional | step start/finish/failure and wait record transitions run in DB transactions | implementation + regression suite |
| Runtime values are not stored as JSONB | `Store` encodes values through Paquito and stores them in bytea columns | store spec Paquito storage + legacy migration specs |
| CLI can operate the prototype | executable supports migrate/run/inspect/resume | cli spec |

## Crash matrix

| Crash point | Expected recovery | Explicit test |
| --- | --- | --- |
| After enqueue, before claim | Later engine/worker can run the pending workflow | complete spec crash matrix |
| After lease claim, before step start | Lease expiry returns workflow to pending; another worker completes it | complete spec crash matrix |
| After step start, before step completion | Step remains incomplete/running; recovery retries it | complete spec crash matrix |
| After a step heartbeat, before step completion | The latest heartbeat cursor is available to the next invocation after lease expiry/recovery | heartbeat spec + DST cursor recovery scenario |
| After step failure, before retry due time | The retry schedule is persisted in the workflow row; a restarted worker cannot claim the workflow early, but a later worker completes it once due | step retry spec + DST retry recovery scenario |
| After step completion, before workflow completion | Completed step is skipped and not re-run; remaining steps continue | complete spec crash matrix + subprocess crash harness |
| While waiting for an event | Wait row survives; signal wakes workflow and execution continues | complete spec crash matrix |
| After outbox insert, before delivery | Outbox message remains claimable exactly once at a time | complete spec crash matrix |
| After outbox claim, before ack | Expired outbox lease can be reclaimed by another sender | hardening outbox recovery spec |
| During lease-routed workflow RPC | Receiver rejects RPC if the workflow lease moved, expired, or workflow shut down after caller lookup; caller refreshes after stale rejection; if no owner exists yet, the router starts and awaits a new workflow lease before rerouting the RPC opaquely to the caller | workflow RPC spec + DST lease-change/shutdown/no-active-owner scenarios |
| During app shutdown with an in-flight step | `WorkerRuntime#shutdown` stops new claims and waits for the step to finish; if the timeout expires, it revokes this worker's leases so the step remains incomplete and a later worker retries it | worker lifecycle spec |

## Worker polling and recovery details

Worker polling is intentionally not a core introductory concept for the prototype. A worker pool is just a set of processes repeatedly calling `Durababble::Worker#tick` or `#run_until_idle`; each tick claims at most one runnable workflow under the lease rules above, then resumes it through `Engine#resume`. RPC routing does not push this retry burden to callers: `WorkflowRpc::Router` can coordinate a no-active-owner race by using `WorkflowRpc::LeaseStarter` to claim/start the workflow, await an active lease, and reroute the original RPC. Expired leases are either reclaimed directly by `claim_runnable_workflow`/`claim_workflow` or moved back to `pending` by `Store#steal_expired_leases!`, so no special worker-pool coordinator is required in this prototype.

For application integration, prefer `Durababble::WorkerRuntime` over hand-written polling loops. A Rails initializer can create one runtime per desired pool during boot, keep the returned object, and call `shutdown(timeout: ...)` from the process shutdown hook. `shutdown` first asks the loop to stop, which means no new workflows are claimed. If the active tick finishes before the timeout, shutdown returns `:stopped`. If user step code is still running after the timeout, shutdown returns `:timeout` after `Store#release_worker_leases!` marks this worker's running workflows/outbox messages claimable again. Late/zombie state writes are guarded by lease ownership checks in `Engine`, so released work remains incomplete and is retried by a later worker instead of being committed by the timed-out process.

## Step retry policy details

Steps are retriable at the step/activity boundary rather than by blindly rerunning whole workflows. The DSL is intentionally close to Temporal's Activity Retry Policy, but shaped as Ruby keyword arguments:

```ruby
Durababble::Workflow.define("import") do
  step "download",
       retry_policy: {
         initial_interval: 1,
         backoff_coefficient: 2.0,
         maximum_interval: 100,
         maximum_attempts: 5,
         non_retryable_errors: [ArgumentError]
       } do |ctx, heartbeat|
    # ...
  end
end
```

`schedule: [1, 5, 30]` may be supplied for an explicit per-retry schedule; after the explicit array is exhausted, Durababble falls back to capped exponential backoff. Intervals are numeric seconds. `maximum_attempts:` counts the first execution plus retries. `non_retryable_errors:` accepts Ruby exception classes or class-name strings.

On a retryable failure, `Engine` records the current step attempt as failed, sets the workflow back to `pending`, clears `locked_by`/`locked_until`, and stores `next_run_at`. `claim_runnable_workflow` ignores pending/failed workflows whose `next_run_at` is still in the future, so retry delay survives process restarts and lease churn. On the final failure, or for a non-retryable error, the workflow itself is marked `failed` and the error bubbles to the durable object state.

Temporal comparison: Temporal Activities use exponential retry policies by default with `initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`, and `non_retryable_errors`; Workflow Executions do not retry by default. Durababble mirrors the Activity-style option names for steps, while leaving workflow-level retry as an explicit future concern.

Cloudflare Durable Objects comparison: Durable Objects provide alarms (`setAlarm`/`getAlarm`/`alarm`) as a storage-backed wakeup mechanism. Durababble's `next_run_at` serves the same durable-wakeup role for step retries, but it is integrated into workflow queue claiming rather than exposed as a separate alarm handler.

## Coverage standard

The suite runs with SimpleCov branch coverage enabled and currently verifies at least:

- line coverage: 85% minimum
- branch coverage: 60% minimum

The latest verification produced substantially higher coverage than the minimums.

## Prototype boundaries

This is still a prototype, not a production Temporal replacement. It has explicit support for the guarantees above, but does not yet include workflow versioning, cron scheduling, a long-lived daemon supervisor, metrics, tracing, or production-grade operational tooling.

Additional explicit prototype boundaries found during faithfulness review:

- Fence owner crash recovery is not specified or implemented. `Store#with_fence` prevents concurrent duplicate side effects while the owner completes or fails normally, but a process crash after fence acquisition and before fence completion leaves a `running` fence that waiters eventually time out on. Retrying/taking over expired fences is future work.
- Worker registry misses are avoided for normal worker polling by claiming only workflow names present in the supplied registry. Enqueuing a workflow name with no corresponding worker pool will leave it pending until an appropriate pool is started.
- Long-running steps do not heartbeat automatically while user code runs, but they can explicitly call the heartbeat object passed as the second step argument. Callers should either choose a `lease_seconds` covering expected step duration or call `heartbeat.record(cursor)` before the lease deadline.
- CLI coverage is happy-path oriented and not a complete UX/error-contract specification.
