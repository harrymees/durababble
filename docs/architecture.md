# Durababble architecture

Durababble is a Ruby 4 durable execution prototype. Ruby owns workflow definitions and execution; YugabyteDB/YSQL owns durable coordination and recovery state.

## Components

- `Durababble::Workflow`: ordered step DSL. A step receives the previous context hash and returns the next context hash or a wait request.
- `Durababble::Engine`: creates/resumes runs, enforces workflow lease ownership, records step transitions, handles waits, and skips completed steps during recovery.
- `Durababble::Worker`: polls for one runnable workflow at a time and executes it under a lease.
- `Durababble::Store`: PostgreSQL/YSQL adapter using the `pg` gem. It owns schema migration and all durable state transitions. Runtime Ruby values are serialized through Paquito and stored in `bytea` columns.
- `exe/durababble`: prototype CLI for migrate/run/inspect/resume of the built-in counter workflow.

## Storage model

- `workflows`: one row per run; stores status, input, result, errors, and workflow lease owner/deadline.
- `steps`: one row per workflow step position; stores latest state and result used for resume.
- `step_attempts`: append-only attempt history for every started step.
- `waits`: durable timer and external-event waits, including context needed to resume.
- `fences`: idempotency fence state. A row is inserted as `running` before the side effect block executes; waiters read the completed result instead of running the block.
- `outbox`: durable outgoing messages with unique keys, processing leases, expiry recovery, and acknowledgements.

## Durability semantics

- Enqueue persists the workflow before work is claimed.
- Claiming work atomically marks one workflow `running` and writes `locked_by`/`locked_until` using locked queue selection.
- A heartbeat extends an owned running lease.
- Expired leases are returned to `pending` for recovery.
- `Engine#resume` refuses to execute a workflow leased by another live worker.
- Before a step runs, its current step row and a new attempt row are persisted transactionally.
- After success/failure/wait, the related step and attempt rows are updated transactionally.
- After success, the current step and attempt are marked `completed` with a Paquito-serialized bytea result.
- After failure, the current step, attempt, and workflow record the error.
- On resume, only `completed` steps are skipped; incomplete/running/failed/waiting work is retried or continued.
- Wait requests persist a `waits` row and put the workflow in `waiting` until timer wake or event signal completes the waiting step.
- Event/timer completion uses a locked update so concurrent signalers wake a wait once.
- Fences persist a running row before side effects and persist the first completed result for all repeated callers.
- Outbox rows are unique by key, leased for delivery, reclaimable after lease expiry, and acknowledged after external delivery.

## Benchmarking and query-shape validation

- `bench/run.rb` is a macro benchmark harness for the storage and coordination operations that dominate durable execution performance.
- The suite records environment metadata, per-operation latency percentiles, throughput, and allocation counts as JSON/CSV/Markdown.
- The large-fixture benchmarks load historical workflow, step, wait, and outbox rows into Yugabyte before measuring queue claims and due-timer scans against large tables.
- GitHub Actions runs the benchmark suite on demand and weekly, then stores timestamped benchmark reports as workflow artifacts for longitudinal comparison.
- Store migrations create queue/recovery indexes for workflow claims, expired leases, pending event waits, due timers, and outbox delivery scans so the benchmark suite exercises production-intended query plans rather than relying on tiny-table behavior.

See `docs/spec.md` for the guarantee and crash matrices implemented by tests.
