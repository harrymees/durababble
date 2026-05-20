# Durababble architecture

Durababble is a Ruby 4 durable execution prototype. Ruby owns workflow definitions and execution; YugabyteDB/YSQL owns all durable coordination and recovery state.

## Components

- `Durababble::Workflow`: ordered step DSL. A step receives the previous context hash and returns the next context hash or a wait request.
- `Durababble::Engine`: creates/resumes runs, records step transitions, handles waits, and skips completed steps during recovery.
- `Durababble::Worker`: polls for one runnable workflow at a time and executes it under a lease.
- `Durababble::Store`: PostgreSQL/YSQL adapter using the `pg` gem. It owns schema migration and all durable state transitions.
- `exe/durababble`: prototype CLI for migrate/run/inspect/resume of the built-in counter workflow.

## Storage model

- `workflows`: one row per run; stores status, input, result, errors, and workflow lease owner/deadline.
- `steps`: one row per workflow step position; stores latest state and result used for resume.
- `step_attempts`: append-only attempt history for every started step.
- `waits`: durable timer and external-event waits, including context needed to resume.
- `fences`: idempotency fence results keyed by workflow and side-effect key.
- `outbox`: durable outgoing messages with unique keys, processing leases, and acknowledgements.

## Durability semantics

- Enqueue persists the workflow before work is claimed.
- Claiming work atomically marks one workflow `running` and writes `locked_by`/`locked_until`.
- A heartbeat extends an owned running lease.
- Expired leases are returned to `pending` for recovery.
- Before a step runs, its current step row and a new attempt row are persisted.
- After success, the current step and attempt are marked `completed` with JSON result.
- After failure, the current step, attempt, and workflow record the error.
- On resume, only `completed` steps are skipped; incomplete/running/failed/waiting work is retried or continued.
- Wait requests persist a `waits` row and put the workflow in `waiting` until timer wake or event signal completes the waiting step.
- Fences persist the first side-effect result and return the persisted result for repeated keys.
- Outbox rows are unique by key, leased for delivery, and acknowledged after external delivery.

See `docs/spec.md` for the guarantee and crash matrices implemented by tests.
