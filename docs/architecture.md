# Durababble architecture

Durababble is intentionally small: Ruby code owns workflow definition and execution, while YugabyteDB owns durable run state.

## Components

- `Durababble::Workflow`: ordered step DSL. A step receives the previous context hash and returns the next context hash.
- `Durababble::Engine`: creates runs, resumes failed/interrupted runs, skips completed steps, and persists transitions.
- `Durababble::Store`: PostgreSQL/YSQL adapter using the `pg` gem. It creates and updates durable tables in a configurable schema.

## Storage model

`workflows` stores one row per run: id, name, status, input, result, error, timestamps.

`steps` stores one row per step attempt slot: workflow_id, position, name, status, result, error, timestamps. Completed step results are reused on resume, so a completed step is not re-executed by the engine.

## Current durability semantics

- Before a step runs, its row is upserted as `running`.
- After success, the row is marked `completed` with JSON result.
- After failure, the row and workflow are marked `failed` with the exception string.
- `resume` marks the workflow `running`, rebuilds context from completed step outputs, and continues at the first non-completed step.

## Future hardening

- Worker leases and heartbeats for multi-process execution.
- Attempt history instead of one row per step slot.
- Transaction boundaries around state transitions plus external side-effect fencing.
- Timers, sleeps, signals, external event waits, and queue polling.
