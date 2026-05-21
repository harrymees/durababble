# Durababble specification

This document is the implemented prototype spec. Every item below is covered by explicit integration tests against real local Yugabyte/YSQL. The hardening suite includes multi-connection concurrency tests and a subprocess crash harness.

## Functional spec

- Ruby 4 gem scaffold managed by mise.
- Yugabyte-backed storage through the PostgreSQL wire protocol.
- Workflow DSL with ordered named steps.
- Durable workflow rows and durable step rows.
- Append-only step attempt history, including waits that transition to completed attempts.
- Runnable workflow queue via `pending` rows.
- Worker polling via `Durababble::Worker#tick` and `#run_until_idle`.
- Distributed workflow leases with `locked_by` and `locked_until`.
- Lease-aware `Engine#resume` that refuses to execute work owned by another worker.
- Heartbeat extension for active leases.
- Expired lease stealing for crashed workers.
- Resume semantics that skip completed steps and retry incomplete work.
- Timer waits via `Durababble.wait_until`.
- External event waits via `Durababble.wait_event` and `Store#signal_event`.
- Side-effect idempotency fences via `Store#with_fence`; the fence is acquired before the block executes so concurrent callers do not duplicate the side effect.
- Durable outbox with unique keys, leasing, expiry recovery, and acknowledgement.
- Prototype CLI commands: `migrate`, `run-counter`, `inspect`, `resume-counter`, and `version`.
- SimpleCov line and branch coverage thresholds for the library.

## Guarantee matrix

| Guarantee | Implementation | Explicit test |
| --- | --- | --- |
| Workflows are durable before execution | `Store#enqueue_workflow` inserts `pending` rows | complete spec guarantee + crash matrix |
| Runnable work is claimable by one worker at a time | `Store#claim_runnable_workflow` atomically updates one row and uses `FOR UPDATE SKIP LOCKED` | hardening concurrency spec |
| Resume honors lease ownership | `Engine#resume` uses `Store#claim_workflow` and raises `LeaseConflict` for another live owner | hardening lease spec |
| Active leases can be heartbeated | `Store#heartbeat` extends `locked_until` only for the owning worker | complete spec guarantee matrix |
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
| Multi-row state transitions are transactional | step start/finish/failure and wait record transitions run in DB transactions | implementation + regression suite |
| CLI can operate the prototype | executable supports migrate/run/inspect/resume | cli spec |

## Crash matrix

| Crash point | Expected recovery | Explicit test |
| --- | --- | --- |
| After enqueue, before claim | Later engine/worker can run the pending workflow | complete spec crash matrix |
| After lease claim, before step start | Lease expiry returns workflow to pending; another worker completes it | complete spec crash matrix |
| After step start, before step completion | Step remains incomplete/running; recovery retries it | complete spec crash matrix |
| After step completion, before workflow completion | Completed step is skipped and not re-run; remaining steps continue | complete spec crash matrix + subprocess crash harness |
| While waiting for an event | Wait row survives; signal wakes workflow and execution continues | complete spec crash matrix |
| After outbox insert, before delivery | Outbox message remains claimable exactly once at a time | complete spec crash matrix |
| After outbox claim, before ack | Expired outbox lease can be reclaimed by another sender | hardening outbox recovery spec |

## Coverage standard

The suite runs with SimpleCov branch coverage enabled and currently verifies at least:

- line coverage: 85% minimum
- branch coverage: 60% minimum

The latest verification produced substantially higher coverage than the minimums.

## Prototype boundaries

This is still a prototype, not a production Temporal replacement. It has explicit support for the guarantees above, but does not yet include workflow versioning, cron scheduling, a long-lived daemon supervisor, metrics, tracing, or production-grade operational tooling.
