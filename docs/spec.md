# Durababble specification

This document is the implemented prototype spec. Every item below is covered by explicit integration tests in `spec/durababble/complete_spec.rb` or `spec/durababble/cli_spec.rb` against real local Yugabyte/YSQL.

## Functional spec

- Ruby 4 gem scaffold managed by mise.
- Yugabyte-backed storage through the PostgreSQL wire protocol.
- Workflow DSL with ordered named steps.
- Durable workflow rows and durable step rows.
- Append-only step attempt history.
- Runnable workflow queue via `pending` rows.
- Worker polling via `Durababble::Worker#tick` and `#run_until_idle`.
- Distributed workflow leases with `locked_by` and `locked_until`.
- Heartbeat extension for active leases.
- Expired lease stealing for crashed workers.
- Resume semantics that skip completed steps and retry incomplete work.
- Timer waits via `Durababble.wait_until`.
- External event waits via `Durababble.wait_event` and `Store#signal_event`.
- Side-effect idempotency fences via `Store#with_fence`.
- Durable outbox with unique keys, leasing, and acknowledgement.
- Prototype CLI commands: `migrate`, `run-counter`, `inspect`, `resume-counter`, and `version`.

## Guarantee matrix

| Guarantee | Implementation | Explicit test |
| --- | --- | --- |
| Workflows are durable before execution | `Store#enqueue_workflow` inserts `pending` rows | complete spec guarantee + crash matrix |
| Runnable work is claimable by one worker at a time | `Store#claim_runnable_workflow` atomically updates one row to `running` with a lease | complete spec guarantee matrix |
| Active leases can be heartbeated | `Store#heartbeat` extends `locked_until` only for the owning worker | complete spec guarantee matrix |
| Expired leases can be recovered | `Store#steal_expired_leases!` returns expired `running` workflows to `pending` | complete spec guarantee + crash matrix |
| Completed steps are not re-executed on resume | `Engine#resume` reconstructs context from completed step rows and skips them | complete spec guarantee + crash matrix |
| Incomplete steps are retried | non-`completed` step rows are not skipped | complete spec crash matrix |
| Step attempts are append-only | `step_attempts` records every started attempt and terminal status | complete spec guarantee matrix |
| Timer waits survive process exit | `waits` rows store timer wake time and context | complete spec timer/event test |
| Event waits survive process exit | `waits` rows store event key and context | complete spec timer/event and crash matrix |
| Signaled waits resume with payload | `signal_event` completes the waiting step with context merged with payload | complete spec timer/event test |
| Side effects can be fenced by key | `with_fence` persists first result and returns it on repeats | complete spec guarantee matrix |
| Outbox delivery is durable and leased | `outbox` rows are unique by key, claimable once, and acknowledgeable | complete spec guarantee + crash matrix |
| CLI can operate the prototype | executable supports migrate/run/inspect/resume | cli spec |

## Crash matrix

| Crash point | Expected recovery | Explicit test |
| --- | --- | --- |
| After enqueue, before claim | Later engine/worker can run the pending workflow | complete spec crash matrix |
| After lease claim, before step start | Lease expiry returns workflow to pending; another worker completes it | complete spec crash matrix |
| After step start, before step completion | Step remains incomplete/running; recovery retries it | complete spec crash matrix |
| After step completion, before workflow completion | Completed step is skipped and not re-run; remaining steps continue | complete spec crash matrix |
| While waiting for an event | Wait row survives; signal wakes workflow and execution continues | complete spec crash matrix |
| After outbox insert, before delivery | Outbox message remains claimable exactly once at a time | complete spec crash matrix |

## Prototype boundaries

This is still a prototype, not a production Temporal replacement. It has explicit support for the guarantees above, but does not yet include workflow versioning, cron scheduling, a long-lived daemon supervisor, metrics, tracing, or production-grade advisory locking strategies.
