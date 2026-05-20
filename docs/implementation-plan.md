# Implementation plan

## Completed prototype

- Ruby 4.0.5 pinned with mise.
- Gem scaffold with gemspec, executable, Rake, RSpec, README, and docs.
- Real Yugabyte-backed persistence via `pg`.
- Workflow DSL and synchronous engine API.
- Runnable workflow queue and worker polling.
- Distributed leases, heartbeats, and stale lease stealing.
- Durable step attempts.
- Timer waits and external event waits.
- Side-effect idempotency fences.
- Durable outbox with unique keys, leasing, and acknowledgement.
- CLI commands for migration, counter workflow execution, inspection, and resume.
- Integration tests against local Yugabyte/YSQL for the spec, guarantee matrix, and crash matrix.

## Remaining production hardening beyond prototype

1. Package a long-lived worker daemon with signal handling and supervision hooks.
2. Add workflow versioning so changed workflow definitions can safely resume old runs.
3. Add richer attempt policies: exponential backoff, max attempts, retryable/non-retryable errors.
4. Add observability: structured logs, metrics, tracing, and run timelines.
5. Add advisory-lock or serializable-transaction hardening for high-concurrency production use.
6. Add more generic CLI workflow loading instead of only the built-in counter workflow.
