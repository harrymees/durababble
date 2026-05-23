# Implementation plan

## Completed prototype

- Ruby 4.0.5 pinned with mise.
- Gem scaffold with gemspec, executable, Rake, Minitest, README, and docs.
- Real SQL-backed persistence via `pg` for Yugabyte/PostgreSQL-compatible URLs and `trilogy` for MySQL/MariaDB URLs.
- Workflow DSL and synchronous engine API.
- Runnable workflow queue and worker polling.
- Distributed leases, heartbeats, stale lease stealing, and lease-aware resume.
- Transactional multi-row step and wait transitions.
- Durable step attempts, including waiting attempts that become completed after wake.
- Timer waits and external event waits with concurrent signal protection.
- Side-effect idempotency fences that acquire before side-effect execution.
- Durable outbox with unique keys, leasing, expiry recovery, and acknowledgement.
- CLI commands for migration, counter workflow execution, inspection, and resume.
- Integration tests against local Yugabyte/YSQL for the spec, guarantee matrix, crash matrix, concurrency cases, and subprocess crash recovery.
- SimpleCov line/branch coverage thresholds.

## Remaining production hardening beyond prototype

1. Package a long-lived worker daemon with signal handling and supervision hooks.
2. Add workflow versioning so changed workflow definitions can safely resume old runs.
3. Add richer attempt policies: exponential backoff, max attempts, retryable/non-retryable errors.
4. Add observability: structured logs, metrics, tracing, and run timelines.
5. Add more generic CLI workflow loading instead of only the built-in counter workflow.
6. Add a larger soak/stress suite for long-running multi-process workloads.
