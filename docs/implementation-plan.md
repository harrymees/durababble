# Implementation plan

## Done in this prototype

- Ruby 4.0.5 pinned with mise.
- Gem scaffold with gemspec, executable stub, Rake, RSpec, and README.
- Real Yugabyte-backed persistence via `pg`.
- Integration tests against local Yugabyte YSQL.
- Basic run/resume behavior.

## Next increments

1. Add leases: `locked_by`, `locked_until`, heartbeat extension, and stale-lock stealing.
2. Add attempt history: append-only `step_attempts` table to preserve failure history.
3. Add a worker loop: poll runnable workflows and execute with leases.
4. Add timers/events: persist sleeps and external wait conditions.
5. Add CLI commands: migrate, run example workflow, inspect run, resume run.
6. Add side-effect fencing API: idempotency keys and outbox rows.
