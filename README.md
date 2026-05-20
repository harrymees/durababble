# Durababble

Durababble is a prototype durable execution engine in Ruby 4. It stores workflow runs and step results in YugabyteDB through the PostgreSQL-compatible YSQL endpoint.

## Local setup

```sh
mise install
mise exec -- bundle install
export DURABABBLE_DATABASE_URL=postgresql://yugabyte@127.0.0.1:15433/yugabyte
mise exec -- bundle exec rake spec
```

The local Yugabyte instance found during scaffolding is `ybsqlite-vfs-yugabyte`, exposed as `127.0.0.1:15433 -> 5433`. A second Yugabyte container, `yugalite-test-yugabyte`, is reachable at `127.0.0.1:32770`.

## Prototype scope

Implemented now:

- Gem scaffold with Ruby 4.0.5 pinned by mise.
- Workflow DSL for ordered named steps.
- Yugabyte-backed schema migration.
- Durable workflow/step state persistence.
- Run and resume behavior that skips completed steps and retries failed/pending work.

Not yet implemented:

- Distributed leasing/heartbeats for multiple workers.
- Timers/sleeps and external event waits.
- Queue polling, worker supervision, or exactly-once side-effect fencing.
- Public CLI beyond the library entrypoint.
