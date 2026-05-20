# Durababble

Durababble is a prototype durable execution engine in Ruby 4. It stores workflow runs, step state, leases, waits, attempts, fences, and outbox messages in YugabyteDB through the PostgreSQL-compatible YSQL endpoint.

## Local setup

```sh
mise install
mise exec -- bundle install
export DURABABBLE_DATABASE_URL=postgresql://yugabyte@127.0.0.1:15433/yugabyte
mise exec -- bundle exec rake spec
```

The local Yugabyte instance used by default is `ybsqlite-vfs-yugabyte`, exposed as `127.0.0.1:15433 -> 5433`. A second Yugabyte container, `yugalite-test-yugabyte`, is reachable at `127.0.0.1:32770`.

## Implemented prototype scope

- Gem scaffold with Ruby 4.0.5 pinned by mise.
- Workflow DSL for ordered named steps.
- Yugabyte-backed schema migration.
- Durable workflow, step, wait, attempt, fence, and outbox persistence.
- Worker polling with one-at-a-time leased claims.
- Heartbeats and stale lease recovery.
- Run and resume behavior that skips completed steps and retries incomplete work.
- Timer waits and external event waits.
- Side-effect idempotency fences.
- Durable outbox with unique keys, leasing, and acknowledgement.
- CLI commands: `migrate`, `run-counter`, `inspect`, `resume-counter`, `version`.

See `docs/spec.md` for the implemented spec, guarantee matrix, and crash matrix.

## CLI

```sh
exe/durababble migrate --schema durababble
exe/durababble run-counter --schema durababble --count 2
exe/durababble inspect RUN_ID --schema durababble
exe/durababble resume-counter RUN_ID --schema durababble
```

## Example library use

```ruby
workflow = Durababble::Workflow.define("counter") do
  step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
  step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
end

store = Durababble::Store.connect(database_url: ENV.fetch("DURABABBLE_DATABASE_URL"), schema: "durababble")
run = Durababble::Engine.new(store:).run(workflow, input: { "count" => 2 })
```

## Prototype boundaries

This is still a prototype, not a production Temporal replacement. It implements the guarantee and crash matrices in `docs/spec.md`, but does not yet include workflow versioning, cron scheduling, metrics, tracing, a packaged daemon supervisor, or production hardening around operational observability.
