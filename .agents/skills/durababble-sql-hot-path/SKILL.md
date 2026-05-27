---
name: durababble-sql-hot-path
description: |
  Generate human-facing MySQL hot-path SQL reports for Durababble enqueueing, worker polling, and custom scenario files. Use when investigating store query order, SQL text, callsites, transaction context, or EXPLAIN plans for enqueue_workflow, claim_runnable_workflow, worker_poll_idle, worker_tick_claim, or a bespoke hot path.
---

# Durababble SQL Hot Path Reports

Use this skill when a task asks for MySQL query traces, hot path SQL, enqueueing SQL, worker polling SQL, or a report that includes StoreQueries descriptions, callsites, transaction context, and EXPLAIN output.

## Command

Run through `mise exec -- ...` from the repo root:

```sh
mise exec -- bundle exec ruby scripts/mysql-hot-path-report.rb --operation claim_runnable_workflow --format html
```

The script prints the generated report path, defaulting to `tmp/sql-hot-path-reports/<operation>.html`. Use `--format markdown` for Markdown output.

## Scenarios

List supported built-in and loaded scenarios:

```sh
mise exec -- bundle exec ruby scripts/mysql-hot-path-report.rb --list-scenarios
```

Useful defaults:

- `enqueue_workflow`: one durable workflow enqueue write.
- `claim_runnable_workflow`: direct store claim path with MySQL queue probes, selected-row lease update, and post-claim workflow read.
- `worker_poll_idle`: one `Worker#tick` with no runnable workflow work, useful for idle polling probes.
- `worker_tick_claim`: one `Worker#tick` that polls, claims, and executes the tiny report workflow to completion.

To trace a new hot path, load a Ruby scenario file:

```sh
mise exec -- bundle exec ruby scripts/mysql-hot-path-report.rb \
  --scenario-file test/support/hot_path_scenarios/my_scenario.rb \
  --scenario my_scenario \
  --format html
```

Scenario files register one or more blocks. The `setup:` block runs before tracing so fixture writes do not pollute the report; the main block is traced and becomes the report body.

```ruby
DurababbleMysqlHotPathReport.register_scenario(
  "my_scenario",
  description: "Trace the store operation I am optimizing.",
  setup: ->(context) { context.seed_pending_workflows(100) },
) do |context|
  context.store.claim_runnable_workflow(
    worker_id: "worker-a",
    lease_seconds: 60,
    workflow_names: ["my-workflow"],
  )
end
```

## Database

By default the script uses `DURABABBLE_DATABASE_URL`, or the local MySQL test URL built from `DURABABBLE_MYSQL_*`/`MYSQL_*` env vars, ending at `mysql://root@127.0.0.1:3306/sidekick_server_test`. Override with `--database-url URL`.

The default schema is workspace-derived and scenario-specific through `Durababble.workspace_schema(..., prefix: "durababble_hot_path")`. Override with `--schema NAME` only when you need a stable report namespace for comparison.

## Options

```sh
mise exec -- bundle exec ruby scripts/mysql-hot-path-report.rb \
  --operation worker_poll_idle \
  --format markdown \
  --output tmp/sql-hot-path-reports/worker-poll-idle.md
```

Use `--scenario NAME` as an alias for `--operation NAME`, `--scenario-file PATH` to load custom scenarios, `--fixture-size N` for built-ins or scenario helpers that read `context.fixture_size`, and `--keep-schema` when you want to inspect the generated MySQL tables after the report. The script resets its report schema before each run and drops it afterward unless `--keep-schema` is set.

## Reading The Report

Start with the Query Timeline table. Each query has the colocated `StoreQueries` description, the concrete registry id, Ruby-side query runtime, callsite, deterministic transaction label such as `tx1 depth=1`, SQL with placeholders, formatted bind params, and a traditional MySQL `EXPLAIN` plan table with access type, possible keys, chosen key, key length, refs, estimated rows, filtered percent, and Extra. The HTML report renders SQL with local syntax highlighting and hoverable table names that show `SHOW CREATE TABLE` DDL; the Markdown report includes those `CREATE TABLE` statements in a schema appendix. The report metadata also includes total captured query runtime for the operation.
