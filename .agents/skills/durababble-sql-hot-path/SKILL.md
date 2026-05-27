---
name: durababble-sql-hot-path
description: |
  Generate human-facing MySQL hot-path SQL reports for Durababble enqueueing and worker polling. Use when investigating store query order, SQL text, callsites, transaction context, or EXPLAIN plans for enqueue_workflow, claim_runnable_workflow, worker_poll_idle, or worker_tick_claim.
---

# Durababble SQL Hot Path Reports

Use this skill when a task asks for MySQL query traces, hot path SQL, enqueueing SQL, worker polling SQL, or a report that includes StoreQueries descriptions, callsites, transaction context, and EXPLAIN output.

## Command

Run through `mise exec -- ...` from the repo root:

```sh
mise exec -- bundle exec ruby scripts/mysql-hot-path-report.rb --operation claim_runnable_workflow --format html
```

The script prints the generated report path, defaulting to `tmp/sql-hot-path-reports/<operation>.html`. Use `--format markdown` for Markdown output.

## Operations

List supported operations:

```sh
mise exec -- bundle exec ruby scripts/mysql-hot-path-report.rb --list-operations
```

Useful defaults:

- `enqueue_workflow`: one durable workflow enqueue write.
- `claim_runnable_workflow`: direct store claim path with MySQL queue probes, selected-row lease update, and post-claim workflow read.
- `worker_poll_idle`: one `Worker#tick` with no runnable workflow work, useful for idle polling probes.
- `worker_tick_claim`: one `Worker#tick` that polls, claims, and executes the tiny report workflow to completion.

## Database

By default the script uses `DURABABBLE_DATABASE_URL`, or the local MySQL test URL built from `DURABABBLE_MYSQL_*`/`MYSQL_*` env vars, ending at `mysql://root@127.0.0.1:3306/sidekick_server_test`. Override with `--database-url URL`.

The default schema is workspace-derived and operation-specific through `Durababble.workspace_schema(..., prefix: "durababble_hot_path")`. Override with `--schema NAME` only when you need a stable report namespace for comparison.

## Options

```sh
mise exec -- bundle exec ruby scripts/mysql-hot-path-report.rb \
  --operation worker_poll_idle \
  --format markdown \
  --output tmp/sql-hot-path-reports/worker-poll-idle.md
```

Use `--fixture-size N` to seed N unrelated pending workflows before tracing, and `--keep-schema` when you want to inspect the generated MySQL tables after the report. The script resets its report schema before each run and drops it afterward unless `--keep-schema` is set.

## Reading The Report

Start with the Query Timeline table. Each query has the colocated `StoreQueries` description, the concrete registry id, callsite, deterministic transaction label such as `tx1 depth=1`, SQL with placeholders, formatted bind params, and a MySQL `EXPLAIN FORMAT=JSON` summary plus raw JSON.
