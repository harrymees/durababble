# Durababble benchmark suite

Durababble uses a purpose-built macrobenchmark harness for storage and coordination performance, plus notes from the Ruby benchmarking ecosystem:

- `benchmark-ips` is excellent for microbenchmarks because it automatically chooses iteration counts and reports variance.
- `benchmark-driver` is the modern low-overhead Ruby benchmarking driver; it can run generated benchmark scripts, repeat runs, and emit machine-readable records.
- `benchmark-memory` is useful for allocation/memory comparisons.

For Durababble, most meaningful costs are database round trips, leases, queue scans, and process boundaries rather than tiny in-process Ruby snippets. The suite therefore uses a custom harness built on `Process.clock_gettime(Process::CLOCK_MONOTONIC)`, explicit warmup, per-operation samples, allocation counters, environment metadata, and machine-readable JSON/CSV/Markdown outputs.

## Running locally

```sh
mise exec -- ruby bench/run.rb --profile smoke
mise exec -- ruby bench/run.rb --profile history-smoke
mise exec -- ruby bench/run.rb --profile full --fixture-size 100000
```

Environment variables:

- `DURABABBLE_DATABASE_URL` defaults to the local agent-server MySQL development database.
- `DURABABBLE_SCHEMA` or `DURABABBLE_WORKSPACE_ROOT` selects the enclosing workspace namespace.
- `DURABABBLE_BENCH_SCHEMA` overrides the benchmark schema; if omitted, benchmarks derive a separate schema from the workspace namespace so benchmark tables do not collide with tests or other worktrees.
- `DURABABBLE_BENCH_PROFILE=smoke|full` controls iteration counts.
- `DURABABBLE_BENCH_FIXTURE_SIZE` controls the large-table fixture size.
- `DURABABBLE_BENCH_OUTPUT` controls output directory.
- `DURABABBLE_BENCH_KEEP_SCHEMA=1` leaves the generated schema behind for manual query inspection.

## Operations covered

The suite measures realistic durable-execution operations:

- enqueueing many workflows;
- claiming/dequeueing pending work under leases;
- lease heartbeat/renewal;
- lease conflict checks;
- timer waits that wake and resume the remaining workflow;
- worker `tick` claim + execute behavior;
- worker `run_until_idle` batch draining;
- resume behavior that skips completed steps and continues remaining steps;
- bounded replay/resume behavior for small, medium, and intentionally large completed workflow histories;
- observability reads for workflow/step/attempt/wait state;
- failed-workflow retry through the runnable queue;
- expired workflow lease recovery;
- idempotency fence first execution and cached-result replay;
- outbox enqueue/claim/ack;
- outbox expired-lease reclaim;
- durable object state and command enqueue/claim/complete/read;
- queue claim performance with large historical workflow tables;
- due-timer wake scans with many unrelated wait rows;
- JSON-line command RPC to a separate Ruby process;
- cross-process enqueue + claim command RPC.

## Adding Store Queries

Every new production store SQL query must be defined in `Durababble::StoreQueries`. The query-plan tests record registered query ids from the large-fixture hot-path operations and fail if a required hot query is not exercised by the performance assertions.

If a registered query is intentionally outside the large-fixture EXPLAIN suite, add it to the explicit uncovered-query list in `test/durababble/query_plan_test.rb` so the exemption is reviewed as source.

For hot queue, lease, wait, outbox, durable-object, or future inbox paths, also update `HOT_QUERY_COVERAGE` with the intended index, lock/order/row-bound assertion, and benchmark operation. Add or extend `test/durababble/query_plan_test.rb`, `test/durababble/mysql_query_plan_test.rb`, and `bench/run.rb` in the same change unless the registry documents a narrow conformance-only exemption.

## Artifacts

Every run emits:

- `durababble-bench-<profile>-<timestamp>.json`
- `durababble-bench-<profile>-<timestamp>.csv`
- `durababble-bench-<profile>-<timestamp>.md`

GitHub Actions uploads these as durable artifacts so runs can be compared over time.
