# Durababble benchmark suite

Durababble uses a purpose-built macro benchmark harness for storage and coordination performance, plus notes from the Ruby benchmarking ecosystem:

- `benchmark-ips` is excellent for microbenchmarks because it automatically chooses iteration counts and reports variance.
- `benchmark-driver` is the modern low-overhead Ruby benchmarking driver; it can run generated benchmark scripts, repeat runs, and emit machine-readable records.
- `benchmark-memory` is useful for allocation/memory comparisons.

For Durababble, most meaningful costs are database round trips, leases, queue scans, and process boundaries rather than tiny in-process Ruby snippets. The suite therefore uses a custom harness built on `Process.clock_gettime(Process::CLOCK_MONOTONIC)`, explicit warmup, per-operation samples, allocation counters, environment metadata, and machine-readable JSON/CSV/Markdown outputs.

## Running locally

```sh
mise exec -- ruby bench/run.rb --profile smoke
mise exec -- ruby bench/run.rb --profile full --fixture-size 100000
```

Environment variables:

- `DURABABBLE_DATABASE_URL` defaults to the local agent-server MySQL development database.
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
- durable event waits and signaling;
- event waits that resume the remaining workflow after an external signal;
- timer waits that wake and resume the remaining workflow;
- worker `tick` claim + execute behavior;
- worker `run_until_idle` batch draining;
- resume behavior that skips completed steps and continues remaining steps;
- observability reads for workflow/step/attempt/wait state;
- failed-workflow retry through the runnable queue;
- expired workflow lease recovery;
- idempotency fence first execution and cached-result replay;
- outbox enqueue/claim/ack;
- outbox expired-lease reclaim;
- queue claim performance with large historical workflow tables;
- due-timer wake scans with many unrelated wait rows;
- event-signal misses with large pending wait sets;
- JSON-line command RPC to a separate Ruby process;
- cross-process enqueue + claim command RPC.

## Artifacts

Every run emits:

- `durababble-bench-<profile>-<timestamp>.json`
- `durababble-bench-<profile>-<timestamp>.csv`
- `durababble-bench-<profile>-<timestamp>.md`

GitHub Actions uploads these as durable artifacts so runs can be compared over time.
