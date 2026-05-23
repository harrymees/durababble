# Absurd test coverage import notes

This note records the source-backed analysis for HAR-1271. It is intentionally
about behavior and regression ideas, not a line-by-line port.

## Upstream identity

| Field | Value |
| --- | --- |
| Repository | https://github.com/earendil-works/absurd.git |
| Branch | `main` |
| Commit inspected | `f2fcc45db4dfa46cd44cab36a4aa1f5d9e393bbd` |
| License | Apache-2.0 |
| Primary test command | `make test` |
| Core SQL test command run | `cd tests && uv run pytest` |
| Python SDK test command run | `cd sdks/python && uv run pytest` |
| TypeScript SDK command run | `cd sdks/typescript && npm ci && npm run type-check && npm test` |
| Full upstream blocker | Host has no `go`, so `make test` cannot complete `test-go`. |

## Upstream test map

| Layer | Absurd coverage observed | Durababble comparison |
| --- | --- | --- |
| Unit tests | CLI validation, queue-name validation, schema-version helpers, partition utility functions, SDK option normalization. | Durababble has smaller unit coverage around wait request construction, retry policy, public API branches, docs examples, and storage namespace helpers. |
| Storage/backend tests | Core pytest suite executes SQL functions against Postgres containers, including queues, indexes, partitioning, cleanup, schema migration/version, claims, checkpoints, events, retries, cancellation, and task result state. | Durababble has MySQL-first backend conformance plus optional Yugabyte tests; it covers queue claims, waits, outbox, object commands, Paquito storage, query plans, and namespace isolation, but not partitioned queues or retention cleanup. |
| Worker/runtime tests | Python, TypeScript, and Go SDK suites exercise worker loops, task registration, unknown task deferral, concurrency, hooks, task context helpers, heartbeat, and await-result behavior. | Durababble covers worker ticks, runtime start/shutdown, lease release on timeout, registered workflow filtering, workflow RPC, and gRPC transport. It lacks SDK hook/context propagation surfaces because those APIs do not exist. |
| Replay/history tests | Step checkpoints are reused after retry; repeated step names are auto-numbered; sleep checkpoints prevent infinite rescheduling; checkpoint reads filter by task/run. | Durababble has method/order replay checks and crash recovery; the imported tests add repeated wait positions and crash-after-wait persistence checks. |
| Concurrency/race tests | Concurrent event emit/await race, lock ordering for cancel vs complete/fail, bounded claim maintenance, concurrent worker behavior, first-write-wins events. | Durababble already has concurrent claims, concurrent fences, concurrent event signalers, outbox claim/reclaim, lease-routed RPC races, and deterministic simulation. It does not cache events emitted before a wait. |
| Cancellation tests | Pending/running/sleeping cancellation, idempotent cancel, cancellation blocking checkpoints, no-op terminal cancellation, max duration/delay cancellation. | Durababble has no full workflow cancellation API yet; workflow `expose_command` currently persists command events only. This is a product/API gap, not a test-only gap. |
| Integration/e2e tests | Core SQL via testcontainers, pg_cron/faketime e2e, Python SDK, TypeScript SDK, Go SDK, Habitat Go server tests. | Durababble has real MySQL tests by default, optional Yugabyte/YSQL conformance, CLI-style examples, gRPC/RPC transport tests, and benchmarks. |
| Deterministic/fault injection tests | Absurd relies on real Postgres races and containerized integration more than a deterministic simulator. | Durababble has a dedicated deterministic simulation harness with seeded crash/recovery, lease, wait, outbox, and RPC fault scenarios. This is a Durababble strength rather than an Absurd gap to import. |

## Selected imports

| Upstream test area | Durababble gap | Imported/adapted test | Result |
| --- | --- | --- | --- |
| Sleep checkpoint survives retry/crash | Existing timer tests covered ordinary wake/resume, but not a crash immediately after the wait row was durably recorded. | `DurababbleAbsurdInspiredTest#test_does_not_recreate_a_timer_wait_after_crashing_immediately_after_persistence...` | Adds crash/recovery assertion that only one wait row exists and completed attempts are preserved. |
| Repeated step/sleep names | Existing replay tests check reordered methods and suffix removal, but not repeating the same step method as multiple durable wait positions. | `DurababbleAbsurdInspiredTest#test_keeps_repeated_durable_waits_from_the_same_step_method_distinct_by_position...` | Adds method/order regression coverage for repeated waits at positions 0 and 1. |

## Not imported now

| Absurd idea | Reason |
| --- | --- |
| Event emitted before await is cached and later delivered. | Durababble currently models events as wakeups for existing waits; there is no event-cache table or public guarantee for pre-wait delivery. Importing this would require a deliberate spec/storage change. |
| Workflow/task cancellation blocks checkpoint writes. | Durababble has no settled cancellation API beyond transitional command events. |
| Idempotent public spawn/start keys. | The target spec calls for caller idempotency keys, but current `Workflow.enqueue` always creates a fresh workflow. This should be implemented with explicit API/storage design, not hidden inside this test-import ticket. |
| Partitioned queues, pg_cron cleanup, and detach planning. | Absurd-specific Postgres operational surface; Durababble uses workspace schema/table-prefix isolation and has different retention/partitioning scope. |
| SDK hook/context propagation tests. | Durababble does not yet expose comparable SDK hooks. |
