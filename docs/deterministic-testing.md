# Deterministic simulation testing

Durababble includes a local deterministic simulation harness inspired by `gadget-inc/silo`'s `tests/turmoil_runner` setup. Silo uses Rust `turmoil` plus `mad-turmoil` to virtualize networking, time, and randomness, then proves determinism by running each scenario twice with the same `DST_SEED` and comparing deterministic trace output byte-for-byte.

Ruby does not appear to have an equivalent to `mad-turmoil` that intercepts libc randomness/time and sockets for arbitrary Ruby code, so Durababble ships a small purpose-built harness in `lib/durababble/deterministic.rb`.

## What is virtualized

- Seeded RNG using a local 64-bit LCG.
- Virtual scheduler with deterministic logical time.
- Virtual network with deterministic latency, drops, partitions, and healing.
- Virtual Yugabyte store implementing the same storage contract used by `Durababble::Engine` and `Durababble::Worker`.
- Simulated clients and worker nodes that run the real workflow/engine code against the virtual store.
- Stable trace formatter with sorted fields and no wall-clock timestamps, thread IDs, random UUIDs, or process-dependent output.

## Determinism prover

`Durababble::Deterministic.prove(name, seed:)` runs one scenario and returns a result with:

- `trace` — deterministic trace text.
- `digest` — SHA256 of the trace.
- `violations` — invariant violations detected after the run.
- `summary` — stable counters for completed workflows, side effects, and processed outbox messages.

The RSpec prover runs the same scenario twice with the same seed and asserts identical trace and digest. It also asserts different seeds produce different traces, proving the seed controls scheduling/fault order.

## Scenario set

Current scenarios:

- `multi_worker_counter` — clients enqueue workflows while multiple workers compete for leases.
- `waits_fences_and_outbox` — event waits, idempotency fences, and outbox processing.
- `lease_expiry` — a crashed worker's workflow lease expires and another worker recovers it.
- `outbox_lease_expiry` — an outbox sender crashes after claim and another sender reclaims after expiry.
- `timer_and_partition` — timer waits plus virtual network partition/drop/heal behavior.
- `chaos` — randomized enqueues, waits, drops, worker crashes, and lease reaping.
- `bug_duplicate_completion` — intentionally broken fixture used to prove invariant detection reports violations.

## Seed search

Use this to search a scenario over many deterministic schedules:

```sh
mise exec -- ruby -Ilib -e 'require "durababble"; p Durababble::Deterministic.search("chaos", seeds: 1..200)'
```

An empty array means no invariant violation was found for that seed range.

## Bug found during harness work

The first deterministic trace revealed that worker ticks claimed the same workflow twice: once in `Worker#tick`/`SimWorker#run_tick`, then again inside `Engine#resume`. This was not a correctness failure, but it was an unnecessary duplicate lease update and trace event. The store now treats an unexpired lease already owned by the same worker as an already-owned claim and returns it without issuing another lease update.
