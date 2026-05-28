# Deterministic simulation testing

Durababble includes a local deterministic simulation harness inspired by `gadget-inc/silo`'s `tests/turmoil_runner` setup. Silo uses Rust `turmoil` plus `mad-turmoil` to virtualize networking, time, and randomness, then proves determinism by running each scenario twice with the same `DST_SEED` and comparing deterministic trace output byte-for-byte.

Ruby does not appear to have an equivalent to `mad-turmoil` that intercepts libc randomness/time and sockets for arbitrary Ruby code, so Durababble keeps a small purpose-built test harness in `test/support/deterministic.rb` rather than shipping it as production library code.

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

The Minitest prover runs the same scenario twice with the same seed and asserts identical trace and digest. It also asserts different seeds produce different traces, proving the seed controls scheduling/fault order.

## Scenario set

Current scenarios:

- `multi_worker_counter` — clients enqueue workflows while multiple workers compete for leases.
- `workflow_durable_before_claim` — enqueue survives before any worker claim.
- `lease_conflict` — non-owning workers cannot resume a live lease.
- `heartbeat_extension` — owner heartbeat prevents premature expired-lease stealing.
- `step_heartbeat_cursor_recovery` — an invocation heartbeats an opaque cursor, crashes before step completion, lease expiry recovery retries the step, and the next invocation receives the prior cursor.
- `step_retry_policy_recovery` — a flaky step fails under a configured retry policy, stores durable `next_run_at` retry delays, cannot be claimed before the due time by a restarted worker, and eventually completes under a later worker with append-only failed/failed/completed attempts.
- `lease_expiry` — a crashed worker's workflow lease expires and another worker recovers it.
- `completed_step_skip_after_crash` — a completed step is skipped after crash/recovery.
- `incomplete_step_retry_after_crash` — a step that crashed after start is retried and stale attempts are closed.
- `attempt_history_append_only` — repeated failures append attempts instead of overwriting history.
- `concurrent_timer_wake_once` — many callers race to wake one due timer exactly once.
- `multiple_named_object_wakes` — one durable object arms several independently named wakes in a single command; each matures into its own `wake` inbox message and `on_wake(name:, payload:)` runs once per name.
- `object_wake_survives_worker_crash` — a worker claims a matured object wake and crashes before committing; after the lease expires a second worker reclaims and the idempotent `on_wake` handler applies the effect exactly once.
- `fenced_side_effect_once` — many callers share one fenced side-effect result.
- `waits_fences_and_outbox` — timer waits, idempotency fences, and outbox processing.
- `outbox_lease_expiry` — an outbox sender crashes after claim and another sender reclaims after expiry.
- `timer_and_partition` — timer waits plus virtual network partition/drop/heal behavior.
- `chaos` — randomized enqueues, waits, drops, worker crashes, and lease reaping.
- `rpc_fault_injection` — process-boundary timeout, connection error, EOF, remote error, idle reconnect, and success paths.
- `workflow_rpc_owner_state_matrix` — workflow RPC ownership races are covered together: lease moves to a new owner, no active owner is internally restarted, and terminal workflow shutdown rejects the stale call without running the unowned handler.
- `cooperative_cancellation_cleanup` — a waiting workflow receives a durable cancellation request, cancels waiting step/attempt state, delivers `CancellationError`, runs cleanup once, ignores a late timer claim, and finishes as canceled.
- `rpc_service_contract` — the protobuf service methods are exercised under the virtual scheduler, including active-owner `DeliverMessage`, stale-owner `DeliverMessage` acknowledgement without work, workflow `CallTransient`, and object/transient `CallTransient`.
- `rpc_workflow_rpc_response_matrix` — RPC `CallTransient` response variants are covered together: `LeaseMoved`, `not_running`, and unavailable-node outcomes decode to typed routing failures instead of subprocess protocol errors.
- `rpc_workflow_rpc_transport_fault_matrix` — workflow `CallTransient` is exposed to timeout, deadline-exceeded, RST, EOF, unavailable, lost-response, and duplicate-response faults.
- `rpc_workflow_rpc_transport_fault_reroute` — owner transport failures race with lease movement, forcing the router to refresh the active lease and reroute.
- `rpc_wakeup_fault_matrix` — `AwakenBatch`, `DeliverMessage`, and `EvictLease` wakeups are exposed to drop, duplicate, timeout, RST, EOF, and unavailable faults while polling remains the correctness path.
- `bug_duplicate_completion` — intentionally broken fixture used to prove invariant detection reports violations.
- `bug_invalid_store_shape` — intentionally broken fixture used to prove DST catches invalid virtual-store row shape, missing cross-references, and missing leases.

The Minitest suite fuzzes each unique scenario target once per seed rather than maintaining a guarantee-to-scenario mapping. Fixed contract/fault-matrix scenarios run once because they already enumerate their cases.

## Spec gap found during workflow RPC review

The original prototype spec covered distributed workflow leases and lease-aware `Engine#resume`, but it did not define node-to-node workflow RPC routing through the current lease holder. That was a spec gap rather than a known implementation mismatch: there was no `WorkflowRpc` component, no `current_workflow_lease` API, and no matrix row for the race where a caller looks up an owner, sends an RPC, and the lease expires or workflow shuts down in flight. The gap is now explicit in `docs/spec.md` and pinned by the `workflow_rpc_owner_state_matrix` scenario plus ordinary workflow RPC unit tests.

## Seed search

Use this to search a scenario over many deterministic schedules:

```sh
mise exec -- ruby -Ilib -Itest -e 'require "support/deterministic"; p Durababble::Deterministic.search("chaos", seeds: 1..200)'
```

An empty array means no invariant violation was found for that seed range.

## Bugs found during harness work

The first deterministic trace revealed that worker ticks claimed the same workflow twice: once in `Worker#tick`/`SimWorker#run_tick`, then again inside `Engine#resume`. This was not a correctness failure, but it was an unnecessary duplicate lease update and trace event. The store now treats an unexpired lease already owned by the same worker as an already-owned claim and returns it without issuing another lease update. This is pinned by a clear store regression test and a deterministic trace-count assertion.

Expanding the crash matrix scenarios found a real attempt-history bug: if a process crashed after `record_step_started` and before step completion, recovery retried the step but left the original attempt in `running` forever after the workflow completed. `record_step_started` now marks stale running attempts for the same workflow/step as failed with `superseded by retry` before appending the retry attempt. This is pinned by a clear regression test that expects attempt statuses `failed, completed, completed`, so DST can return to being a bug-hunting tool rather than the only long-term proof.
