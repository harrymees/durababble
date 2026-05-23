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

The Minitest prover runs the same scenario twice with the same seed and asserts identical trace and digest.
It also asserts different seeds produce different traces, proving the seed controls scheduling/fault order.

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
- `concurrent_signal_once` — many signalers wake one wait exactly once.
- `fenced_side_effect_once` — many callers share one fenced side-effect result.
- `waits_fences_and_outbox` — event waits, idempotency fences, and outbox processing.
- `outbox_lease_expiry` — an outbox sender crashes after claim and another sender reclaims after expiry.
- `timer_and_partition` — timer waits plus virtual network partition/drop/heal behavior.
- `chaos` — randomized enqueues, waits, drops, worker crashes, and lease reaping.
- `rpc_fault_injection` — process-boundary timeout, connection error, EOF, remote error, idle reconnect, and success paths.
- `workflow_rpc_owner_state_matrix` — workflow RPC ownership races are covered together: lease moves to a new owner, no active owner is internally restarted, and terminal workflow shutdown rejects the stale call without running the unowned handler.
- `grpc_service_contract` — the protobuf service methods are exercised under the virtual scheduler, including active-owner `DeliverMessage`, stale-owner `DeliverMessage` acknowledgement without work, workflow `CallTransient`, and object/transient `CallTransient`.
- `grpc_workflow_rpc_response_matrix` — gRPC `CallTransient` response variants are covered together: `LeaseMoved`, `not_running`, and unavailable-node outcomes decode to typed routing failures instead of subprocess protocol errors.
- `grpc_workflow_rpc_transport_fault_matrix` — workflow `CallTransient` is exposed to timeout, deadline-exceeded, RST, EOF, unavailable, lost-response, and duplicate-response faults.
- `grpc_workflow_rpc_transport_fault_reroute` — owner transport failures race with lease movement, forcing the router to refresh the active lease and reroute.
- `grpc_wakeup_fault_matrix` — `AwakenBatch`, `DeliverMessage`, and `EvictLease` wakeups are exposed to drop, duplicate, timeout, RST, EOF, and unavailable faults while polling remains the correctness path.
- `bug_duplicate_completion` — intentionally broken fixture used to prove invariant detection reports violations.

The Minitest suite fuzzes each unique scenario target once per seed rather than maintaining a guarantee-to-scenario mapping.
Fixed contract/fault-matrix scenarios run once because they already enumerate their cases.

## Spec gap found during workflow RPC review

The original prototype spec covered distributed workflow leases and lease-aware `Engine#resume`, but it did not define node-to-node workflow RPC routing through the current lease holder. That was a spec gap rather than a known implementation mismatch: there was no `WorkflowRpc` component, no `current_workflow_lease` API, and no matrix row for the race where a caller looks up an owner, sends an RPC, and the lease expires or workflow shuts down in flight. The gap is now explicit in `docs/spec.md` and pinned by the `workflow_rpc_owner_state_matrix` scenario plus ordinary workflow RPC unit tests.

## Seed search

Use this to search a scenario over many deterministic schedules:

```sh
mise exec -- ruby -Ilib -e 'require "durababble"; p Durababble::Deterministic.search("chaos", seeds: 1..200)'
```

An empty array means no invariant violation was found for that seed range.

For mutation testing and handoff reports, prefer `search_reports`, which includes
the scenario, first failing seed, deterministic trace digest, and violation text:

```sh
mise exec -- ruby -Ilib -e 'require "durababble"; p Durababble::Deterministic.search_reports("bug_stale_lease_commit", seeds: 1..25).first'
```

Interpretation:

- A non-empty report means the harness illuminated the seeded bug. Record the first
  failing seed, digest, scenario, and violation, then keep or add a permanent
  meta-test for that bug class.
- An empty report over the searched range means the current scenario/invariants
  did not expose that mutation. Tighten the scenario, trace checks, or invariant
  coverage before treating the mutation as tested.
- The digest is the SHA256 of the deterministic trace. It is useful for comparing
  exact schedules while the human-readable violation names the broken guarantee.

## Mutation testing

Durababble keeps intentionally broken behavior out of production paths by using
test-only mutations in the virtual Yugabyte store. Each `bug_*` scenario enables
one controlled mutation and asserts the harness reports a violation. The permanent
meta-test in `test/durababble/deterministic_test.rb` fails if any seeded bug stops
being detected within seeds `1..3`.

Current mutation matrix:

| Mutation class | Scenario | Seed range | First failing seed | Digest | Violation signal |
| --- | --- | --- | --- | --- | --- |
| duplicate/stale step completion fixture | `bug_duplicate_completion` | `1..25` | `1` | `732352eae3ae7f3a02930c955a519a779c9c3611fa3521169cc2332756f7ef60` | completed workflow has running attempt |
| stale step completion hook | `bug_stale_step_completion` | `1..25` | `1` | `35c152c73c08f10dec19dae2cdbd77c1c11100eb8aeafec6a4aad59e5ccd87bc` | completed workflow has running attempts |
| stale lease commit | `bug_stale_lease_commit` | `1..25` | `1` | `c126d9954a6e59b7029b7e19f118cb779fa4798bc2c972135d9d348c1d3a45a7` | stale lease commit was accepted |
| missed event wake | `bug_missed_event_wake` | `1..25` | `1` | `f70c344418dbdcc8b6600a1a5196fc88d50f2f36302ea5d72b55b90c3b501d2e` | waiting workflow did not complete after signal |
| duplicated event wake | `bug_duplicated_event_wake` | `1..25` | `1` | `19291fa0af41f23ab4f84240ffd914bd365fef6e059309765070f8fe34c1878c` | event wake completed more than once |
| duplicate outbox key | `bug_outbox_duplicate_by_key` | `1..25` | `1` | `050be88e8886e0a025780ee1746688557138e56c8898c94e21c97ffdd52c1ddd` | duplicate outbox key |
| stuck outbox lease | `bug_outbox_stuck_lease` | `1..25` | `1` | `cd65a5c47e21cef55d0e7f27a3a5fda86df4701f6a3994520fb279a733854756` | expired outbox lease was not reclaimed |
| retry/attempt-history corruption | `bug_attempt_history_corruption` | `1..25` | `1` | `f1315eb62321d0f3a226d90a7c6f3bab5b96f251aa1f851b39a08c61fa44c7ed` | attempt history was not append-only |

## Bugs found during harness work

The first deterministic trace revealed that worker ticks claimed the same workflow twice: once in `Worker#tick`/`SimWorker#run_tick`, then again inside `Engine#resume`. This was not a correctness failure, but it was an unnecessary duplicate lease update and trace event. The store now treats an unexpired lease already owned by the same worker as an already-owned claim and returns it without issuing another lease update. This is pinned by a clear store regression test and a deterministic trace-count assertion.

Expanding the crash matrix scenarios found a real attempt-history bug: if a process crashed after `record_step_started` and before step completion, recovery retried the step but left the original attempt in `running` forever after the workflow completed. `record_step_started` now marks stale running attempts for the same workflow/step as failed with `superseded by retry` before appending the retry attempt. This is pinned by a clear regression test that expects attempt statuses `failed, completed, completed`, so DST can return to being a bug-hunting tool rather than the only long-term proof.
