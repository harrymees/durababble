# DST effectiveness report

This report reviews the deterministic simulation testing (DST) system in
`lib/durababble/deterministic.rb` and `test/durababble/deterministic_test.rb`,
then records temporary source bug-injection probes used to check whether the
suite catches real consistency regressions.

## Summary

The DST suite is effective for the workflow-engine and workflow-RPC bugs it
explicitly models. It runs the real `Durababble::Engine`, workflow step
wrappers, retry logic, heartbeat logic, and workflow-RPC router/handler code
against a deterministic scheduler, virtual network, and virtual SQL-like store.
The two artificial source bugs injected during this review were caught quickly
and consistently by the relevant DST scenarios.

The suite is not yet a broad distributed-systems bug finder. The scenarios are
mostly hand-scripted, the virtual store is an in-memory model rather than a SQL
isolation/locking simulator, the chaos scenario has weak liveness oracles, and
several target surfaces from `docs/spec.md` are not modeled yet because the
runtime features are still missing or transitional.

## Code review

### Harness shape

`Durababble::Deterministic.prove` runs one named scenario, and
`search` repeats a scenario over a caller-provided seed range, returning only
seeds whose `Result#violations` are non-empty. That makes the harness useful
both as a deterministic contract test and as a seed-search tool.

The deterministic substrate is intentionally small:

- `Rng` is a local 64-bit LCG.
- `Trace` emits stable, sorted-field trace lines with no wall-clock time,
  object ids, thread ids, or other process-dependent values.
- `Scheduler` serializes all work onto a virtual event queue ordered by logical
  time and insertion sequence.
- `VirtualNetwork` models latency, directional partitions, drops, and duplicate
  delivery.
- `FaultPlan` injects crash-like exceptions after selected virtual store write
  operations.

This is enough to make schedules reproducible and debuggable. It also keeps the
suite very fast: the full deterministic test file completed locally in about
2.5 seconds.

### Multi-entity modeling

The suite models several entities in one logical run:

- clients enqueue workflows or signals through the virtual network;
- worker nodes claim workflows and execute the real engine;
- reapers steal expired workflow leases;
- outbox senders claim and ack virtual outbox rows;
- workflow-RPC clients, routers, and handlers exercise owner routing and stale
  ownership races;
- gRPC matrix scenarios simulate transport outcomes and service responses.

This is a meaningful multi-actor model for workflow safety. It can expose bugs
where a worker, signaler, sender, or RPC caller observes stale or duplicated
state under a deterministic interleaving.

Important gaps remain:

- durable objects are not modeled in DST today;
- object command serialization, per-object FIFO, ask/tell ordering, object
  sleeps, dead-letter behavior, and unified inbox behavior are outside the
  current simulator;
- worker-pool persistence/routing and node registry behavior are not modeled;
- workflow signals/history and patch-marker history are not modeled;
- cross-backend differences between MySQL/MariaDB and PostgreSQL/YSQL are not
  modeled by the virtual store.

Those gaps line up with target or partially implemented areas in the spec, but
they mean DST should not be read as coverage for those future contracts.

### Fault injection

The suite has useful targeted fault models:

- network drop, partition, heal, and duplicate-delivery paths;
- worker pre-tick crash simulation;
- engine-level crash points after step start, wait record, step completion, and
  workflow completion;
- store write faults after completed step writes, wait writes, and outbox
  enqueues;
- workflow-RPC stale owner, no-active-owner, shutdown, unavailable node, and
  transport fault matrices;
- gRPC timeout, deadline, RST, EOF, lost-response, duplicate-response, drop, and
  duplicate wakeup cases.

The fault model is still narrower than a real distributed SQL system. The
virtual store is a mutable Ruby hash model with handwritten lease and wait
semantics. It does not exercise real transaction isolation, SQL lock ordering,
`SKIP LOCKED`, serialization failures, statement timeouts, connection pool
behavior, binary serialization failures, or backend-specific timestamp/clock
behavior. The fault plan injects at named boundaries rather than exploring
arbitrary mid-transaction or concurrent SQL interleavings.

That design is reasonable for speed and determinism, but it means any
DST-discovered storage issue still needs a real backend regression test, as the
spec already requires.

### Scenario comprehensiveness

The current test file fuzzes 26 scenario targets over seeds `1..100` and runs
three fixed contract/fault-matrix scenarios once. The scenario set covers the
core implemented workflow guarantees well:

- workflow durability before claim;
- lease ownership, heartbeat extension, zombie heartbeat rejection, and lease
  expiry recovery;
- completed-step replay and incomplete-step retry after crash;
- append-only attempt history;
- heartbeat cursor recovery;
- step retry scheduling and due-time behavior;
- event waits, timer waits, stale terminal waits, and duplicate signal delivery;
- fence deduplication and outbox deduplication, leasing, expiry, and ack owner
  behavior;
- workflow-RPC owner movement, no-active owner restart, shutdown rejection, and
  transport response mapping;
- gRPC service method and wakeup fault matrix coverage.

The weakest areas are scenario oracles and breadth outside workflow safety. For
example, `chaos` creates randomized counters and waiters, injects network drops
and worker crashes, and runs reapers, but it has no scenario-specific checks.
It relies only on generic harness invariants. That makes it useful as a
deterministic smoke test for gross state corruption, but weak as a bug finder
for lost work, missed wakeups, unexpected workflow terminal states, duplicate
logical side effects, or starvation.

### Assertions and violations

The assertion model has two layers:

- scenario-specific `h.check` blocks assert expected trace events, final
  workflow status, attempt histories, side-effect counts, outbox processing
  counts, or absence of unowned RPC handler execution;
- global harness checks flag completed workflows that remain locked, completed
  workflows with running attempts, and duplicate completed step positions.

The scenario-specific checks are the main source of value. The generic
invariants are useful but shallow. In particular, duplicate completed step
positions are hard to produce in the current virtual representation because
steps are stored in a hash keyed by position, so that invariant is less
powerful than it sounds. The built-in broken scenario catches the running
attempt invariant, not a true duplicate-row invariant.

The suite currently discovers violations as strings in `Result#violations`.
That is simple and readable, but it does not classify failures by guarantee,
component, or bug class. A failing seed tells us what check failed, but not
which spec guarantee it was intended to protect unless the scenario name or
check text is already obvious.

## Artificial bug injection results

All temporary edits below were reverted after each probe; the final git diff was
clean before writing this report.

### Probe 1: completed-step replay disabled

Temporary source bug:

- File: `lib/durababble/engine.rb`
- Change: forced `WorkflowExecution#call_step` to ignore
  `@completed_steps.key?(position)` so recovery reran a step that should have
  replayed from durable history.

Command:

```sh
mise exec -- ruby -Ilib -e 'require "durababble"; p Durababble::Deterministic.search("completed_step_skip_after_crash", seeds: 1..50)'
```

Outcome:

- 50 out of 50 seeds failed.
- Every failure reported `check failed: completed step was not re-started`.

Interpretation:

The suite is effective for this class of workflow replay regression. The
scenario directly asserts that only the original two logical counter steps are
started across crash and recovery.

### Probe 2: workflow-RPC owner validation removed

Temporary source bug:

- File: `lib/durababble/workflow_rpc.rb`
- Change: removed the pre-handler `assert_current_lease!` call from
  `WorkflowRpc::Handler#call`, allowing a stale owner to enter user handler
  code before ownership validation.

Command:

```sh
mise exec -- ruby -Ilib -e 'require "durababble"; p Durababble::Deterministic.search("workflow_rpc_owner_state_matrix", seeds: 1..20)'
```

Outcome:

- 20 out of 20 seeds failed.
- Every failure reported `check failed: unowned handler did not run`.

Interpretation:

The suite is effective for this workflow-RPC lease-ownership regression. The
owner-state matrix has a concrete negative oracle: stale or terminal owners must
not run the handler body.

## Effectiveness assessment

The suite is already useful and has demonstrated bug-finding value for the
implemented workflow safety surface. It is especially strong where a scenario
has a precise invariant: completed-step replay, retry due times, heartbeat
cursor recovery, stale lease rejection, stale wait rejection, duplicate signal
deduplication, outbox idempotency, and RPC owner validation.

The suite is less effective as a general-purpose consistency detector. It will
miss bugs outside the virtual store contract, bugs in SQL adapter-specific
behavior, bugs in unmodeled durable-object/inbox/signal/patch-marker surfaces,
and bugs in chaos schedules that do not violate the generic harness checks.
Right now, "run enough seeds" mostly explores timing around a fixed scenario
shape; it does not synthesize many new operation sequences or automatically
derive stronger final-state oracles.

The realisticness is therefore mixed:

- realistic for high-level workflow lease/replay/RPC state-machine logic,
  because the real engine and router code run;
- partially realistic for storage, because the virtual store mirrors many store
  methods and durable boundaries;
- not realistic for SQL concurrency, backend portability, process death,
  binary serialization, or future mailbox/inbox semantics.

## Suggested improvements

1. Add a guarantee-to-scenario matrix in `docs/deterministic-testing.md` that
   lists each guarantee, the scenario(s) that cover it, the exact oracle that
   would fail, and whether there is also a real backend regression test.
2. Strengthen `chaos` with explicit liveness and accounting oracles: number of
   accepted enqueues, terminal/waiting/pending states allowed at the end, max
   duplicate side effects per logical operation, no unrecoverable running
   attempts, and no expired lease left without a recovery reason.
3. Add a small mutation/sensitivity runner for DST. Keep the mutations out of
   production code, but automate probes like "disable completed-step replay",
   "skip owner validation", "drop wait completion idempotency", and "ignore
   retry due time" so CI or a periodic job can prove that core scenarios still
   fail when their protected invariant is broken.
4. Rename or document `VirtualYugabyte` as a virtual store model, not a
   Yugabyte/YSQL simulator. Add a parity checklist comparing each virtual store
   method with the real PostgreSQL/YSQL and MySQL/MariaDB store methods it is
   approximating.
5. Add real-store confirmation tests for any DST-found storage bug. Where
   practical, create scenario recipes that can run once against the virtual
   store and once against the real store backend conformance harness.
6. Expand the virtual fault model to include serialization failures, transaction
   rollback-after-partial-work simulation, connection loss before versus after
   commit, lock contention, and stale writer attempts after a lease release.
7. Add durable-object and mailbox scenarios as those features harden: FIFO
   command ordering, blocked head behavior, ask/tell ordering, object command
   owner crash, object sleep wakeup, dead-letter repair, and idempotency
   conflicts.
8. Add workflow signal/history and patch-marker scenarios when those APIs land:
   accepted signal replay order, terminal signal rejection, patch marker
   first-run recording, old-history false branch, marker-history true branch,
   and missing-marker nondeterminism.
9. Classify violations with structured metadata such as guarantee id, component,
   operation, and seed. Keep the readable strings, but make failure aggregation
   and trend tracking possible.
10. Add trace coverage counters for important modeled events, and fail when a
    scenario seed range stops exercising the intended fault path even though the
    scenario still passes.
