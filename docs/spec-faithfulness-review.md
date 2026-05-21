# Spec faithfulness review

> Historical note: this review predates the class-oriented workflow/durable-object API replacement. It is retained as an implementation archaeology note for the earlier prototype; do not treat rows that mention the old workflow DSL as current API documentation. Current spec content lives in `docs/spec.md`.

Review date: 2026-05-21

## Executive summary

Durababble is broadly faithful to the prototype spec in `docs/spec.md`: the main durable-execution features exist, the real-Yugabyte integration suite passes, and the DST suite maps every safety/crash row to deterministic scenarios. The strongest coverage is around workflow claims, lease-aware resume, waits, outbox leases, and crash/recovery paths.

Follow-up correction: runtime values are intended to use Paquito, not JSON/JSONB. The JSON-specific findings below describe the earlier implementation drift that existed during this review. See `docs/paquito-storage-review.md` for the correction that moved runtime payload storage to Paquito `bytea` columns and added explicit tests for that guarantee.

The main divergences found are smaller but important correctness details:

1. **Implementation bug: generic row decoding parsed every text column as JSON.** Text values like workflow names, topics, keys, and error strings could be type-corrupted if they happened to look like JSON (`"123"`, `"true"`, `"null"`, `{...}`). The implementation should decode only known JSON/JSONB columns.
2. **Implementation bug: JSON serialization collapsed `false` and `nil` to `{}`.** `dump_json(value || {})` made `false` indistinguishable from `{}`. The implementation should preserve all JSON values and only default nil where the schema/DSL intentionally requires object context.
3. **Spec gap: fence crash recovery is underspecified.** `with_fence` acquires before executing the side effect and prevents concurrent duplicate blocks, matching the current spec, but if the owner process dies mid-block, the fence remains `running` until callers time out. The table has `locked_until`, but no takeover/retry semantics. This should either become an explicit prototype boundary or be added to the guarantee matrix later.
4. **Resolved worker-pool boundary: worker behavior for unknown workflow names is now avoided during normal polling.** `Worker#tick` asks the store to claim only workflow names present in the supplied registry, so a pool does not lease work it cannot execute. Workflows with no matching pool remain pending until an appropriate pool starts.
5. **Test gap: CLI coverage is happy-path only.** It verifies migrate/run/inspect/resume, but not bad commands, missing IDs, or schema/database errors. This is acceptable for prototype scope but not comprehensive CLI behavior.
6. **Test gap: heartbeats and acknowledgements check positive paths more than negative ownership results.** Lease ownership and outbox ownership are mostly tested through behavior, but return values/cmd tuple effects are not exposed by public API. This is acceptable, though future APIs should return booleans for owner-sensitive mutations.

7. **Spec gap found after initial review: lease-routed workflow RPC was not specified.** The spec covered workflow leases, lease-aware resume, and generic process-boundary command RPC benchmarks, but it did not specify node-to-node workflow RPC routing through the current lease holder or stale in-flight behavior when leases move/shutdown occurs. This was a spec gap, not an implementation drift from an existing requirement. It is now covered by `WorkflowRpc`, `Store#current_workflow_lease`, unit tests, query-plan coverage, and DST scenarios.
8. **Step retry policy scope is now explicit.** Durababble implements Temporal Activity-style retry options at the step boundary (`initial_interval`, `backoff_coefficient`, `maximum_interval`, `maximum_attempts`, `schedule`, `non_retryable_errors`) and persists retry wakeups with `workflows.next_run_at`. It does not yet specify workflow-level retry policies; that remains future work, consistent with Temporal's distinction between Activity retry defaults and explicit Workflow Execution retry policy.

## Spec row-by-row assessment

| Spec assertion | Status | Decision |
| --- | --- | --- |
| Ruby 4 gem scaffold managed by mise | Faithful | No change. |
| Yugabyte storage through PostgreSQL wire protocol | Faithful | No change. |
| Workflow DSL with ordered named steps | Faithful for basic ordered steps | Spec is intentionally minimal; no need to add duplicate-name validation now. |
| Durable workflow and step rows | Faithful | No change. |
| Append-only attempt history | Mostly faithful | Records are appended, terminal statuses are updated in-place. The wording is acceptable because attempts remain as records, but avoid implying immutable event sourcing. |
| Runnable workflow queue via pending rows | Faithful | No change. |
| Worker polling | Faithful for registered workflow names; normal polling filters claims to the worker registry | Workflows with no matching worker pool remain pending until a suitable pool starts. |
| Distributed workflow leases | Mostly faithful | Claims use `FOR UPDATE SKIP LOCKED`; execution itself relies on lease ownership at start and does not heartbeat automatically. Prototype boundary should keep long-running step heartbeat out of scope. |
| Lease-aware resume | Faithful | No change. |
| Heartbeat extension | Faithful | Test coverage adequate, but public return value could improve later. |
| Expired lease stealing | Faithful | No change. |
| Skip completed steps / retry incomplete work | Faithful after stale-attempt fix | Regression test exists. |
| Timer waits | Faithful | No change. |
| Event waits | Faithful | No change. |
| Side-effect idempotency fences | Faithful for concurrent callers while owner completes/fails | Spec gap for owner crash during fenced block; mark as boundary unless implemented later. |
| Durable outbox unique keys/leases/ack | Faithful | No change. |
| CLI commands | Faithful happy path | CLI error-path tests missing; acceptable prototype gap. |
| SimpleCov thresholds | Faithful | Current full run exceeds thresholds. |

## Test coverage review

### Strengths

- Full suite passes against real local Yugabyte/YSQL, not mocks.
- Multi-connection concurrency tests cover workflow claims, fences, outbox claims, and event signals.
- Subprocess crash harness covers a real process failure after step completion.
- DST suite proves same-seed determinism and searches every safety/crash matrix row across many seeds.
- Coverage is high: latest run before this review reported 28 examples, 0 failures, 97.72% line coverage and 76.61% branch coverage.

### Weak spots

- `complete_spec.rb` packs many guarantees into broad scenario tests; failures can be harder to diagnose than one-spec-per-row tests.
- The DST virtual store is useful for exploration but can drift from the real `Store`; real-Yugabyte regression specs should pin every DST-found bug.
- CLI negative/error behavior is not tested.
- JSON/text decoding edge cases were not covered before this review.
- Fence-owner crash semantics are not covered because the spec does not define recovery behavior.

## Actions started from this review

- Add real-Yugabyte regression tests for JSON/text decoding and JSON value preservation.
- Fix `Store#decode_row` to decode only JSON columns instead of all text columns.
- Fix `Store#dump_json` to preserve `false` and explicit `nil` instead of coercing falsey values to `{}`.
- Update docs to mark fence-owner crash recovery as a prototype boundary and document worker-pool claim filtering for registry misses.
