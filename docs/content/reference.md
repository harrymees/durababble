---
title: "Reference"
weight: 60
---

# Reference

This reference summarizes the implemented prototype. The spec records the intended public direction so reviewers can distinguish current behavior from target behavior.

## Implemented Surface

- Class-oriented workflow API with `#execute`, `step def`, retry policy, step idempotency keys, class-method enqueueing, `Workflow.start` / `Workflow.handle` aliases, and optional `engine:` overrides.
- First-class cooperative workflow cancellation through `Workflow.handle(...).cancel(reason:)`, persisted cancellation requests, `canceling` / `canceled` states, and replay-safe cleanup steps.
- Class-oriented durable object API with `at` / `ref`, `tell`, `expose`, `expose_command`, optional `engine:` overrides, command idempotency keys, and explicit state updates.
- PostgreSQL/YSQL and MySQL/MariaDB store implementations.
- Durable workflow, step, wait, attempt, fence, outbox, durable-object, and durable-object-command persistence.
- Worker polling with leased workflow claims.
- Heartbeats, stale lease recovery, and lease-aware resume.
- Timer waits, side-effect fences, and durable outbox primitives.
- Retry due-time claims distinguish retryable failures from terminal failed workflows.
- Lease-routed workflow RPC helpers.
- Deterministic simulation tests for workflow safety and crash-recovery scenarios.

## Prototype Boundaries

- `Durababble.configure` installs a default store and default engine for the top-level workflow and object helpers. Callers can still pass `store:` for compatibility or `engine:` when they need explicit routing.
- Workflow command methods currently persist command events; executing command bodies through the workflow owner and returning command results is target runtime work.
- Full durable workflow signals (`signal def`, `wait_condition`) are target work. Implemented today are lower-level timer waits, event waits, and event signaling.
- Durable-object commands persist command rows and execute inline in the current prototype. Per-object FIFO mailbox leasing, async `tell`, sleeps, and worker-driven object execution are target work.
- Fences deduplicate side effects after a fence row is inserted, but fence-owner crash recovery is not complete.
- The gRPC transport and workflow RPC routing are implemented for the prototype test matrix, but production mTLS/Spiffe policy, admin surfaces, metrics, tracing, and operator tooling are not yet implemented.
- There is no compatibility promise for production workloads yet. Treat the SQL schema, public names, and operational knobs as prototype surfaces unless the spec states otherwise.

Detailed guarantees live in [the spec](../spec.md), and the component overview lives in [the architecture doc](architecture.md).
