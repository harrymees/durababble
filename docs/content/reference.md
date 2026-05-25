---
title: "Reference"
weight: 60
---

# Reference

This reference summarizes the implemented prototype. The spec records the intended public direction so reviewers can distinguish current behavior from target behavior.

## Implemented Surface

- Class-oriented workflow API with `#execute`, `step def`, retry policy, step idempotency keys, class-method enqueueing, `Workflow.start` / `Workflow.at` / `Workflow.handle` aliases, and optional `engine:` overrides.
- First-class cooperative workflow cancellation through `Workflow.handle(...).cancel(reason:)`, persisted cancellation requests, `canceling` / `canceled` states, and replay-safe cleanup steps.
- Class-oriented durable object API with typed `at` / `ref` handles, `expose`, `expose_command`, command idempotency keys, and explicit state updates.
- PostgreSQL/YSQL and MySQL/MariaDB store implementations.
- Durable workflow, step, wait, attempt, fence, outbox, durable-object, and durable-object-command persistence.
- Worker polling with leased workflow claims.
- Heartbeats, stale lease recovery, and lease-aware resume.
- Timer waits, side-effect fences, and durable outbox primitives.
- Retry due-time claims distinguish retryable failures from terminal failed workflows.
- Lease-routed workflow RPC helpers.
- Deterministic simulation tests for workflow safety and crash-recovery scenarios.

## Prototype Boundaries

- `Durababble.configure` installs a default store and default engine for top-level workflow and object helpers. Callers can still pass `store:` for compatibility or `engine:` when they need explicit routing.
- Workflow command methods currently persist durable inbox rows, execute command bodies through the workflow owner, and return command results to synchronous callers.
- Workflow `wait_condition` is implemented as a timer-backed durable wait. Broader broadcast-style delivery concepts are intentionally out of scope.
- Durable-object commands persist inbox rows and execute through object workers with per-object FIFO activation.
- Fences deduplicate side effects after a fence row is inserted, but fence-owner crash recovery is not complete.
- The gRPC transport and workflow RPC routing are implemented for the prototype test matrix, but production mTLS/Spiffe policy, admin surfaces, metrics, tracing, and operator tooling are not yet implemented.
- There is no compatibility promise for production workloads yet. Treat the SQL schema, public names, and operational knobs as prototype surfaces unless the spec states otherwise.

Detailed guarantees live in [the spec](../spec.md), and the component overview lives in [the architecture doc](architecture.md).
