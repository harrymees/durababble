---
title: "Comparisons"
weight: 80
---

# Comparisons

Durababble overlaps with three different kinds of systems: Ruby background job servers, big durable execution platforms like Temporal, and minimalist database-resident projects like Absurd. This page sketches where Durababble fits relative to each.

## Beyond Background Jobs

Background jobs are still the right tool for simple, short, idempotent work: send one email, refresh one cache entry, enqueue one webhook delivery, or run a task where rerunning the whole job is acceptable.

Durababble is for the cases where a single retry loop becomes the hard part. If a job charges a card, writes a record, waits for a webhook, calls another service, and then ships an order, a crash in the middle forces you to rebuild durable progress tracking yourself. You end up adding status columns, idempotency keys, retry schedules, leases, recovery scans, cancellation flags, and custom "what step was I on?" logic.

Durababble makes those pieces part of the programming model. Workflows persist step history and resume from durable boundaries. Durable objects keep id-addressed state behind query and command methods. RPC-style handles let other code ask durable entities for status or send durable commands without reaching into worker memory. The goal is not to replace every job; it is to make the stateful, multi-step, long-lived jobs explicit and recoverable.

## Versus Ruby Job Servers (Sidekiq, GoodJob, SolidQueue, Resque)

Ruby job servers are excellent at what they are designed for: enqueue a unit of work, run it once on a worker, retry the whole unit on failure. They typically share Durababble's "use your existing database" property — GoodJob and SolidQueue store jobs in your app database, Sidekiq uses Redis — and they handle the dispatch, queue, and worker concerns well.

Where the model breaks is in jobs that are not really one unit of work:

- **Multi-step progress.** A six-step fulfillment job that crashes after step three has to either redo the first three steps or carry ad-hoc `status` columns to know what to skip. Durababble persists each `step` boundary, so replay reuses completed step results from the database.
- **Waits.** Pausing a job for a day usually becomes a chain of "re-enqueue myself later" jobs plus extra rows to record where you were. Durababble has first-class durable timer waits that release worker resources while the workflow is parked, and durable workflow commands let other processes wake a workflow on demand.
- **Cooperative cancellation.** Cancelling a multi-step background job mid-flight typically means a flag column plus per-job checks; cleanup is something you write yourself. Durababble cancels through a handle, raises `CancellationError` into the workflow at the next durable boundary, and runs cleanup as ordinary durable steps that themselves survive crashes.
- **Long-lived addressable state.** Job servers do not have an analogue of durable objects: an entity addressed by id (`acct_123`, `cart_456`) that other parts of the system can query and command. People emulate this with database rows plus jobs that mutate them, but locking, ordering, and inbox semantics are left to the caller.
- **In-flight RPC.** Asking a running job "what is your status?" or "apply this update" usually requires writing to a side table and waiting for the worker to notice. Durababble routes simple RPCs and durable commands to the worker that currently owns the workflow or object lease.

A reasonable rule of thumb: if a unit of work is short enough that "restart from scratch on failure" is acceptable, a job server is the right tool. If you keep wishing you could checkpoint inside a job, address it by id from elsewhere, or pause it for a day, Durababble exists for that. The two coexist happily — most apps that use Durababble still have a Sidekiq or SolidQueue for the cheap, single-shot work.

## Versus Temporal

Temporal is the canonical durable execution platform: Temporal Server, a worker fleet, multi-language SDKs, deterministic replay, signals, child workflows, schedules, and a dashboard. It is the right answer for organizations that want a durable execution product, not a library.

Durababble takes the same core idea — replay completed steps from durable history, persist waits and retries — and trims it to fit inside an existing Rails-or-similar Ruby app:

- **No separate server.** Durababble is a gem. The durable state lives in tables you already operate (MySQL/MariaDB or Postgres/Yugabyte). There is no Temporal Server to deploy, scale, upgrade, or back up separately.
- **Ruby-shaped API.** Workflows and durable objects are ordinary Ruby classes, steps are method-decorated checkpoints, and `step_context.idempotency_key` is what you pass to external APIs. Temporal's Ruby story is comparatively thin; Durababble is Ruby-native.
- **Replay model.** Both rely on deterministic execution around step boundaries, but Durababble's SDK is far smaller and intentionally limited to the primitives we use. Temporal's SDKs are large because they implement a much larger feature surface.
- **Durable objects.** Durababble exposes long-lived id-addressed entities as a first-class primitive alongside workflows. Temporal's idiomatic equivalent is an "entity workflow" — a never-ending workflow used as state — which works but has different ergonomics.
- **Operational surface.** Temporal gives you a control plane, history retention policies, namespaces, cross-cluster replication, schedules, and detailed UI tooling out of the box. Durababble does not; if you need those, Temporal is the better fit. Durababble emits OpenTelemetry spans and metrics into whatever your app already uses.

If you have a polyglot fleet, a dedicated platform team, or workloads that must survive years of history, Temporal is what it sounds like. If the work lives inside one Ruby app and you want durable execution without a second platform to operate, Durababble aims at that point.

## Versus Absurd

[Absurd](https://earendil-works.github.io/absurd/) is the closest sibling in spirit: durable execution implemented directly on top of a relational database, with the goal of "just use Postgres." Absurd ships as a single SQL schema with stored procedures, and provides thin SDKs in TypeScript, Python, and Go.

Durababble shares Absurd's "no extra service to run" stance, and the primitives overlap closely:

- Both have tasks/workflows with explicit step checkpoints whose results are persisted.
- Both have sleeps as a durable primitive.
- Both have pull-based workers that claim work under leases.
- Both treat the database schema as the orchestration layer.

The differences come from where the orchestration lives and what is exposed on top:

- **Where the engine runs.** Absurd pushes orchestration into Postgres stored procedures; the SDKs are thin clients on top of that schema. Durababble's engine, retry policy, replay, and RPC routing live in Ruby and use ActiveRecord for SQL access. Absurd's choice keeps the SDK small and language-agnostic; Durababble's choice gives you ordinary Ruby code for workflow control flow and a Ruby-idiomatic public API.
- **Database support.** Absurd is Postgres-only by design. Durababble runs on MySQL/MariaDB (the most heavily tested backend), PostgreSQL, and YugabyteDB. Apps that already use MySQL can adopt Durababble without adding Postgres.
- **Language scope.** Absurd targets TypeScript, Python, and Go. Durababble is Ruby-only and embraces Ruby idioms (`step def`, `expose`, `expose_command`, async-gem-friendly parallelism).
- **Primitive set.** Absurd's primitives are tasks, steps, sleep, and events. Durababble's primitives are workflows, steps, timer waits, and first-class durable objects (long-lived id-addressed entities with commands and queries), plus workflow RPC (`expose`/`expose_command`), cooperative cancellation with replay-safe cleanup, side-effect fences, and a durable outbox. Durababble does not currently expose an event-wait primitive; equivalent "wake when X happens" flows are expressed as durable commands from the signaling process to the waiting workflow or object.
- **Replay model.** Both persist step return values and skip completed steps on resume, so neither requires Temporal-style deterministic replay of all branch logic. Durababble does still require deterministic ordering around step calls and raises `NonDeterminismError` on divergence.

Pick Absurd if you want the smallest possible Postgres-only schema and SDKs across multiple languages. Pick Durababble if you are a Ruby shop, want first-class durable objects alongside workflows, or need MySQL/MariaDB support.
