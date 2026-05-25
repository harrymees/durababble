---
title: "Beyond Background Jobs"
weight: 15
---

# Beyond Background Jobs

Background jobs are still the right tool for simple, short, idempotent work: send one email, refresh one cache entry, enqueue one webhook delivery, or run a task where rerunning the whole job is acceptable.

Durababble is for the cases where a single retry loop becomes the hard part. If a job charges a card, writes a record, waits for a webhook, calls another service, and then ships an order, a crash in the middle forces you to rebuild durable progress tracking yourself. You end up adding status columns, idempotency keys, retry schedules, leases, recovery scans, cancellation flags, and custom "what step was I on?" logic.

Durababble makes those pieces part of the programming model. Workflows persist step history and resume from durable boundaries. Durable objects keep id-addressed state behind query and command methods. RPC-style handles let other code ask durable entities for status or send durable commands without reaching into worker memory. The goal is not to replace every job; it is to make the stateful, multi-step, long-lived jobs explicit and recoverable.
