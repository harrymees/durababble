---
title: "Query Perf"
weight: 10
---

# Query Perf

The docs publishing path regenerates these MySQL hot-path reports before Hugo builds the site. Each report captures the SQL timeline, transaction context, callsites, EXPLAIN output, and touched table DDL for one registered query reporter scenario.

## Reports

- [Enqueue workflow](/query-perf/enqueue_workflow.html) traces the durable write for a new pending workflow.
- [Claim runnable workflow](/query-perf/claim_runnable_workflow.html) traces the queue probes and lease update used by worker claims.
- [Claim target activation](/query-perf/claim_target_activation.html) traces the queue probes and lease update used by mailbox wakeup claims.
- [Worker poll idle](/query-perf/worker_poll_idle.html) traces one worker tick when no matching workflow work is available.
- [Worker tick claim](/query-perf/worker_tick_claim.html) traces one worker tick that claims and completes a workflow.
