# Huginn Durababble Integration Report

HAR-1264 used Huginn as an in-situ adoption target for Durababble. The target app was kept in the local validation checkout under `validation-targets/huginn`; no Huginn PR was opened.

## Target Baseline

- Repository: `https://github.com/huginn/huginn`
- Default branch: `master`
- Baseline commit: `12187af9ed0d49acf2798eb9b6f31807d6b59f4e`
- Local validation branch: `har-1264-durababble-validation`
- Local port commit: `483938c4` (`Validate Huginn agent execution with Durababble`)

The ticket text said `huggit`, but a later Linear comment corrected the target to `huggin`, described as the open-source Rails app. That was resolved to `huginn/huginn`.

## Source Architecture Review

Huginn's durable behavior is concentrated around `Agent`, `Event`, ActiveJob jobs, and the long-running runner:

- `Agent` is the core persisted unit. It owns options, memory, schedule, logs, event counters, links to source/receiver agents, and cursor fields such as `last_checked_event_id`.
- Scheduled checks flow through `Agent.run_schedule`, `Agent.bulk_check`, and `Agent.async_check`.
- Event propagation flows through `Agent.receive!`, which finds receiver/source/event pairs, advances `last_checked_event_id`, and enqueues receiver work.
- Worker execution previously used `AgentCheckJob`, `AgentReceiveJob`, `AgentRunScheduleJob`, `AgentPropagateJob`, `AgentCleanupExpiredJob`, and `AgentReemitJob` through ActiveJob/DelayedJob.
- `HuginnScheduler` uses Rufus to enqueue schedule, propagation, cleanup, and SchedulerAgent control work.
- `AgentRunner` supervises long-running workers, including `HuginnScheduler` and `DelayedJobWorker`.

## Adoption Points

- Workflows: agent check, agent receive, run schedule, propagate events, cleanup expired events, and reemit events.
- Steps: each workflow uses a single durable step for the side-effecting operation, with retry policy preserving retry history outside ActiveJob.
- Retries: step retries use `maximum_attempts: 5` and a short schedule compatible with the old DelayedJob retry intent.
- Worker runtime: `DurababbleAgentWorker` embeds `Durababble::WorkerRuntime` in Huginn's existing `AgentRunner`.
- Idempotency fences: good future fit for external API agents and event creation, but not added in this validation because Huginn agents vary widely and require per-agent idempotency keys.
- Waits/signals: scheduler cron and webhook-triggered work are natural future `wait_until` / event signal candidates. The validation kept Rufus as the trigger source and moved the enqueued work to Durababble.
- Durable objects: each Huginn agent is a natural durable-object identity, but Durababble's durable-object mailbox is still transitional. The validation used workflows around existing ActiveRecord state rather than pretending per-agent object serialization is complete.

## What Changed In Huginn

The local Huginn port commit adds Durababble as a path gem and introduces:

- `lib/huginn_durababble.rb` for store, schema, worker, and enqueue helpers.
- `lib/durable_agent_workflows.rb` with workflows for check, receive, schedule, propagation, cleanup, and reemit.
- `lib/durababble_agent_worker.rb` to start `Durababble::WorkerRuntime` under `AgentRunner`.
- Job wrappers that keep existing class names/call sites but enqueue Durababble workflows.
- A focused integration spec covering durable enqueue/run, crash-before-step-body recovery, and propagation-to-receive execution.

## Baseline Test Signal

Local MySQL was not usable for Huginn tests: `mysqladmin --host=127.0.0.1 --port=3306 --user=root ping` and socket/TCP probes failed with `Access denied for user 'root'@'localhost'`. PostgreSQL on port 5432 was not listening. The only local PostgreSQL-compatible endpoint was the workspace Yugabyte endpoint.

After creating ignored Huginn `.env` and `.env.local` files, installing `jq`, running `bundle check`, and preparing `huginn_har_1264_test`, the baseline command:

```sh
RAILS_ENV=test RSPEC_RETRY_RETRY_COUNT=0 mise exec -- bundle exec rspec
```

was interrupted after 4m13s once the failure mode was clear: 111 examples had run and all 111 failed. The failures were fixture transaction failures on Yugabyte, for example `PG::TRSerializationFailure` with `current transaction is expired or aborted` and repeated `WARNING: there is no transaction in progress`.

## Port Validation

The focused port validation avoids Huginn's global fixture loader and creates only the rows needed for the durable path:

```sh
RAILS_ENV=test RSPEC_RETRY_RETRY_COUNT=0 mise exec -- bundle exec rspec spec/lib/durababble_agent_workflows_spec.rb --format documentation
```

Result: 3 examples, 0 failures in 3m18.8s.

Coverage:

- `Agent.async_check` no longer creates a DelayedJob row; it durably enqueues a Durababble workflow and a worker tick updates `last_check_at`.
- A crash injected after `record_step_started` leaves a durable incomplete step; after lease release, the worker retries, completes the workflow, and records attempt statuses `failed, completed`.
- Propagation enqueues a durable propagation workflow, advances the receiver cursor, enqueues a durable receive workflow, and creates the formatted downstream event.

## Durababble Findings

No Durababble correctness bug was found. The crash/recovery path used by the Huginn port behaved as designed.

The integration did expose adoption friction:

- Rails apps need a small official integration layer for configuration, migrations, and worker lifecycle.
- ActiveJob/DelayedJob users would benefit from an adapter or migration pattern rather than hand-written wrapper jobs.
- Public idempotent workflow start keys would make app-level enqueue deduplication cleaner than querying workflow rows by name.
- Durable objects are promising for per-agent serialization, but the object mailbox/runtime needs to mature before it is honest to port Huginn agents as durable objects.
- Store migrations are noisy and slow on Yugabyte when repeatedly run in app boot/spec setup.

## Remaining Risks

- The port validates the agent execution path, not every Huginn agent class.
- Full Huginn suite validation remains blocked on the local database mismatch.
- The port uses workflows over existing ActiveRecord state; it does not make Huginn agent state itself Paquito-serialized Durababble object state.
- Duplicate enqueue prevention is only implemented for propagation workflow checks in this validation.

## Suggestions

- Add a Durababble Rails integration guide covering `Durababble.configure`, schema selection, migrations, and `WorkerRuntime` shutdown hooks.
- Provide a first-class ActiveJob adapter or a documented bridge from ActiveJob jobs to Durababble workflows.
- Add idempotent `Workflow.start` with caller-supplied keys before recommending queue replacement in Rails apps.
- Harden durable-object mailbox execution before positioning durable objects as a replacement for app-level actor rows such as Huginn agents.
- Add an optional quiet/idempotent migration mode to reduce startup/spec noise.
