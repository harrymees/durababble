# Huginn Durababble Integration Report

HAR-1264 used Huginn as an in-situ adoption target for Durababble. The target app was kept in the local validation checkout under `validation-targets/huginn`; no Huginn PR was opened.

## Target Baseline

- Repository: `https://github.com/huginn/huginn`
- Default branch: `master`
- Baseline commit: `12187af9ed0d49acf2798eb9b6f31807d6b59f4e`
- Local validation branch: `har-1264-durababble-validation`
- Local port commits:
  - `483938c4` (`Validate Huginn agent execution with Durababble`)
  - `5f714562` (`Exercise Durababble through Huginn test paths`)

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
- Retries: production step retries use `maximum_attempts: 5` and a short schedule compatible with the old DelayedJob retry intent. Test-mode inline execution uses one attempt to preserve Huginn's previous `Delayed::Worker.delay_jobs = false` behavior.
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
- Test-mode Durababble inline draining so existing Huginn specs exercise the durable workflow path instead of bypassing it through DelayedJob.
- Specs covering durable enqueue/run, crash-before-step-body recovery, propagation-to-receive execution, and propagation queue checks against Durababble workflow rows.

## Baseline Test Signal

A Docker MySQL service was started for Huginn on `127.0.0.1:13306` using `mysql:latest` with root password `password`. A detached baseline worktree was created at `validation-targets/huginn-baseline` from commit `12187af9ed0d49acf2798eb9b6f31807d6b59f4e`.

Baseline setup commands:

```sh
env APP_SECRET_TOKEN=har1264localtestsecret DATABASE_ADAPTER=mysql2 DATABASE_HOST=127.0.0.1 DATABASE_PORT=13306 DATABASE_USERNAME=root DATABASE_PASSWORD=password DATABASE_NAME=huginn_har1264_baseline_development TEST_DATABASE_NAME=huginn_har1264_baseline_test CI=true mise exec -- bundle check
env APP_SECRET_TOKEN=har1264localtestsecret DATABASE_ADAPTER=mysql2 DATABASE_HOST=127.0.0.1 DATABASE_PORT=13306 DATABASE_USERNAME=root DATABASE_PASSWORD=password DATABASE_NAME=huginn_har1264_baseline_development TEST_DATABASE_NAME=huginn_har1264_baseline_test CI=true mise exec -- npm install
env APP_SECRET_TOKEN=har1264localtestsecret DATABASE_ADAPTER=mysql2 DATABASE_HOST=127.0.0.1 DATABASE_PORT=13306 DATABASE_USERNAME=root DATABASE_PASSWORD=password DATABASE_NAME=huginn_har1264_baseline_development TEST_DATABASE_NAME=huginn_har1264_baseline_test CI=true mise exec -- bundle exec rake db:drop db:create db:migrate
```

Baseline suite command:

```sh
env APP_SECRET_TOKEN=har1264localtestsecret DATABASE_ADAPTER=mysql2 DATABASE_HOST=127.0.0.1 DATABASE_PORT=13306 DATABASE_USERNAME=root DATABASE_PASSWORD=password DATABASE_NAME=huginn_har1264_baseline_development TEST_DATABASE_NAME=huginn_har1264_baseline_test CI=true RAILS_ENV=test RSPEC_RETRY_RETRY_COUNT=0 mise exec -- bundle exec rake
```

Result: `1831 examples, 0 failures` in 52.84s. This replaces the earlier Yugabyte-based attempt, which was invalid for the acceptance bar because it never established Huginn's baseline suite on its normal MySQL path.

## Port Validation

The port was rerun against the same Docker MySQL service, with separate Huginn databases and a Durababble MySQL store/table prefix:

```sh
env APP_SECRET_TOKEN=har1264localtestsecret DATABASE_ADAPTER=mysql2 DATABASE_HOST=127.0.0.1 DATABASE_PORT=13306 DATABASE_USERNAME=root DATABASE_PASSWORD=password DATABASE_NAME=huginn_har1264_port_development TEST_DATABASE_NAME=huginn_har1264_port_test HUGINN_DURABABBLE_DATABASE_URL=mysql://root:password@127.0.0.1:13306/huginn_har1264_port_test HUGINN_DURABABBLE_SCHEMA=huginn_har1264_port_durababble_full CI=true RAILS_ENV=test RSPEC_RETRY_RETRY_COUNT=0 mise exec -- bundle exec rake
```

Result: `1834 examples, 0 failures` in 46.32s.

Focused regression command:

```sh
env APP_SECRET_TOKEN=har1264localtestsecret DATABASE_ADAPTER=mysql2 DATABASE_HOST=127.0.0.1 DATABASE_PORT=13306 DATABASE_USERNAME=root DATABASE_PASSWORD=password DATABASE_NAME=huginn_har1264_port_development TEST_DATABASE_NAME=huginn_har1264_port_test HUGINN_DURABABBLE_DATABASE_URL=mysql://root:password@127.0.0.1:13306/huginn_har1264_port_test HUGINN_DURABABBLE_SCHEMA=huginn_har1264_port_durababble CI=true RAILS_ENV=test RSPEC_RETRY_RETRY_COUNT=0 mise exec -- bundle exec rspec spec/models/agent_spec.rb:250 spec/models/agent_spec.rb:263 spec/models/agent_spec.rb:290 spec/models/agent_spec.rb:327 spec/jobs/agent_propagate_job_spec.rb spec/lib/durababble_agent_workflows_spec.rb --format documentation
```

Result: 11 examples, 0 failures in 0.53s.

Coverage:

- Existing `Agent.async_check`, `Agent.receive!`, scheduler, propagation, and agent receive specs now exercise Durababble-backed job wrappers under Huginn's normal call sites.
- `Agent.async_check` no longer creates a DelayedJob row; it durably enqueues a Durababble workflow and test-mode inline draining updates `last_check_at` through a worker tick.
- A crash injected after `record_step_started` leaves a durable incomplete step; after lease release, the worker retries, completes the workflow, and records attempt statuses `failed, completed`.
- Propagation enqueues a durable propagation workflow, advances the receiver cursor, enqueues a durable receive workflow, and creates the formatted downstream event through the same path the existing Huginn suite calls.
- `AgentPropagateJob.can_enqueue?` now checks pending/running/waiting Durababble workflow rows instead of DelayedJob rows.

## Durababble Findings

The integration exposed one Durababble correctness bug: terminal `failed` workflows with `next_run_at = NULL` were still considered runnable by `claim_runnable_workflow` and `claim_workflow`, so a permanently failed workflow could be reclaimed repeatedly. The Huginn test-mode inline path surfaced this as repeated failed Agent executions.

Durababble fix:

- `claim_runnable_workflow` and `claim_workflow` now only treat `failed` rows as retryable when `next_run_at` is non-null and due.
- Terminal failures still clear `next_run_at`, so they remain terminal.
- Backend conformance coverage now asserts terminal failed workflows are not claimable.
- Queue-order, engine-resume, deterministic simulation, MySQL query-plan, and PostgreSQL/YSQL query-plan fixtures were updated for the stricter failed-retry predicate.
- `README.md`, `docs/spec.md`, and `docs/architecture.md` now document the terminal-failure guarantee.

Durababble validation:

```sh
mise exec -- env DURABABBLE_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_MYSQL_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_YUGABYTE_DATABASE_URL= bundle exec ruby -Ilib -Itest test/durababble/store_queue_correctness_test.rb
mise exec -- env DURABABBLE_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_MYSQL_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_YUGABYTE_DATABASE_URL= bundle exec ruby -Ilib -Itest test/durababble/engine_test.rb
mise exec -- env DURABABBLE_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_MYSQL_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_YUGABYTE_DATABASE_URL= bundle exec ruby -Ilib -Itest test/durababble/store_backend_conformance_test.rb
mise exec -- env DURABABBLE_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_MYSQL_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_YUGABYTE_DATABASE_URL= bundle exec ruby -Ilib -Itest test/durababble/mysql_query_plan_test.rb
mise exec -- env DURABABBLE_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_MYSQL_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_YUGABYTE_DATABASE_URL=postgresql://yugabyte@127.0.0.1:15433/yugabyte bundle exec ruby -Ilib -Itest test/durababble/deterministic_test.rb
mise exec -- env DURABABBLE_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_MYSQL_DATABASE_URL=mysql://root:password@127.0.0.1:13307/sidekick_server_test DURABABBLE_YUGABYTE_DATABASE_URL=postgresql://yugabyte@127.0.0.1:15433/yugabyte bundle exec rake test
```

Results:

- Queue correctness: `7 runs, 44 assertions, 0 failures, 0 errors, 0 skips`.
- Engine: `2 runs, 10 assertions, 0 failures, 0 errors, 0 skips`.
- Backend conformance: `10 runs, 171 assertions, 0 failures, 0 errors, 0 skips`.
- MySQL query plan: `1 runs, 28 assertions, 0 failures, 0 errors, 0 skips`.
- Deterministic simulation: `18 runs, 239 assertions, 0 failures, 0 errors, 0 skips`.
- Full suite with MySQL default and Yugabyte enabled: `196 runs, 1547 assertions, 0 failures, 0 errors, 1 skips`.

The Durababble validation used a separate `mysql:8.0` Docker service on `127.0.0.1:13307` because Trilogy cannot authenticate to `mysql:latest`'s default `caching_sha2_password` mode without TLS/socket support.

The integration did expose adoption friction:

- Rails apps need a small official integration layer for configuration, migrations, and worker lifecycle.
- ActiveJob/DelayedJob users would benefit from an adapter or migration pattern rather than hand-written wrapper jobs.
- Public idempotent workflow start keys would make app-level enqueue deduplication cleaner than querying workflow rows by name.
- Durable objects are promising for per-agent serialization, but the object mailbox/runtime needs to mature before it is honest to port Huginn agents as durable objects.
- Store migrations are noisy and slow on Yugabyte when repeatedly run in app boot/spec setup.

## Remaining Risks

- The port validates Huginn's existing suite and agent execution path, but it remains a validation branch rather than a production-quality Huginn migration.
- The port uses workflows over existing ActiveRecord state; it does not make Huginn agent state itself Paquito-serialized Durababble object state.
- Duplicate enqueue prevention is only implemented for propagation workflow checks in this validation.
- Test-mode inline draining preserves Huginn's previous DelayedJob test semantics, while production still depends on `DurababbleAgentWorker` running under `AgentRunner`.

## Suggestions

- Add a Durababble Rails integration guide covering `Durababble.configure`, schema selection, migrations, and `WorkerRuntime` shutdown hooks.
- Provide a first-class ActiveJob adapter or a documented bridge from ActiveJob jobs to Durababble workflows.
- Add idempotent `Workflow.start` with caller-supplied keys before recommending queue replacement in Rails apps.
- Harden durable-object mailbox execution before positioning durable objects as a replacement for app-level actor rows such as Huginn agents.
- Add an optional quiet/idempotent migration mode to reduce startup/spec noise.
