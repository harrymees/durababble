# Huginn integration validation report

## Summary

HAR-1330 used Huginn as an in-situ adoption target for Durababble. The validation port is local only, lives in the ignored `tmp/huginn` checkout, and was not opened as a Huginn pull request. The port replaces Huginn's agent `check` and `receive` enqueue path with Durababble durable-object commands keyed by `agent.id`, while leaving Huginn's own Rails models, events, scheduler, and long-running runner structure intact.

## Repository Evidence

- Target repository: `https://github.com/huginn/huginn.git`.
- Baseline branch and commit: `master` at `12187af9` (`Merge remote-tracking branch 'origin/improve-json-editor-ux'`).
- Local validation branch and port commits: `durababble-validation/har-1330` at `4d4ffc37` (`Adapt durable agent validation tests`), on top of `89104090` (`Port agent work to Durababble`).
- Local checkout path: `tmp/huginn` inside this Durababble workspace.
- No PR was opened against Huginn.

## Source Review

Huginn is a Rails 8.1 application where `Agent` subclasses own user-defined `check` and `receive` behavior, persist mutable `memory`, create `Event` rows, and connect through `Link` rows for propagation. `HuginnScheduler` uses Rufus scheduler ticks to enqueue `AgentRunScheduleJob`, `AgentPropagateJob`, and cleanup jobs through ActiveJob/DelayedJob. `AgentCheckJob` and `AgentReceiveJob` were the most direct durable execution candidates because they wrap the side-effecting agent method calls and update `last_check_at` / `last_receive_at`. `AgentRunner` and `LongRunnable` are the process integration point for workers that should run beside the existing DelayedJob and scheduler workers.

## Integration Points Identified

- Durable objects: one `Huginn::DurableAgent` per `Agent` id serializes `check` and `receive` commands for that agent.
- Durable commands: `Agent.async_check` and `Agent.async_receive` enqueue Durababble object commands instead of ActiveJob jobs in the validation branch.
- Steps / side-effect boundaries: each agent `check` or `receive` method remains the application side-effect boundary and is invoked from a persisted Durababble inbox command.
- Retries: `Huginn::DurableAgent` declares a retry policy for transient command failures; validation includes a receive command that fails once and then completes on retry.
- Idempotency fences: `async_receive` derives a stable idempotency key from the receiver id and sorted event ids so duplicate propagation enqueues reattach to the same Durababble inbox row.
- Waits/signals: Huginn's `DelayAgent#check` uses process sleep for `emit_interval`, and `SchedulerAgent` uses Rufus cron callbacks; both are natural future Durababble wait/sleep candidates, but they were outside the targeted port.
- Worker runtime: a `DurababbleWorker` integrates `Durababble::WorkerRuntime` into Huginn's `AgentRunner` so object inbox activations are drained by an application worker process.

## What Changed In The Huginn Port

- Added `gem 'durababble', path: '../..'` to the Huginn Gemfile for local validation against this Durababble checkout.
- Added `Huginn::DurableAgent < Durababble::DurableObject` with `check` and `receive` durable commands, persisted command counters, application log-on-failure behavior, and retry policy.
- Added `Huginn::DurababbleRuntime` to derive a Durababble database URL from ActiveRecord config or `DURABABBLE_DATABASE_URL`, select a schema/table prefix, enqueue agent commands, and construct workers/runtimes.
- Added `DurababbleWorker` and registered it through `lib/agent_runner.rb`.
- Updated `Agent.async_check` and `Agent.async_receive` to enqueue Durababble object commands with idempotency keys instead of ActiveJob jobs.
- Kept scheduled checks idempotent by agent/schedule/minute while allowing manual `async_check` calls to enqueue distinct durable commands unless an explicit idempotency key is supplied.
- Normalized durable object ids back to integer ActiveRecord ids before `Agent.find`.
- Added `spec/lib/durababble_agent_integration_spec.rb` covering persisted enqueue/drain, idempotent receive enqueue, and retry recovery, plus an experimental real-worker spec helper so existing model specs can drain Durababble instead of relying on DelayedJob inline execution.

## Baseline Tests

- Initial baseline setup command `mise exec -- bundle check` failed until Huginn's documented `.env` prerequisite existed; after copying `.env.example` to `.env`, `bundle check` passed.
- MySQL on `127.0.0.1:3306` rejected local root access, so the validation used the available isolated MySQL service at `127.0.0.1:13307` with `root/password`.
- Baseline broad command `DATABASE_ADAPTER=mysql2 DATABASE_HOST=127.0.0.1 DATABASE_PORT=13307 DATABASE_USERNAME=root DATABASE_PASSWORD=password TEST_DATABASE_NAME=huginn_har_1330_baseline_test RAILS_ENV=test mise exec -- bundle exec rake db:migrate spec:nofeatures` initially produced `1792 examples, 39 failures`; the dominant blocker was missing npm assets (`fontawesome`) and JavaScript URL polyfill output.
- After `npm ci` and `npm run build`, the JavaScriptAgent failures cleared, but the no-feature suite still failed with `1792 examples, 34 failures` because Rails reported `Asset application.css was not declared to be precompiled in production` from the baseline `app/assets/config/manifest.js` / Rails 8.1 asset setup.
- The baseline durable scheduler/job/model slice passed before the port with `DATABASE_ADAPTER=mysql2 DATABASE_HOST=127.0.0.1 DATABASE_PORT=13307 DATABASE_USERNAME=root DATABASE_PASSWORD=password TEST_DATABASE_NAME=huginn_har_1330_baseline_test RAILS_ENV=test mise exec -- bundle exec rspec spec/jobs spec/lib/huginn_scheduler_spec.rb spec/lib/agent_runner_spec.rb spec/lib/delayed_job_worker_spec.rb spec/models/agent_spec.rb`: `127 examples, 0 failures`.

## Ported Tests

- Ported Durababble integration command: `mise exec -- env DATABASE_ADAPTER=mysql2 DATABASE_HOST=127.0.0.1 DATABASE_PORT=13307 DATABASE_USERNAME=root DATABASE_PASSWORD=password TEST_DATABASE_NAME=huginn_har_1330_port_test RAILS_ENV=test DURABABBLE_DATABASE_URL=mysql2://root:password@127.0.0.1:13307/huginn_har_1330_port_test DURABABBLE_SCHEMA=huginn_har_1330_port_test bundle exec rspec spec/lib/durababble_agent_integration_spec.rb`.
- Ported Durababble integration result: `3 examples, 0 failures`.
- Ported scheduler/job/model slice command after adapting tests to drain real Durababble workers: `mise exec -- env DATABASE_ADAPTER=mysql2 DATABASE_HOST=127.0.0.1 DATABASE_PORT=13307 DATABASE_USERNAME=root DATABASE_PASSWORD=password TEST_DATABASE_NAME=huginn_har_1330_port_test RAILS_ENV=test DURABABBLE_DATABASE_URL=mysql2://root:password@127.0.0.1:13307/huginn_har_1330_port_test DURABABBLE_SCHEMA=huginn_har_1330_port_test bundle exec rspec spec/jobs spec/lib/huginn_scheduler_spec.rb spec/lib/agent_runner_spec.rb spec/lib/delayed_job_worker_spec.rb spec/models/agent_spec.rb spec/lib/durababble_agent_integration_spec.rb`.
- Ported scheduler/job/model slice result after those adaptations, re-run from this workspace before handoff: `130 examples, 8 failures`. This improved the first broad ported run but still shows unresolved test isolation and semantic rewrite work: some examples enqueue Durababble work inside Huginn's transactional fixtures and hit `SAVEPOINT active_record_1 does not exist`, while others still share durable inbox state across examples or assert inline DelayedJob method-call behavior.

## Durababble Findings

- No Durababble runtime correctness bug was found in the targeted persisted object command flow; MySQL-backed object inbox enqueue, idempotency-key dedupe, worker drain, retry, and object state persistence worked for the Huginn ported behavior.
- The integration surfaced an adoption caveat for Rails test suites: if the first Durababble enqueue performs MySQL table-prefix migrations inside RSpec transactional fixtures, MySQL DDL can invalidate ActiveRecord savepoints and produce `SAVEPOINT active_record_1 does not exist`. The passing integration spec disables transactional fixtures for Durababble worker-drain examples and migrates the Durababble namespace before assertions.
- The integration also surfaced an ergonomics gap: applications with existing inline ActiveJob test semantics need a documented pattern for draining Durababble workers in tests and rewriting expectations from "method ran inline" to "command was persisted and then drained."
- The integration surfaced a Huginn-port bug, not a Durababble bug: deriving manual check idempotency keys from the current minute deduped repeated manual checks. The validation branch now only derives that stable key for scheduled checks unless the caller passes an explicit key.

## Durababble Validation

- Targeted documentation validation passed with `mise exec -- env DURABABBLE_YUGABYTE_DATABASE_URL= bundle exec ruby -Ilib -Itest test/durababble/documentation_test.rb`: `3 runs, 32 assertions, 0 failures, 0 errors, 0 skips`.
- Workspace namespace probe passed with `mise exec -- bundle exec ruby -Ilib -e 'require "durababble"; puts Durababble.default_schema'`, which printed `durababble_har_1330_c4d3a929cbba`.
- Aggregate `mise exec -- bundle exec rake test` was attempted in this retry and terminated by `SignalException: SIGTERM` before Minitest produced a summary, after printing late-suite progress and several `E` markers. I did not count that aggregate run as green.
- No Durababble library code changed for this handoff; the repo change is the written integration report. The Huginn validation branch exercised Durababble's MySQL-backed durable-object enqueue, idempotency, worker-drain, retry, and state-persistence path through the ported app tests above.

## Remaining Risks

- The validation port does not yet cover `DelayAgent` process sleeps, Rufus `SchedulerAgent` cron jobs, long-running stream agents, web request agents, or external delivery outbox behavior.
- The broad Huginn no-feature suite is blocked in the unmodified baseline by a Rails asset manifest issue, so the validation relies on the passing scheduler/job/model baseline slice plus the passing targeted Durababble integration spec.
- The local port intentionally changes async semantics from DelayedJob inline execution in tests to Durababble persisted command drain; the validation branch adapts some touched model specs, but the broader slice still has 8 failures and a production-quality upstream port would need a wider test rewrite plus a dedicated Huginn test helper for durable worker draining and durable namespace cleanup.

## Suggestions

- Add a Durababble Rails testing guide showing how to migrate an isolated namespace before transactional fixtures, when to disable transactional tests for worker-drain examples, and how to write assertions against inbox rows and worker drain results.
- Consider a small `Durababble::Testing` helper that drains a registered worker until idle and reports pending/failed inbox rows, so Rails apps do not invent local helpers.
- Consider an explicit app-boot migration helper for embedded apps that want to run Durababble migrations at process start rather than on first enqueue.
- For a fuller Huginn port, move `DelayAgent` sleeps to durable waits, model `SchedulerAgent` cron fires as durable timer work, and add recovery tests for process shutdown while an agent command is running.
