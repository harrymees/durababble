# Chatwoot Integration Report

## Scope

HAR-1331 used chatwoot as an in-situ validation target for Durababble adoption. The chatwoot work remains local under `tmp/chatwoot`; no chatwoot PR was opened.

## Baseline

- Repository URL: `https://github.com/chatwoot/chatwoot.git`
- Baseline branch: `develop`
- Baseline commit: `c4a6a19e9be899c96fd2c1cbb3454b56b7ef76fc`
- Local validation branch: `har-1331-durababble-validation`
- Local port commit: `010109c Validate agent bot webhooks with Durababble`

## Architecture Review

- Chatwoot runs asynchronous work through Rails ActiveJob with Sidekiq in development, staging, and production; `config/sidekiq.yml` defines priority queues from `critical` through housekeeping queues and a default retry budget of 3.
- Scheduled work is loaded from `config/schedule.yml` through sidekiq-cron, including recurring scheduled item triggers, IMAP fetches, account deletion, assignment, notification cleanup, and housekeeping.
- Durable candidates are concentrated in `app/jobs`, controller/listener `perform_later` call sites, `MutexApplicationJob`, webhook event jobs, notification and conversation jobs, campaign scheduling, auto-assignment, inbound channel processing, and outbound integration hooks.
- `MutexApplicationJob` uses Redis locks for per-resource mutual exclusion and ActiveJob `retry_on` for lock contention, which maps naturally to durable object commands or workflow steps with persisted retries and lease fencing.
- Webhook delivery is split between the generic `WebhookJob` and `AgentBots::WebhookJob`; `Webhooks::Trigger` performs the HTTP request, treats agent bot 429/500 responses as retryable, and updates conversation or message state when failure is terminal.
- The selected validation target was agent bot webhook delivery because it has real retry/failure behavior, persisted application side effects on terminal failure, a focused existing spec, and a small enough blast radius for an isolated integration.

## Port

- Added `gem 'durababble', path: '../..'` to chatwoot's local Gemfile so the checkout used the Durababble repository under test.
- Added `DurababbleChatwoot`, which builds a Durababble store from `ApplicationRecord.connection_pool`, migrates a `chatwoot_durababble` namespace, starts an optional worker runtime via `DURABABBLE_WEBHOOK_WORKER`, and exposes a one-shot worker helper for specs.
- Replaced `AgentBots::WebhookJob` inline ActiveJob retry behavior with durable workflow enqueueing.
- Added `AgentBots::WebhookDeliveryWorkflow` with a durable `deliver` step, three persisted attempts, scheduled retry waits, and a terminal `handle_failure` step that calls chatwoot's existing `Webhooks::Trigger#handle_failure` path.
- Extended `spec/jobs/agent_bots/webhook_job_spec.rb` to prove the job persists a workflow, the worker executes delivery, retry state is stored across worker ticks, and terminal retry exhaustion invokes the application failure handler exactly once.

## Chatwoot Validation

- Bootstrap used Ruby `3.4.4` and Bundler `2.5.16` via `mise x ruby@3.4.4`; local Postgres and Redis containers were started for chatwoot tests.
- Baseline database preparation passed: `mise x ruby@3.4.4 -- bundle _2.5.16_ exec rails db:prepare`.
- Baseline focused tests passed before the port: `mise x ruby@3.4.4 -- bundle _2.5.16_ exec rspec spec/jobs/agent_bots/webhook_job_spec.rb spec/jobs/webhook_job_spec.rb spec/lib/webhooks/trigger_spec.rb` -> `27 examples, 0 failures`.
- Ported focused tests passed after the Durababble integration: `mise x ruby@3.4.4 -- bundle _2.5.16_ exec rspec spec/jobs/agent_bots/webhook_job_spec.rb spec/jobs/webhook_job_spec.rb spec/lib/webhooks/trigger_spec.rb` -> `28 examples, 0 failures`.
- Full chatwoot suite validation was attempted with `mise x ruby@3.4.4 -- bundle _2.5.16_ exec rspec`; it did not produce a reliable application result because the local run spawned many `vite build --mode test` processes, stopped emitting useful progress for more than 30 minutes, and later produced Vite IO errors after termination. The focused backend path above is the reliable validation evidence for this ticket.

## Durababble Bugs Found And Fixed

- `Durababble::Store.from_active_record(connection_pool:)` assumed `connection_pool.lease_connection`, which is not available in chatwoot's ActiveRecord 7.1 pool. Durababble now uses `lease_connection` when present and falls back to `connection_pool.connection`.
- `Durababble::PostgresStore#execute_params` assumed ActiveRecord result objects expose `affected_rows`, which chatwoot's ActiveRecord 7.1 PostgreSQL path did not. Durababble now uses the raw PG connection's `exec_params` when available and wraps the PG result so store code can keep using `affected_rows`.
- The repo's Sorbet gate scanned the required local `tmp/chatwoot` checkout until `tmp/` was added to `sorbet/config`; this keeps ignored validation workspaces from polluting Durababble typechecking.
- Added Durababble regressions for ActiveRecord pools without `lease_connection` and PostgreSQL raw results with `cmd_tuples`.

## Durababble Validation

- Targeted regression passed: `mise exec -- bundle exec ruby -I lib -I test test/durababble/store_test.rb --name '/active_record_connections|affected_rows/'` -> `2 runs, 15 assertions, 0 failures, 0 errors`.
- Default workspace full test command attempted against `DURABABBLE_DATABASE_URL=postgresql://yugabyte@127.0.0.1:15433/yugabyte`: `mise exec -- bundle exec rake test` -> aborted with `SignalException: SIGTERM` before a Minitest failure summary, matching earlier unstable Yugabyte workspace behavior.
- Full suite passed on the available MySQL CI-style backend with optional Yugabyte tests disabled: `mise exec -- env DURABABBLE_DATABASE_URL=mysql://root@127.0.0.1:13308/sidekick_server_test DURABABBLE_YUGABYTE_DATABASE_URL= bundle exec rake test` -> `280 runs, 1989 assertions, 0 failures, 0 errors, 4 skips`.
- Lint/typecheck passed after excluding ignored `tmp/` validation workspaces from Sorbet: `mise exec -- bundle exec rake lint`.

## Remaining Risks

- The local chatwoot port validates one high-value durable path, not every candidate async behavior in chatwoot.
- A production chatwoot integration would need deployment decisions for worker process ownership, namespace migration timing, metrics, and how broadly to replace Sidekiq retry semantics.
- The default Yugabyte workspace run remained unstable under the local long-running test command, so PostgreSQL-compatible confidence comes from the chatwoot focused port and targeted Durababble regression rather than a completed full Yugabyte Durababble suite in this session.

## Suggestions

- Add a Rails integration guide that shows `Store.from_active_record(connection_pool: ApplicationRecord.connection_pool)`, namespace selection, migration timing, and worker runtime boot patterns.
- Keep compatibility tests for older ActiveRecord result and connection-pool APIs because real Rails applications lag the latest ActiveRecord API surface.
- Add a small Rails dummy app fixture or appraisal-style integration smoke to catch ActiveRecord pool/result compatibility without requiring a large external app checkout.
- Consider a workflow helper for the common "enqueue from ActiveJob, then let a Durababble worker own retries" migration path so adopters can incrementally replace Sidekiq retry behavior.
