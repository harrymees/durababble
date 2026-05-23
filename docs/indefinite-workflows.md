# Bounded indefinite workflows

Status: research and target proposal. This is not implemented yet.

Durababble's current workflow rows are durable and recoverable, but they are run-shaped: one `workflows` row owns a deterministic step sequence, waits, attempts, and eventual terminal state. That is a good fit for finite orchestration. It is a poor fit for code such as "sync this shop forever" because the step and attempt history grows without an obvious developer-level boundary.

The goal is to bound replay and storage growth without making application authors learn Temporal's Continue-As-New ceremony.

## Sources

Research used public docs plus repository metadata. No third-party repository was cloned into this workspace.

| System | Primary docs reviewed | Repository source checked |
| --- | --- | --- |
| Temporal | [Continue-As-New in Go](https://docs.temporal.io/develop/go/continue-as-new), [Workflow execution events](https://docs.temporal.io/workflow-execution/event) | `temporalio/temporal` HEAD `4f2da02c676304b352a7461b9cffa126aa827ef2` |
| Cadence | [Cadence workflows](https://cadenceworkflow.io/docs/concepts/workflows), [Go client workflow APIs](https://pkg.go.dev/go.uber.org/cadence/workflow) | `cadence-workflow/cadence` HEAD `f3f8cd2dffc3525bc0d648d172cf84eb64fdcc2d` |
| Azure Durable Functions | [Eternal orchestrations](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-eternal-orchestrations) | `Azure/azure-functions-durable-extension` HEAD `f5fd13971a939753cda42dd589d7af93b71a7bd4` |
| Hatchet | [Durable execution docs](https://docs.hatchet.run/home/durable-execution), [cron trigger docs](https://docs.hatchet.run/home/triggers/cron-trigger) | `hatchet-dev/hatchet` HEAD `7de79aafd7195879ca81164bbe986a0606123a3b` |
| Inngest | [How functions are executed](https://www.inngest.com/docs/learn/how-functions-are-executed), [scheduled functions](https://www.inngest.com/docs/guides/scheduled-functions) | `inngest/inngest` HEAD `b43287f38ae77ba3040066e5112bd7036ae328ac` |
| Restate | [Workflows](https://docs.restate.dev/use-cases/workflows), [Virtual Objects](https://docs.restate.dev/use-cases/virtual-objects), [timers and awakeables](https://docs.restate.dev/references/durable-promises) | `restatedev/restate` HEAD `770f1e860862d0e26faf073640b8905ddb93e917` |
| DBOS | [Workflows](https://docs.dbos.dev/python/tutorials/workflow-tutorial), [scheduled workflows](https://docs.dbos.dev/python/tutorials/scheduled-workflows), [migrating from Temporal](https://docs.dbos.dev/explanations/migrating-from-temporal) | `dbos-inc/dbos-transact-py` HEAD `e29e57ac27fe72160fc21144b78739b49a4a50f6` |
| Absurd | [Absurd concepts](https://earendil-works.github.io/absurd/concepts/), [GitHub repository](https://github.com/earendil-works/absurd) | `earendil-works/absurd` HEAD `f2fcc45db4dfa46cd44cab36a4aa1f5d9e393bbd` |
| Shikibu | In-repo historical comparison in [`docs/shikibu-comparison.md`](shikibu-comparison.md) | Not fetched during this task |

## Models reviewed

| System | Developer-facing API | Operational behavior | Replay and history semantics | Failure and cancellation | Observability | Migration and compatibility |
| --- | --- | --- | --- | --- | --- | --- |
| Temporal | Workflow code calls SDK Continue-As-New from inside workflow code, normally passing the next run's state as new input. Handles can follow the chain, but each generation has a new run id. | Current run closes as continued and the server starts a new run with the same workflow id. SDKs expose history-length hints so code can continue before hitting limits. | History is bounded per run; the logical workflow is a chain of runs. Completed activities and timers from prior runs are not replayed in the new run except through explicit input/checkpoint state. | Failures are run-local; cancellation and termination apply through workflow identity but application code must be careful to drain signals before continuing. | Web/UI surfaces show run chains and individual histories, but debugging crosses run boundaries. | Code evolution is mature, but checkpoint state becomes an application-owned input contract across generations. |
| Cadence | Similar ContinueAsNew API and cron workflow support. | Cadence closes one run and schedules the next, either manually or by cron schedule. | Run histories are bounded by starting a new run; recurring cron runs are separate runs. | Cancellation and timeouts are workflow/run concepts; carry-over state is explicit. | Visibility is run oriented, with workflow id plus run id. | Temporal/Cadence users already understand the pattern, but Durababble should not inherit its API ceremony by default. |
| Azure Durable Functions | Orchestrators call `ContinueAsNew` inside an eternal loop, often after a timer. | The Durable Task hub truncates the old history and restarts the orchestration with new input. | The logical instance id continues while execution history is discarded/restarted around the new input. | Orchestrator replay rules still apply; external events can optionally be preserved or dropped depending on platform overloads/options. | Azure Portal/Application Insights show orchestration instances, but detailed old history is bounded. | Simple for periodic loops, but the API still requires the author to know when and what to pass forward. |
| Hatchet | Developers write durable tasks/workflows with steps, retries, schedules, cron triggers, and concurrency controls. | Recurring work is generally modeled as many scheduled/cron-triggered runs, not one infinite run. | Step state is persisted per run; history is bounded by run boundaries and retention rather than by replaying one eternal function. | Retries, cancellations, and timeouts are operational controls on tasks/runs. | Hatchet emphasizes dashboard visibility into runs, logs, retries, and schedules. | The model is approachable because recurrence is external scheduling, but it is less direct for one logical stateful process that must keep private cursor state. |
| Inngest | Functions are event-triggered or scheduled; code uses `step.run`, `step.sleep`, and `step.waitForEvent`. | Recurrence is represented as cron/event-triggered function invocations. Long sleeps/waits are persisted by the platform. | Each function run has step checkpoints; indefinite processes are usually decomposed into repeated invocations. | Step retries and cancellation controls are per function/run; event ids and steps provide dedupe. | Inngest exposes function runs, steps, retries, and event traces. | Migration pressure is toward evented decomposition rather than long-lived workflow migration. |
| Restate | Services, Virtual Objects, and Workflows use durable handlers. Virtual Objects hold state by key; workflows coordinate durable steps and timers. | Long-lived state naturally belongs in Virtual Objects; workflows handle bounded processes around that state. Timers/reminders wake handlers. | Object state is a compact latest-state checkpoint; handler journals retain execution metadata for recovery and retention. | Restate retries failed handlers and can cancel or kill invocations operationally. Object commands serialize by key. | Restate exposes services, invocations, journals, and stateful object keys through operational tooling. | Strong fit for Durababble's durable-object direction: keep indefinite mutable state on an object and run short workflows for episodes. |
| DBOS | Python/TypeScript functions are marked as workflows; steps are checkpointed in Postgres. Scheduled workflows and queues handle recurring work. | DBOS stores workflow/step status in Postgres and resumes from the last completed step after crashes. Recurrence is often a schedule creating new workflow executions. | Replay is checkpoint-based, not event-sourced. Histories are bounded by execution boundaries and retention. | Workflow ids provide idempotency; failed workflows can be inspected/recovered. Cancellation exists as workflow management. | DBOS console/metadata tables show workflow status, steps, queues, and schedules. | Very close to Durababble's SQL-first style; the natural migration is short run boundaries plus database-backed state. |
| Absurd | Workflows are ordinary functions backed by Postgres, with durable steps/checkpoints and retries. | The project emphasizes simple Postgres durability over a large service runtime. | Checkpoint rows let completed work be skipped; recurrence is not a first-class Continue-As-New story in the reviewed docs. | Failure recovery is checkpoint/retry oriented. | Observability is database/job oriented rather than a large workflow UI. | Useful evidence that SQL-checkpointed workflows can be simpler than Temporal, but it does not solve indefinite loops by itself. |
| Shikibu | The in-repo comparison notes a `recur` API that archives history and creates a continued instance. | Recurrence is explicit but higher-level than manually calling Continue-As-New. | History is archived and a continued instance starts with carried state. | Cancellation/compensation are part of Shikibu's broader workflow model. | Conventional workflow history plus archived generations. | The archived-generation idea is worth borrowing, while keeping Durababble's stricter fence/outbox honesty. |

## Continue-As-New pain points

Continue-As-New solves a real storage/replay problem, but its ergonomics leak too much runtime machinery:

- Application code must know history thresholds and decide when to roll over.
- State transfer is manual. Developers must serialize "the future" into the next run's input and keep that shape compatible forever.
- Identity is split. Users see one workflow id but many run ids, and tooling/debugging follows a chain.
- Signals and inbox messages become subtle around rollover. Code must drain, preserve, or intentionally discard pending work.
- Cancellation, termination, and retention have both logical-workflow and current-run meanings.
- Continue-As-New is often expressed as a special return/error/control-flow path instead of as a normal durable boundary.
- Migration is harder because generation input becomes a long-lived API surface in addition to workflow args/results and patch markers.

These are not all inherent durability requirements. The inherent requirements are:

- bound replay by cutting deterministic history into finite units;
- persist a checkpoint before a cut is allowed;
- preserve idempotency coordinates for external side effects across retries;
- define whether signals/inbox rows are accepted by the old or next unit;
- retain enough old history for debugging and deterministic compatibility;
- expose the logical process and its generations to operators;
- make cancellation and terminal cleanup chain-aware.

Durababble should own those requirements in the library instead of asking every workflow author to rediscover them.

## Durababble alternatives

### Option A: managed workflow epochs

`Workflow.epoch_loop` creates a logical workflow chain whose current epoch is just a normal Durababble workflow run. The library rolls to a successor epoch when a configured step/history/inbox threshold is reached or when user code calls `epoch.checkpoint`.

Developer code names durable work, not continuation mechanics:

```ruby
class ShopSync < Durababble::Workflow
  workflow_name "shop_sync"

  def execute(shop_id)
    Workflow.epoch_loop(
      initial_state: { "cursor" => nil },
      max_steps: 500,
      max_attempts: 2_000,
    ) do |epoch|
      result = sync_batch(shop_id, epoch.state.fetch("cursor"))
      epoch.state = { "cursor" => result.fetch("next_cursor") }

      if result.fetch("done")
        epoch.sleep_until(Time.now.utc + 3600)
      else
        epoch.continue
      end
    end
  end

  step def sync_batch(shop_id, cursor)
    Shopify.fetch_and_store_batch(
      shop_id,
      cursor: cursor,
      idempotency_key: step_context.idempotency_key,
    )
  end
end
```

The current epoch completes with a distinct status such as `continued` after the checkpoint commits, and the successor starts from the Paquito-serialized epoch state. A `Workflow.handle(logical_id)` follows the chain to the active epoch by default; operator APIs can inspect individual epochs.

### Option B: object-backed long-lived state with short workflow runs

For many Durababble use cases, the stateful thing should be a durable object and recurring work should be short workflows or object wakeups:

```ruby
class ShopSyncState < Durababble::DurableObject
  object_type "shop_sync_state"

  def initialize_state
    { "cursor" => nil, "active" => true }
  end

  expose_command def record_batch(next_cursor)
    update_state(current_state.merge("cursor" => next_cursor))
  end
end

class SyncBatch < Durababble::Workflow
  workflow_name "sync_batch"

  def execute(shop_id)
    state = ShopSyncState.at(shop_id)
    result = sync_batch(shop_id, state.cursor)
    ShopSyncState.tell(shop_id, :record_batch, result.fetch("next_cursor"))
  end
end
```

This should be the recommendation when there is a natural id-addressed entity and the actual work can be broken into independent episodes.

### Option C: scheduled short runs

Cron/schedule style recurrence is the simplest model for "do this every N minutes" when no private cursor must survive inside an infinite function. It is operationally obvious and has bounded histories by construction, but it needs idempotent starts and overlap/concurrency policy.

### Option D: explicit generation handles

Expose lower-level handles such as `Workflow.handle(id).generation(12)` and `handle.current_generation` for operators and advanced users. This is a useful management API, not the primary authoring model.

### Option E: direct Continue-As-New compatibility

Durababble can eventually expose `Workflow.continue_as_new(input)` as an escape hatch for migration from Temporal/Cadence. It should be implemented on top of managed epochs and documented as low-level.

## Recommendation

Make managed workflow epochs the primary workflow answer, and document object-backed state plus short runs as the preferred modeling answer when an addressed durable object exists.

The public names should be boring and descriptive:

- `Workflow.epoch_loop(initial_state:, max_steps:, max_attempts:, max_age:, max_inbox_messages:)`
- `epoch.state` and `epoch.state=`
- `epoch.checkpoint(state = epoch.state, reason: nil)`
- `epoch.continue`
- `epoch.stop(result = nil)`
- `Workflow.handle(logical_id).current_epoch`
- `Workflow.handle(logical_id).epochs`

Direct `continue_as_new` can exist later as an alias or compatibility helper, but examples should use `epoch_loop`.

### Before and after

Current Durababble can express an indefinite loop, but every iteration consumes new step/wait positions forever:

```ruby
class PollForever < Durababble::Workflow
  workflow_name "poll_forever"

  def execute(shop_id)
    cursor = nil

    loop do
      cursor = poll_once(shop_id, cursor)
      Durababble.wait_until(Time.now.utc + 300)
    end
  end

  step def poll_once(shop_id, cursor)
    RemoteApi.poll(shop_id, cursor:, idempotency_key: step_context.idempotency_key)
  end
end
```

Proposed Durababble keeps the loop body but gives the library responsibility for the cut:

```ruby
class PollForever < Durababble::Workflow
  workflow_name "poll_forever"

  def execute(shop_id)
    Workflow.epoch_loop(initial_state: { "cursor" => nil }, max_steps: 250) do |epoch|
      cursor = poll_once(shop_id, epoch.state.fetch("cursor"))
      epoch.state = { "cursor" => cursor }
      epoch.sleep_until(Time.now.utc + 300)
    end
  end

  step def poll_once(shop_id, cursor)
    RemoteApi.poll(shop_id, cursor:, idempotency_key: step_context.idempotency_key)
  end
end
```

The author still sees an indefinite loop. Durababble persists each batch's checkpoint before rolling the epoch and keeps each epoch's replay bounded.

## Storage and schema requirements

Managed epochs should be implemented as a portable SQL extension to the current schema:

- Add a logical chain identifier. Either add `logical_workflow_id`, `epoch`, `previous_workflow_id`, `continued_to_workflow_id`, `continued_at`, `rollover_reason`, and `epoch_checkpoint` columns to `workflows`, or add a `workflow_chains` table plus run-level columns. The latter is cleaner for operator state.
- Store epoch checkpoints as Paquito bytes, not JSON, to match runtime payload policy.
- Persist a rollover decision before the successor can run. A crash after checkpoint commit but before successor enqueue must be recoverable by a sweeper/claim path that creates the missing successor once.
- Mark old epochs terminal with a specific `continued` status so they do not look like user success/failure.
- Keep step, attempt, wait, fence, outbox, and inbox rows run-scoped. They can age out by epoch retention after the successor checkpoint is durable.
- Keep a chain-level `current_workflow_id`, `current_epoch`, `status`, and cancellation flag to avoid scanning all epochs for normal handle operations.
- Use unique constraints on `(worker_pool, logical_workflow_id, epoch)` and `(worker_pool, logical_workflow_id, status/current marker)` in backend-portable form. Avoid PostgreSQL-only partial indexes unless the MySQL adapter has equivalent behavior.

## Runtime semantics

- Max-history enforcement should be library-managed. The engine counts current epoch step positions, attempts, waits, and accepted inbox messages and rolls before configured thresholds are exceeded.
- A user checkpoint is a durable boundary. It must be persisted under the current workflow lease before any successor is claimable.
- Step idempotency keys remain run/epoch scoped. Cross-epoch side effects must use application business keys or fence/outbox keys when they intentionally span epochs.
- Method/order step identity resets at each epoch because the checkpoint state is the new deterministic input. Old epoch history remains inspectable but is not replayed by the successor.
- `patched` markers are epoch-local, but the chain view should summarize patch usage across epochs. A code change inside the epoch loop still needs patch markers until old epochs age out.
- `Workflow.handle(logical_id)` defaults to the active epoch. `Workflow.handle(logical_id, epoch:)` addresses historical generations for read-only inspection or explicit management.

## Cancellation, inbox, and cleanup

- Cancellation targets the logical chain by default. It sets a chain-level cancellation flag, wakes the active epoch, prevents successor creation, and cancels active waits/inbox consumption according to the normal workflow cancellation rules.
- A run-level cancellation escape hatch can exist for operators, but it should be clearly lower-level because canceling an old epoch must not strand the chain.
- Workflow signals and commands should target the logical chain. Acceptance writes a durable inbox row against the chain; delivery assigns it to the active epoch under lock. During rollover, undelivered rows move to or are claimed by the successor without changing message ids.
- If a signal is accepted before the rollover checkpoint commits, the old epoch must either consume it before continuing or mark it as carried to the successor in the same transaction as rollover. Silent drops are not allowed.
- Retention should keep at least the last completed epoch and enough prior epochs to debug the latest checkpoint. Operators should be able to configure count-based and age-based retention.

## Operator UI expectations

Operator surfaces should show the logical workflow first:

- logical id, workflow class, worker pool, status, current epoch, current run id, last checkpoint time, and rollover reason;
- an epoch list with run id, status, start/end time, step count, attempt count, wait count, inbox count, and patch markers;
- "open current epoch" and "open previous epoch" links for debugging;
- cancellation controls that clearly distinguish chain cancellation from run-level force termination;
- warnings when an epoch is near max-history thresholds or a rollover recovery sweeper had to create a missing successor.

## Follow-up implementation work

Implementation is intentionally out of scope for this research ticket. The follow-up work should be split into:

1. Schema and store operations for workflow chains/epochs, including portable MySQL/MariaDB and PostgreSQL/YSQL constraints.
2. Engine support for `Workflow.epoch_loop`, checkpoint commit, successor enqueue, and crash recovery around rollover.
3. Logical-handle, cancellation, and inbox routing semantics across epochs.
4. Operator/observability reads and retention sweepers for epoch chains.
5. Backend conformance and deterministic/crash tests covering rollover, lost lease during checkpoint, signal carry-over, cancellation cleanup, and retention.
