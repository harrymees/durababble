---
name: durababble-formal
description: |
  Run and reason about Durababble's Alloy formal model in formal/workflow_storage.als.
  Use when editing the model, adding or moving `[DURABABBLE-*]` sigil comments,
  investigating CI failures in the `formal` workflow, or chasing model/Ruby drift.
---

# Durababble Formal Model

Durababble keeps an Alloy 6 model in `formal/workflow_storage.als` that proves
the workflow, lease, fence, outbox, wait, and durable-object invariants
independently of the Ruby implementation. The model and the Ruby code are
linked by `[DURABABBLE-*]` sigil comments — drift between them is a real bug
class and is caught in CI.

## When this skill applies

- Editing `formal/workflow_storage.als` or `scripts/verify-alloy.sh`.
- Adding, moving, or removing a `[DURABABBLE-*]` comment in `lib/` or `test/`.
- Refactoring code in `lib/durababble/store*.rb`, `lib/durababble/workflow_*`,
  durable-object inbox paths, fences, outbox, or wait completion — these all
  carry sigils that must follow the code.
- `formal` CI job red, or `FormalSigilDriftTest` red in the fast `test` job.
- Merging `main` into the formal-model branch (main does not run formal CI; see
  the drift trap note below).

## Commands

Run the Alloy verifier (slow, ~30 min on a clean checkout):

```sh
mise exec -- bundle exec rake formal
```

Or run the verifier script directly:

```sh
mise exec -- scripts/verify-alloy.sh
```

`scripts/verify-alloy.sh` uses `alloy6` when installed; otherwise it downloads
the Alloy 6 CLI jar to `tmp/alloy/` and runs it with Java from the active
toolchain (`mise.toml` includes Java). Every Alloy command must declare an
`expect` result; the verifier fails if any command omits it. Expected results:
all `run` SAT, all `check` UNSAT.

Iterate on a single command (or wildcard) without rerunning the whole suite:

```sh
mise exec -- env ALLOY_COMMAND=exampleWaitWake scripts/verify-alloy.sh
mise exec -- env ALLOY_COMMAND='exampleInboxCommand*' scripts/verify-alloy.sh
```

Check sigil drift between the Alloy model and the Ruby tree (fast — runs in
the regular test suite, but useful to run alone before pushing a merge):

```sh
mise exec -- bundle exec ruby -Ilib -Itest test/durababble/formal_sigil_drift_test.rb
```

## CI layout

The slow Alloy verifier is wired into `.github/workflows/formal.yml` and only
runs when `formal/**`, `scripts/verify-alloy.sh`, or that workflow file
changes — it is too slow to ride every PR.

Sigil drift is caught on every PR by `test/durababble/formal_sigil_drift_test.rb`
which rides the fast `test` job in `.github/workflows/ci.yml`. It is a normal
Minitest test, not a separate script or rake task. To validate sigils outside
the full suite, run the file directly (see the command above).

## Drift trap when merging main

`main` does not run the `formal` job. A refactor on `main` that drops a sigil
comment will leave `main` green, but the next merge of `main` into a
formal-model branch will fail `FormalSigilDriftTest` in the fast `test` job.

Pre-merge check after `git merge origin/main`:

```sh
mise exec -- bundle exec ruby -Ilib -Itest test/durababble/formal_sigil_drift_test.rb
```

If the test fails, the failure message lists the unmatched tags and the files
where they appear. Re-anchor each missing tag on the closest equivalent method
in the new layout before pushing.

## Model-to-implementation matrix

The model proves the following invariants. Each tag must appear at least once
in `formal/workflow_storage.als` and at least once in `lib/` or `test/`. When
you move code, move the comment with it.

| Tag | Alloy assertion/example | Ruby anchor |
| --- | --- | --- |
| `[DURABABBLE-WF-1]` | `enqueueWorkflow`, `requestWorkflowCancellation`, `terminalStatesDoNotMutate`, `terminalWorkflowsHaveNoIncompleteWork`, `exampleWorkflowCompletes`, `exampleCancellationCompletesAfterCleanup`, `exampleWaitingCancellationCancelsWait`, `exampleTerminalCancellationCleansPendingWait`, `exampleBackoffCancellationClearsDue` | `PostgresStore#enqueue_workflow`, cancellation request/cleanup paths, terminal failure cleanup paths, `SqlStore#require_fenced_workflow_update!` (and MySQL equivalents) |
| `[DURABABBLE-LEASE-1]` | `claimWorkflow`, `atMostOneLiveOwner`, `exampleWorkflowCompletes`, `exampleExpiredRunningWorkflowReclaimedDirectly` | `PostgresStore#claim_runnable_workflow`, `PostgresStore#claim_workflow` (and MySQL equivalents) |
| `[DURABABBLE-LEASE-2]` | `heartbeatWorkflow` | `PostgresStore#heartbeat`, `PostgresStore#heartbeat_step` (and MySQL equivalents) |
| `[DURABABBLE-LEASE-3]` | `releaseOrStealLease`, `exampleLeaseStealAndReplay` | `PostgresStore#release_worker_leases!`, `PostgresStore#steal_expired_leases!` (and MySQL equivalents) |
| `[DURABABBLE-LEASE-4]` | `completeStep`, `completeWorkflow`, `completeInboxCommand`, `staleOwnersCannotCommit`, `workflowInboxCommandCommitsNeedWorkflowLease` | `WorkflowExecution#assert_workflow_lease!`, `WorkflowStepRunner` step commit paths, `SqlStore#require_fenced_workflow_update!`, workflow inbox claim/completion fences |
| `[DURABABBLE-CONCURRENCY-1]` | `scheduleWorkflowCommand`, `startStep`, `completeStep`, `recordWait`, `wakeWait`, `scheduledCommandHistoryIsReplayStable`, `terminalCommandHistoryUsesLatestReplayEvent`, `commandHistoryFollowsRuntimeLifecycle`, `exampleParallelCommandSchedules`, `exampleScheduledCommandReplayBeforeStepStart`, `exampleTerminalCommandHistoryCanResolveOutOfScheduleOrder` | `WorkflowExecution#schedule_command!`, `WorkflowReplayHistory`, step scheduled/started/completed/waiting/failed/canceled history writes in `PostgresStore` (and MySQL equivalents) |
| `[DURABABBLE-STEP-1]` | `resumeReplayCompletedStep`, `completedStepsAreNotReexecuted` | `WorkflowExecution#call_step` completed-step replay branch |
| `[DURABABBLE-STEP-2]` | `startStep`, `retryStep`, `incompleteStepsRetrySafely`, `retryBackoffPreventsEarlyClaim`, `exampleRetryThenCompletesWithFailedAttemptHistory` | `Store#record_step_started`, `SqlStore#record_step_failed_and_schedule_retry`, retry scheduling and claim paths |
| `[DURABABBLE-WAIT-1]` | `recordWait`, `wakeWait`, `waitsWakeOnce`, `exampleWaitWake`, `exampleWaitAllowsSiblingCompletionBeforeSuspension` | `WorkflowExecution#call_wait`, `WorkflowStepRunner#record_wait`, `PostgresStore#complete_timer_waits` (and MySQL equivalents) |
| `[DURABABBLE-FENCE-1]` | `acquireFence`, `completeFence`, `failFence`, `idempotencyFencesPreventDuplicateSideEffects`, `staleFenceTokensCannotFinish`, `exampleAbandonedFenceRemainsRunningAfterCrashReplay`, `exampleFenceFailureReplaysError` | `PostgresStore#with_fence` (and MySQL equivalents) |
| `[DURABABBLE-OUTBOX-1]` | `enqueueOutbox`, `claimOutbox`, `ackOutbox`, `outboxAckLeaseBehaviorIsSafe`, `exampleFenceOutbox`, `exampleOutboxExpiryReclaimAndAck` | `PostgresStore#enqueue_outbox`, `PostgresStore#claim_outbox`, `PostgresStore#ack_outbox` (and MySQL equivalents) |
| `[DURABABBLE-OBJ-1]` | `enqueueInboxCommand`, `claimTargetActivation`, `claimInboxCommand`, `completeInboxCommand`, `failInboxCommand`, `deadLetterInboxCommand`, `durableInboxCommandSerializationHolds`, `inboxClaimsRequireExistingRows`, inbox command examples | `DurableObjectRef#run_command`, workflow inbox RPC, `SqlStore#enqueue_inbox_message`, `SqlStore#claim_target_activation`, `SqlStore#claim_object_command`, `SqlStore#complete_object_command`, `SqlStore#fail_object_command`, workflow inbox command fencing |

## Modeling notes

- Workflow command history is modeled as the concrete replay events the
  implementation writes: `CommandScheduled`, `CommandStarted`,
  `CommandSucceeded`, `CommandWaiting`, `CommandCanceled`, `CommandRejected`,
  and retry diagnostic `CommandErrored`. Lifecycle assertions require scheduled
  history before starts, waits, and terminal outcomes; final replay uses the
  latest terminal event for a command, so a completed timer wake supersedes
  the earlier waiting event while retry diagnostics remain non-terminal.
- Retryable step failure is one atomic SQL transition: failed step/attempt and
  diagnostic history are committed in the same transaction that releases the
  workflow lease and writes `next_run_at` — matching
  `SqlStore#record_step_failed_and_schedule_retry`. The model rejects a retry
  schedule without the failed-attempt history (and vice versa).
- Terminal-state assertions cover completed and canceled workflow rows;
  final failed rows are terminal only when `next_run_at` is null. Failed rows
  with a non-null due `next_run_at` are claimable; retry-backoff assertions
  prove pending/failed rows with a future deadline cannot be claimed early,
  and final failed rows with no retry deadline stay out of claim paths.
- Cooperative cancellation is modeled through `Canceling`, immediate
  cancellation of pending waits, durable cleanup steps, and final `Canceled`.
- Durable-object serialization has one target activation owner and FIFO
  command sequences per target. A worker may drain a contiguous prefix, but a
  failed or dead-lettered head command blocks later commands until the head is
  retried or deliberately remains terminal.
