# Durababble formal model

Durababble keeps an Alloy model in `formal/workflow_storage.als` to check the prototype's workflow, lease, and durable-storage safety claims independently of the Ruby implementation.

## Commands

Run all formal checks:

```sh
mise exec -- bundle exec rake formal
```

Or run the pieces directly:

```sh
mise exec -- scripts/verify-alloy.sh
mise exec -- bundle exec ruby scripts/validate-durababble-sigils.rb --verbose
```

`scripts/verify-alloy.sh` uses `alloy6` when installed. Otherwise it downloads the Alloy 6 CLI jar to `tmp/alloy/` and runs it with Java from the active toolchain. The shared `mise.toml` includes Java so `mise exec -- scripts/verify-alloy.sh` works on a clean checkout. Every Alloy command must declare an `expect` result; the verifier fails if any command omits it. The expected Alloy result is all `run` commands SAT and all `check` commands UNSAT.

For local iteration, run one command or an Alloy wildcard without changing the solver/runtime path:

```sh
mise exec -- env ALLOY_COMMAND=exampleWaitWake scripts/verify-alloy.sh
mise exec -- env ALLOY_COMMAND='exampleInboxCommand*' scripts/verify-alloy.sh
```

The GitHub Actions `formal` job runs both the Alloy verifier and the sigil validator, so CI fails if the model regresses or if model/Ruby sigils drift.

## Model-to-implementation matrix

| Tag | Alloy assertion/example | Ruby implementation | Test/DST coverage | Result |
| --- | --- | --- | --- | --- |
| `[DURABABBLE-WF-1]` | `enqueueWorkflow`, `requestWorkflowCancellation`, `terminalStatesDoNotMutate`, `terminalWorkflowsHaveNoIncompleteWork`, `exampleWorkflowCompletes`, `exampleCancellationCompletesAfterCleanup`, `exampleWaitingCancellationCancelsWait`, `exampleBackoffCancellationClearsDue` | `PostgresStore#enqueue_workflow`, cancellation request/cleanup paths, terminal failure cleanup paths, `SqlStore#require_fenced_workflow_update!` plus MySQL equivalent paths | `DurababbleCompleteTest`, `DurababbleEngineTest`, `DurababbleHatchetInspiredTest`, `DurababbleWorkflowCancellationTest`, `DurababbleStoreBackendConformanceTest`, DST `workflow_durable_before_claim` | Alloy check UNSAT; examples SAT; tests pass |
| `[DURABABBLE-LEASE-1]` | `claimWorkflow`, `atMostOneLiveOwner`, `exampleWorkflowCompletes`, `exampleExpiredRunningWorkflowReclaimedDirectly` | `PostgresStore#claim_runnable_workflow`, `PostgresStore#claim_workflow` plus MySQL equivalent paths | `DurababbleHardeningTest`, backend conformance, DST `multi_worker_counter`/`lease_conflict` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-LEASE-2]` | `heartbeatWorkflow` | `PostgresStore#heartbeat`, `PostgresStore#heartbeat_step` plus MySQL equivalent paths | backend conformance, DST `heartbeat_extension`/`zombie_workflow_heartbeat_after_expiry` | Alloy check UNSAT through lease assertions; tests pass |
| `[DURABABBLE-LEASE-3]` | `releaseOrStealLease`, `exampleLeaseStealAndReplay` | `PostgresStore#release_worker_leases!`, `PostgresStore#steal_expired_leases!` plus MySQL equivalent paths | `DurababbleCompleteTest`, backend conformance, DST `lease_expiry` | Alloy example SAT; tests pass |
| `[DURABABBLE-LEASE-4]` | `completeStep`, `completeWorkflow`, `staleOwnersCannotCommit` | `WorkflowExecution#assert_workflow_lease!`, `WorkflowStepRunner` step commit paths, `SqlStore#require_fenced_workflow_update!` | `DurababbleHardeningTest`, worker lifecycle, DST workflow RPC owner matrix | Alloy check UNSAT; tests pass |
| `[DURABABBLE-CONCURRENCY-1]` | `scheduleWorkflowCommand`, `startStep`, `completeStep`, `recordWait`, `wakeWait`, `scheduledCommandHistoryIsReplayStable`, `terminalCommandHistoryUsesLatestReplayEvent`, `commandHistoryFollowsRuntimeLifecycle`, `exampleParallelCommandSchedules`, `exampleScheduledCommandReplayBeforeStepStart`, `exampleTerminalCommandHistoryCanResolveOutOfScheduleOrder` | `WorkflowExecution#schedule_command!`, `WorkflowReplayHistory`, step scheduled/started/completed/waiting/failed/canceled history writes in `PostgresStore` plus MySQL equivalent paths | `DurababbleAsyncWorkflowTest`, backend conformance workflow-history tests | Alloy checks UNSAT; examples SAT; tests pass |
| `[DURABABBLE-STEP-1]` | `resumeReplayCompletedStep`, `completedStepsAreNotReexecuted` | `WorkflowExecution#call_step` completed-step replay branch | `DurababbleCompleteTest`, subprocess crash test, DST `completed_step_skip_after_crash` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-STEP-2]` | `startStep`, `retryStep`, `incompleteStepsRetrySafely`, `retryBackoffPreventsEarlyClaim`, `exampleRetryThenCompletesWithFailedAttemptHistory` | `Store#record_step_started`, `SqlStore#record_step_failed_and_schedule_retry`, retry scheduling and claim paths | `DurababbleHardeningTest`, `DurababbleCompleteTest`, queue correctness, backend conformance, DST `incomplete_step_retry_after_crash`/`step_retry_policy_recovery` | Alloy checks UNSAT; examples SAT; tests pass |
| `[DURABABBLE-WAIT-1]` | `recordWait`, `wakeWait`, `waitsWakeOnce`, `exampleWaitWake`, `exampleWaitAllowsSiblingCompletionBeforeSuspension` | `WorkflowExecution#call_wait`, `WorkflowStepRunner#record_wait`, `PostgresStore#complete_timer_waits` plus MySQL equivalent paths | `DurababbleWorkflowWaitTest`, `DurababbleAsyncWorkflowTest`, backend conformance timer-wait tests | Alloy check UNSAT; examples SAT; tests pass |
| `[DURABABBLE-FENCE-1]` | `acquireFence`, `completeFence`, `idempotencyFencesPreventDuplicateSideEffects`, `exampleAbandonedFenceRemainsRunningAfterCrashReplay` | `PostgresStore#with_fence` plus MySQL equivalent paths | `DurababbleHardeningTest`, backend conformance, DST `fenced_side_effect_once` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-OUTBOX-1]` | `enqueueOutbox`, `claimOutbox`, `ackOutbox`, `outboxAckLeaseBehaviorIsSafe`, `exampleFenceOutbox`, `exampleOutboxExpiryReclaimAndAck` | `PostgresStore#enqueue_outbox`, `PostgresStore#claim_outbox`, `PostgresStore#ack_outbox` plus MySQL equivalent paths | `DurababbleHardeningTest`, backend conformance, DST `outbox_lease_expiry` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-OBJ-1]` | `enqueueInboxCommand`, `claimTargetActivation`, `claimInboxCommand`, `completeInboxCommand`, `failInboxCommand`, `deadLetterInboxCommand`, `durableInboxCommandSerializationHolds`, inbox command examples | `DurableObjectRef#run_command`, workflow inbox RPC, `SqlStore#enqueue_inbox_message`, `SqlStore#claim_target_activation`, `SqlStore#claim_object_command`, `SqlStore#complete_object_command`, `SqlStore#fail_object_command` | `DurababbleDurableObjectTest`, backend conformance inbox/mailbox tests | Alloy check UNSAT; tests pass |

The model includes implemented workflow command-history rows, timer waits, target activations, ordered inbox command rows for workflow and object targets, fences, and outbox rows. Current assertions focus on the implemented prototype tables and the high-risk target semantics that already have Ruby surface area.

Workflow command history is modeled as the concrete replay events the implementation writes: `CommandScheduled`, `CommandStarted`, `CommandSucceeded`, `CommandWaiting`, `CommandCanceled`, `CommandRejected`, and retry diagnostic `CommandErrored`. The lifecycle assertion requires scheduled history before starts, waits, and terminal outcomes; final replay uses the latest terminal event for a command so a completed timer wake supersedes the earlier waiting event, while retry diagnostics remain non-terminal.

Retryable step failure is modeled as one atomic SQL transition: the failed step/attempt and diagnostic history row are committed in the same transaction that releases the workflow lease and writes `next_run_at`. This matches `SqlStore#record_step_failed_and_schedule_retry` and prevents the model from assuming a retry can be scheduled without the failed attempt history, or vice versa.

The terminal-state assertions cover completed and canceled workflow rows, and separately treat final failed rows as terminal when `next_run_at` is null. Failed workflow rows are modeled as claimable only when they carry a non-null due `next_run_at`; retry backoff assertions prove that pending or failed rows with a future deadline cannot be claimed early and final failed rows with no retry deadline stay out of claim paths. Cooperative cancellation is modeled through `Canceling`, immediate cancellation of pending waits, durable cleanup steps, and final `Canceled` completion.

Durable-object serialization is modeled with one target activation owner and FIFO command sequences per target. A worker may drain a contiguous prefix, but a failed or dead-lettered head command blocks later commands until the head is retried or deliberately remains terminal, matching the implementation's mailbox behavior.
