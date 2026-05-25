# Durababble formal model

Durababble keeps an Alloy model in `formal/workflow_storage.als` to check the
prototype's workflow, lease, and durable-storage safety claims independently of
the Ruby implementation.

## Commands

Run all formal checks:

```sh
mise exec -- bundle exec rake formal
```

Or run the pieces directly:

```sh
scripts/verify-alloy.sh
mise exec -- bundle exec ruby scripts/validate-durababble-sigils.rb --verbose
```

`scripts/verify-alloy.sh` uses `alloy6` when installed. Otherwise it downloads
the Alloy 6 CLI jar to `tmp/alloy/` and runs it with Java. The expected Alloy
result is all `run` commands SAT and all `check` commands UNSAT.

The GitHub Actions `formal` job runs both the Alloy verifier and the sigil
validator, so CI fails if the model regresses or if model/Ruby sigils drift.

## Model-to-implementation matrix

| Tag | Alloy assertion/example | Ruby implementation | Test/DST coverage | Result |
| --- | --- | --- | --- | --- |
| `[DURABABBLE-WF-1]` | `enqueueWorkflow`, `terminalStatesDoNotMutate`, `terminalWorkflowsHaveNoIncompleteWork`, `exampleWorkflowCompletes` | `PostgresStore#enqueue_workflow`, terminal failure cleanup paths, `SqlStore#require_fenced_workflow_update!` plus MySQL equivalent paths | `DurababbleCompleteTest`, `DurababbleEngineTest`, `DurababbleHatchetInspiredTest`, `DurababbleStoreBackendConformanceTest`, DST `workflow_durable_before_claim` | Alloy check UNSAT; examples SAT; tests pass |
| `[DURABABBLE-LEASE-1]` | `claimWorkflow`, `atMostOneLiveOwner`, `exampleWorkflowCompletes` | `PostgresStore#claim_runnable_workflow`, `PostgresStore#claim_workflow` plus MySQL equivalent paths | `DurababbleHardeningTest`, backend conformance, DST `multi_worker_counter`/`lease_conflict` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-LEASE-2]` | `heartbeatWorkflow` | `PostgresStore#heartbeat`, `PostgresStore#heartbeat_step` plus MySQL equivalent paths | backend conformance, DST `heartbeat_extension`/`zombie_workflow_heartbeat_after_expiry` | Alloy check UNSAT through lease assertions; tests pass |
| `[DURABABBLE-LEASE-3]` | `releaseOrStealLease`, `exampleLeaseStealAndReplay` | `PostgresStore#release_worker_leases!`, `PostgresStore#steal_expired_leases!` plus MySQL equivalent paths | `DurababbleCompleteTest`, backend conformance, DST `lease_expiry` | Alloy example SAT; tests pass |
| `[DURABABBLE-LEASE-4]` | `completeStep`, `completeWorkflow`, `staleOwnersCannotCommit` | `WorkflowExecution#assert_workflow_lease!`, `WorkflowStepRunner` step commit paths, `SqlStore#require_fenced_workflow_update!` | `DurababbleHardeningTest`, worker lifecycle, DST workflow RPC owner matrix | Alloy check UNSAT; tests pass |
| `[DURABABBLE-CONCURRENCY-1]` | `scheduleWorkflowCommand`, `scheduledCommandHistoryIsReplayStable`, `exampleParallelCommandSchedules` | `WorkflowExecution#schedule_command!`, `PostgresStore#record_step_scheduled` plus MySQL equivalent paths | `DurababbleAsyncWorkflowTest`, backend conformance workflow-history tests | Alloy check UNSAT; examples SAT; tests pass |
| `[DURABABBLE-STEP-1]` | `resumeReplayCompletedStep`, `completedStepsAreNotReexecuted` | `WorkflowExecution#call_step` completed-step replay branch | `DurababbleCompleteTest`, subprocess crash test, DST `completed_step_skip_after_crash` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-STEP-2]` | `startStep`, `retryStep`, `incompleteStepsRetrySafely`, `retryBackoffPreventsEarlyClaim` | `Store#record_step_started`, retry scheduling and claim paths | `DurababbleHardeningTest`, `DurababbleCompleteTest`, queue correctness, backend conformance, DST `incomplete_step_retry_after_crash`/`step_retry_policy_recovery` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-WAIT-1]` | `recordWait`, `wakeWait`, `waitsWakeOnce`, `exampleWaitWake`, `exampleWaitAllowsSiblingCompletionBeforeSuspension` | `WorkflowExecution#call_wait`, `WorkflowStepRunner#record_wait`, `PostgresStore#complete_timer_waits` plus MySQL equivalent paths | `DurababbleWorkflowWaitTest`, `DurababbleAsyncWorkflowTest`, backend conformance timer-wait tests | Alloy check UNSAT; examples SAT; tests pass |
| `[DURABABBLE-FENCE-1]` | `acquireFence`, `completeFence`, `idempotencyFencesPreventDuplicateSideEffects` | `PostgresStore#with_fence` plus MySQL equivalent paths | `DurababbleHardeningTest`, backend conformance, DST `fenced_side_effect_once` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-OUTBOX-1]` | `enqueueOutbox`, `claimOutbox`, `ackOutbox`, `outboxAckLeaseBehaviorIsSafe`, `exampleFenceOutbox` | `PostgresStore#enqueue_outbox`, `PostgresStore#claim_outbox`, `PostgresStore#ack_outbox` plus MySQL equivalent paths | `DurababbleHardeningTest`, backend conformance, DST `outbox_lease_expiry` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-OBJ-1]` | `enqueueObjectCommand`, `claimObjectCommand`, `completeObjectCommand`, `failObjectCommand`, `durableObjectCommandSerializationHolds`, object command examples | `DurableObjectRef#run_command`, `SqlStore#enqueue_inbox_message`, `SqlStore#claim_object_command`, `SqlStore#complete_object_command`, `SqlStore#fail_object_command` | `DurababbleDurableObjectTest`, backend conformance inbox/mailbox tests | Alloy check UNSAT; tests pass |

The model includes implemented workflow command-history rows, unified inbox/object-command rows, timer waits, target command serialization, and structural placeholders only for future surfaces not yet implemented. Current assertions focus on the implemented prototype tables and the high-risk target semantics that already have Ruby surface area.

The terminal-state assertions cover completed, cancelled, and terminated
workflow rows. Failed workflow rows are modeled as claimable only when they
carry a non-null due `next_run_at`; retry backoff assertions prove that pending
or failed rows with a future deadline cannot be claimed early and terminal
failed rows with no retry deadline stay out of claim paths.
