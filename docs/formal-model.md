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
| `[DURABABBLE-WF-1]` | `enqueueWorkflow`, `terminalStatesDoNotMutate`, `exampleWorkflowCompletes` | `Store#enqueue_workflow` | `DurababbleCompleteTest`, `DurababbleStoreBackendConformanceTest`, DST `workflow_durable_before_claim` | Alloy check UNSAT; examples SAT; tests pass |
| `[DURABABBLE-LEASE-1]` | `claimWorkflow`, `atMostOneLiveOwner`, `exampleWorkflowCompletes` | `Store#claim_runnable_workflow`, `Store#claim_workflow` | `DurababbleHardeningTest`, backend conformance, DST `multi_worker_counter`/`lease_conflict` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-LEASE-2]` | `heartbeatWorkflow` | `Store#heartbeat`, `Store#heartbeat_step` | backend conformance, DST `heartbeat_extension`/`zombie_workflow_heartbeat_after_expiry` | Alloy check UNSAT through lease assertions; tests pass |
| `[DURABABBLE-LEASE-3]` | `releaseOrStealLease`, `exampleLeaseStealAndReplay` | `Store#release_worker_leases!`, `Store#steal_expired_leases!` | `DurababbleCompleteTest`, backend conformance, DST `lease_expiry` | Alloy example SAT; tests pass |
| `[DURABABBLE-LEASE-4]` | `completeStep`, `completeWorkflow`, `staleOwnersCannotCommit` | `WorkflowExecution#assert_workflow_lease!`, `Engine#assert_workflow_lease!` | `DurababbleHardeningTest`, worker lifecycle, DST workflow RPC owner matrix | Alloy check UNSAT; tests pass |
| `[DURABABBLE-STEP-1]` | `resumeReplayCompletedStep`, `completedStepsAreNotReexecuted` | `WorkflowExecution#call_step` completed-step replay branch | `DurababbleCompleteTest`, subprocess crash test, DST `completed_step_skip_after_crash` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-STEP-2]` | `startStep`, `retryStep`, `incompleteStepsRetrySafely` | `Store#record_step_started`, retry scheduling path | `DurababbleHardeningTest`, `DurababbleCompleteTest`, DST `incomplete_step_retry_after_crash`/`step_retry_policy_recovery` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-WAIT-1]` | `wakeWait`, `waitsWakeOnce`, `exampleWaitWake` | `Store#complete_waits`, `Store#complete_waits_mysql` | `DurababbleHardeningTest`, backend conformance, DST `concurrent_signal_once` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-FENCE-1]` | `acquireFence`, `completeFence`, `idempotencyFencesPreventDuplicateSideEffects` | `Store#with_fence` | `DurababbleHardeningTest`, backend conformance, DST `fenced_side_effect_once` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-OUTBOX-1]` | `enqueueOutbox`, `claimOutbox`, `ackOutbox`, `outboxAckLeaseBehaviorIsSafe` | `Store#enqueue_outbox`, `Store#claim_outbox`, `Store#ack_outbox` | `DurababbleHardeningTest`, backend conformance, DST `outbox_lease_expiry` | Alloy check UNSAT; tests pass |
| `[DURABABBLE-OBJ-1]` | `enqueueObjectCommand`, `claimObjectCommand`, `completeObjectCommand`, `durableObjectCommandSerializationHolds` | `DurableObjectRef#run_command`, store object-command lifecycle methods | `DurababbleDurableObjectTest`, backend conformance | Alloy check UNSAT; tests pass |

The model includes structural placeholders for future inbox/history rows because
the settled spec direction is a unified inbox and workflow history. Current
assertions focus on the implemented prototype tables and the high-risk target
semantics that already have Ruby surface area.
