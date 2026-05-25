# Temporal Test Import Matrix

This note records the upstream Temporal test sources inspected for HAR-1272 and
maps their behavior ideas into Durababble coverage. The intent is to import
durability and replay ideas, not Temporal's full API surface.

## Upstream Sources

| Repository | URL | Branch | Commit | License | Local path |
| --- | --- | --- | --- | --- | --- |
| Temporal server | `https://github.com/temporalio/temporal.git` | `main` | `4f2da02c676304b352a7461b9cffa126aa827ef2` | MIT | `tmp/temporal/temporal` |
| Temporal Go SDK | `https://github.com/temporalio/sdk-go.git` | `main` | `9dc86fb5d4e94b4afa38f66bc4b966efaa4e2f0f` | MIT | `tmp/temporal/sdk-go` |

Representative upstream test runs were attempted with:

```sh
cd tmp/temporal/sdk-go
go test ./internal -run 'TestDeterministicKeys|Test_TimerStateMachine_CompleteWithoutCancel' -count=1

cd tmp/temporal/temporal
go test ./service/history/workflow -run 'TestCalculateExternalPayloadSize_NoExternalPayloads|TestCalculateExternalPayloadSize_WithExternalPayloads' -count=1
```

Both attempts were blocked by the host missing the Go toolchain:

```text
/bin/bash: line 1: go: command not found
```

The analysis below is source-backed from the cloned repositories.

## Behavior Mapping

| Behavior area | Temporal source inspiration | Durababble coverage before this import | Imported or rejected outcome |
| --- | --- | --- | --- |
| Replay determinism | `sdk-go/test/integration_test.go` `TestNonDeterminismFailureCause*`; `sdk-go/contrib/tools/workflowcheck/determinism/determinism_test.go` | `test/durababble/hatchet_inspired_test.rb` covers method mismatch, suffix removal, and step reorder failures. | Kept existing coverage; no API change. |
| History-size / long replay | `sdk-go/test/integration_test.go` `TestLargeHistoryReplay`, `TestHistoryLength` | Small replay and retry-skip tests existed, but not a long completed prefix followed by a later wake. | Added `test/durababble/engine_test.rb` large-history replay: 75 completed step positions are replayed without rerunning step bodies before the workflow consumes the wake and finishes. |
| Continue-as-new | `temporal/tests/continue_as_new_test.go`; scheduler `TestCANBy*` cases | No Durababble run-chain or continue-as-new API exists. | Rejected for now. This is a future public API/storage decision, not a test-only import. |
| Timers | `temporal/tests/workflow_timer_test.go`; `sdk-go/internal/internal_command_state_machine_test.go` timer state-machine cases | Timer waits, due wakeups, sequential timer waits, and DST timer/partition paths already exist. | Kept current coverage. Timer cancellation was rejected because Durababble has no timer-cancel API yet. |
| Signals and workflow commands | `temporal/tests/signal_workflow_test.go`; `temporal/tests/query_workflow_test.go`; SDK update/signal ordering tests | General signal-style delivery is intentionally out of scope; workflow `expose_command` covers durable RPC delivery through the workflow inbox. | Added `test/durababble/workflow_test.rb` end-to-end exposed-command delivery through persisted workflow inbox rows. |
| Queries | `temporal/tests/query_workflow_test.go` sticky and consistency cases | Durababble exposed workflow queries are transient local refs; durable object `expose` queries read persisted state. | Rejected Temporal sticky/consistent query semantics until remote owner routing and unified inbox are implemented. |
| Cancellation / termination | `temporal/tests/cancel_workflow_test.go`; `temporal/tests/workflow_test.go` termination cases | `test/durababble/workflow_cancellation_test.rb` covers cooperative cancellation, cleanup, retry-backoff, duplicate requests, and terminal idempotency. Durababble has no hard termination API. | Kept current cooperative-cancellation coverage; rejected Temporal hard-termination import as API scope expansion. |
| Retries | `sdk-go/internal/common/backoff/retry_test.go`; server `tests/workflow_test.go` retry cases | Step retry policies and restart persistence already existed. | Added `test/durababble/durable_object_test.rb` durable-object command retry idempotency coverage so the same command operation key survives retry. |
| Child workflows / activities | `temporal/tests/child_workflow_test.go`; `sdk-go/test/integration_test.go` child/local activity restart cases | Durababble models side-effect boundaries as workflow steps and uses outbox rows for child-like effects. | Kept `hatchet_inspired` outbox child-effect retry test; rejected Temporal child-workflow API shape. |
| Parallel async execution | SDK goroutine/coroutine and update ordering tests in `sdk-go/test/integration_test.go` | Durababble supports raw `Async` workflow fanout with deterministic command replay, out-of-order completion history, and branch-suspension coverage. | Imported as raw Async workflow tests; Durababble-specific async helper APIs are not part of the contract. |
| Worker crashes and workflow task failure | `sdk-go/internal/internal_task_pollers_test.go`; `sdk-go/test/integration_test.go` worker restart and fatal error tests | Worker lifecycle, lease release, stale lease recovery, and DST crash scenarios are present. | Kept existing coverage; no new worker API behavior needed. |
| Sticky / lease behavior | `temporal/tests/stickytq_test.go`; SDK worker cache tests | Durababble covers SQL leases, stale owner rejection, lease-routed RPC, and gRPC reroute faults. | Rejected sticky-cache-specific tests until target sticky placement exists. Existing lease tests remain the Durababble equivalent. |
| Visibility / ops APIs | `temporal/tests/workflow_visibility_test.go`; query-plan and history inspection tests | Durababble has store read APIs and query-plan tests, but no Temporal-style visibility service. | Rejected Temporal visibility API import. Query-plan and benchmark coverage remain the current operational surface. |

## Ported Subset

The ported subset lives in the behavior suites that own each Durababble contract:

- `test/durababble/engine_test.rb`: large completed history prefix replay without rerunning completed step bodies.
- `test/durababble/workflow_test.rb`: exposed workflow command delivery through persisted workflow inbox rows.
- `test/durababble/durable_object_test.rb`: durable object command retry preserving a stable generated idempotency key.

These tests translate Temporal concepts into Durababble's current workflows, steps, waits, workflow command inbox rows, durable objects, and command retry model.
