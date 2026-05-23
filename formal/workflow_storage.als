/**
 * Durababble workflow, lease, and storage semantics.
 *
 * Run:
 *   ./scripts/verify-alloy.sh
 *
 * This model follows the Silo style: time-indexed durable rows, explicit
 * transition predicates, frame conditions, safety assertions, and SAT examples.
 */
module workflow_storage

open util/ordering[Time]

sig Time {}
sig Workflow {}
sig Step { step_workflow: one Workflow }
sig Attempt { attempt_step: one Step }
sig Worker {}
sig Wait { wait_step: one Step }
sig Signal {}
sig Fence { fence_workflow: one Workflow }
sig OutboxMessage { outbox_workflow: one Workflow }
sig ObjectTarget {}
sig ObjectCommand { command_target: one ObjectTarget }
sig InboxRow { inbox_target: one ObjectTarget }
sig HistoryRow { history_workflow: one Workflow }

abstract sig WorkflowStatus {}
one sig Pending, Running, Waiting, Retrying, Completed, Failed, Cancelled, Terminated extends WorkflowStatus {}

abstract sig StepStatus {}
one sig StepRunning, StepWaiting, StepCompleted, StepFailed extends StepStatus {}

abstract sig AttemptStatus {}
one sig AttemptRunning, AttemptWaiting, AttemptCompleted, AttemptFailed extends AttemptStatus {}

abstract sig WaitStatus {}
one sig WaitPending, WaitCompleted extends WaitStatus {}

abstract sig FenceStatus {}
one sig FenceRunning, FenceCompleted, FenceFailed extends FenceStatus {}

abstract sig OutboxStatus {}
one sig OutboxPending, OutboxProcessing, OutboxAcked extends OutboxStatus {}

abstract sig CommandStatus {}
one sig CommandPending, CommandRunning, CommandCompleted, CommandFailed extends CommandStatus {}

abstract sig CommitKind {}
one sig WorkflowCommit, StepCommit, WaitCommit, FenceCommit, OutboxCommit, ObjectCommandCommit extends CommitKind {}

sig WorkflowRow {
  wr_workflow: one Workflow,
  wr_status: one WorkflowStatus,
  wr_nextRunAt: lone Time,
  wr_time: one Time
}

sig StepRow {
  sr_step: one Step,
  sr_status: one StepStatus,
  sr_time: one Time
}

sig AttemptRow {
  ar_attempt: one Attempt,
  ar_status: one AttemptStatus,
  ar_time: one Time
}

sig LeaseRow {
  lr_workflow: one Workflow,
  lr_worker: one Worker,
  lr_time: one Time,
  lr_expiresAt: one Time
}

sig WaitRow {
  wait_row: one Wait,
  wait_status: one WaitStatus,
  wait_signal: lone Signal,
  wait_time: one Time
}

sig FenceRow {
  fence_row: one Fence,
  fence_status: one FenceStatus,
  fence_owner: lone Worker,
  fence_time: one Time
}

sig OutboxRow {
  outbox_row: one OutboxMessage,
  outbox_status: one OutboxStatus,
  outbox_owner: lone Worker,
  outbox_expiresAt: lone Time,
  outbox_time: one Time
}

sig CommandRow {
  command_row: one ObjectCommand,
  command_status: one CommandStatus,
  command_owner: lone Worker,
  command_time: one Time
}

sig InboxState {
  inbox_row: one InboxRow,
  inbox_sequence: one Time,
  inbox_time: one Time
}

sig HistoryState {
  history_row: one HistoryRow,
  history_sequence: one Time,
  history_time: one Time
}

sig DurableCommit {
  commit_workflow: lone Workflow,
  commit_step: lone Step,
  commit_outbox: lone OutboxMessage,
  commit_command: lone ObjectCommand,
  commit_worker: lone Worker,
  commit_kind: one CommitKind,
  commit_time: one Time
}

sig WakeEvent {
  wake_wait: one Wait,
  wake_signal: one Signal,
  wake_time: one Time
}

sig OutboxAck {
  ack_message: one OutboxMessage,
  ack_worker: one Worker,
  ack_time: one Time
}

pred terminal[s: WorkflowStatus] {
  s in (Completed + Cancelled + Terminated)
}

pred terminalWorkflow[wf: Workflow, t: Time] {
  some workflowStatus[wf, t] and workflowStatus[wf, t] in (Completed + Cancelled + Terminated)
  or (workflowStatus[wf, t] = Failed and no workflowNextRun[wf, t])
}

fun workflowStatus[w: Workflow, t: Time]: set WorkflowStatus {
  ((wr_workflow.w) & (wr_time.t)).wr_status
}

fun workflowNextRun[w: Workflow, t: Time]: set Time {
  ((wr_workflow.w) & (wr_time.t)).wr_nextRunAt
}

fun stepStatus[s: Step, t: Time]: set StepStatus {
  ((sr_step.s) & (sr_time.t)).sr_status
}

fun attemptStatus[a: Attempt, t: Time]: set AttemptStatus {
  ((ar_attempt.a) & (ar_time.t)).ar_status
}

fun waitStatus[w: Wait, t: Time]: set WaitStatus {
  ((wait_row.w) & (wait_time.t)).wait_status
}

fun fenceStatus[f: Fence, t: Time]: set FenceStatus {
  ((fence_row.f) & (fence_time.t)).fence_status
}

fun outboxStatus[o: OutboxMessage, t: Time]: set OutboxStatus {
  ((outbox_row.o) & (outbox_time.t)).outbox_status
}

fun commandStatus[c: ObjectCommand, t: Time]: set CommandStatus {
  ((command_row.c) & (command_time.t)).command_status
}

pred liveWorkflowLease[w: Workflow, worker: Worker, t: Time] {
  some l: LeaseRow | l.lr_workflow = w and l.lr_worker = worker and l.lr_time = t and gt[l.lr_expiresAt, t]
}

pred liveOutboxLease[o: OutboxMessage, worker: Worker, t: Time] {
  some r: OutboxRow | r.outbox_row = o and r.outbox_status = OutboxProcessing and
    r.outbox_owner = worker and r.outbox_time = t and some r.outbox_expiresAt and gt[r.outbox_expiresAt, t]
}

pred workflowSame[w: Workflow, t: Time, tnext: Time] {
  workflowStatus[w, tnext] = workflowStatus[w, t]
  workflowNextRun[w, tnext] = workflowNextRun[w, t]
}

pred stepSame[s: Step, t: Time, tnext: Time] {
  stepStatus[s, tnext] = stepStatus[s, t]
}

pred attemptSame[a: Attempt, t: Time, tnext: Time] {
  attemptStatus[a, tnext] = attemptStatus[a, t]
}

pred waitSame[w: Wait, t: Time, tnext: Time] {
  waitStatus[w, tnext] = waitStatus[w, t]
}

pred fenceSame[f: Fence, t: Time, tnext: Time] {
  fenceStatus[f, tnext] = fenceStatus[f, t]
}

pred outboxSame[o: OutboxMessage, t: Time, tnext: Time] {
  outboxStatus[o, tnext] = outboxStatus[o, t]
}

pred commandSame[c: ObjectCommand, t: Time, tnext: Time] {
  commandStatus[c, tnext] = commandStatus[c, t]
}

fact wellFormedRows {
  all w: Workflow, t: Time | lone r: WorkflowRow | r.wr_workflow = w and r.wr_time = t
  all s: Step, t: Time | lone r: StepRow | r.sr_step = s and r.sr_time = t
  all a: Attempt, t: Time | lone r: AttemptRow | r.ar_attempt = a and r.ar_time = t
  all w: Wait, t: Time | lone r: WaitRow | r.wait_row = w and r.wait_time = t
  all f: Fence, t: Time | lone r: FenceRow | r.fence_row = f and r.fence_time = t
  all o: OutboxMessage, t: Time | lone r: OutboxRow | r.outbox_row = o and r.outbox_time = t
  all c: ObjectCommand, t: Time | lone r: CommandRow | r.command_row = c and r.command_time = t

  all s: Step, t: Time | some stepStatus[s, t] implies some workflowStatus[s.step_workflow, t]
  all a: Attempt, t: Time | some attemptStatus[a, t] implies some stepStatus[a.attempt_step, t]
  all w: Wait, t: Time | some waitStatus[w, t] implies some stepStatus[w.wait_step, t]
  all f: Fence, t: Time | some fenceStatus[f, t] implies some workflowStatus[f.fence_workflow, t]
  all o: OutboxMessage, t: Time | some outboxStatus[o, t] implies some workflowStatus[o.outbox_workflow, t]

  all l: LeaseRow | some workflowStatus[l.lr_workflow, l.lr_time] and gte[l.lr_expiresAt, l.lr_time]
}

pred init[t: Time] {
  no WorkflowRow & wr_time.t
  no StepRow & sr_time.t
  no AttemptRow & ar_time.t
  no LeaseRow & lr_time.t
  no WaitRow & wait_time.t
  no FenceRow & fence_time.t
  no OutboxRow & outbox_time.t
  no CommandRow & command_time.t
  no InboxState & inbox_time.t
  no HistoryState & history_time.t
}

pred globalFrame[t: Time, tnext: Time] {
  -- Future inbox/history rows are represented structurally in this model.
  -- The current prototype transitions do not mutate them yet.
}

pred unchangedExcept[wf: lone Workflow, st: lone Step, att: lone Attempt, wt: lone Wait, f: lone Fence, o: lone OutboxMessage, c: lone ObjectCommand, t: Time, tnext: Time] {
  all other: Workflow - wf | workflowSame[other, t, tnext]
  all other: Step - st | stepSame[other, t, tnext]
  all other: Attempt - att | attemptSame[other, t, tnext]
  all other: Wait - wt | waitSame[other, t, tnext]
  all other: Fence - f | fenceSame[other, t, tnext]
  all other: OutboxMessage - o | outboxSame[other, t, tnext]
  all other: ObjectCommand - c | commandSame[other, t, tnext]
}

pred preserveLeasesExcept[wf: lone Workflow, t: Time, tnext: Time] {
  all w: Workflow - wf, worker: Worker, exp: Time |
    (some l: LeaseRow | l.lr_workflow = w and l.lr_worker = worker and l.lr_expiresAt = exp and l.lr_time = t)
    iff
    (some l2: LeaseRow | l2.lr_workflow = w and l2.lr_worker = worker and l2.lr_expiresAt = exp and l2.lr_time = tnext)
}

pred enqueueWorkflow[wf: Workflow, t: Time, tnext: Time] {
  -- [DURABABBLE-WF-1] Enqueue persists a pending workflow before any worker can claim it.
  no workflowStatus[wf, t]
  workflowStatus[wf, tnext] = Pending
  no workflowNextRun[wf, tnext]
  unchangedExcept[wf, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred claimWorkflow[wf: Workflow, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-LEASE-1] Claim writes exactly one active owner for runnable work.
  workflowStatus[wf, t] in (Pending + Retrying) or (workflowStatus[wf, t] = Failed and some workflowNextRun[wf, t])
  no live: LeaseRow | live.lr_workflow = wf and live.lr_time = t and gt[live.lr_expiresAt, t]
  workflowStatus[wf, tnext] = Running
  no workflowNextRun[wf, tnext]
  some exp: Time | gt[exp, tnext] and one l: LeaseRow |
    l.lr_workflow = wf and l.lr_worker = worker and l.lr_time = tnext and l.lr_expiresAt = exp
  all l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext implies l.lr_worker = worker
  unchangedExcept[wf, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
  globalFrame[t, tnext]
}

pred heartbeatWorkflow[wf: Workflow, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-LEASE-2] Only the live owner may extend a lease/step heartbeat.
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, tnext] = Running
  some exp: Time | gt[exp, tnext] and one l: LeaseRow |
    l.lr_workflow = wf and l.lr_worker = worker and l.lr_time = tnext and l.lr_expiresAt = exp
  all l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext implies l.lr_worker = worker
  unchangedExcept[wf, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
  globalFrame[t, tnext]
}

pred releaseOrStealLease[wf: Workflow, t: Time, tnext: Time] {
  -- [DURABABBLE-LEASE-3] Shutdown release and expiry reclaim make the workflow runnable again.
  workflowStatus[wf, t] = Running
  some l: LeaseRow | l.lr_workflow = wf and l.lr_time = t
  workflowStatus[wf, tnext] = Pending
  no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  unchangedExcept[wf, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
  globalFrame[t, tnext]
}

pred startStep[wf: Workflow, st: Step, att: Attempt, t: Time, tnext: Time] {
  -- [DURABABBLE-STEP-2] Incomplete steps can retry by appending a fresh running attempt.
  st.step_workflow = wf
  att.attempt_step = st
  workflowStatus[wf, t] = Running
  stepStatus[st, t] != StepCompleted
  no attemptStatus[att, t]
  workflowStatus[wf, tnext] = Running
  stepStatus[st, tnext] = StepRunning
  attemptStatus[att, tnext] = AttemptRunning
  all old: Attempt - att | old.attempt_step = st and attemptStatus[old, t] = AttemptRunning implies attemptStatus[old, tnext] = AttemptFailed
  unchangedExcept[wf, st, att, none, none, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred completeStep[wf: Workflow, st: Step, att: Attempt, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-LEASE-4] Step commit is fenced by the current workflow lease owner.
  st.step_workflow = wf
  att.attempt_step = st
  liveWorkflowLease[wf, worker, t]
  stepStatus[st, t] = StepRunning
  attemptStatus[att, t] = AttemptRunning
  workflowStatus[wf, tnext] = Running
  stepStatus[st, tnext] = StepCompleted
  attemptStatus[att, tnext] = AttemptCompleted
  one c: DurableCommit | c.commit_workflow = wf and c.commit_step = st and c.commit_worker = worker and c.commit_kind = StepCommit and c.commit_time = t
  unchangedExcept[wf, st, att, none, none, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred retryStep[wf: Workflow, st: Step, att: Attempt, worker: Worker, due: Time, t: Time, tnext: Time] {
  st.step_workflow = wf
  att.attempt_step = st
  liveWorkflowLease[wf, worker, t]
  stepStatus[st, t] = StepRunning
  attemptStatus[att, t] = AttemptRunning
  workflowStatus[wf, tnext] = Retrying
  workflowNextRun[wf, tnext] = due
  stepStatus[st, tnext] = StepFailed
  attemptStatus[att, tnext] = AttemptFailed
  no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  unchangedExcept[wf, st, att, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
  globalFrame[t, tnext]
}

pred recordWait[wf: Workflow, st: Step, att: Attempt, wait: Wait, worker: Worker, t: Time, tnext: Time] {
  st.step_workflow = wf
  att.attempt_step = st
  wait.wait_step = st
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  stepStatus[st, t] = StepRunning
  workflowStatus[wf, tnext] = Waiting
  stepStatus[st, tnext] = StepWaiting
  attemptStatus[att, tnext] = AttemptWaiting
  waitStatus[wait, tnext] = WaitPending
  no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  unchangedExcept[wf, st, att, wait, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
  globalFrame[t, tnext]
}

pred wakeWait[wf: Workflow, st: Step, wait: Wait, signal: Signal, t: Time, tnext: Time] {
  -- [DURABABBLE-WAIT-1] A pending timer/event wait completes once and wakes the workflow.
  st.step_workflow = wf
  wait.wait_step = st
  workflowStatus[wf, t] = Waiting
  stepStatus[st, t] = StepWaiting
  waitStatus[wait, t] = WaitPending
  workflowStatus[wf, tnext] = Pending
  stepStatus[st, tnext] = StepCompleted
  waitStatus[wait, tnext] = WaitCompleted
  one e: WakeEvent | e.wake_wait = wait and e.wake_signal = signal and e.wake_time = t
  unchangedExcept[wf, st, none, wait, none, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred completeWorkflow[wf: Workflow, worker: Worker, t: Time, tnext: Time] {
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  workflowStatus[wf, tnext] = Completed
  no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  one c: DurableCommit | c.commit_workflow = wf and c.commit_worker = worker and c.commit_kind = WorkflowCommit and c.commit_time = t
  unchangedExcept[wf, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
  globalFrame[t, tnext]
}

pred failCancelOrTerminateWorkflow[wf: Workflow, worker: lone Worker, status: WorkflowStatus, t: Time, tnext: Time] {
  status in (Failed + Cancelled + Terminated)
  workflowStatus[wf, t] in (Pending + Running + Waiting + Retrying)
  worker in Worker implies liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, tnext] = status
  no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  unchangedExcept[wf, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
  globalFrame[t, tnext]
}

pred resumeReplayCompletedStep[wf: Workflow, st: Step, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-STEP-1] Resume/replay returns completed step results without re-execution.
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  stepStatus[st, t] = StepCompleted
  workflowStatus[wf, tnext] = Running
  stepStatus[st, tnext] = StepCompleted
  no a: Attempt | a.attempt_step = st and attemptStatus[a, tnext] = AttemptRunning and no attemptStatus[a, t]
  unchangedExcept[wf, st, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred acquireFence[f: Fence, wf: Workflow, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-FENCE-1] Fence row is persisted before the external side effect.
  f.fence_workflow = wf
  workflowStatus[wf, t] in (Running + Completed + Pending)
  no fenceStatus[f, t]
  workflowSame[wf, t, tnext]
  fenceStatus[f, tnext] = FenceRunning
  one r: FenceRow | r.fence_row = f and r.fence_status = FenceRunning and r.fence_owner = worker and r.fence_time = tnext
  unchangedExcept[wf, none, none, none, f, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred completeFence[f: Fence, worker: Worker, t: Time, tnext: Time] {
  fenceStatus[f, t] = FenceRunning
  one r: FenceRow | r.fence_row = f and r.fence_time = t and r.fence_owner = worker
  workflowSame[f.fence_workflow, t, tnext]
  fenceStatus[f, tnext] = FenceCompleted
  one c: DurableCommit | c.commit_workflow = f.fence_workflow and c.commit_worker = worker and c.commit_kind = FenceCommit and c.commit_time = t
  unchangedExcept[f.fence_workflow, none, none, none, f, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred enqueueOutbox[o: OutboxMessage, wf: Workflow, t: Time, tnext: Time] {
  -- [DURABABBLE-OUTBOX-1] Outbox keys map to one durable message that is leased before ack.
  o.outbox_workflow = wf
  no outboxStatus[o, t]
  workflowSame[wf, t, tnext]
  outboxStatus[o, tnext] = OutboxPending
  unchangedExcept[wf, none, none, none, none, o, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred claimOutbox[o: OutboxMessage, worker: Worker, t: Time, tnext: Time] {
  outboxStatus[o, t] in (OutboxPending + OutboxProcessing)
  workflowSame[o.outbox_workflow, t, tnext]
  outboxStatus[o, tnext] = OutboxProcessing
  one r: OutboxRow | r.outbox_row = o and r.outbox_status = OutboxProcessing and r.outbox_owner = worker and r.outbox_time = tnext and some r.outbox_expiresAt and gt[r.outbox_expiresAt, tnext]
  unchangedExcept[o.outbox_workflow, none, none, none, none, o, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred ackOutbox[o: OutboxMessage, worker: Worker, t: Time, tnext: Time] {
  liveOutboxLease[o, worker, t]
  workflowSame[o.outbox_workflow, t, tnext]
  outboxStatus[o, tnext] = OutboxAcked
  one a: OutboxAck | a.ack_message = o and a.ack_worker = worker and a.ack_time = t
  unchangedExcept[o.outbox_workflow, none, none, none, none, o, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred enqueueObjectCommand[cmd: ObjectCommand, t: Time, tnext: Time] {
  -- [DURABABBLE-OBJ-1] Durable-object commands serialize by target identity.
  no commandStatus[cmd, t]
  commandStatus[cmd, tnext] = CommandPending
  some inbox: InboxState | inbox.inbox_row.inbox_target = cmd.command_target and inbox.inbox_time = tnext
  unchangedExcept[none, none, none, none, none, none, cmd, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred claimObjectCommand[cmd: ObjectCommand, worker: Worker, t: Time, tnext: Time] {
  commandStatus[cmd, t] in (CommandPending + CommandFailed)
  no other: ObjectCommand - cmd | other.command_target = cmd.command_target and commandStatus[other, t] = CommandRunning
  commandStatus[cmd, tnext] = CommandRunning
  one r: CommandRow | r.command_row = cmd and r.command_status = CommandRunning and r.command_owner = worker and r.command_time = tnext
  unchangedExcept[none, none, none, none, none, none, cmd, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred completeObjectCommand[cmd: ObjectCommand, worker: Worker, t: Time, tnext: Time] {
  commandStatus[cmd, t] = CommandRunning
  one r: CommandRow | r.command_row = cmd and r.command_time = t and r.command_owner = worker
  commandStatus[cmd, tnext] = CommandCompleted
  one c: DurableCommit | c.commit_command = cmd and c.commit_worker = worker and c.commit_kind = ObjectCommandCommit and c.commit_time = t
  unchangedExcept[none, none, none, none, none, none, cmd, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred stutter[t: Time, tnext: Time] {
  all w: Workflow | workflowSame[w, t, tnext]
  all s: Step | stepSame[s, t, tnext]
  all a: Attempt | attemptSame[a, t, tnext]
  all w: Wait | waitSame[w, t, tnext]
  all f: Fence | fenceSame[f, t, tnext]
  all o: OutboxMessage | outboxSame[o, t, tnext]
  all c: ObjectCommand | commandSame[c, t, tnext]
  preserveLeasesExcept[none, t, tnext]
  globalFrame[t, tnext]
}

pred step[t: Time, tnext: Time] {
  stutter[t, tnext]
  or some wf: Workflow | enqueueWorkflow[wf, t, tnext]
  or some wf: Workflow, worker: Worker | claimWorkflow[wf, worker, t, tnext]
  or some wf: Workflow, worker: Worker | heartbeatWorkflow[wf, worker, t, tnext]
  or some wf: Workflow | releaseOrStealLease[wf, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt | startStep[wf, st, att, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt, worker: Worker | completeStep[wf, st, att, worker, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt, worker: Worker, due: Time | retryStep[wf, st, att, worker, due, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt, wait: Wait, worker: Worker | recordWait[wf, st, att, wait, worker, t, tnext]
  or some wf: Workflow, st: Step, wait: Wait, signal: Signal | wakeWait[wf, st, wait, signal, t, tnext]
  or some wf: Workflow, worker: Worker | completeWorkflow[wf, worker, t, tnext]
  or some wf: Workflow, status: WorkflowStatus | failCancelOrTerminateWorkflow[wf, none, status, t, tnext]
  or some wf: Workflow, worker: Worker, status: WorkflowStatus | failCancelOrTerminateWorkflow[wf, worker, status, t, tnext]
  or some wf: Workflow, st: Step, worker: Worker | resumeReplayCompletedStep[wf, st, worker, t, tnext]
  or some f: Fence, wf: Workflow, worker: Worker | acquireFence[f, wf, worker, t, tnext]
  or some f: Fence, worker: Worker | completeFence[f, worker, t, tnext]
  or some o: OutboxMessage, wf: Workflow | enqueueOutbox[o, wf, t, tnext]
  or some o: OutboxMessage, worker: Worker | claimOutbox[o, worker, t, tnext]
  or some o: OutboxMessage, worker: Worker | ackOutbox[o, worker, t, tnext]
  or some cmd: ObjectCommand | enqueueObjectCommand[cmd, t, tnext]
  or some cmd: ObjectCommand, worker: Worker | claimObjectCommand[cmd, worker, t, tnext]
  or some cmd: ObjectCommand, worker: Worker | completeObjectCommand[cmd, worker, t, tnext]
}

fact traces {
  init[first]
  all t: Time - last | step[t, t.next]
}

/**
 * [DURABABBLE-LEASE-1] At most one live owner can hold a workflow lease.
 */
assert atMostOneLiveOwner {
  all wf: Workflow, t: Time | lone worker: Worker | liveWorkflowLease[wf, worker, t]
}

/**
 * [DURABABBLE-WF-1] Terminal workflow states do not mutate after completion,
 * failure, cancellation, or termination.
 */
assert terminalStatesDoNotMutate {
  all wf: Workflow, t: Time - last |
    terminalWorkflow[wf, t]
    implies workflowStatus[wf, t.next] = workflowStatus[wf, t]
}

/**
 * [DURABABBLE-STEP-1] Completed steps are never re-executed.
 */
assert completedStepsAreNotReexecuted {
  all st: Step, t: Time - last |
    stepStatus[st, t] = StepCompleted implies stepStatus[st, t.next] = StepCompleted
}

/**
 * [DURABABBLE-STEP-2] Retried incomplete steps append attempts instead of
 * removing previous attempt history.
 */
assert incompleteStepsRetrySafely {
  all att: Attempt, t: Time - last |
    some attemptStatus[att, t] implies some attemptStatus[att, t.next]
}

/**
 * [DURABABBLE-WAIT-1] A wait can produce at most one durable wake event.
 */
assert waitsWakeOnce {
  all w: Wait | lone t: Time - last | waitStatus[w, t] = WaitPending and waitStatus[w, t.next] = WaitCompleted
  all w: Wait, t: Time - last | waitStatus[w, t] = WaitCompleted implies waitStatus[w, t.next] = WaitCompleted
}

/**
 * [DURABABBLE-LEASE-4] Stale workflow owners cannot commit step or workflow
 * results.
 */
assert staleOwnersCannotCommit {
  all wf: Workflow, worker: Worker, t: Time - last |
    completeWorkflow[wf, worker, t, t.next] implies liveWorkflowLease[wf, worker, t]
  all wf: Workflow, st: Step, att: Attempt, worker: Worker, t: Time - last |
    completeStep[wf, st, att, worker, t, t.next] implies liveWorkflowLease[wf, worker, t]
  all wf: Workflow, st: Step, att: Attempt, wait: Wait, worker: Worker, t: Time - last |
    recordWait[wf, st, att, wait, worker, t, t.next] implies liveWorkflowLease[wf, worker, t]
}

/**
 * [DURABABBLE-FENCE-1] A side-effect fence has one running owner and one
 * completed result.
 */
assert idempotencyFencesPreventDuplicateSideEffects {
  all f: Fence, t: Time | lone r: FenceRow | r.fence_row = f and r.fence_time = t and r.fence_status = FenceRunning
  all f: Fence | lone t: Time - last | fenceStatus[f, t] = FenceRunning and fenceStatus[f, t.next] = FenceCompleted
}

/**
 * [DURABABBLE-OUTBOX-1] Outbox acknowledgement requires the current outbox
 * lease owner, and acknowledgement is final.
 */
assert outboxAckLeaseBehaviorIsSafe {
  all o: OutboxMessage, worker: Worker, t: Time - last |
    ackOutbox[o, worker, t, t.next] implies liveOutboxLease[o, worker, t]
  all o: OutboxMessage, t: Time - last | outboxStatus[o, t] = OutboxAcked implies outboxStatus[o, t.next] = OutboxAcked
}

/**
 * [DURABABBLE-OBJ-1] Durable-object command execution is serialized for a
 * target identity.
 */
assert durableObjectCommandSerializationHolds {
  all target: ObjectTarget, t: Time |
    lone cmd: ObjectCommand | cmd.command_target = target and commandStatus[cmd, t] = CommandRunning
}

pred exampleWorkflowCompletes {
  some wf: Workflow, worker: Worker | {
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    completeWorkflow[wf, worker, first.next.next, first.next.next.next]
    workflowStatus[wf, first.next.next.next] = Completed
  }
}

pred exampleLeaseStealAndReplay {
  some wf: Workflow, st: Step, att: Attempt, worker1, worker2: Worker | {
    worker1 != worker2
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker1, first.next, first.next.next]
    releaseOrStealLease[wf, first.next.next, first.next.next.next]
    claimWorkflow[wf, worker2, first.next.next.next, first.next.next.next.next]
    startStep[wf, st, att, first.next.next.next.next, first.next.next.next.next.next]
  }
}

pred exampleWaitWake {
  some wf: Workflow, st: Step, att: Attempt, wait: Wait, signal: Signal, worker: Worker | {
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    startStep[wf, st, att, first.next.next, first.next.next.next]
    recordWait[wf, st, att, wait, worker, first.next.next.next, first.next.next.next.next]
    wakeWait[wf, st, wait, signal, first.next.next.next.next, first.next.next.next.next.next]
  }
}

pred exampleRetryBackoff {
  some wf: Workflow, st: Step, att: Attempt, worker: Worker, due: Time | {
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    startStep[wf, st, att, first.next.next, first.next.next.next]
    retryStep[wf, st, att, worker, due, first.next.next.next, first.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next] = Retrying
  }
}

pred exampleStepStart {
  some wf: Workflow, st: Step, att: Attempt, worker: Worker | {
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    startStep[wf, st, att, first.next.next, first.next.next.next]
  }
}

pred exampleFenceOutboxObject {
  some wf: Workflow, worker: Worker, f: Fence, o: OutboxMessage, target: ObjectTarget, cmd: ObjectCommand | {
    cmd.command_target = target
    enqueueWorkflow[wf, first, first.next]
    acquireFence[f, wf, worker, first.next, first.next.next]
    completeFence[f, worker, first.next.next, first.next.next.next]
    enqueueOutbox[o, wf, first.next.next.next, first.next.next.next.next]
    claimOutbox[o, worker, first.next.next.next.next, first.next.next.next.next.next]
    ackOutbox[o, worker, first.next.next.next.next.next, first.next.next.next.next.next.next]
    enqueueObjectCommand[cmd, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
    claimObjectCommand[cmd, worker, first.next.next.next.next.next.next.next, first.next.next.next.next.next.next.next.next]
    completeObjectCommand[cmd, worker, first.next.next.next.next.next.next.next.next, last]
  }
}

run exampleWorkflowCompletes for 8 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 6 Time
run exampleLeaseStealAndReplay for 10 but exactly 1 Workflow, 2 Worker, 1 Step, 1 Attempt, 8 Time
run exampleWaitWake for 10 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 Wait, 1 Signal, 8 Time
run exampleRetryBackoff for 8 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 6 Time
run exampleStepStart for 6 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 5 Time
run exampleFenceOutboxObject for 12 but exactly 1 Workflow, 1 Worker, 1 Fence, 1 OutboxMessage, 1 ObjectTarget, 1 ObjectCommand, 10 Time

check atMostOneLiveOwner for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 6 Time
check terminalStatesDoNotMutate for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 6 Time
check completedStepsAreNotReexecuted for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 6 Time
check incompleteStepsRetrySafely for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 6 Time
check waitsWakeOnce for 4 but 2 Workflow, 2 Worker, 2 Step, 3 Attempt, 3 Wait, 2 Signal, 6 Time
check staleOwnersCannotCommit for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 6 Time
check idempotencyFencesPreventDuplicateSideEffects for 4 but 2 Workflow, 2 Worker, 2 Fence, 6 Time
check outboxAckLeaseBehaviorIsSafe for 4 but 2 Workflow, 2 Worker, 3 OutboxMessage, 6 Time
check durableObjectCommandSerializationHolds for 4 but 2 ObjectTarget, 4 ObjectCommand, 3 Worker, 6 Time
