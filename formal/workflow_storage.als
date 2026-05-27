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
sig WaitTrigger {}
sig Fence { fence_workflow: one Workflow }
sig FenceToken {}
sig OutboxMessage { outbox_workflow: one Workflow }
abstract sig TargetKind {}
one sig ObjectInbox, WorkflowInbox extends TargetKind {}
sig InboxTarget {
  target_kind: one TargetKind,
  target_workflow: lone Workflow
}
sig InboxCommand {
  command_target: one InboxTarget,
  command_sequence: one Time
}
sig WorkflowCommand {
  wc_workflow: one Workflow,
  wc_step: one Step
}
sig CommandShape {}

abstract sig WorkflowStatus {}
one sig Pending, Running, Waiting, Canceling, Completed, Failed, Canceled extends WorkflowStatus {}

abstract sig StepStatus {}
one sig StepScheduled, StepRunning, StepWaiting, StepCompleted, StepFailed, StepCanceled extends StepStatus {}

abstract sig AttemptStatus {}
one sig AttemptRunning, AttemptWaiting, AttemptCompleted, AttemptFailed, AttemptCanceled extends AttemptStatus {}

abstract sig WaitStatus {}
one sig WaitPending, WaitCompleted, WaitCanceled extends WaitStatus {}

abstract sig FenceStatus {}
one sig FenceRunning, FenceCompleted, FenceFailed extends FenceStatus {}

abstract sig OutboxStatus {}
one sig OutboxPending, OutboxProcessing, OutboxAcked extends OutboxStatus {}

abstract sig CommandStatus {}
one sig CommandPending, CommandRunning, CommandCompleted, CommandFailed, CommandDeadLettered extends CommandStatus {}

abstract sig ActivationStatus {}
one sig ActivationPending, ActivationRunning extends ActivationStatus {}

abstract sig CommandHistoryKind {}
one sig CommandScheduled, CommandStarted, CommandSucceeded, CommandWaiting, CommandCanceled, CommandRejected, CommandErrored extends CommandHistoryKind {}

abstract sig CommitKind {}
one sig WorkflowCommit, StepCommit, WaitCommit, FenceCommit, OutboxCommit, InboxCommandCommit extends CommitKind {}

sig WorkflowRow {
  wr_workflow: one Workflow,
  wr_status: one WorkflowStatus,
  wr_nextRunAt: lone Time,
  wr_cancelRequestedAt: lone Time,
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
  wait_trigger: lone WaitTrigger,
  wait_time: one Time
}

sig FenceRow {
  fence_row: one Fence,
  fence_status: one FenceStatus,
  fence_owner: lone FenceToken,
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
  command_row: one InboxCommand,
  command_status: one CommandStatus,
  command_owner: lone Worker,
  command_expiresAt: lone Time,
  command_time: one Time
}

sig TargetActivationRow {
  activation_target: one InboxTarget,
  activation_status: one ActivationStatus,
  activation_owner: lone Worker,
  activation_expiresAt: lone Time,
  activation_time: one Time
}

sig CommandHistoryRow {
  chr_command: one WorkflowCommand,
  chr_kind: one CommandHistoryKind,
  chr_shape: lone CommandShape,
  chr_sequence: one Time,
  chr_time: one Time
}

sig DurableCommit {
  commit_workflow: lone Workflow,
  commit_step: lone Step,
  commit_outbox: lone OutboxMessage,
  commit_command: lone InboxCommand,
  commit_worker: lone Worker,
  commit_kind: one CommitKind,
  commit_time: one Time
}

sig WakeEvent {
  wake_wait: one Wait,
  wake_trigger: one WaitTrigger,
  wake_time: one Time
}

sig OutboxAck {
  ack_message: one OutboxMessage,
  ack_worker: one Worker,
  ack_time: one Time
}

pred terminal[s: WorkflowStatus] {
  s in (Completed + Canceled)
}

pred terminalWorkflow[wf: Workflow, t: Time] {
  some workflowStatus[wf, t]
  workflowStatus[wf, t] in (Completed + Canceled)
  or (workflowStatus[wf, t] = Failed and no workflowNextRun[wf, t])
}

pred closedWorkflow[wf: Workflow, t: Time] {
  some workflowStatus[wf, t]
  workflowStatus[wf, t] in (Completed + Canceled)
}

fun workflowStatus[w: Workflow, t: Time]: set WorkflowStatus {
  ((wr_workflow.w) & (wr_time.t)).wr_status
}

fun workflowNextRun[w: Workflow, t: Time]: set Time {
  ((wr_workflow.w) & (wr_time.t)).wr_nextRunAt
}

fun workflowCancelRequestedAt[w: Workflow, t: Time]: set Time {
  ((wr_workflow.w) & (wr_time.t)).wr_cancelRequestedAt
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

fun commandStatus[c: InboxCommand, t: Time]: set CommandStatus {
  ((command_row.c) & (command_time.t)).command_status
}

fun activationStatus[target: InboxTarget, t: Time]: set ActivationStatus {
  ((activation_target.target) & (activation_time.t)).activation_status
}

fun commandScheduledShape[c: WorkflowCommand, t: Time]: set CommandShape {
  ((chr_command.c) & (chr_kind.CommandScheduled) & (chr_time.t)).chr_shape
}

fun commandHistorySequence[c: WorkflowCommand, kind: CommandHistoryKind, t: Time]: set Time {
  ((chr_command.c) & (chr_kind.kind) & (chr_time.t)).chr_sequence
}

fun terminalCommandHistoryKinds: set CommandHistoryKind {
  CommandSucceeded + CommandWaiting + CommandCanceled + CommandRejected
}

fun terminalCommandHistoryRows[c: WorkflowCommand, t: Time]: set CommandHistoryRow {
  { r: CommandHistoryRow | r.chr_command = c and r.chr_time = t and r.chr_kind in terminalCommandHistoryKinds }
}

fun latestTerminalCommandHistoryRows[c: WorkflowCommand, t: Time]: set CommandHistoryRow {
  { r: terminalCommandHistoryRows[c, t] | no later: terminalCommandHistoryRows[c, t] | lt[r.chr_sequence, later.chr_sequence] }
}

fun latestTerminalCommandHistoryKind[c: WorkflowCommand, t: Time]: set CommandHistoryKind {
  latestTerminalCommandHistoryRows[c, t].chr_kind
}

pred liveWorkflowLease[w: Workflow, worker: Worker, t: Time] {
  some l: LeaseRow | l.lr_workflow = w and l.lr_worker = worker and l.lr_time = t and gt[l.lr_expiresAt, t]
}

pred liveOutboxLease[o: OutboxMessage, worker: Worker, t: Time] {
  some r: OutboxRow | r.outbox_row = o and r.outbox_status = OutboxProcessing and
    r.outbox_owner = worker and r.outbox_time = t and some r.outbox_expiresAt and gt[r.outbox_expiresAt, t]
}

pred liveCommandLease[cmd: InboxCommand, worker: Worker, t: Time] {
  some r: CommandRow | r.command_row = cmd and r.command_status = CommandRunning and
    r.command_owner = worker and r.command_time = t and some r.command_expiresAt and gt[r.command_expiresAt, t]
}

pred liveTargetActivation[target: InboxTarget, worker: Worker, t: Time] {
  some r: TargetActivationRow | r.activation_target = target and r.activation_status = ActivationRunning and
    r.activation_owner = worker and r.activation_time = t and some r.activation_expiresAt and gt[r.activation_expiresAt, t]
}

pred workflowInboxLeaseHeld[cmd: InboxCommand, worker: Worker, t: Time] {
  cmd.command_target.target_kind = ObjectInbox
  or liveWorkflowLease[cmd.command_target.target_workflow, worker, t]
}

pred workflowSame[w: Workflow, t: Time, tnext: Time] {
  workflowStatus[w, tnext] = workflowStatus[w, t]
  workflowNextRun[w, tnext] = workflowNextRun[w, t]
  workflowCancelRequestedAt[w, tnext] = workflowCancelRequestedAt[w, t]
}

pred workflowCancelSame[w: Workflow, t: Time, tnext: Time] {
  workflowCancelRequestedAt[w, tnext] = workflowCancelRequestedAt[w, t]
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
  ((fence_row.f) & (fence_time.tnext)).fence_owner = ((fence_row.f) & (fence_time.t)).fence_owner
}

pred outboxSame[o: OutboxMessage, t: Time, tnext: Time] {
  outboxStatus[o, tnext] = outboxStatus[o, t]
  ((outbox_row.o) & (outbox_time.tnext)).outbox_owner = ((outbox_row.o) & (outbox_time.t)).outbox_owner
  ((outbox_row.o) & (outbox_time.tnext)).outbox_expiresAt = ((outbox_row.o) & (outbox_time.t)).outbox_expiresAt
}

pred commandSame[c: InboxCommand, t: Time, tnext: Time] {
  commandStatus[c, tnext] = commandStatus[c, t]
  ((command_row.c) & (command_time.tnext)).command_owner = ((command_row.c) & (command_time.t)).command_owner
  ((command_row.c) & (command_time.tnext)).command_expiresAt = ((command_row.c) & (command_time.t)).command_expiresAt
}

pred targetActivationSame[target: InboxTarget, t: Time, tnext: Time] {
  activationStatus[target, tnext] = activationStatus[target, t]
  ((activation_target.target) & (activation_time.tnext)).activation_owner = ((activation_target.target) & (activation_time.t)).activation_owner
  ((activation_target.target) & (activation_time.tnext)).activation_expiresAt = ((activation_target.target) & (activation_time.t)).activation_expiresAt
}

pred commandHistorySame[c: WorkflowCommand, t: Time, tnext: Time] {
  all old: CommandHistoryRow | old.chr_command = c and old.chr_time = t implies
    some new: CommandHistoryRow |
      new.chr_command = c and
      new.chr_kind = old.chr_kind and
      new.chr_shape = old.chr_shape and
      new.chr_sequence = old.chr_sequence and
      new.chr_time = tnext
  all new: CommandHistoryRow | new.chr_command = c and new.chr_time = tnext implies
    some old: CommandHistoryRow |
      old.chr_command = c and
      old.chr_kind = new.chr_kind and
      old.chr_shape = new.chr_shape and
      old.chr_sequence = new.chr_sequence and
      old.chr_time = t
}

pred commandHistoryAppend[c: WorkflowCommand, kind: CommandHistoryKind, shape: lone CommandShape, sequence: Time, t: Time, tnext: Time] {
  all old: CommandHistoryRow | old.chr_command = c and old.chr_time = t implies
    some new: CommandHistoryRow |
      new.chr_command = c and
      new.chr_kind = old.chr_kind and
      new.chr_shape = old.chr_shape and
      new.chr_sequence = old.chr_sequence and
      new.chr_time = tnext
  one appended: CommandHistoryRow |
    appended.chr_command = c and
    appended.chr_kind = kind and
    appended.chr_shape = shape and
    appended.chr_sequence = sequence and
    appended.chr_time = tnext
  all new: CommandHistoryRow | new.chr_command = c and new.chr_time = tnext implies {
    (
      new.chr_kind = kind and
      new.chr_shape = shape and
      new.chr_sequence = sequence
    ) or (
      some old: CommandHistoryRow |
        old.chr_command = c and
        old.chr_kind = new.chr_kind and
        old.chr_shape = new.chr_shape and
        old.chr_sequence = new.chr_sequence and
        old.chr_time = t
    )
  }
}

fact wellFormedRows {
  all w: Workflow, t: Time | lone r: WorkflowRow | r.wr_workflow = w and r.wr_time = t
  all s: Step, t: Time | lone r: StepRow | r.sr_step = s and r.sr_time = t
  all a: Attempt, t: Time | lone r: AttemptRow | r.ar_attempt = a and r.ar_time = t
  all w: Wait, t: Time | lone r: WaitRow | r.wait_row = w and r.wait_time = t
  all f: Fence, t: Time | lone r: FenceRow | r.fence_row = f and r.fence_time = t
  all o: OutboxMessage, t: Time | lone r: OutboxRow | r.outbox_row = o and r.outbox_time = t
  all c: InboxCommand, t: Time | lone r: CommandRow | r.command_row = c and r.command_time = t
  all target: InboxTarget, t: Time | lone r: TargetActivationRow | r.activation_target = target and r.activation_time = t
  all disj c1, c2: InboxCommand | c1.command_target = c2.command_target implies c1.command_sequence != c2.command_sequence
  all target: InboxTarget | target.target_kind = WorkflowInbox iff one target.target_workflow
  all target: InboxTarget | target.target_kind = ObjectInbox iff no target.target_workflow
  all cmd: WorkflowCommand | cmd.wc_step.step_workflow = cmd.wc_workflow
  all st: Step | lone cmd: WorkflowCommand | cmd.wc_step = st
  all cmd: WorkflowCommand, t, sequence: Time, kind: CommandHistoryKind |
    lone r: CommandHistoryRow | r.chr_command = cmd and r.chr_time = t and r.chr_sequence = sequence and r.chr_kind = kind

  all s: Step, t: Time | some stepStatus[s, t] implies some workflowStatus[s.step_workflow, t]
  all a: Attempt, t: Time | some attemptStatus[a, t] implies some stepStatus[a.attempt_step, t]
  all w: Wait, t: Time | some waitStatus[w, t] implies some stepStatus[w.wait_step, t]
  all f: Fence, t: Time | some fenceStatus[f, t] implies some workflowStatus[f.fence_workflow, t]
  all o: OutboxMessage, t: Time | some outboxStatus[o, t] implies some workflowStatus[o.outbox_workflow, t]
  all r: CommandHistoryRow | some workflowStatus[r.chr_command.wc_workflow, r.chr_time]
  all r: CommandHistoryRow | (r.chr_kind = CommandScheduled) iff (one r.chr_shape)

  all r: WorkflowRow | some r.wr_cancelRequestedAt implies gte[r.wr_time, r.wr_cancelRequestedAt]
  all l: LeaseRow | some workflowStatus[l.lr_workflow, l.lr_time] and gte[l.lr_expiresAt, l.lr_time]
  all r: CommandRow | some r.command_expiresAt implies gte[r.command_expiresAt, r.command_time]
  all r: TargetActivationRow | some r.activation_expiresAt implies gte[r.activation_expiresAt, r.activation_time]
}

fact durableEventsComeFromRows {
  all c: DurableCommit | c.commit_time != last
  all c: DurableCommit | c.commit_kind = WorkflowCommit implies {
    no c.commit_step
    no c.commit_outbox
    no c.commit_command
    completeWorkflow[c.commit_workflow, c.commit_worker, c.commit_time, c.commit_time.next]
  }
  all c: DurableCommit | c.commit_kind = StepCommit implies {
    no c.commit_outbox
    no c.commit_command
    some att: Attempt | completeStep[c.commit_workflow, c.commit_step, att, c.commit_worker, c.commit_time, c.commit_time.next]
  }
  all c: DurableCommit | c.commit_kind = WaitCommit implies {
    no c.commit_outbox
    no c.commit_command
    some wait: Wait | {
      recordWait[c.commit_workflow, c.commit_step, none, wait, c.commit_worker, c.commit_time, c.commit_time.next]
      or some att: Attempt | recordWait[c.commit_workflow, c.commit_step, att, wait, c.commit_worker, c.commit_time, c.commit_time.next]
    }
  }
  all c: DurableCommit | c.commit_kind = FenceCommit implies {
    no c.commit_step
    no c.commit_outbox
    no c.commit_command
    some f: Fence, token: FenceToken | f.fence_workflow = c.commit_workflow and completeFence[f, token, c.commit_worker, c.commit_time, c.commit_time.next]
  }
  all c: DurableCommit | c.commit_kind = OutboxCommit implies {
    some c.commit_outbox
    no c.commit_step
    no c.commit_command
  }
  all c: DurableCommit | c.commit_kind = InboxCommandCommit implies {
    no c.commit_workflow
    no c.commit_step
    no c.commit_outbox
    completeInboxCommand[c.commit_command, c.commit_worker, c.commit_time, c.commit_time.next]
  }

  all e: WakeEvent | e.wake_time != last
  all e: WakeEvent | some wf: Workflow, st: Step | {
    wakeWait[wf, st, none, e.wake_wait, e.wake_trigger, e.wake_time, e.wake_time.next]
    or some att: Attempt | wakeWait[wf, st, att, e.wake_wait, e.wake_trigger, e.wake_time, e.wake_time.next]
  }

  all a: OutboxAck | a.ack_time != last
  all a: OutboxAck | ackOutbox[a.ack_message, a.ack_worker, a.ack_time, a.ack_time.next]
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
  no TargetActivationRow & activation_time.t
  no CommandHistoryRow & chr_time.t
}


pred unchangedExcept[wf: lone Workflow, st: lone Step, att: lone Attempt, wt: lone Wait, f: lone Fence, o: lone OutboxMessage, c: lone InboxCommand, target: lone InboxTarget, t: Time, tnext: Time] {
  all other: Workflow - wf | workflowSame[other, t, tnext]
  all other: Step - st | stepSame[other, t, tnext]
  all other: Attempt - att | attemptSame[other, t, tnext]
  all other: Wait - wt | waitSame[other, t, tnext]
  all other: Fence - f | fenceSame[other, t, tnext]
  all other: OutboxMessage - o | outboxSame[other, t, tnext]
  all other: InboxCommand - c | commandSame[other, t, tnext]
  all other: InboxTarget - target | targetActivationSame[other, t, tnext]
  all cmd: WorkflowCommand | commandHistorySame[cmd, t, tnext]
}

pred unchangedExceptHistory[wf: lone Workflow, st: lone Step, att: lone Attempt, wt: lone Wait, f: lone Fence, o: lone OutboxMessage, c: lone InboxCommand, target: lone InboxTarget, hist: lone WorkflowCommand, t: Time, tnext: Time] {
  all other: Workflow - wf | workflowSame[other, t, tnext]
  all other: Step - st | stepSame[other, t, tnext]
  all other: Attempt - att | attemptSame[other, t, tnext]
  all other: Wait - wt | waitSame[other, t, tnext]
  all other: Fence - f | fenceSame[other, t, tnext]
  all other: OutboxMessage - o | outboxSame[other, t, tnext]
  all other: InboxCommand - c | commandSame[other, t, tnext]
  all other: InboxTarget - target | targetActivationSame[other, t, tnext]
  all cmd: WorkflowCommand - hist | commandHistorySame[cmd, t, tnext]
}

pred unchangedExceptStartStepHistory[wf: Workflow, st: Step, att: Attempt, hist: WorkflowCommand, t: Time, tnext: Time] {
  all other: Workflow - wf | workflowSame[other, t, tnext]
  all other: Step - st | stepSame[other, t, tnext]
  all other: Attempt - att | {
    other.attempt_step = st and attemptStatus[other, t] = AttemptRunning implies attemptStatus[other, tnext] = AttemptFailed
    not (other.attempt_step = st and attemptStatus[other, t] = AttemptRunning) implies attemptSame[other, t, tnext]
  }
  all other: Wait | waitSame[other, t, tnext]
  all other: Fence | fenceSame[other, t, tnext]
  all other: OutboxMessage | outboxSame[other, t, tnext]
  all other: InboxCommand | commandSame[other, t, tnext]
  all other: InboxTarget | targetActivationSame[other, t, tnext]
  all cmd: WorkflowCommand - hist | commandHistorySame[cmd, t, tnext]
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
  no workflowCancelRequestedAt[wf, tnext]
  unchangedExcept[wf, none, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred claimWorkflow[wf: Workflow, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-LEASE-1] Claim writes exactly one active owner for runnable work.
  (
    workflowStatus[wf, t] = Pending and (no workflowNextRun[wf, t] or some due: workflowNextRun[wf, t] | not gt[due, t])
  )
  or (
    workflowStatus[wf, t] = Failed and some due: workflowNextRun[wf, t] | not gt[due, t]
  )
  or (
    workflowStatus[wf, t] = Canceling and (no workflowNextRun[wf, t] or some due: workflowNextRun[wf, t] | not gt[due, t])
  )
  or (
    workflowStatus[wf, t] = Running and no owner: Worker | liveWorkflowLease[wf, owner, t]
  )
  no live: LeaseRow | live.lr_workflow = wf and live.lr_time = t and gt[live.lr_expiresAt, t]
  workflowStatus[wf, tnext] = Running
  no workflowNextRun[wf, tnext]
  some exp: Time | gt[exp, tnext] and one l: LeaseRow |
    l.lr_workflow = wf and l.lr_worker = worker and l.lr_time = tnext and l.lr_expiresAt = exp
  all l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext implies l.lr_worker = worker
  workflowCancelSame[wf, t, tnext]
  unchangedExcept[wf, none, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
}

pred heartbeatWorkflow[wf: Workflow, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-LEASE-2] Only the live owner may extend a lease/step heartbeat.
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, tnext] = Running
  workflowCancelSame[wf, t, tnext]
  some exp: Time | gt[exp, tnext] and one l: LeaseRow |
    l.lr_workflow = wf and l.lr_worker = worker and l.lr_time = tnext and l.lr_expiresAt = exp
  all l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext implies l.lr_worker = worker
  unchangedExcept[wf, none, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
}

pred releaseOrStealLease[wf: Workflow, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-LEASE-3] Shutdown release and expiry reclaim make the workflow runnable again.
  workflowStatus[wf, t] = Running
  some l: LeaseRow | l.lr_workflow = wf and l.lr_time = t and (
    (l.lr_worker = worker and gt[l.lr_expiresAt, t])
    or not gt[l.lr_expiresAt, t]
  )
  some workflowCancelRequestedAt[wf, t] implies workflowStatus[wf, tnext] = Canceling
  no workflowCancelRequestedAt[wf, t] implies workflowStatus[wf, tnext] = Pending
  no workflowNextRun[wf, tnext]
  no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  workflowCancelSame[wf, t, tnext]
  unchangedExcept[wf, none, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
}

pred claimWorkflowForActivation[wf: Workflow, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-LEASE-1] The inbox-driven activation claim path (drain_workflow_inbox)
  -- may additionally wake a suspended (waiting) workflow to deliver a command, while
  -- pending/failed backoff rows remain protected by their due time exactly as in
  -- claimWorkflow. This mirrors Store#claim_workflow_for_activation.
  (
    workflowStatus[wf, t] = Pending and (no workflowNextRun[wf, t] or some due: workflowNextRun[wf, t] | not gt[due, t])
  )
  or (
    workflowStatus[wf, t] = Waiting
  )
  or (
    workflowStatus[wf, t] = Canceling and (no workflowNextRun[wf, t] or some due: workflowNextRun[wf, t] | not gt[due, t])
  )
  or (
    workflowStatus[wf, t] = Failed and some due: workflowNextRun[wf, t] | not gt[due, t]
  )
  or (
    workflowStatus[wf, t] = Running and no owner: Worker | liveWorkflowLease[wf, owner, t]
  )
  no live: LeaseRow | live.lr_workflow = wf and live.lr_time = t and gt[live.lr_expiresAt, t]
  workflowStatus[wf, tnext] = Running
  no workflowNextRun[wf, tnext]
  some exp: Time | gt[exp, tnext] and one l: LeaseRow |
    l.lr_workflow = wf and l.lr_worker = worker and l.lr_time = tnext and l.lr_expiresAt = exp
  all l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext implies l.lr_worker = worker
  workflowCancelSame[wf, t, tnext]
  unchangedExcept[wf, none, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
}

pred suspendWorkflow[wf: Workflow, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-WAIT-1] The owning worker re-suspends a running workflow back to
  -- waiting (its pending waits remain) or pending, releasing the lease. This models
  -- Store#suspend_workflow on the activation drain path.
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  some workflowCancelRequestedAt[wf, t] implies workflowStatus[wf, tnext] = Canceling
  no workflowCancelRequestedAt[wf, t] and (some wt: Wait | wt.wait_step.step_workflow = wf and waitStatus[wt, t] = WaitPending)
    implies workflowStatus[wf, tnext] = Waiting
  no workflowCancelRequestedAt[wf, t] and (no wt: Wait | wt.wait_step.step_workflow = wf and waitStatus[wt, t] = WaitPending)
    implies workflowStatus[wf, tnext] = Pending
  no workflowNextRun[wf, tnext]
  no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  workflowCancelSame[wf, t, tnext]
  unchangedExcept[wf, none, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
}

pred scheduleWorkflowCommand[wf: Workflow, cmd: WorkflowCommand, shape: CommandShape, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-CONCURRENCY-1] Concurrent workflow fibers append ordered step command history before side-effect execution.
  cmd.wc_workflow = wf
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  no r: CommandHistoryRow | r.chr_command = cmd and r.chr_time = t
  workflowSame[wf, t, tnext]
  stepStatus[cmd.wc_step, t] != StepCompleted
  stepStatus[cmd.wc_step, tnext] = StepScheduled
  all st: Step - cmd.wc_step | stepSame[st, t, tnext]
  all att: Attempt | attemptSame[att, t, tnext]
  all wait: Wait | waitSame[wait, t, tnext]
  all f: Fence | fenceSame[f, t, tnext]
  all o: OutboxMessage | outboxSame[o, t, tnext]
  all c: InboxCommand | commandSame[c, t, tnext]
  all target: InboxTarget | targetActivationSame[target, t, tnext]
  all other: WorkflowCommand - cmd | commandHistorySame[other, t, tnext]
  one r: CommandHistoryRow |
    r.chr_command = cmd and
    r.chr_kind = CommandScheduled and
    r.chr_shape = shape and
    r.chr_sequence = tnext and
    r.chr_time = tnext
  all r: CommandHistoryRow | r.chr_command = cmd and r.chr_time = tnext implies
    r.chr_kind = CommandScheduled and r.chr_shape = shape and r.chr_sequence = tnext
  preserveLeasesExcept[none, t, tnext]
}

pred startStep[wf: Workflow, st: Step, att: Attempt, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-STEP-2] Incomplete steps can retry by appending a fresh running attempt.
  st.step_workflow = wf
  att.attempt_step = st
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  stepStatus[st, t] != StepCompleted
  no attemptStatus[att, t]
  workflowStatus[wf, tnext] = Running
  workflowCancelSame[wf, t, tnext]
  stepStatus[st, tnext] = StepRunning
  attemptStatus[att, tnext] = AttemptRunning
  all old: Attempt - att | old.attempt_step = st and attemptStatus[old, t] = AttemptRunning implies attemptStatus[old, tnext] = AttemptFailed
  some cmd: WorkflowCommand | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    some commandScheduledShape[cmd, t]
    commandHistoryAppend[cmd, CommandStarted, none, tnext, t, tnext]
    unchangedExceptStartStepHistory[wf, st, att, cmd, t, tnext]
  }
  preserveLeasesExcept[none, t, tnext]
}

pred completeStep[wf: Workflow, st: Step, att: Attempt, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-LEASE-4] Step commit is fenced by the current workflow lease owner.
  st.step_workflow = wf
  att.attempt_step = st
  liveWorkflowLease[wf, worker, t]
  stepStatus[st, t] = StepRunning
  attemptStatus[att, t] = AttemptRunning
  workflowStatus[wf, tnext] = Running
  workflowCancelSame[wf, t, tnext]
  stepStatus[st, tnext] = StepCompleted
  attemptStatus[att, tnext] = AttemptCompleted
  one c: DurableCommit | c.commit_workflow = wf and c.commit_step = st and c.commit_worker = worker and c.commit_kind = StepCommit and c.commit_time = t
  some cmd: WorkflowCommand | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    some commandHistorySequence[cmd, CommandStarted, t]
    commandHistoryAppend[cmd, CommandSucceeded, none, tnext, t, tnext]
    unchangedExceptHistory[wf, st, att, none, none, none, none, none, cmd, t, tnext]
  }
  preserveLeasesExcept[none, t, tnext]
}

pred retryStep[wf: Workflow, st: Step, att: Attempt, worker: Worker, due: Time, t: Time, tnext: Time] {
  -- [DURABABBLE-STEP-2] Retryable failure history and retry backoff commit atomically.
  st.step_workflow = wf
  att.attempt_step = st
  liveWorkflowLease[wf, worker, t]
  stepStatus[st, t] = StepRunning
  attemptStatus[att, t] = AttemptRunning
  gt[due, tnext]
  some workflowCancelRequestedAt[wf, t] implies workflowStatus[wf, tnext] = Canceling
  no workflowCancelRequestedAt[wf, t] implies workflowStatus[wf, tnext] = Pending
  workflowNextRun[wf, tnext] = due
  workflowCancelSame[wf, t, tnext]
  stepStatus[st, tnext] = StepFailed
  attemptStatus[att, tnext] = AttemptFailed
  no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  some cmd: WorkflowCommand | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    some commandHistorySequence[cmd, CommandStarted, t]
    commandHistoryAppend[cmd, CommandErrored, none, tnext, t, tnext]
    unchangedExceptHistory[wf, st, att, none, none, none, none, none, cmd, t, tnext]
  }
  preserveLeasesExcept[wf, t, tnext]
}

pred failStep[wf: Workflow, st: Step, att: Attempt, worker: Worker, t: Time, tnext: Time] {
  st.step_workflow = wf
  att.attempt_step = st
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  stepStatus[st, t] = StepRunning
  attemptStatus[att, t] = AttemptRunning
  workflowStatus[wf, tnext] = Running
  workflowCancelSame[wf, t, tnext]
  stepStatus[st, tnext] = StepFailed
  attemptStatus[att, tnext] = AttemptFailed
  some cmd: WorkflowCommand | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    some commandHistorySequence[cmd, CommandStarted, t]
    commandHistoryAppend[cmd, CommandRejected, none, tnext, t, tnext]
    unchangedExceptHistory[wf, st, att, none, none, none, none, none, cmd, t, tnext]
  }
  preserveLeasesExcept[none, t, tnext]
}

pred cancelStep[wf: Workflow, st: Step, att: Attempt, worker: Worker, t: Time, tnext: Time] {
  st.step_workflow = wf
  att.attempt_step = st
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  some workflowCancelRequestedAt[wf, t]
  stepStatus[st, t] = StepRunning
  attemptStatus[att, t] = AttemptRunning
  workflowStatus[wf, tnext] = Running
  workflowCancelSame[wf, t, tnext]
  stepStatus[st, tnext] = StepCanceled
  attemptStatus[att, tnext] = AttemptCanceled
  some cmd: WorkflowCommand | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    some commandHistorySequence[cmd, CommandStarted, t]
    commandHistoryAppend[cmd, CommandCanceled, none, tnext, t, tnext]
    unchangedExceptHistory[wf, st, att, none, none, none, none, none, cmd, t, tnext]
  }
  preserveLeasesExcept[none, t, tnext]
}

pred recordWait[wf: Workflow, st: Step, att: lone Attempt, wait: Wait, worker: Worker, t: Time, tnext: Time] {
  st.step_workflow = wf
  wait.wait_step = st
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  some att implies {
    att.attempt_step = st
    stepStatus[st, t] = StepRunning
    attemptStatus[att, t] = AttemptRunning
  }
  no att implies stepStatus[st, t] = StepScheduled
  -- A wait may suspend the workflow immediately, or remain in a running activation
  -- while already-started sibling workflow fibers drain and commit.
  workflowStatus[wf, tnext] in (Waiting + Running + Canceling)
  some workflowCancelRequestedAt[wf, t] and workflowStatus[wf, tnext] != Running implies workflowStatus[wf, tnext] = Canceling
  no workflowCancelRequestedAt[wf, t] and workflowStatus[wf, tnext] != Running implies workflowStatus[wf, tnext] = Waiting
  workflowCancelSame[wf, t, tnext]
  stepStatus[st, tnext] = StepWaiting
  some att implies attemptStatus[att, tnext] = AttemptWaiting
  waitStatus[wait, tnext] = WaitPending
  workflowStatus[wf, tnext] in (Waiting + Canceling) implies no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  workflowStatus[wf, tnext] = Running implies liveWorkflowLease[wf, worker, tnext]
  some cmd: WorkflowCommand | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    some commandScheduledShape[cmd, t]
    some att implies some commandHistorySequence[cmd, CommandStarted, t]
    commandHistoryAppend[cmd, CommandWaiting, none, tnext, t, tnext]
    unchangedExceptHistory[wf, st, att, wait, none, none, none, none, cmd, t, tnext]
  }
  workflowStatus[wf, tnext] in (Waiting + Canceling) implies preserveLeasesExcept[wf, t, tnext]
  workflowStatus[wf, tnext] = Running implies preserveLeasesExcept[none, t, tnext]
}

pred wakeWait[wf: Workflow, st: Step, att: lone Attempt, wait: Wait, trigger: WaitTrigger, t: Time, tnext: Time] {
  -- [DURABABBLE-WAIT-1] A pending timer wait completes once and wakes the workflow.
  st.step_workflow = wf
  wait.wait_step = st
  workflowStatus[wf, t] in (Waiting + Running)
  stepStatus[st, t] = StepWaiting
  some att implies {
    att.attempt_step = st
    attemptStatus[att, t] = AttemptWaiting
  }
  waitStatus[wait, t] = WaitPending
  workflowStatus[wf, t] = Waiting implies workflowStatus[wf, tnext] = Pending
  workflowStatus[wf, t] = Running implies workflowStatus[wf, tnext] = Running
  workflowCancelSame[wf, t, tnext]
  stepStatus[st, tnext] = StepCompleted
  some att implies attemptStatus[att, tnext] = AttemptCompleted
  waitStatus[wait, tnext] = WaitCompleted
  one e: WakeEvent | e.wake_wait = wait and e.wake_trigger = trigger and e.wake_time = t
  some cmd: WorkflowCommand | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    some commandHistorySequence[cmd, CommandWaiting, t]
    commandHistoryAppend[cmd, CommandSucceeded, none, tnext, t, tnext]
    unchangedExceptHistory[wf, st, att, wait, none, none, none, none, cmd, t, tnext]
  }
  preserveLeasesExcept[none, t, tnext]
}

pred completeWorkflow[wf: Workflow, worker: Worker, t: Time, tnext: Time] {
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  all st: Step | st.step_workflow = wf and some stepStatus[st, t] implies stepStatus[st, t] = StepCompleted
  all att: Attempt | att.attempt_step.step_workflow = wf and some attemptStatus[att, t] implies attemptStatus[att, t] not in (AttemptRunning + AttemptWaiting)
  no wt: Wait | wt.wait_step.step_workflow = wf and waitStatus[wt, t] = WaitPending
  workflowStatus[wf, tnext] = Completed
  workflowCancelSame[wf, t, tnext]
  no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  one c: DurableCommit | c.commit_workflow = wf and c.commit_worker = worker and c.commit_kind = WorkflowCommit and c.commit_time = t
  unchangedExcept[wf, none, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[wf, t, tnext]
}

pred failOrFinishCancellationWorkflow[wf: Workflow, worker: lone Worker, status: WorkflowStatus, t: Time, tnext: Time] {
  status in (Failed + Canceled)
  some workflowStatus[wf, t]
  workflowStatus[wf, t] in (Pending + Running + Waiting + Canceling)
  some worker implies liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, tnext] = status
  no workflowNextRun[wf, tnext]
  status = Failed implies workflowCancelSame[wf, t, tnext]
  status = Canceled and some workflowCancelRequestedAt[wf, t] implies workflowCancelSame[wf, t, tnext]
  status = Canceled and no workflowCancelRequestedAt[wf, t] implies workflowCancelRequestedAt[wf, tnext] = t
  no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  status = Failed implies unchangedExcept[wf, none, none, none, none, none, none, none, t, tnext]
  status = Canceled implies {
    all other: Workflow - wf | workflowSame[other, t, tnext]
    all st: Step | st.step_workflow != wf implies stepSame[st, t, tnext]
    all st: Step | st.step_workflow = wf implies {
      no stepStatus[st, t] implies no stepStatus[st, tnext]
      some stepStatus[st, t] and stepStatus[st, t] in (StepScheduled + StepRunning + StepWaiting) implies stepStatus[st, tnext] = StepCanceled
      some stepStatus[st, t] and stepStatus[st, t] not in (StepScheduled + StepRunning + StepWaiting) implies stepSame[st, t, tnext]
    }
    all att: Attempt | att.attempt_step.step_workflow != wf implies attemptSame[att, t, tnext]
    all att: Attempt | att.attempt_step.step_workflow = wf implies {
      no attemptStatus[att, t] implies no attemptStatus[att, tnext]
      some attemptStatus[att, t] and attemptStatus[att, t] in (AttemptRunning + AttemptWaiting) implies attemptStatus[att, tnext] = AttemptCanceled
      some attemptStatus[att, t] and attemptStatus[att, t] not in (AttemptRunning + AttemptWaiting) implies attemptSame[att, t, tnext]
    }
    all wt: Wait | wt.wait_step.step_workflow != wf implies waitSame[wt, t, tnext]
    all wt: Wait | wt.wait_step.step_workflow = wf implies {
      no waitStatus[wt, t] implies no waitStatus[wt, tnext]
      waitStatus[wt, t] = WaitPending implies waitStatus[wt, tnext] = WaitCanceled
      some waitStatus[wt, t] and waitStatus[wt, t] != WaitPending implies waitSame[wt, t, tnext]
    }
    all f: Fence | fenceSame[f, t, tnext]
    all o: OutboxMessage | outboxSame[o, t, tnext]
    all c: InboxCommand | commandSame[c, t, tnext]
    all target: InboxTarget | targetActivationSame[target, t, tnext]
    all cmd: WorkflowCommand | commandHistorySame[cmd, t, tnext]
  }
  preserveLeasesExcept[wf, t, tnext]
}

pred requestWorkflowCancellation[wf: Workflow, t: Time, tnext: Time] {
  -- Workflow cancellation is cooperative and stored as metadata. Runnable and
  -- suspended workflows enter canceling immediately, while a running workflow
  -- keeps its lease until it suspends, retries, or releases.
  some workflowStatus[wf, t]
  workflowStatus[wf, t] in (Pending + Running + Waiting + Canceling + Failed)
  not terminalWorkflow[wf, t]
  no workflowCancelRequestedAt[wf, t]
  workflowCancelRequestedAt[wf, tnext] = t
  workflowStatus[wf, t] = Running implies {
    workflowStatus[wf, tnext] = Running
    workflowNextRun[wf, tnext] = workflowNextRun[wf, t]
  }
  workflowStatus[wf, t] != Running implies {
    workflowStatus[wf, tnext] = Canceling
    no workflowNextRun[wf, tnext]
    no l: LeaseRow | l.lr_workflow = wf and l.lr_time = tnext
  }

  all other: Workflow - wf | workflowSame[other, t, tnext]
  all st: Step | st.step_workflow != wf implies stepSame[st, t, tnext]
  all st: Step | st.step_workflow = wf implies {
    no stepStatus[st, t] implies no stepStatus[st, tnext]
    some stepStatus[st, t] and stepStatus[st, t] = StepWaiting implies stepStatus[st, tnext] = StepCanceled
    some stepStatus[st, t] and stepStatus[st, t] != StepWaiting implies stepSame[st, t, tnext]
  }
  all att: Attempt | att.attempt_step.step_workflow != wf implies attemptSame[att, t, tnext]
  all att: Attempt | att.attempt_step.step_workflow = wf implies {
    no attemptStatus[att, t] implies no attemptStatus[att, tnext]
    some attemptStatus[att, t] and attemptStatus[att, t] = AttemptWaiting implies attemptStatus[att, tnext] = AttemptCanceled
    some attemptStatus[att, t] and attemptStatus[att, t] != AttemptWaiting implies attemptSame[att, t, tnext]
  }
  all wt: Wait | wt.wait_step.step_workflow != wf implies waitSame[wt, t, tnext]
  all wt: Wait | wt.wait_step.step_workflow = wf implies {
    no waitStatus[wt, t] implies no waitStatus[wt, tnext]
    waitStatus[wt, t] = WaitPending implies waitStatus[wt, tnext] = WaitCanceled
    some waitStatus[wt, t] and waitStatus[wt, t] != WaitPending implies waitSame[wt, t, tnext]
  }
  all f: Fence | fenceSame[f, t, tnext]
  all o: OutboxMessage | outboxSame[o, t, tnext]
  all c: InboxCommand | commandSame[c, t, tnext]
  all target: InboxTarget | targetActivationSame[target, t, tnext]
  all cmd: WorkflowCommand | commandHistorySame[cmd, t, tnext]
  workflowStatus[wf, t] = Running implies preserveLeasesExcept[none, t, tnext]
  workflowStatus[wf, t] != Running implies preserveLeasesExcept[wf, t, tnext]
}

pred resumeReplayCompletedStep[wf: Workflow, st: Step, worker: Worker, t: Time, tnext: Time] {
  -- [DURABABBLE-STEP-1] Resume/replay returns completed step results without re-execution.
  liveWorkflowLease[wf, worker, t]
  workflowStatus[wf, t] = Running
  stepStatus[st, t] = StepCompleted
  workflowStatus[wf, tnext] = Running
  workflowCancelSame[wf, t, tnext]
  stepStatus[st, tnext] = StepCompleted
  no a: Attempt | a.attempt_step = st and attemptStatus[a, tnext] = AttemptRunning and no attemptStatus[a, t]
  unchangedExcept[wf, st, none, none, none, none, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred acquireFence[f: Fence, wf: Workflow, token: FenceToken, t: Time, tnext: Time] {
  -- [DURABABBLE-FENCE-1] Fence row is persisted before the external side effect.
  f.fence_workflow = wf
  workflowStatus[wf, t] in (Running + Completed + Pending)
  no fenceStatus[f, t]
  workflowSame[wf, t, tnext]
  fenceStatus[f, tnext] = FenceRunning
  one r: FenceRow | r.fence_row = f and r.fence_status = FenceRunning and r.fence_owner = token and r.fence_time = tnext
  unchangedExcept[wf, none, none, none, f, none, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred completeFence[f: Fence, token: FenceToken, worker: Worker, t: Time, tnext: Time] {
  fenceStatus[f, t] = FenceRunning
  one r: FenceRow | r.fence_row = f and r.fence_time = t and r.fence_owner = token
  workflowSame[f.fence_workflow, t, tnext]
  fenceStatus[f, tnext] = FenceCompleted
  one r: FenceRow | r.fence_row = f and r.fence_time = tnext and r.fence_owner = token
  one c: DurableCommit | c.commit_workflow = f.fence_workflow and c.commit_worker = worker and c.commit_kind = FenceCommit and c.commit_time = t
  unchangedExcept[f.fence_workflow, none, none, none, f, none, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred failFence[f: Fence, token: FenceToken, worker: Worker, t: Time, tnext: Time] {
  fenceStatus[f, t] = FenceRunning
  one r: FenceRow | r.fence_row = f and r.fence_time = t and r.fence_owner = token
  workflowSame[f.fence_workflow, t, tnext]
  fenceStatus[f, tnext] = FenceFailed
  one r: FenceRow | r.fence_row = f and r.fence_time = tnext and r.fence_owner = token
  unchangedExcept[f.fence_workflow, none, none, none, f, none, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred enqueueOutbox[o: OutboxMessage, wf: Workflow, t: Time, tnext: Time] {
  -- [DURABABBLE-OUTBOX-1] Outbox keys map to one durable message that is leased before ack.
  o.outbox_workflow = wf
  no outboxStatus[o, t]
  workflowSame[wf, t, tnext]
  outboxStatus[o, tnext] = OutboxPending
  unchangedExcept[wf, none, none, none, none, o, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred claimOutbox[o: OutboxMessage, worker: Worker, t: Time, tnext: Time] {
  outboxStatus[o, t] = OutboxPending
  or some r: OutboxRow | r.outbox_row = o and r.outbox_status = OutboxProcessing and r.outbox_time = t and some r.outbox_expiresAt and not gt[r.outbox_expiresAt, t]
  workflowSame[o.outbox_workflow, t, tnext]
  outboxStatus[o, tnext] = OutboxProcessing
  one r: OutboxRow | r.outbox_row = o and r.outbox_status = OutboxProcessing and r.outbox_owner = worker and r.outbox_time = tnext and some r.outbox_expiresAt and gt[r.outbox_expiresAt, tnext]
  unchangedExcept[o.outbox_workflow, none, none, none, none, o, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred ackOutbox[o: OutboxMessage, worker: Worker, t: Time, tnext: Time] {
  liveOutboxLease[o, worker, t]
  workflowSame[o.outbox_workflow, t, tnext]
  outboxStatus[o, tnext] = OutboxAcked
  one a: OutboxAck | a.ack_message = o and a.ack_worker = worker and a.ack_time = t
  unchangedExcept[o.outbox_workflow, none, none, none, none, o, none, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred enqueueInboxCommand[cmd: InboxCommand, t: Time, tnext: Time] {
  -- [DURABABBLE-OBJ-1] Inbox commands for workflow and object targets serialize by target identity.
  no commandStatus[cmd, t]
  cmd.command_target.target_kind = WorkflowInbox implies not terminalWorkflow[cmd.command_target.target_workflow, t]
  commandStatus[cmd, tnext] = CommandPending
  activationStatus[cmd.command_target, t] = ActivationRunning implies targetActivationSame[cmd.command_target, t, tnext]
  activationStatus[cmd.command_target, t] != ActivationRunning implies {
    activationStatus[cmd.command_target, tnext] = ActivationPending
    no ((activation_target.(cmd.command_target)) & (activation_time.tnext)).activation_owner
    no ((activation_target.(cmd.command_target)) & (activation_time.tnext)).activation_expiresAt
  }
  unchangedExcept[none, none, none, none, none, none, cmd, cmd.command_target, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred claimTargetActivation[target: InboxTarget, worker: Worker, t: Time, tnext: Time] {
  -- Target activations coalesce inbox rows; only one worker owns a target drain.
  activationStatus[target, t] = ActivationPending
  or some r: TargetActivationRow | r.activation_target = target and r.activation_status = ActivationRunning and
    r.activation_time = t and some r.activation_expiresAt and not gt[r.activation_expiresAt, t]
  activationStatus[target, tnext] = ActivationRunning
  one r: TargetActivationRow | r.activation_target = target and r.activation_status = ActivationRunning and
    r.activation_owner = worker and r.activation_time = tnext and some r.activation_expiresAt and gt[r.activation_expiresAt, tnext]
  unchangedExcept[none, none, none, none, none, none, none, target, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred completeTargetActivation[target: InboxTarget, worker: Worker, t: Time, tnext: Time] {
  liveTargetActivation[target, worker, t]
  (some cmd: InboxCommand | mailboxHead[cmd, target, t] and commandStatus[cmd, t] in (CommandPending + CommandRunning + CommandFailed)) implies {
    activationStatus[target, tnext] = ActivationPending
    no ((activation_target.target) & (activation_time.tnext)).activation_owner
    no ((activation_target.target) & (activation_time.tnext)).activation_expiresAt
  }
  (no cmd: InboxCommand | mailboxHead[cmd, target, t] and commandStatus[cmd, t] in (CommandPending + CommandRunning + CommandFailed)) implies
    no r: TargetActivationRow | r.activation_target = target and r.activation_time = tnext
  unchangedExcept[none, none, none, none, none, none, none, target, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred mailboxHead[cmd: InboxCommand, target: InboxTarget, t: Time] {
  cmd.command_target = target
  some commandStatus[cmd, t]
  commandStatus[cmd, t] != CommandCompleted
  no earlier: InboxCommand - cmd |
    earlier.command_target = target and
    lt[earlier.command_sequence, cmd.command_sequence] and
    some commandStatus[earlier, t] and
    commandStatus[earlier, t] != CommandCompleted
}

pred inboxCommandBlocks[cmd: InboxCommand, worker: Worker, t: Time] {
  some commandStatus[cmd, t]
  commandStatus[cmd, t] in (CommandPending + CommandFailed + CommandDeadLettered)
  or (commandStatus[cmd, t] = CommandRunning and not liveCommandLease[cmd, worker, t])
}

pred claimInboxCommand[cmd: InboxCommand, worker: Worker, t: Time, tnext: Time] {
  some commandStatus[cmd, t]
  (
    commandStatus[cmd, t] in (CommandPending + CommandFailed)
  )
  or (
    commandStatus[cmd, t] = CommandRunning and no owner: Worker | liveCommandLease[cmd, owner, t]
  )
  liveTargetActivation[cmd.command_target, worker, t]
  workflowInboxLeaseHeld[cmd, worker, t]
  no other: InboxCommand - cmd | other.command_target = cmd.command_target and
    lt[other.command_sequence, cmd.command_sequence] and inboxCommandBlocks[other, worker, t]
  commandStatus[cmd, tnext] = CommandRunning
  one r: CommandRow | r.command_row = cmd and r.command_status = CommandRunning and r.command_owner = worker and r.command_time = tnext and some r.command_expiresAt and gt[r.command_expiresAt, tnext]
  unchangedExcept[none, none, none, none, none, none, cmd, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred completeInboxCommand[cmd: InboxCommand, worker: Worker, t: Time, tnext: Time] {
  liveCommandLease[cmd, worker, t]
  workflowInboxLeaseHeld[cmd, worker, t]
  commandStatus[cmd, tnext] = CommandCompleted
  no ((command_row.cmd) & (command_time.tnext)).command_owner
  no ((command_row.cmd) & (command_time.tnext)).command_expiresAt
  one c: DurableCommit | c.commit_command = cmd and c.commit_worker = worker and c.commit_kind = InboxCommandCommit and c.commit_time = t
  unchangedExcept[none, none, none, none, none, none, cmd, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred failInboxCommand[cmd: InboxCommand, worker: Worker, t: Time, tnext: Time] {
  liveCommandLease[cmd, worker, t]
  workflowInboxLeaseHeld[cmd, worker, t]
  commandStatus[cmd, tnext] = CommandFailed
  no ((command_row.cmd) & (command_time.tnext)).command_owner
  no ((command_row.cmd) & (command_time.tnext)).command_expiresAt
  unchangedExcept[none, none, none, none, none, none, cmd, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred deadLetterInboxCommand[cmd: InboxCommand, worker: Worker, t: Time, tnext: Time] {
  liveCommandLease[cmd, worker, t]
  workflowInboxLeaseHeld[cmd, worker, t]
  commandStatus[cmd, tnext] = CommandDeadLettered
  no ((command_row.cmd) & (command_time.tnext)).command_owner
  no ((command_row.cmd) & (command_time.tnext)).command_expiresAt
  unchangedExcept[none, none, none, none, none, none, cmd, none, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred stutter[t: Time, tnext: Time] {
  all w: Workflow | workflowSame[w, t, tnext]
  all s: Step | stepSame[s, t, tnext]
  all a: Attempt | attemptSame[a, t, tnext]
  all w: Wait | waitSame[w, t, tnext]
  all f: Fence | fenceSame[f, t, tnext]
  all o: OutboxMessage | outboxSame[o, t, tnext]
  all c: InboxCommand | commandSame[c, t, tnext]
  all target: InboxTarget | targetActivationSame[target, t, tnext]
  all cmd: WorkflowCommand | commandHistorySame[cmd, t, tnext]
  preserveLeasesExcept[none, t, tnext]
}

pred step[t: Time, tnext: Time] {
  stutter[t, tnext]
  or some wf: Workflow | enqueueWorkflow[wf, t, tnext]
  or some wf: Workflow, worker: Worker | claimWorkflow[wf, worker, t, tnext]
  or some wf: Workflow, worker: Worker | claimWorkflowForActivation[wf, worker, t, tnext]
  or some wf: Workflow, worker: Worker | suspendWorkflow[wf, worker, t, tnext]
  or some wf: Workflow, worker: Worker | heartbeatWorkflow[wf, worker, t, tnext]
  or some wf: Workflow, worker: Worker | releaseOrStealLease[wf, worker, t, tnext]
  or some wf: Workflow, cmd: WorkflowCommand, shape: CommandShape, worker: Worker | scheduleWorkflowCommand[wf, cmd, shape, worker, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt, worker: Worker | startStep[wf, st, att, worker, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt, worker: Worker | completeStep[wf, st, att, worker, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt, worker: Worker, due: Time | retryStep[wf, st, att, worker, due, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt, worker: Worker | failStep[wf, st, att, worker, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt, worker: Worker | cancelStep[wf, st, att, worker, t, tnext]
  or some wf: Workflow, st: Step, wait: Wait, worker: Worker | recordWait[wf, st, none, wait, worker, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt, wait: Wait, worker: Worker | recordWait[wf, st, att, wait, worker, t, tnext]
  or some wf: Workflow, st: Step, wait: Wait, trigger: WaitTrigger | wakeWait[wf, st, none, wait, trigger, t, tnext]
  or some wf: Workflow, st: Step, att: Attempt, wait: Wait, trigger: WaitTrigger | wakeWait[wf, st, att, wait, trigger, t, tnext]
  or some wf: Workflow, worker: Worker | completeWorkflow[wf, worker, t, tnext]
  or some wf: Workflow | requestWorkflowCancellation[wf, t, tnext]
  or some wf: Workflow, status: WorkflowStatus | failOrFinishCancellationWorkflow[wf, none, status, t, tnext]
  or some wf: Workflow, worker: Worker, status: WorkflowStatus | failOrFinishCancellationWorkflow[wf, worker, status, t, tnext]
  or some wf: Workflow, st: Step, worker: Worker | resumeReplayCompletedStep[wf, st, worker, t, tnext]
  or some f: Fence, wf: Workflow, token: FenceToken | acquireFence[f, wf, token, t, tnext]
  or some f: Fence, token: FenceToken, worker: Worker | completeFence[f, token, worker, t, tnext]
  or some f: Fence, token: FenceToken, worker: Worker | failFence[f, token, worker, t, tnext]
  or some o: OutboxMessage, wf: Workflow | enqueueOutbox[o, wf, t, tnext]
  or some o: OutboxMessage, worker: Worker | claimOutbox[o, worker, t, tnext]
  or some o: OutboxMessage, worker: Worker | ackOutbox[o, worker, t, tnext]
  or some cmd: InboxCommand | enqueueInboxCommand[cmd, t, tnext]
  or some target: InboxTarget, worker: Worker | claimTargetActivation[target, worker, t, tnext]
  or some target: InboxTarget, worker: Worker | completeTargetActivation[target, worker, t, tnext]
  or some cmd: InboxCommand, worker: Worker | claimInboxCommand[cmd, worker, t, tnext]
  or some cmd: InboxCommand, worker: Worker | completeInboxCommand[cmd, worker, t, tnext]
  or some cmd: InboxCommand, worker: Worker | failInboxCommand[cmd, worker, t, tnext]
  or some cmd: InboxCommand, worker: Worker | deadLetterInboxCommand[cmd, worker, t, tnext]
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
 * final failure, or cancellation.
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
 * [DURABABBLE-CONCURRENCY-1] Scheduled workflow command history is ordered,
 * shape-stable, and unique per workflow command id even when several workflow
 * fibers schedule work before any completion resolves.
 */
assert scheduledCommandHistoryIsReplayStable {
  all cmd: WorkflowCommand, t1, t2: Time |
    some commandScheduledShape[cmd, t1] and some commandScheduledShape[cmd, t2]
    implies commandScheduledShape[cmd, t1] = commandScheduledShape[cmd, t2]
  all wf: Workflow, sequence, t: Time |
    lone cmd: WorkflowCommand | cmd.wc_workflow = wf and some r: CommandHistoryRow |
      r.chr_command = cmd and r.chr_kind = CommandScheduled and r.chr_sequence = sequence and r.chr_time = t
}

/**
 * [DURABABBLE-CONCURRENCY-1] Replay indexes terminal command history by command
 * id and uses the latest terminal event, so a completed timer wait supersedes
 * the earlier waiting event for the same command.
 */
assert terminalCommandHistoryUsesLatestReplayEvent {
  all cmd: WorkflowCommand, t: Time | lone latestTerminalCommandHistoryRows[cmd, t]
  all cmd: WorkflowCommand, t: Time |
    some commandHistorySequence[cmd, CommandSucceeded, t] and some commandHistorySequence[cmd, CommandWaiting, t]
    implies latestTerminalCommandHistoryKind[cmd, t] = CommandSucceeded
}

/**
 * [DURABABBLE-CONCURRENCY-1] Runtime command history is append-only in the
 * same lifecycle order the engine writes to MySQL/PostgreSQL: commands are
 * scheduled before they start or wait, and terminal step failures/cancellations
 * only follow a started attempt.
 */
assert commandHistoryFollowsRuntimeLifecycle {
  all r: CommandHistoryRow | r.chr_kind != CommandScheduled implies
    some scheduled: CommandHistoryRow |
      scheduled.chr_command = r.chr_command and
      scheduled.chr_kind = CommandScheduled and
      scheduled.chr_time = r.chr_time and
      lt[scheduled.chr_sequence, r.chr_sequence]
  all r: CommandHistoryRow | r.chr_kind in (CommandRejected + CommandCanceled + CommandErrored) implies
    some started: CommandHistoryRow |
      started.chr_command = r.chr_command and
      started.chr_kind = CommandStarted and
      started.chr_time = r.chr_time and
      lt[started.chr_sequence, r.chr_sequence]
  all r: CommandHistoryRow | r.chr_kind = CommandSucceeded implies
    some earlier: CommandHistoryRow |
      earlier.chr_command = r.chr_command and
      earlier.chr_kind in (CommandStarted + CommandWaiting) and
      earlier.chr_time = r.chr_time and
      lt[earlier.chr_sequence, r.chr_sequence]
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
 * [DURABABBLE-STEP-2] Pending retry backoff rows are not claimable before
 * their due time.
 */
assert retryBackoffPreventsEarlyClaim {
  all wf: Workflow, due: Time, t: Time - last |
    workflowStatus[wf, t] = Pending and workflowNextRun[wf, t] = due and gt[due, t]
    implies workflowStatus[wf, t.next] != Running
  all wf: Workflow, due: Time, t: Time - last |
    workflowStatus[wf, t] = Failed and workflowNextRun[wf, t] = due and gt[due, t]
    implies workflowStatus[wf, t.next] != Running
  all wf: Workflow, t: Time - last |
    workflowStatus[wf, t] = Failed and no workflowNextRun[wf, t]
    implies workflowStatus[wf, t.next] != Running
}

/**
 * [DURABABBLE-WAIT-1] A wait can produce at most one durable wake event.
 */
assert waitsWakeOnce {
  all w: Wait | lone e: WakeEvent | e.wake_wait = w
  all w: Wait | lone t: Time - last | waitStatus[w, t] = WaitPending and waitStatus[w, t.next] = WaitCompleted
  all w: Wait, t: Time - last | waitStatus[w, t] = WaitCompleted implies waitStatus[w, t.next] = WaitCompleted
}

/**
 * [DURABABBLE-LEASE-4] Stale workflow owners cannot commit step or workflow
 * results.
 */
assert staleOwnersCannotCommit {
  all c: DurableCommit |
    c.commit_kind in (WorkflowCommit + StepCommit + WaitCommit) implies liveWorkflowLease[c.commit_workflow, c.commit_worker, c.commit_time]
}

assert workflowInboxCommandCommitsNeedWorkflowLease {
  all c: DurableCommit |
    c.commit_kind = InboxCommandCommit and c.commit_command.command_target.target_kind = WorkflowInbox implies
      liveWorkflowLease[c.commit_command.command_target.target_workflow, c.commit_worker, c.commit_time]
}

/**
 * [DURABABBLE-FENCE-1] A side-effect fence has one running owner and one
 * terminal result. Completed fences replay the committed result; failed fences
 * replay the stored error without committing the side effect.
 */
assert idempotencyFencesPreventDuplicateSideEffects {
  all f: Fence, t: Time | lone r: FenceRow | r.fence_row = f and r.fence_time = t and r.fence_status = FenceRunning
  all f: Fence | lone t: Time - last | fenceStatus[f, t] = FenceRunning and (fenceStatus[f, t.next] = FenceCompleted or fenceStatus[f, t.next] = FenceFailed)
  all f: Fence, t: Time - last | (fenceStatus[f, t] = FenceCompleted or fenceStatus[f, t] = FenceFailed) implies fenceStatus[f, t.next] = fenceStatus[f, t]
}

assert staleFenceTokensCannotFinish {
  all f: Fence, token: FenceToken, worker: Worker, t: Time - last |
    {
      fenceStatus[f, t] = FenceRunning
      no r: FenceRow | r.fence_row = f and r.fence_time = t and r.fence_owner = token
    } implies {
      not completeFence[f, token, worker, t, t.next]
      not failFence[f, token, worker, t, t.next]
    }
}

/**
 * [DURABABBLE-OUTBOX-1] Outbox acknowledgement requires the current outbox
 * lease owner, and acknowledgement is final.
 */
assert outboxAckLeaseBehaviorIsSafe {
  all a: OutboxAck | liveOutboxLease[a.ack_message, a.ack_worker, a.ack_time]
  all o: OutboxMessage, t: Time - last | outboxStatus[o, t] = OutboxAcked implies outboxStatus[o, t.next] = OutboxAcked
}

/**
 * [DURABABBLE-OBJ-1] Inbox command execution is serialized by target ownership.
 * A worker may claim a contiguous inbox prefix, but two workers cannot hold live
 * commands for the same workflow or object target.
 */
assert durableInboxCommandSerializationHolds {
  all target: InboxTarget, t: Time |
    lone worker: Worker | liveTargetActivation[target, worker, t]
  all target: InboxTarget, t: Time |
    lone worker: Worker | some cmd: InboxCommand | cmd.command_target = target and liveCommandLease[cmd, worker, t]
  all c: DurableCommit |
    c.commit_kind = InboxCommandCommit implies liveCommandLease[c.commit_command, c.commit_worker, c.commit_time]
}

assert inboxClaimsRequireExistingRows {
  all cmd: InboxCommand, worker: Worker, t: Time - last |
    no commandStatus[cmd, t] implies not claimInboxCommand[cmd, worker, t, t.next]
}

/**
 * [DURABABBLE-WF-1] Completed/canceled workflows cannot retain unfinished step,
 * attempt, or wait rows. Failed rows are terminal when they have no retry
 * deadline, but may retain diagnostic incomplete work.
 */
assert terminalWorkflowsHaveNoIncompleteWork {
  all wf: Workflow, t: Time |
    closedWorkflow[wf, t] implies {
      no st: Step | st.step_workflow = wf and some stepStatus[st, t] and stepStatus[st, t] in (StepScheduled + StepRunning + StepWaiting)
      no att: Attempt | att.attempt_step.step_workflow = wf and some attemptStatus[att, t] and attemptStatus[att, t] in (AttemptRunning + AttemptWaiting)
      no wt: Wait | wt.wait_step.step_workflow = wf and waitStatus[wt, t] = WaitPending
    }
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
  some wf: Workflow, st: Step, att: Attempt, worker1, worker2: Worker, cmd: WorkflowCommand, shape: CommandShape | {
    worker1 != worker2
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker1, first.next, first.next.next]
    stutter[first.next.next, first.next.next.next]
    releaseOrStealLease[wf, worker2, first.next.next.next, first.next.next.next.next]
    claimWorkflow[wf, worker2, first.next.next.next.next, first.next.next.next.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker2, first.next.next.next.next.next, first.next.next.next.next.next.next]
    startStep[wf, st, att, worker2, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
  }
}

pred exampleWaitWake {
  some wf: Workflow, st: Step, att: Attempt, wait: Wait, trigger: WaitTrigger, worker: Worker, cmd: WorkflowCommand, shape: CommandShape | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    startStep[wf, st, att, worker, first.next.next.next, first.next.next.next.next]
    recordWait[wf, st, att, wait, worker, first.next.next.next.next, first.next.next.next.next.next]
    wakeWait[wf, st, att, wait, trigger, first.next.next.next.next.next, first.next.next.next.next.next.next]
    lt[
      commandHistorySequence[cmd, CommandWaiting, first.next.next.next.next.next.next],
      commandHistorySequence[cmd, CommandSucceeded, first.next.next.next.next.next.next]
    ]
    latestTerminalCommandHistoryKind[cmd, first.next.next.next.next.next.next] = CommandSucceeded
  }
}

pred exampleDirectWaitWake {
  some wf: Workflow, st: Step, cmd: WorkflowCommand, shape: CommandShape, wait: Wait, trigger: WaitTrigger, worker: Worker | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    recordWait[wf, st, none, wait, worker, first.next.next.next, first.next.next.next.next]
    wakeWait[wf, st, none, wait, trigger, first.next.next.next.next, first.next.next.next.next.next]
    stepStatus[st, first.next.next.next.next.next] = StepCompleted
    no AttemptRow
  }
}

pred exampleWaitAllowsSiblingCompletionBeforeSuspension {
  some wf: Workflow, waitStep, siblingStep: Step, waitAttempt, siblingAttempt: Attempt, wait: Wait, worker: Worker, waitCmd, siblingCmd: WorkflowCommand, waitShape, siblingShape: CommandShape | {
    waitStep != siblingStep
    waitCmd != siblingCmd
    waitCmd.wc_workflow = wf
    waitCmd.wc_step = waitStep
    siblingCmd.wc_workflow = wf
    siblingCmd.wc_step = siblingStep
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    some l: LeaseRow | l.lr_workflow = wf and l.lr_time = first.next.next and l.lr_expiresAt = last
    scheduleWorkflowCommand[wf, waitCmd, waitShape, worker, first.next.next, first.next.next.next]
    scheduleWorkflowCommand[wf, siblingCmd, siblingShape, worker, first.next.next.next, first.next.next.next.next]
    startStep[wf, waitStep, waitAttempt, worker, first.next.next.next.next, first.next.next.next.next.next]
    startStep[wf, siblingStep, siblingAttempt, worker, first.next.next.next.next.next, first.next.next.next.next.next.next]
    recordWait[wf, waitStep, waitAttempt, wait, worker, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next.next.next] = Running
    completeStep[wf, siblingStep, siblingAttempt, worker, first.next.next.next.next.next.next.next, first.next.next.next.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next.next.next.next] = Running
    stepStatus[siblingStep, first.next.next.next.next.next.next.next.next] = StepCompleted
    stepStatus[waitStep, first.next.next.next.next.next.next.next.next] = StepWaiting
  }
}

pred exampleRetryBackoff {
  some wf: Workflow, st: Step, att: Attempt, worker: Worker, due: Time, cmd: WorkflowCommand, shape: CommandShape | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    startStep[wf, st, att, worker, first.next.next.next, first.next.next.next.next]
    retryStep[wf, st, att, worker, due, first.next.next.next.next, first.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next] = Pending
    workflowNextRun[wf, first.next.next.next.next.next] = due
    stutter[first.next.next.next.next.next, first.next.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next.next] = Pending
  }
}

pred exampleRetryThenCompletesWithFailedAttemptHistory {
  some wf: Workflow, st: Step, failedAttempt, completedAttempt: Attempt, worker: Worker, due: Time, cmd: WorkflowCommand, shape: CommandShape | {
    failedAttempt != completedAttempt
    cmd.wc_workflow = wf
    cmd.wc_step = st
    due = first.next.next.next.next.next.next
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    startStep[wf, st, failedAttempt, worker, first.next.next.next, first.next.next.next.next]
    retryStep[wf, st, failedAttempt, worker, due, first.next.next.next.next, first.next.next.next.next.next]
    stutter[first.next.next.next.next.next, first.next.next.next.next.next.next]
    claimWorkflow[wf, worker, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
    startStep[wf, st, completedAttempt, worker, first.next.next.next.next.next.next.next, first.next.next.next.next.next.next.next.next]
    completeStep[wf, st, completedAttempt, worker, first.next.next.next.next.next.next.next.next, first.next.next.next.next.next.next.next.next.next]
    attemptStatus[failedAttempt, first.next.next.next.next.next.next.next.next.next] = AttemptFailed
    attemptStatus[completedAttempt, first.next.next.next.next.next.next.next.next.next] = AttemptCompleted
    stepStatus[st, first.next.next.next.next.next.next.next.next.next] = StepCompleted
  }
}

pred exampleStepFailureReplaysAsTerminalHistory {
  some wf: Workflow, st: Step, att: Attempt, worker: Worker, cmd: WorkflowCommand, shape: CommandShape | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    startStep[wf, st, att, worker, first.next.next.next, first.next.next.next.next]
    failStep[wf, st, att, worker, first.next.next.next.next, first.next.next.next.next.next]
    stepStatus[st, first.next.next.next.next.next] = StepFailed
    attemptStatus[att, first.next.next.next.next.next] = AttemptFailed
    latestTerminalCommandHistoryKind[cmd, first.next.next.next.next.next] = CommandRejected
  }
}

pred exampleCancellationCompletesAfterCleanup {
  some wf: Workflow, cleanupStep: Step, cleanupAttempt: Attempt, worker: Worker, cleanupCmd: WorkflowCommand, cleanupShape: CommandShape | {
    cleanupCmd.wc_workflow = wf
    cleanupCmd.wc_step = cleanupStep
    enqueueWorkflow[wf, first, first.next]
    requestWorkflowCancellation[wf, first.next, first.next.next]
    workflowStatus[wf, first.next.next] = Canceling
    claimWorkflow[wf, worker, first.next.next, first.next.next.next]
    scheduleWorkflowCommand[wf, cleanupCmd, cleanupShape, worker, first.next.next.next, first.next.next.next.next]
    startStep[wf, cleanupStep, cleanupAttempt, worker, first.next.next.next.next, first.next.next.next.next.next]
    completeStep[wf, cleanupStep, cleanupAttempt, worker, first.next.next.next.next.next, first.next.next.next.next.next.next]
    failOrFinishCancellationWorkflow[wf, worker, Canceled, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next.next.next] = Canceled
  }
}

pred exampleWaitingCancellationCancelsWait {
  some wf: Workflow, st: Step, att: Attempt, wait: Wait, worker: Worker, cmd: WorkflowCommand, shape: CommandShape | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    startStep[wf, st, att, worker, first.next.next.next, first.next.next.next.next]
    recordWait[wf, st, att, wait, worker, first.next.next.next.next, first.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next] = Waiting
    requestWorkflowCancellation[wf, first.next.next.next.next.next, first.next.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next.next] = Canceling
    stepStatus[st, first.next.next.next.next.next.next] = StepCanceled
    attemptStatus[att, first.next.next.next.next.next.next] = AttemptCanceled
    waitStatus[wait, first.next.next.next.next.next.next] = WaitCanceled
    failOrFinishCancellationWorkflow[wf, none, Canceled, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
  }
}

pred exampleTerminalCancellationCleansPendingWait {
  some wf: Workflow, st: Step, att: Attempt, wait: Wait, worker: Worker, cmd: WorkflowCommand, shape: CommandShape | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    startStep[wf, st, att, worker, first.next.next.next, first.next.next.next.next]
    recordWait[wf, st, att, wait, worker, first.next.next.next.next, first.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next] = Waiting
    waitStatus[wait, first.next.next.next.next.next] = WaitPending
    failOrFinishCancellationWorkflow[wf, none, Canceled, first.next.next.next.next.next, first.next.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next.next] = Canceled
    stepStatus[st, first.next.next.next.next.next.next] = StepCanceled
    attemptStatus[att, first.next.next.next.next.next.next] = AttemptCanceled
    waitStatus[wait, first.next.next.next.next.next.next] = WaitCanceled
  }
}

pred exampleRunningStepCancellationRecordsHistory {
  some wf: Workflow, st: Step, att: Attempt, worker: Worker, cmd: WorkflowCommand, shape: CommandShape | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    startStep[wf, st, att, worker, first.next.next.next, first.next.next.next.next]
    requestWorkflowCancellation[wf, first.next.next.next.next, first.next.next.next.next.next]
    cancelStep[wf, st, att, worker, first.next.next.next.next.next, first.next.next.next.next.next.next]
    stepStatus[st, first.next.next.next.next.next.next] = StepCanceled
    attemptStatus[att, first.next.next.next.next.next.next] = AttemptCanceled
    latestTerminalCommandHistoryKind[cmd, first.next.next.next.next.next.next] = CommandCanceled
  }
}

pred exampleBackoffCancellationClearsDue {
  some wf: Workflow, st: Step, att: Attempt, worker: Worker, due: Time, cmd: WorkflowCommand, shape: CommandShape | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    startStep[wf, st, att, worker, first.next.next.next, first.next.next.next.next]
    retryStep[wf, st, att, worker, due, first.next.next.next.next, first.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next] = Pending
    some workflowNextRun[wf, first.next.next.next.next.next]
    requestWorkflowCancellation[wf, first.next.next.next.next.next, first.next.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next.next] = Canceling
    no workflowNextRun[wf, first.next.next.next.next.next.next]
  }
}

pred exampleRunningCancellationMetadataReleasedToCanceling {
  some wf: Workflow, worker: Worker | {
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    requestWorkflowCancellation[wf, first.next.next, first.next.next.next]
    workflowStatus[wf, first.next.next.next] = Running
    some workflowCancelRequestedAt[wf, first.next.next.next]
    liveWorkflowLease[wf, worker, first.next.next.next]
    releaseOrStealLease[wf, worker, first.next.next.next, first.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next] = Canceling
    some workflowCancelRequestedAt[wf, first.next.next.next.next]
  }
}

pred exampleExpiredRunningWorkflowReclaimedDirectly {
  some wf: Workflow, worker1, worker2: Worker | {
    worker1 != worker2
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker1, first.next, first.next.next]
    stutter[first.next.next, first.next.next.next]
    no owner: Worker | liveWorkflowLease[wf, owner, first.next.next.next]
    workflowStatus[wf, first.next.next.next] = Running
    claimWorkflow[wf, worker2, first.next.next.next, first.next.next.next.next]
    liveWorkflowLease[wf, worker2, first.next.next.next.next]
  }
}

pred exampleStepStart {
  some wf: Workflow, st: Step, att: Attempt, worker: Worker, cmd: WorkflowCommand, shape: CommandShape | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    startStep[wf, st, att, worker, first.next.next.next, first.next.next.next.next]
  }
}

pred exampleStepStartSupersedesRunningAttempt {
  some wf: Workflow, st: Step, oldAttempt, newAttempt: Attempt, worker1, worker2: Worker, cmd: WorkflowCommand, shape: CommandShape | {
    oldAttempt != newAttempt
    worker1 != worker2
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker1, first.next, first.next.next]
    some l: LeaseRow | l.lr_workflow = wf and l.lr_time = first.next.next and l.lr_expiresAt = first.next.next.next.next
    scheduleWorkflowCommand[wf, cmd, shape, worker1, first.next.next, first.next.next.next]
    startStep[wf, st, oldAttempt, worker1, first.next.next.next, first.next.next.next.next]
    no owner: Worker | liveWorkflowLease[wf, owner, first.next.next.next.next]
    claimWorkflow[wf, worker2, first.next.next.next.next, first.next.next.next.next.next]
    startStep[wf, st, newAttempt, worker2, first.next.next.next.next.next, first.next.next.next.next.next.next]
    attemptStatus[oldAttempt, first.next.next.next.next.next.next] = AttemptFailed
    attemptStatus[newAttempt, first.next.next.next.next.next.next] = AttemptRunning
  }
}

pred exampleParallelCommandSchedules {
  some wf: Workflow, st1, st2: Step, cmd1, cmd2: WorkflowCommand, shape1, shape2: CommandShape, worker: Worker | {
    cmd1 != cmd2
    st1 != st2
    cmd1.wc_step = st1
    cmd2.wc_step = st2
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd1, shape1, worker, first.next.next, first.next.next.next]
    scheduleWorkflowCommand[wf, cmd2, shape2, worker, first.next.next.next, first.next.next.next.next]
    some commandScheduledShape[cmd1, first.next.next.next.next]
    some commandScheduledShape[cmd2, first.next.next.next.next]
  }
}

pred exampleFenceOutbox {
  some wf: Workflow, worker: Worker, token: FenceToken, f: Fence, o: OutboxMessage | {
    enqueueWorkflow[wf, first, first.next]
    acquireFence[f, wf, token, first.next, first.next.next]
    completeFence[f, token, worker, first.next.next, first.next.next.next]
    enqueueOutbox[o, wf, first.next.next.next, first.next.next.next.next]
    claimOutbox[o, worker, first.next.next.next.next, first.next.next.next.next.next]
    ackOutbox[o, worker, first.next.next.next.next.next, first.next.next.next.next.next.next]
  }
}

pred exampleAbandonedFenceRemainsRunningAfterCrashReplay {
  some wf: Workflow, originalToken, replayToken: FenceToken, f: Fence | {
    originalToken != replayToken
    enqueueWorkflow[wf, first, first.next]
    acquireFence[f, wf, originalToken, first.next, first.next.next]
    fenceStatus[f, first.next.next] = FenceRunning
    stutter[first.next.next, first.next.next.next]
    fenceStatus[f, first.next.next.next] = FenceRunning
    no r: FenceRow | r.fence_row = f and r.fence_time = first.next.next.next and r.fence_owner = replayToken
    stutter[first.next.next.next, first.next.next.next.next]
    fenceStatus[f, first.next.next.next.next] = FenceRunning
    no c: DurableCommit | c.commit_kind = FenceCommit
  }
}

pred exampleFenceFailureReplaysError {
  some wf: Workflow, worker: Worker, originalToken, replayToken: FenceToken, f: Fence | {
    originalToken != replayToken
    enqueueWorkflow[wf, first, first.next]
    acquireFence[f, wf, originalToken, first.next, first.next.next]
    failFence[f, originalToken, worker, first.next.next, first.next.next.next]
    fenceStatus[f, first.next.next.next] = FenceFailed
    no c: DurableCommit | c.commit_kind = FenceCommit
    stutter[first.next.next.next, first.next.next.next.next]
    fenceStatus[f, first.next.next.next.next] = FenceFailed
    no r: FenceRow | r.fence_row = f and r.fence_time = first.next.next.next.next and r.fence_owner = replayToken
  }
}

pred exampleOutboxExpiryReclaimAndAck {
  some wf: Workflow, o: OutboxMessage, worker1, worker2: Worker | {
    worker1 != worker2
    enqueueWorkflow[wf, first, first.next]
    enqueueOutbox[o, wf, first.next, first.next.next]
    claimOutbox[o, worker1, first.next.next, first.next.next.next]
    stutter[first.next.next.next, first.next.next.next.next]
    no owner: Worker | liveOutboxLease[o, owner, first.next.next.next.next]
    claimOutbox[o, worker2, first.next.next.next.next, first.next.next.next.next.next]
    ackOutbox[o, worker2, first.next.next.next.next.next, first.next.next.next.next.next.next]
    outboxStatus[o, first.next.next.next.next.next.next] = OutboxAcked
  }
}

pred exampleScheduledCommandReplayBeforeStepStart {
  some wf: Workflow, st: Step, att: Attempt, cmd: WorkflowCommand, shape: CommandShape, worker: Worker | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    stepStatus[st, first.next.next.next] = StepScheduled
    stutter[first.next.next.next, first.next.next.next.next]
    some commandScheduledShape[cmd, first.next.next.next.next]
    startStep[wf, st, att, worker, first.next.next.next.next, first.next.next.next.next.next]
    stepStatus[st, first.next.next.next.next.next] = StepRunning
  }
}

pred exampleTerminalCommandHistoryCanResolveOutOfScheduleOrder {
  some wf: Workflow, worker: Worker, cmd0, cmd1: WorkflowCommand, step0, step1: Step, att0, att1: Attempt, shape0, shape1: CommandShape | {
    cmd0 != cmd1
    step0 != step1
    att0 != att1
    cmd0.wc_workflow = wf
    cmd0.wc_step = step0
    cmd1.wc_workflow = wf
    cmd1.wc_step = step1
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    some l: LeaseRow | l.lr_workflow = wf and l.lr_time = first.next.next and l.lr_expiresAt = last
    scheduleWorkflowCommand[wf, cmd0, shape0, worker, first.next.next, first.next.next.next]
    scheduleWorkflowCommand[wf, cmd1, shape1, worker, first.next.next.next, first.next.next.next.next]
    startStep[wf, step0, att0, worker, first.next.next.next.next, first.next.next.next.next.next]
    startStep[wf, step1, att1, worker, first.next.next.next.next.next, first.next.next.next.next.next.next]
    completeStep[wf, step1, att1, worker, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
    completeStep[wf, step0, att0, worker, first.next.next.next.next.next.next.next, first.next.next.next.next.next.next.next.next]
    lt[
      commandHistorySequence[cmd0, CommandScheduled, first.next.next.next.next.next.next.next.next],
      commandHistorySequence[cmd1, CommandScheduled, first.next.next.next.next.next.next.next.next]
    ]
    lt[
      commandHistorySequence[cmd1, CommandSucceeded, first.next.next.next.next.next.next.next.next],
      commandHistorySequence[cmd0, CommandSucceeded, first.next.next.next.next.next.next.next.next]
    ]
  }
}

pred exampleInboxCommandCompletes {
  some target: InboxTarget, cmd: InboxCommand, worker: Worker | {
    cmd.command_target = target
    enqueueInboxCommand[cmd, first, first.next]
    claimTargetActivation[target, worker, first.next, first.next.next]
    claimInboxCommand[cmd, worker, first.next.next, first.next.next.next]
    completeInboxCommand[cmd, worker, first.next.next.next, first.next.next.next.next]
  }
}

pred exampleWorkflowInboxCommandCompletes {
  some wf: Workflow, target: InboxTarget, cmd: InboxCommand, worker: Worker | {
    target.target_kind = WorkflowInbox
    target.target_workflow = wf
    cmd.command_target = target
    enqueueWorkflow[wf, first, first.next]
    enqueueInboxCommand[cmd, first.next, first.next.next]
    claimTargetActivation[target, worker, first.next.next, first.next.next.next]
    claimWorkflow[wf, worker, first.next.next.next, first.next.next.next.next]
    claimInboxCommand[cmd, worker, first.next.next.next.next, first.next.next.next.next.next]
    completeInboxCommand[cmd, worker, first.next.next.next.next.next, first.next.next.next.next.next.next]
  }
}

pred exampleWorkflowInboxActivatesWaitingWorkflow {
  -- [DURABABBLE-LEASE-1] The activation drain path wakes a *waiting* workflow to
  -- deliver an inbox command (claim_workflow_for_activation), commits the command
  -- under the workflow lease, and re-suspends the workflow (suspend_workflow).
  some wf: Workflow, st: Step, wait: Wait, target: InboxTarget, ic: InboxCommand, worker: Worker, cmd: WorkflowCommand, shape: CommandShape | {
    target.target_kind = WorkflowInbox
    target.target_workflow = wf
    ic.command_target = target
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    recordWait[wf, st, none, wait, worker, first.next.next.next, first.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next] = Waiting
    enqueueInboxCommand[ic, first.next.next.next.next, first.next.next.next.next.next]
    claimTargetActivation[target, worker, first.next.next.next.next.next, first.next.next.next.next.next.next]
    claimWorkflowForActivation[wf, worker, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next.next.next] = Running
    stepStatus[st, first.next.next.next.next.next.next.next] = StepWaiting
    waitStatus[wait, first.next.next.next.next.next.next.next] = WaitPending
    claimInboxCommand[ic, worker, first.next.next.next.next.next.next.next, first.next.next.next.next.next.next.next.next]
    completeInboxCommand[ic, worker, first.next.next.next.next.next.next.next.next, first.next.next.next.next.next.next.next.next.next]
    suspendWorkflow[wf, worker, first.next.next.next.next.next.next.next.next.next, first.next.next.next.next.next.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next.next.next.next.next.next] = Waiting
  }
}

pred exampleLiveLeaseBlocksCompetingClaim {
  -- [DURABABBLE-LEASE-1] Near-miss witness: while one worker holds a live workflow
  -- lease, no other worker can claim the same workflow.
  some wf: Workflow, owner, other: Worker | {
    owner != other
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, owner, first.next, first.next.next]
    liveWorkflowLease[wf, owner, first.next.next]
    no w: Worker, tn: Time | w != owner and claimWorkflow[wf, w, first.next.next, tn]
  }
}

pred exampleStaleOwnerCannotCommitStep {
  -- [DURABABBLE-LEASE-4] Near-miss witness: after a lease expires and is reclaimed by
  -- another worker, the original (now stale) owner cannot commit the step it started.
  some wf: Workflow, st: Step, att: Attempt, owner, thief: Worker, cmd: WorkflowCommand, shape: CommandShape | {
    owner != thief
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, owner, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, owner, first.next.next, first.next.next.next]
    startStep[wf, st, att, owner, first.next.next.next, first.next.next.next.next]
    no l: LeaseRow | l.lr_workflow = wf and l.lr_time = first.next.next.next.next and gt[l.lr_expiresAt, first.next.next.next.next]
    releaseOrStealLease[wf, thief, first.next.next.next.next, first.next.next.next.next.next]
    claimWorkflow[wf, thief, first.next.next.next.next.next, first.next.next.next.next.next.next]
    stepStatus[st, first.next.next.next.next.next.next] = StepRunning
    not completeStep[wf, st, att, owner, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
  }
}

pred exampleBackedOffWorkflowEventuallyRuns {
  -- [DURABABBLE-STEP-2] Liveness/progress witness: a workflow parked with a retry
  -- backoff deadline cannot be claimed early, but once its due time arrives it is
  -- claimable again and reaches Running.
  some wf: Workflow, st: Step, att: Attempt, worker: Worker, due: Time, cmd: WorkflowCommand, shape: CommandShape | {
    cmd.wc_workflow = wf
    cmd.wc_step = st
    enqueueWorkflow[wf, first, first.next]
    claimWorkflow[wf, worker, first.next, first.next.next]
    scheduleWorkflowCommand[wf, cmd, shape, worker, first.next.next, first.next.next.next]
    startStep[wf, st, att, worker, first.next.next.next, first.next.next.next.next]
    retryStep[wf, st, att, worker, due, first.next.next.next.next, first.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next] = Pending
    workflowNextRun[wf, first.next.next.next.next.next] = due
    due = first.next.next.next.next.next.next
    no w: Worker, tn: Time | claimWorkflow[wf, w, first.next.next.next.next.next, tn]
    stutter[first.next.next.next.next.next, first.next.next.next.next.next.next]
    claimWorkflow[wf, worker, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
    workflowStatus[wf, first.next.next.next.next.next.next.next] = Running
  }
}

pred exampleInboxCommandEnqueues {
  some target: InboxTarget, cmd: InboxCommand | {
    cmd.command_target = target
    enqueueInboxCommand[cmd, first, first.next]
    commandStatus[cmd, first.next] = CommandPending
    activationStatus[target, first.next] = ActivationPending
  }
}

pred exampleTargetActivationClaims {
  some target: InboxTarget, cmd: InboxCommand, worker: Worker | {
    cmd.command_target = target
    enqueueInboxCommand[cmd, first, first.next]
    claimTargetActivation[target, worker, first.next, first.next.next]
    liveTargetActivation[target, worker, first.next.next]
  }
}

pred exampleInboxCommandClaims {
  some target: InboxTarget, cmd: InboxCommand, worker: Worker | {
    cmd.command_target = target
    enqueueInboxCommand[cmd, first, first.next]
    claimTargetActivation[target, worker, first.next, first.next.next]
    claimInboxCommand[cmd, worker, first.next.next, first.next.next.next]
    liveCommandLease[cmd, worker, first.next.next.next]
  }
}

pred exampleInboxCommandFailureRetry {
  some target: InboxTarget, cmd: InboxCommand, worker1, worker2: Worker | {
    cmd.command_target = target
    worker1 != worker2
    enqueueInboxCommand[cmd, first, first.next]
    claimTargetActivation[target, worker1, first.next, first.next.next]
    claimInboxCommand[cmd, worker1, first.next.next, first.next.next.next]
    failInboxCommand[cmd, worker1, first.next.next.next, first.next.next.next.next]
    completeTargetActivation[target, worker1, first.next.next.next.next, first.next.next.next.next.next]
    claimTargetActivation[target, worker2, first.next.next.next.next.next, first.next.next.next.next.next.next]
    claimInboxCommand[cmd, worker2, first.next.next.next.next.next.next, first.next.next.next.next.next.next.next]
    commandStatus[cmd, first.next.next.next.next.next.next.next] = CommandRunning
  }
}

pred exampleInboxCommandFifoHeadBlocksLaterCommand {
  some target: InboxTarget, head, tail: InboxCommand, worker: Worker | {
    head != tail
    target.target_kind = ObjectInbox
    head.command_target = target
    tail.command_target = target
    lt[head.command_sequence, tail.command_sequence]
    tail.command_sequence = head.command_sequence.next
    enqueueInboxCommand[head, first, first.next]
    enqueueInboxCommand[tail, first.next, first.next.next]
    claimTargetActivation[target, worker, first.next.next, first.next.next.next]
    some r: TargetActivationRow | r.activation_target = target and r.activation_time = first.next.next.next and r.activation_expiresAt = last
    claimInboxCommand[head, worker, first.next.next.next, first.next.next.next.next]
    failInboxCommand[head, worker, first.next.next.next.next, first.next.next.next.next.next]
    completeTargetActivation[target, worker, first.next.next.next.next.next, first.next.next.next.next.next.next]
    commandStatus[head, first.next.next.next.next.next.next] = CommandFailed
    commandStatus[tail, first.next.next.next.next.next.next] = CommandPending
    activationStatus[target, first.next.next.next.next.next.next] = ActivationPending
  }
}

pred exampleInboxCommandFailureRearmsActivation {
  some target: InboxTarget, cmd: InboxCommand, worker: Worker | {
    cmd.command_target = target
    enqueueInboxCommand[cmd, first, first.next]
    claimTargetActivation[target, worker, first.next, first.next.next]
    claimInboxCommand[cmd, worker, first.next.next, first.next.next.next]
    failInboxCommand[cmd, worker, first.next.next.next, first.next.next.next.next]
    completeTargetActivation[target, worker, first.next.next.next.next, first.next.next.next.next.next]
    commandStatus[cmd, first.next.next.next.next.next] = CommandFailed
    activationStatus[target, first.next.next.next.next.next] = ActivationPending
  }
}

pred exampleTargetActivationExpiresAndReclaims {
  some target: InboxTarget, cmd: InboxCommand, worker1, worker2: Worker | {
    cmd.command_target = target
    worker1 != worker2
    enqueueInboxCommand[cmd, first, first.next]
    claimTargetActivation[target, worker1, first.next, first.next.next]
    stutter[first.next.next, first.next.next.next]
    no owner: Worker | liveTargetActivation[target, owner, first.next.next.next]
    claimTargetActivation[target, worker2, first.next.next.next, first.next.next.next.next]
    liveTargetActivation[target, worker2, first.next.next.next.next]
  }
}

pred exampleInboxCommandDeadLettersAndStopsActivation {
  some target: InboxTarget, head, tail: InboxCommand, worker: Worker | {
    head != tail
    target.target_kind = ObjectInbox
    head.command_target = target
    tail.command_target = target
    head.command_sequence = first
    tail.command_sequence = first.next
    enqueueInboxCommand[head, first, first.next]
    enqueueInboxCommand[tail, first.next, first.next.next]
    claimTargetActivation[target, worker, first.next.next, first.next.next.next]
    some r: TargetActivationRow | r.activation_target = target and r.activation_time = first.next.next.next and r.activation_expiresAt = last
    claimInboxCommand[head, worker, first.next.next.next, first.next.next.next.next]
    deadLetterInboxCommand[head, worker, first.next.next.next.next, first.next.next.next.next.next]
    commandStatus[head, first.next.next.next.next.next] = CommandDeadLettered
    commandStatus[tail, first.next.next.next.next.next] = CommandPending
    completeTargetActivation[target, worker, first.next.next.next.next.next, first.next.next.next.next.next.next]
    no activationStatus[target, first.next.next.next.next.next.next]
  }
}

run exampleWorkflowCompletes for 8 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 WorkflowCommand, 1 CommandShape, 6 Time expect 1
run exampleLeaseStealAndReplay for 10 but exactly 1 Workflow, 2 Worker, 1 Step, 1 Attempt, 1 WorkflowCommand, 1 CommandShape, 8 Time expect 1
run exampleWaitWake for 10 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 Wait, 1 WaitTrigger, 1 WorkflowCommand, 1 CommandShape, 14 CommandHistoryRow, 8 Time expect 1
run exampleDirectWaitWake for 9 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 Wait, 1 WaitTrigger, 1 WorkflowCommand, 1 CommandShape, 6 Time expect 1
run exampleWaitAllowsSiblingCompletionBeforeSuspension for 7 but exactly 1 Workflow, exactly 1 Worker, exactly 2 Step, exactly 2 Attempt, exactly 1 Wait, 0 WaitTrigger, exactly 2 WorkflowCommand, exactly 2 CommandShape, exactly 8 WorkflowRow, exactly 11 StepRow, exactly 7 AttemptRow, exactly 7 LeaseRow, exactly 2 WaitRow, exactly 21 CommandHistoryRow, exactly 1 DurableCommit, 0 WakeEvent, 0 Fence, 0 FenceToken, 0 OutboxMessage, 0 InboxTarget, 0 InboxCommand, 0 OutboxAck, 0 FenceRow, 0 OutboxRow, 0 CommandRow, 0 TargetActivationRow, 9 Time expect 1
run exampleRetryBackoff for 8 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 WorkflowCommand, 1 CommandShape, exactly 9 CommandHistoryRow, 7 Time expect 1
run exampleRetryThenCompletesWithFailedAttemptHistory for 6 but exactly 1 Workflow, exactly 1 Worker, exactly 1 Step, exactly 2 Attempt, exactly 1 WorkflowCommand, exactly 1 CommandShape, exactly 9 WorkflowRow, exactly 7 StepRow, exactly 8 AttemptRow, exactly 6 LeaseRow, exactly 21 CommandHistoryRow, exactly 1 DurableCommit, 0 Wait, 0 WaitTrigger, 0 Fence, 0 FenceToken, 0 OutboxMessage, 0 InboxTarget, 0 InboxCommand, 0 WakeEvent, 0 OutboxAck, 0 WaitRow, 0 FenceRow, 0 OutboxRow, 0 CommandRow, 0 TargetActivationRow, 10 Time expect 1
run exampleStepFailureReplaysAsTerminalHistory for 8 but exactly 1 Workflow, exactly 1 Worker, exactly 1 Step, exactly 1 Attempt, exactly 1 WorkflowCommand, exactly 1 CommandShape, 6 Time expect 1
run exampleCancellationCompletesAfterCleanup for 9 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 WorkflowCommand, 1 CommandShape, 8 Time expect 1
run exampleWaitingCancellationCancelsWait for 10 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 Wait, 1 WorkflowCommand, 1 CommandShape, exactly 12 CommandHistoryRow, 8 Time expect 1
run exampleTerminalCancellationCleansPendingWait for 10 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 Wait, 1 WorkflowCommand, 1 CommandShape, exactly 9 CommandHistoryRow, 7 Time expect 1
run exampleRunningStepCancellationRecordsHistory for 8 but exactly 1 Workflow, exactly 1 Worker, exactly 1 Step, exactly 1 Attempt, exactly 1 WorkflowCommand, exactly 1 CommandShape, 7 Time expect 1
run exampleBackoffCancellationClearsDue for 9 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 1
run exampleRunningCancellationMetadataReleasedToCanceling for 7 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 WorkflowCommand, 1 CommandShape, 5 Time expect 1
run exampleExpiredRunningWorkflowReclaimedDirectly for 8 but exactly 1 Workflow, 2 Worker, 1 Step, 1 Attempt, 1 WorkflowCommand, 1 CommandShape, 6 Time expect 1
run exampleStepStart for 6 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 WorkflowCommand, 1 CommandShape, 5 Time expect 1
run exampleStepStartSupersedesRunningAttempt for 8 but exactly 1 Workflow, 2 Worker, 1 Step, 2 Attempt, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 1
run exampleParallelCommandSchedules for 8 but exactly 1 Workflow, 1 Worker, 2 Step, 2 WorkflowCommand, 2 CommandShape, 6 Time expect 1
run exampleFenceOutbox for 9 but exactly 1 Workflow, 1 Worker, 1 FenceToken, 1 Fence, 1 OutboxMessage, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 1
run exampleAbandonedFenceRemainsRunningAfterCrashReplay for 8 but exactly 1 Workflow, 1 Worker, exactly 1 Step, 2 FenceToken, 1 Fence, 1 WorkflowCommand, 1 CommandShape, 0 DurableCommit, 5 Time expect 1
run exampleFenceFailureReplaysError for 8 but exactly 1 Workflow, 1 Worker, exactly 1 Step, 2 FenceToken, 1 Fence, 1 WorkflowCommand, 1 CommandShape, 0 DurableCommit, 5 Time expect 1
run exampleOutboxExpiryReclaimAndAck for 10 but exactly 1 Workflow, 2 Worker, 1 OutboxMessage, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 1
run exampleScheduledCommandReplayBeforeStepStart for 9 but exactly 1 Workflow, 1 Worker, 1 Step, 1 Attempt, 1 WorkflowCommand, 1 CommandShape, 6 Time expect 1
run exampleTerminalCommandHistoryCanResolveOutOfScheduleOrder for 4 but exactly 1 Workflow, exactly 1 Worker, exactly 2 Step, exactly 2 Attempt, exactly 2 WorkflowCommand, exactly 2 CommandShape, exactly 8 WorkflowRow, exactly 11 StepRow, exactly 7 AttemptRow, exactly 7 LeaseRow, exactly 21 CommandHistoryRow, exactly 2 DurableCommit, 0 Wait, 0 WaitTrigger, 0 Fence, 0 FenceToken, 0 OutboxMessage, 0 InboxTarget, 0 InboxCommand, 0 WakeEvent, 0 OutboxAck, 0 WaitRow, 0 FenceRow, 0 OutboxRow, 0 CommandRow, 0 TargetActivationRow, 9 Time expect 1
run exampleInboxCommandEnqueues for 5 but exactly 1 InboxTarget, 1 InboxCommand, exactly 1 Workflow, exactly 1 Step, 1 WorkflowCommand, 1 CommandShape, 0 DurableCommit, 3 Time expect 1
run exampleTargetActivationClaims for 6 but exactly 1 InboxTarget, 1 InboxCommand, 1 Worker, exactly 1 Workflow, exactly 1 Step, 1 WorkflowCommand, 1 CommandShape, 0 DurableCommit, 4 Time expect 1
run exampleInboxCommandClaims for 7 but exactly 1 InboxTarget, 1 InboxCommand, 1 Worker, exactly 1 Workflow, exactly 1 Step, 1 WorkflowCommand, 1 CommandShape, 0 DurableCommit, 5 Time expect 1
run exampleInboxCommandCompletes for 7 but exactly 1 InboxTarget, 1 InboxCommand, 1 Worker, exactly 1 Workflow, exactly 1 Step, 1 WorkflowCommand, 1 CommandShape, 1 DurableCommit, 5 Time expect 1
run exampleWorkflowInboxCommandCompletes for 8 but exactly 1 InboxTarget, 1 InboxCommand, 1 Worker, exactly 1 Workflow, exactly 1 Step, 1 WorkflowCommand, 1 CommandShape, 1 DurableCommit, 7 Time expect 1
run exampleInboxCommandFailureRetry for 9 but exactly 1 InboxTarget, 1 InboxCommand, 2 Worker, exactly 1 Workflow, exactly 1 Step, 1 WorkflowCommand, 1 CommandShape, 0 DurableCommit, 9 Time expect 1
run exampleInboxCommandFifoHeadBlocksLaterCommand for 12 but exactly 1 InboxTarget, 2 InboxCommand, 1 Worker, exactly 1 Workflow, exactly 1 Step, 1 WorkflowCommand, 1 CommandShape, 0 DurableCommit, 7 Time expect 1
run exampleInboxCommandFailureRearmsActivation for 7 but exactly 1 InboxTarget, 1 InboxCommand, 1 Worker, exactly 1 Workflow, exactly 1 Step, 1 WorkflowCommand, 1 CommandShape, 0 DurableCommit, 6 Time expect 1
run exampleTargetActivationExpiresAndReclaims for 7 but exactly 1 InboxTarget, 1 InboxCommand, 2 Worker, exactly 1 Workflow, exactly 1 Step, 1 WorkflowCommand, 1 CommandShape, 0 DurableCommit, 6 Time expect 1
run exampleInboxCommandDeadLettersAndStopsActivation for 10 but exactly 1 InboxTarget, 2 InboxCommand, 13 CommandRow, 1 Worker, exactly 1 Workflow, exactly 1 Step, 1 WorkflowCommand, 1 CommandShape, 0 DurableCommit, 8 Time expect 1
run exampleWorkflowInboxActivatesWaitingWorkflow for 16 but exactly 1 InboxTarget, exactly 1 InboxCommand, exactly 1 Worker, exactly 1 Workflow, exactly 1 Step, 1 Attempt, 0 AttemptRow, exactly 1 Wait, exactly 1 WorkflowCommand, exactly 1 CommandShape, 1 WaitTrigger, 0 WakeEvent, 1 DurableCommit, 16 CommandHistoryRow, 11 Time expect 1
run exampleLiveLeaseBlocksCompetingClaim for 4 but exactly 1 Workflow, exactly 2 Worker, 4 Time expect 1
run exampleStaleOwnerCannotCommitStep for 8 but exactly 1 Workflow, exactly 2 Worker, exactly 1 Step, exactly 1 Attempt, exactly 1 WorkflowCommand, exactly 1 CommandShape, 12 CommandHistoryRow, 8 Time expect 1
run exampleBackedOffWorkflowEventuallyRuns for 9 but exactly 1 Workflow, exactly 1 Worker, exactly 1 Step, exactly 1 Attempt, exactly 1 WorkflowCommand, exactly 1 CommandShape, 16 CommandHistoryRow, 9 Time expect 1

check atMostOneLiveOwner for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
check terminalStatesDoNotMutate for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
check terminalWorkflowsHaveNoIncompleteWork for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
check completedStepsAreNotReexecuted for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
check scheduledCommandHistoryIsReplayStable for 4 but 2 Workflow, 2 Worker, 3 Step, 3 WorkflowCommand, 3 CommandShape, 6 Time expect 0
check terminalCommandHistoryUsesLatestReplayEvent for 4 but 1 Workflow, 2 Worker, 2 Step, 2 Attempt, 2 Wait, 2 WaitTrigger, 2 WorkflowCommand, 2 CommandShape, 7 Time expect 0
check commandHistoryFollowsRuntimeLifecycle for 4 but 1 Workflow, 2 Worker, 2 Step, 2 Attempt, 2 Wait, 2 WaitTrigger, 2 WorkflowCommand, 2 CommandShape, 7 Time expect 0
check incompleteStepsRetrySafely for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
check retryBackoffPreventsEarlyClaim for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
check waitsWakeOnce for 4 but 2 Workflow, 2 Worker, 2 Step, 3 Attempt, 3 Wait, 2 WaitTrigger, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
check staleOwnersCannotCommit for 4 but 2 Workflow, 3 Worker, 3 Step, 4 Attempt, 1 WorkflowCommand, 1 CommandShape, 8 Time expect 0
check idempotencyFencesPreventDuplicateSideEffects for 4 but 2 Workflow, 2 Worker, 2 Fence, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
check staleFenceTokensCannotFinish for 4 but 2 Workflow, 2 Worker, 2 FenceToken, 2 Fence, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
check outboxAckLeaseBehaviorIsSafe for 4 but 2 Workflow, 2 Worker, 3 OutboxMessage, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
check durableInboxCommandSerializationHolds for 4 but 2 InboxTarget, 4 InboxCommand, 3 Worker, 1 WorkflowCommand, 1 CommandShape, 6 Time expect 0
check inboxClaimsRequireExistingRows for 4 but 2 InboxTarget, 4 InboxCommand, 3 Worker, 1 WorkflowCommand, 1 CommandShape, 6 Time expect 0
check workflowInboxCommandCommitsNeedWorkflowLease for 4 but 2 Workflow, 2 InboxTarget, 4 InboxCommand, 3 Worker, 1 WorkflowCommand, 1 CommandShape, 7 Time expect 0
