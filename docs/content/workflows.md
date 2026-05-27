---
title: "Workflows"
weight: 20
---

# Workflows

Use a durable workflow when you want to describe a process from beginning to end. The workflow's `#execute` method is the recipe, and each step method or reusable step constant is a checkpoint where Durababble persists what happened when some side effect executed. If a worker dies after step two of five, another worker can replay the workflow, reuse the completed step results from the database, and continue at the next unfinished step instead of starting over.

Good workflow-shaped examples include charging then shipping an order, importing a large CSV across several API calls, running a complicated LLM agent turn with tool calls till completion, or an automated review process that sometimes has a human-in-the-loop approval step. The key sign is that the work has a start, an expected finish, and a sequence of durable side effects you do not want to accidentally repeat.

Because they are durable, durable workflows can sleep for many days, await some event that might take a long time to occur, be cancelled mid-execution, and return a result to callers.

<!-- DOCS:workflow-example:start -->

<!-- DOCS:workflow-example:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
# an example workflow
class FulfillOrder < Durababble::Workflow
  def execute(order)
    payment = charge_card(order)
    label = buy_shipping_label(order, payment)

    { "payment_id" => payment.fetch("id"), "label_id" => label.fetch("id") }
  end

  step retry: { maximum_attempts: 5, schedule: [1, 5, 30] }
  def charge_card(order)
    Payments.charge(
      order.fetch("card_token"),
      amount: order.fetch("total_cents"),
      idempotency_key: step_context.idempotency_key,
    )
  end

  step def buy_shipping_label(order, payment)
    Shipping.buy_label(
      order.fetch("address"),
      payment_id: payment.fetch("id"),
      idempotency_key: step_context.idempotency_key,
    )
  end
end
```

<!-- DOCS:workflow-example:hidden
```ruby
module Payments
  def self.charge(card_token, amount:, idempotency_key:)
    { "id" => "pay_#{card_token}", "amount" => amount, "key" => idempotency_key }
  end
end

module Shipping
  def self.buy_label(address, payment_id:, idempotency_key:)
    { "id" => "label_#{payment_id}", "address" => address, "key" => idempotency_key }
  end
end
```
-->

```ruby
# enqueue the workflow and keep a typed handle
fulfillment = FulfillOrder.start({
  "card_token" => "card_123",
  "total_cents" => 5_000,
  "address" => { "postal_code" => "10001" },
})
```

<!-- DOCS:workflow-example:hidden
```ruby
worker = Durababble::Worker.new(
  store:,
  workflows: [FulfillOrder],
  worker_id: "orders-worker-1",
  migrate: false,
)
worker.run_until_idle
```
-->

```ruby
# later, any process can recover the same handle by id
fulfillment = FulfillOrder.at(fulfillment.workflow_id)
fulfillment.result
```

<!-- DOCS:workflow-example:hidden
```ruby
fulfillment.result
```
-->

<!-- DOCS:workflow-example:end -->

## Enqueuing And Workflow Handles

Outside workflow execution, `Workflow.enqueue` creates a workflow row through the configured default engine and returns the workflow id. In an application, you usually enqueue first and let one or more workers claim the run under SQL leases. Pass `id:` when the caller needs to choose a stable workflow id; Durababble persists that exact id and raises `Durababble::WorkflowAlreadyExists` if it is already taken. Pass `engine:` when you want an explicit engine instead of the configured default.

```ruby
workflow_id = FulfillOrder.enqueue(order)
workflow_id = FulfillOrder.enqueue(order, id: "fulfillment-order-123")

# in some other long lived process, you'd do:
worker = Durababble::Worker.new(
  store: Durababble.store,
  workflows: [FulfillOrder],
  worker_id: "orders-worker-1",
)
worker.run_until_idle
```

`FulfillOrder.start(order)` is a convenience that enqueues and returns a handle immediately. It also accepts `id:` and returns a handle whose `workflow_id` is exactly that value. `FulfillOrder.at(workflow_id)` and `FulfillOrder.handle(workflow_id)` give you the same handle later, so web requests, jobs, or other workflows can query or command the durable run without knowing which worker owns it. Each helper accepts `engine:` when a caller needs to route through a non-default engine.

```ruby
handle = FulfillOrder.start(order, id: "fulfillment-order-123")
handle.workflow_id
handle.cancel(reason: "customer requested cancellation")
handle.terminate(reason: "operator hard stop")
```

## Calling Handles From Workflow Code

Workflow orchestration code can use the same workflow and durable-object handles that external callers use. You do not need to wrap every handle call in an explicit `step`; Durababble records the handle RPC itself as workflow history before dispatching it.

```ruby
class CollectApproval < Durababble::Workflow
  def execute(input)
    approval = ApprovalWorkflow.handle(input.fetch("approval_workflow_id"))
    counter = ApprovalCounter.at(input.fetch("counter_id"))

    counter.increment(1)
    approval.approve(reason: "workflow")
  end
end
```

The calls above are persisted as ordered workflow command history entries with names such as `handle_rpc:object:approval_counter:increment` and `handle_rpc:workflow:approval_workflow:approve`. On replay or crash recovery, completed handle RPC results are returned from history and Durababble does not enqueue the outbound workflow/object command a second time. `DurableObject.tell(id, method, ...)` works the same way from workflow code and replays to the original tell message id.

## Child Workflows

Workflow orchestration can start another workflow as a child by calling that workflow's normal class helper: `CreateShipment.enqueue(input, ...)` or `CreateShipment.start(input, ...)`. Outside workflow execution, `enqueue` still returns the workflow id and `start` still returns a regular workflow handle; inside workflow execution, both calls schedule a durable child start and return a `Durababble::ChildWorkflowHandle`, which has `workflow_id`, `worker_pool`, `status`, `result`, `error`, `await`, `cancel`, `terminate`, and `ref`.

```ruby
class ShipOrder < Durababble::Workflow
  def execute(order)
    handle = CreateShipment.enqueue(
      { "order_id" => order.fetch("id") },
      id: "shipment-#{order.fetch('id')}",
      cancellation: :request_cancel,
    )

    shipment = handle.result
    notify_customer(order, shipment)
  end

  step def notify_customer(order, shipment)
    Notifications.shipped(order.fetch("id"), shipment.fetch("tracking_number"), idempotency_key: step_context.idempotency_key)
  end
end
```

Starting a child is safe to retry after crashes. If the parent workflow crashes after asking for the child, replay continues with the same child handle instead of starting another run. Passing `id:` gives the child a stable workflow id that application code can inspect or reuse across systems; if `id:` is omitted, Durababble chooses a stable id for that parent command.

In workflow code, `handle.result` observes the child through recorded workflow commands and durably waits until the child is terminal. If the child is still pending or running, the parent records a timer wait and suspends normally; a crash while waiting resumes from the same child handle. When the child completes, `result` returns the child result. If the child fails, cancels, or is terminated, `result` raises `Durababble::ChildWorkflowFailed`, `Durababble::ChildWorkflowCanceled`, or `Durababble::ChildWorkflowTerminated`, so parent workflow code can rescue and handle the outcome. `handle.await(poll_interval:, timeout:)` remains available when workflow code needs an explicit poll interval or timeout, while external `handle.result` reads the latest stored result without blocking.

Child workflows inherit the parent worker pool unless `worker_pool:` is supplied. A child in another worker pool is not claimed by parent-pool workers, and a child whose workflow class is not registered in any worker remains pending and inspectable until a matching worker is deployed or an operator cancels/terminates it.

Cancellation is explicit. `cancellation: :request_cancel` asks Durababble to request child cancellation when the parent observes cooperative cancellation. `cancellation: :abandon` leaves the child running or pending if the parent is canceled. Parent hard termination does not request child cancellation or run cleanup; use the child handle if the child should be stopped separately.

### Deduplicating Workflow Starts

Use deterministic workflow ids when the caller has a natural idempotency key, such as an order id, import id, or external request id. `FulfillOrder.enqueue(order, id: "fulfillment-order-123")` and `FulfillOrder.start(order, id: "fulfillment-order-123")` insert the workflow row with that exact id. If any workflow row already has that id, Durababble raises `Durababble::WorkflowAlreadyExists` before creating workflow history, waits, inbox messages, or activations.

Workflow ids are permanent uniqueness keys. A completed, failed, canceled, or terminated workflow still counts as existing, so a later enqueue with the same deterministic id is rejected rather than starting a second run. Callers that want retry-after-completion semantics should choose a new workflow id, such as one that includes an attempt number or version.

## Sleeping

A workflow can park itself without keeping a worker thread busy. `sleep_until(time, context)` and `wait_until(time, context)` are timer waits: the workflow resumes at or after the given time. Under the hood, Durababble stores the wait, releases the worker lease, and wakes the workflow when the timer is due.

Timer waits are useful for reminders, delayed retries that are part of business logic, cooling-off periods, scheduled followups, or "do not continue before this time" gates:

<!-- DOCS:workflow-sleep-example:start -->

<!-- DOCS:workflow-sleep-example:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class SendReminderAfterDelay < Durababble::Workflow
  def execute(reminder)
    after_delay = sleep_until(reminder.fetch("send_at"), reminder)
    send_reminder(after_delay)
  end

  step def send_reminder(reminder)
    Reminders.send(
      reminder.fetch("user_id"),
      reminder.fetch("message"),
      idempotency_key: step_context.idempotency_key,
    )
  end
end
```

<!-- DOCS:workflow-sleep-example:hidden
```ruby
module Reminders
  def self.send(user_id, message, idempotency_key:)
    { "sent_to" => user_id, "message" => message, "key" => idempotency_key }
  end
end
```
-->

```ruby
reminder = SendReminderAfterDelay.start({
  "user_id" => "user_123",
  "message" => "renew subscription",
  "send_at" => Time.now + 3600,
})
```

<!-- DOCS:workflow-sleep-example:hidden
```ruby
worker = Durababble::Worker.new(
  store:,
  workflows: [SendReminderAfterDelay],
  worker_id: "reminder-worker",
  migrate: false,
)
worker.run_until_idle
send_at = store.workflow(reminder.workflow_id).fetch("input").fetch("send_at")
store.wake_due_timers(now: send_at + 1)
worker.run_until_idle

{
  "status" => reminder.status,
  "sent_to" => reminder.result.fetch("sent_to"),
  "message" => reminder.result.fetch("message"),
}
```
-->

<!-- DOCS:workflow-sleep-example:end -->

The `context` you pass to `sleep_until` or `wait_until` is the value Durababble resumes the workflow with when the timer fires. For workflows that need to resume on an external signal rather than a clock — webhook delivery, human approval, a batch finishing elsewhere — use a workflow command (`expose_command`) from the signaling process instead.

Do not use `Thread.sleep` in workflow code, because that actually blocks the worker thread instead of durably parking the workflow. Direct host wall-clock time, randomness, blocking sleeps, process calls, and blocking file/IO calls from workflow orchestration raise `Durababble::DeterminismError`; put those effects in durable steps or outside workflow execution, where ordinary Ruby host semantics still apply.

## Cancellation

Workflows can be cancelled before they finish. `Workflow.handle(run_id).cancel(reason:)` durably records the reason and asks the workflow to cancel. Any outstanding steps will be cancelled and throw a `Durababble::CancellationError` back to your workflow, which will usually then shut the whole workflow down.

Cancellation is a request, not a hard kill, where the workflow has a chance to clean up, depending on how your workflow code handles the cancellation. Workflow code can let `Durababble::CancellationError` bubble, or it can rescue the error and run cleanup as ordinary durable steps. Cleanup steps get the same replay behavior as any other step, so a crash halfway through cancellation recovery does not force completed cleanup to run again.

<!-- DOCS:workflow-cancellation-example:start -->

<!-- DOCS:workflow-cancellation-example:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class ImportCustomers < Durababble::Workflow
  def execute(import)
    copy_rows(import)
  rescue Durababble::CancellationError => e
    mark_import_canceled(import.merge("reason" => e.reason))
    raise
  end

  step def copy_rows(import)
    Importer.copy(import.fetch("file_id"), idempotency_key: step_context.idempotency_key)
  end

  step def mark_import_canceled(import)
    Importer.mark_canceled(import.fetch("file_id"), reason: import.fetch("reason"))
  end
end
```

<!-- DOCS:workflow-cancellation-example:hidden
```ruby
module Importer
  def self.copy(file_id, idempotency_key:)
    { "file_id" => file_id, "copy_key" => idempotency_key }
  end

  def self.mark_canceled(file_id, reason:)
    { "file_id" => file_id, "status" => "canceled", "reason" => reason }
  end
end
```
-->

```ruby
handle = ImportCustomers.start({ "file_id" => "file_123" })
handle.cancel(reason: "user uploaded a replacement file")
```

<!-- DOCS:workflow-cancellation-example:hidden
```ruby
worker = Durababble::Worker.new(
  store:,
  workflows: [ImportCustomers],
  worker_id: "import-worker",
  migrate: false,
)
worker.run_until_idle

{
  "status" => handle.status,
  "result" => handle.result,
  "steps" => store.steps_for(handle.workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] },
}
```
-->

<!-- DOCS:workflow-cancellation-example:end -->

If a workflow is already completed, failed, or canceled, cancellation is idempotent and does not re-cancel.

## Termination

Termination is the hard-stop operator path. `Workflow.handle(run_id).terminate(reason:)` durably marks the workflow `terminated` without asking workflow code to observe a cancellation request and without running cleanup steps. The terminal run has no result and stores the reason as its error.

Use cancellation when workflow code should unwind, compensate, or mark external state through ordinary durable cleanup. Use termination when the caller needs the workflow to stop at the next safe durable boundary and must prevent later waits, workflow commands, completion writes, or recovery from reviving it.

```ruby
handle = ImportCustomers.start({ "file_id" => "file_123" })
handle.terminate(reason: "operator replaced the import")
```

If a workflow is already completed, failed, canceled, or terminated, termination is idempotent and returns the existing terminal run.

## Using `async` for parallelism

Workflow orchestration can use the normal `async` gem APIs. `Sync` gives the workflow a structured concurrency scope, and child tasks inherit the workflow context, so each branch can call durable steps directly. Durababble records every scheduled step, start, completion, failure, and wait in history, which makes scatter/gather and continuation fanout safe even when branches finish in a different order than they were started.

```ruby
class FetchProfiles < Durababble::Workflow
  def execute(user_ids)
    Sync do |task|
      tasks = user_ids.map do |user_id|
        task.async do
          profile = fetch_profile(user_id)
          score_profile(profile)
        end
      end

      tasks.map(&:result)
    end
  end

  step def fetch_profile(user_id)
    Profiles.fetch(user_id, idempotency_key: step_context.idempotency_key)
  end

  step def score_profile(profile)
    ProfileScoring.score(profile, idempotency_key: step_context.idempotency_key)
  end
end
```

If `fetch_profile(2)` completes before `fetch_profile(1)`, replay resumes the same branch first and records the dependent `score_profile` steps in the same order. If one branch fails after others complete, the completed durable steps stay completed and the failure is recorded against the failing branch.

## Reusable Step Constants

Use `Durababble.step("name") { ... }` when a step should live outside one workflow class, for example in another file that several workflows import. The returned object is an ordinary Ruby constant with `#call`; invoking it from workflow orchestration records the same durable schedule, attempt, retry, and result rows as a method step. Give each reusable step a stable explicit name, because that name is part of replay validation and stored step metadata.

```ruby
ChargeCardStep = Durababble.step("charge_card", retry: { maximum_attempts: 5, schedule: [1, 5, 30] }) do |order|
  Payments.charge(order.fetch("card_token"), idempotency_key: step_context.idempotency_key)
end

class FulfillOrder < Durababble::Workflow
  def execute(order)
    payment = ChargeCardStep.call(order)
    { "payment_id" => payment.fetch("id") }
  end
end
```

Reusable step bodies run with the workflow instance as `self`, so `step_context` works the same way it does in a `step def` method. If a reusable step delegates to plain helper functions, those helpers can call `Durababble.step_context` while the step is executing.

## RPC

Workflows can expose an RPC surface for other members of the Durababble cluster to invoke. Transient query RPCs return owner-local state without durable mutation, while command RPCs can change how the workflow execution will proceed.

RPCs are done by getting a workflow handle for your workflow, and then calling Ruby methods on the handle itself. A caller does not need a Ruby object in the same process as the worker; it needs the workflow id, the workflow class, and a store.

There are two kinds of RPCs you can expose: simple RPCs, and command RPCs.

Simple RPCs are transient queries. They route to the workflow's current active lease owner through `CallTransient`, run against the live workflow instance, and fail with the same not-running, stale-lease, or unavailable-owner errors as transient gRPC routing when no active owner can serve the query. They are not recorded durably, cannot use `idempotency_key:`, cannot call workflow steps or waits, and do not warm or start an inactive workflow.

Commands can mutate state on the workflow, and are processed in serial and recorded and redelivered durably. Use commands for RPCs that need to make it to the workflow, and that change the way the workflow will behave moving forward, like editing local state.

Workflows can declare simple RPC methods with `expose` on the class, and commands RPC methods with `expose_command`.

```ruby
class ReviewWorkflow < Durababble::Workflow
  expose def label
    "reviewable"
  end

  expose_command def note(message:)
    message
  end
end

handle = ReviewWorkflow.at(run_id)
handle.label
handle.note(message: "approved by legal")
```

Durababble handles the RPC machinery between your workers automatically, routing transient queries to the worker that currently owns the live workflow lease and routing durable commands through the inbox. If a workflow is not active when you send a command to it, Durababble will warm it up on a worker and then deliver your message; transient queries require an active owner and return a not-running error instead of creating durable work.

## Replay

Replay is what lets a workflow continue after a crash without rerunning completed side effects. When Durababble resumes a workflow, it calls `#execute` again from the top, but completed step positions return their persisted results instead of invoking the Ruby method body. The workflow code must therefore be deterministic around step calls. Any branch on input, persisted step results, or durable wait payloads must happen the same way it did the first time. For this reason, Durababble guards workflow orchestration against direct host randomness, wall-clock time, blocking sleeps, process calls, and blocking file/IO. The guard is scoped to managed workflow fibers, so step bodies and unrelated host fibers keep normal Ruby semantics.

```ruby
def execute(order)
  payment = charge_card(order)      # reused from storage if step 0 completed
  label = buy_shipping_label(order, payment) # reruns only if step 1 did not complete

  { "payment_id" => payment.fetch("id"), "label_id" => label.fetch("id") }
end
```

Replay is intentionally strict. If deployed code reaches a different completed step method at the same position, or returns before consuming completed history, the run fails with `Durababble::ReplayDivergenceError` instead of quietly attaching old side effects to new control flow.

### Workflow History Length

Every durable boundary leaves history behind: workflow rows, step rows, attempts, waits, retries, cancellation metadata, fences, and outbox rows. That history is what makes replay honest, but it also means workflows should usually be finite processes rather than permanent entities. A workflow that never ends accumulates an ever-growing event log, and as that log grows replay takes longer and system performance suffers.

Prefer durable objects for long-lived identities, and split very large jobs into a workflow per bounded batch or phase:

```ruby
# Prefer this shape for ongoing per-shop state:
ShopSync.at(shop_id).record_cursor(cursor)

# Prefer this shape for bounded work:
SyncOneShopBatch.enqueue({ "shop_id" => shop_id, "cursor" => cursor })
```

To keep an unbounded run from degrading silently, Durababble bounds replay by counting persisted `workflow_history` rows before loading replay payloads. The hard limit defaults to `10_000` events and can be tuned with `DURABABBLE_MAX_WORKFLOW_HISTORY_EVENTS` or `Durababble.max_workflow_history_events = 20_000`. The warning threshold defaults to `8_000` events and can be tuned with `DURABABBLE_WARN_WORKFLOW_HISTORY_EVENTS` or `Durababble.workflow_history_warning_events = 8_000`; reaching it logs a warning through `Durababble.logger` but does not stop the run.

When an open workflow exceeds the hard limit, resume fails durably with `Durababble::WorkflowHistoryLimitExceeded` and the workflow becomes terminal `failed`. The terminal failure clears workflow leases and retry deadlines, and terminal workflow target activations dead-letter pending workflow-command inbox work instead of re-arming it, so workers do not repeatedly claim the same oversized run. Completed, canceled, and failed workflows are returned as-is and remain inspectable.

Treat a warning log or `WorkflowHistoryLimitExceeded` as a design or retention signal rather than a number to raise reflexively. Reshape the workload using the patterns above, compact completed history through a deliberate retention tool, or raise the hard limit only after benchmarking replay latency with `mise exec -- ruby bench/run.rb --profile history-smoke`.
