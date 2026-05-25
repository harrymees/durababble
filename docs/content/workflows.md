---
title: "Workflows"
weight: 20
---

# Workflows

Use a durable workflow when you want to describe a process from beginning to end. The workflow's `#execute` method is the recipe, and each `step` method is a checkpoint where Durababble persists what happened when some side effect executed. If a worker dies after step two of five, another worker can replay the workflow, reuse the completed step results from the database, and continue at the next unfinished step instead of starting over.

Good workflow-shaped examples include charging then shipping an order, importing a large CSV across several API calls, running a complicated LLM agent turn with tool calls till completion, or an automated review process that sometimes has a human-in-the-loop approval step. The key sign is that the work has a start, an expected finish, and a sequence of durable side effects you do not want to accidentally repeat.

Because they are durable, durable workflows can sleep for many days, await some event that might take a long time to occur, be cancelled mid-execution, and return a result to callers.

<!-- DOCS:workflow-example:start -->

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

store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
engine ||= Durababble::Engine.new(store:)

order ||= {
  "card_token" => "card_123",
  "total_cents" => 5_000,
  "address" => { "postal_code" => "10001" },
}

# enqueue the workflow
run = engine.run(FulfillOrder, input: order)

# wait for it to finish and get its return value -- will overcome step failures or crashes or whatever strange issues arise by retrying each step, and eventually return!
run.result
```

<!-- DOCS:workflow-example:end -->

## Parallel Steps With Async

Workflow orchestration can use the normal `async` gem APIs. Child tasks inherit the workflow context, so each branch can call durable steps directly. Durababble records every scheduled step, start, completion, failure, and wait in history, which makes scatter/gather and continuation fanout safe even when branches finish in a different order than they were started.

```ruby
class FetchProfiles < Durababble::Workflow
  def execute(user_ids)
    Async do |task|
      user_ids.map do |user_id|
        task.async do
          profile = fetch_profile(user_id)
          score_profile(profile)
        end
      end.map(&:wait)
    end.wait
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

## Enqueuing And Workflow Handles

`Engine#run` is the small-script path: it creates a workflow row, runs it immediately in the current process, and returns the completed `Durababble::Run`. In an application, you usually enqueue first and let one or more workers claim the run under SQL leases.

```ruby
workflow_id = FulfillOrder.enqueue(order, store:)

# in some other long lived process, you'd do:
worker = Durababble::Worker.new(
  store:,
  workflows: [FulfillOrder],
  worker_id: "orders-worker-1",
)
worker.run_until_idle
```

`FulfillOrder.start(order, store:)` is a convenience that enqueues and returns a handle immediately. `FulfillOrder.handle(workflow_id, store:)` and `FulfillOrder.ref(workflow_id, store:)` give you the same handle later, so web requests, jobs, or other workflows can query or command the durable run without knowing which worker owns it.

```ruby
handle = FulfillOrder.start(order, store:)
handle.workflow_id
handle.cancel(reason: "customer requested cancellation")
```

## Sleeping

A workflow can park itself without keeping a worker thread busy. `wait_until(time, context)` is a timer wait: the workflow resumes at or after the given time. Under the hood, Durababble stores the wait, releases the worker lease, and wakes the workflow when the timer is due.

Timer waits are useful for reminders, delayed retries that are part of business logic, cooling-off periods, scheduled followups, or "do not continue before this time" gates:

```ruby
class SendReminderAfterDelay < Durababble::Workflow
  def execute(reminder)
    after_delay = sleep_until_reminder_time(reminder)
    send_reminder(after_delay)
  end

  step def sleep_until_reminder_time(reminder)
    wait_until(reminder.fetch("send_at"), reminder)
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

The `context` you pass to `wait_until` is the value Durababble resumes the workflow with when the timer fires. For workflows that need to resume on an external signal rather than a clock — webhook delivery, human approval, a batch finishing elsewhere — use a workflow command (`expose_command`) from the signaling process instead.

The current implementation records waits through step history, so the example returns the wait from a small step method. That is an implementation limitation, not the ideal public shape; workflow waits should become workflow-level yield points. Do not use `Thread.sleep` in workflow code, because that actually blocks the worker thread instead of durably parking the workflow.

## Cancellation

Workflows can be cancelled before they finish. `Workflow.handle(run_id).cancel(reason:)` durably records the reason and asks the workflow to cancel. Any outstanding steps will be cancelled and throw a `Durababble::CancellationError` back to your workflow, which will usually then shut the whole workflow down.

Cancellation is a request, not a hard kill, where the workflow has a chance to clean up, depending on how your workflow code handles the cancellation. Workflow code can let `Durababble::CancellationError` bubble, or it can rescue the error and run cleanup as ordinary durable steps. Cleanup steps get the same replay behavior as any other step, so a crash halfway through cancellation recovery does not force completed cleanup to run again.

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

handle = ImportCustomers.start({ "file_id" => "file_123" }, store:)
handle.cancel(reason: "user uploaded a replacement file")
```

If a workflow is already completed, failed, or canceled, cancellation is idempotent and does not re-cancel.

## RPC

Workflows can expose an RPC surface for other members of the Durababble cluster to invoke. RPCs can just return state, mutate it, change how the workflow execution will proceed, or all of the above.

RPCs are done by getting a workflow handle for your workflow, and then calling Ruby methods on the handle itself. A caller does not need a Ruby object in the same process as the worker; it needs the workflow id, the workflow class, and a store.

There are two kinds of RPCs you can expose: simple RPCs, and command RPCs.

Simple RPCs are run in parallel and are not expected to ever mutate state on the workflow. They are not recorded durably, and so they can be lost. Use simple RPCs for reads, for things you need to be really cheap, or for situations where the workflow is the "owner" of another resource under the hood that does not record durable state in the workflow itself.

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

handle = ReviewWorkflow.handle(run_id, store:)
handle.label
handle.note(message: "approved by legal")
```

Durababble handles the RPC machinery between your workers automatically, routing messages to the right worker that has a workflow active. If a workflow is not active when you send an RPC to it, Durababble will warm it up on a worker and then deliver your message.

## Replay

Replay is what lets a workflow continue after a crash without rerunning completed side effects. When Durababble resumes a workflow, it calls `#execute` again from the top, but completed step positions return their persisted results instead of invoking the Ruby method body. The workflow code must therefore be deterministic around step calls. Any branch on input, persisted step results, or durable wait payloads must happen the same way it did the first time. For this reason, Durababble patches sources of non-determinism for workflow code to ensure that randomness, wall clock time, and process local state is the same for each execution of the workflow.

```ruby
def execute(order)
  payment = charge_card(order)      # reused from storage if step 0 completed
  label = buy_shipping_label(order, payment) # reruns only if step 1 did not complete

  { "payment_id" => payment.fetch("id"), "label_id" => label.fetch("id") }
end
```

Replay is intentionally strict. If deployed code reaches a different completed step method at the same position, or returns before consuming completed history, the run fails with `Durababble::NonDeterminismError` instead of quietly attaching old side effects to new control flow.

## Workflow Event Log Length

Every durable boundary leaves history behind: workflow rows, step rows, attempts, waits, retries, cancellation metadata, fences, and outbox rows. That history is what makes replay honest, but it also means workflows should usually be finite processes rather than permanent entities. Otherwise, the recorded event history will grow to be very long, replay will take very long, and system performance will suffer.

Prefer durable objects for long-lived identities, and prefer splitting very large jobs into a workflow per bounded batch or phase.

```ruby
# Prefer this shape for ongoing per-shop state:
ShopSync.ref(shop_id, store:).record_cursor(cursor)

# Prefer this shape for bounded work:
SyncOneShopBatch.enqueue({ "shop_id" => shop_id, "cursor" => cursor }, store:)
```
