---
title: "Quickstart"
weight: 10
---

# Quickstart

A tour of Durababble's core features in a handful of small snippets. See [Install Instructions](install.md) for the gem and database setup, then follow along. Every snippet assumes:

```ruby
require "durababble"

Durababble.configure(database_url: Durababble.default_database_url)
store = Durababble.store
store.migrate!
```

## A Workflow With Retries

Durable workflows are ordinary Ruby classes. Each `step def` is a checkpoint: its result is persisted, so a crash mid-workflow does not re-run completed side effects. Retry policy is declared inline.

```ruby
class FulfillOrder < Durababble::Workflow
  def execute(order)
    payment = charge_card(order)
    label = buy_shipping_label(order, payment)
    { "payment_id" => payment.fetch("id"), "label_id" => label.fetch("id") }
  end

  step retry: { maximum_attempts: 5, schedule: [1, 5, 30] }
  def charge_card(order)
    Payments.charge(order.fetch("card_token"), amount: order.fetch("total_cents"))
  end

  step def buy_shipping_label(order, payment)
    Shipping.buy_label(order.fetch("address"), payment_id: payment.fetch("id"))
  end
end

handle = FulfillOrder.start(order)
Durababble::Worker.new(store:, workflows: [FulfillOrder], worker_id: "orders-1", migrate: false).run_until_idle
handle.result
```

## Enqueue Now, Run Later On A Worker

In a real application, web requests enqueue and long-running workers claim work under SQL leases. The handle is portable across processes — anything with the workflow id and a store can query or cancel.

```ruby
handle = FulfillOrder.start(order)
handle.workflow_id
handle.cancel(reason: "customer requested cancellation")

Durababble::Worker.new(store:, workflows: [FulfillOrder], worker_id: "orders-1", migrate: false).run_until_idle
```

## Sleeping

Workflows can park themselves without holding a worker thread. `wait_until` is a durable timer: the engine persists the wait, releases the worker lease, and resumes the workflow at or after the given time.

```ruby
class SendReminderAfterDelay < Durababble::Workflow
  def execute(reminder)
    delayed = sleep_until_reminder_time(reminder)
    send_reminder(delayed)
  end

  step def sleep_until_reminder_time(reminder)
    wait_until(reminder.fetch("send_at"), reminder)
  end

  step def send_reminder(reminder) = Reminders.send(reminder.fetch("user_id"), reminder.fetch("message"))
end
```

To resume a workflow on an external signal rather than a clock (human approval, webhook delivery, a batch finishing elsewhere), send it a workflow command from the signaling process — see Workflow RPC below.

## Parallel Steps With Async

Workflow orchestration plays nicely with the `async` gem. Branches run concurrently and Durababble records each scheduled step, completion, and failure in history, so scatter/gather is replay-safe.

```ruby
class FetchProfiles < Durababble::Workflow
  def execute(user_ids)
    Async do |task|
      user_ids.map { |id| task.async { score_profile(fetch_profile(id)) } }.map(&:wait)
    end.wait
  end

  step def fetch_profile(user_id) = Profiles.fetch(user_id)
  step def score_profile(profile) = ProfileScoring.score(profile)
end
```

## Durable Objects: State With An Identity

Where a workflow finishes, a durable object persists. Address it by id, send commands that update durable state, and read with simple RPCs. Durababble routes calls to whichever worker currently has it live, and hydrates it from the database if it does not.

```ruby
class Account < Durababble::DurableObject
  object_type "account"

  def initialize_state = { "balance_cents" => 0 }

  expose_command retry: { maximum_attempts: 5 }
  def credit(amount_cents)
    update_state("balance_cents" => current_state.fetch("balance_cents") + amount_cents)
  end

  expose def balance = current_state.fetch("balance_cents")
end

account = Account.at("acct_123")
account.credit(1_000)   # durable command: written to the database, processed exactly once
account.balance         # simple RPC: reads the latest persisted state
```

## Workflow RPC

Workflows can expose methods too. `expose` is a parallel-safe read; `expose_command` is a serialized, durable mutation.

```ruby
class ReviewWorkflow < Durababble::Workflow
  expose def label = "reviewable"
  expose_command def note(message:) = message
end

handle = ReviewWorkflow.handle(run_id)
handle.label
handle.note(message: "approved by legal")
```

## Next Steps

- [Workflows](workflows.md) for cancellation, replay, and the full step model.
- [Durable Objects](durable-objects.md) for object commands, mailboxes, and RPC.
- [Storage](storage.md) for what gets persisted and why.
- [Observability](observability.md) for OpenTelemetry spans and metrics.
- [Testing](testing.md) for deterministic simulation tests.
