# Durababble

Durababble is durable executor for workflows and durable objects. It is for work that might run for a long time and must survive process exits, retries, deploys, and other changes in which process is actually running the code. It adds durability by storing state in your existing database.

The library gives you two primitives:

| Primitive         | Use it for                                                                       | Current API                                                                                                               |
| ----------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Durable workflows | One-off executions with durable steps, waits, retries, cancellation, and results | `Durababble::Workflow`, `Workflow.start`, `Workflow.handle`, `Durababble::Engine#run`, `Workflow.enqueue`, `Workflow.ref` |
| Durable objects   | Long-lived instances with durable state, like Cloudflare's Durable Objects       | `Durababble::DurableObject`, `DurableObject.ref`, `expose`, `expose_command`                                              |

Detailed guarantees live in [docs/spec.md](docs/spec.md) and [docs/architecture.md](docs/architecture.md).

## Why durable execution?

Applications often need orchestration that supports more sophisticated patterns than background jobs or non-durable actors, but deploying and running a whole durable workflow system is operationally undesirable. Instead, Durababble reuses your existing, robust backend storage for the durability part, and then orchestrates everything using familiar looking background worker processes.

In this middle ground:

- workflow code is ordinary Ruby with explicit durable `step` boundaries;
- every durable boundary is persisted before and after execution;
- workers claim work with SQL leases and fence stale ownership;
- completed steps replay from storage instead of rerunning side effects;
- waits, fences, outbox rows, and durable-object commands are database state, not in-memory coordination;
- deterministic and crash-recovery tests exercise the failure model directly.

Durababble also has one important feature: cheap RPCs between durable entities. This lets you easily query and command your durable entities to do useful stuff! This makes them spiritually similar to actors in actor frameworks, or genservers in BEAM/OTP, but with added durable goodness.

## Choosing A Primitive

Durable workflows and durable objects share the same durable store, but they fit different shapes of work:

| Choose            | When                                                                                                                                                                                                                    | Mental model                                                          |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| Durable workflows | The work is a finite process with a start, a result, ordered durable steps, waits, retries, or cancellation: indexing pipelines, tool sequences, resumable imports, fulfillment flows.                                  | A function or process that survives restarts and finishes eventually. |
| Durable objects   | The work centers on an id that should keep mutable state over time, potentially indefinitely, and accept repeated queries or commands: sessions, carts, conversations, agent contexts, project state, per-shop workers. | An addressed object with durable state.                               |

Use a workflow to orchestrate a process; use a durable object to own an entity's state. Compose them when a process needs durable per-entity state, but avoid turning a long-lived entity into one never-ending workflow just to make it addressable, or turning a finite process into ad hoc object state just to make retries durable.

## Durable Workflows

Use a durable workflow when you want to describe a process from beginning to end. The workflow's `#execute` method is the recipe, and each `step` method is a checkpoint where Durababble persists what happened when some side effect executed. If a worker dies after step two of five, another worker can replay the workflow, reuse the completed step results from the database, and continue at the next unfinished step instead of starting over.

Good workflow-shaped examples include charging then shipping an order, importing a large CSV across several API calls, running a complicated LLM agent turn with tool calls till completion, or an automated review process that sometimes has a human-in-the-loop approval step. The key sign is that the work has a start, an expected finish, and a sequence of durable side effects you do not want to accidentally repeat.

Because they are durable, durable workflows can sleep for many days, or await some event that might take a long time to occur. Durable workflows can be cancelled mid execution, and can return a result to callers.

<!-- README:workflow-example:start -->

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

<!-- README:workflow-example:end -->

### Parallel steps with Async

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

### Enqueuing and workflow handles

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

### Sleeping

A workflow can park itself without keeping a worker thread busy. There are two different shapes:

- `wait_until(time, context)` is a timer wait. Use it when the workflow should resume at or after a known time.
- `wait_event(event_key, context)` is an external event wait. Use it when the workflow should resume only after another process records a matching event with `store.signal_event`.

Under the hood, Durababble stores the wait, releases the worker lease, and resumes the workflow later after the timer or event is completed.

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

Event waits are useful when the workflow cannot know its resume time up front: human approval, webhook delivery, a batch import finishing elsewhere, or another durable entity signaling that some state changed.

```ruby
class AwaitOrderApproval < Durababble::Workflow
  def execute(order)
    approval = wait_for_approval(order)
    apply_approval(order, approval)
  end

  step def wait_for_approval(order)
    wait_event("approval:#{order.fetch("id")}", order)
  end

  step def apply_approval(order, approval)
    order.merge("approved" => approval.fetch("approved"))
  end
end

store.signal_event("approval:ord_123", payload: { "approved" => true })
```

The `context` you pass to `wait_until` or `wait_event` is the base value Durababble resumes with when the wait completes. For event waits, the signal payload is merged into that resumed value, which is how the approval example receives `"approved" => true`.

The current implementation records waits through step history, so the examples return waits from small step methods. That is an implementation limitation, not the ideal public shape; workflow waits should become workflow-level yield points. Do not use `Thread.sleep` in workflow code, because that actually blocks the worker thread instead of durably parking the workflow.

### Cancellation

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

### RPC

Workflows can expose an RPC surface for other members of the Durababble cluster to invoke. RPCs can just return state, or mutate it, or change how the workflow execution will proceed, or all of the above! 

RPCs are done by getting a workflow handle for your workflow, and then calling Ruby methods on the handle itself. A caller does not need a Ruby object in the same process as the worker; it needs the workflow id, the workflow class, and a store. 

There's two kinds of RPCs you can expose: simple RPCs, and command RPCs.

Simple RPCs are run in parallel and are not expected to ever mutate state on the workflow -- they aren't recorded durably, and so they can be lost. Use simple RPCs for reads, for things you need to be really cheap, or for situations where the workflow is the "owner" of another resource under the hood that doesn't record durable state in the workflow itself.

Commands can mutate state on the workflow, and are thusly processed in serial and recorded and redelivered durably. Use commands for RPCs that *need* to make it to the workflow, and that change the way the workflow will behave moving forward, like editing local state. Command calls are stored in the durable inbox for the workflow target, coalesced into a target activation, and completed by storing a result or error on the ask row; callers do not need to arrange a matching `wait_event` in workflow code.

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

### Replay

Replay is what lets a workflow continue after a crash without rerunning completed side effects. When Durababble resumes a workflow, it calls `#execute` again from the top, but completed step positions return their persisted results instead of invoking the Ruby method body. The workflow code __must__ therefore be deterministic around step calls! Any branchs on input, persisted step results, or durable wait payloads must happen the same way they did the first time. For this reason, Durababble patches sources of non-determinism for workflow code to ensure that randomness, wall clock time, and process local state is the same for each execution of the workflow.

```ruby
def execute(order)
  payment = charge_card(order)      # reused from storage if step 0 completed
  label = buy_shipping_label(order, payment) # reruns only if step 1 did not complete

  { "payment_id" => payment.fetch("id"), "label_id" => label.fetch("id") }
end
```

Replay is intentionally strict. If deployed code reaches a different completed step method at the same position, or returns before consuming completed history, the run fails with `Durababble::NonDeterminismError` instead of quietly attaching old side effects to new control flow.

### Workflow event log length

Every durable boundary leaves history behind: workflow rows, step rows, attempts, waits, retries, cancellation metadata, fences, and outbox rows. That history is what makes replay honest, but it also means workflows should usually be finite processes rather than permanent entities. Otherwise, the recorded event history will grow to be very long, and replay will take very long, and system performance will suffer.

Prefer durable objects for long-lived identities, and prefer splitting very large jobs into a workflow per bounded batch or phase.

```ruby
# Prefer this shape for ongoing per-shop state:
ShopSync.ref(shop_id, store:).record_cursor(cursor)

# Prefer this shape for bounded work:
SyncOneShopBatch.enqueue({ "shop_id" => shop_id, "cursor" => cursor }, store:)
```

## Durable Objects

Durable objects are for state with a durable identity. Think of one like a class instance that outlives the memory of any one process: you address it by id, ask it questions, and send it commands that update its persisted state. Where a workflow is usually "do this and eventually finish," a durable object is "this thing exists over time, and callers keep coming back to it."

Good object-shaped work includes an account in a bank, a cart, a chatroom, or a small state machine that other entities need to coordinate with. The key sign is that the id matters: all callers talking about `acct_123`, `cart_456`, or `channel-tmp-durable-execution-discussions` should see and update the same durable state.

Durababble then helps you RPC to these objects to read or write the state within them. Durababble's RPC layer correctly routes your messages to the worker where a durable object is currently live, and instantiates it or retrieves it from storage if it isn't already live. 

You can safely create many many thousands of object instances, and rely on Durababble's orchestration to move the instances in and out of durable storage as they send and recieve messages. A durable object doesn't have a fixed footprint resource requirement, as when it is inactive, it's just a row in the DB recording what state the entity with that ID is currently in.

Durable object methods are not workflow steps. Instead, the command is the durable boundary, and the object either applies your command or doesn't, and the state is durably persisted after. Object commands are inbox messages ordered by a per-object mailbox sequence, so a later command cannot overtake a pending, backoff, or dead-lettered head message for the same object.

<!-- README:durable-object-example:start -->

```ruby
class Account < Durababble::DurableObject
  object_type "account"

  def initialize_state
    { "balance_cents" => 0 }
  end

  expose_command retry: { maximum_attempts: 5 }
  def credit(amount_cents)
    update_state(
      "balance_cents" => current_state.fetch("balance_cents") + amount_cents,
    )
  end

  expose def balance
    current_state.fetch("balance_cents")
  end
end

store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!

account = Account.ref("acct_readme", store:)
account.credit(1_000)
account.balance
```

<!-- README:durable-object-example:end -->

### Object RPCs

Durable objects can expose methods to callers. You first _expose_ a method on the durable object, and then using a reference to the object, you can call that method from any other process in the system.

There's two kinds of methods that can be exposed: simple RPCs, and command RPCs. 

Simple RPCs are run in parallel and are not expected to ever mutate state on the object -- they aren't recorded durably, and so they can be lost. Use simple RPCs for reads, for things you need to be really cheap, or for situations where the object is the "owner" of another resource under the hood that doesn't record durable state in the durable object itself.

Commands can mutate state on the object, and are thusly processed in serial and recorded and redelivered durably. Use commands for RPCs that *need* to make it to the object, and that change the way the object will behave moving forward, like editing local state.


```ruby
account = Account.ref("acct_123", store:)
account.credit(1_000) # durable command: this call is written to the database and eventually processed even in the face of crashes
account.balance       # query: reads latest persisted state
```

Use `expose` for simple RPCs such as `balance`, `status`, `members`, or `current_cursor`. Use `expose_command` for changes such as `credit`, `join`, `append_message`, `advance_cursor`, or `close`. Command methods can use `command_context.idempotency_key` when calling external systems, and command retry policy is declared on the method the same way workflow step retry policy is.

```ruby
class Channel < Durababble::DurableObject
  object_type "channel"

  def initialize_state
    { "messages" => [] }
  end

  expose_command retry: { maximum_attempts: 3 }
  def append(message)
    update_state("messages" => current_state.fetch("messages") + [message])
  end

  expose def recent
    current_state.fetch("messages").last(10)
  end
end

channel = Channel.ref("durable-execution-discussions", store:)
channel.append({ "from" => "harry", "body" => "ship it" })
channel.recent
```

## Why Not Just Use Background Jobs?

Background jobs are still the right tool for simple, short, idempotent work: send one email, refresh one cache entry, enqueue one webhook delivery, or run a task where rerunning the whole job is acceptable.

Durababble is for the cases where a single retry loop becomes the hard part. If a job charges a card, writes a record, waits for a webhook, calls another service, and then ships an order, a crash in the middle forces you to rebuild durable progress tracking yourself. You end up adding status columns, idempotency keys, retry schedules, leases, recovery scans, cancellation flags, and custom "what step was I on?" logic.

Durababble makes those pieces part of the programming model. Workflows persist step history and resume from durable boundaries. Durable objects keep id-addressed state behind query and command methods. RPC-style handles let other code ask durable entities for status or send durable commands without reaching into worker memory. The goal is not to replace every job; it is to make the stateful, multi-step, long-lived jobs explicit and recoverable.
