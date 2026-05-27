---
title: "Durable Objects"
weight: 30
---

# Durable Objects

Durable objects are for state with a durable identity. Think of one like a class instance that outlives the memory of any one process: you address it by id, ask it questions, and send it commands that update its persisted state. Where a workflow is usually "do this and eventually finish," a durable object is "this thing exists over time, and callers keep coming back to it."

Good object-shaped work includes an account in a bank, a cart, a chatroom, or a small state machine that other entities need to coordinate with. The key sign is that the id matters: all callers talking about `acct_123`, `cart_456`, or `channel-tmp-durable-execution-discussions` should see and update the same durable state.

Durababble helps you RPC to these objects to read or write the state within them. Durababble's RPC layer routes messages to the worker where a durable object is currently live, and instantiates it or retrieves it from storage if it is not already live.

You can safely create many thousands of object instances, and rely on Durababble's orchestration to move the instances in and out of durable storage as they send and receive messages. A durable object does not have a fixed footprint resource requirement. When it is inactive, it is just a row in the database recording the state for the entity with that id.

Durable object methods are not workflow steps. Instead, the command is the durable boundary, and the object either applies your command or does not, and the state is durably persisted after. Object commands are inbox messages ordered by a per-object mailbox sequence, so a later command cannot overtake a pending, backoff, or dead-lettered head message for the same object.

<!-- DOCS:durable-object-example:start -->

<!-- DOCS:durable-object-example:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

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

account = Account.at("acct_readme")
```

<!-- DOCS:durable-object-example:hidden
```ruby
worker_database_url = respond_to?(:backend_descriptor) ? backend_descriptor.database_url : Durababble.default_database_url
worker_store = Durababble::Store.connect(database_url: worker_database_url, schema: store.schema)
worker = Durababble::Worker.new(
  store: worker_store,
  workflows: [],
  objects: [Account],
  worker_id: "account-worker-1",
  migrate: false,
)
worker_thread = Thread.new do
  loop do
    worker.run_until_idle
    sleep 0.01
  end
end
```
-->

```ruby
account.credit(1_000)

account.balance
```

<!-- DOCS:durable-object-example:hidden
```ruby
object_result = account.balance
worker_thread.kill
worker_thread.join
worker_store.close
object_result
```
-->

<!-- DOCS:durable-object-example:end -->

## Object RPCs

Durable objects can expose methods to callers. You first expose a method on the durable object, and then using a handle to the object, you can call that method from any other process in the system.

There are two kinds of methods that can be exposed: simple RPCs, and command RPCs.

Simple RPCs are run in parallel and are not expected to ever mutate state on the object. They are not recorded durably, and so they can be lost. Use simple RPCs for reads, for things you need to be really cheap, or for situations where the object is the "owner" of another resource under the hood that does not record durable state in the durable object itself.

Commands can mutate state on the object, and are processed in serial and recorded and redelivered durably. Use commands for RPCs that need to make it to the object, and that change the way the object will behave moving forward, like editing local state.

```ruby
account = Account.at("acct_123")
account.credit(1_000) # durable command: this call is written to the database and eventually processed by an object worker
account.balance       # query: reads latest persisted state
```

Workflow code can call the same object handles directly from `#execute`. Outside an explicit workflow step, Durababble records the object handle call as workflow history before dispatching the object query, ask, or tell, so replay returns the recorded result or tell message id without duplicating the outbound object inbox row.

Use `expose` for simple RPCs such as `balance`, `status`, `members`, or `current_cursor`. Use `expose_command` for changes such as `credit`, `join`, `append_message`, `advance_cursor`, or `close`. Command methods can use `command_context.idempotency_key` when calling external systems, and command retry policy is declared on the method the same way workflow step retry policy is.

`Account.at("acct_123", worker_pool: "orders")` sends command wakeups to that worker pool, and `idempotency_key:` on `at` or `handle` becomes the default idempotency key for commands sent through that handle; passing `idempotency_key:` to an individual command overrides the handle default.

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

channel = Channel.at("durable-execution-discussions")
channel.append({ "from" => "harry", "body" => "ship it" })
channel.recent
```

## Starting Workflows From Objects

Durable object commands can start workflows with the workflow class API: `ImportFile.enqueue(...)` or `ImportFile.start(...)`. This is command-only because starting a workflow mutates durable state; exposed queries and code outside an object command cannot use it.

```ruby
class ImportCoordinator < Durababble::DurableObject
  object_type "import_coordinator"

  def initialize_state
    { "child_id" => nil }
  end

  expose_command def begin_import(file_id)
    child = ImportFile.enqueue({ "file_id" => file_id }, cancellation: :abandon)
    schedule_wake(name: "check-import", at: Time.now + 60, payload: { "child_id" => child.workflow_id })
    update_state(current_state.merge("child_id" => child.workflow_id))
    child.workflow_id
  end

  def on_wake(name:, payload:)
    child = ImportFile.handle(payload.fetch("child_id"))
    update_state(current_state.merge("status" => child.status, "result" => child.result)) if child.status == "completed"
  end
end
```

The object command records the workflow as object-origin work using the durable mailbox command id, so retrying the same object command reattaches to the same workflow instead of starting a duplicate. Object commands should not synchronously wait for children; store the child id in object state, schedule a wake, receive a later command/signal, or read the child handle after the command commits.

## Alarms

Object commands can schedule persisted, named wakeups for the object with `schedule_wake(name:, at:, payload: nil)`. Each `name` addresses an independent wake, so one object can hold several outstanding wakes at once — a TTL sweep, a retry, and a daily flush can all be pending against the same id. Calling `schedule_wake` again with the same `name` before it fires replaces that wake's time and payload while leaving the other names untouched. `cancel_wake(name:)` removes a single named wake, and `cancel_all_wakes` removes every pending wake for the object. Scheduling and cancellation are committed with the command, so a failed or retried command does not leave behind an unrelated process-local timer.

Wake names are required, non-empty strings of at most 191 bytes.

When a wake time matures, `store.wake_due_timers(now:)` converts each due wake into the object's durable mailbox as a `wake` message in the same worker pool, carrying the wake's name. The ordinary object worker path later claims that message and invokes `on_wake(name:, payload:)`, where `name` is the wake that fired so the handler can dispatch on it.

Because the wake is delivered as an ordinary inbox message, `on_wake` inherits at-least-once delivery: a worker that crashes after running `on_wake` but before committing the message can run it again. Keep `on_wake` handlers idempotent (key the side effect off the name/payload, or guard against duplicates in state) rather than assuming exactly-once delivery.

```ruby
class Reminder < Durababble::DurableObject
  object_type "reminder"

  def initialize_state
    { "delivered" => [] }
  end

  # Several reminders can be outstanding at once, each addressed by name.
  expose_command def remind_at(name, time, label)
    schedule_wake(name:, at: time, payload: { "label" => label })
  end

  expose_command def clear_reminder(name)
    cancel_wake(name:)
  end

  expose_command def clear_all_reminders
    cancel_all_wakes
  end

  def on_wake(name:, payload:)
    # idempotent: redelivery of the same wake must not double-record the label
    update_state("delivered" => current_state.fetch("delivered") | [payload.fetch("label")])
  end
end
```

## Choosing Objects Or Workflows

Durable workflows and durable objects share the same durable store, but they fit different shapes of work. Use a workflow when the work is a finite process with a start, a result, ordered durable steps, waits, retries, or cancellation: indexing pipelines, tool sequences, resumable imports, or fulfillment flows. Use a durable object when the work centers on an id that should keep mutable state over time, potentially indefinitely, and accept repeated queries or commands: sessions, carts, conversations, agent contexts, project state, or per-shop workers.

Use a workflow to orchestrate a process; use a durable object to own an entity's state. Compose them when a process needs durable per-entity state, but avoid turning a long-lived entity into one never-ending workflow just to make it addressable, or turning a finite process into ad hoc object state just to make retries durable. See [Object Patterns](object-patterns.md) for executable examples of common object shapes.
