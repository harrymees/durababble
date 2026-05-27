---
title: "Workflow Patterns"
weight: 25
---

# Workflow Patterns

Reference patterns for common workflow shapes. Each example is a complete, runnable Durababble script — the same machinery the test suite exercises against the live public API. Copy a section verbatim, swap in your own side effects, and you have the skeleton.

The patterns range from "do these things one after another" to "fan a queue of work out across many parallel branches with bounded concurrency." Pick the one that matches the shape of the work, not the one that looks most clever — Durababble's durability guarantees do not depend on how parallel the workflow is.

## Sequential Pipeline

The simplest shape: a finite list of steps that run one after another, each one feeding the next. Use this when the work is a linear recipe and replay should resume at the first unfinished step.

<!-- DOCS:patterns-sequential:start -->

<!-- DOCS:patterns-sequential:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class GenerateReport < Durababble::Workflow
  def execute(request)
    rows = fetch_rows(request)
    upload_report(rows, request)
  end

  step def fetch_rows(request)
    ReportSource.query(request.fetch("query"))
  end

  step def upload_report(rows, request)
    ReportSink.upload(
      name: request.fetch("name"),
      rows:,
      idempotency_key: step_context.idempotency_key,
    )
  end
end

module ReportSource
  def self.query(_sql)
    [{ "id" => 1, "value" => "a" }, { "id" => 2, "value" => "b" }]
  end
end

module ReportSink
  def self.upload(name:, rows:, idempotency_key:)
    { "report_id" => "rep_#{name}", "row_count" => rows.length, "key" => idempotency_key }
  end
end

report = GenerateReport.start({ "name" => "weekly", "query" => "SELECT 1" })
```

<!-- DOCS:patterns-sequential:hidden
```ruby
worker = Durababble::Worker.new(
  store:,
  workflows: [GenerateReport],
  worker_id: "report-worker",
  migrate: false,
)
worker.run_until_idle
```
-->

```ruby
report.result
```

<!-- DOCS:patterns-sequential:hidden
```ruby
{ "status" => report.status, "row_count" => report.result.fetch("row_count") }
```
-->

<!-- DOCS:patterns-sequential:end -->

If `upload_report` crashes after `fetch_rows` completed, a replaying worker reuses the persisted rows from history and resumes at `upload_report` rather than re-running the query.

## Parallel Fanout

When the workflow needs to run the same step against many inputs and the inputs do not depend on each other, fan them out with `Async`. Each branch is its own durable step, scheduled before any of them complete. Replay records the schedule order in history, so out-of-order completions stay consistent across crashes.

<!-- DOCS:patterns-fanout:start -->

<!-- DOCS:patterns-fanout:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class IndexShopProducts < Durababble::Workflow
  def execute(product_ids)
    Async do |task|
      product_ids.map do |id|
        task.async { index_one(id) }
      end.map(&:wait)
    end.wait
  end

  step def index_one(product_id)
    SearchIndex.put(product_id, idempotency_key: step_context.idempotency_key)
  end
end

module SearchIndex
  def self.put(product_id, idempotency_key:)
    { "product_id" => product_id, "key" => idempotency_key }
  end
end

indexing = IndexShopProducts.start([101, 102, 103])
```

<!-- DOCS:patterns-fanout:hidden
```ruby
worker = Durababble::Worker.new(
  store:,
  workflows: [IndexShopProducts],
  worker_id: "indexer-worker",
  migrate: false,
)
worker.run_until_idle
```
-->

```ruby
indexing.result
```

<!-- DOCS:patterns-fanout:hidden
```ruby
indexing.result.map { |row| row.fetch("product_id") }.sort
```
-->

<!-- DOCS:patterns-fanout:end -->

Use fanout when the work is "do this to each item" and you do not need to gate or rate-limit the parallelism. Every branch is scheduled durably, so a worker crash partway through still resumes with the completed branches reused.

## Bounded Concurrency

Pure fanout dispatches every item at once, which is the wrong shape when each item costs a paid API call, a slow third-party request, or a database connection from a limited pool. Process the queue in fixed-size chunks instead: each chunk fans out in parallel, the next chunk does not start until the previous one's branches all complete.

<!-- DOCS:patterns-bounded-concurrency:start -->

<!-- DOCS:patterns-bounded-concurrency:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class RefreshTokens < Durababble::Workflow
  CONCURRENCY = 2

  def execute(user_ids)
    user_ids.each_slice(CONCURRENCY).flat_map do |chunk|
      Async do |task|
        chunk.map { |user_id| task.async { refresh_one(user_id) } }.map(&:wait)
      end.wait
    end
  end

  step def refresh_one(user_id)
    TokenService.rotate(user_id, idempotency_key: step_context.idempotency_key)
  end
end

module TokenService
  def self.rotate(user_id, idempotency_key:)
    { "user_id" => user_id, "token" => "tok_#{user_id}", "key" => idempotency_key }
  end
end

rotation = RefreshTokens.start([1, 2, 3, 4, 5])
```

<!-- DOCS:patterns-bounded-concurrency:hidden
```ruby
worker = Durababble::Worker.new(
  store:,
  workflows: [RefreshTokens],
  worker_id: "token-worker",
  migrate: false,
)
worker.run_until_idle
```
-->

```ruby
rotation.result
```

<!-- DOCS:patterns-bounded-concurrency:hidden
```ruby
rotation.result.map { |row| row.fetch("user_id") }
```
-->

<!-- DOCS:patterns-bounded-concurrency:end -->

Chunking by `each_slice` gives a hard ceiling on in-flight steps without any extra primitives, and replay is straightforward because each chunk is just another scheduled-then-awaited batch in step history. Tune `CONCURRENCY` to match the upstream limit (token bucket, connection pool size, vendor rate cap). For unbounded inputs that arrive over time, prefer a durable object with a per-id mailbox instead of one workflow that grows forever.

## Saga With Compensations

When a workflow performs side effects that have to roll back on failure — a charge that has to be refunded, a reservation that has to be released — encode the cleanup as ordinary durable steps in a `rescue` block. Compensation steps are recorded in history exactly like forward steps, so a crash during cleanup does not re-run completed cleanup.

<!-- DOCS:patterns-saga:start -->

<!-- DOCS:patterns-saga:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class TicketPrinterUnavailable < StandardError; end

class BookTrip < Durababble::Workflow
  def execute(booking)
    seat = reserve_seat(booking)
    charged = charge_card(seat)
    issue_ticket(charged)
  rescue TicketPrinterUnavailable
    refund_card(booking)
    release_seat(booking)
    raise
  end

  step def reserve_seat(booking)
    Bookings.reserve(booking.fetch("trip_id"), idempotency_key: step_context.idempotency_key)
  end

  step def charge_card(seat)
    Payments.charge(seat.fetch("trip_id"), idempotency_key: step_context.idempotency_key).merge(seat)
  end

  step def issue_ticket(_charged)
    raise TicketPrinterUnavailable, "ticket printer offline"
  end

  step def refund_card(booking)
    Payments.refund(booking.fetch("trip_id"))
  end

  step def release_seat(booking)
    Bookings.release(booking.fetch("trip_id"))
  end
end

module Bookings
  def self.reserve(trip_id, idempotency_key:)
    { "trip_id" => trip_id, "seat" => "1A", "reserve_key" => idempotency_key }
  end

  def self.release(trip_id)
    { "released" => trip_id }
  end
end

module Payments
  def self.charge(trip_id, idempotency_key:)
    { "charge_id" => "ch_#{trip_id}", "charge_key" => idempotency_key }
  end

  def self.refund(trip_id)
    { "refunded" => trip_id }
  end
end

booking = BookTrip.start({ "trip_id" => "trip-1" })
```

<!-- DOCS:patterns-saga:hidden
```ruby
worker = Durababble::Worker.new(
  store:,
  workflows: [BookTrip],
  worker_id: "booking-worker",
  migrate: false,
)
worker.run_until_idle
```
-->

```ruby
booking.status # => "failed" — the saga re-raised after compensating
booking.error  # => "TicketPrinterUnavailable: ticket printer offline"
```

<!-- DOCS:patterns-saga:hidden
```ruby
{
  "status" => booking.status,
  "steps" => store.steps_for(booking.workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] },
}
```
-->

<!-- DOCS:patterns-saga:end -->

The `rescue` runs the compensating steps and then re-raises so the workflow ends in `failed`. Callers and observers see a terminal failure, but the durable record shows that `refund_card` and `release_seat` ran. If the worker crashed between the refund and the release, replay would skip the already-refunded charge and only retry the release.

The pattern generalises: catch the specific failure you want to compensate for (here, `TicketPrinterUnavailable`), run compensating steps in the reverse order of the forward steps they undo, and re-raise.

## Cancellation Cleanup With Ensure

When a workflow creates temporary external state that must be cleaned up no matter how the run exits, wrap the owned region in Ruby's `ensure`. Cancellation is delivered as `Durababble::CancellationError` at a durable boundary, so the `ensure` block runs while the workflow unwinds. Keep the cleanup itself in durable steps, because cleanup may crash or retry just like the forward work.

<!-- DOCS:patterns-cancellation-ensure:start -->

<!-- DOCS:patterns-cancellation-ensure:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class ExportProject < Durababble::Workflow
  def execute(export)
    workspace = nil
    begin
      workspace = create_workspace(export)
      copy_rows(workspace)
      publish_export(workspace)
    ensure
      delete_workspace(workspace) if workspace
    end
  end

  step def create_workspace(export)
    ExportWorkspace.create(export.fetch("project_id"), idempotency_key: step_context.idempotency_key)
  end

  step def copy_rows(workspace)
    ExportWorkspace.copy(workspace, idempotency_key: step_context.idempotency_key)
  end

  step def publish_export(workspace)
    ExportWorkspace.publish(workspace, idempotency_key: step_context.idempotency_key)
  end

  step def delete_workspace(workspace)
    ExportWorkspace.delete(workspace.fetch("workspace_id"))
  end
end
```

<!-- DOCS:patterns-cancellation-ensure:hidden
```ruby
module ExportWorkspace
  @deleted = []
  @cancel_target = nil

  class << self
    attr_reader :deleted

    def cancel_during_copy(workflow_class:, workflow_id:, store:, reason:)
      @cancel_target = { workflow_class:, workflow_id:, store:, reason: }
    end

    def create(project_id, idempotency_key:)
      { "project_id" => project_id, "workspace_id" => "tmp_#{project_id}", "create_key" => idempotency_key }
    end

    def copy(workspace, idempotency_key:)
      if @cancel_target
        target = @cancel_target
        @cancel_target = nil
        target.fetch(:workflow_class).handle(target.fetch(:workflow_id), store: target.fetch(:store)).cancel(reason: target.fetch(:reason))
      end

      workspace.merge("copy_key" => idempotency_key)
    end

    def publish(workspace, idempotency_key:)
      workspace.merge("published" => true, "publish_key" => idempotency_key)
    end

    def delete(workspace_id)
      @deleted << workspace_id
      { "deleted" => workspace_id }
    end
  end
end
```
-->

```ruby
export = ExportProject.start({ "project_id" => "proj_123" })
```

<!-- DOCS:patterns-cancellation-ensure:hidden
```ruby
ExportWorkspace.cancel_during_copy(
  workflow_class: ExportProject,
  workflow_id: export.workflow_id,
  store:,
  reason: "project archived",
)

worker = Durababble::Worker.new(
  store:,
  workflows: [ExportProject],
  worker_id: "export-worker",
  migrate: false,
)
worker.run_until_idle
```
-->

```ruby
export.status # => "canceled"
```

<!-- DOCS:patterns-cancellation-ensure:hidden
```ruby
{
  "status" => export.status,
  "result" => export.result,
  "deleted" => ExportWorkspace.deleted,
  "steps" => store.steps_for(export.workflow_id).select { |step|
    ["create_workspace", "copy_rows", "delete_workspace"].include?(step.fetch("name"))
  }.map { |step| [step.fetch("name"), step.fetch("status")] },
}
```
-->

<!-- DOCS:patterns-cancellation-ensure:end -->

Here `copy_rows` represents work that was allowed to finish before the cancellation request was observed. Durababble raises cancellation before the workflow can schedule `publish_export`, then Ruby runs `ensure` and `delete_workspace` is recorded as a completed durable cleanup step. If the worker crashes after deleting the workspace but before the workflow reaches terminal `canceled`, replay skips the completed delete step instead of deleting the workspace twice.

## Other Patterns

A few common shapes do not need their own executable example because they compose the patterns above with the other workflow primitives. The relevant references in the workflow docs:

- **Long timer / scheduled followup** — use `wait_until(time, context)` to park the workflow until a wall-clock moment. See [Sleeping]({{< ref "workflows#sleeping" >}}).
- **Polling external state** — use `wait_condition { ... }` to suspend until a block returns true, with an optional timeout. The block runs in workflow context on each wake, so call a durable `step` inside it if the check itself has side effects worth recording.
- **External signal / human-in-the-loop approval** — expose a command with `expose_command` and have the workflow wait on a durable object or `wait_condition` watching the durable side-effect of that command. See [RPC]({{< ref "workflows#rpc" >}}).
- **Long-lived per-identity state** — do not stretch a workflow to live forever. Use a [durable object]({{< ref "durable-objects" >}}) keyed by the identity (shop id, cart id, channel id) and enqueue bounded workflows from it when there is a finite job to run.
- **Cancellation-aware cleanup** — use `ensure` for resources that must be released on success, failure, or cancellation. Use an explicit `rescue Durababble::CancellationError` instead when cleanup needs the cancellation reason or should return a cleanup result.
