---
title: "Object Patterns"
weight: 35
---

# Object Patterns

Reference patterns for common durable object shapes. Each example is executable against Durababble's local public API; hidden setup starts a worker or delivers wake messages where the visible snippet would normally be driven by application infrastructure.

Durable objects work best when the id is a natural ownership boundary: one account, cart, chat room, tenant, document, stream, cache key, or rate-limit bucket. The examples here are adapted from common actor and Cloudflare Durable Objects patterns, including counters, RPC/session state, alarms, WebSocket-style rooms, KV coordination, and streams; the local Durababble version uses object commands, queries, and wake messages instead of Cloudflare-specific worker APIs.

## Counter

Use a counter when every caller for the same id should see the same accumulated value and updates must not race each other.

<!-- DOCS:object-pattern-counter:start -->

<!-- DOCS:object-pattern-counter:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class Counter < Durababble::DurableObject
  object_type "pattern_counter"

  def initialize_state
    { "value" => 0 }
  end

  expose_command def increment(amount = 1)
    update_state("value" => current_state.fetch("value") + amount)
  end

  expose_command def decrement(amount = 1)
    update_state("value" => current_state.fetch("value") - amount)
  end

  expose def value
    current_state.fetch("value")
  end
end

Counter.tell("global", :increment, 3)
Counter.tell("global", :decrement, 1)
```

<!-- DOCS:object-pattern-counter:hidden
```ruby
worker = Durababble::Worker.new(store:, workflows: [], objects: [Counter], worker_id: "counter-worker", migrate: false)
worker.run_until_idle
```
-->

```ruby
Counter.at("global").value
```

<!-- DOCS:object-pattern-counter:end -->

## Session Registry

A connection or session registry object keeps per-session metadata behind one id. This mirrors the shape of WebSocket edge workers that need to remember which sessions are currently attached to a room, shard, or gateway.

<!-- DOCS:object-pattern-session-registry:start -->

<!-- DOCS:object-pattern-session-registry:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class SessionRegistry < Durababble::DurableObject
  object_type "pattern_session_registry"

  def initialize_state
    { "sessions" => {} }
  end

  expose_command def connect(session_id, metadata)
    update_state(
      "sessions" => current_state.fetch("sessions").merge(
        session_id => metadata.merge("operation_id" => command_context.idempotency_key),
      ),
    )
  end

  expose def metadata_for(session_id)
    current_state.fetch("sessions").fetch(session_id)
  end
end

SessionRegistry.tell(
  "socket-worker",
  :connect,
  "session-1",
  { "country" => "US", "plan" => "pro" },
)
```

<!-- DOCS:object-pattern-session-registry:hidden
```ruby
worker = Durababble::Worker.new(store:, workflows: [], objects: [SessionRegistry], worker_id: "session-worker", migrate: false)
worker.run_until_idle
```
-->

```ruby
metadata = SessionRegistry.at("socket-worker").metadata_for("session-1")
{
  "country" => metadata.fetch("country"),
  "plan" => metadata.fetch("plan"),
  "operation_id_recorded" => metadata.key?("operation_id"),
}
```

<!-- DOCS:object-pattern-session-registry:end -->

## Alarm Batcher

A batcher object collects writes behind one id and flushes them later when a wake message arrives. In production the wake is usually an alarm, timer, or scheduler tick; the important part is that the flush sees the object's persisted state.

<!-- DOCS:object-pattern-batcher:start -->

<!-- DOCS:object-pattern-batcher:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class Batcher < Durababble::DurableObject
  object_type "pattern_batcher"

  def initialize_state
    { "messages" => [], "flushes" => [] }
  end

  expose_command def add(message)
    update_state(current_state.merge("messages" => current_state.fetch("messages") + [message]))
  end

  def on_wake(payload:)
    update_state(
      "messages" => [],
      "flushes" => current_state.fetch("flushes") + [
        { "messages" => current_state.fetch("messages"), "reason" => payload.fetch("reason") },
      ],
    )
  end

  expose def snapshot
    current_state
  end
end

Batcher.tell("email-digest", :add, "first")
Batcher.tell("email-digest", :add, "second")
```

<!-- DOCS:object-pattern-batcher:hidden
```ruby
worker = Durababble::Worker.new(store:, workflows: [], objects: [Batcher], worker_id: "batcher-worker", migrate: false)
worker.run_until_idle
store.enqueue_inbox_message(
  target_kind: "object",
  target_type: Batcher.object_type,
  target_id: "email-digest",
  message_kind: "wake",
  payload: { "reason" => "alarm" },
)
worker.run_until_idle
```
-->

```ruby
Batcher.at("email-digest").snapshot
```

<!-- DOCS:object-pattern-batcher:end -->

## TTL Cache Entry

A TTL cache entry object owns one key and clears itself when a wake message says the expiration time has passed.

<!-- DOCS:object-pattern-ttl-cache:start -->

<!-- DOCS:object-pattern-ttl-cache:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class CacheEntry < Durababble::DurableObject
  object_type "pattern_cache_entry"

  def initialize_state
    { "value" => nil, "expires_at" => nil, "expired" => false }
  end

  expose_command def put(value, expires_at:)
    update_state("value" => value, "expires_at" => expires_at, "expired" => false)
  end

  def on_wake(payload:)
    expires_at = current_state.fetch("expires_at")
    return current_state unless expires_at && payload.fetch("now") >= expires_at

    update_state("value" => nil, "expires_at" => nil, "expired" => true)
  end

  expose def snapshot
    current_state
  end
end

CacheEntry.tell("feature-flags", :put, "cached", expires_at: 100)
```

<!-- DOCS:object-pattern-ttl-cache:hidden
```ruby
worker = Durababble::Worker.new(store:, workflows: [], objects: [CacheEntry], worker_id: "cache-worker", migrate: false)
worker.run_until_idle
store.enqueue_inbox_message(
  target_kind: "object",
  target_type: CacheEntry.object_type,
  target_id: "feature-flags",
  message_kind: "wake",
  payload: { "now" => 101 },
)
worker.run_until_idle
```
-->

```ruby
CacheEntry.at("feature-flags").snapshot
```

<!-- DOCS:object-pattern-ttl-cache:end -->

## KV Coordinator

A coordinator object can serialize writes for a logical namespace, then expose cheap reads of the latest state. This is useful when external storage is eventually consistent or when you need one place to assign versions.

<!-- DOCS:object-pattern-kv-coordinator:start -->

<!-- DOCS:object-pattern-kv-coordinator:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class KvCoordinator < Durababble::DurableObject
  object_type "pattern_kv_coordinator"

  def initialize_state
    { "kv" => {}, "versions" => {} }
  end

  expose_command def put(key, value)
    update_state(
      "kv" => current_state.fetch("kv").merge(key => value),
      "versions" => current_state.fetch("versions").merge(key => command_context.idempotency_key),
    )
  end

  expose def get(key)
    current_state.fetch("kv").fetch(key)
  end

  expose def version_recorded?(key)
    current_state.fetch("versions").key?(key)
  end
end

KvCoordinator.tell("namespace", :put, "feature:enabled", true)
```

<!-- DOCS:object-pattern-kv-coordinator:hidden
```ruby
worker = Durababble::Worker.new(store:, workflows: [], objects: [KvCoordinator], worker_id: "kv-worker", migrate: false)
worker.run_until_idle
```
-->

```ruby
{
  "enabled" => KvCoordinator.at("namespace").get("feature:enabled"),
  "version_recorded" => KvCoordinator.at("namespace").version_recorded?("feature:enabled"),
}
```

<!-- DOCS:object-pattern-kv-coordinator:end -->

## Room

A room object serializes membership changes and broadcasts through one id while still exposing read methods for the current members and transcript.

<!-- DOCS:object-pattern-room:start -->

<!-- DOCS:object-pattern-room:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class Room < Durababble::DurableObject
  object_type "pattern_room"

  def initialize_state
    { "members" => {}, "messages" => [] }
  end

  expose_command def join(session_id, metadata)
    update_state(current_state.merge("members" => current_state.fetch("members").merge(session_id => metadata)))
  end

  expose_command def broadcast(body, from:)
    message = { "from" => from, "body" => body, "member_count" => current_state.fetch("members").length }
    update_state(current_state.merge("messages" => current_state.fetch("messages") + [message]))
  end

  expose def members
    current_state.fetch("members")
  end

  expose def transcript
    current_state.fetch("messages")
  end
end

Room.tell("lobby", :join, "session-a", { "name" => "Ada" })
Room.tell("lobby", :join, "session-b", { "name" => "Grace" })
Room.tell("lobby", :broadcast, "hello", from: "session-a")
```

<!-- DOCS:object-pattern-room:hidden
```ruby
worker = Durababble::Worker.new(store:, workflows: [], objects: [Room], worker_id: "room-worker", migrate: false)
worker.run_until_idle
```
-->

```ruby
room = Room.at("lobby")
{
  "members" => room.members.keys,
  "transcript" => room.transcript,
}
```

<!-- DOCS:object-pattern-room:end -->

## Stream Cursor

A stream object can keep an ordered list of chunks and a durable read cursor. This is not a replacement for a large blob store, but it is a compact shape for resumable, id-owned feeds.

<!-- DOCS:object-pattern-stream:start -->

<!-- DOCS:object-pattern-stream:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class ChunkStream < Durababble::DurableObject
  object_type "pattern_chunk_stream"

  def initialize_state
    { "chunks" => [], "cursor" => 0, "last_read" => [] }
  end

  expose_command def append_chunks(chunks)
    update_state(current_state.merge("chunks" => current_state.fetch("chunks") + chunks))
  end

  expose_command def read_next(limit)
    cursor = current_state.fetch("cursor")
    next_cursor = [cursor + limit, current_state.fetch("chunks").length].min
    update_state(current_state.merge("cursor" => next_cursor, "last_read" => current_state.fetch("chunks")[cursor...next_cursor]))
  end

  expose def snapshot
    current_state
  end
end

ChunkStream.tell("feed", :append_chunks, ["a", "b", "c"])
ChunkStream.tell("feed", :read_next, 2)
```

<!-- DOCS:object-pattern-stream:hidden
```ruby
worker = Durababble::Worker.new(store:, workflows: [], objects: [ChunkStream], worker_id: "stream-worker", migrate: false)
worker.run_until_idle
```
-->

```ruby
ChunkStream.at("feed").snapshot
```

<!-- DOCS:object-pattern-stream:end -->

## Per-Tenant Rate Window

A rate window object works when the key is a tenant, user, IP, or route bucket. Avoid one global rate-limit object unless the traffic volume is tiny; a durable object serializes work for its own id.

<!-- DOCS:object-pattern-rate-window:start -->

<!-- DOCS:object-pattern-rate-window:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class RateWindow < Durababble::DurableObject
  object_type "pattern_rate_window"

  def initialize_state
    { "window_start" => nil, "count" => 0, "decisions" => [] }
  end

  expose_command def record(now:, limit:, window_seconds:)
    window_start = current_state.fetch("window_start")
    count = current_state.fetch("count")
    if window_start.nil? || now >= window_start + window_seconds
      window_start = now
      count = 0
    end

    allowed = count < limit
    count += 1 if allowed
    update_state("window_start" => window_start, "count" => count, "decisions" => current_state.fetch("decisions") + [allowed])
  end

  expose def decisions
    current_state.fetch("decisions")
  end
end

RateWindow.tell("tenant-42", :record, now: 0, limit: 2, window_seconds: 60)
RateWindow.tell("tenant-42", :record, now: 1, limit: 2, window_seconds: 60)
RateWindow.tell("tenant-42", :record, now: 2, limit: 2, window_seconds: 60)
RateWindow.tell("tenant-42", :record, now: 61, limit: 2, window_seconds: 60)
```

<!-- DOCS:object-pattern-rate-window:hidden
```ruby
worker = Durababble::Worker.new(store:, workflows: [], objects: [RateWindow], worker_id: "rate-worker", migrate: false)
worker.run_until_idle
```
-->

```ruby
RateWindow.at("tenant-42").decisions
```

<!-- DOCS:object-pattern-rate-window:end -->

## Collaborative Document

A document object can serialize edits and reject commands that are based on the wrong revision. The example keeps the document body small for readability; production systems would usually store large document content elsewhere and keep object state to metadata, cursors, or recent operations.

<!-- DOCS:object-pattern-document:start -->

<!-- DOCS:object-pattern-document:hidden
```ruby
store ||= Durababble::Store.connect(database_url: Durababble.default_database_url)
store.migrate!
Durababble.default_store = store
```
-->

```ruby
class CollaborativeDocument < Durababble::DurableObject
  object_type "pattern_collaborative_document"

  def initialize_state
    { "revision" => 0, "content" => "" }
  end

  expose_command def append(text, expected_revision:)
    raise ArgumentError, "stale revision" unless current_state.fetch("revision") == expected_revision

    update_state(
      "revision" => current_state.fetch("revision") + 1,
      "content" => current_state.fetch("content") + text,
    )
  end

  expose def snapshot
    current_state
  end
end

CollaborativeDocument.tell("proposal", :append, "Hello", expected_revision: 0)
CollaborativeDocument.tell("proposal", :append, " world", expected_revision: 1)
```

<!-- DOCS:object-pattern-document:hidden
```ruby
worker = Durababble::Worker.new(store:, workflows: [], objects: [CollaborativeDocument], worker_id: "document-worker", migrate: false)
worker.run_until_idle
```
-->

```ruby
CollaborativeDocument.at("proposal").snapshot
```

<!-- DOCS:object-pattern-document:end -->
