---
title: "Build A Durable Chat Room"
linkTitle: "Chat Room"
weight: 20
---

# Build A Durable Chat Room

This tutorial walks through building a small multi-user chat room whose users, topic, and message log survive process restarts. By the end you will have a durable object that owns each room's state and a workflow that schedules announcements into the room without double-posting if a worker dies halfway through.

The finished version lives in [`examples/chat-room`](https://github.com/harrymees/durababble-gamma/tree/main/examples/chat-room) — open that directory in another tab if you want to compare while you read.

## Why A Chat Room Is Object-Shaped

A chat room is not a one-time job. The room has an identity, callers keep returning to that identity, and every join, leave, topic change, and message should see the same ordered state. That is exactly what a durable object is for: long-lived state addressed by id, with commands serialized through a durable mailbox.

Scheduled announcements are different. "Post this message after a delay" is a finite process with a beginning and an end. A workflow fits that shape: record that the announcement was scheduled, durably sleep if needed, then command the room object to post the final message.

## What We Are Building

A Ruby file with three things in it:

1. `ChatRoom`, a `Durababble::DurableObject` keyed by room name.
2. `ScheduledAnnouncementWorkflow`, a `Durababble::Workflow` that posts delayed system messages into a room.
3. A short boot script that enqueues room commands, runs a worker, starts an announcement workflow, and reads the room snapshot.

Create `chat_room.rb` next to your application code and follow along.

## Prerequisites

You should already have Durababble installed and a database the store can connect to. If not, follow [Installation](../install.md) first.

```ruby
require "durababble"
require "json"
require "time"

Durababble.configure(database_url: Durababble.default_database_url)
store = Durababble.store
store.migrate!
```

## Step 1: A Durable Room Object

The durable object owns the room state. Mutating operations are `expose_command` methods so they run through the object's mailbox and are persisted with an idempotency record. Reads are `expose` methods because they can be served as cheap RPCs against the live object state.

```ruby
SYSTEM_USER = {
  "user_id" => "system",
  "display_name" => "System",
}.freeze

class ChatRoom < Durababble::DurableObject
  object_type "tutorial_chat_room"

  def initialize_state
    {
      "topic" => "",
      "users" => {},
      "messages" => [],
      "next_message_id" => 1,
    }
  end

  expose_command def join(user_id, display_name)
    user_id = normalize_user_id(user_id)
    display_name = display_name.to_s.strip
    raise ArgumentError, "display_name cannot be empty" if display_name.empty?

    users = current_state.fetch("users").dup
    already = users[user_id]
    users[user_id] = { "display_name" => display_name, "joined_at" => now_iso }
    state = current_state.merge("users" => users)
    state = append_message(state, SYSTEM_USER, "#{display_name} joined", "kind" => "join", "user_id" => user_id) unless already
    update_state(state)

    {
      "user_id" => user_id,
      "display_name" => display_name,
      "already_in_room" => !already.nil?,
    }
  end

  expose_command def leave(user_id)
    user_id = normalize_user_id(user_id)
    users = current_state.fetch("users").dup
    user = users.delete(user_id)
    return { "user_id" => user_id, "was_in_room" => false } unless user

    state = current_state.merge("users" => users)
    state = append_message(state, SYSTEM_USER, "#{user.fetch("display_name")} left", "kind" => "leave", "user_id" => user_id)
    update_state(state)
    { "user_id" => user_id, "was_in_room" => true }
  end

  expose_command def post_message(user_id, text)
    user_id = normalize_user_id(user_id)
    text = text.to_s.strip
    raise ArgumentError, "message text cannot be empty" if text.empty?

    sender = current_state.fetch("users").fetch(user_id) do
      raise KeyError, "user #{user_id} is not in the room"
    end
    author = { "user_id" => user_id, "display_name" => sender.fetch("display_name") }
    state = append_message(current_state, author, text, "kind" => "chat")
    update_state(state)
    state.fetch("messages").last
  end

  expose_command def post_system_message(text, metadata = {})
    text = text.to_s.strip
    raise ArgumentError, "message text cannot be empty" if text.empty?

    meta = metadata.is_a?(Hash) ? metadata : {}
    kind = meta["kind"] || "system"
    state = append_message(current_state, SYSTEM_USER, text, meta.merge("kind" => kind))
    update_state(state)
    state.fetch("messages").last
  end

  expose_command def set_topic(user_id, topic)
    user_id = normalize_user_id(user_id)
    topic = topic.to_s.strip
    user = current_state.fetch("users").fetch(user_id) do
      raise KeyError, "user #{user_id} is not in the room"
    end
    state = current_state.merge("topic" => topic)
    text = topic.empty? ? "#{user.fetch("display_name")} cleared the topic" : "#{user.fetch("display_name")} set the topic to #{topic.inspect}"
    state = append_message(state, SYSTEM_USER, text, "kind" => "topic", "user_id" => user_id, "topic" => topic)
    update_state(state)
    { "topic" => topic }
  end

  expose def snapshot(since: 0)
    since = Integer(since || 0)
    messages = current_state.fetch("messages")
    visible = since.positive? ? messages.select { |message| message.fetch("id") >= since } : messages
    {
      "topic" => current_state.fetch("topic"),
      "users" => current_state.fetch("users"),
      "messages" => visible,
      "next_message_id" => current_state.fetch("next_message_id"),
    }
  end

  private

  def append_message(state, author, text, metadata = {})
    next_id = state.fetch("next_message_id")
    message = {
      "id" => next_id,
      "author" => author,
      "text" => text,
      "posted_at" => now_iso,
      "metadata" => metadata,
      "operation_id" => command_context&.idempotency_key,
    }
    state.merge(
      "messages" => state.fetch("messages") + [message],
      "next_message_id" => next_id + 1,
    )
  end

  def normalize_user_id(user_id)
    user_id = user_id.to_s.strip
    raise ArgumentError, "user_id cannot be empty" if user_id.empty?

    user_id
  end

  def now_iso
    Time.now.utc.iso8601
  end
end
```

We use the room name as the object id, so every call to `ChatRoom.at("lobby")` or `ChatRoom.tell("lobby", ...)` lands on the same persisted room. The real example uses this directly behind an HTTP API; the durable object does not know or care whether the caller is a web request, a workflow, or a test.

## Step 2: Commands, Queries, And Idempotency

Room mutations are commands because ordering matters. Two users posting at the same time should produce two messages with distinct ids, and a retry of the same HTTP request should be able to reuse the same `idempotency_key` without appending duplicate messages.

```ruby
ChatRoom.tell("lobby", :join, "u1", "Alice", store:, idempotency_key: "join-u1")
ChatRoom.tell("lobby", :post_message, "u1", "hello durable chat", store:, idempotency_key: "message-1")
```

`tell` enqueues the command and returns after the command is durably accepted. A worker drains the mailbox later. For request/response flows, use a handle and call the command directly:

```ruby
room = ChatRoom.at("lobby")
message = room.post_message("u1", "same mailbox, synchronous result", idempotency_key: "message-2")
snapshot = room.snapshot
```

The direct command still goes through the durable mailbox; it just waits for the command result. `snapshot` is an exposed query, so it reads the current object state without appending a mailbox message.

## Step 3: A Scheduled Announcement Workflow

Now add a workflow for delayed system messages. It records the request in the room first, sleeps durably if the caller asked for a delay, then posts the final announcement through the same room object.

```ruby
class ScheduledAnnouncementWorkflow < Durababble::Workflow
  workflow_name "tutorial-chat-room-announcement"

  def execute(input)
    room_id = input.fetch("room").to_s.strip
    raise ArgumentError, "room cannot be empty" if room_id.empty?

    text = input.fetch("text").to_s
    raise ArgumentError, "text cannot be empty" if text.strip.empty?

    delay_seconds = Float(input.fetch("delay_seconds", 0))
    raise ArgumentError, "delay_seconds cannot be negative" if delay_seconds.negative?

    scheduled = record_request(room_id, text, delay_seconds)
    Durababble.sleep(delay_seconds) if delay_seconds.positive?
    posted = post_announcement(room_id, text)
    finalize(room_id, scheduled, posted)
  end

  step def record_request(room_id, text, delay_seconds)
    room = ChatRoom.at(room_id)
    preview = delay_seconds.positive? ? "Announcement scheduled in #{format_delay(delay_seconds)}: #{text}" : "Announcement queued: #{text}"
    message = room.post_system_message(
      preview,
      { "kind" => "announcement_scheduled", "delay_seconds" => delay_seconds, "preview_text" => text },
      idempotency_key: step_context.idempotency_key,
    )
    { "scheduled_message_id" => message.fetch("id"), "preview" => preview }
  end

  step def post_announcement(room_id, text)
    room = ChatRoom.at(room_id)
    message = room.post_system_message(
      text,
      { "kind" => "announcement" },
      idempotency_key: step_context.idempotency_key,
    )
    { "message_id" => message.fetch("id"), "posted_at" => message.fetch("posted_at"), "text" => message.fetch("text") }
  end

  step def finalize(room_id, scheduled, posted)
    {
      "status" => "finished",
      "room" => room_id,
      "scheduled_message_id" => scheduled.fetch("scheduled_message_id"),
      "announcement_message_id" => posted.fetch("message_id"),
      "posted_at" => posted.fetch("posted_at"),
      "text" => posted.fetch("text"),
    }
  end

  private

  def format_delay(seconds)
    seconds = Float(seconds)
    return "#{seconds.to_i}s" if seconds == seconds.to_i

    "#{seconds}s"
  end
end
```

A few things to notice:

- The workflow does not mutate room state directly. It uses room commands, so the chat log has one owner and all messages share one ordering rule.
- `record_request` and `post_announcement` are steps. If the workflow worker crashes after recording the request, replay gets the persisted step result and does not post another "scheduled" message.
- Each room command uses `step_context.idempotency_key`. If a step is retried while waiting for the object command result, the retry lands as the same mailbox command instead of appending a duplicate message.

## Step 4: Running It

A worker process needs to know about both classes. In a real app this lives in your worker boot code; here we run a worker just long enough to drain the room commands and finish one zero-delay announcement.

```ruby
worker = Durababble::Worker.new(
  store:,
  workflows: [ScheduledAnnouncementWorkflow],
  objects: [ChatRoom],
  worker_id: "chat-room-tutorial-worker",
  migrate: false,
)

ChatRoom.tell("lobby", :join, "u1", "Alice", store:, idempotency_key: "join-alice")
ChatRoom.tell("lobby", :set_topic, "u1", "Durable chat", store:, idempotency_key: "topic-1")
ChatRoom.tell("lobby", :post_message, "u1", "hello from the room object", store:, idempotency_key: "message-1")
worker.run_until_idle

handle = ScheduledAnnouncementWorkflow.start({
  "room" => "lobby",
  "text" => "Daily standup starts now.",
  "delay_seconds" => 0,
})

worker.run_until_idle
room = ChatRoom.at("lobby")
puts JSON.pretty_generate(
  "announcement" => handle.result,
  "room" => room.snapshot,
)
```

The output includes the seeded chat messages, the "announcement queued" system message, and the final announcement. If the process dies after the scheduled message is recorded but before the announcement is posted, starting a worker again resumes from the persisted workflow history and the room's persisted mailbox state.

For non-zero delays, something also needs to wake due timers. The full example's server runs a tiny timer loop around `store.wake_due_timers(now: Time.now)`; production deployments usually wire the same call into whichever scheduler or worker heartbeat owns timer advancement.

## Where To Go Next

The real [`examples/chat-room`](https://github.com/harrymees/durababble-gamma/tree/main/examples/chat-room) adds a small HTTP API, a static web UI, separate `WorkerRuntime` pools for workflows and room objects, and an announcement status endpoint that returns both workflow status and the latest room snapshot. The durable object and workflow shape are the same as what you just built.

From here, useful directions:

- Put the room object and announcement workflow in different worker pools. The API can route room commands to the object pool and scheduled work to the workflow pool while both share the same durable store.
- Add retention or transcript export. Because the room owns the ordered message log, commands like `trim_messages_before(id)` or `export_transcript` have a single place to enforce consistency.
- Add moderation or approvals. A workflow can wait for a human decision before posting a system message, then command the same room object once the decision arrives.
