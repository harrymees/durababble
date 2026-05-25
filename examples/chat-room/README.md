# Chat Room Example

This example shows a multi-user chat room implemented on top of Durababble. Each room is a `Durababble::DurableObject` keyed by room name; every join, leave, topic change, and chat post is an `expose_command` so the message log survives process restarts and dedupes by idempotency key. A small `ScheduledAnnouncementWorkflow` demonstrates durable orchestration: it records that an announcement is queued, optionally durably sleeps via `Durababble.sleep`, and then posts the announcement to the room — each step a durable RPC against the room object.

Start with `chat_room.rb` when reading the example. It contains the `ChatRoom` durable object and the `ScheduledAnnouncementWorkflow`. `server.rb` is the local HTTP API + static web UI wrapper, hosted with two `WorkerRuntime` pools (one for workflows, one for the room object) — the same shape as the agent-loop example.

Run the web UI:

```sh
examples/chat-room/run-server.sh
```

You can also run it manually:

```sh
mise exec -- env \
  DURABABBLE_DATABASE_URL=mysql://root@127.0.0.1:3306/sidekick_server_test \
  bundle exec ruby examples/chat-room/server.rb
```

Useful environment variables:

```sh
CHAT_ROOM_HOST=127.0.0.1
CHAT_ROOM_PORT=9293
DURABABBLE_DATABASE_URL=mysql://root@127.0.0.1:3306/sidekick_server_test
DURABABBLE_SCHEMA=durababble_chat_room_dev
```

## HTTP API

The server exposes a thin JSON API in front of the durable object and workflow:

```text
GET  /                              static HTML chat UI
GET  /api/rooms/:room               snapshot { topic, users, messages, next_message_id }
GET  /api/rooms/:room?since=N       only return messages with id >= N
POST /api/rooms/:room/join          { user_id, display_name }
POST /api/rooms/:room/leave         { user_id }
POST /api/rooms/:room/messages      { user_id, text }
POST /api/rooms/:room/topic         { user_id, topic }
POST /api/announcements             { room, text, delay_seconds }  → enqueues workflow
GET  /api/announcements/:id         workflow status + room snapshot
```

All room commands accept an optional `idempotency_key` field so client retries dedupe at the durable object inbox.

## Smoke test from the command line

```sh
curl -s -X POST -H 'content-type: application/json' \
  -d '{"user_id":"u1","display_name":"alice"}' \
  http://127.0.0.1:9293/api/rooms/lobby/join

curl -s -X POST -H 'content-type: application/json' \
  -d '{"user_id":"u1","text":"hello world"}' \
  http://127.0.0.1:9293/api/rooms/lobby/messages

curl -s -X POST -H 'content-type: application/json' \
  -d '{"room":"lobby","text":"All hands","delay_seconds":1}' \
  http://127.0.0.1:9293/api/announcements

curl -s http://127.0.0.1:9293/api/rooms/lobby | jq
```
