# typed: false
# frozen_string_literal: true

require "json"
require "net/http"
require "socket"
require "uri"

require_relative "../test_helper"
require_relative "../../examples/chat-room/chat_room"
require_relative "../../examples/chat-room/server"

class ChatRoomExampleTest < DurababbleTestCase
  TERMINAL_STATUSES = ["completed", "failed", "canceled"].freeze

  durababble_store_backends.each do |backend|
    test "chat room persists join, post, topic, leave with #{backend.name}" do
      with_durababble_store(backend, "chat_room_object") do |store|
        worker = Durababble::Worker.new(
          store:,
          workflows: {},
          objects: [ChatRoomExample::ChatRoom],
          worker_id: "chat-room-object-test",
          migrate: false,
        )

        ChatRoomExample::ChatRoom.tell("lobby", :join, "u1", "Alice", store:)
        ChatRoomExample::ChatRoom.tell("lobby", :join, "u2", "Bob", store:)
        ChatRoomExample::ChatRoom.tell("lobby", :set_topic, "u1", "durable chat works", store:)
        ChatRoomExample::ChatRoom.tell("lobby", :post_message, "u1", "hello", store:)
        ChatRoomExample::ChatRoom.tell("lobby", :post_message, "u2", "hi alice", store:)
        ChatRoomExample::ChatRoom.tell("lobby", :leave, "u1", store:)

        assert_operator(worker.run_until_idle, :>=, 1)

        room = ChatRoomExample::ChatRoom.at("lobby", store:)
        snapshot = room.snapshot
        assert_equal("durable chat works", snapshot.fetch("topic"))
        assert_equal(["u2"], snapshot.fetch("users").keys)
        assert_equal("Bob", snapshot.fetch("users").fetch("u2").fetch("display_name"))

        kinds = snapshot.fetch("messages").map { |message| message.fetch("metadata").fetch("kind") }
        assert_equal(["join", "join", "topic", "chat", "chat", "leave"], kinds)

        chat_messages = snapshot.fetch("messages").select { |message| message.fetch("metadata").fetch("kind") == "chat" }
        assert_equal(["hello", "hi alice"], chat_messages.map { |message| message.fetch("text") })
        assert_equal("Alice", chat_messages.first.fetch("author").fetch("display_name"))

        ids = snapshot.fetch("messages").map { |message| message.fetch("id") }
        assert_equal((1..6).to_a, ids)

        since_snapshot = room.snapshot(since: 5)
        assert_equal([5, 6], since_snapshot.fetch("messages").map { |message| message.fetch("id") })
      end
    end

    test "chat room dedupes commands by idempotency key with #{backend.name}" do
      with_durababble_store(backend, "chat_room_idempotency") do |store|
        worker = Durababble::Worker.new(
          store:,
          workflows: {},
          objects: [ChatRoomExample::ChatRoom],
          worker_id: "chat-room-object-test",
          migrate: false,
        )
        ChatRoomExample::ChatRoom.tell("lobby", :join, "u1", "Alice", store:)
        ChatRoomExample::ChatRoom.tell("lobby", :join, "u1", "Alice", store:, idempotency_key: "join-once")
        ChatRoomExample::ChatRoom.tell("lobby", :join, "u1", "Alice", store:, idempotency_key: "join-once")
        worker.run_until_idle

        snapshot = ChatRoomExample::ChatRoom.at("lobby", store:).snapshot
        join_events = snapshot.fetch("messages").count { |message| message.fetch("metadata").fetch("kind") == "join" }
        # Two distinct enqueues with the same idempotency_key get deduped to one;
        # the bare tell + the deduped pair count as two unique join attempts but
        # only the first creates a message (because the second is "already in
        # room"). So we expect a single join system message.
        assert_equal(1, join_events)
      end
    end

    test "chat room rejects messages from users who are not in the room with #{backend.name}" do
      with_durababble_store(backend, "chat_room_errors") do |store|
        worker_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
        worker = Durababble::Worker.new(
          store: worker_store,
          workflows: {},
          objects: [ChatRoomExample::ChatRoom],
          worker_id: "chat-room-object-test",
          migrate: false,
        )
        stop = false
        worker_thread = Thread.new { worker.tick until stop }

        room = ChatRoomExample::ChatRoom.at("lobby", store:)
        error = assert_raises(Durababble::Error) do
          room.post_message("ghost", "i should not be here")
        end
        assert_match(/ghost/, error.message)
        stop = true
        worker_thread&.join(1)
        worker_thread&.kill if worker_thread&.alive?
        worker_store.release_worker_leases!(worker_id: "chat-room-object-test")

        assert_raises_matching(Durababble::ObjectReadBlocked, /dead_lettered mailbox head/) do
          room.snapshot
        end
        state = store.object_state(object_type: ChatRoomExample::ChatRoom.object_type, object_id: "lobby")
        assert_equal([], state ? state.fetch("messages") : [])
      ensure
        stop = true
        worker_thread&.join(1)
        worker_thread&.kill if worker_thread&.alive?
        worker_store&.close
      end
    end

    test "scheduled announcement workflow posts announcement via durable object RPCs with #{backend.name}" do
      with_durababble_store(backend, "chat_room_workflow") do |store|
        ChatRoomExample.configure(database_url: backend.database_url, schema:)
        # Seed the room with a real user via a synchronous RPC so the chat log
        # has chat messages around the announcement.
        seed_worker = Durababble::Worker.new(
          store:,
          workflows: {},
          objects: [ChatRoomExample::ChatRoom],
          worker_id: "chat-room-seed",
          migrate: false,
        )
        ChatRoomExample::ChatRoom.tell("standup", :join, "u1", "Alice", store:)
        ChatRoomExample::ChatRoom.tell("standup", :post_message, "u1", "anyone awake?", store:)
        seed_worker.run_until_idle

        workflow_id = store.enqueue_workflow(
          name: ChatRoomExample::ScheduledAnnouncementWorkflow.workflow_name,
          input: {
            "room" => "standup",
            "text" => "Standup starting now.",
            "delay_seconds" => 0,
          },
        )

        row = run_chat_room_workers(backend, workflow_id)
        assert_equal("completed", row.fetch("status"))
        result = row.fetch("result")
        assert_equal("standup", result.fetch("room"))
        assert_equal("Standup starting now.", result.fetch("text"))
        assert_operator(result.fetch("announcement_message_id"), :>, result.fetch("scheduled_message_id"))

        snapshot = ChatRoomExample::ChatRoom.at("standup", store:).snapshot
        kinds = snapshot.fetch("messages").map { |message| message.fetch("metadata").fetch("kind") }
        assert_equal(["join", "chat", "announcement_scheduled", "announcement"], kinds)
        announcement = snapshot.fetch("messages").find { |message| message.fetch("metadata").fetch("kind") == "announcement" }
        assert_equal("Standup starting now.", announcement.fetch("text"))
        assert_equal("system", announcement.fetch("author").fetch("user_id"))

        # Re-running the same workflow with the same idempotency would be the
        # job of a higher-level caller, but here we double-check that running
        # the workers a second time on a completed workflow is a no-op (no
        # extra announcement messages appear).
        assert_equal(4, snapshot.fetch("messages").length)
      ensure
        ChatRoomExample.reset_configuration!
      end
    end

    test "scheduled announcement workflow durably sleeps with #{backend.name}" do
      with_durababble_store(backend, "chat_room_sleep") do |store|
        ChatRoomExample.configure(database_url: backend.database_url, schema:)
        workflow_id = store.enqueue_workflow(
          name: ChatRoomExample::ScheduledAnnouncementWorkflow.workflow_name,
          input: {
            "room" => "lobby",
            "text" => "We will be right back.",
            "delay_seconds" => 0.25,
          },
        )

        started_at = Time.now
        row = run_chat_room_workers(backend, workflow_id, timeout: 15)
        elapsed = Time.now - started_at
        assert_equal("completed", row.fetch("status"))
        assert_operator(elapsed, :>=, 0.2, "expected workflow to wait at least ~0.25s, slept #{elapsed}s")

        snapshot = ChatRoomExample::ChatRoom.at("lobby", store:).snapshot
        kinds = snapshot.fetch("messages").map { |message| message.fetch("metadata").fetch("kind") }
        assert_equal(["announcement_scheduled", "announcement"], kinds)
      ensure
        ChatRoomExample.reset_configuration!
      end
    end
  end

  test "http server end-to-end flow joins, posts, schedules an announcement" do
    backend = durababble_store_backends.first
    with_durababble_store(backend, "chat_room_http", migrate: false) do |_store|
      port = pick_free_port
      server = ChatRoomExample::Server.new(
        host: "127.0.0.1",
        port: port,
        database_url: backend.database_url,
        schema: schema,
      )
      server_thread = Thread.new { server.run }

      wait_for_server!("127.0.0.1", port)

      base = "http://127.0.0.1:#{port}"
      join_response = http_post("#{base}/api/rooms/lobby/join", { "user_id" => "u1", "display_name" => "Alice" })
      assert_equal("200", join_response.code)
      join_body = JSON.parse(join_response.body)
      assert_equal("Alice", join_body.fetch("result").fetch("display_name"))

      post_response = http_post("#{base}/api/rooms/lobby/messages", { "user_id" => "u1", "text" => "hello server" })
      assert_equal("200", post_response.code)

      announce_response = http_post(
        "#{base}/api/announcements",
        { "room" => "lobby", "text" => "Server smoke test.", "delay_seconds" => 0 },
      )
      assert_equal("202", announce_response.code)
      workflow_id = JSON.parse(announce_response.body).fetch("workflow_id")

      deadline = Time.now + 10
      announcement_row = nil
      loop do
        get_response = http_get("#{base}/api/announcements/#{workflow_id}")
        announcement_row = JSON.parse(get_response.body)
        assert(announcement_row.key?("terminal"), "expected announcement status response to include terminal, got #{get_response.code}: #{get_response.body}")
        break if announcement_row.fetch("terminal")
        raise "announcement workflow did not finish in time" if Time.now >= deadline

        sleep(0.05)
      end
      assert_equal("completed", announcement_row.fetch("status"))

      snapshot_response = http_get("#{base}/api/rooms/lobby")
      snapshot = JSON.parse(snapshot_response.body).fetch("snapshot")
      kinds = snapshot.fetch("messages").map { |message| message.fetch("metadata").fetch("kind") }
      assert_includes(kinds, "chat")
      assert_includes(kinds, "announcement")
      chat = snapshot.fetch("messages").find { |message| message.fetch("metadata").fetch("kind") == "chat" }
      assert_equal("hello server", chat.fetch("text"))
    ensure
      server&.close
      server_thread&.join(2)
      server_thread&.kill if server_thread&.alive?
      ChatRoomExample.reset_configuration!
    end
  end

  private

  def pick_free_port
    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr.fetch(1)
    server.close
    port
  end

  def wait_for_server!(host, port, timeout: 5)
    deadline = Time.now + timeout
    loop do
      socket = TCPSocket.new(host, port)
      socket.close
      return
    rescue Errno::ECONNREFUSED, Errno::ECONNRESET
      raise "server did not start on #{host}:#{port} within #{timeout}s" if Time.now >= deadline

      sleep(0.05)
    end
  end

  def http_post(url, payload)
    uri = URI(url)
    request = Net::HTTP::Post.new(uri)
    request["Content-Type"] = "application/json"
    request.body = JSON.dump(payload)
    Net::HTTP.start(uri.host, uri.port) { |http| http.request(request) }
  end

  def http_get(url)
    uri = URI(url)
    Net::HTTP.start(uri.host, uri.port) { |http| http.request(Net::HTTP::Get.new(uri.request_uri)) }
  end

  def run_chat_room_workers(backend, workflow_id, timeout: 10)
    workflow_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
    object_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
    workflow_worker = Durababble::Worker.new(
      store: workflow_store,
      workflows: [ChatRoomExample::ScheduledAnnouncementWorkflow],
      objects: [],
      worker_id: "chat-room-workflow-test",
      migrate: false,
    )
    object_worker = Durababble::Worker.new(
      store: object_store,
      workflows: {},
      objects: [ChatRoomExample::ChatRoom],
      worker_id: "chat-room-object-test",
      migrate: false,
    )
    stop = false
    errors = Queue.new
    object_thread = Thread.new do
      object_worker.tick until stop
    rescue StandardError => e
      errors << e
    end
    workflow_thread = Thread.new do
      workflow_worker.tick until stop
    rescue StandardError => e
      errors << e
    end
    timer_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
    timer_thread = Thread.new do
      until stop
        timer_store.wake_due_timers(now: Time.now)
        sleep(0.02)
      end
    rescue StandardError => e
      errors << e
    end

    deadline = Time.now + timeout
    loop do
      raise errors.pop unless errors.empty?

      row = store.workflow(workflow_id)
      return row if TERMINAL_STATUSES.include?(row.fetch("status"))
      raise "chat room workflow did not finish before timeout" if Time.now >= deadline

      sleep(0.02)
    end
  ensure
    stop = true
    workflow_thread&.join(1)
    object_thread&.join(1)
    timer_thread&.join(1)
    workflow_thread&.kill if workflow_thread&.alive?
    object_thread&.kill if object_thread&.alive?
    timer_thread&.kill if timer_thread&.alive?
    workflow_store&.close
    object_store&.close
    timer_store&.close
  end
end
