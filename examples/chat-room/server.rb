# typed: false
# frozen_string_literal: true

require "json"
require "securerandom"
require "socket"
require "uri"

require_relative "chat_room"

module ChatRoomExample
  class Server
    TERMINAL_STATUSES = ["completed", "failed", "canceled"].freeze
    WORKFLOW_WORKER_POOL = "chat-room-workflows"
    OBJECT_WORKER_POOL = "chat-room-objects"
    ROOM_PATH = %r{\A/api/rooms/(?<room>[^/]+)(?<rest>/[^?]*)?\z}

    def initialize(host:, port:, database_url:, schema:)
      @host = host
      @port = port
      @database_url = database_url
      @schema = schema
      @store = Durababble::Store.connect(database_url:, schema:)
      @store.migrate!
      ChatRoomExample.configure(database_url:, schema:, workflow_worker_pool: WORKFLOW_WORKER_POOL, object_worker_pool: OBJECT_WORKER_POOL)
      @workflow_runtime = Durababble::WorkerRuntime.new(
        workflows: [ScheduledAnnouncementWorkflow],
        objects: [],
        database_url:,
        schema:,
        worker_pool: ChatRoomExample.workflow_worker_pool,
        poll_interval: 0.05,
      )
      @object_runtime = Durababble::WorkerRuntime.new(
        workflows: [],
        objects: [ChatRoom],
        database_url:,
        schema:,
        worker_pool: ChatRoomExample.object_worker_pool,
        poll_interval: 0.05,
      )
      # WorkerRuntime processes activations and inbox messages, but durable
      # timers (Durababble.sleep / wait_until) still need an external mechanism
      # to mark waits ready. A tiny async task polling wake_due_timers
      # keeps scheduled announcements with non-zero delays moving forward.
      @timer_store = Durababble::Store.connect(database_url:, schema:)
      @timer_stop = false
      @timer_task = nil
      @tcp_server = TCPServer.new(@host, @port)
    end

    def url
      port = @tcp_server.addr.fetch(1)
      "http://#{@host}:#{port}"
    end

    def run
      Async do
        @workflow_runtime.start
        @object_runtime.start
        @timer_task = Async::Task.current.async { run_timer_loop }
        puts "Chat room example listening on #{url}"
        loop do
          socket = @tcp_server.accept
          handle(socket)
        end
      rescue IOError
        nil
      ensure
        shutdown_runtimes
      end
    rescue Interrupt, IOError
      nil
    ensure
      close
    end

    def close
      @tcp_server&.close unless @tcp_server&.closed?
      @timer_stop = true
      @timer_store&.close
      @store&.close
    rescue IOError
      nil
    end

    private

    def shutdown_runtimes
      @timer_stop = true
      @timer_task&.stop
      @timer_task = nil
      @workflow_runtime&.shutdown
      @object_runtime&.shutdown
    end

    def run_timer_loop
      until @timer_stop
        @timer_store.wake_due_timers(now: Time.now)
        sleep(0.1)
      end
    rescue StandardError
      nil
    end

    def handle(socket)
      request_line = socket.gets&.strip
      return unless request_line

      method, raw_path, = request_line.split(" ")
      headers = read_headers(socket)
      body = socket.read(headers.fetch("content-length", "0").to_i).to_s
      uri = URI.parse(raw_path)

      response = route(method, uri.path, uri.query, body)
      write_response(socket, *response)
    rescue StandardError => e
      write_response(socket, 500, "application/json", JSON.dump("error" => "#{e.class}: #{e.message}"))
    ensure
      socket.close
    end

    def read_headers(socket)
      headers = {}
      while (line = socket.gets)
        line = line.chomp
        break if line.empty?

        key, value = line.split(":", 2)
        headers[key.downcase] = value.to_s.strip
      end
      headers
    end

    def route(method, path, query, body)
      case [method, path]
      when ["GET", "/"]
        [200, "text/html; charset=utf-8", html]
      when ["GET", "/api/config"]
        json(200, "rooms_endpoint" => "/api/rooms/:room", "announcements_endpoint" => "/api/announcements")
      when ["POST", "/api/announcements"]
        schedule_announcement(body)
      else
        if method == "GET" && (match = path.match(%r{\A/api/announcements/(?<id>[^/]+)\z}))
          return show_announcement(match[:id])
        end

        if (match = path.match(ROOM_PATH))
          return route_room(method, match[:room], match[:rest].to_s, query, body)
        end

        [404, "application/json", JSON.dump("error" => "not found")]
      end
    end

    def route_room(method, room_id, rest, query, body)
      room_id = URI.decode_www_form_component(room_id)
      case [method, rest]
      when ["GET", ""], ["GET", "/"]
        show_room(room_id, query)
      when ["POST", "/join"]
        room_command(room_id, body) { |room, payload, key| room.join(payload.fetch("user_id"), payload.fetch("display_name"), idempotency_key: key) }
      when ["POST", "/leave"]
        room_command(room_id, body) { |room, payload, key| room.leave(payload.fetch("user_id"), idempotency_key: key) }
      when ["POST", "/messages"]
        room_command(room_id, body) { |room, payload, key| room.post_message(payload.fetch("user_id"), payload.fetch("text"), idempotency_key: key) }
      when ["POST", "/topic"]
        room_command(room_id, body) { |room, payload, key| room.set_topic(payload.fetch("user_id"), payload.fetch("topic"), idempotency_key: key) }
      else
        [404, "application/json", JSON.dump("error" => "not found")]
      end
    end

    def parse_body(body)
      body.empty? ? {} : JSON.parse(body)
    end

    def show_room(room_id, query)
      params = query ? URI.decode_www_form(query).to_h : {}
      since = Integer(params.fetch("since", 0))
      snapshot = ChatRoom.at(room_id, store: @store, worker_pool: ChatRoomExample.object_worker_pool).snapshot(since:)
      json(200, "room" => room_id, "snapshot" => snapshot)
    end

    def room_command(room_id, body)
      payload = parse_body(body)
      idempotency_key = payload["idempotency_key"]
      idempotency_key = nil if idempotency_key.to_s.empty?
      room = ChatRoom.at(room_id, store: @store, worker_pool: ChatRoomExample.object_worker_pool)
      result = yield(room, payload, idempotency_key)
      json(200, "room" => room_id, "result" => result)
    rescue ArgumentError, KeyError, Durababble::Error => e
      json(400, "error" => "#{e.class}: #{e.message}")
    end

    def schedule_announcement(body)
      payload = parse_body(body)
      room_id = payload.fetch("room")
      text = payload.fetch("text")
      delay_seconds = Float(payload.fetch("delay_seconds", 0))
      workflow_id = @store.enqueue_workflow(
        name: ScheduledAnnouncementWorkflow.workflow_name,
        input: {
          "room" => room_id,
          "text" => text,
          "delay_seconds" => delay_seconds,
        },
        worker_pool: ChatRoomExample.workflow_worker_pool,
      )
      json(202, "workflow_id" => workflow_id, "room" => room_id, "delay_seconds" => delay_seconds)
    rescue KeyError => e
      json(400, "error" => "missing field: #{e.message}")
    rescue ArgumentError => e
      json(400, "error" => e.message)
    end

    def show_announcement(workflow_id)
      row = @store.workflow(workflow_id)
      input = row.fetch("input")
      room_id = input.fetch("room")
      payload = {
        "workflow_id" => workflow_id,
        "room" => room_id,
        "status" => row.fetch("status"),
        "terminal" => TERMINAL_STATUSES.include?(row.fetch("status")),
        "result" => row["result"],
        "error" => row["error"],
      }
      begin
        payload["room_snapshot"] = ChatRoom.at(room_id, store: @store, worker_pool: ChatRoomExample.object_worker_pool).snapshot
      rescue Durababble::ObjectReadBlocked, Durababble::WorkflowRpc::Error => e
        payload["room_snapshot_blocked"] = e.message
      end
      json(200, payload)
    end

    def json(status, payload)
      [status, "application/json", JSON.pretty_generate(payload)]
    end

    def write_response(socket, status, content_type, body)
      reason = { 200 => "OK", 202 => "Accepted", 400 => "Bad Request", 404 => "Not Found", 500 => "Server Error" }.fetch(status, "OK")
      socket.write("HTTP/1.1 #{status} #{reason}\r\n")
      socket.write("Content-Type: #{content_type}\r\n")
      socket.write("Content-Length: #{body.bytesize}\r\n")
      socket.write("Cache-Control: no-store\r\n")
      socket.write("Connection: close\r\n")
      socket.write("\r\n")
      socket.write(body)
    end

    def html
      <<~HTML
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Chat Room</title>
          <style>
            :root {
              color-scheme: light;
              --bg: #f6f5f0;
              --ink: #20242b;
              --muted: #667085;
              --line: #d9d6ca;
              --panel: #ffffff;
              --accent: #0f766e;
              --accent-dark: #0d5f58;
              --soft: #eef6f2;
              --danger: #b42318;
              --system: #856404;
            }

            * { box-sizing: border-box; }

            body {
              margin: 0;
              min-height: 100vh;
              background: var(--bg);
              color: var(--ink);
              font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            }

            header {
              border-bottom: 1px solid var(--line);
              background: var(--panel);
              padding: 14px 18px;
              display: flex;
              align-items: baseline;
              justify-content: space-between;
              gap: 14px;
            }

            h1 { margin: 0; font-size: 18px; font-weight: 650; }
            h2 { margin: 0 0 10px; font-size: 14px; }

            .api {
              color: var(--muted);
              font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
            }

            main {
              display: grid;
              grid-template-columns: minmax(260px, 320px) minmax(0, 1fr) minmax(220px, 280px);
              gap: 14px;
              padding: 14px;
            }

            .panel {
              background: var(--panel);
              border: 1px solid var(--line);
              border-radius: 8px;
              padding: 12px;
            }

            label {
              display: block;
              margin: 8px 0 4px;
              color: var(--muted);
              font-weight: 600;
              font-size: 12px;
            }

            input, textarea, button {
              width: 100%;
              border: 1px solid var(--line);
              border-radius: 6px;
              padding: 8px 10px;
              font: inherit;
              color: var(--ink);
              background: #fff;
            }

            button {
              background: var(--accent);
              color: #fff;
              border: 0;
              font-weight: 650;
              cursor: pointer;
              margin-top: 6px;
            }

            button.secondary { background: #eef0eb; color: var(--ink); }
            button:disabled { opacity: 0.6; cursor: wait; }

            .topic {
              padding: 8px 10px;
              border: 1px dashed var(--line);
              border-radius: 6px;
              color: var(--muted);
              background: var(--soft);
              margin-bottom: 8px;
            }

            .messages {
              display: flex;
              flex-direction: column;
              gap: 6px;
              max-height: 60vh;
              overflow-y: auto;
              padding: 4px;
            }

            .message {
              padding: 6px 8px;
              border-radius: 6px;
              background: #fafaf6;
              border: 1px solid var(--line);
            }

            .message.system {
              background: #fff8e0;
              border-color: #f1d97b;
              color: var(--system);
              font-style: italic;
            }

            .meta {
              font: 11px/1.3 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
              color: var(--muted);
              margin-bottom: 2px;
            }

            .users {
              display: flex;
              flex-direction: column;
              gap: 4px;
            }

            .user {
              border: 1px solid var(--line);
              border-radius: 6px;
              padding: 6px 8px;
              background: #fafaf6;
              font-size: 13px;
            }

            .status {
              color: var(--muted);
              font-size: 12px;
              margin-top: 6px;
            }

            .error { color: var(--danger); margin-top: 6px; }

            @media (max-width: 980px) {
              main { display: block; }
              main > .panel + .panel { margin-top: 12px; }
            }
          </style>
        </head>
        <body>
          <header>
            <h1>Chat Room</h1>
            <span id="api-target" class="api">durababble worker pools: chat-room-workflows + chat-room-objects</span>
          </header>
          <main>
            <section class="panel">
              <h2>Identity</h2>
              <label for="room">Room</label>
              <input id="room" value="lobby">
              <label for="user-id">User ID</label>
              <input id="user-id">
              <label for="display-name">Display name</label>
              <input id="display-name" value="alice">
              <button id="join-btn">Join</button>
              <button id="leave-btn" class="secondary">Leave</button>
              <p class="status" id="identity-status"></p>
              <p class="error" id="identity-error"></p>

              <h2 style="margin-top:18px">Announcement</h2>
              <label for="announce-text">Text</label>
              <textarea id="announce-text" rows="2">A scheduled system message.</textarea>
              <label for="announce-delay">Delay (seconds)</label>
              <input id="announce-delay" type="number" min="0" step="0.5" value="2">
              <button id="announce-btn">Schedule announcement</button>
              <p class="status" id="announce-status"></p>
            </section>

            <section class="panel">
              <h2>Conversation</h2>
              <div class="topic" id="topic">No topic.</div>
              <div class="messages" id="messages"></div>
              <label for="message">Message</label>
              <textarea id="message" rows="2" placeholder="Say something..."></textarea>
              <button id="send-btn">Send</button>
              <label for="topic-input" style="margin-top:8px">Set topic</label>
              <input id="topic-input" placeholder="durable chat works">
              <button id="topic-btn" class="secondary">Update topic</button>
              <p class="error" id="message-error"></p>
            </section>

            <section class="panel">
              <h2>Users</h2>
              <div class="users" id="users"></div>
              <h2 style="margin-top:18px">Recent system events</h2>
              <div class="messages" id="events"></div>
            </section>
          </main>

          <script>
            const $ = (id) => document.getElementById(id);
            const roomInput = $("room");
            const userIdInput = $("user-id");
            const displayNameInput = $("display-name");
            const joinBtn = $("join-btn");
            const leaveBtn = $("leave-btn");
            const identityStatus = $("identity-status");
            const identityError = $("identity-error");
            const topicNode = $("topic");
            const messagesNode = $("messages");
            const usersNode = $("users");
            const eventsNode = $("events");
            const sendBtn = $("send-btn");
            const messageInput = $("message");
            const topicInput = $("topic-input");
            const topicBtn = $("topic-btn");
            const messageError = $("message-error");
            const announceBtn = $("announce-btn");
            const announceStatus = $("announce-status");

            if (!userIdInput.value) {
              userIdInput.value = "user-" + Math.random().toString(36).slice(2, 8);
            }

            let pollTimer = null;
            let joined = false;

            async function api(method, path, payload) {
              const init = { method, headers: { "Content-Type": "application/json" } };
              if (payload !== undefined) init.body = JSON.stringify(payload);
              const response = await fetch(path, init);
              const body = await response.json();
              if (!response.ok) throw new Error(body.error || response.statusText);
              return body;
            }

            function escapeHtml(text) {
              return String(text).replace(/[&<>"']/g, (c) => ({
                "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
              }[c]));
            }

            function render(snapshot) {
              topicNode.textContent = snapshot.topic ? "Topic: " + snapshot.topic : "No topic.";
              messagesNode.replaceChildren(...snapshot.messages.map((message) => {
                const div = document.createElement("div");
                const kind = (message.metadata && message.metadata.kind) || "chat";
                div.className = "message" + (message.author.user_id === "system" ? " system" : "");
                const meta = document.createElement("div");
                meta.className = "meta";
                meta.textContent = `#${message.id} ${message.author.display_name} · ${message.posted_at} · ${kind}`;
                const body = document.createElement("div");
                body.textContent = message.text;
                div.append(meta, body);
                return div;
              }));
              messagesNode.scrollTop = messagesNode.scrollHeight;

              usersNode.replaceChildren(...Object.entries(snapshot.users).map(([id, user]) => {
                const div = document.createElement("div");
                div.className = "user";
                div.textContent = `${user.display_name} (${id})`;
                return div;
              }));

              const systemEvents = snapshot.messages.filter((message) => message.author.user_id === "system").slice(-12);
              eventsNode.replaceChildren(...systemEvents.map((message) => {
                const div = document.createElement("div");
                div.className = "message system";
                const meta = document.createElement("div");
                meta.className = "meta";
                meta.textContent = `#${message.id} · ${(message.metadata && message.metadata.kind) || "system"}`;
                const body = document.createElement("div");
                body.textContent = message.text;
                div.append(meta, body);
                return div;
              }));
            }

            async function poll() {
              try {
                const data = await api("GET", `/api/rooms/${encodeURIComponent(roomInput.value)}`);
                render(data.snapshot);
              } catch (error) {
                messageError.textContent = error.message;
              } finally {
                pollTimer = setTimeout(poll, 1000);
              }
            }

            joinBtn.addEventListener("click", async () => {
              identityError.textContent = "";
              try {
                const result = await api("POST", `/api/rooms/${encodeURIComponent(roomInput.value)}/join`, {
                  user_id: userIdInput.value,
                  display_name: displayNameInput.value,
                });
                identityStatus.textContent = result.result.already_in_room ? "Updated display name." : "Joined.";
                joined = true;
                if (!pollTimer) poll();
              } catch (error) {
                identityError.textContent = error.message;
              }
            });

            leaveBtn.addEventListener("click", async () => {
              identityError.textContent = "";
              try {
                await api("POST", `/api/rooms/${encodeURIComponent(roomInput.value)}/leave`, {
                  user_id: userIdInput.value,
                });
                identityStatus.textContent = "Left.";
                joined = false;
              } catch (error) {
                identityError.textContent = error.message;
              }
            });

            sendBtn.addEventListener("click", async () => {
              messageError.textContent = "";
              if (!messageInput.value.trim()) return;
              try {
                await api("POST", `/api/rooms/${encodeURIComponent(roomInput.value)}/messages`, {
                  user_id: userIdInput.value,
                  text: messageInput.value,
                });
                messageInput.value = "";
              } catch (error) {
                messageError.textContent = error.message;
              }
            });

            topicBtn.addEventListener("click", async () => {
              messageError.textContent = "";
              try {
                await api("POST", `/api/rooms/${encodeURIComponent(roomInput.value)}/topic`, {
                  user_id: userIdInput.value,
                  topic: topicInput.value,
                });
                topicInput.value = "";
              } catch (error) {
                messageError.textContent = error.message;
              }
            });

            announceBtn.addEventListener("click", async () => {
              announceStatus.textContent = "";
              try {
                const result = await api("POST", `/api/announcements`, {
                  room: roomInput.value,
                  text: $("announce-text").value,
                  delay_seconds: Number($("announce-delay").value) || 0,
                });
                announceStatus.textContent = `Workflow ${result.workflow_id} queued (delay ${result.delay_seconds}s).`;
              } catch (error) {
                announceStatus.textContent = error.message;
              }
            });

            poll();
          </script>
        </body>
        </html>
      HTML
    end
  end
end

if $PROGRAM_NAME == __FILE__
  host = ENV.fetch("CHAT_ROOM_HOST", "127.0.0.1")
  port = Integer(ENV.fetch("CHAT_ROOM_PORT", "9293"))
  database_url = ENV.fetch("DURABABBLE_DATABASE_URL", Durababble.default_database_url)
  schema = ENV.fetch("DURABABBLE_SCHEMA") do
    Durababble.workspace_schema(__dir__, prefix: "durababble_chat_room")
  end
  server = ChatRoomExample::Server.new(host:, port:, database_url:, schema:)
  server.run
end
