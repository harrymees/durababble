# typed: false
# frozen_string_literal: true

require "json"
require "securerandom"
require "socket"
require "uri"

require_relative "agent_loop"

module AgentLoopExample
  class Server
    TERMINAL_STATUSES = ["completed", "failed", "canceled"].freeze

    def initialize(host:, port:, database_url:, schema:)
      @host = host
      @port = port
      @database_url = database_url
      @schema = schema
      @store = Durababble::Store.connect(database_url:, schema:)
      @store.migrate!
      AgentLoopExample.configure(database_url:, schema:)
      # Everything runs in the default worker pool. WorkerRuntime requires the
      # pool name explicitly, but because it is the default, nothing in the
      # workflow steps, the enqueue, or the snapshot query has to name a pool.
      # Workflows and objects are split across two runtimes so object work can
      # keep draining while a workflow step waits for a command result.
      @workflow_runtime = Durababble::WorkerRuntime.new(
        workflows: [AgentLoopWorkflow],
        objects: [],
        database_url:,
        schema:,
        worker_pool: "default",
        poll_interval: 0.05,
      )
      @object_runtime = Durababble::WorkerRuntime.new(
        workflows: [],
        objects: [VirtualFileSystem],
        database_url:,
        schema:,
        worker_pool: "default",
        poll_interval: 0.05,
      )
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
        puts "Agent loop example listening on #{url}"
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
      @store&.close
    rescue IOError
      nil
    end

    private

    def shutdown_runtimes
      @workflow_runtime&.shutdown
      @object_runtime&.shutdown
    end

    def handle(socket)
      request_line = socket.gets&.strip
      return unless request_line

      method, raw_path, = request_line.split(" ")
      headers = read_headers(socket)
      body = socket.read(headers.fetch("content-length", "0").to_i).to_s
      path = URI.parse(raw_path).path

      response = route(method, path, body)
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

    def route(method, path, body)
      case [method, path]
      when ["GET", "/"]
        [200, "text/html; charset=utf-8", html]
      when ["GET", "/api/config"]
        json(200, client_config)
      when ["POST", "/api/runs"]
        create_run(body)
      else
        if method == "GET" && path.match?(%r{\A/api/runs/[^/]+\z})
          return show_run(path.split("/").last)
        end

        [404, "application/json", JSON.dump("error" => "not found")]
      end
    end

    def create_run(body)
      payload = body.empty? ? {} : JSON.parse(body)
      session_id = payload["session_id"].to_s
      session_id = "session-#{SecureRandom.hex(4)}" if session_id.empty?
      request = payload.fetch("request", "")
      workflow_id = @store.enqueue_workflow(
        name: AgentLoopWorkflow.workflow_name,
        input: {
          "session_id" => session_id,
          "request" => request,
          "max_turns" => Integer(payload.fetch("max_turns", 12)),
        },
      )
      json(202, "workflow_id" => workflow_id, "session_id" => session_id)
    end

    def client_config
      client = AgentLoopExample.llm_client
      base_url = client.respond_to?(:base_url) ? client.base_url : nil
      {
        "model" => client.respond_to?(:model_name) ? client.model_name : client.class.name,
        "base_url" => base_url,
        "base_host" => base_url && URI(base_url).host,
      }
    rescue URI::InvalidURIError
      {
        "model" => client.respond_to?(:model_name) ? client.model_name : client.class.name,
        "base_url" => base_url,
        "base_host" => base_url,
      }
    end

    def show_run(workflow_id)
      row = @store.workflow(workflow_id)
      input = row.fetch("input")
      session_id = input.fetch("session_id")
      snapshot = VirtualFileSystem.at(session_id, store: @store).snapshot
      json(
        200,
        "workflow_id" => workflow_id,
        "session_id" => session_id,
        "status" => row.fetch("status"),
        "terminal" => TERMINAL_STATUSES.include?(row.fetch("status")),
        "result" => row["result"],
        "error" => row["error"],
        "filesystem" => snapshot,
      )
    end

    def json(status, payload)
      [status, "application/json", JSON.pretty_generate(payload)]
    end

    def write_response(socket, status, content_type, body)
      reason = { 200 => "OK", 202 => "Accepted", 404 => "Not Found", 500 => "Server Error" }.fetch(status, "OK")
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
          <title>Agent Loop</title>
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
            }

            * {
              box-sizing: border-box;
            }

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
              padding: 16px 20px;
            }

            .topbar {
              display: flex;
              align-items: baseline;
              justify-content: space-between;
              gap: 14px;
            }

            h1 {
              margin: 0;
              font-size: 20px;
              font-weight: 650;
              letter-spacing: 0;
            }

            .api {
              color: var(--muted);
              font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
              overflow-wrap: anywhere;
            }

            main {
              display: grid;
              grid-template-columns: minmax(290px, 420px) minmax(0, 1fr);
              gap: 18px;
              padding: 18px;
            }

            section {
              min-width: 0;
            }

            .panel {
              background: var(--panel);
              border: 1px solid var(--line);
              border-radius: 8px;
              padding: 14px;
            }

            label {
              display: block;
              margin-bottom: 8px;
              color: var(--muted);
              font-weight: 600;
            }

            textarea,
            input {
              width: 100%;
              border: 1px solid var(--line);
              border-radius: 6px;
              padding: 10px;
              color: var(--ink);
              background: #fff;
            }

            textarea {
              min-height: 230px;
              resize: vertical;
              font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
            }

            input {
              height: 38px;
              font: inherit;
            }

            .field {
              margin-top: 12px;
            }

            button {
              margin-top: 14px;
              width: 100%;
              min-height: 38px;
              border: 0;
              border-radius: 6px;
              background: var(--accent);
              color: white;
              font-weight: 700;
              cursor: pointer;
            }

            button:disabled {
              cursor: wait;
              opacity: 0.68;
            }

            .status {
              display: flex;
              justify-content: space-between;
              gap: 12px;
              margin-top: 12px;
              color: var(--muted);
              overflow-wrap: anywhere;
            }

            .grid {
              display: grid;
              grid-template-columns: minmax(0, 1fr) minmax(260px, 34%);
              gap: 18px;
            }

            h2 {
              margin: 0 0 10px;
              font-size: 15px;
              letter-spacing: 0;
            }

            .files {
              display: grid;
              gap: 12px;
            }

            .file {
              border: 1px solid var(--line);
              border-radius: 8px;
              overflow: hidden;
              background: #fbfbfa;
            }

            .file header {
              display: flex;
              justify-content: space-between;
              gap: 12px;
              padding: 8px 10px;
              background: #eef4f2;
              border-bottom: 1px solid var(--line);
              font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
              font-size: 12px;
            }

            .file header span:first-child {
              overflow-wrap: anywhere;
            }

            pre {
              margin: 0;
              overflow: auto;
              padding: 10px;
              white-space: pre-wrap;
              overflow-wrap: anywhere;
              font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
            }

            ol {
              margin: 0;
              padding-left: 22px;
            }

            li {
              margin-bottom: 8px;
              color: var(--muted);
              overflow-wrap: anywhere;
            }

            li strong {
              color: var(--ink);
              font-weight: 650;
            }

            .error {
              color: var(--danger);
              font-weight: 650;
            }

            @media (max-width: 860px) {
              .topbar,
              main,
              .grid {
                display: block;
              }

              main {
                padding: 12px;
              }

              main > section + section,
              .grid > .panel + .panel {
                margin-top: 12px;
              }
            }
          </style>
        </head>
        <body>
          <header>
            <div class="topbar">
              <h1>Agent Loop</h1>
              <span id="api-target" class="api">loading model</span>
            </div>
          </header>
          <main>
            <section class="panel">
              <form id="request-form">
                <label for="request">Request</label>
                <textarea id="request" name="request">Create /plan.md with a short title, two bullets, and the word draft.
        Read /plan.md back.
        Replace draft with durable.
        Append a final "Next: run smoke test." line.
        Create /summary.txt summarizing what changed.</textarea>
                <div class="field">
                  <label for="max-turns">Max Model Turns</label>
                  <input id="max-turns" name="max-turns" type="number" min="1" max="20" value="12">
                </div>
                <button id="submit" type="submit">Run Agent</button>
              </form>
              <div class="status">
                <span id="run-id">No run</span>
                <strong id="status">idle</strong>
              </div>
              <p id="error" class="error"></p>
            </section>
            <section class="grid">
              <div class="panel">
                <h2>Files</h2>
                <div id="files" class="files"></div>
              </div>
              <div class="panel">
                <h2>Events</h2>
                <ol id="events"></ol>
              </div>
            </section>
          </main>
          <script>
            const form = document.getElementById("request-form");
            const submit = document.getElementById("submit");
            const statusNode = document.getElementById("status");
            const runIdNode = document.getElementById("run-id");
            const errorNode = document.getElementById("error");
            const filesNode = document.getElementById("files");
            const eventsNode = document.getElementById("events");
            const apiTargetNode = document.getElementById("api-target");
            let pollTimer = null;

            loadConfig();

            form.addEventListener("submit", async (event) => {
              event.preventDefault();
              clearTimeout(pollTimer);
              try {
                submit.disabled = true;
                errorNode.textContent = "";
                statusNode.textContent = "queued";
                runIdNode.textContent = "starting";
                filesNode.replaceChildren();
                eventsNode.replaceChildren();

                const response = await fetch("/api/runs", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                    request: document.getElementById("request").value,
                    max_turns: Number(document.getElementById("max-turns").value) || 12,
                  }),
                });
                const payload = await response.json();
                if (!response.ok) throw new Error(payload.error || "request failed");
                runIdNode.textContent = payload.workflow_id;
                poll(payload.workflow_id);
              } catch (error) {
                submit.disabled = false;
                statusNode.textContent = "error";
                errorNode.textContent = error.message;
              }
            });

            async function loadConfig() {
              try {
                const response = await fetch("/api/config");
                const payload = await response.json();
                if (!response.ok) throw new Error(payload.error || "config failed");
                apiTargetNode.textContent = [payload.model, payload.base_host || payload.base_url].filter(Boolean).join(" @ ");
              } catch (error) {
                apiTargetNode.textContent = "model unavailable";
              }
            }

            async function poll(workflowId) {
              try {
                const response = await fetch(`/api/runs/${workflowId}`);
                const payload = await response.json();
                if (!response.ok) throw new Error(payload.error || "poll failed");
                render(payload);
                if (!payload.terminal) {
                  pollTimer = setTimeout(() => poll(workflowId), 500);
                } else {
                  submit.disabled = false;
                }
              } catch (error) {
                submit.disabled = false;
                statusNode.textContent = "error";
                errorNode.textContent = error.message;
              }
            }

            function render(payload) {
              statusNode.textContent = payload.status;
              const files = payload.filesystem.files;
              filesNode.replaceChildren(...Object.entries(files).map(([path, file]) => {
                const wrapper = document.createElement("article");
                wrapper.className = "file";
                const header = document.createElement("header");
                const pathNode = document.createElement("span");
                pathNode.textContent = path;
                const revisionNode = document.createElement("span");
                revisionNode.textContent = `r${file.revision}`;
                header.append(pathNode, revisionNode);
                const body = document.createElement("pre");
                body.textContent = file.content;
                wrapper.append(header, body);
                return wrapper;
              }));
              if (Object.keys(files).length === 0) {
                const empty = document.createElement("p");
                empty.textContent = "No files";
                filesNode.append(empty);
              }

              eventsNode.replaceChildren(...payload.filesystem.events.map((item) => {
                const event = document.createElement("li");
                const operation = document.createElement("strong");
                operation.textContent = item.operation;
                event.append(operation, ` ${item.message}`);
                return event;
              }));
              if (payload.error) errorNode.textContent = payload.error;
            }
          </script>
        </body>
        </html>
      HTML
    end
  end
end

if $PROGRAM_NAME == __FILE__
  host = ENV.fetch("AGENT_LOOP_HOST", "127.0.0.1")
  port = Integer(ENV.fetch("AGENT_LOOP_PORT", "9292"))
  database_url = ENV.fetch("DURABABBLE_DATABASE_URL", Durababble.default_database_url)
  schema = ENV.fetch("DURABABBLE_SCHEMA") do
    Durababble.workspace_schema(__dir__, prefix: "durababble_agent_loop")
  end
  server = AgentLoopExample::Server.new(host:, port:, database_url:, schema:)
  server.run
end
