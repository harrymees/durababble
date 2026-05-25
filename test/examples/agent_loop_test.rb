# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../examples/agent-loop/agent_loop"

class AgentLoopExampleTest < DurababbleTestCase
  TERMINAL_STATUSES = ["completed", "failed", "canceled"].freeze

  class FakeAnthropicClient
    attr_reader :requests

    def initialize(responses)
      @responses = responses
      @requests = []
    end

    def model_name
      "fake-claude"
    end

    def create_message(system:, messages:, tools:, max_tokens: nil)
      @requests << JSON.parse(JSON.generate(
        "system" => system,
        "messages" => messages,
        "tools" => tools,
        "max_tokens" => max_tokens,
      ))
      response = @responses.shift
      raise "no fake Anthropic response queued" unless response

      JSON.parse(JSON.generate(response))
    end
  end

  durababble_store_backends.each do |backend|
    test "virtual file system persists file commands with #{backend.name}" do
      with_durababble_store(backend, "agent_loop_vfs") do |store|
        worker = Durababble::Worker.new(
          store:,
          workflows: {},
          objects: [AgentLoopExample::VirtualFileSystem],
          worker_id: "agent-loop-object-test",
          migrate: false,
        )

        AgentLoopExample::VirtualFileSystem.tell("session-1", :write_file, "/notes.txt", "Hello", store:)
        AgentLoopExample::VirtualFileSystem.tell("session-1", :append_file, "/notes.txt", " world", store:)
        AgentLoopExample::VirtualFileSystem.tell("session-1", :replace_text, "/notes.txt", "Hello", "Hi", store:)

        assert_equal 1, worker.run_until_idle
        filesystem = AgentLoopExample::VirtualFileSystem.at("session-1", store:)
        assert_equal "Hi world", filesystem.read_file("/notes.txt")
        assert_equal 3, filesystem.snapshot.fetch("files").fetch("/notes.txt").fetch("revision")
        assert_equal ["write_file", "append_file", "replace_text"], filesystem.snapshot.fetch("events").map { |event| event.fetch("operation") }
      end
    end

    test "agent loop workflow drives Anthropic-style tool calls through object RPCs with #{backend.name}" do
      with_durababble_store(backend, "agent_loop_workflow") do |store|
        fake_client = FakeAnthropicClient.new(
          [
            tool_response("toolu_list", "list_files", {}),
            tool_response("toolu_write", "write_file", { "path" => "/notes.txt", "content" => "Hello" }),
            tool_response("toolu_append", "append_file", { "path" => "/notes.txt", "content" => " world" }),
            tool_response("toolu_replace", "replace_text", { "path" => "/notes.txt", "before" => "Hello", "after" => "Hi" }),
            final_response("Done. Updated /notes.txt."),
          ],
        )
        AgentLoopExample.configure(database_url: backend.database_url, schema:, llm_client: fake_client)
        workflow_id = store.enqueue_workflow(
          name: AgentLoopExample::AgentLoopWorkflow.workflow_name,
          input: {
            "session_id" => "session-2",
            "request" => "Create and revise a note.",
            "max_turns" => 8,
          },
        )

        row = run_agent_loop_workers(backend, workflow_id)
        file = row.fetch("result").fetch("files").fetch("/notes.txt")
        assert_equal("completed", row.fetch("status"))
        assert_equal("Hi world", file.fetch("content"))
        assert_equal(3, file.fetch("revision"))
        assert_equal("end_turn", row.fetch("result").fetch("stop_reason"))

        assert_equal(5, fake_client.requests.length)
        assert_includes(fake_client.requests.first.fetch("system"), "durable file-system agent")
        tool_names = fake_client.requests.first.fetch("tools").map { |tool| tool.fetch("name") }
        assert_equal(["list_files", "read_file", "write_file", "append_file", "replace_text", "delete_file"], tool_names)
        first_tool_result = fake_client.requests.fetch(1).fetch("messages").last.fetch("content").first
        assert_equal("tool_result", first_tool_result.fetch("type"))
        assert_equal("toolu_list", first_tool_result.fetch("tool_use_id"))

        messages = store.inbox_messages_for(
          target_kind: "object",
          target_type: AgentLoopExample::VirtualFileSystem.object_type,
          target_id: "session-2",
        )
        method_names = messages.map { |message| message.fetch("method_name") }
        assert_includes(method_names, "write_file")
        assert_includes(method_names, "append_file")
        assert_includes(method_names, "replace_text")
        assert_operator(method_names.count("log"), :>=, 6)
        assert(messages.all? { |message| message.fetch("status") == "completed" })
        assert(messages.all? { |message| message.fetch("idempotency_key").start_with?("durababble:v1:workflow:#{workflow_id}:step:") })
      ensure
        AgentLoopExample.reset_configuration!
      end
    end
  end

  test "anthropic client mirrors Claude Code gateway request shape" do
    server = TCPServer.new("127.0.0.1", 0)
    captured = Queue.new
    server_thread = Thread.new do
      socket = server.accept
      request_line = socket.gets&.strip
      headers = read_http_headers(socket)
      body = socket.read(headers.fetch("content-length", "0").to_i)
      captured << {
        "request_line" => request_line,
        "headers" => headers,
        "body" => JSON.parse(body),
      }
      response_body = JSON.generate(final_response("ok"))
      socket.write("HTTP/1.1 200 OK\r\n")
      socket.write("Content-Type: application/json\r\n")
      socket.write("Content-Length: #{response_body.bytesize}\r\n")
      socket.write("Connection: close\r\n")
      socket.write("\r\n")
      socket.write(response_body)
    ensure
      socket&.close
    end

    with_env(
      "ANTHROPIC_CLAUDE_CODE_MAX_TOKENS" => "64000",
      "ANTHROPIC_CLAUDE_CODE_VERSION" => "2.1.143",
      "ANTHROPIC_EFFORT" => nil,
    ) do
      client = AgentLoopExample::AnthropicClient.new(
        api_key: "helper-token",
        base_url: "http://127.0.0.1:#{server.addr.fetch(1)}/vendors/anthropic-claude-code",
        custom_headers: "Shopify-Usage-Tag: [\"claude_code_cli\"]\nX-Shopify-Session-Affinity-Header: X-Claude-Code-Session-Id",
        model: "claude-haiku-4-5-20251001",
        open_timeout: 1,
        read_timeout: 3,
      )

      response = client.create_message(
        system: "system prompt",
        messages: [{ "role" => "user", "content" => [{ "type" => "text", "text" => "hi" }] }],
        tools: AgentLoopExample.tool_definitions,
      )
      assert_equal("msg_final", response.fetch("id"))
    end

    request = captured.pop
    headers = request.fetch("headers")
    body = request.fetch("body")
    session_id = headers.fetch("x-claude-code-session-id")

    assert_equal("POST /vendors/anthropic-claude-code/v1/messages?beta=true HTTP/1.1", request.fetch("request_line"))
    assert_equal("helper-token", headers.fetch("x-api-key"))
    assert_equal("Bearer helper-token", headers.fetch("authorization"))
    assert_includes(headers.fetch("anthropic-beta"), "claude-code-20250219")
    assert_includes(headers.fetch("anthropic-beta"), "structured-outputs-2025-12-15")
    assert_equal("true", headers.fetch("anthropic-dangerous-direct-browser-access"))
    assert_equal("cli", headers.fetch("x-app"))
    assert_equal("claude-cli/2.1.143 (external, sdk-cli)", headers.fetch("user-agent"))
    assert_equal("X-Claude-Code-Session-Id", headers.fetch("x-shopify-session-affinity-header"))
    assert_equal("[\"claude_code_cli\"]", headers.fetch("shopify-usage-tag"))
    assert_equal(session_id, JSON.parse(headers.fetch("shopify-request-context")).fetch("dev_invocation_id"))

    user_id = JSON.parse(body.fetch("metadata").fetch("user_id"))
    assert_equal(session_id, user_id.fetch("session_id"))
    assert_equal("claude-haiku-4-5-20251001", body.fetch("model"))
    assert_equal(64_000, body.fetch("max_tokens"))
    refute_includes(body.keys, "thinking")
    refute_includes(body.keys, "context_management")
    refute_includes(body.keys, "output_config")
    system_blocks = body.fetch("system")
    assert_match(/\Ax-anthropic-billing-header: cc_version=2\.1\.143; cc_entrypoint=sdk-cli; cch=[0-9a-f]{5};\z/, system_blocks.first.fetch("text"))
    assert_equal("system prompt", system_blocks.last.fetch("text"))
  ensure
    server&.close
    server_thread&.join(1)
    server_thread&.kill if server_thread&.alive?
  end

  private

  def read_http_headers(socket)
    headers = {}
    while (line = socket.gets)
      line = line.chomp
      break if line.empty?

      key, value = line.split(":", 2)
      headers[key.downcase] = value.to_s.strip
    end
    headers
  end

  def with_env(values)
    previous = values.each_with_object({}) { |(key, _), memo| memo[key] = ENV.fetch(key, nil) }
    values.each { |key, value| ENV[key] = value }
    yield
  ensure
    previous.each { |key, value| value.nil? ? ENV.delete(key) : ENV[key] = value }
  end

  def tool_response(id, name, input)
    {
      "id" => "msg_#{id}",
      "type" => "message",
      "role" => "assistant",
      "stop_reason" => "tool_use",
      "content" => [
        { "type" => "text", "text" => "Calling #{name}." },
        { "type" => "tool_use", "id" => id, "name" => name, "input" => input },
      ],
    }
  end

  def final_response(text)
    {
      "id" => "msg_final",
      "type" => "message",
      "role" => "assistant",
      "stop_reason" => "end_turn",
      "content" => [{ "type" => "text", "text" => text }],
    }
  end

  def run_agent_loop_workers(backend, workflow_id, timeout: 10)
    workflow_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
    object_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
    workflow_worker = Durababble::Worker.new(
      store: workflow_store,
      workflows: [AgentLoopExample::AgentLoopWorkflow],
      objects: [],
      worker_id: "agent-loop-workflow-test",
      migrate: false,
    )
    object_worker = Durababble::Worker.new(
      store: object_store,
      workflows: {},
      objects: [AgentLoopExample::VirtualFileSystem],
      worker_id: "agent-loop-object-test",
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
      workflow_worker.tick
    rescue StandardError => e
      errors << e
    end

    deadline = Time.now + timeout
    loop do
      raise errors.pop unless errors.empty?

      row = store.workflow(workflow_id)
      return row if TERMINAL_STATUSES.include?(row.fetch("status"))
      raise "agent loop workflow did not finish before timeout" if Time.now >= deadline

      sleep(0.02)
    end
  ensure
    stop = true
    workflow_thread&.join(1)
    object_thread&.join(1)
    workflow_thread&.kill if workflow_thread&.alive?
    object_thread&.kill if object_thread&.alive?
    workflow_store&.close
    object_store&.close
  end
end
