# typed: false
# frozen_string_literal: true

require "json"
require "active_support/isolated_execution_state"

require_relative "../../lib/durababble"
require_relative "anthropic_client"

# Durababble requires :fiber isolation so each reactor fiber checks out its own
# ActiveRecord connection. In a Rails+Falcon host the Falcon Railtie sets this
# defensively; standalone scripts like this one set it explicitly.
ActiveSupport::IsolatedExecutionState.isolation_level = :fiber

module AgentLoopExample
  # These are the tools the model can call. Each tool is implemented below by
  # sending an RPC to the VirtualFileSystem durable object.
  TOOL_DEFINITIONS = [
    {
      "name" => "list_files",
      "description" => "List the files currently stored in the durable virtual file system.",
      "input_schema" => {
        "type" => "object",
        "properties" => {},
        "required" => [],
      },
    },
    {
      "name" => "read_file",
      "description" => "Read one file from the durable virtual file system.",
      "input_schema" => {
        "type" => "object",
        "properties" => {
          "path" => { "type" => "string", "description" => "The absolute or relative file path to read." },
        },
        "required" => ["path"],
      },
    },
    {
      "name" => "write_file",
      "description" => "Write a complete file, creating it if necessary and replacing existing content if it already exists.",
      "input_schema" => {
        "type" => "object",
        "properties" => {
          "path" => { "type" => "string", "description" => "The absolute or relative file path to write." },
          "content" => { "type" => "string", "description" => "The complete file content." },
        },
        "required" => ["path", "content"],
      },
    },
    {
      "name" => "append_file",
      "description" => "Append text to the end of an existing or new file.",
      "input_schema" => {
        "type" => "object",
        "properties" => {
          "path" => { "type" => "string", "description" => "The absolute or relative file path to append to." },
          "content" => { "type" => "string", "description" => "The text to append." },
        },
        "required" => ["path", "content"],
      },
    },
    {
      "name" => "replace_text",
      "description" => "Replace the first occurrence of text in an existing file.",
      "input_schema" => {
        "type" => "object",
        "properties" => {
          "path" => { "type" => "string", "description" => "The absolute or relative file path to edit." },
          "before" => { "type" => "string", "description" => "The existing text to replace." },
          "after" => { "type" => "string", "description" => "The replacement text." },
        },
        "required" => ["path", "before", "after"],
      },
    },
    {
      "name" => "delete_file",
      "description" => "Delete a file from the durable virtual file system.",
      "input_schema" => {
        "type" => "object",
        "properties" => {
          "path" => { "type" => "string", "description" => "The absolute or relative file path to delete." },
        },
        "required" => ["path"],
      },
    },
  ].freeze

  SYSTEM_PROMPT = <<~PROMPT
    You are a durable file-system agent running inside a Durababble workflow.
    Use the provided tools to inspect, create, read, edit, and delete files in the virtual file system.
    Start by calling list_files unless the user explicitly asks for only a direct answer.
    For multi-step requests, take multiple tool-calling turns and wait for tool results before continuing.
    When the request is complete, stop calling tools and respond with a concise summary of the files changed.
  PROMPT

  class << self
    attr_writer :llm_client

    def configure(database_url: nil, schema: nil, llm_client: nil)
      @database_url = database_url
      @schema = schema
      @llm_client = llm_client if llm_client
    end

    def reset_configuration!
      @database_url = nil
      @schema = nil
      @llm_client = nil
    end

    def llm_client
      @llm_client ||= AnthropicClient.new
    end

    def tool_definitions
      JSON.parse(JSON.generate(TOOL_DEFINITIONS))
    end

    def with_store
      if @database_url
        store = Durababble::Store.connect(database_url: @database_url, schema: @schema || Durababble.default_schema)
        return yield store
      end

      yield Durababble.store
    ensure
      store&.close
    end
  end

  class VirtualFileSystem < Durababble::DurableObject
    object_type "agent_loop_virtual_file_system"

    def initialize_state
      { "files" => {}, "events" => [] }
    end

    expose_command def log(message, metadata = {})
      event = build_event("log", nil, message.to_s, metadata)
      update_state(current_state.merge("events" => current_state.fetch("events") + [event]))
      event
    end

    expose_command def write_file(path, content)
      path = normalize_path(path)
      files = current_state.fetch("files").dup
      revision = next_revision(files[path])
      files[path] = { "content" => content.to_s, "revision" => revision }
      event = build_event("write_file", path, "Wrote #{path}", "revision" => revision)
      update_state(current_state.merge("files" => files, "events" => current_state.fetch("events") + [event]))
      event
    end

    expose_command def append_file(path, content)
      path = normalize_path(path)
      files = current_state.fetch("files").dup
      existing = files[path] || { "content" => "", "revision" => 0 }
      revision = next_revision(existing)
      files[path] = { "content" => "#{existing.fetch("content")}#{content}", "revision" => revision }
      event = build_event("append_file", path, "Appended to #{path}", "revision" => revision)
      update_state(current_state.merge("files" => files, "events" => current_state.fetch("events") + [event]))
      event
    end

    expose_command def replace_text(path, before, after)
      path = normalize_path(path)
      before = before.to_s
      raise ArgumentError, "replacement text cannot be empty" if before.empty?

      files = current_state.fetch("files").dup
      existing = files.fetch(path) { raise KeyError, "file not found: #{path}" }
      content = existing.fetch("content")
      raise ArgumentError, "#{path} does not include #{before.inspect}" unless content.include?(before)

      revision = next_revision(existing)
      files[path] = { "content" => content.sub(before, after.to_s), "revision" => revision }
      event = build_event("replace_text", path, "Edited #{path}", "revision" => revision, "before" => before)
      update_state(current_state.merge("files" => files, "events" => current_state.fetch("events") + [event]))
      event
    end

    expose_command def delete_file(path)
      path = normalize_path(path)
      files = current_state.fetch("files").dup
      files.delete(path) { raise KeyError, "file not found: #{path}" }
      event = build_event("delete_file", path, "Deleted #{path}")
      update_state(current_state.merge("files" => files, "events" => current_state.fetch("events") + [event]))
      event
    end

    expose def read_file(path)
      current_state.fetch("files").fetch(normalize_path(path)).fetch("content")
    end

    expose def snapshot
      {
        "files" => current_state.fetch("files").sort.to_h,
        "events" => current_state.fetch("events"),
      }
    end

    private

    def normalize_path(path)
      path = path.to_s.strip
      raise ArgumentError, "path cannot be empty" if path.empty?

      path = "/#{path}" unless path.start_with?("/")
      parts = path.split("/").reject(&:empty?)
      raise ArgumentError, "path cannot contain .." if parts.any? { |part| part == ".." }

      "/#{parts.join("/")}"
    end

    def next_revision(file)
      file ? file.fetch("revision").to_i + 1 : 1
    end

    def build_event(operation, path, message, metadata = {})
      {
        "id" => current_state.fetch("events").length + 1,
        "operation" => operation,
        "path" => path,
        "message" => message,
        "metadata" => metadata,
        "operation_id" => command_context&.idempotency_key,
      }
    end
  end

  class AgentLoopWorkflow < Durababble::Workflow
    workflow_name "agent-loop"

    # The workflow owns the model conversation. Model calls and tool calls are
    # separate durable steps, so progress is persisted throughout a long turn.
    def execute(input)
      session_id = input.fetch("session_id")
      request = input.fetch("request").to_s
      max_turns = Integer(input.fetch("max_turns", 12))
      messages = initial_messages(request)
      transcript = [record_session_started(session_id, request, max_turns)]
      stop_reason = "max_turns"

      max_turns.times do |turn|
        response = call_model(session_id, messages, turn)
        content = response.fetch("content")
        transcript << {
          "role" => "assistant",
          "turn" => turn,
          "stop_reason" => response["stop_reason"],
          "content" => content,
        }
        messages << { "role" => "assistant", "content" => content }

        tool_uses = content.select { |block| block.fetch("type") == "tool_use" }
        if tool_uses.empty?
          stop_reason = response["stop_reason"] || "assistant_done"
          break
        end

        tool_results = tool_uses.each_with_index.map do |tool_use, index|
          result = apply_tool(session_id, turn, index, tool_use)
          transcript << {
            "role" => "tool",
            "turn" => turn,
            "tool_use_id" => tool_use.fetch("id"),
            "name" => tool_use.fetch("name"),
            "result" => result,
          }
          tool_result_block(tool_use.fetch("id"), result)
        end
        messages << { "role" => "user", "content" => tool_results }
      end

      finish_session(session_id, transcript, stop_reason)
    end

    step def record_session_started(session_id, request, max_turns)
      event = AgentLoopExample.with_store do |store|
        VirtualFileSystem.at(session_id, store:).log(
          "Agent request received",
          { "request" => request, "max_turns" => max_turns },
          idempotency_key: step_context.idempotency_key,
        )
      end
      { "role" => "system", "event" => event }
    end

    step def call_model(session_id, messages, turn)
      AgentLoopExample.with_store do |store|
        filesystem = VirtualFileSystem.at(session_id, store:)
        client = AgentLoopExample.llm_client
        model_name = client.respond_to?(:model_name) ? client.model_name : client.class.name
        filesystem.log(
          "Calling Anthropic model",
          { "turn" => turn, "model" => model_name, "message_count" => messages.length },
          idempotency_key: "#{step_context.idempotency_key}:model-request",
        )

        response = client.create_message(
          system: SYSTEM_PROMPT,
          messages: messages,
          tools: AgentLoopExample.tool_definitions,
        )
        normalized = normalize_model_response(response)
        tool_calls = normalized.fetch("content").select { |block| block.fetch("type") == "tool_use" }
        filesystem.log(
          "Model returned #{tool_calls.length} tool call(s)",
          {
            "turn" => turn,
            "stop_reason" => normalized["stop_reason"],
            "tool_calls" => tool_calls.map { |block| { "id" => block.fetch("id"), "name" => block.fetch("name") } },
          },
          idempotency_key: "#{step_context.idempotency_key}:model-response",
        )
        normalized
      rescue StandardError => e
        begin
          filesystem&.log(
            "Model call failed",
            { "turn" => turn, "error" => "#{e.class}: #{e.message}" },
            idempotency_key: "#{step_context.idempotency_key}:model-error",
          )
        rescue StandardError
          nil
        end
        raise
      end
    end

    step def apply_tool(session_id, turn, index, tool_use)
      tool_use = stringify_keys(tool_use)
      name = tool_use.fetch("name")
      input = stringify_keys(tool_use.fetch("input", {}))
      tool_use_id = tool_use.fetch("id")
      idempotency_key = "#{step_context.idempotency_key}:#{tool_use_id}:#{index}"

      AgentLoopExample.with_store do |store|
        filesystem = VirtualFileSystem.at(session_id, store:)
        result = dispatch_tool(filesystem, name, input, idempotency_key)
        result.merge(
          "ok" => true,
          "tool" => name,
          "tool_use_id" => tool_use_id,
          "turn" => turn,
        )
      rescue StandardError => e
        log_tool_failure(filesystem, tool_use, turn, e, idempotency_key)
        {
          "ok" => false,
          "tool" => name,
          "tool_use_id" => tool_use_id,
          "turn" => turn,
          "error" => "#{e.class}: #{e.message}",
        }
      end
    end

    step def finish_session(session_id, transcript, stop_reason)
      snapshot = AgentLoopExample.with_store do |store|
        filesystem = VirtualFileSystem.at(session_id, store:)
        filesystem.log(
          "Agent turn finished",
          { "stop_reason" => stop_reason },
          idempotency_key: step_context.idempotency_key,
        )
        filesystem.snapshot
      end
      {
        "session_id" => session_id,
        "status" => "finished",
        "stop_reason" => stop_reason,
        "turns" => transcript.count { |entry| entry.fetch("role") == "assistant" },
        "files" => snapshot.fetch("files"),
        "events" => snapshot.fetch("events"),
        "transcript" => transcript,
      }
    end

    private

    def initial_messages(request)
      [
        {
          "role" => "user",
          "content" => [
            {
              "type" => "text",
              "text" => <<~TEXT,
                Run a durable agent turn for this request:

                #{request}
              TEXT
            },
          ],
        },
      ]
    end

    def normalize_model_response(response)
      response = stringify_keys(response)
      content = Array(response.fetch("content")).map { |block| stringify_keys(block) }
      response.merge("content" => content)
    end

    def tool_result_block(tool_use_id, result)
      block = {
        "type" => "tool_result",
        "tool_use_id" => tool_use_id,
        "content" => JSON.pretty_generate(result),
      }
      block["is_error"] = true unless result.fetch("ok")
      block
    end

    def dispatch_tool(filesystem, name, input, idempotency_key)
      case name
      when "list_files"
        snapshot = filesystem.snapshot
        event = filesystem.log(
          "Listed files",
          { "paths" => snapshot.fetch("files").keys },
          idempotency_key: "#{idempotency_key}:list",
        )
        { "event" => event, "files" => snapshot.fetch("files") }
      when "read_file"
        read_file(filesystem, input, idempotency_key)
      when "write_file"
        filesystem.write_file(input.fetch("path"), input.fetch("content"), idempotency_key:)
      when "append_file"
        filesystem.append_file(input.fetch("path"), input.fetch("content"), idempotency_key:)
      when "replace_text"
        filesystem.replace_text(input.fetch("path"), input.fetch("before"), input.fetch("after"), idempotency_key:)
      when "delete_file"
        filesystem.delete_file(input.fetch("path"), idempotency_key:)
      else
        raise ArgumentError, "unknown tool #{name.inspect}"
      end
    end

    def read_file(filesystem, input, idempotency_key)
      path = input.fetch("path")
      content = filesystem.read_file(path)
      event = filesystem.log(
        "Read #{path}",
        { "path" => path, "bytes" => content.bytesize },
        idempotency_key: "#{idempotency_key}:read",
      )
      { "event" => event, "content" => content }
    end

    def log_tool_failure(filesystem, tool_use, turn, error, idempotency_key)
      filesystem&.log(
        "Tool #{tool_use.fetch("name")} failed",
        {
          "turn" => turn,
          "tool_use_id" => tool_use.fetch("id"),
          "error" => "#{error.class}: #{error.message}",
        },
        idempotency_key: "#{idempotency_key}:failure",
      )
    rescue StandardError
      nil
    end

    def stringify_keys(value)
      case value
      when Hash
        value.to_h { |key, nested| [key.to_s, stringify_keys(nested)] }
      when Array
        value.map { |nested| stringify_keys(nested) }
      else
        value
      end
    end
  end
end
