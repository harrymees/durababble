---
title: "Build A Durable Agent Loop"
linkTitle: "Agent Loop"
weight: 10
---

# Build A Durable Agent Loop

This tutorial walks through building a small LLM agent whose conversation, tool calls, and file-system state all survive process restarts. By the end you will have a workflow that drives a tool-calling loop against the Anthropic Messages API and a durable object that owns a virtual file system the agent edits across turns.

The finished version lives in [`examples/agent-loop`](https://github.com/harrymees/durababble-gamma/tree/main/examples/agent-loop) — open that directory in another tab if you want to compare while you read.

## Why An Agent Loop Is Workflow-Shaped

A tool-calling agent alternates between two side effects: ask the model what to do next, then run the tools the model asked for. A long request can take many turns — list the files, read three of them, write two new ones, run a search, summarize. If the process dies halfway through, you do not want to re-prompt the model from scratch and you definitely do not want to re-run the tools that already ran. That is exactly what `step def` was designed for.

The state the agent edits — files, anything that should persist between turns and be visible to other parts of the system — belongs in a durable object addressed by session id. Workflows describe the process; objects own the state. Putting them together is the whole point of the example.

## What We Are Building

A Ruby file with three things in it:

1. `VirtualFileSystem`, a `Durababble::DurableObject` that stores files keyed by session id.
2. A tiny `AnthropicClient` that posts to `/v1/messages` and returns the parsed response.
3. `AgentLoopWorkflow`, a `Durababble::Workflow` that drives the loop: call the model, dispatch any tool calls to the durable object, hand the tool results back to the model, repeat until the model stops asking for tools.

Create `agent_loop.rb` next to your application code and follow along.

## Prerequisites

You should already have Durababble installed and a database the store can connect to. If not, follow [Installation](../install.md) first. You also need an Anthropic API key in `ANTHROPIC_API_KEY`.

```ruby
require "durababble"
require "json"
require "net/http"
require "uri"

Durababble.configure(database_url: Durababble.default_database_url)
store = Durababble.store
store.migrate!
```

## Step 1: A Durable Virtual File System

The durable object owns the agent's working state. Every tool call goes through it, so the file contents persist between turns and across worker restarts. We expose mutating tools as `expose_command` so they are written through the durable mailbox, and reads as `expose` for cheap RPCs.

```ruby
class VirtualFileSystem < Durababble::DurableObject
  object_type "agent_loop_virtual_file_system"

  def initialize_state
    { "files" => {} }
  end

  expose_command def write_file(path, content)
    files = current_state.fetch("files").dup
    files[path] = { "content" => content.to_s }
    update_state(current_state.merge("files" => files))
  end

  expose_command def delete_file(path)
    files = current_state.fetch("files").dup
    files.delete(path) { raise KeyError, "file not found: #{path}" }
    update_state(current_state.merge("files" => files))
  end

  expose def read_file(path)
    current_state.fetch("files").fetch(path).fetch("content")
  end

  expose def files
    current_state.fetch("files")
  end
end
```

We use the agent's session id as the object id, so every call to `VirtualFileSystem.at(session_id)` lands on the same persisted state regardless of which worker holds the lease. The real example adds `append_file` and `replace_text` for richer editing — they follow the same pattern.

## Step 2: A Minimal Anthropic Client

The agent loop is provider-agnostic in shape, but we need something concrete that returns the tool-call structure the loop will dispatch. This is just an HTTP wrapper; nothing about it is Durababble-specific.

```ruby
class AnthropicClient
  ENDPOINT = URI("https://api.anthropic.com/v1/messages")
  MODEL = ENV.fetch("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")

  def create_message(system:, messages:, tools:)
    request = Net::HTTP::Post.new(ENDPOINT)
    request["content-type"] = "application/json"
    request["x-api-key"] = ENV.fetch("ANTHROPIC_API_KEY")
    request["anthropic-version"] = "2023-06-01"
    request.body = JSON.generate(
      "model" => MODEL,
      "max_tokens" => 2_048,
      "system" => system,
      "messages" => messages,
      "tools" => tools,
    )

    response = Net::HTTP.start(ENDPOINT.host, ENDPOINT.port, use_ssl: true) { |http| http.request(request) }
    JSON.parse(response.body)
  end
end
```

The response contains a `content` array of blocks. Some blocks are `text`, some are `tool_use`. Our loop only cares about `tool_use` blocks.

## Step 3: Define The Tools

Anthropic's tool-calling API needs a JSON schema for every tool the model is allowed to call. We declare three for this tutorial — the real example adds three more, but the loop does not care how many there are.

```ruby
TOOL_DEFINITIONS = [
  {
    "name" => "list_files",
    "description" => "List the files currently stored in the durable virtual file system.",
    "input_schema" => { "type" => "object", "properties" => {}, "required" => [] },
  },
  {
    "name" => "read_file",
    "description" => "Read one file from the durable virtual file system.",
    "input_schema" => {
      "type" => "object",
      "properties" => { "path" => { "type" => "string" } },
      "required" => ["path"],
    },
  },
  {
    "name" => "write_file",
    "description" => "Write a complete file, creating it or overwriting it.",
    "input_schema" => {
      "type" => "object",
      "properties" => {
        "path" => { "type" => "string" },
        "content" => { "type" => "string" },
      },
      "required" => ["path", "content"],
    },
  },
].freeze

SYSTEM_PROMPT = <<~PROMPT
  You are a file-system agent.
  Use the provided tools to inspect, create, and edit files in the virtual file system in accordance with the user's requests.
  When the request is complete, stop calling tools and respond with a short summary.
PROMPT
```

## Step 4: The Agent Loop Workflow

Now the interesting piece. `execute` is the outer loop: it owns the message history but does no durable work itself. Every side effect — calling the model, dispatching a tool call — is its own `step def`. That is what makes the loop replay-safe.

```ruby
class AgentLoopWorkflow < Durababble::Workflow
  workflow_name "agent-loop"

  def execute(input)
    session_id = input.fetch("session_id")
    request = input.fetch("request").to_s
    max_turns = Integer(input.fetch("max_turns", 12))
    messages = [{ "role" => "user", "content" => [{ "type" => "text", "text" => request }] }]
    stop_reason = "max_turns"

    max_turns.times do |turn|
      response = call_model(messages, turn)
      content = response.fetch("content")
      messages << { "role" => "assistant", "content" => content }

      tool_uses = content.select { |block| block.fetch("type") == "tool_use" }
      if tool_uses.empty?
        stop_reason = response["stop_reason"] || "assistant_done"
        break
      end

      tool_results = tool_uses.each_with_index.map do |tool_use, index|
        result = apply_tool(session_id, turn, index, tool_use)
        {
          "type" => "tool_result",
          "tool_use_id" => tool_use.fetch("id"),
          "content" => JSON.generate(result),
          "is_error" => !result.fetch("ok"),
        }
      end
      messages << { "role" => "user", "content" => tool_results }
    end

    stop_reason
  end

  step def call_model(messages, _turn)
    AnthropicClient.new.create_message(
      system: SYSTEM_PROMPT,
      messages: messages,
      tools: TOOL_DEFINITIONS,
    )
  end

  step def apply_tool(session_id, turn, index, tool_use)
    name = tool_use.fetch("name")
    input = tool_use.fetch("input", {})
    tool_use_id = tool_use.fetch("id")
    idempotency_key = "#{step_context.idempotency_key}:#{tool_use_id}:#{index}"
    filesystem = VirtualFileSystem.at(session_id)

    result =
      case name
      when "list_files"
        filesystem.files
      when "read_file"
        filesystem.read_file(input.fetch("path"))
      when "write_file"
        filesystem.write_file(input.fetch("path"), input.fetch("content"), idempotency_key:)
      else
        raise ArgumentError, "unknown tool #{name.inspect}"
      end

    { "ok" => true, "tool" => name, "turn" => turn, "result" => result }
  rescue StandardError => e
    { "ok" => false, "tool" => name, "turn" => turn, "error" => "#{e.class}: #{e.message}" }
  end
end
```

A few things to notice:

- The message-history accumulation in `execute` is ordinary Ruby. It is not a step. That is fine: it has no side effects, so replay just rebuilds it.
- `call_model` is a step. The Anthropic POST is the side effect that must not repeat after a crash. Its return value is persisted in step history, so on replay the workflow gets the same model response back without making another HTTP call.
- `apply_tool` is a step. We pass an `idempotency_key` derived from the step context to the durable command so a retry of the same step lands as the same command in the object's mailbox instead of double-applying. Reads (`list_files`, `read_file`) go through `expose` queries — cheap in-cluster RPCs that bypass the mailbox entirely.

## Step 5: Running It

A worker process needs to know about both the workflow and the durable object. In a real app this lives in your worker boot code; here we run a worker just long enough to drive the workflow to completion.

```ruby
worker = Durababble::Worker.new(
  store:,
  workflows: [AgentLoopWorkflow],
  objects: [VirtualFileSystem],
  worker_id: "agent-loop-worker",
  migrate: false,
)

handle = AgentLoopWorkflow.start({
  "session_id" => "tutorial-session",
  "request" => "Write a README.md that describes a fictional project called 'Marlin', then list the files you wrote.",
})

worker.run_until_idle
puts JSON.pretty_generate(handle.result)
```

If you kill the process partway through and start it again, the workflow picks up where it left off — completed model calls and tool calls do not repeat, and the durable file system still contains everything the agent has written so far.

## Where To Go Next

The real [`examples/agent-loop`](https://github.com/harrymees/durababble-gamma/tree/main/examples/agent-loop) adds a small Rack web UI, the additional `append_file`, `replace_text`, and `delete_file` tools, and gateway-compatibility plumbing for the local Claude Code endpoint. The shape of the workflow and the durable object is identical to what you just built.

From here, useful directions:

- Replace `AnthropicClient` with a different provider. The loop only assumes a `content` array with `tool_use` blocks; swap providers without touching the workflow.
- Add a long-running review step. `wait_until` and workflow commands ([Workflows](../workflows.md)) let you pause the agent for human approval before it writes a file, with the rest of the conversation still persisted.
- Run many sessions in parallel. Each session is its own workflow handle and durable object instance; nothing in this code prevents you from running hundreds at once across a worker pool.
