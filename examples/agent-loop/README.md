# Agent Loop Example

This example shows a durable workflow running a real Anthropic Messages API tool-calling loop while a `Durababble::DurableObject` owns the virtual file system state.

The workflow calls the model with raw `Net::HTTP` requests, sends tool definitions for `list_files`, `read_file`, `write_file`, `append_file`, `replace_text`, and `delete_file`, executes each returned tool use through durable object RPCs, appends `tool_result` blocks back into the message history, and keeps going until the model ends the turn or `max_turns` is reached.

Start with `agent_loop.rb` when reading the example. It contains the tool definitions, the `VirtualFileSystem` durable object, and the `AgentLoopWorkflow`. `anthropic_client.rb` is kept separate because most of it is provider-specific HTTP/auth setup. `server.rb` is only the local API and static web UI wrapper.

Run the web UI with the Claude Code-compatible launcher:

```sh
examples/agent-loop/run-server.sh
```

The launcher reads `~/.claude/settings.json`, exports `env.ANTHROPIC_BASE_URL` and `env.ANTHROPIC_CUSTOM_HEADERS` when present, runs `apiKeyHelper` when `ANTHROPIC_API_KEY` is not already set, exports the helper output as `ANTHROPIC_API_KEY`, and starts the server without printing the token. It does not reuse Claude Code's `model` setting because local aliases such as `opus[1m]` are not Messages API model ids; set `ANTHROPIC_MODEL` explicitly to override the default Haiku model.

When `ANTHROPIC_BASE_URL` points at the local Claude Code gateway (`anthropic-claude-code`), the raw HTTP client also mirrors Claude Code's gateway request shape: `?beta=true`, `Authorization: Bearer ...` plus `x-api-key`, the Claude Code session-affinity headers, request metadata, and the Claude Code billing marker system block. The client sends adaptive thinking, context management, and `output_config.effort` for non-Haiku models, but leaves those fields out for Haiku because the gateway rejects them on Haiku.

You can also run it manually:

```sh
mise exec -- env \
  DURABABBLE_DATABASE_URL=mysql://root@127.0.0.1:3306/sidekick_server_test \
  ANTHROPIC_API_KEY=... \
  ANTHROPIC_MODEL=claude-haiku-4-5-20251001 \
  bundle exec ruby examples/agent-loop/server.rb
```

Useful environment variables:

```sh
ANTHROPIC_API_KEY=...
ANTHROPIC_BASE_URL=https://api.anthropic.com
ANTHROPIC_CUSTOM_HEADERS='Header-Name: value'
ANTHROPIC_MODEL=claude-haiku-4-5-20251001
ANTHROPIC_MAX_TOKENS=2048
ANTHROPIC_CLAUDE_CODE_VERSION=2.1.143
ANTHROPIC_CLAUDE_CODE_COMPAT=0
AGENT_LOOP_PORT=9292
```

The web UI submits a long natural-language request, polls the workflow while it runs, and renders the durable file-system contents and event log as model calls and object RPCs complete.
