#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

settings_path="${CLAUDE_SETTINGS_PATH:-$HOME/.claude/settings.json}"

json_value() {
  local key="$1"
  if [[ -r "$settings_path" ]]; then
    ruby -rjson -e '
      settings = JSON.parse(File.read(ARGV.fetch(0)))
      value = ARGV.fetch(1).split(".").reduce(settings) { |object, key| object.is_a?(Hash) ? object[key] : nil }
      print value.to_s unless value.nil?
    ' "$settings_path" "$key"
  fi
}

if [[ -z "${ANTHROPIC_BASE_URL:-}" ]]; then
  value="$(json_value "env.ANTHROPIC_BASE_URL")"
  [[ -n "$value" ]] && export ANTHROPIC_BASE_URL="$value"
fi

if [[ -z "${ANTHROPIC_CUSTOM_HEADERS:-}" ]]; then
  value="$(json_value "env.ANTHROPIC_CUSTOM_HEADERS")"
  [[ -n "$value" ]] && export ANTHROPIC_CUSTOM_HEADERS="$value"
fi

if [[ -z "${ANTHROPIC_MODEL:-}" ]]; then
  export ANTHROPIC_MODEL="claude-haiku-4-5-20251001"
fi

if [[ -z "${ANTHROPIC_CLAUDE_CODE_VERSION:-}" ]] && command -v claude >/dev/null 2>&1; then
  value="$(claude --version 2>/dev/null || true)"
  value="${value%% *}"
  [[ -n "$value" ]] && export ANTHROPIC_CLAUDE_CODE_VERSION="$value"
fi

if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
  helper="${ANTHROPIC_API_KEY_HELPER:-$(json_value "apiKeyHelper")}"
  if [[ -z "$helper" ]]; then
    echo "ANTHROPIC_API_KEY is not set and no apiKeyHelper was found in $settings_path" >&2
    exit 1
  fi

  token="$(/bin/sh -lc "$helper")"
  if [[ -z "$token" ]]; then
    echo "apiKeyHelper did not return a token" >&2
    exit 1
  fi
  export ANTHROPIC_API_KEY="$token"
fi

if [[ -z "${DURABABBLE_DATABASE_URL:-}" ]]; then
  export DURABABBLE_DATABASE_URL="$(
    ruby -ruri -e '
      user = URI.encode_www_form_component(ENV.fetch("DURABABBLE_MYSQL_USERNAME", ENV.fetch("MYSQL_USER", "root")))
      password = ENV.fetch("DURABABBLE_MYSQL_PASSWORD", ENV.fetch("MYSQL_PASSWORD", nil))
      password = nil if password.to_s.empty?
      host = ENV.fetch("DURABABBLE_MYSQL_HOST", ENV.fetch("MYSQL_HOST", "127.0.0.1"))
      port = ENV.fetch("DURABABBLE_MYSQL_PORT", ENV.fetch("MYSQL_PORT", "3306"))
      database = ENV.fetch("DURABABBLE_MYSQL_DATABASE", "sidekick_server_test")
      credentials = password ? "#{user}:#{URI.encode_www_form_component(password)}" : user
      print "mysql://#{credentials}@#{host}:#{port}/#{database}"
    '
  )"
fi

exec mise exec -- bundle exec ruby examples/agent-loop/server.rb
