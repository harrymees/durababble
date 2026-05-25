# typed: false
# frozen_string_literal: true

require "json"
require "net/http"
require "securerandom"
require "socket"
require "uri"

module AgentLoopExample
  # Provider-specific HTTP and auth plumbing. The Durababble workflow itself
  # lives in agent_loop.rb.
  class AnthropicClient
    DEFAULT_BASE_URL = "https://api.anthropic.com"
    DEFAULT_MODEL = "claude-haiku-4-5-20251001"
    DEFAULT_VERSION = "2023-06-01"
    DEFAULT_MAX_TOKENS = 2_048
    CLAUDE_CODE_BETA_HEADER = "claude-code-20250219,context-1m-2025-08-07,interleaved-thinking-2025-05-14,context-management-2025-06-27,prompt-caching-scope-2026-01-05,advisor-tool-2026-03-01,effort-2025-11-24,structured-outputs-2025-12-15"

    class Error < StandardError; end

    attr_reader :base_url, :model

    def initialize(
      api_key: ENV["ANTHROPIC_API_KEY"],
      base_url: ENV.fetch("ANTHROPIC_BASE_URL", DEFAULT_BASE_URL),
      custom_headers: ENV["ANTHROPIC_CUSTOM_HEADERS"],
      model: ENV.fetch("ANTHROPIC_MODEL", DEFAULT_MODEL),
      anthropic_version: ENV.fetch("ANTHROPIC_VERSION", DEFAULT_VERSION),
      max_tokens: Integer(ENV.fetch("ANTHROPIC_MAX_TOKENS", DEFAULT_MAX_TOKENS)),
      open_timeout: Float(ENV.fetch("ANTHROPIC_OPEN_TIMEOUT", 10)),
      read_timeout: Float(ENV.fetch("ANTHROPIC_READ_TIMEOUT", 120))
    )
      @api_key = api_key.to_s
      @base_url = base_url.to_s
      @custom_headers = parse_custom_headers(custom_headers)
      @model = model.to_s
      @anthropic_version = anthropic_version.to_s
      @max_tokens = max_tokens
      @open_timeout = open_timeout
      @read_timeout = read_timeout
      @session_id = SecureRandom.uuid
      @claude_code_gateway = claude_code_gateway?
      @claude_code_version = ENV.fetch("ANTHROPIC_CLAUDE_CODE_VERSION", "2.1.143").to_s
    end

    def model_name
      model
    end

    def create_message(system:, messages:, tools:, max_tokens: @max_tokens)
      raise Error, "ANTHROPIC_API_KEY is required; run examples/agent-loop/run-server.sh or export it directly" if @api_key.empty?

      uri = messages_uri
      request = Net::HTTP::Post.new(uri)
      request_headers.each { |key, value| request[key] = value }
      request.body = JSON.generate(message_body(
        "model" => model,
        "max_tokens" => max_tokens,
        "system" => system,
        "messages" => messages,
        "tools" => tools,
      ))

      response = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https", open_timeout: @open_timeout, read_timeout: @read_timeout) do |http|
        http.request(request)
      end
      parsed = parse_json(response.body)
      return parsed if response.is_a?(Net::HTTPSuccess)

      raise Error, "Anthropic request failed (#{response.code}): #{error_description(parsed, response.body)}"
    end

    private

    def messages_uri
      stripped = base_url.strip
      raise Error, "ANTHROPIC_BASE_URL cannot be empty" if stripped.empty?

      root = stripped.chomp("/")
      endpoint = root.end_with?("/v1") ? "#{root}/messages" : "#{root}/v1/messages"
      uri = URI(endpoint)
      uri.query = [uri.query, "beta=true"].compact.join("&") if @claude_code_gateway
      uri
    end

    def request_headers
      headers = {
        "content-type" => "application/json",
        "x-api-key" => @api_key,
        "anthropic-version" => @anthropic_version,
      }
      headers = headers.merge(claude_code_gateway_headers) if @claude_code_gateway
      headers.merge(@custom_headers)
    end

    def message_body(payload)
      return payload unless @claude_code_gateway

      body = payload.merge(
        "max_tokens" => Integer(ENV.fetch("ANTHROPIC_CLAUDE_CODE_MAX_TOKENS", 64_000)),
        "system" => claude_code_system(payload.fetch("system")),
        "metadata" => {
          "user_id" => JSON.generate(
            "device_id" => Socket.gethostname,
            "account_uuid" => "",
            "session_id" => @session_id,
          ),
        },
      )

      unless haiku_model?
        body["thinking"] = { "type" => "adaptive" }
        body["context_management"] = {
          "edits" => [
            {
              "type" => "clear_thinking_20251015",
              "keep" => "all",
            },
          ],
        }
      end

      effort = ENV.fetch("ANTHROPIC_EFFORT", "xhigh")
      body["output_config"] = { "effort" => effort } unless haiku_model? || effort.empty?
      body
    end

    def haiku_model?
      model.include?("haiku")
    end

    def claude_code_gateway_headers
      {
        "authorization" => "Bearer #{@api_key}",
        "anthropic-beta" => ENV.fetch("ANTHROPIC_BETA", CLAUDE_CODE_BETA_HEADER),
        "anthropic-dangerous-direct-browser-access" => "true",
        "shopify-request-context" => JSON.generate("dev_invocation_id" => @session_id),
        "user-agent" => "claude-cli/#{@claude_code_version} (external, sdk-cli)",
        "x-app" => "cli",
        "x-claude-code-session-id" => @session_id,
      }
    end

    def claude_code_system(system)
      [
        {
          "type" => "text",
          "text" => "x-anthropic-billing-header: cc_version=#{@claude_code_version}; cc_entrypoint=sdk-cli; cch=#{SecureRandom.hex(3)[0, 5]};",
        },
        {
          "type" => "text",
          "text" => system,
        },
      ]
    end

    def claude_code_gateway?
      return ENV["ANTHROPIC_CLAUDE_CODE_COMPAT"] != "0" if ENV.key?("ANTHROPIC_CLAUDE_CODE_COMPAT")

      base_url.include?("anthropic-claude-code")
    end

    def parse_custom_headers(raw_headers)
      raw_headers.to_s.each_line.with_object({}) do |line, headers|
        stripped = line.strip
        next if stripped.empty?

        name, value = stripped.split(":", 2)
        raise Error, "invalid ANTHROPIC_CUSTOM_HEADERS line: #{stripped.inspect}" if value.nil? || name.to_s.strip.empty?

        headers[name.strip] = value.strip
      end
    end

    def parse_json(body)
      JSON.parse(body.to_s)
    rescue JSON::ParserError => e
      raise Error, "Anthropic returned invalid JSON: #{e.message}"
    end

    def error_description(parsed, body)
      return body.to_s unless parsed.is_a?(Hash)

      error = parsed["error"]
      parts = []
      if error.is_a?(Hash)
        parts << error["type"]
        parts << error["message"]
      else
        parts << error
      end
      parts << "request_id=#{parsed["request_id"]}" if parsed["request_id"]
      parts.compact.reject(&:empty?).uniq.join(": ")
    end
  end
end
