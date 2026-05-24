# typed: true
# frozen_string_literal: true

require "time"
require "uri"

module Durababble
  class OperatorApp
    DEFAULT_LIMIT = 50
    WORKFLOW_DETAIL_KEYS = ["id", "name", "status", "locked_by", "locked_until", "next_run_at", "created_at", "updated_at"].freeze
    STEP_TABLE_KEYS = ["position", "name", "status", "started_at", "completed_at", "error"].freeze
    WAIT_TABLE_KEYS = ["position", "kind", "status", "event_key", "wake_at", "completed_at"].freeze
    OUTBOX_TABLE_KEYS = ["topic", "key", "status", "locked_by", "locked_until", "processed_at"].freeze
    COMMAND_TABLE_KEYS = ["method_name", "status", "locked_by", "locked_until", "created_at", "completed_at", "error"].freeze

    #: (?store: Store?, ?title: String) -> void
    def initialize(store: nil, title: "Durababble Operator")
      @store = store
      @title = title
    end

    #: (Hash[Object, Object] env) -> Array[Object]
    def call(env)
      request_method = env.fetch("REQUEST_METHOD", "GET").to_s
      return response(405, "Method Not Allowed", "text/plain; charset=utf-8") unless request_method == "GET"

      path = normalize_path(env.fetch("PATH_INFO", "/").to_s)
      script_name = env.fetch("SCRIPT_NAME", "").to_s
      query = parse_query(env.fetch("QUERY_STRING", "").to_s)
      store = resolved_store

      case path
      when "/", "/workflows"
        html(overview_page(store, script_name:, query:))
      when %r{\A/workflows/([^/]+)\z}
        workflow_id = decode_path_component(Regexp.last_match(1).to_s)
        html(workflow_page(store, workflow_id:, script_name:))
      when "/objects"
        html(objects_page(store, script_name:))
      when %r{\A/objects/([^/]+)/([^/]+)\z}
        object_type = decode_path_component(Regexp.last_match(1).to_s)
        object_id = decode_path_component(Regexp.last_match(2).to_s)
        html(object_page(store, object_type:, object_id:, script_name:))
      when "/health"
        response(200, "ok\n", "text/plain; charset=utf-8")
      else
        html(layout(title: "Not found", script_name:) { empty_state("Page not found", "No operator route exists for #{path}.") }, status: 404)
      end
    rescue Error => e
      html(
        layout(title: "Not configured", script_name: env.fetch("SCRIPT_NAME", "").to_s) do
          empty_state("Durababble store is not configured", e.message)
        end,
        status: 503,
      )
    rescue KeyError => e
      html(
        layout(title: "Not found", script_name: env.fetch("SCRIPT_NAME", "").to_s) do
          empty_state("Record not found", e.message)
        end,
        status: 404,
      )
    end

    private

    #: () -> Store
    def resolved_store
      store = @store || Durababble.store
      store.migrate! if store.respond_to?(:migrate!) && !@store
      store
    end

    #: (Store store, script_name: String, query: Hash[String, String]) -> String
    def overview_page(store, script_name:, query:)
      status = query["status"]
      workflows = store.list_workflows(status: blank?(status) ? nil : status, limit: DEFAULT_LIMIT)
      counts = workflows.each_with_object(Hash.new(0)) { |workflow, memo| memo[workflow.fetch("status").to_s] += 1 }

      layout(title: "Workflows", script_name:) do
        <<~HTML
          #{summary_grid(store:, workflows:, counts:)}
          <section class="surface">
            <div class="surface-header">
              <div>
                <h2>Workflows</h2>
                <p>Recent durable runs from persisted workflow rows.</p>
              </div>
              #{status_filter(script_name:, selected: status)}
            </div>
            #{workflow_table(workflows, script_name:)}
          </section>
        HTML
      end
    end

    #: (Store store, workflow_id: String, script_name: String) -> String
    def workflow_page(store, workflow_id:, script_name:)
      workflow = store.workflow(workflow_id)
      steps = store.steps_for(workflow_id)
      attempts = store.step_attempts_for(workflow_id)
      waits = store.waits_for(workflow_id)
      outbox = store.list_outbox_messages(workflow_id:, limit: DEFAULT_LIMIT)

      layout(title: "Workflow #{workflow_id}", script_name:) do
        <<~HTML
          <section class="hero-strip">
            <div>
              <a class="back-link" href="#{href(script_name, "/workflows")}">Workflows</a>
              <h1>#{h(workflow.fetch("name"))}</h1>
              <p class="mono">#{h(workflow.fetch("id"))}</p>
            </div>
            <div class="status-stack">
              #{badge(workflow.fetch("status"))}
              <span>Updated #{time_text(workflow["updated_at"])}</span>
            </div>
          </section>
          <section class="detail-grid">
            #{kv_panel("Workflow", workflow, WORKFLOW_DETAIL_KEYS)}
            #{payload_panel("Input", workflow["input"])}
            #{payload_panel("Result", workflow["result"])}
            #{error_panel(workflow["error"])}
          </section>
          <section class="surface">
            <div class="surface-header"><h2>Progress</h2><p>Deterministic step positions and latest state.</p></div>
            #{rows_table(steps, STEP_TABLE_KEYS)}
          </section>
          <section class="surface">
            <div class="surface-header"><h2>Attempts</h2><p>Append-only attempts, including retries and waits.</p></div>
            #{rows_table(attempts, STEP_TABLE_KEYS)}
          </section>
          <section class="surface">
            <div class="surface-header"><h2>Waits, Events, Timers</h2><p>Pending and completed durable waits.</p></div>
            #{rows_table(waits, WAIT_TABLE_KEYS)}
          </section>
          <section class="surface">
            <div class="surface-header"><h2>Outbox</h2><p>Persisted outgoing messages for this workflow.</p></div>
            #{rows_table(outbox, OUTBOX_TABLE_KEYS)}
          </section>
        HTML
      end
    end

    #: (Store store, script_name: String) -> String
    def objects_page(store, script_name:)
      objects = store.list_durable_objects(limit: DEFAULT_LIMIT)

      layout(title: "Durable Objects", script_name:) do
        <<~HTML
          <section class="surface">
            <div class="surface-header">
              <div>
                <h2>Durable Objects</h2>
                <p>Recently updated object state rows.</p>
              </div>
            </div>
            #{object_table(objects, script_name:)}
          </section>
        HTML
      end
    end

    #: (Store store, object_type: String, object_id: String, script_name: String) -> String
    def object_page(store, object_type:, object_id:, script_name:)
      state = store.object_state(object_type:, object_id:)
      commands = store.list_object_commands(object_type:, object_id:, limit: DEFAULT_LIMIT)

      layout(title: "Object #{object_type}/#{object_id}", script_name:) do
        <<~HTML
          <section class="hero-strip">
            <div>
              <a class="back-link" href="#{href(script_name, "/objects")}">Objects</a>
              <h1>#{h(object_type)}</h1>
              <p class="mono">#{h(object_id)}</p>
            </div>
            <div class="status-stack">
              <span class="badge neutral">state #{payload_summary(state)}</span>
            </div>
          </section>
          <section class="detail-grid">
            #{payload_panel("State", state)}
          </section>
          <section class="surface">
            <div class="surface-header"><h2>Command History</h2><p>Persisted durable-object command rows.</p></div>
            #{rows_table(commands, COMMAND_TABLE_KEYS)}
          </section>
        HTML
      end
    end

    #: (store: Store, workflows: Array[Hash[String, Object?]], counts: Hash[String, Integer]) -> String
    def summary_grid(store:, workflows:, counts:)
      stale = workflows.count { |workflow| stale_lease?(workflow["locked_until"]) }
      <<~HTML
        <section class="summary-grid">
          <div><span>Namespace</span><strong>#{h(store.schema)}</strong></div>
          <div><span>Rows</span><strong>#{workflows.length}</strong></div>
          <div><span>Running</span><strong>#{counts["running"]}</strong></div>
          <div><span>Stale leases</span><strong>#{stale}</strong></div>
        </section>
      HTML
    end

    #: (Array[Hash[String, Object?]] workflows, script_name: String) -> String
    def workflow_table(workflows, script_name:)
      return empty_state("No workflows", "No workflow rows matched the current filters.") if workflows.empty?

      body = workflows.map do |workflow|
        <<~HTML
          <tr>
            <td>#{badge(workflow.fetch("status"))}</td>
            <td><a href="#{href(script_name, "/workflows/#{encode_path_component(workflow.fetch("id").to_s)}")}">#{h(workflow.fetch("name"))}</a><span class="subtle mono">#{h(workflow.fetch("id"))}</span></td>
            <td>#{h(workflow["locked_by"] || "none")}<span class="subtle">#{lease_text(workflow["locked_until"])}</span></td>
            <td>#{time_text(workflow["next_run_at"])}</td>
            <td>#{time_text(workflow["updated_at"])}</td>
          </tr>
        HTML
      end.join

      <<~HTML
        <div class="table-wrap">
          <table>
            <thead><tr><th>Status</th><th>Workflow</th><th>Lease</th><th>Next run</th><th>Updated</th></tr></thead>
            <tbody>#{body}</tbody>
          </table>
        </div>
      HTML
    end

    #: (Array[Hash[String, Object?]] objects, script_name: String) -> String
    def object_table(objects, script_name:)
      return empty_state("No durable objects", "No durable object rows are present.") if objects.empty?

      body = objects.map do |object|
        object_type = object.fetch("object_type").to_s
        object_id = object.fetch("object_id").to_s
        <<~HTML
          <tr>
            <td><a href="#{href(script_name, "/objects/#{encode_path_component(object_type)}/#{encode_path_component(object_id)}")}">#{h(object_type)}</a><span class="subtle mono">#{h(object_id)}</span></td>
            <td>#{payload_summary(object["state"])}</td>
            <td>#{h(object["locked_by"] || "none")}<span class="subtle">#{lease_text(object["locked_until"])}</span></td>
            <td>#{time_text(object["updated_at"])}</td>
          </tr>
        HTML
      end.join

      <<~HTML
        <div class="table-wrap">
          <table>
            <thead><tr><th>Object</th><th>State</th><th>Lease</th><th>Updated</th></tr></thead>
            <tbody>#{body}</tbody>
          </table>
        </div>
      HTML
    end

    #: (String title, Hash[String, Object?] row, Array[String] keys) -> String
    def kv_panel(title, row, keys)
      items = keys.map { |key| "<dt>#{h(key)}</dt><dd>#{h(display_value(row[key]))}</dd>" }.join
      <<~HTML
        <section class="panel">
          <h2>#{h(title)}</h2>
          <dl>#{items}</dl>
        </section>
      HTML
    end

    #: (String title, Object? value) -> String
    def payload_panel(title, value)
      <<~HTML
        <section class="panel">
          <h2>#{h(title)}</h2>
          <p class="payload-summary">#{payload_summary(value)}</p>
        </section>
      HTML
    end

    #: (Object? value) -> String
    def error_panel(value)
      return "" if blank?(value)

      <<~HTML
        <section class="panel danger">
          <h2>Error</h2>
          <p>#{h(value)}</p>
        </section>
      HTML
    end

    #: (Array[Hash[String, Object?]] rows, Array[String] keys) -> String
    def rows_table(rows, keys)
      return empty_state("No rows", "No persisted rows are available for this view.") if rows.empty?

      headers = keys.map { |key| "<th>#{h(key)}</th>" }.join
      body = rows.map do |row|
        cells = keys.map { |key| "<td>#{h(display_value(row[key]))}</td>" }.join
        "<tr>#{cells}</tr>"
      end.join

      <<~HTML
        <div class="table-wrap">
          <table>
            <thead><tr>#{headers}</tr></thead>
            <tbody>#{body}</tbody>
          </table>
        </div>
      HTML
    end

    #: (script_name: String, selected: String?) -> String
    def status_filter(script_name:, selected:)
      statuses = ["", "pending", "running", "waiting", "failed", "completed"]
      options = statuses.map do |status|
        label = status.empty? ? "all statuses" : status
        selected_attr = status == selected.to_s ? " selected" : ""
        "<option value=\"#{h(status)}\"#{selected_attr}>#{h(label)}</option>"
      end.join

      <<~HTML
        <form class="filter" method="get" action="#{href(script_name, "/workflows")}">
          <label>Status <select name="status">#{options}</select></label>
          <button type="submit">Apply</button>
        </form>
      HTML
    end

    #: (title: String, script_name: String) { -> String } -> String
    def layout(title:, script_name:, &block)
      content = block.call
      <<~HTML
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>#{h(title)} - #{h(@title)}</title>
          <style>#{stylesheet}</style>
        </head>
        <body>
          <header class="topbar">
            <div>
              <strong>#{h(@title)}</strong>
              <span>Persisted workflow and object state</span>
            </div>
            <nav>
              <a href="#{href(script_name, "/workflows")}">Workflows</a>
              <a href="#{href(script_name, "/objects")}">Objects</a>
            </nav>
          </header>
          <main>#{content}</main>
        </body>
        </html>
      HTML
    end

    #: (String value) -> String
    def normalize_path(value)
      path = value.empty? ? "/" : value
      path.end_with?("/") && path.length > 1 ? path.delete_suffix("/") : path
    end

    #: (String query_string) -> Hash[String, String]
    def parse_query(query_string)
      URI.decode_www_form(query_string).to_h
    rescue ArgumentError
      {}
    end

    #: (String value) -> String
    def decode_path_component(value)
      URI.decode_www_form_component(value)
    end

    #: (String value) -> String
    def encode_path_component(value)
      URI.encode_www_form_component(value)
    end

    #: (String script_name, String path) -> String
    def href(script_name, path)
      "#{script_name}#{path}"
    end

    #: (Integer status, String body, String content_type) -> Array[Object]
    def response(status, body, content_type)
      [status, { "content-type" => content_type, "cache-control" => "no-store" }, [body]]
    end

    #: (String body, ?status: Integer) -> Array[Object]
    def html(body, status: 200)
      response(status, body, "text/html; charset=utf-8")
    end

    #: (Object? value) -> String
    def badge(value)
      status = value.to_s
      klass = case status
      when "completed"
        "success"
      when "failed"
        "danger"
      when "running"
        "active"
      when "waiting"
        "waiting"
      else
        "neutral"
      end
      "<span class=\"badge #{klass}\">#{h(status)}</span>"
    end

    #: (Object? value) -> String
    def payload_summary(value)
      case value
      when nil
        "empty"
      when Hash
        "Hash with #{value.length} keys"
      when Array
        "Array with #{value.length} items"
      else
        value.class.name || "Object"
      end
    end

    #: (Object? value) -> String
    def display_value(value)
      return "-" if blank?(value)

      case value
      when Time
        value.utc.iso8601
      when Hash, Array
        payload_summary(value)
      else
        value.to_s
      end
    end

    #: (Object? value) -> String
    def time_text(value)
      return "-" if blank?(value)

      parsed_time(value)&.utc&.iso8601 || value.to_s
    end

    #: (Object? value) -> String
    def lease_text(value)
      return "No lease deadline" if blank?(value)

      stale_lease?(value) ? "Expired #{time_text(value)}" : "Until #{time_text(value)}"
    end

    #: (Object? value) -> bool
    def stale_lease?(value)
      time = parsed_time(value)
      !!(time && time < Time.now)
    end

    #: (Object? value) -> Time?
    def parsed_time(value)
      return value if value.is_a?(Time)
      return if blank?(value)

      Time.parse(value.to_s)
    rescue ArgumentError
      nil
    end

    #: (Object? value) -> bool
    def blank?(value)
      value.nil? || value.to_s.empty?
    end

    #: (Object? value) -> String
    def h(value)
      value.to_s
        .gsub("&", "&amp;")
        .gsub("<", "&lt;")
        .gsub(">", "&gt;")
        .gsub("\"", "&quot;")
        .gsub("'", "&#39;")
    end

    #: (String title, String message) -> String
    def empty_state(title, message)
      <<~HTML
        <div class="empty-state">
          <h2>#{h(title)}</h2>
          <p>#{h(message)}</p>
        </div>
      HTML
    end

    #: () -> String
    def stylesheet
      <<~CSS
        :root { color-scheme: light; --bg: #f7f8fa; --ink: #17202a; --muted: #5d6978; --line: #d9dee7; --panel: #ffffff; --accent: #0f766e; --danger: #b42318; --warn: #b54708; --ok: #16714a; }
        * { box-sizing: border-box; }
        body { margin: 0; background: var(--bg); color: var(--ink); font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
        a { color: #0b5cad; text-decoration: none; }
        a:hover { text-decoration: underline; }
        .topbar { display: flex; align-items: center; justify-content: space-between; gap: 24px; padding: 14px 24px; border-bottom: 1px solid var(--line); background: #fff; position: sticky; top: 0; z-index: 1; }
        .topbar strong { display: block; font-size: 15px; }
        .topbar span, .subtle { color: var(--muted); display: block; font-size: 12px; margin-top: 2px; }
        .topbar nav { display: flex; gap: 16px; }
        main { max-width: 1180px; margin: 0 auto; padding: 24px; }
        h1, h2 { margin: 0; line-height: 1.2; }
        h1 { font-size: 24px; }
        h2 { font-size: 15px; }
        p { margin: 4px 0 0; color: var(--muted); }
        .summary-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }
        .summary-grid div, .surface, .panel, .hero-strip, .empty-state { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; }
        .summary-grid div { padding: 14px; }
        .summary-grid span { color: var(--muted); display: block; font-size: 12px; }
        .summary-grid strong { display: block; margin-top: 4px; font-size: 20px; overflow-wrap: anywhere; }
        .surface { margin-bottom: 16px; overflow: hidden; }
        .surface-header { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 16px; border-bottom: 1px solid var(--line); }
        .table-wrap { overflow-x: auto; }
        table { border-collapse: collapse; width: 100%; min-width: 720px; }
        th, td { border-bottom: 1px solid var(--line); padding: 10px 12px; text-align: left; vertical-align: top; }
        th { color: var(--muted); font-size: 12px; font-weight: 600; background: #fbfcfd; }
        tr:last-child td { border-bottom: 0; }
        .badge { display: inline-flex; align-items: center; min-height: 24px; border-radius: 999px; padding: 3px 9px; border: 1px solid var(--line); font-size: 12px; font-weight: 650; }
        .badge.success { color: var(--ok); background: #eefbf5; border-color: #b7ead2; }
        .badge.danger, .panel.danger { color: var(--danger); background: #fff3f0; border-color: #ffd0c7; }
        .badge.active { color: var(--accent); background: #eefaf8; border-color: #b8e7e1; }
        .badge.waiting { color: var(--warn); background: #fff7ed; border-color: #fed7aa; }
        .badge.neutral { color: #374151; background: #f4f6f8; border-color: var(--line); }
        .filter { display: flex; align-items: center; gap: 8px; }
        select, button { min-height: 32px; border: 1px solid var(--line); border-radius: 6px; background: #fff; color: var(--ink); padding: 0 10px; }
        button { background: var(--ink); color: #fff; border-color: var(--ink); }
        .hero-strip { display: flex; justify-content: space-between; gap: 18px; padding: 18px; margin-bottom: 16px; }
        .back-link { display: inline-block; margin-bottom: 8px; font-size: 12px; }
        .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
        .status-stack { display: flex; flex-direction: column; align-items: flex-end; gap: 8px; color: var(--muted); }
        .detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; margin-bottom: 16px; }
        .panel { padding: 14px; min-width: 0; }
        .panel dl { display: grid; grid-template-columns: minmax(84px, 0.7fr) minmax(0, 1.3fr); gap: 8px 12px; margin: 12px 0 0; }
        .panel dt { color: var(--muted); }
        .panel dd { margin: 0; overflow-wrap: anywhere; }
        .payload-summary { margin-top: 12px; color: var(--ink); font-weight: 600; }
        .empty-state { padding: 28px; margin: 16px; text-align: center; }
        @media (max-width: 820px) { .topbar, .surface-header, .hero-strip { align-items: flex-start; flex-direction: column; } .summary-grid { grid-template-columns: 1fr; } main { padding: 14px; } .status-stack { align-items: flex-start; } }
      CSS
    end
  end
end
