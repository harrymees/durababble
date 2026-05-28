#!/usr/bin/env ruby
# typed: true
# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "active_support/isolated_execution_state"
require "cgi"
require "digest"
require "fileutils"
require "json"
require "optparse"
require "time"
require "uri"

require "durababble"

ActiveSupport::IsolatedExecutionState.isolation_level = :fiber

module DurababbleMysqlHotPathReport
  DEFAULT_WORKFLOW_NAME = "hot-path-report-workflow"
  DEFAULT_WORKER_ID = "hot-path-worker"
  DEFAULT_WORKER_POOL = "default"
  DEFAULT_OUTPUT_DIR = "tmp/sql-hot-path-reports"
  DEFAULT_OPERATION = "claim_runnable_workflow"

  BUILTIN_OPERATION_DESCRIPTIONS = {
    "enqueue_workflow" => "Trace the single durable write that enqueues a pending workflow.",
    "claim_runnable_workflow" => "Trace the MySQL queue probes and lease update used when a worker claims runnable workflow work.",
    "claim_target_activation" => "Trace the target activation queue probes and lease update used when a worker claims mailbox wakeup work.",
    "worker_poll_idle" => "Trace one Worker#tick poll when no workflow work is available.",
    "worker_tick_claim" => "Trace one Worker#tick that polls, claims a workflow, and runs it to completion.",
  }.freeze
  OPERATIONS = BUILTIN_OPERATION_DESCRIPTIONS
  SQL_KEYWORDS = [
    "ALL",
    "AND",
    "AS",
    "ASC",
    "BETWEEN",
    "BY",
    "CASE",
    "CHARSET",
    "COALESCE",
    "COLLATE",
    "COUNT",
    "CREATE",
    "CURRENT_TIMESTAMP",
    "DATE_ADD",
    "DATETIME",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DO",
    "ELSE",
    "END",
    "ENGINE",
    "EXISTS",
    "FORCE",
    "FOR",
    "FROM",
    "GREATEST",
    "GROUP",
    "HAVING",
    "IF",
    "IGNORE",
    "IN",
    "INDEX",
    "INSERT",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "KEY",
    "LEAST",
    "LIMIT",
    "LOCKED",
    "LONGBLOB",
    "MAX",
    "MIN",
    "NOT",
    "NOW",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "PRIMARY",
    "SELECT",
    "SET",
    "SKIP",
    "TABLE",
    "TEXT",
    "THEN",
    "UNIQUE",
    "UPDATE",
    "VALUES",
    "VARCHAR",
    "WHEN",
    "WHERE",
  ].freeze
  SQL_TOKEN_PATTERN = %r{
    --[^\n]*
    |\/\*.*?\*\/
    |`(?:``|[^`])*`
    |'(?:''|\\.|[^'\\])*'
    |"(?:""|\\.|[^"\\])*"
    |\b(?:#{Regexp.union(SQL_KEYWORDS).source})\b
    |\b\d+(?:\.\d+)?\b
    |\$[0-9]+|\?
  }imx
  EXPLAIN_COLUMNS = [
    ["id", "ID"],
    ["select_type", "Select"],
    ["table", "Table"],
    ["partitions", "Partitions"],
    ["type", "Access"],
    ["possible_keys", "Possible keys"],
    ["key", "Chosen key"],
    ["key_len", "Key length"],
    ["ref", "Ref"],
    ["rows", "Rows"],
    ["filtered", "Filtered %"],
    ["Extra", "Extra"],
  ].freeze

  class ReportWorkflow < Durababble::Workflow
    workflow_name DEFAULT_WORKFLOW_NAME

    def execute(input)
      input.merge("executed" => true)
    end
  end

  class Scenario
    attr_reader :name, :description

    def initialize(name:, description:, setup:, run:)
      @name = name
      @description = description
      @setup = setup
      @run = run
    end

    def setup(context)
      @setup&.call(context)
    end

    def run(context)
      @run.call(context)
    end
  end

  class ScenarioContext
    attr_reader :store, :options

    def initialize(store:, options:)
      @store = store
      @options = options
    end

    def fixture_size
      @options.fetch(:fixture_size)
    end

    def worker(
      workflows: { DEFAULT_WORKFLOW_NAME => ReportWorkflow },
      worker_id: DEFAULT_WORKER_ID,
      worker_pool: DEFAULT_WORKER_POOL,
      lease_seconds: 60
    )
      Durababble::Worker.new(
        store:,
        workflows:,
        worker_id:,
        worker_pool:,
        lease_seconds:,
        migrate: false,
      )
    end

    def enqueue_report_workflow(
      id: "hot-path-claimable",
      input: { "kind" => "claim" },
      name: DEFAULT_WORKFLOW_NAME,
      worker_pool: DEFAULT_WORKER_POOL
    )
      store.enqueue_workflow(name:, input:, id:, worker_pool:)
    end

    def seed_pending_workflows(
      count = fixture_size,
      name: "other-workflow",
      worker_pool: DEFAULT_WORKER_POOL,
      id_prefix: "hot-path-background"
    )
      count.times do |index|
        store.enqueue_workflow(
          name:,
          input: { "n" => index },
          id: "#{id_prefix}-#{index}",
          worker_pool:,
        )
      end
    end

    def seed_target_activations(
      count = fixture_size,
      target_kind: "workflow",
      target_type: "background-workflow",
      worker_pool: DEFAULT_WORKER_POOL,
      id_prefix: "hot-path-background-activation"
    )
      count.times do |index|
        store.rearm_target_activation(
          target_kind:,
          target_type:,
          target_id: "#{id_prefix}-#{index}",
          ready_at: Time.now - 1,
          worker_pool:,
        )
      end
    end
  end

  class SqlRecorder
    attr_reader :queries, :events

    def initialize(root:)
      @root = root
      @queries = []
      @events = []
      @transaction_stack = []
      @next_query = 0
      @next_transaction = 0
      @enabled = true
    end

    def active?
      @enabled
    end

    def disabled
      previous = @enabled
      @enabled = false
      yield
    ensure
      @enabled = previous
    end

    def begin_transaction(options)
      @next_transaction += 1
      transaction = {
        id: @next_transaction,
        depth: @transaction_stack.length + 1,
        parent_id: @transaction_stack.last&.fetch(:id),
        options: normalize_options(options),
      }
      @transaction_stack << transaction
      @events << { type: "transaction.begin", transaction: transaction.dup }
      transaction
    end

    def end_transaction(transaction, status)
      @transaction_stack.pop if @transaction_stack.last&.fetch(:id) == transaction.fetch(:id)
      @events << { type: "transaction.#{status}", transaction: transaction.dup }
    end

    def record_query(query_id:, sql:, params:, callsite:)
      @next_query += 1
      query = {
        sequence: @next_query,
        query_id: query_id,
        description: Durababble::StoreQueries.description_for(query_id),
        sql: sql.strip,
        params: params,
        callsite: callsite,
        transaction: @transaction_stack.last&.dup,
        started_at_monotonic: Process.clock_gettime(Process::CLOCK_MONOTONIC),
      }
      @queries << query
      @events << { type: "query", query: query }
      query
    end

    def finish_query(query, result)
      record_duration(query)
      query[:row_count] = result_row_count(result)
      query[:affected_rows] = result_affected_rows(result)
    end

    def fail_query(query, error)
      record_duration(query)
      query[:error] = "#{error.class}: #{error.message}"
    end

    def callsite(locations)
      selected = locations.find { |location| relative_path(location.path).start_with?("lib/durababble/store/") && relative_path(location.path) != "lib/durababble/store.rb" }
      selected ||= locations.find { |location| relative_path(location.path).start_with?("lib/durababble/") }
      selected ||= locations.first
      return "unknown" unless selected

      "#{relative_path(selected.path)}:#{selected.lineno}:in #{selected.label}"
    end

    private

    def normalize_options(options)
      options.transform_values { |value| value.is_a?(Symbol) ? value.to_s : value }
    end

    def record_duration(query)
      started_at = query.delete(:started_at_monotonic)
      return unless started_at

      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
      query[:duration_ms] = (elapsed * 1000).round(3)
    end

    def result_row_count(result)
      return unless result.respond_to?(:rows)

      result.rows.length
    end

    def result_affected_rows(result)
      return result.affected_rows if result.respond_to?(:affected_rows)

      nil
    end

    def relative_path(path)
      path = File.expand_path(path)
      path.delete_prefix("#{@root}/")
    end
  end

  module StoreSqlInstrumentation
    def execute_store_query(id, params = [], **locals)
      store = self #: as untyped
      recorder = store.instance_variable_get(:@durababble_hot_path_recorder)
      return super unless recorder&.active?

      query_id = store.send(:qualified_store_query_id, id)
      sql = store.send(:store_query_sql, id, **locals)
      query = recorder.record_query(
        query_id:,
        sql:,
        params:,
        callsite: recorder.callsite(Kernel.caller_locations(1)),
      )
      result = store.send(:with_connection) { store.send(:execute_store_query_sql, sql, params) }
      recorder.finish_query(query, result)
      result
    rescue StandardError => error
      recorder&.fail_query(query, error) if query
      Kernel.raise
    end

    def transaction(**options, &block)
      store = self #: as untyped
      recorder = store.instance_variable_get(:@durababble_hot_path_recorder)
      return super unless recorder&.active?

      transaction = recorder.begin_transaction(options)
      result = super
      recorder.end_transaction(transaction, "commit")
      result
    rescue StandardError
      recorder.end_transaction(transaction, "rollback") if transaction
      Kernel.raise
    end
  end

  class MysqlExplainer
    def initialize(store:, recorder:)
      @store = store
      @recorder = recorder
    end

    def explain!(queries)
      queries.each do |query|
        query[:explain] = explain_query(query)
      end
    end

    private

    def explain_query(query)
      return { skipped: "query failed before execution" } if query[:error]

      rows = @recorder.disabled do
        @store.send(:execute_store_query_sql, "EXPLAIN #{query.fetch(:sql)}", query.fetch(:params))
      end
      rows = normalize_rows(rows)
      return { skipped: "EXPLAIN returned no rows" } if rows.empty?

      { rows: rows }
    rescue StandardError => error
      { error: "#{error.class}: #{error.message}" }
    end

    def normalize_rows(rows)
      rows.map do |row|
        row.to_h.transform_keys(&:to_s)
      end
    end
  end

  class Runner
    def initialize(options)
      @options = options
      @root = File.expand_path("..", __dir__)
    end

    def run
      scenario = validate_operation!
      store = connect_store
      recorder = SqlRecorder.new(root: @root)
      store.singleton_class.prepend(StoreSqlInstrumentation) unless store.singleton_class.ancestors.include?(StoreSqlInstrumentation)
      store.instance_variable_set(:@durababble_hot_path_recorder, recorder)

      begin
        recorder.disabled { reset_schema(store) }
        recorder.disabled { store.migrate! }
        context = ScenarioContext.new(store:, options: @options)
        recorder.disabled { scenario.setup(context) }
        result = scenario.run(context)
        MysqlExplainer.new(store:, recorder:).explain!(recorder.queries)
        report = build_report(store:, recorder:, scenario:, result:)
        write_report(report)
      ensure
        store.instance_variable_set(:@durababble_hot_path_recorder, nil)
        cleanup_schema(store)
        store.close
      end
    end

    private

    def connect_store
      store = Durababble::Store.connect(database_url: @options.fetch(:database_url), schema: @options.fetch(:schema))
      unless store.is_a?(Durababble::MysqlStore)
        store.close
        raise ArgumentError, "mysql-hot-path-report requires a MySQL/MariaDB database URL"
      end
      store
    end

    def reset_schema(store)
      store.drop_schema!
      store.instance_variable_set(:@migrated, false)
    rescue StandardError
      store.instance_variable_set(:@migrated, false)
    end

    def cleanup_schema(store)
      return if @options.fetch(:keep_schema)

      store.drop_schema!
    rescue StandardError => error
      warn("failed to drop report schema #{@options.fetch(:schema)}: #{error.class}: #{error.message}")
    end

    def build_report(store:, recorder:, scenario:, result:)
      {
        operation: scenario.name,
        operation_description: scenario.description,
        database_url: redacted_database_url(@options.fetch(:database_url)),
        schema: store.schema,
        table_prefix: store.send(:table_prefix),
        fixture_size: @options.fetch(:fixture_size),
        result: DurababbleMysqlHotPathReport.format_value(result),
        total_query_runtime_ms: recorder.queries.sum { |query| query.fetch(:duration_ms, 0.0) }.round(3),
        table_ddls: collect_table_ddls(store, recorder.queries),
        queries: recorder.queries,
        events: recorder.events,
      }
    end

    def collect_table_ddls(store, queries)
      available_tables = store.send(:execute, "SHOW TABLES").map { |row| row.values.first }.select { |table_name| table_name.start_with?("#{store.send(:table_prefix)}_") }
      table_names = DurababbleMysqlHotPathReport.table_names_from_queries(queries, table_prefix: store.send(:table_prefix)) & available_tables
      table_names.to_h do |table_name|
        row = store.send(:execute, "SHOW CREATE TABLE #{DurababbleMysqlHotPathReport.quote_identifier(table_name)}").first
        [table_name, row.fetch("Create Table")]
      end
    end

    def write_report(report)
      FileUtils.mkdir_p(@options.fetch(:output_dir))
      extension = @options.fetch(:format) == "html" ? "html" : "md"
      path = @options[:output] || File.join(@options.fetch(:output_dir), "#{report.fetch(:operation)}.#{extension}")
      body = @options.fetch(:format) == "html" ? HtmlRenderer.new(report).render : MarkdownRenderer.new(report).render
      File.write(path, body)
      puts path
    end

    def validate_operation!
      DurababbleMysqlHotPathReport.scenario_for(@options.fetch(:operation))
    rescue KeyError
      scenario_names = DurababbleMysqlHotPathReport.scenarios.keys.sort.join(", ")

      raise ArgumentError, "unknown --operation #{@options.fetch(:operation).inspect}; choose one of #{scenario_names}"
    end

    def redacted_database_url(value)
      uri = URI.parse(value)
      uri.password = "REDACTED" if uri.password
      uri.to_s
    rescue URI::InvalidURIError
      "<invalid database URL>"
    end
  end

  module RenderHelpers
    def format_params(params)
      DurababbleMysqlHotPathReport.format_params(params)
    end

    def transaction_label(query)
      DurababbleMysqlHotPathReport.transaction_label(query)
    end

    def rows_label(query)
      DurababbleMysqlHotPathReport.rows_label(query)
    end

    def runtime_label(value)
      DurababbleMysqlHotPathReport.runtime_label(value)
    end

    def explain_value(value)
      DurababbleMysqlHotPathReport.explain_value(value)
    end

    def escape_table(value)
      DurababbleMysqlHotPathReport.escape_table(value)
    end
  end

  class MarkdownRenderer
    include RenderHelpers

    def initialize(report)
      @report = report
    end

    def render
      lines = []
      lines << "# Durababble MySQL hot path: #{@report.fetch(:operation)}"
      lines << ""
      lines << @report.fetch(:operation_description)
      lines << ""
      lines << "- Database: `#{@report.fetch(:database_url)}`"
      lines << "- Schema: `#{@report.fetch(:schema)}`"
      lines << "- Table prefix: `#{@report.fetch(:table_prefix)}`"
      lines << "- Fixture size: `#{@report.fetch(:fixture_size)}`"
      lines << "- Result: `#{@report.fetch(:result)}`"
      lines << "- SQL statements: `#{queries.length}`"
      lines << "- Total query runtime: `#{runtime_label(@report.fetch(:total_query_runtime_ms))}`"
      lines << ""
      lines << "## Query Timeline"
      lines << ""
      lines << "| # | Query | Runtime | Transaction | Callsite | Rows | Purpose |"
      lines << "| --- | --- | --- | --- | --- | --- | --- |"
      queries.each do |query|
        lines << "| #{query.fetch(:sequence)} | `#{query.fetch(:query_id)}` | #{runtime_label(query.fetch(:duration_ms, nil))} | #{transaction_label(query)} | `#{query.fetch(:callsite)}` | #{rows_label(query)} | #{escape_table(query.fetch(:description))} |"
      end
      lines << ""
      queries.each do |query|
        append_query(lines, query)
      end
      append_schema_appendix(lines)
      lines.join("\n")
    end

    private

    def queries = @report.fetch(:queries)

    def append_query(lines, query)
      lines << "## #{query.fetch(:sequence)}. `#{query.fetch(:query_id)}`"
      lines << ""
      lines << query.fetch(:description)
      lines << ""
      lines << "- Transaction: #{transaction_label(query)}"
      lines << "- Runtime: `#{runtime_label(query.fetch(:duration_ms, nil))}`"
      lines << "- Callsite: `#{query.fetch(:callsite)}`"
      lines << "- Params: `#{format_params(query.fetch(:params))}`"
      lines << "- Rows: #{rows_label(query)}"
      lines << ""
      lines << "```sql"
      lines << query.fetch(:sql)
      lines << "```"
      lines << ""
      append_explain(lines, query.fetch(:explain))
      lines << ""
    end

    def append_explain(lines, explain)
      if explain[:error]
        lines << "**EXPLAIN error:** `#{explain.fetch(:error)}`"
        return
      end

      if explain[:skipped]
        lines << "**EXPLAIN skipped:** #{explain.fetch(:skipped)}"
        return
      end

      rows = explain.fetch(:rows)
      if rows.empty?
        lines << "**EXPLAIN plan:** none reported"
        return
      end

      lines << "**EXPLAIN plan:**"
      lines << ""
      lines << "| #{EXPLAIN_COLUMNS.map(&:last).join(" | ")} |"
      lines << "| #{EXPLAIN_COLUMNS.map { "---" }.join(" | ")} |"
      rows.each do |row|
        lines << "| #{EXPLAIN_COLUMNS.map { |key, _label| escape_table(explain_value(row[key])) }.join(" | ")} |"
      end
      lines << ""
    end

    def append_schema_appendix(lines)
      table_ddls = @report.fetch(:table_ddls, {})
      return if table_ddls.empty?

      lines << "## Schema Appendix"
      lines << ""
      table_ddls.each do |table_name, ddl|
        lines << "### `#{table_name}`"
        lines << ""
        lines << "```sql"
        lines << ddl
        lines << "```"
        lines << ""
      end
    end
  end

  class HtmlRenderer
    include RenderHelpers

    def initialize(report)
      @report = report
    end

    def render
      <<~HTML
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <title>Durababble MySQL hot path: #{h(@report.fetch(:operation))}</title>
          <style>
            body { color: #1f2328; font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; max-width: 1180px; }
            h1, h2 { line-height: 1.2; }
            code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
            pre { background: #f6f8fa; border: 1px solid #d8dee4; border-radius: 6px; overflow: auto; padding: 12px; }
            pre.sql { line-height: 1.5; overflow: visible; white-space: pre-wrap; }
            .sql-keyword { color: #cf222e; font-weight: 600; }
            .sql-identifier { color: #0550ae; }
            .sql-string { color: #0a3069; }
            .sql-number { color: #953800; }
            .sql-placeholder { color: #116329; font-weight: 600; }
            .sql-comment { color: #6e7781; font-style: italic; }
            .sql-table { border-bottom: 1px dotted #0550ae; cursor: help; position: relative; }
            .schema-popover { background: #ffffff; border: 1px solid #d8dee4; border-radius: 6px; box-shadow: 0 8px 24px rgb(140 149 159 / 35%); color: #1f2328; display: none; left: 0; max-height: 380px; min-width: min(560px, calc(100vw - 96px)); max-width: min(920px, calc(100vw - 96px)); overflow: auto; padding: 10px; position: absolute; top: 1.6em; white-space: normal; z-index: 10; }
            .sql-table:hover .schema-popover, .sql-table:focus .schema-popover { display: block; }
            .schema-popover-title { display: block; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; font-weight: 600; margin-bottom: 8px; }
            .schema-popover .schema-ddl { background: #f6f8fa; border: 1px solid #d8dee4; border-radius: 6px; display: block; font-size: 12px; line-height: 1.5; overflow: auto; padding: 8px; white-space: pre; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #d8dee4; padding: 6px 8px; text-align: left; vertical-align: top; }
            th { background: #f6f8fa; }
            td, th { overflow-wrap: anywhere; }
            .meta { display: grid; grid-template-columns: 140px 1fr; gap: 4px 12px; }
            .timeline { table-layout: fixed; }
            .timeline col.sequence { width: 4%; }
            .timeline col.query-id { width: 17%; }
            .timeline col.runtime { width: 8%; }
            .timeline col.transaction { width: 12%; }
            .timeline col.callsite { width: 27%; }
            .timeline col.rows { width: 8%; }
            .timeline col.purpose-col { width: 24%; }
            .timeline code { white-space: normal; }
            .explain-list { display: grid; gap: 12px; margin-top: 8px; }
            .explain-card { background: #ffffff; border: 1px solid #d8dee4; border-radius: 6px; padding: 14px; }
            .explain-card-header { align-items: start; display: grid; gap: 8px 12px; grid-template-columns: minmax(0, 1fr) auto; margin-bottom: 12px; }
            .explain-title { font-weight: 700; overflow-wrap: anywhere; }
            .explain-badges { display: flex; flex-wrap: wrap; gap: 6px; justify-content: flex-end; }
            .explain-badge { background: #f6f8fa; border: 1px solid #d8dee4; border-radius: 999px; font-size: 12px; padding: 2px 8px; white-space: nowrap; }
            .explain-access { background: #ddf4ff; border-color: #54aeef; }
            .explain-metrics { display: grid; gap: 8px; grid-template-columns: repeat(auto-fit, minmax(112px, 1fr)); margin: 0 0 12px; }
            .explain-metric { background: #f6f8fa; border: 1px solid #d8dee4; border-radius: 6px; min-width: 0; padding: 7px 8px; }
            .explain-metric dt, .explain-section-label { color: #57606a; font-size: 12px; margin: 0 0 2px; }
            .explain-metric dd { font-weight: 600; margin: 0; overflow-wrap: anywhere; }
            .explain-section { margin-top: 10px; }
            .explain-pills { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 4px; }
            .explain-pill { background: #f6f8fa; border: 1px solid #d8dee4; border-radius: 999px; max-width: 100%; overflow-wrap: anywhere; padding: 1px 7px; white-space: normal; }
            .explain-pill-selected { background: #ddf4ff; border-color: #54aeef; }
            .explain-extra .explain-pill { background: #fff8c5; border-color: #d4a72c; }
            .muted { color: #6e7781; }
            .query { border-top: 1px solid #d8dee4; margin-top: 24px; padding-top: 16px; }
            .purpose { color: #57606a; }
            summary { cursor: pointer; font-weight: 600; }
          </style>
        </head>
        <body>
          <h1>Durababble MySQL hot path: #{h(@report.fetch(:operation))}</h1>
          <p>#{h(@report.fetch(:operation_description))}</p>
          <dl class="meta">
            <dt>Database</dt><dd><code>#{h(@report.fetch(:database_url))}</code></dd>
            <dt>Schema</dt><dd><code>#{h(@report.fetch(:schema))}</code></dd>
            <dt>Table prefix</dt><dd><code>#{h(@report.fetch(:table_prefix))}</code></dd>
            <dt>Fixture size</dt><dd><code>#{h(@report.fetch(:fixture_size).to_s)}</code></dd>
            <dt>Result</dt><dd><code>#{h(@report.fetch(:result))}</code></dd>
            <dt>SQL statements</dt><dd><code>#{queries.length}</code></dd>
            <dt>Total runtime</dt><dd><code>#{h(runtime_label(@report.fetch(:total_query_runtime_ms)))}</code></dd>
          </dl>
          <h2>Query Timeline</h2>
          #{timeline_table}
          #{queries.map { |query| query_section(query) }.join("\n")}
        </body>
        </html>
      HTML
    end

    private

    def queries = @report.fetch(:queries)

    def timeline_table
      rows = queries.map do |query|
        "<tr><td>#{query.fetch(:sequence)}</td><td><code>#{h(query.fetch(:query_id).to_s)}</code></td><td>#{h(runtime_label(query.fetch(:duration_ms, nil)))}</td><td>#{h(transaction_label(query))}</td><td><code>#{h(query.fetch(:callsite))}</code></td><td>#{h(rows_label(query))}</td><td>#{h(query.fetch(:description))}</td></tr>"
      end.join("\n")
      <<~HTML
        <table class="timeline">
          <colgroup>
            <col class="sequence">
            <col class="query-id">
            <col class="runtime">
            <col class="transaction">
            <col class="callsite">
            <col class="rows">
            <col class="purpose-col">
          </colgroup>
          <thead><tr><th>#</th><th>Query</th><th>Runtime</th><th>Transaction</th><th>Callsite</th><th>Rows</th><th>Purpose</th></tr></thead>
          <tbody>#{rows}</tbody>
        </table>
      HTML
    end

    def query_section(query)
      <<~HTML
        <section class="query">
          <h2>#{query.fetch(:sequence)}. <code>#{h(query.fetch(:query_id).to_s)}</code></h2>
          <p class="purpose">#{h(query.fetch(:description))}</p>
          <dl class="meta">
            <dt>Transaction</dt><dd>#{h(transaction_label(query))}</dd>
            <dt>Runtime</dt><dd><code>#{h(runtime_label(query.fetch(:duration_ms, nil)))}</code></dd>
            <dt>Callsite</dt><dd><code>#{h(query.fetch(:callsite))}</code></dd>
            <dt>Params</dt><dd><code>#{h(format_params(query.fetch(:params)))}</code></dd>
            <dt>Rows</dt><dd>#{h(rows_label(query))}</dd>
          </dl>
          <details open><summary>SQL</summary><pre class="sql"><code>#{highlight_sql(query.fetch(:sql))}</code></pre></details>
          #{explain_section(query.fetch(:explain))}
        </section>
      HTML
    end

    def explain_section(explain)
      return "<p><strong>EXPLAIN error:</strong> <code>#{h(explain.fetch(:error))}</code></p>" if explain[:error]
      return "<p><strong>EXPLAIN skipped:</strong> #{h(explain.fetch(:skipped))}</p>" if explain[:skipped]

      rows = explain.fetch(:rows)
      return "<p><strong>EXPLAIN plan:</strong> none reported</p>" if rows.empty?

      <<~HTML
        <details open>
          <summary>EXPLAIN plan</summary>
          <div class="explain-list">#{rows.map { |row| explain_card(row) }.join("\n")}</div>
        </details>
      HTML
    end

    def explain_card(row)
      possible_keys = explain_terms(row["possible_keys"], separator: ",")
      extras = explain_terms(row["Extra"], separator: ";")

      <<~HTML
        <article class="explain-card">
          <div class="explain-card-header">
            <span class="explain-title"><code>#{h(explain_value(row["table"]))}</code></span>
            <div class="explain-badges">
              <span class="explain-badge">#{h(explain_value(row["select_type"]))} ##{h(explain_value(row["id"]))}</span>
              <span class="explain-badge explain-access">access: #{h(explain_value(row["type"]))}</span>
            </div>
          </div>
          <dl class="explain-metrics">
            <div class="explain-metric"><dt>Rows</dt><dd>#{h(explain_value(row["rows"]))}</dd></div>
            <div class="explain-metric"><dt>Filtered</dt><dd>#{h(filtered_value(row["filtered"]))}</dd></div>
            <div class="explain-metric"><dt>Key length</dt><dd>#{h(explain_value(row["key_len"]))}</dd></div>
            <div class="explain-metric"><dt>Ref</dt><dd>#{h(explain_value(row["ref"]))}</dd></div>
            <div class="explain-metric"><dt>Partitions</dt><dd>#{h(explain_value(row["partitions"]))}</dd></div>
          </dl>
          <div class="explain-section"><span class="explain-section-label">Chosen key</span><div class="explain-pills">#{pill_list(explain_terms(row["key"], separator: ","), selected: true)}</div></div>
          <div class="explain-section"><span class="explain-section-label">Possible keys</span><div class="explain-pills">#{pill_list(possible_keys, selected_value: row["key"])}</div></div>
          <div class="explain-section explain-extra"><span class="explain-section-label">Extra</span><div class="explain-pills">#{pill_list(extras)}</div></div>
        </article>
      HTML
    end

    def explain_terms(value, separator:)
      return [] if value.nil?

      value.to_s.split(separator).map(&:strip).reject(&:empty?)
    end

    def filtered_value(value)
      return explain_value(value) if value.nil?

      "#{explain_value(value)}%"
    end

    def pill_list(values, selected: false, selected_value: nil)
      return %(<span class="muted">NULL</span>) if values.empty?

      values.map do |value|
        classes = ["explain-pill"]
        classes << "explain-pill-selected" if selected || (!selected_value.nil? && value == selected_value.to_s)
        %(<code class="#{classes.join(" ")}">#{h(value)}</code>)
      end.join
    end

    def h(value)
      CGI.escapeHTML(value.to_s)
    end

    def highlight_sql(value)
      DurababbleMysqlHotPathReport.highlight_sql(value, table_ddls: @report.fetch(:table_ddls, {}))
    end
  end

  class << self
    def scenarios
      @scenarios ||= {}
    end

    def register_scenario(name, description:, setup: nil, &block)
      raise ArgumentError, "scenario #{name.inspect} needs a run block" unless block

      scenarios[name.to_s] = Scenario.new(
        name: name.to_s,
        description:,
        setup:,
        run: block,
      )
    end
    alias_method :scenario, :register_scenario

    def scenario_for(name)
      scenarios.fetch(name.to_s)
    end

    def load_scenario_file(path)
      load(File.expand_path(path))
    end

    def print_scenarios(io = $stdout)
      scenarios.sort.each do |name, scenario|
        io.puts "#{name}\t#{scenario.description}"
      end
    end

    def parse_options(argv)
      options = {
        operation: nil,
        database_url: default_database_url,
        schema: nil,
        output_dir: DEFAULT_OUTPUT_DIR,
        format: "html",
        fixture_size: 0,
        keep_schema: false,
        list_operations: false,
        scenario_files: [],
      } #: Hash[Symbol, untyped]

      parser = OptionParser.new do |opts|
        opts.banner = "Usage: #{$PROGRAM_NAME} [options]"
        opts.on("--operation NAME", "Scenario to run; use --list-scenarios to inspect choices") do |value|
          options[:operation] = value
        end
        opts.on("--scenario NAME", "Alias for --operation") do |value|
          options[:operation] = value
        end
        opts.on("--scenario-file PATH", "Load a Ruby file that registers additional scenarios") { |value| options[:scenario_files] << value }
        opts.on("--database-url URL", "MySQL URL; defaults to DURABABBLE_DATABASE_URL or local MySQL test URL") { |value| options[:database_url] = value }
        opts.on("--schema NAME", "Durababble schema/table prefix namespace") { |value| options[:schema] = value }
        opts.on("--format FORMAT", "html or markdown") { |value| options[:format] = value == "markdown" ? "markdown" : value }
        opts.on("--output PATH", "Write the report to PATH") { |value| options[:output] = value }
        opts.on("--output-dir DIR", "Directory for default report path") { |value| options[:output_dir] = value }
        opts.on("--fixture-size N", Integer, "Seed N unrelated workflows before tracing") { |value| options[:fixture_size] = value }
        opts.on("--keep-schema", "Leave the report schema/tables in MySQL after the run") { options[:keep_schema] = true }
        opts.on("--list-operations", "Print scenario names and exit") { options[:list_operations] = true }
        opts.on("--list-scenarios", "Print scenario names and exit") { options[:list_operations] = true }
      end
      parser.parse!(argv)
      raise ArgumentError, "--format must be html or markdown" unless ["html", "markdown"].include?(options.fetch(:format))
      raise ArgumentError, "--fixture-size must be non-negative" if options.fetch(:fixture_size).negative?

      existing_names = scenarios.keys
      options.fetch(:scenario_files).each { |path| load_scenario_file(path) }
      loaded_names = scenarios.keys - existing_names
      if options.fetch(:list_operations)
        print_scenarios
        exit
      end
      if options[:operation].nil?
        if loaded_names.length == 1
          options[:operation] = loaded_names.first
        elsif loaded_names.length > 1
          raise ArgumentError, "--scenario-file registered multiple scenarios; choose one with --operation"
        else
          options[:operation] = DEFAULT_OPERATION
        end
      end

      options[:schema] ||= Durababble.workspace_schema(
        File.join(Dir.pwd, "sql-hot-path-report", options.fetch(:operation)),
        prefix: "durababble_hot_path",
      )
      options.delete(:list_operations)
      options.delete(:scenario_files)
      options
    end

    def default_database_url
      ENV.fetch("DURABABBLE_DATABASE_URL") do
        user = URI.encode_www_form_component(ENV.fetch("DURABABBLE_MYSQL_USERNAME", ENV.fetch("MYSQL_USER", "root")))
        password = ENV.fetch("DURABABBLE_MYSQL_PASSWORD", ENV.fetch("MYSQL_PASSWORD", nil))
        password = nil if password.to_s.empty?
        host = ENV.fetch("DURABABBLE_MYSQL_HOST", ENV.fetch("MYSQL_HOST", "127.0.0.1"))
        port = ENV.fetch("DURABABBLE_MYSQL_PORT", ENV.fetch("MYSQL_PORT", "3306"))
        database = ENV.fetch("DURABABBLE_MYSQL_DATABASE", "sidekick_server_test")
        credentials = password ? "#{user}:#{URI.encode_www_form_component(password)}" : user

        "mysql://#{credentials}@#{host}:#{port}/#{database}"
      end
    end

    def format_params(params)
      return "[]" if params.empty?

      "[" + params.map { |param| format_value(param) }.join(", ") + "]"
    end

    def format_value(value)
      case value
      when String
        if binary_string?(value)
          "bytes(#{value.bytesize}, sha256=#{Digest::SHA256.hexdigest(value)[0, 12]})"
        elsif value.length > 120
          "#{value[0, 117].inspect}..."
        else
          value.inspect
        end
      when Time
        value.utc.iso8601(6)
      when NilClass, TrueClass, FalseClass, Numeric, Symbol
        value.inspect
      else
        value.inspect
      end
    end

    def binary_string?(value)
      value.encoding == Encoding::BINARY || value.bytes.any? { |byte| byte < 32 && ![9, 10, 13].include?(byte) }
    end

    def transaction_label(query)
      transaction = query[:transaction]
      return "autocommit" unless transaction

      options = transaction.fetch(:options)
      option_suffix = options.empty? ? "" : " #{options.inspect}"
      "tx#{transaction.fetch(:id)} depth=#{transaction.fetch(:depth)}#{option_suffix}"
    end

    def rows_label(query)
      parts = []
      parts << "rows=#{query[:row_count]}" if query.key?(:row_count)
      parts << "affected=#{query[:affected_rows]}" if query[:affected_rows]
      parts << "error=#{query[:error]}" if query[:error]
      parts.empty? ? "" : parts.join(", ")
    end

    def runtime_label(value)
      return "" if value.nil?

      "#{format("%.3f", value)}ms"
    end

    def explain_value(value)
      return "NULL" if value.nil?

      value.to_s
    end

    def highlight_sql(value, table_ddls: {}, popovers: true)
      sql = value.to_s
      result = +""
      offset = 0
      sql.to_enum(:scan, SQL_TOKEN_PATTERN).each do
        match = Regexp.last_match #: as MatchData
        result << CGI.escapeHTML(sql[offset...match.begin(0)].to_s)
        result << highlight_sql_token(match[0], table_ddls:, popovers:)
        offset = match.end(0)
      end
      result << CGI.escapeHTML(sql[offset..].to_s)
      result
    end

    def highlight_sql_token(token, table_ddls:, popovers:)
      escaped_token = CGI.escapeHTML(token)
      table_name = sql_identifier_name(token)
      if popovers && table_name && table_ddls.key?(table_name)
        ddl = table_ddls.fetch(table_name)
        highlighted_ddl = highlight_sql(ddl, popovers: false)
        return %(<span class="sql-identifier sql-table" tabindex="0">#{escaped_token}<span class="schema-popover" role="tooltip"><span class="schema-popover-title">#{CGI.escapeHTML(table_name)}</span><code class="schema-ddl">#{highlighted_ddl}</code></span></span>)
      end

      %(<span class="#{sql_token_class(token)}">#{escaped_token}</span>)
    end

    def sql_identifier_name(token)
      return unless token.start_with?("`")

      token[1...-1].gsub("``", "`")
    end

    def sql_token_class(token)
      case token
      when %r{\A(?:--|/\*)}
        "sql-comment"
      when /\A`/
        "sql-identifier"
      when /\A['"]/
        "sql-string"
      when /\A(?:\$\d+|\?)\z/
        "sql-placeholder"
      when /\A\d/
        "sql-number"
      else
        "sql-keyword"
      end
    end

    def escape_table(value)
      value.to_s.gsub("|", "\\|")
    end

    def table_names_from_queries(queries, table_prefix:)
      table_names = queries.flat_map do |query|
        table_names_from_sql(query.fetch(:sql), table_prefix:)
      end
      table_names.uniq.sort
    end

    def table_names_from_sql(sql, table_prefix:)
      prefixed_table = /#{Regexp.escape(table_prefix)}_[A-Za-z0-9_]+/
      quoted_tables = sql.scan(/\b(?:FROM|JOIN|UPDATE|INTO)\s+`((?:``|[^`])+)`/i).flatten.map { |name| name.gsub("``", "`") }
      unquoted_tables = sql.scan(/\b(?:FROM|JOIN|UPDATE|INTO)\s+(#{prefixed_table})\b/i).flatten
      (quoted_tables + unquoted_tables).select { |name| name.start_with?("#{table_prefix}_") }
    end

    def quote_identifier(identifier)
      "`#{identifier.to_s.gsub("`", "``")}`"
    end
  end

  register_scenario(
    "enqueue_workflow",
    description: BUILTIN_OPERATION_DESCRIPTIONS.fetch("enqueue_workflow"),
    setup: proc(&:seed_pending_workflows),
  ) do |context|
    context.store.enqueue_workflow(
      name: DEFAULT_WORKFLOW_NAME,
      input: { "kind" => "enqueue" },
      id: "hot-path-enqueue",
      worker_pool: DEFAULT_WORKER_POOL,
    )
  end

  register_scenario(
    "claim_runnable_workflow",
    description: BUILTIN_OPERATION_DESCRIPTIONS.fetch("claim_runnable_workflow"),
    setup: lambda do |context|
      context.enqueue_report_workflow
      context.seed_pending_workflows
    end,
  ) do |context|
    context.store.claim_runnable_workflow(
      worker_id: DEFAULT_WORKER_ID,
      lease_seconds: 60,
      workflow_names: [DEFAULT_WORKFLOW_NAME],
      worker_pool: DEFAULT_WORKER_POOL,
    )
  end

  register_scenario(
    "claim_target_activation",
    description: BUILTIN_OPERATION_DESCRIPTIONS.fetch("claim_target_activation"),
    setup: lambda do |context|
      context.seed_target_activations
      context.store.rearm_target_activation(
        target_kind: "workflow",
        target_type: DEFAULT_WORKFLOW_NAME,
        target_id: "hot-path-activation",
        ready_at: Time.now - 1,
        worker_pool: DEFAULT_WORKER_POOL,
      )
    end,
  ) do |context|
    context.store.claim_target_activation(
      worker_id: DEFAULT_WORKER_ID,
      lease_seconds: 60,
      target_kinds: ["workflow"],
      target_types: [DEFAULT_WORKFLOW_NAME],
      worker_pool: DEFAULT_WORKER_POOL,
    )
  end

  register_scenario(
    "worker_poll_idle",
    description: BUILTIN_OPERATION_DESCRIPTIONS.fetch("worker_poll_idle"),
    setup: proc(&:seed_pending_workflows),
  ) do |context|
    context.worker.tick
  end

  register_scenario(
    "worker_tick_claim",
    description: BUILTIN_OPERATION_DESCRIPTIONS.fetch("worker_tick_claim"),
    setup: lambda do |context|
      context.enqueue_report_workflow
      context.seed_pending_workflows
    end,
  ) do |context|
    context.worker.tick
  end
end

if $PROGRAM_NAME == __FILE__
  options = DurababbleMysqlHotPathReport.parse_options(ARGV)
  DurababbleMysqlHotPathReport::Runner.new(options).run
end
