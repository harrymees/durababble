# typed: false
# frozen_string_literal: true

require "tempfile"

require_relative "../test_helper"
require_relative "../../scripts/mysql-hot-path-report"

class DurababbleMysqlHotPathReportTest < DurababbleTestCase
  test "records colocated query descriptions and deterministic transaction context" do
    recorder = DurababbleMysqlHotPathReport::SqlRecorder.new(root: File.expand_path("../..", __dir__))
    transaction = recorder.begin_transaction(isolation: :read_committed)
    query = recorder.record_query(
      query_id: :mysql_claim_pending_workflow,
      sql: "SELECT id FROM workflows WHERE status = 'pending'",
      params: ["default"],
      callsite: "lib/durababble/store/mysql.rb:34:in block in claim_runnable_workflow",
    )
    recorder.finish_query(query, ActiveRecord::Result.empty(affected_rows: 0))
    recorder.end_transaction(transaction, "commit")

    assert_equal "Probe the pending workflow queue for the oldest runnable candidate in this worker pool.", query.fetch(:description)
    assert_equal 1, query.fetch(:transaction).fetch(:id)
    assert_equal 1, query.fetch(:transaction).fetch(:depth)
    assert_equal({ isolation: "read_committed" }, query.fetch(:transaction).fetch(:options))
    assert_operator query.fetch(:duration_ms), :>=, 0.0
    assert_equal ["transaction.begin", "query", "transaction.commit"], recorder.events.map { |event| event.fetch(:type) }
  end

  test "markdown report includes query SQL callsite explain and transaction details" do
    markdown = DurababbleMysqlHotPathReport::MarkdownRenderer.new(hot_path_report_fixture).render

    assert_includes markdown, "Probe the pending workflow queue"
    assert_includes markdown, "0.123ms"
    assert_includes markdown, "tx1 depth=1"
    assert_includes markdown, "lib/durababble/store/mysql.rb"
    assert_includes markdown, "SELECT id FROM"
    assert_includes markdown, "durababble_hot_path_test_workflows_queue_idx"
    assert_includes markdown, "Using index condition"
    assert_includes markdown, "## Schema Appendix"
    assert_includes markdown, "CREATE TABLE `durababble_hot_path_test_workflows`"
    refute_includes markdown, "Traditional MySQL EXPLAIN"
  end

  test "html report syntax highlights SQL" do
    html = DurababbleMysqlHotPathReport::HtmlRenderer.new(hot_path_report_fixture).render

    assert_includes html, '<pre class="sql"><code><span class="sql-keyword">SELECT</span>'
    assert_includes html, '<span class="sql-keyword">WHERE</span>'
    assert_includes html, '<span class="sql-identifier sql-table" tabindex="0">`durababble_hot_path_test_workflows`'
    assert_includes html, '<span class="sql-string">&#39;pending&#39;</span>'
    assert_includes html, '<span class="sql-placeholder">?</span>'
  end

  test "html report shows table ddl popovers" do
    html = DurababbleMysqlHotPathReport::HtmlRenderer.new(hot_path_report_fixture).render

    assert_includes html, 'class="schema-popover"'
    assert_includes html, '<span class="sql-keyword">CREATE</span> <span class="sql-keyword">TABLE</span>'
    assert_includes html, '<span class="sql-keyword">PRIMARY</span> <span class="sql-keyword">KEY</span>'
  end

  test "html report renders traditional explain without raw JSON" do
    html = DurababbleMysqlHotPathReport::HtmlRenderer.new(hot_path_report_fixture).render

    assert_includes html, "<summary>EXPLAIN plan</summary>"
    assert_includes html, 'class="explain-card"'
    assert_includes html, "Possible keys"
    assert_includes html, "Chosen key"
    assert_includes html, "access: ref"
    assert_includes html, "Using index condition"
    refute_includes html, "EXPLAIN JSON"
    refute_includes html, "query_block"
    refute_includes html, "Traditional MySQL EXPLAIN"
  end

  test "table name extraction ignores index names" do
    sql = "SELECT id FROM `dura_test_workflows` FORCE INDEX (`dura_test_workflows_queue_idx`) WHERE status = ?"

    assert_equal ["dura_test_workflows"], DurababbleMysqlHotPathReport.table_names_from_sql(sql, table_prefix: "dura_test")
  end

  test "scenario files register reusable custom hot paths" do
    original_scenarios = DurababbleMysqlHotPathReport.scenarios.dup

    Tempfile.create(["durababble-hot-path-scenario", ".rb"]) do |file|
      file.write(<<~RUBY)
        DurababbleMysqlHotPathReport.register_scenario(
          "custom_store_read",
          description: "Trace a custom store read.",
          setup: ->(context) { context.seed_pending_workflows(2, name: "ignored") },
        ) do |context|
          context.store.workflow("workflow-1")
        end
      RUBY
      file.flush

      options = DurababbleMysqlHotPathReport.parse_options(["--scenario-file", file.path, "--format", "markdown"])

      assert_equal("custom_store_read", options.fetch(:operation))
      assert_equal("Trace a custom store read.", DurababbleMysqlHotPathReport.scenario_for("custom_store_read").description)
    end
  ensure
    DurababbleMysqlHotPathReport.instance_variable_set(:@scenarios, original_scenarios) if original_scenarios
  end

  private

  def hot_path_report_fixture
    {
      operation: "claim_runnable_workflow",
      operation_description: DurababbleMysqlHotPathReport::OPERATIONS.fetch("claim_runnable_workflow"),
      database_url: "mysql://root@127.0.0.1/sidekick_server_test",
      schema: "durababble_hot_path_test",
      table_prefix: "durababble_hot_path_test",
      fixture_size: 0,
      result: "{\"id\"=>\"hot-path-claimable\"}",
      total_query_runtime_ms: 0.123,
      table_ddls: {
        "durababble_hot_path_test_workflows" => <<~SQL.strip,
          CREATE TABLE `durababble_hot_path_test_workflows` (
            `id` varchar(191) NOT NULL,
            `worker_pool` varchar(191) NOT NULL DEFAULT 'default',
            `status` varchar(64) NOT NULL,
            PRIMARY KEY (`id`),
            KEY `durababble_hot_path_test_workflows_queue_idx` (`worker_pool`,`status`,`created_at`)
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        SQL
      },
      events: [],
      queries: [
        {
          sequence: 1,
          query_id: :mysql_claim_pending_workflow,
          description: Durababble::StoreQueries.description_for(:mysql_claim_pending_workflow),
          sql: "SELECT id FROM `durababble_hot_path_test_workflows` WHERE status = 'pending' AND worker_pool = ?",
          params: ["default"],
          callsite: "lib/durababble/store/mysql.rb:34:in block in claim_runnable_workflow",
          transaction: { id: 1, depth: 1, parent_id: nil, options: {} },
          duration_ms: 0.123,
          row_count: 1,
          affected_rows: 0,
          explain: {
            rows: [
              {
                "id" => 1,
                "select_type" => "SIMPLE",
                "table" => "durababble_hot_path_test_workflows",
                "partitions" => nil,
                "type" => "ref",
                "possible_keys" => "durababble_hot_path_test_workflows_queue_idx",
                "key" => "durababble_hot_path_test_workflows_queue_idx",
                "key_len" => "1022",
                "ref" => "const,const",
                "rows" => 1,
                "filtered" => 100.0,
                "Extra" => "Using index condition; Using where",
              },
            ],
          },
        },
      ],
    }
  end
end
