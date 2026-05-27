# typed: false
# frozen_string_literal: true

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
    assert_equal ["transaction.begin", "query", "transaction.commit"], recorder.events.map { |event| event.fetch(:type) }
  end

  test "markdown report includes query SQL callsite explain and transaction details" do
    report = {
      operation: "claim_runnable_workflow",
      operation_description: DurababbleMysqlHotPathReport::OPERATIONS.fetch("claim_runnable_workflow"),
      database_url: "mysql://root@127.0.0.1/sidekick_server_test",
      schema: "durababble_hot_path_test",
      table_prefix: "durababble_hot_path_test",
      fixture_size: 0,
      result: "{\"id\"=>\"hot-path-claimable\"}",
      events: [],
      queries: [
        {
          sequence: 1,
          query_id: :mysql_claim_pending_workflow,
          description: Durababble::StoreQueries.description_for(:mysql_claim_pending_workflow),
          sql: "SELECT id FROM `durababble_hot_path_test_workflows` WHERE worker_pool = ?",
          params: ["default"],
          callsite: "lib/durababble/store/mysql.rb:34:in block in claim_runnable_workflow",
          transaction: { id: 1, depth: 1, parent_id: nil, options: {} },
          row_count: 1,
          affected_rows: 0,
          explain: {
            summary: [
              {
                "table" => "durababble_hot_path_test_workflows",
                "access_type" => "ref",
                "key" => "durababble_hot_path_test_workflows_queue_idx",
                "used_key_parts" => ["worker_pool", "status"],
                "rows_examined_per_scan" => 1,
              },
            ],
            json: {
              "query_block" => {
                "table" => {
                  "table_name" => "durababble_hot_path_test_workflows",
                  "access_type" => "ref",
                },
              },
            },
          },
        },
      ],
    }

    markdown = DurababbleMysqlHotPathReport::MarkdownRenderer.new(report).render

    assert_includes markdown, "Probe the pending workflow queue"
    assert_includes markdown, "tx1 depth=1"
    assert_includes markdown, "lib/durababble/store/mysql.rb"
    assert_includes markdown, "SELECT id FROM"
    assert_includes markdown, "durababble_hot_path_test_workflows_queue_idx"
  end
end
