# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "pg"

class DurababbleStoreTest < DurababbleTestCase
  class MysqlMigrationProbeStore < Durababble::MysqlStore
    attr_reader :executed

    def initialize(schema:, columns: {})
      super(nil, schema:)
      @columns = columns
      @executed = []
    end

    def execute_params(sql, params)
      @executed << [:execute_params, sql, params]
      table = params.first
      column = params[1]
      rows = if sql.include?("information_schema.columns")
        @columns.fetch(table, []).include?(column) ? [{ "exists" => 1 }] : []
      else
        []
      end
      Durababble::MysqlStore::MysqlResult.new(rows, rows.length)
    end

    def execute(sql)
      @executed << [:execute, sql]
      Durababble::MysqlStore::MysqlResult.new([], 0)
    end
  end

  test "migrates and persists workflow plus step state" do
    with_durababble_store(durababble_store_backends.first, "store_test") do |store|
      store.migrate!
      workflow_id = store.create_workflow(name: "demo", input: { "count" => 1 })

      store.record_step_started(workflow_id:, position: 0, name: "add_one")
      store.record_step_completed(workflow_id:, position: 0, result: { "count" => 2 })
      store.complete_workflow(workflow_id, result: { "count" => 2 })

      workflow = store.workflow(workflow_id)
      assert_equal "completed", workflow.fetch("status")
      assert_equal({ "count" => 2 }, workflow.fetch("result"))
      assert_equal "completed", store.steps_for(workflow_id).first.fetch("status")
    end
  end

  test "stores runtime values as Paquito bytea payloads instead of JSONB in Yugabyte" do
    with_yugabyte_store do |store|
      store.migrate!
      workflow_id = store.create_workflow(name: "demo", input: { "count" => 1 })
      store.complete_workflow(workflow_id, result: { "count" => 2 })

      columns = PG.connect(durababble_yugabyte_database_url) do |connection|
        connection.exec_params(<<~SQL, [schema]).map { |row| [row.fetch("table_name"), row.fetch("column_name"), row.fetch("data_type")] }
          SELECT table_name, column_name, data_type
          FROM information_schema.columns
          WHERE table_schema = $1
            AND column_name IN ('input', 'result', 'context', 'payload', 'heartbeat_cursor')
          ORDER BY table_name, column_name
        SQL
      end

      assert_includes columns, ["workflows", "input", "bytea"]
      assert_includes columns, ["workflows", "result", "bytea"]
      assert_includes columns, ["steps", "result", "bytea"]
      assert_includes columns, ["steps", "heartbeat_cursor", "bytea"]
      assert_includes columns, ["step_attempts", "heartbeat_cursor", "bytea"]
      assert_includes columns, ["waits", "context", "bytea"]
      assert_includes columns, ["outbox", "payload", "bytea"]
      refute columns.any? { |column| column.include?("jsonb") }

      encoded_input = PG.connect(durababble_yugabyte_database_url) do |connection|
        sql = "SELECT input FROM #{PG::Connection.quote_ident(schema)}.workflows WHERE id = $1"
        connection.exec_params(sql, [workflow_id]).first.fetch("input")
      end
      payload = PG::Connection.unescape_bytea(encoded_input)
      assert_equal 1, payload.bytes.first
      assert_equal({ "count" => 1 }, Durababble::Store::SERIALIZER.load(payload))
    end
  end

  test "creates queue and recovery indexes for production-sized tables in Yugabyte" do
    with_yugabyte_store do |store|
      store.migrate!

      indexes = PG.connect(durababble_yugabyte_database_url) do |connection|
        connection.exec_params(<<~SQL, [schema]).map { |row| row.fetch("indexname") }
          SELECT indexname
          FROM pg_indexes
          WHERE schemaname = $1
          ORDER BY indexname
        SQL
      end

      assert_includes indexes, "workflows_queue_idx"
      assert_includes indexes, "workflows_runnable_due_idx"
      assert_includes indexes, "workflows_expired_lease_idx"
      assert_includes indexes, "waits_event_pending_idx"
      assert_includes indexes, "waits_timer_pending_idx"
      assert_includes indexes, "waits_workflow_created_idx"
      assert_includes indexes, "step_attempts_workflow_started_position_idx"
      assert_includes indexes, "step_attempts_workflow_position_status_started_idx"
      assert_includes indexes, "outbox_queue_idx"
      assert_includes indexes, "outbox_expired_lease_idx"
    end
  end

  test "does not claim work when a worker pool has no registered workflow names" do
    with_durababble_store(durababble_store_backends.first, "store_test") do |store|
      store.migrate!
      workflow_id = store.enqueue_workflow(name: "unserved", input: {})

      assert_nil store.claim_runnable_workflow(worker_id: "worker-a", lease_seconds: 60, workflow_names: [])
      assert_hash_includes store.workflow(workflow_id), "status" => "pending", "locked_by" => nil
    end
  end

  test "reports missing workflow lease and cursor lookups as absent" do
    with_durababble_store(durababble_store_backends.first, "store_test") do |store|
      store.migrate!

      assert_raises_matching(KeyError, /missing/) { store.workflow("missing") }
      assert_nil store.current_workflow_lease("missing")
      assert_nil store.step_heartbeat_cursor(workflow_id: "missing", position: 0)
    end
  end

  test "can mark a workflow running under a concrete worker lease" do
    with_durababble_store(durababble_store_backends.first, "store_test") do |store|
      store.migrate!
      workflow_id = store.enqueue_workflow(name: "demo", input: {})

      store.mark_workflow_running(workflow_id, worker_id: "owner", lease_seconds: 60)

      assert_hash_includes store.current_workflow_lease(workflow_id), "worker_id" => "owner"
    end
  end

  test "migrates legacy JSONB runtime columns into Paquito bytea columns in Yugabyte" do
    with_yugabyte_store do |store|
      connection = PG.connect(durababble_yugabyte_database_url)
      connection.exec("CREATE SCHEMA #{PG::Connection.quote_ident(schema)}")
      connection.exec(<<~SQL)
        CREATE TABLE #{PG::Connection.quote_ident(schema)}.workflows (
          id text PRIMARY KEY,
          name text NOT NULL,
          status text NOT NULL,
          input jsonb NOT NULL DEFAULT '{}'::jsonb,
          result jsonb,
          error text,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      SQL
      connection.exec_params(
        "INSERT INTO #{PG::Connection.quote_ident(schema)}.workflows (id, name, status, input, result) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)",
        ["legacy", "demo", "completed", '{"count":1}', '{"count":2}'],
      )
      connection.close

      store.migrate!

      assert_equal({ "count" => 1 }, store.workflow("legacy").fetch("input"))
      assert_equal({ "count" => 2 }, store.workflow("legacy").fetch("result"))
      data_type = PG.connect(durababble_yugabyte_database_url) do |verify|
        verify.exec_params(<<~SQL, [schema, "workflows", "input"]).first.fetch("data_type")
          SELECT data_type
          FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
        SQL
      end
      assert_equal("bytea", data_type)
    ensure
      connection&.close unless connection&.finished?
    end
  end

  test "adds missing MySQL workflow cancellation columns only once" do
    store = MysqlMigrationProbeStore.new(
      schema: "mysql_schema",
      columns: { "mysql_schema_workflows" => ["cancel_reason"] },
    )

    store.send(:add_column_if_missing, "workflows", "cancel_reason", "TEXT")
    store.send(:add_column_if_missing, "workflows", "cancel_requested_at", "DATETIME(6)")

    executed_sql = store.executed.select { |kind, _sql, _params| kind == :execute }.map { |_kind, sql| sql }
    assert_equal ["ALTER TABLE `mysql_schema_workflows` ADD COLUMN `cancel_requested_at` DATETIME(6)"], executed_sql
  end

  private

  def with_yugabyte_store(&block)
    skip_without_yugabyte!
    require "pg"

    backend = DurababbleStoreBackend.new(
      name: "yugabyte",
      database_url: durababble_yugabyte_database_url,
      default_schema_prefix: "durababble_yb",
    )
    with_durababble_store(backend, "store_test", &block)
  end
end
