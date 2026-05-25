# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/scripted_sql"

require "pg"

class DurababbleStoreTest < DurababbleTestCase
  include DurababbleScriptedSqlSupport

  ScriptedPgConnection = DurababbleScriptedSqlSupport::ScriptedPgConnection
  ScriptedMysqlConnection = DurababbleScriptedSqlSupport::ScriptedMysqlConnection
  FlakyDeliveryClient = DurababbleScriptedSqlSupport::FlakyDeliveryClient
  MysqlMigrationProbeStore = DurababbleScriptedSqlSupport::MysqlMigrationProbeStore

  test "routes active record connections through mysql and postgres adapters" do
    assert_kind_of Durababble::MysqlStore, Durababble::Store.from_active_record(connection: ScriptedMysqlConnection.new, schema: "schema")
    assert_kind_of Durababble::PostgresStore, Durababble::Store.from_active_record(connection: ScriptedPgConnection.new, schema: "schema")
    assert_kind_of Durababble::PostgresStore, Durababble::Store.new(ScriptedPgConnection.new, schema: "schema")
    assert_raises(ArgumentError) { Durababble::Store.new(schema: "schema") }
    assert_raises(ArgumentError) { Durababble::Store.from_active_record(schema: "schema") }

    pool = Struct.new(:connection) do
      def lease_connection = connection
    end.new(ScriptedMysqlConnection.new)
    owner_pool = Struct.new(:disconnected) do
      def disconnect!
        self.disconnected = true
      end
    end.new(false)
    owner = Struct.new(:connection_pool).new(owner_pool)
    store = Durababble::Store.from_active_record(connection_pool: pool, schema: "schema", owner:)
    assert_kind_of Durababble::MysqlStore, store
    store.close
    assert owner_pool.disconnected

    unsupported = Object.new
    unsupported.define_singleton_method(:adapter_name) { "SQLite" }
    assert_raises(ArgumentError) { Durababble::Store.from_active_record(connection: unsupported, schema: "schema") }
    assert_equal "postgresql", Durababble::Store.send(:active_record_config_for, "postgres://user:pass@example.test:5432/db").fetch(:adapter)
    assert_equal "trilogy", Durababble::Store.send(:active_record_config_for, "mysql://user:pass@example.test:3306/db").fetch(:adapter)
    assert_equal "trilogy", Durababble::Store.send(:active_record_config_for, "trilogy://user:pass@example.test:3306/db").fetch(:adapter)
    assert_equal "sqlite", Durababble::Store.send(:active_record_config_for, "sqlite:///tmp/durababble.sqlite").fetch(:adapter)
  end

  test "inbox_row_claimable? rejects blocked inbox statuses" do
    store = shared_store
    now = Time.utc(2024, 1, 1)
    refute store.send(:inbox_row_claimable?, { "status" => "dead_lettered" }, now:)
    refute store.send(:inbox_row_claimable?, { "status" => "running", "locked_until" => nil }, now:)
    refute store.send(:inbox_row_claimable?, { "status" => "running", "locked_until" => "2024-01-02T00:00:00Z" }, now:)
    refute store.send(:inbox_row_claimable?, { "status" => "pending", "ready_at" => "2024-01-02T00:00:00Z" }, now:)
  end

  test "inbox_row_claimable? allows expired running and ready pending rows" do
    store = shared_store
    now = Time.utc(2024, 1, 1)
    assert store.send(:inbox_row_claimable?, { "status" => "running", "locked_until" => "2023-12-31T00:00:00Z" }, now:)
    assert store.send(:inbox_row_claimable?, { "status" => "pending", "ready_at" => nil }, now:)
  end

  test "object_command_message? recognizes legacy and object ask messages" do
    store = shared_store
    refute store.send(:object_command_message?, nil)
    assert store.send(:object_command_message?, { "message_kind" => "ask" })
    assert store.send(:object_command_message?, { "target_kind" => "object", "message_kind" => "ask" })
    refute store.send(:object_command_message?, { "target_kind" => "workflow", "message_kind" => "ask" })
  end

  test "object_command_row returns legacy command rows unchanged" do
    store = shared_store
    legacy = { "id" => "legacy" }
    assert_same legacy, store.send(:object_command_row, legacy)
  end

  test "object_command_row maps object payload fields onto command rows" do
    store = shared_store
    assert_equal(
      {
        "target_kind" => "object",
        "target_type" => "Cart",
        "target_id" => "cart-1",
        "method_name" => "checkout",
        "payload" => { "method_name" => "checkout", "args" => [1], "kwargs" => { "fast" => true } },
        "object_type" => "Cart",
        "object_id" => "cart-1",
        "args" => [1],
        "kwargs" => { "fast" => true },
      },
      store.send(:object_command_row, {
        "target_kind" => "object",
        "target_type" => "Cart",
        "target_id" => "cart-1",
        "payload" => { "method_name" => "checkout", "args" => [1], "kwargs" => { "fast" => true } },
      }),
    )
  end

  test "with_command_id backfills command_id from legacy position" do
    store = shared_store
    assert_equal({ "id" => "no-position" }, store.send(:with_command_id, { "id" => "no-position" }))
    assert_equal({ "position" => 3, "command_id" => 9 }, store.send(:with_command_id, { "position" => 3, "command_id" => 9 }))
    assert_equal({ "position" => 3, "command_id" => 3 }, store.send(:with_command_id, { "position" => 3 }))
  end

  test "migrates and persists workflow plus step state" do
    with_durababble_store(durababble_store_backends.first, "store_test") do |store|
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
    with_yugabyte_store do
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
      workflow_id = store.enqueue_workflow(name: "unserved", input: {})

      assert_nil store.claim_runnable_workflow(worker_id: "worker-a", lease_seconds: 60, workflow_names: [])
      assert_hash_includes store.workflow(workflow_id), "status" => "pending", "locked_by" => nil
    end
  end

  test "reports missing workflow lease and cursor lookups as absent" do
    with_durababble_store(durababble_store_backends.first, "store_test") do |store|
      assert_raises_matching(KeyError, /missing/) { store.workflow("missing") }
      assert_nil store.current_workflow_lease("missing")
      assert_nil store.step_heartbeat_cursor(workflow_id: "missing", position: 0)
    end
  end

  test "can mark a workflow running under a concrete worker lease" do
    with_durababble_store(durababble_store_backends.first, "store_test") do |store|
      workflow_id = store.enqueue_workflow(name: "demo", input: {})

      store.mark_workflow_running(workflow_id, worker_id: "owner", lease_seconds: 60)

      assert_hash_includes store.current_workflow_lease(workflow_id), "worker_id" => "owner"
    end
  end

  test "migrates legacy JSONB runtime columns into Paquito bytea columns in Yugabyte" do
    with_yugabyte_store(migrate: false) do |store|
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

  test "handles postgres queue, lease, wait, fence, outbox, and object command miss paths" do
    connection = ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "pending", "created_at" => "2024-01-02T00:00:00Z" }]),
      sql_result([{ "id" => "failed", "created_at" => "2024-01-01T00:00:00Z" }]),
      sql_result,
      sql_result,
      sql_result([{ "id" => "failed", "input" => pg_dump({ "count" => 1 }) }]),
      sql_result([{ "id" => "wf", "input" => pg_dump({ "ok" => true }) }]),
      sql_result,
      sql_result,
      sql_result([{ "locked_until" => "future" }]),
      sql_result,
      sql_result([{ "locked_until" => "future" }]),
      sql_result([{ "heartbeat_cursor" => pg_dump({ "cursor" => 1 }) }]),
      sql_result,
      sql_result([{ "workflow_id" => "wf", "worker_id" => "other", "locked_until" => "future" }]),
      sql_result,
      sql_result([{ "state" => pg_dump({ "value" => 3 }) }]),
      sql_result([{ "id" => "cmd" }]),
      sql_result,
      sql_result([{ "id" => "cmd", "args" => pg_dump([]), "kwargs" => pg_dump({}) }]),
      sql_result,
      sql_result,
      sql_result,
      sql_result,
      sql_result([{ "id" => "outbox-old" }]),
      sql_result,
      sql_result([{ "id" => "outbox-new", "created_at" => "2024-01-01T00:00:00Z" }]),
      sql_result([{ "id" => "outbox-new", "payload" => pg_dump({ "ok" => true }) }]),
      sql_result([{ "id" => "wf", "input" => pg_dump({}) }]),
      sql_result,
    ])
    store = pg_store(connection)

    assert_nil store.claim_runnable_workflow(worker_id: "w", lease_seconds: 5, workflow_names: [])
    assert_equal({ "id" => "failed", "input" => { "count" => 1 } }, store.claim_runnable_workflow(worker_id: "w", lease_seconds: 5))
    assert_equal({ "id" => "wf", "input" => { "ok" => true } }, store.claim_workflow(workflow_id: "wf", worker_id: "w", lease_seconds: 5))
    assert_nil store.claim_workflow(workflow_id: "missing", worker_id: "w", lease_seconds: 5)
    assert_nil store.heartbeat_step(workflow_id: "wf", position: 0, worker_id: "w", lease_seconds: 5, cursor: { "cursor" => 1 })
    assert_equal "future", store.heartbeat_step(workflow_id: "wf", position: 0, worker_id: "w", lease_seconds: 5, cursor: { "cursor" => 1 })
    assert_equal({ "workflow_id" => "wf", "worker_id" => "other", "locked_until" => "future" }, store.current_workflow_lease("wf"))
    assert_nil store.object_state(object_type: "account", object_id: "1")
    assert_equal({ "value" => 3 }, store.object_state(object_type: "account", object_id: "1"))
    store.complete_object_command(command_id: "cmd", result: "ignored", worker_id: "w")
    store.complete_object_command(command_id: "cmd", result: "ok", object_type: "account", object_id: "1", state: { "value" => 4 }, worker_id: "w")
    store.fail_object_command(command_id: "cmd", error: "boom", worker_id: "w")
    store.fail_object_command(command_id: "cmd", error: "boom")
    assert_equal "outbox-old", store.enqueue_outbox(workflow_id: "wf", topic: "email", payload: {}, key: "k")
    assert_equal({ "id" => "outbox-new", "payload" => { "ok" => true } }, store.claim_outbox(worker_id: "w", lease_seconds: 5))
    assert_equal({ "id" => "wf", "input" => {} }, store.workflow("wf"))
    assert_raises(KeyError) { store.workflow("missing") }
  end

  test "handles postgres inbox idempotency branches" do
    store = pg_store
    shape_hash = store.send(
      :inbox_shape_hash,
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf-1",
      message_kind: "workflow_signal",
      method_name: nil,
      payload: { "approved" => true },
    )
    new_connection = ScriptedPgConnection.new(params_results: [
      sql_result,
      sql_result,
      sql_result([{ "last_sequence" => "0" }]),
      sql_result,
      sql_result,
    ])
    new_id = pg_store(new_connection).enqueue_inbox_message(
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf-1",
      message_kind: "workflow_signal",
      payload: { "approved" => true },
      idempotency_key: "signal:wf-1",
    )

    assert_match(/\A[0-9a-f-]{36}\z/, new_id)
    inbox_insert = new_connection.exec_params_calls.find do |sql, _params|
      sql.include?("INSERT INTO") && sql.include?("inbox") && !sql.include?("target_activations")
    end
    assert_equal "signal:wf-1", inbox_insert.fetch(1)[8]

    duplicate = pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "existing-inbox-id", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => shape_hash }]),
    ])).enqueue_inbox_message(
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf-1",
      message_kind: "workflow_signal",
      payload: { "approved" => true },
      idempotency_key: "signal:wf-1",
    )
    assert_equal "existing-inbox-id", duplicate

    pending_duplicate = pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "pending-inbox-id", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "pending", "ready_at" => nil, "shape_hash" => shape_hash }]),
      sql_result,
    ])).enqueue_inbox_message(
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf-1",
      message_kind: "workflow_signal",
      payload: { "approved" => true },
      idempotency_key: "signal:wf-1",
    )
    assert_equal "pending-inbox-id", pending_duplicate

    assert_raises(Durababble::IdempotencyKeyConflict) do
      pg_store(ScriptedPgConnection.new(params_results: [
        sql_result([{ "id" => "existing-inbox-id", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => "different" }]),
      ])).enqueue_inbox_message(
        target_kind: "workflow",
        target_type: "approval",
        target_id: "wf-1",
        message_kind: "workflow_signal",
        payload: { "approved" => true },
        idempotency_key: "signal:wf-1",
      )
    end
  end

  test "handles postgres workflow command enqueue branches" do
    store = pg_store
    payload = { "method" => "approve", "args" => [], "kwargs" => { reason: "ok" } }
    shape_hash = store.send(
      :inbox_shape_hash,
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf-1",
      message_kind: "workflow_command",
      method_name: "approve",
      payload:,
    )
    new_connection = ScriptedPgConnection.new(params_results: [
      sql_result,
      sql_result([{ "id" => "wf-1", "status" => "running", "next_run_at" => nil }]),
      sql_result,
      sql_result([{ "last_sequence" => "0" }]),
      sql_result,
      sql_result,
      sql_result,
    ])
    new_id = pg_store(new_connection).enqueue_workflow_command(
      workflow_id: "wf-1",
      workflow_name: "approval",
      method_name: "approve",
      payload:,
      idempotency_key: "approve:1",
    )

    assert_match(/\A[0-9a-f-]{36}\z/, new_id)
    assert new_connection.exec_params_calls.any? { |sql, _params| sql.include?("SELECT * FROM") && sql.include?("workflows") && sql.include?("FOR UPDATE") }

    duplicate = pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "existing-command", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => shape_hash }]),
    ])).enqueue_workflow_command(
      workflow_id: "wf-1",
      workflow_name: "approval",
      method_name: "approve",
      payload:,
      idempotency_key: "approve:1",
    )
    assert_equal "existing-command", duplicate

    pending_duplicate = pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "pending-command", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "pending", "ready_at" => nil, "shape_hash" => shape_hash }]),
      sql_result,
    ])).enqueue_workflow_command(
      workflow_id: "wf-1",
      workflow_name: "approval",
      method_name: "approve",
      payload:,
      idempotency_key: "approve:1",
    )
    assert_equal "pending-command", pending_duplicate

    assert_raises(Durababble::IdempotencyKeyConflict) do
      pg_store(ScriptedPgConnection.new(params_results: [
        sql_result([{ "id" => "existing-command", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => "different" }]),
      ])).enqueue_workflow_command(
        workflow_id: "wf-1",
        workflow_name: "approval",
        method_name: "approve",
        payload:,
        idempotency_key: "approve:1",
      )
    end

    assert_raises(KeyError) do
      pg_store(ScriptedPgConnection.new(params_results: [sql_result, sql_result]))
        .enqueue_workflow_command(workflow_id: "missing", workflow_name: "approval", method_name: "approve", payload:)
    end
    assert_raises_matching(Durababble::Error, /terminal/) do
      pg_store(ScriptedPgConnection.new(params_results: [
        sql_result([{ "id" => "wf-1", "status" => "completed", "next_run_at" => nil }]),
      ])).enqueue_workflow_command(workflow_id: "wf-1", workflow_name: "approval", method_name: "approve", payload:)
    end
  end

  test "handles mysql inbox idempotency branches" do
    store = mysql_store
    shape_hash = store.send(
      :inbox_shape_hash,
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf-1",
      message_kind: "workflow_signal",
      method_name: nil,
      payload: { "approved" => true },
    )
    new_connection = ScriptedMysqlConnection.new do |sql|
      sql_result([{ "last_sequence" => 0 }]) if sql.include?("SELECT last_sequence")
    end
    new_id = mysql_store(new_connection).enqueue_inbox_message(
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf-1",
      message_kind: "workflow_signal",
      payload: { "approved" => true },
      idempotency_key: "signal:wf-1",
    )

    assert_match(/\A[0-9a-f-]{36}\z/, new_id)
    inbox_insert = new_connection.queries.find do |sql|
      sql.include?("INSERT INTO") && sql.include?("`branch_schema_inbox`")
    end
    assert_includes inbox_insert, "'signal:wf-1'"
    refute_includes inbox_insert, "retained_until"

    duplicate = mysql_store(ScriptedMysqlConnection.new do |sql|
      if sql.include?("idempotency_key =")
        sql_result([{ "id" => "existing-inbox-id", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => shape_hash }])
      end
    end).enqueue_inbox_message(
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf-1",
      message_kind: "workflow_signal",
      payload: { "approved" => true },
      idempotency_key: "signal:wf-1",
    )
    assert_equal "existing-inbox-id", duplicate

    pending_duplicate_connection = ScriptedMysqlConnection.new do |sql|
      if sql.include?("idempotency_key =")
        sql_result([{ "id" => "pending-inbox-id", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "pending", "ready_at" => nil, "shape_hash" => shape_hash }])
      end
    end
    pending_duplicate = mysql_store(pending_duplicate_connection).enqueue_inbox_message(
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf-1",
      message_kind: "workflow_signal",
      payload: { "approved" => true },
      idempotency_key: "signal:wf-1",
    )
    assert_equal "pending-inbox-id", pending_duplicate
    assert pending_duplicate_connection.queries.any? { |sql| sql.include?("INSERT INTO `branch_schema_target_activations`") }

    assert_raises(Durababble::IdempotencyKeyConflict) do
      mysql_store(ScriptedMysqlConnection.new do |sql|
        if sql.include?("idempotency_key =")
          sql_result([{ "id" => "existing-inbox-id", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => "different" }])
        end
      end).enqueue_inbox_message(
        target_kind: "workflow",
        target_type: "approval",
        target_id: "wf-1",
        message_kind: "workflow_signal",
        payload: { "approved" => true },
        idempotency_key: "signal:wf-1",
      )
    end
  end

  test "handles postgres target activation and inbox wait branches" do
    assert_nil pg_store.claim_target_activation(worker_id: "w", lease_seconds: 5, target_kinds: [])
    assert_nil pg_store.claim_target_activation(worker_id: "w", lease_seconds: 5, target_types: [])

    assert_equal(
      { "id" => "wf", "input" => {} },
      pg_store(ScriptedPgConnection.new(params_results: [
        sql_result([{ "id" => "wf", "input" => pg_dump({}) }]),
      ])).claim_workflow_for_activation(workflow_id: "wf", worker_id: "w", lease_seconds: 5),
    )
    assert_equal(
      { "id" => "wf", "input" => { "claimed" => true } },
      pg_store(ScriptedPgConnection.new(params_results: [
        sql_result,
        sql_result([{ "id" => "wf", "input" => pg_dump({ "claimed" => true }) }]),
      ])).claim_workflow_for_activation(workflow_id: "wf", worker_id: "w", lease_seconds: 5),
    )
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result, sql_result]))
      .claim_workflow_for_activation(workflow_id: "wf", worker_id: "w", lease_seconds: 5)

    activation = pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "target_kind" => "workflow", "target_type" => "late", "target_id" => "wf-late", "ready_at" => "2024-01-01T00:00:00Z", "created_at" => "2024-01-02T00:00:00Z" }]),
      sql_result([{ "target_kind" => "workflow", "target_type" => "early", "target_id" => "wf-early", "ready_at" => "2024-01-01T00:00:00Z", "created_at" => "2024-01-01T00:00:00Z" }]),
      sql_result([{ "target_kind" => "workflow", "target_type" => "early", "target_id" => "wf-early" }]),
    ])).claim_target_activation(worker_id: "w", lease_seconds: 5, target_kinds: ["workflow"], target_types: ["early"])
    assert_hash_includes activation, "target_type" => "early", "target_id" => "wf-early"

    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result, sql_result]))
      .claim_target_activation(worker_id: "w", lease_seconds: 5)
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result]))
      .complete_target_activation(target_kind: "workflow", target_type: "approval", target_id: "wf", worker_id: "w")
    pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "present" => 1 }]),
      sql_result([{ "status" => "pending", "ready_at" => nil }]),
      sql_result,
    ])).complete_target_activation(target_kind: "workflow", target_type: "approval", target_id: "wf", worker_id: "w")
    pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "present" => 1 }]),
      sql_result([{ "status" => "dead_lettered", "ready_at" => nil }]),
      sql_result,
    ])).complete_target_activation(target_kind: "workflow", target_type: "approval", target_id: "wf", worker_id: "w")

    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result]))
      .target_activation(target_kind: "workflow", target_type: "approval", target_id: "wf")
    assert_hash_includes(
      pg_store(ScriptedPgConnection.new(params_results: [sql_result([{ "target_id" => "wf" }])]))
        .target_activation(target_kind: "workflow", target_type: "approval", target_id: "wf"),
      "target_id" => "wf",
    )

    helper = pg_store
    assert_nil helper.send(:existing_inbox_message_for_idempotency, nil, target_kind: "workflow", target_type: "approval", target_id: "wf")
    assert helper.send(:activatable_inbox_status?, "pending")
    refute helper.send(:activatable_inbox_status?, "completed")
    assert_equal ["", []], helper.send(:target_activation_filter, target_kinds: nil, target_types: nil)
    malicious_kind = "workflow'); DROP TABLE inbox; --"
    assert_equal ["AND target_kind IN ($2)", [malicious_kind]], helper.send(:target_activation_filter, target_kinds: [malicious_kind], target_types: nil, offset: 2)
    assert_equal ["AND target_type IN ($2)", ["approval"]], helper.send(:target_activation_filter, target_kinds: nil, target_types: ["approval"], offset: 2)
    assert_equal Time.utc(2024, 1, 1), helper.send(:target_activation_ready_at_for, { "status" => "pending", "ready_at" => nil }, now: Time.utc(2024, 1, 1))
    assert_equal "2024-01-02T00:00:00Z", helper.send(:target_activation_ready_at_for, { "status" => "pending", "ready_at" => "2024-01-02T00:00:00Z" }, now: Time.utc(2024, 1, 1))
    assert_equal "2024-01-02T00:00:00Z", helper.send(:target_activation_ready_at_for, { "status" => "running", "locked_until" => "2024-01-02T00:00:00Z" }, now: Time.utc(2024, 1, 1))
    refute helper.send(:inbox_row_claimable?, { "status" => "dead_lettered" }, now: Time.utc(2024, 1, 1))
    refute helper.send(:inbox_row_claimable?, { "status" => "running", "locked_until" => nil }, now: Time.utc(2024, 1, 1))
    assert helper.send(:inbox_row_claimable?, { "status" => "running", "locked_until" => "2023-12-31T00:00:00Z" }, now: Time.utc(2024, 1, 1))
    refute helper.send(:inbox_row_claimable?, { "status" => "pending", "ready_at" => "2024-01-02T00:00:00Z" }, now: Time.utc(2024, 1, 1))

    messages = [{ "status" => "completed", "result" => { "ok" => true } }]
    helper.define_singleton_method(:inbox_message) { |_message_id| messages.shift }
    assert_equal({ "ok" => true }, helper.wait_for_inbox_message("msg", poll_interval: 0, timeout: 0.01))
    messages = [{ "status" => "failed", "error" => "boom" }]
    assert_raises_matching(Durababble::Error, /boom/) { helper.wait_for_inbox_message("msg", poll_interval: 0, timeout: 0.01) }
    messages = [nil]
    assert_raises(KeyError) { helper.wait_for_inbox_message("msg", poll_interval: 0, timeout: 0.01) }
    messages = [{ "status" => "pending" }]
    assert_raises(Durababble::CommandTimeout) { helper.wait_for_inbox_message("msg", poll_interval: 0, timeout: 0) }
    messages = [{ "status" => "pending" }, { "status" => "completed", "result" => { "later" => true } }]
    assert_equal({ "later" => true }, helper.wait_for_inbox_message("msg", poll_interval: 0, timeout: nil))

    pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "cmd", "target_kind" => "object", "target_type" => "account", "target_id" => "1" }]),
      sql_result,
      sql_result,
      sql_result,
    ])).complete_object_command(command_id: "cmd", result: "ok", worker_id: "w")
    pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "cmd", "target_kind" => "object", "target_type" => "account", "target_id" => "1" }]),
      sql_result,
      sql_result,
      sql_result,
    ])).fail_object_command(command_id: "cmd", error: "boom", worker_id: "w")
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result]))
      .complete_workflow_command(message_id: "msg", workflow_id: "wf", result: "ok", worker_id: "w")
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result]))
      .fail_workflow_command(message_id: "msg", workflow_id: "wf", error: "boom", worker_id: "w")
    pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "msg", "method_name" => "approve", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf" }]),
      sql_result,
      sql_result([{ "event_index" => "0" }]),
      sql_result,
      sql_result,
      sql_result,
      sql_result,
    ])).complete_workflow_command(message_id: "msg", workflow_id: "wf", result: "ok", worker_id: "w")
    pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "msg", "method_name" => "reject", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf" }]),
      sql_result,
      sql_result([{ "event_index" => "1" }]),
      sql_result,
      sql_result,
      sql_result,
      sql_result,
    ])).fail_workflow_command(message_id: "msg", workflow_id: "wf", error: "boom", worker_id: "w")
  end

  test "handles advisory target delivery retry and fallback branches" do
    pg_client = FlakyDeliveryClient.new(failures: 1)
    pg_delivered = pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "workflow_id" => "wf", "worker_id" => "127.0.0.1:12345", "locked_until" => Time.now + 60 }]),
    ])).deliver_target_message(
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf",
      client_factory: lambda do |address|
        assert_equal("127.0.0.1:12345", address)
        pg_client
      end,
    )
    assert_equal(true, pg_delivered)
    assert_equal(2, pg_client.deliveries.length)

    assert_equal false, pg_store.deliver_target_message(
      target_kind: "object",
      target_type: "account",
      target_id: "acct-1",
      client_factory: ->(_address) { raise "no lease should skip client construction" },
    )

    pg_down_client = FlakyDeliveryClient.new(failures: 2)
    assert_equal false, pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "workflow_id" => "wf", "worker_id" => "127.0.0.1:12345", "locked_until" => Time.now + 60 }]),
    ])).deliver_target_message(
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf",
      client_factory: ->(_address) { pg_down_client },
    )
    assert_equal 2, pg_down_client.deliveries.length

    mysql_client = FlakyDeliveryClient.new(failures: 1)
    mysql_connection = ScriptedMysqlConnection.new do |sql|
      if sql.include?("SELECT id AS workflow_id")
        sql_result([{ "workflow_id" => "wf", "worker_id" => "127.0.0.1:23456", "locked_until" => Time.now + 60 }])
      end
    end
    mysql_delivered = mysql_store(mysql_connection).deliver_target_message(
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf",
      client_factory: lambda do |address|
        assert_equal("127.0.0.1:23456", address)
        mysql_client
      end,
    )
    assert_equal(true, mysql_delivered)
    assert_equal(2, mysql_client.deliveries.length)
    assert_equal false, mysql_store.deliver_target_message(
      target_kind: "object",
      target_type: "account",
      target_id: "acct-1",
      client_factory: ->(_address) { raise "no lease should skip client construction" },
    )
  end

  test "terminal workflow updates choose fenced and unfenced SQL paths" do
    pg_fenced_connection = ScriptedPgConnection.new(params_results: [
      sql_result([], affected_rows: 1),
      sql_result([], affected_rows: 1),
      sql_result([], affected_rows: 1),
    ])
    pg_fenced_store = pg_store(pg_fenced_connection)
    pg_fenced_store.complete_workflow("wf", result: { "ok" => true }, worker_id: "worker-1")
    pg_fenced_store.cancel_workflow("wf", reason: "stop", result: nil, worker_id: "worker-1")
    pg_fenced_store.fail_workflow("wf", error: "boom", worker_id: "worker-1")

    assert_equal 5, pg_fenced_connection.exec_params_calls.length
    pg_fenced_workflow_updates = pg_fenced_connection.exec_params_calls.select { |sql, _params| sql.include?("UPDATE") && sql.include?("workflows") }
    assert_equal 3, pg_fenced_workflow_updates.length
    assert pg_fenced_workflow_updates.all? { |sql, _params| sql.include?("locked_by") && sql.include?("locked_until >= now()") }

    pg_unfenced_connection = ScriptedPgConnection.new(params_results: [
      sql_result([], affected_rows: 1),
      sql_result([], affected_rows: 1),
      sql_result([], affected_rows: 1),
    ])
    pg_unfenced_store = pg_store(pg_unfenced_connection)
    pg_unfenced_store.complete_workflow("wf", result: { "ok" => true })
    pg_unfenced_store.cancel_workflow("wf", reason: "stop")
    pg_unfenced_store.fail_workflow("wf", error: "boom")

    assert_equal 5, pg_unfenced_connection.exec_params_calls.length
    pg_unfenced_workflow_updates = pg_unfenced_connection.exec_params_calls.select { |sql, _params| sql.include?("UPDATE") && sql.include?("workflows") }
    assert_equal 3, pg_unfenced_workflow_updates.length
    assert pg_unfenced_workflow_updates.none? { |sql, _params| sql.include?("AND locked_by =") }

    mysql_fenced_connection = ScriptedMysqlConnection.new { sql_result([], affected_rows: 1) }
    mysql_fenced_store = mysql_store(mysql_fenced_connection)
    mysql_fenced_store.complete_workflow("wf", result: { "ok" => true }, worker_id: "worker-1")
    mysql_fenced_store.cancel_workflow("wf", reason: "stop", result: nil, worker_id: "worker-1")
    mysql_fenced_store.fail_workflow("wf", error: "boom", worker_id: "worker-1")

    assert_equal 5, mysql_fenced_connection.queries.length
    mysql_fenced_workflow_updates = mysql_fenced_connection.queries.select { |sql| sql.include?("UPDATE") && sql.include?("workflows") }
    assert_equal 3, mysql_fenced_workflow_updates.length
    assert mysql_fenced_workflow_updates.all? { |sql| sql.include?("locked_by") && sql.include?("locked_until >= NOW(6)") }

    mysql_unfenced_connection = ScriptedMysqlConnection.new
    mysql_unfenced_store = mysql_store(mysql_unfenced_connection)
    mysql_unfenced_store.complete_workflow("wf", result: { "ok" => true })
    mysql_unfenced_store.cancel_workflow("wf", reason: "stop")
    mysql_unfenced_store.fail_workflow("wf", error: "boom")

    assert_equal 5, mysql_unfenced_connection.queries.length
    mysql_unfenced_workflow_updates = mysql_unfenced_connection.queries.select { |sql| sql.include?("UPDATE") && sql.include?("workflows") }
    assert_equal 3, mysql_unfenced_workflow_updates.length
    assert mysql_unfenced_workflow_updates.none? { |sql| sql.include?("AND locked_by =") }

    stale_pg_store = pg_store(ScriptedPgConnection.new(params_results: [sql_result([], affected_rows: 0)]))
    assert_raises(Durababble::LeaseConflict) do
      stale_pg_store.complete_workflow("wf", result: { "ok" => true }, worker_id: "stale-worker")
    end
  end

  test "handles postgres fence replay, serialization migration, retry, and helper branches" do
    completed = { "status" => "completed", "result" => pg_dump({ "done" => true }), "error" => nil }
    failed = { "status" => "failed", "result" => nil, "error" => "boom" }
    connection = ScriptedPgConnection.new(
      params_results: [
        sql_result([], affected_rows: 1),
        sql_result([], affected_rows: 1),
        sql_result([], affected_rows: 0),
        sql_result([completed]),
        sql_result([], affected_rows: 0),
        sql_result([failed]),
        sql_result([{ "data_type" => "jsonb", "is_nullable" => "YES" }]),
        sql_result([{ "column_name" => "id" }]),
        sql_result,
        sql_result,
        sql_result([{ "data_type" => "bytea", "is_nullable" => "YES" }]),
        sql_result,
      ],
      exec_results: [
        sql_result,
        sql_result([
          { "id" => "one", "payload" => "{\"ok\":true}" },
          { "id" => "two", "payload" => nil },
        ]),
      ],
    )
    store = pg_store(connection)

    assert_equal({ "created" => true }, store.with_fence(workflow_id: "wf", key: "created") { { "created" => true } })
    assert_equal({ "done" => true }, store.with_fence(workflow_id: "wf", key: "done", timeout: 0))
    assert_raises(Durababble::Error) { store.with_fence(workflow_id: "wf", key: "failed", timeout: 0) }
    store.send(:migrate_serialized_column!, "outbox", "payload", not_null: true)
    store.send(:migrate_serialized_column!, "outbox", "payload")
    store.send(:migrate_serialized_column!, "outbox", "missing")

    attempts = 0
    result = store.send(:retry_serialization_failures) do
      attempts += 1
      raise ActiveRecord::SerializationFailure if attempts == 1

      :retried
    end
    assert_equal :retried, result
    assert_raises(ActiveRecord::SerializationFailure) do
      store.send(:retry_serialization_failures, max_attempts: 1) { raise ActiveRecord::SerializationFailure }
    end
    assert_equal ["", []], store.send(:workflow_name_filter, nil)
    malicious_name = "a'); DROP TABLE workflows; --"
    assert_equal ["AND name IN ($1, $2)", [malicious_name, "b"]], store.send(:workflow_name_filter, [malicious_name, "b"])
    assert_nil store.send(:timestamp_or_nil, nil)
  end

  test "handles isolated postgres adapter migration and retry edge paths" do
    migrated_connection = ScriptedPgConnection.new
    migration_store = pg_store(migrated_connection)
    assert_same migration_store, migration_store.migrate!
    assert migrated_connection.exec_calls.any? { |sql| sql.include?("CREATE TABLE IF NOT EXISTS") }

    migrated = pg_store
    migrated.instance_variable_set(:@migrated, true)
    assert_same migrated, migrated.migrate!

    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result, sql_result, sql_result]))
      .claim_runnable_workflow(worker_id: "w", lease_seconds: 5)
    assert_equal(
      { "id" => "wf", "input" => { "fresh" => true } },
      pg_store(ScriptedPgConnection.new(params_results: [
        sql_result,
        sql_result([{ "id" => "wf", "input" => pg_dump({ "fresh" => true }) }]),
      ])).claim_workflow(workflow_id: "wf", worker_id: "w", lease_seconds: 5),
    )
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result]))
      .heartbeat_step(workflow_id: "wf", position: 0, worker_id: "w", lease_seconds: 5, cursor: {})
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result]))
      .current_workflow_lease("wf")

    pg_store(ScriptedPgConnection.new(params_results: [sql_result([{ "id" => "wf" }])]))
      .mark_workflow_running("wf", worker_id: "w")
    pg_store(ScriptedPgConnection.new(params_results: [sql_result]))
      .mark_workflow_running("wf")

    assert_raises(Durababble::FenceTimeout) do
      pg_store(ScriptedPgConnection.new(params_results: [
        sql_result([], affected_rows: 0),
        sql_result,
      ])).with_fence(workflow_id: "wf", key: "slow", timeout: 0)
    end
    assert_equal "new-outbox", pg_store(ScriptedPgConnection.new(params_results: [
      sql_result,
      sql_result,
      sql_result([{ "id" => "new-outbox" }]),
    ])).enqueue_outbox(workflow_id: "wf", topic: "email", payload: {}, key: "new")
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result, sql_result]))
      .claim_outbox(worker_id: "w", lease_seconds: 5)
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result]))
      .claim_object_command(command_id: "cmd", worker_id: "w")
    assert_equal(
      { "id" => "cmd", "args" => [], "kwargs" => {} },
      pg_store(ScriptedPgConnection.new(params_results: [
        sql_result([{ "id" => "cmd", "args" => pg_dump([]), "kwargs" => pg_dump({}) }]),
      ])).claim_object_command(command_id: "cmd", worker_id: "w"),
    )
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [sql_result]))
      .complete_object_command(command_id: "cmd", result: "ignored", worker_id: "w")
    pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "cmd" }]),
      sql_result,
    ])).complete_object_command(command_id: "cmd", result: "ok")

    retry_connection = ScriptedPgConnection.new(exec_results: [
      ->(_sql) { raise ActiveRecord::Deadlocked },
      sql_result,
    ])
    pg_store(retry_connection).send(:execute, "SELECT 1")
    assert_raises(ActiveRecord::Deadlocked) do
      pg_store(ScriptedPgConnection.new(exec_results: Array.new(20) { ->(_sql) { raise ActiveRecord::Deadlocked } }))
        .send(:execute, "SELECT 1")
    end

    migration_connection = ScriptedPgConnection.new(
      params_results: [
        sql_result([{ "data_type" => "jsonb", "is_nullable" => "YES" }]),
        sql_result([{ "column_name" => "id" }]),
      ],
      exec_results: [
        sql_result,
        sql_result([{ "id" => "one", "payload" => "{\"ok\":true}" }]),
        sql_result,
        sql_result,
      ],
    )
    pg_store(migration_connection).send(:migrate_serialized_column!, "outbox", "payload")
  end

  test "uses active record mysql quoting, sanitization, and transaction retry" do
    connection = ScriptedMysqlConnection.new
    store = Durababble::MysqlStore.new(connection, schema: "branch-schema-with-a-very-long-name-that-will-be-hashed")

    assert_match(/\Adura_[0-9a-f]{10}\z/, store.send(:table_prefix))
    assert_equal "`dura_#{Digest::SHA1.hexdigest("branch_schema_with_a_very_long_name_that_will_be_hashed")[0, 10]}_workflows`", store.send(:table, "workflows")
    store.send(:execute_params, "SELECT ?, ?, ?, ?", [nil, true, false, 4])
    assert_equal "SELECT NULL, '1', '0', '4'", connection.queries.last
    store.send(:execute_params, "SELECT ?", ["x'; DROP TABLE workflows; --"])
    assert_equal "SELECT 'x''; DROP TABLE workflows; --'", connection.queries.last
    store.send(:execute_params, "SELECT * FROM workflows WHERE name IN (?)", [["x'; DROP TABLE workflows; --", "safe"]])
    assert_equal "SELECT * FROM workflows WHERE name IN ('x''; DROP TABLE workflows; --','safe')", connection.queries.last
    assert_equal ["AND name IN (?, ?)", ["x'; DROP TABLE workflows; --", "safe"]], store.send(:workflow_name_filter, ["x'; DROP TABLE workflows; --", "safe"])
    assert_equal ["AND target_kind IN (?)", ["workflow'); DROP TABLE inbox; --"]], store.send(:target_activation_filter_sql, target_kinds: ["workflow'); DROP TABLE inbox; --"], target_types: nil)
    assert_raises(ActiveRecord::PreparedStatementInvalid) { store.send(:execute_params, "SELECT ?, ?", [1]) }
    assert_raises(ActiveRecord::PreparedStatementInvalid) { store.send(:execute_params, "SELECT ?", [1, 2]) }
    assert store.send(:retryable_mysql_error?, ActiveRecord::Deadlocked.new("deadlocked"))
    refute store.send(:retryable_mysql_error?, RuntimeError.new("boom"))

    attempts = 0
    assert_equal :ok, store.send(:transaction) {
      attempts += 1
      raise ActiveRecord::Deadlocked, "deadlocked" if attempts == 1

      :ok
    }
    assert_equal 2, attempts

    index_connection = ScriptedMysqlConnection.new do |sql|
      if sql.include?("information_schema.statistics") && sql.include?("'present_idx'")
        sql_result([{ "exists" => 1 }])
      end
    end
    index_store = mysql_store(index_connection)
    index_store.send(:add_index_if_missing, "inbox", "missing_idx", "INDEX `missing_idx` (status)")
    index_store.send(:drop_index_if_present, "inbox", "present_idx")
    assert index_connection.queries.any? { |sql| sql.include?("ADD INDEX `missing_idx`") }
    assert index_connection.queries.any? { |sql| sql.include?("DROP INDEX `present_idx`") }
  end

  private

  def shared_store
    Durababble::Store.allocate.tap do |store|
      store.send(:initialize, ScriptedPgConnection.new, schema: "schema")
    end
  end

  def pg_store(connection = ScriptedPgConnection.new)
    Durababble::Store.new(connection, schema: "branch_schema")
  end

  def mysql_store(connection = ScriptedMysqlConnection.new)
    Durababble::MysqlStore.new(connection, schema: "branch_schema")
  end

  def with_yugabyte_store(migrate: true, &block)
    skip_without_yugabyte!
    require "pg"

    backend = DurababbleStoreBackend.new(
      name: "yugabyte",
      database_url: durababble_yugabyte_database_url,
      default_schema_prefix: "durababble_yb",
    )
    with_durababble_store(backend, "store_test", migrate:, &block)
  end
end
