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

  test "routes active record connections through mysql and postgres adapters" do
    assert_kind_of Durababble::MysqlStore, Durababble::Store.from_active_record(connection_pool: scripted_pool(ScriptedMysqlConnection.new), schema: "schema")
    assert_kind_of Durababble::PostgresStore, Durababble::Store.from_active_record(connection_pool: scripted_pool(ScriptedPgConnection.new), schema: "schema")
    assert_kind_of Durababble::PostgresStore, Durababble::Store.new(connection_pool: scripted_pool(ScriptedPgConnection.new), schema: "schema")
    assert_raises(ArgumentError) { Durababble::Store.new(schema: "schema") }
    assert_raises(ArgumentError) { Durababble::Store.new(scripted_pool(ScriptedPgConnection.new), schema: "schema") }
    assert_raises(ArgumentError) { Durababble::MysqlStore.new(ScriptedMysqlConnection.new, schema: "schema") }
    assert_raises(ArgumentError) { Durababble::Store.from_active_record(schema: "schema") }

    owner_pool = scripted_pool(ScriptedMysqlConnection.new)
    owner = Struct.new(:connection_pool).new(owner_pool)
    store = Durababble::Store.from_active_record(connection_pool: owner_pool, schema: "schema", owner:)
    assert_kind_of Durababble::MysqlStore, store
    store.close
    assert owner_pool.disconnected

    unsupported = Object.new
    unsupported.define_singleton_method(:adapter_name) { "SQLite" }
    assert_raises(ArgumentError) { Durababble::Store.from_active_record(connection: unsupported, schema: "schema") }
    assert_raises(ArgumentError) { Durababble::Store.from_active_record(connection_pool: scripted_pool(unsupported), schema: "schema") }
    assert_equal "postgresql", Durababble::Store.send(:active_record_config_for, "postgres://user:pass@example.test:5432/db").fetch(:adapter)
    assert_equal "trilogy", Durababble::Store.send(:active_record_config_for, "mysql://user:pass@example.test:3306/db").fetch(:adapter)
    assert_equal "trilogy", Durababble::Store.send(:active_record_config_for, "trilogy://user:pass@example.test:3306/db").fetch(:adapter)
    assert_equal "sqlite", Durababble::Store.send(:active_record_config_for, "sqlite:///tmp/durababble.sqlite").fetch(:adapter)
  end

  test "stores checkout connections from the active record pool as needed" do
    pool = scripted_pool(ScriptedMysqlConnection.new)

    store = Durababble::Store.from_active_record(connection_pool: pool, schema: "schema")

    assert_equal "schema", store.schema
    refute_respond_to store, :pooled_connections
    assert_equal 1, pool.checked_out

    before_query = pool.checked_out
    store.send(:execute_params, "SELECT ?", [1])

    assert_operator pool.checked_out, :>, before_query

    before_transaction = pool.checked_out
    store.send(:transaction) do
      store.send(:execute_params, "SELECT ?", [1])
      store.send(:execute_params, "SELECT ?", [2])
    end

    assert_equal before_transaction + 1, pool.checked_out
  end

  test "database url query parameters are preserved in active record config" do
    config = Durababble::Store.send(
      :active_record_config_for,
      "postgres://user:p%40ss@example.test:5432/db?sslmode=require&connect_timeout=5&application_name=durababble",
    )

    assert_equal "postgresql", config.fetch(:adapter)
    assert_equal "user", config.fetch(:username)
    assert_equal "p@ss", config.fetch(:password)
    assert_equal "example.test", config.fetch(:host)
    assert_equal 5432, config.fetch(:port)
    assert_equal "db", config.fetch(:database)
    assert_equal "require", config.fetch(:sslmode)
    assert_equal "5", config.fetch(:connect_timeout)
    assert_equal "durababble", config.fetch(:application_name)

    mysql_config = Durababble::Store.send(
      :active_record_config_for,
      "mysql://user:pass@example.test/db?socket=%2Ftmp%2Fmysql.sock&read_timeout=10",
    )

    assert_equal "trilogy", mysql_config.fetch(:adapter)
    assert_equal "/tmp/mysql.sock", mysql_config.fetch(:socket)
    assert_equal "10", mysql_config.fetch(:read_timeout)
  end

  test "test backend selection can require standard postgres without yugabyte" do
    with_env(
      "DURABABBLE_TEST_BACKENDS" => "postgres",
      "DURABABBLE_POSTGRES_DATABASE_URL" => "postgresql://postgres@127.0.0.1:5432/postgres",
      "DURABABBLE_YUGABYTE_DATABASE_URL" => nil,
    ) do
      backends = durababble_store_backends

      assert_equal ["postgres"], backends.map(&:name)
      assert_equal "postgresql://postgres@127.0.0.1:5432/postgres", backends.first.database_url
      assert_equal "durababble_pg", backends.first.default_schema_prefix
    end
  end

  test "test backend selection keeps postgres explicit and yugabyte optional" do
    with_env(
      "DURABABBLE_TEST_BACKENDS" => "mysql",
      "DURABABBLE_POSTGRES_DATABASE_URL" => "postgresql://postgres@127.0.0.1:5432/postgres",
      "DURABABBLE_YUGABYTE_DATABASE_URL" => "postgresql://yugabyte@127.0.0.1:15433/yugabyte",
    ) do
      assert_equal ["mysql"], durababble_store_backends.map(&:name)
    end

    with_env(
      "DURABABBLE_TEST_BACKENDS" => nil,
      "DURABABBLE_POSTGRES_DATABASE_URL" => "postgresql://postgres@127.0.0.1:5432/postgres",
      "DURABABBLE_YUGABYTE_DATABASE_URL" => "postgresql://yugabyte@127.0.0.1:15433/yugabyte",
    ) do
      assert_equal ["mysql", "yugabyte"], durababble_store_backends.map(&:name)
    end
  end

  test "test backend selection rejects unknown names" do
    with_env("DURABABBLE_TEST_BACKENDS" => "mysql,sqlite") do
      error = assert_raises(ArgumentError) { durababble_store_backends }
      assert_match(/unknown DURABABBLE_TEST_BACKENDS value/, error.message)
    end
  end

  test "close removes generated active record connection constants" do
    owner = nil
    const_name = nil
    owner = Durababble::Store.send(:active_record_class_for, "mysql://root@example.invalid/durababble_test")
    const_name = owner.instance_variable_get(:@durababble_store_connection_const_name)
    assert_kind_of(String, const_name)
    assert_equal("Durababble::#{const_name}", owner.name)
    assert(Durababble.const_defined?(const_name, false))

    store = Durababble::Store.from_active_record(connection_pool: scripted_pool(ScriptedMysqlConnection.new), schema: "schema", owner:)
    store.close

    refute(Durababble.const_defined?(const_name, false))
    store.close
  ensure
    owner&.connection_pool&.disconnect!
    Durababble.send(:remove_const, const_name) if const_name && Durababble.const_defined?(const_name, false)
  end

  test "generated active record cleanup ignores nil owners" do
    assert_nil Durababble::Store.send(:remove_active_record_class_const, nil)
  end

  test "observe_claim_latency ignores missing rows" do
    assert_nil shared_store.send(:observe_claim_latency, nil, "workflow")
  end

  test "observe_claim_latency accepts serialized timestamps" do
    assert_nil shared_store.send(:observe_claim_latency, { "created_at" => "2024-01-01T00:00:00Z" }, "workflow")
  end

  test "record_wait_latency accepts serialized timestamps" do
    wait = {
      "kind" => "timer",
      "created_at" => "2024-01-01T00:00:00Z",
      "completed_at" => "2024-01-01T00:00:01Z",
    }

    assert_nil shared_store.send(:record_wait_latency, wait)
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

  test "current_target_lease ignores unsupported target kinds" do
    assert_nil shared_store.send(:current_target_lease, target_kind: "queue", target_type: "approval", target_id: "wf", worker_pool: "default")
  end

  test "current lease lookups ignore worker pool because target ids identify leases" do
    connection = ScriptedPgConnection.new(params_results: [
      lambda do |sql, params|
        assert_equal(["wf"], params)
        refute_match(/worker_pool\s*=/, sql)
        sql_result([{ "workflow_id" => "wf", "worker_pool" => "pool-a", "worker_id" => "worker-a", "locked_until" => Time.now + 60 }])
      end,
      lambda do |sql, params|
        assert_equal(["counter", "object-1"], params)
        refute_match(/worker_pool\s*=/, sql)
        sql_result([{ "worker_pool" => "pool-a", "object_id" => "object-1", "worker_id" => "worker-a", "locked_until" => Time.now + 60 }])
      end,
    ])
    store = pg_store(connection)

    assert_hash_includes store.current_workflow_lease("wf", worker_pool: "pool-b"), "workflow_id" => "wf", "worker_pool" => "pool-a"
    assert_hash_includes store.current_object_lease("counter", "object-1", worker_pool: "pool-b"), "object_id" => "object-1", "worker_pool" => "pool-a"
  end

  test "postgres enqueue_workflow inserts the pending row in one statement" do
    connection = ScriptedPgConnection.new
    store = pg_store(connection, schema: "durababble_test")

    workflow_id = store.enqueue_workflow(name: "demo", input: { "count" => 1 })

    assert_match(/\A[0-9a-f-]{36}\z/, workflow_id)
    assert_equal 1, connection.exec_params_calls.length
    sql, params = connection.exec_params_calls.first
    assert_includes sql, "INSERT INTO"
    assert_includes sql, "status"
    refute_includes sql, "UPDATE"
    assert_equal "demo", params[1]
    assert_equal "default", params[2]
    assert_equal "pending", params[3]
  end

  test "postgres enqueue_workflow uses an explicit id and maps duplicate ids" do
    connection = ScriptedPgConnection.new
    store = pg_store(connection, schema: "durababble_test")

    workflow_id = store.enqueue_workflow(name: "demo", input: { "count" => 1 }, id: "wf-explicit")

    assert_equal "wf-explicit", workflow_id
    _sql, params = connection.exec_params_calls.first
    assert_equal "wf-explicit", params[0]

    duplicate = pg_store(
      ScriptedPgConnection.new(params_results: [->(_sql, _params) { raise ActiveRecord::RecordNotUnique, "duplicate key value violates unique constraint" }]),
      schema: "durababble_test",
    )
    error = assert_raises(Durababble::WorkflowAlreadyExists) do
      duplicate.enqueue_workflow(name: "demo", input: {}, id: "wf-explicit")
    end
    assert_match(/workflow wf-explicit already exists/, error.message)
  end

  test "postgres create_workflow inserts the initial running row in one statement" do
    connection = ScriptedPgConnection.new
    store = pg_store(connection, schema: "durababble_test")

    workflow_id = store.create_workflow(name: "demo", input: { "count" => 1 }, worker_id: "worker-a", lease_seconds: 9)

    assert_match(/\A[0-9a-f-]{36}\z/, workflow_id)
    assert_equal 1, connection.exec_params_calls.length
    sql, params = connection.exec_params_calls.first
    assert_includes sql, "INSERT INTO"
    assert_includes sql, "status"
    assert_includes sql, "locked_by"
    assert_includes sql, "locked_until"
    refute_includes sql, "UPDATE"
    assert_equal "demo", params[1]
    assert_equal "default", params[2]
    assert_equal "running", params[3]
    assert_equal "worker-a", params[5]
    assert_equal 9, params[6]
  end

  test "mysql enqueue_workflow inserts the pending row in one statement" do
    connection = ScriptedMysqlConnection.new
    store = mysql_store(connection, schema: "durababble_test")

    workflow_id = store.enqueue_workflow(name: "demo", input: { "count" => 1 })

    assert_match(/\A[0-9a-f-]{36}\z/, workflow_id)
    assert_equal 1, connection.queries.length
    sql = connection.queries.first
    assert_includes sql, "INSERT INTO"
    assert_includes sql, "status"
    refute_includes sql, "UPDATE"
  end

  test "mysql enqueue_workflow uses an explicit id and maps duplicate ids" do
    connection = ScriptedMysqlConnection.new
    store = mysql_store(connection, schema: "durababble_test")

    workflow_id = store.enqueue_workflow(name: "demo", input: { "count" => 1 }, id: "wf-explicit")

    assert_equal "wf-explicit", workflow_id
    sql = connection.queries.first
    assert_includes sql, "'wf-explicit'"

    duplicate = mysql_store(
      ScriptedMysqlConnection.new { |_sql| raise ActiveRecord::RecordNotUnique, "Duplicate entry 'wf-explicit' for key 'PRIMARY'" },
      schema: "durababble_test",
    )
    error = assert_raises(Durababble::WorkflowAlreadyExists) do
      duplicate.enqueue_workflow(name: "demo", input: {}, id: "wf-explicit")
    end
    assert_match(/workflow wf-explicit already exists/, error.message)
  end

  test "mysql create_workflow inserts the initial running row in one statement" do
    connection = ScriptedMysqlConnection.new
    store = mysql_store(connection, schema: "durababble_test")

    workflow_id = store.create_workflow(name: "demo", input: { "count" => 1 }, worker_id: "worker-a", lease_seconds: 9)

    assert_match(/\A[0-9a-f-]{36}\z/, workflow_id)
    assert_equal 1, connection.queries.length
    sql = connection.queries.first
    assert_includes sql, "INSERT INTO"
    assert_includes sql, "status"
    assert_includes sql, "locked_by"
    assert_includes sql, "locked_until"
    refute_includes sql, "UPDATE"
  end

  test "postgres claim_runnable_workflow claims pending work with one update returning statement" do
    connection = ScriptedPgConnection.new(params_results: [
      sql_result([{
        "id" => "workflow-1",
        "name" => "demo",
        "status" => "running",
        "input" => pg_dump({ "count" => 1 }),
        "created_at" => Time.utc(2026, 1, 1),
        "locked_by" => "worker-a",
        "locked_until" => Time.utc(2026, 1, 1, 0, 1),
      }]),
    ])
    store = pg_store(connection, schema: "durababble_test")

    claimed = store.claim_runnable_workflow(worker_id: "worker-a", lease_seconds: 9, workflow_names: ["demo"])

    assert_equal "workflow-1", claimed.fetch("id")
    assert_equal({ "count" => 1 }, claimed.fetch("input"))
    assert_equal 1, connection.exec_params_calls.length
    sql, params = connection.exec_params_calls.first
    assert_includes sql, "WITH candidate AS"
    assert_includes sql, "UPDATE"
    assert_includes sql, "RETURNING workflows.*"
    assert_includes sql, "FOR UPDATE SKIP LOCKED"
    assert_includes sql, "COALESCE(next_run_at, created_at)"
    refute_includes sql, "UNION ALL"
    refute_includes sql, "ORDER BY"
    assert_equal ["default", "worker-a", 9, "demo"], params
  end

  test "migrates and persists workflow plus step state" do
    with_durababble_store(durababble_store_backends.first, "store_test") do |store|
      workflow_id = store.create_workflow(name: "demo", input: { "count" => 1 })

      assert_hash_includes store.workflow(workflow_id), "status" => "running", "input" => { "count" => 1 }

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

      assert_includes indexes, "workflows_claim_idx"
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

  test "handles postgres queue, lease, wait, fence, outbox, and object command miss paths" do
    connection = ScriptedPgConnection.new(params_results: [
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
      sql_result([{ "worker_pool" => "default", "last_sequence" => "0" }]),
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
    assert_equal "signal:wf-1", inbox_insert.fetch(1)[9]

    duplicate = pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "existing-inbox-id", "worker_pool" => "default", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => shape_hash }]),
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
      sql_result([{ "id" => "pending-inbox-id", "worker_pool" => "default", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "pending", "ready_at" => nil, "shape_hash" => shape_hash }]),
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
        sql_result([{ "id" => "existing-inbox-id", "worker_pool" => "default", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => "different" }]),
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
      sql_result([{ "id" => "wf-1", "name" => "approval", "worker_pool" => "default", "status" => "running", "next_run_at" => nil }]),
      sql_result,
      sql_result,
      sql_result([{ "worker_pool" => "default", "last_sequence" => "0" }]),
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
      sql_result([{ "id" => "wf-1", "name" => "approval", "worker_pool" => "default", "status" => "running", "next_run_at" => nil }]),
      sql_result([{ "id" => "existing-command", "worker_pool" => "default", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => shape_hash }]),
    ])).enqueue_workflow_command(
      workflow_id: "wf-1",
      workflow_name: "approval",
      method_name: "approve",
      payload:,
      idempotency_key: "approve:1",
    )
    assert_equal "existing-command", duplicate

    pending_duplicate = pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "wf-1", "name" => "approval", "worker_pool" => "default", "status" => "running", "next_run_at" => nil }]),
      sql_result([{ "id" => "pending-command", "worker_pool" => "default", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "pending", "ready_at" => nil, "shape_hash" => shape_hash }]),
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
        sql_result([{ "id" => "wf-1", "name" => "approval", "worker_pool" => "default", "status" => "running", "next_run_at" => nil }]),
        sql_result([{ "id" => "existing-command", "worker_pool" => "default", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => "different" }]),
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
        sql_result([{ "id" => "wf-1", "name" => "approval", "status" => "completed", "next_run_at" => nil }]),
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
      sql_result([{ "worker_pool" => "default", "last_sequence" => 0 }]) if sql.include?("SELECT worker_pool, last_sequence")
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
      if sql.include?("idempotency_hash =")
        sql_result([{ "id" => "existing-inbox-id", "worker_pool" => "default", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => shape_hash }])
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
      if sql.include?("idempotency_hash =")
        sql_result([{ "id" => "pending-inbox-id", "worker_pool" => "default", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "pending", "ready_at" => nil, "shape_hash" => shape_hash }])
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
        if sql.include?("idempotency_hash =")
          sql_result([{ "id" => "existing-inbox-id", "worker_pool" => "default", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => "different" }])
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
    assert_raises(Durababble::LeaseConflict) do
      pg_store(ScriptedPgConnection.new(params_results: [
        sql_result([{ "id" => "msg", "method_name" => "approve", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf" }]),
        sql_result([{ "id" => "wf", "status" => "running", "next_run_at" => nil }]),
        sql_result,
      ])).complete_workflow_command(message_id: "msg", workflow_id: "wf", result: "ok", worker_id: "w")
    end
    assert_raises(Durababble::LeaseConflict) do
      pg_store(ScriptedPgConnection.new(params_results: [
        sql_result([{ "id" => "msg", "method_name" => "reject", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf" }]),
        sql_result([{ "id" => "wf", "status" => "running", "next_run_at" => nil }]),
        sql_result,
      ])).fail_workflow_command(message_id: "msg", workflow_id: "wf", error: "boom", worker_id: "w")
    end
    pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "msg", "method_name" => "approve", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf" }]),
      sql_result([{ "id" => "wf", "status" => "running", "next_run_at" => nil }]),
      sql_result([{ "owned" => 1 }]),
      sql_result,
      sql_result([{ "event_index" => "0" }]),
      sql_result,
      sql_result,
      sql_result,
      sql_result,
    ])).complete_workflow_command(message_id: "msg", workflow_id: "wf", result: "ok", worker_id: "w")
    pg_store(ScriptedPgConnection.new(params_results: [
      sql_result([{ "id" => "msg", "method_name" => "reject", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf" }]),
      sql_result([{ "id" => "wf", "status" => "running", "next_run_at" => nil }]),
      sql_result([{ "owned" => 1 }]),
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

  test "advisory object delivery uses the active lease worker pool" do
    client = FlakyDeliveryClient.new(failures: 0)
    connection = ScriptedPgConnection.new(params_results: [
      lambda do |_sql, params|
        assert_equal(["counter", "same"], params)
        sql_result([{ "worker_pool" => "pool-a", "object_id" => "same", "worker_id" => "127.0.0.1:34567", "locked_until" => Time.now + 60 }])
      end,
    ])

    delivered = pg_store(connection).deliver_target_message(
      worker_pool: "pool-b",
      target_kind: "object",
      target_type: "counter",
      target_id: "same",
      client_factory: lambda do |address|
        assert_equal("127.0.0.1:34567", address)
        client
      end,
    )

    assert_equal true, delivered
    assert_equal(
      [
        {
          worker_pool: "pool-a",
          target_kind: "object",
          target_class: "counter",
          target_id: "same",
          expected_worker_id: "127.0.0.1:34567",
        },
      ],
      client.deliveries,
    )
  end

  test "terminal workflow updates choose fenced and unfenced SQL paths" do
    # complete_workflow / cancel_workflow / fail_workflow each also issue the idempotent
    # wait/step/attempt cleanup cascade (three extra queries per terminal transition), so
    # each terminal call emits four queries; scope the fence assertions to the terminal
    # workflow-row updates (the only queries that touch next_run_at). Provide one
    # affected_rows:1 result per emitted query so every fenced workflow-row update sees a
    # live lease rather than falling onto a default empty result and raising LeaseConflict.
    pg_fenced_connection = ScriptedPgConnection.new(params_results: Array.new(12) { sql_result([], affected_rows: 1) })
    pg_fenced_store = pg_store(pg_fenced_connection)
    pg_fenced_store.complete_workflow("wf", result: { "ok" => true }, worker_id: "worker-1")
    pg_fenced_store.cancel_workflow("wf", reason: "stop", result: nil, worker_id: "worker-1")
    pg_fenced_store.fail_workflow("wf", error: "boom", worker_id: "worker-1")

    pg_fenced_workflow_updates = pg_fenced_connection.exec_params_calls.select { |sql, _params| sql.include?("next_run_at") }
    assert_equal 3, pg_fenced_workflow_updates.length
    assert pg_fenced_workflow_updates.all? { |sql, _params| sql.include?("locked_by") && sql.include?("locked_until >= now()") }

    pg_unfenced_connection = ScriptedPgConnection.new(params_results: Array.new(6) { sql_result([], affected_rows: 1) })
    pg_unfenced_store = pg_store(pg_unfenced_connection)
    pg_unfenced_store.complete_workflow("wf", result: { "ok" => true })
    pg_unfenced_store.cancel_workflow("wf", reason: "stop")
    pg_unfenced_store.fail_workflow("wf", error: "boom")

    pg_unfenced_workflow_updates = pg_unfenced_connection.exec_params_calls.select { |sql, _params| sql.include?("next_run_at") }
    assert_equal 3, pg_unfenced_workflow_updates.length
    assert pg_unfenced_workflow_updates.none? { |sql, _params| sql.include?("AND locked_by =") }

    mysql_fenced_connection = ScriptedMysqlConnection.new { sql_result([], affected_rows: 1) }
    mysql_fenced_store = mysql_store(mysql_fenced_connection)
    mysql_fenced_store.complete_workflow("wf", result: { "ok" => true }, worker_id: "worker-1")
    mysql_fenced_store.cancel_workflow("wf", reason: "stop", result: nil, worker_id: "worker-1")
    mysql_fenced_store.fail_workflow("wf", error: "boom", worker_id: "worker-1")

    mysql_fenced_workflow_updates = mysql_fenced_connection.queries.select { |sql| sql.include?("next_run_at") }
    assert_equal 3, mysql_fenced_workflow_updates.length
    assert mysql_fenced_workflow_updates.all? { |sql| sql.include?("locked_by") && sql.include?("locked_until >= NOW(6)") }

    mysql_unfenced_connection = ScriptedMysqlConnection.new { sql_result([], affected_rows: 1) }
    mysql_unfenced_store = mysql_store(mysql_unfenced_connection)
    mysql_unfenced_store.complete_workflow("wf", result: { "ok" => true })
    mysql_unfenced_store.cancel_workflow("wf", reason: "stop")
    mysql_unfenced_store.fail_workflow("wf", error: "boom")

    mysql_unfenced_workflow_updates = mysql_unfenced_connection.queries.select { |sql| sql.include?("next_run_at") }
    assert_equal 3, mysql_unfenced_workflow_updates.length
    assert mysql_unfenced_workflow_updates.none? { |sql| sql.include?("AND locked_by =") }

    stale_pg_store = pg_store(ScriptedPgConnection.new(params_results: [sql_result([], affected_rows: 0)]))
    assert_raises(Durababble::LeaseConflict) do
      stale_pg_store.complete_workflow("wf", result: { "ok" => true }, worker_id: "stale-worker")
    end
  end

  test "handles postgres fence replay, retry, and helper branches" do
    completed = { "status" => "completed", "result" => pg_dump({ "done" => true }), "error" => nil }
    failed = { "status" => "failed", "result" => nil, "error" => "boom" }
    connection = ScriptedPgConnection.new(
      params_results: [
        sql_result([], affected_rows: 1),
        sql_result([], affected_rows: 1),
        sql_result([], affected_rows: 0),
        sql_result([], affected_rows: 0),
        sql_result([completed]),
        sql_result([], affected_rows: 0),
        sql_result([], affected_rows: 0),
        sql_result([failed]),
      ],
    )
    store = pg_store(connection)

    assert_equal({ "created" => true }, store.with_fence(workflow_id: "wf", key: "created") { { "created" => true } })
    assert_equal({ "done" => true }, store.with_fence(workflow_id: "wf", key: "done", timeout: 0))
    assert_raises(Durababble::Error) { store.with_fence(workflow_id: "wf", key: "failed", timeout: 0) }

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

    # Transaction-level deadlocks must also be retried: every #transaction is wrapped in
    # retry_serialization_failures, so a deadlock that aborts the whole transaction (not just
    # a single statement) has to be replayed rather than surfaced to the caller.
    deadlock_attempts = 0
    deadlock_result = store.send(:retry_serialization_failures) do
      deadlock_attempts += 1
      raise ActiveRecord::Deadlocked if deadlock_attempts == 1

      :recovered
    end
    assert_equal :recovered, deadlock_result
    assert_equal 2, deadlock_attempts
    assert_raises(ActiveRecord::Deadlocked) do
      store.send(:retry_serialization_failures, max_attempts: 1) { raise ActiveRecord::Deadlocked }
    end

    assert_equal ["", []], store.send(:workflow_name_filter, nil)
    malicious_name = "a'); DROP TABLE workflows; --"
    assert_equal ["AND name IN ($1, $2)", [malicious_name, "b"]], store.send(:workflow_name_filter, [malicious_name, "b"])
    assert_equal ["", []], store.send(:workflow_exclusion_filter, nil)
    assert_equal ["AND id NOT IN ($3, $4)", ["wf'); DROP TABLE workflows; --", "safe"]], store.send(:workflow_exclusion_filter, ["wf'); DROP TABLE workflows; --", "safe"], offset: 3)
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
  end

  test "uses active record mysql quoting, sanitization, and transaction retry" do
    connection = ScriptedMysqlConnection.new
    store = mysql_store(connection, schema: "branch-schema-with-a-very-long-name-that-will-be-hashed")

    assert_match(/\Adura_[0-9a-f]{10}\z/, store.send(:table_prefix))
    assert_equal "`dura_#{Digest::SHA1.hexdigest("branch_schema_with_a_very_long_name_that_will_be_hashed")[0, 10]}_workflows`", store.send(:table, "workflows")
    store.send(:execute_params, "SELECT ?, ?, ?, ?", [nil, true, false, 4])
    assert_equal "SELECT NULL, '1', '0', '4'", connection.queries.last
    store.send(:execute_params, "SELECT ?", ["x'; DROP TABLE workflows; --"])
    assert_equal "SELECT 'x''; DROP TABLE workflows; --'", connection.queries.last
    store.send(:execute_params, "SELECT * FROM workflows WHERE name IN (?)", [["x'; DROP TABLE workflows; --", "safe"]])
    assert_equal "SELECT * FROM workflows WHERE name IN ('x''; DROP TABLE workflows; --','safe')", connection.queries.last
    assert_equal ["AND name IN (?, ?)", ["x'; DROP TABLE workflows; --", "safe"]], store.send(:workflow_name_filter, ["x'; DROP TABLE workflows; --", "safe"])
    assert_equal ["", []], store.send(:workflow_exclusion_filter, [])
    assert_equal ["AND id NOT IN (?)", ["wf'; DROP TABLE workflows; --"]], store.send(:workflow_exclusion_filter, ["wf'; DROP TABLE workflows; --"])
    assert_equal ["", []], store.send(:target_activation_filter_sql, target_kinds: nil, target_types: nil)
    assert_equal ["AND target_kind IN (?)", ["workflow'); DROP TABLE inbox; --"]], store.send(:target_activation_filter_sql, target_kinds: ["workflow'); DROP TABLE inbox; --"], target_types: nil)
    assert_equal ["AND target_type IN (?)", ["approval"]], store.send(:target_activation_filter_sql, target_kinds: nil, target_types: ["approval"])
    assert_raises(ArgumentError) { store.send(:normalize_command_id, nil, nil) }
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
  end

  private

  def shared_store
    Durababble::Store.allocate.tap do |store|
      store.send(:initialize, scripted_pool(ScriptedPgConnection.new), schema: "schema")
    end
  end

  def pg_store(connection = ScriptedPgConnection.new, schema: "branch_schema")
    Durababble::Store.new(connection_pool: scripted_pool(connection), schema:)
  end

  def mysql_store(connection = ScriptedMysqlConnection.new, schema: "branch_schema")
    Durababble::MysqlStore.new(scripted_pool(connection), schema:)
  end

  def with_env(values)
    previous = {}
    values.each do |key, value|
      previous[key] = ENV.key?(key) ? ENV.fetch(key) : nil
      value.nil? ? ENV.delete(key) : ENV[key] = value
    end

    yield
  ensure
    previous.each do |key, value|
      value.nil? ? ENV.delete(key) : ENV[key] = value
    end
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
