# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

require "pg"

class DurababbleStoreTest < DurababbleTestCase
  class PgResult < Array
    attr_reader :cmd_tuples

    def initialize(rows = [], cmd_tuples: rows.length)
      super(rows)
      @cmd_tuples = cmd_tuples
    end
  end

  class ScriptedPgConnection
    attr_reader :exec_params_calls, :exec_calls, :closed

    def initialize(params_results: [], exec_results: [], finished: false)
      @params_results = params_results
      @exec_results = exec_results
      @exec_params_calls = []
      @exec_calls = []
      @finished = finished
      @closed = false
    end

    def exec_params(sql, params)
      @exec_params_calls << [sql, params]
      result = @params_results.shift || PgResult.new
      result.respond_to?(:call) ? result.call(sql, params) : result
    end

    def exec(sql)
      @exec_calls << sql
      result = @exec_results.shift || PgResult.new
      result.respond_to?(:call) ? result.call(sql) : result
    end

    def transaction
      yield
    end

    def finished?
      @finished
    end

    def close
      @closed = true
    end

    def escape_literal(value)
      "'#{value.to_s.gsub("'", "''")}'"
    end
  end

  class FakeMysqlConnection
    attr_reader :queries
    attr_accessor :affected_rows

    def initialize
      @queries = []
      @affected_rows = 0
    end

    def query(sql)
      @queries << sql
      MysqlResultLike.new([])
    end

    def escape(value)
      value.to_s.gsub("'", "''")
    end
  end

  class MysqlResultLike
    attr_reader :affected_rows

    def initialize(rows, affected_rows: nil)
      @rows = rows
      @affected_rows = affected_rows
    end

    def each_hash
      @rows.each
    end
  end

  class MysqlRetryableError < StandardError
    attr_reader :error_code

    def initialize(error_code)
      @error_code = error_code
      super("mysql #{error_code}")
    end
  end

  class ObjectWithString
    def initialize(value)
      @value = value
    end

    def to_s
      @value
    end
  end

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

  test "routes configured database URLs through mysql and postgres adapters" do
    mysql = Object.new
    pg = ScriptedPgConnection.new

    Durababble::MysqlStore.expects(:connect).with do |uri:, schema:|
      uri.scheme == "mysql" && schema == "schema"
    end.returns(mysql)
    assert_same mysql, Durababble::Store.connect(database_url: "mysql://user@127.0.0.1/db", schema: "schema")

    PG.expects(:connect).with("postgresql://127.0.0.1/db").returns(pg)
    store = Durababble::Store.connect(database_url: "postgresql://127.0.0.1/db", schema: "schema")
    assert_kind_of Durababble::Store, store
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
      PgResult.new([{ "id" => "pending", "created_at" => "2024-01-02T00:00:00Z" }]),
      PgResult.new([{ "id" => "failed", "created_at" => "2024-01-01T00:00:00Z" }]),
      PgResult.new,
      PgResult.new,
      PgResult.new([{ "id" => "failed", "input" => pg_dump({ "count" => 1 }) }]),
      PgResult.new([{ "id" => "wf", "input" => pg_dump({ "ok" => true }) }]),
      PgResult.new,
      PgResult.new,
      PgResult.new([{ "locked_until" => "future" }]),
      PgResult.new,
      PgResult.new([{ "locked_until" => "future" }]),
      PgResult.new([{ "heartbeat_cursor" => pg_dump({ "cursor" => 1 }) }]),
      PgResult.new,
      PgResult.new([{ "workflow_id" => "wf", "worker_id" => "other", "locked_until" => "future" }]),
      PgResult.new,
      PgResult.new([{ "state" => pg_dump({ "value" => 3 }) }]),
      PgResult.new([{ "id" => "cmd" }]),
      PgResult.new,
      PgResult.new([{ "id" => "cmd", "args" => pg_dump([]), "kwargs" => pg_dump({}) }]),
      PgResult.new,
      PgResult.new,
      PgResult.new,
      PgResult.new,
      PgResult.new([{ "id" => "outbox-old" }]),
      PgResult.new,
      PgResult.new([{ "id" => "outbox-new", "created_at" => "2024-01-01T00:00:00Z" }]),
      PgResult.new([{ "id" => "outbox-new", "payload" => pg_dump({ "ok" => true }) }]),
      PgResult.new([{ "id" => "wf", "input" => pg_dump({}) }]),
      PgResult.new,
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
      PgResult.new,
      PgResult.new,
      PgResult.new([{ "last_sequence" => "0" }]),
      PgResult.new,
      PgResult.new,
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
      PgResult.new([{ "id" => "existing-inbox-id", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => shape_hash }]),
    ])).enqueue_inbox_message(
      target_kind: "workflow",
      target_type: "approval",
      target_id: "wf-1",
      message_kind: "workflow_signal",
      payload: { "approved" => true },
      idempotency_key: "signal:wf-1",
    )
    assert_equal "existing-inbox-id", duplicate

    assert_raises(Durababble::IdempotencyKeyConflict) do
      pg_store(ScriptedPgConnection.new(params_results: [
        PgResult.new([{ "id" => "existing-inbox-id", "target_kind" => "workflow", "target_type" => "approval", "target_id" => "wf-1", "status" => "completed", "ready_at" => nil, "shape_hash" => "different" }]),
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

  test "handles postgres fence replay, serialization migration, retry, and helper branches" do
    completed = { "status" => "completed", "result" => pg_dump({ "done" => true }), "error" => nil }
    failed = { "status" => "failed", "result" => nil, "error" => "boom" }
    connection = ScriptedPgConnection.new(
      params_results: [
        PgResult.new([], cmd_tuples: 1),
        PgResult.new([], cmd_tuples: 1),
        PgResult.new([], cmd_tuples: 0),
        PgResult.new([completed]),
        PgResult.new([], cmd_tuples: 0),
        PgResult.new([failed]),
        PgResult.new([{ "data_type" => "jsonb", "is_nullable" => "YES" }]),
        PgResult.new([{ "column_name" => "id" }]),
        PgResult.new,
        PgResult.new,
        PgResult.new([{ "data_type" => "bytea", "is_nullable" => "YES" }]),
        PgResult.new,
      ],
      exec_results: [
        PgResult.new,
        PgResult.new([
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
      raise PG::TRSerializationFailure if attempts == 1

      :retried
    end
    assert_equal :retried, result
    assert_raises(PG::TRSerializationFailure) do
      store.send(:retry_serialization_failures, max_attempts: 1) { raise PG::TRSerializationFailure }
    end
    assert_equal "", store.send(:workflow_name_filter, nil)
    assert_includes store.send(:workflow_name_filter, ["a", "b"]), "name IN"
    assert_nil store.send(:timestamp_or_nil, nil)
  end

  test "handles isolated postgres adapter migration and retry edge paths" do
    migrated = pg_store
    migrated.instance_variable_set(:@migrated, true)
    assert_same migrated, migrated.migrate!

    open_connection = ScriptedPgConnection.new(finished: false)
    closed_connection = ScriptedPgConnection.new(finished: true)
    pg_store(open_connection).close
    pg_store(closed_connection).close
    assert open_connection.closed
    refute closed_connection.closed

    assert_nil pg_store(ScriptedPgConnection.new(params_results: [PgResult.new, PgResult.new, PgResult.new]))
      .claim_runnable_workflow(worker_id: "w", lease_seconds: 5)
    assert_equal(
      { "id" => "wf", "input" => { "fresh" => true } },
      pg_store(ScriptedPgConnection.new(params_results: [
        PgResult.new,
        PgResult.new([{ "id" => "wf", "input" => pg_dump({ "fresh" => true }) }]),
      ])).claim_workflow(workflow_id: "wf", worker_id: "w", lease_seconds: 5),
    )
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [PgResult.new]))
      .heartbeat_step(workflow_id: "wf", position: 0, worker_id: "w", lease_seconds: 5, cursor: {})
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [PgResult.new]))
      .current_workflow_lease("wf")

    pg_store(ScriptedPgConnection.new(params_results: [PgResult.new([{ "id" => "wf" }])]))
      .mark_workflow_running("wf", worker_id: "w")
    pg_store(ScriptedPgConnection.new(params_results: [PgResult.new]))
      .mark_workflow_running("wf")

    assert_raises(Durababble::FenceTimeout) do
      pg_store(ScriptedPgConnection.new(params_results: [
        PgResult.new([], cmd_tuples: 0),
        PgResult.new,
      ])).with_fence(workflow_id: "wf", key: "slow", timeout: 0)
    end
    assert_equal "new-outbox", pg_store(ScriptedPgConnection.new(params_results: [
      PgResult.new,
      PgResult.new,
      PgResult.new([{ "id" => "new-outbox" }]),
    ])).enqueue_outbox(workflow_id: "wf", topic: "email", payload: {}, key: "new")
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [PgResult.new, PgResult.new]))
      .claim_outbox(worker_id: "w", lease_seconds: 5)
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [PgResult.new]))
      .claim_object_command(command_id: "cmd", worker_id: "w")
    assert_equal(
      { "id" => "cmd", "args" => [], "kwargs" => {} },
      pg_store(ScriptedPgConnection.new(params_results: [
        PgResult.new([{ "id" => "cmd", "args" => pg_dump([]), "kwargs" => pg_dump({}) }]),
      ])).claim_object_command(command_id: "cmd", worker_id: "w"),
    )
    assert_nil pg_store(ScriptedPgConnection.new(params_results: [PgResult.new]))
      .complete_object_command(command_id: "cmd", result: "ignored", worker_id: "w")
    pg_store(ScriptedPgConnection.new(params_results: [
      PgResult.new([{ "id" => "cmd" }]),
      PgResult.new,
    ])).complete_object_command(command_id: "cmd", result: "ok")

    retry_connection = ScriptedPgConnection.new(exec_results: [
      ->(_sql) { raise PG::TRDeadlockDetected },
      PgResult.new,
    ])
    pg_store(retry_connection).send(:execute, "SELECT 1")
    assert_raises(PG::TRDeadlockDetected) do
      pg_store(ScriptedPgConnection.new(exec_results: Array.new(20) { ->(_sql) { raise PG::TRDeadlockDetected } }))
        .send(:execute, "SELECT 1")
    end

    migration_connection = ScriptedPgConnection.new(
      params_results: [
        PgResult.new([{ "data_type" => "jsonb", "is_nullable" => "YES" }]),
        PgResult.new([{ "column_name" => "id" }]),
      ],
      exec_results: [
        PgResult.new,
        PgResult.new([{ "id" => "one", "payload" => "{\"ok\":true}" }]),
        PgResult.new,
        PgResult.new,
      ],
    )
    pg_store(migration_connection).send(:migrate_serialized_column!, "outbox", "payload")
  end

  test "binds mysql literals, rows, identifiers, and retries transient transactions" do
    connection = FakeMysqlConnection.new
    store = Durababble::MysqlStore.new(connection, schema: "branch-schema-with-a-very-long-name-that-will-be-hashed")

    assert_match(/\Adura_[0-9a-f]{10}\z/, store.send(:table_prefix))
    assert_equal "`has``tick`", store.send(:quote_ident, "has`tick")
    assert_equal("SELECT NULL, TRUE, FALSE, 4", store.send(:bind_params, "SELECT ?, ?, ?, ?", [nil, true, false, 4]))
    assert_equal "x'616263'", store.send(:mysql_literal, "abc".b)
    assert_equal "'O''Reilly'", store.send(:mysql_literal, "O'Reilly")
    assert_equal("'2024-01-01 00:00:00.123456'", store.send(:mysql_literal, Time.utc(2024, 1, 1, 0, 0, 0, 123_456)))
    assert_equal "'object-value'", store.send(:mysql_literal, ObjectWithString.new("object-value"))
    assert_raises(ArgumentError) { store.send(:bind_params, "SELECT ?, ?", [1]) }
    assert_raises(ArgumentError) { store.send(:bind_params, "SELECT ?", [1, 2]) }
    assert_equal([], store.send(:mysql_rows, Object.new))
    assert_equal([{ "id" => 1 }], store.send(:mysql_rows, MysqlResultLike.new([{ id: 1 }])))
    assert store.send(:retryable_mysql_error?, MysqlRetryableError.new(1213))
    refute store.send(:retryable_mysql_error?, MysqlRetryableError.new(9999))

    attempts = 0
    assert_equal :ok, store.send(:transaction) {
      attempts += 1
      raise MysqlRetryableError, 1213 if attempts == 1

      :ok
    }
    assert_includes connection.queries, "ROLLBACK"
    assert_includes connection.queries, "COMMIT"
  end

  private

  def pg_store(connection = ScriptedPgConnection.new)
    Durababble::Store.new(connection, schema: "branch_schema")
  end

  def pg_dump(value)
    Durababble::Store::SERIALIZER.dump(value).then { |bytes| "\\x#{bytes.unpack1("H*")}" }
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
