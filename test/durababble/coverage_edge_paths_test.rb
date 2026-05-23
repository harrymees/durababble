# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

require "pg"

class DurababbleCoverageEdgePathsTest < DurababbleTestCase
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

  FakeTransientResponse = Struct.new(:result, :ok, :err, :moved, keyword_init: true)
  FakeRemoteError = Struct.new(:klass, :message, keyword_init: true)
  FakeLeaseMoved = Struct.new(:new_node_id, :new_rpc_address, keyword_init: true)

  def pg_store(connection = ScriptedPgConnection.new)
    Durababble::Store.new(connection, schema: "branch_schema")
  end

  test "routes store connections through both mysql and postgres adapters" do
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

  test "covers postgres store queue, lease, wait, fence, outbox, and object command edge paths" do
    connection = ScriptedPgConnection.new(params_results: [
      PgResult.new([{ "id" => "pending", "created_at" => "2024-01-02T00:00:00Z" }]),
      PgResult.new([{ "id" => "failed", "created_at" => "2024-01-01T00:00:00Z" }]),
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

  test "covers postgres fence, serialization migration, retry, and helper branches" do
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

  test "covers isolated postgres adapter miss, retry, and migration edge branches" do
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
      pg_store(ScriptedPgConnection.new(exec_results: Array.new(5) { ->(_sql) { raise PG::TRDeadlockDetected } }))
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

  test "covers mysql helper, literal binding, result, and transaction retry branches" do
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

  test "covers rpc transport response, authorization, stale delivery, and custom transient branches" do
    assert_nil Durababble::Rpc.load(nil)
    assert_nil Durababble::Rpc.load("")
    assert_equal({ "ok" => true }, Durababble::Rpc.load(Durababble::Rpc.dump({ "ok" => true })))
    assert_nil Durababble::Rpc::Client.decode_transient_response(FakeTransientResponse.new(result: :unknown))
    assert_raises(Durababble::RpcClient::RemoteError) do
      Durababble::Rpc::Client.decode_transient_response(
        FakeTransientResponse.new(result: :err, err: FakeRemoteError.new(klass: "UnknownRemote", message: "bad")),
      )
    end

    stale_store = Object.new
    def stale_store.current_workflow_lease(_id) = nil

    delivered = []
    service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store: stale_store,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: ->(request:, args:) { ["custom", request["method"], args] },
      node_directory: Durababble::Rpc::NodeDirectory.new("node-b" => "127.0.0.1:6000"),
      authorize: ->(_call) { true },
      awaken_batch: ->(**kwargs) { delivered << [:awaken, kwargs] },
      evict_lease: ->(**kwargs) { delivered << [:evict, kwargs] },
      deliver_message: ->(**kwargs) { delivered << [:deliver, kwargs] },
    )

    service.awaken_batch(Durababble::Rpc::Proto::AwakenBatchRequest.new(worker_pool: "default", workflow_ids: ["wf"]), :call)
    service.evict_lease(Durababble::Rpc::Proto::EvictLeaseRequest.new(worker_pool: "default", target_kind: "workflow", target_class: "", target_id: "wf"), :call)
    service.deliver_message(Durababble::Rpc::Proto::DeliverMessageRequest.new(worker_pool: "default", target_kind: "workflow", target_class: "", target_id: "wf"), :call)
    assert_equal [:awaken, :evict], delivered.map(&:first)

    response = service.call_transient(
      Durababble::Rpc::Proto::TransientRequest.new(worker_pool: "default", method: "ping", args: Durababble::Rpc.dump({ "x" => 1 })),
      :call,
    )
    assert_equal ["custom", "ping", { "x" => 1 }], Durababble::Rpc.load(response.ok)

    unauthorized = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store: stale_store,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: nil,
      node_directory: Durababble::Rpc::NodeDirectory.new,
      authorize: ->(_call) { false },
      awaken_batch: nil,
      evict_lease: nil,
      deliver_message: nil,
    )
    assert_raises(GRPC::Unauthenticated) do
      unauthorized.awaken_batch(Durababble::Rpc::Proto::AwakenBatchRequest.new(worker_pool: "default", workflow_ids: []), :call)
    end

    moved_store = Object.new
    def moved_store.current_workflow_lease(_id) = { "worker_id" => "node-b" }

    moved_service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store: moved_store,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: nil,
      node_directory: Durababble::Rpc::NodeDirectory.new("node-b" => "127.0.0.1:6000"),
      authorize: nil,
      awaken_batch: nil,
      evict_lease: nil,
      deliver_message: nil,
    )
    moved = moved_service.call_transient(
      Durababble::Rpc::Proto::TransientRequest.new(worker_pool: "default", workflow_id: "wf", method: "status", args: Durababble::Rpc.dump({})),
      :call,
    )
    assert_equal "node-b", moved.moved.new_node_id

    same_node_store = Object.new
    def same_node_store.current_workflow_lease(_id) = { "worker_id" => "node-a" }

    same_node_service = Durababble::Rpc::Service.new(
      node_id: "node-a",
      store: same_node_store,
      worker_pool: "default",
      workflow_handlers: {},
      transient_handler: nil,
      node_directory: Durababble::Rpc::NodeDirectory.new,
      authorize: nil,
      awaken_batch: nil,
      evict_lease: nil,
      deliver_message: nil,
    )
    remote_error = same_node_service.call_transient(
      Durababble::Rpc::Proto::TransientRequest.new(worker_pool: "default", workflow_id: "wf", method: "status", args: Durababble::Rpc.dump({})),
      :call,
    )
    assert_equal "Durababble::WorkflowRpc::UnknownCommand", remote_error.err.klass
  end

  test "covers workflow, durable object, engine, and root module edge branches" do
    anonymous_workflow = Class.new(Durababble::Workflow)
    assert_match(/\A\d+\z/, anonymous_workflow.workflow_name)

    odd_workflow = Class.new(Durababble::Workflow)
    odd_workflow.instance_variable_set(:@pending_durable_macro, [:unknown, {}])
    odd_workflow.class_eval { def ignored_macro = true }
    odd_workflow.class_eval do
      def repeat(input) = input
      step :repeat
      step :repeat
    end
    assert_equal [:repeat], odd_workflow.step_order

    positional_query = Class.new(Durababble::Workflow) do
      expose def describe(prefix)
        "#{prefix}:#{@__durababble_ref_workflow_id}"
      end
    end
    assert_equal "wf:wf-1", positional_query.ref("wf-1", store: Object.new).describe("wf")

    anonymous_object = Class.new(Durababble::DurableObject)
    assert_match(/\A\d+\z/, anonymous_object.object_type)

    odd_object = Class.new(Durababble::DurableObject)
    odd_object.instance_variable_set(:@pending_durable_macro, [:unknown, {}])
    odd_object.class_eval { def ignored_macro = true }

    save_store = Object.new
    saved = []
    save_store.define_singleton_method(:save_object_state) { |**kwargs| saved << kwargs }
    object = anonymous_object.new(durable_id: "obj-1", store: save_store)
    object.update_state({ "saved" => true })
    assert_equal [{ object_type: anonymous_object.object_type, object_id: "obj-1", state: { "saved" => true } }], saved

    clean_command_store = BranchCommandStore.new
    clean_ref = CleanCommandObject.ref("clean", store: clean_command_store)
    assert_equal "unchanged", clean_ref.read_only
    assert_equal 1, clean_command_store.completed.length

    lost_lease_store = BranchCommandStore.new(complete_result: Durababble::MysqlStore::MysqlResult.new([], 0))
    assert_raises(Durababble::LeaseConflict) { CleanCommandObject.ref("lost", store: lost_lease_store).read_only }
    assert_equal 1, lost_lease_store.failed.length

    no_lease_store = Object.new
    engine = Durababble::Engine.new(store: no_lease_store, migrate: false)
    assert_nil engine.send(:assert_workflow_lease!, "wf")
    crashy_engine = Durababble::Engine.new(store: no_lease_store, migrate: false, crash_after: :workflow_completed)
    assert_raises(Durababble::InjectedCrash) { crashy_engine.send(:crash!, :workflow_completed) }

    previous_workspace_root = ENV["DURABABBLE_WORKSPACE_ROOT"]
    ENV["DURABABBLE_WORKSPACE_ROOT"] = "/tmp/Workspace With Caps"
    begin
      assert_match(/\Adurababble_/, Durababble.workspace_schema)
    ensure
      ENV["DURABABBLE_WORKSPACE_ROOT"] = previous_workspace_root
    end
  end

  class BranchCommandStore
    attr_reader :completed, :failed

    def initialize(complete_result: Durababble::MysqlStore::MysqlResult.new([], 1))
      @complete_result = complete_result
      @completed = []
      @failed = []
    end

    def migrate! = self
    def object_state(object_type:, object_id:) = { "value" => 1 }
    def enqueue_object_command(object_type:, object_id:, method_name:, args:, kwargs:) = "cmd-1"
    def claim_object_command(command_id:, worker_id:) = { "id" => command_id }

    def complete_object_command(command_id:, result:, **_kwargs)
      @completed << [command_id, result]
      @complete_result
    end

    def fail_object_command(command_id:, error:, worker_id:)
      @failed << [command_id, error, worker_id]
    end
  end

  class CleanCommandObject < Durababble::DurableObject
    expose_command def read_only
      "unchanged"
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

  def pg_dump(value)
    Durababble::Store::SERIALIZER.dump(value).then { |bytes| "\\x#{bytes.unpack1("H*")}" }
  end
end
