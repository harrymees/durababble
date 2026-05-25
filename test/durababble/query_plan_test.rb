# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "delegate"
require "json"

class DurababbleQueryPlanTest < DurababbleTestCase
  class RecordingConnection < SimpleDelegator
    attr_reader :recorded_queries

    def initialize(connection)
      super(connection)
      @recorded_queries = []
      @recording = false
    end

    def record
      @recorded_queries.clear
      @recording = true
      yield
    ensure
      @recording = false
    end

    def adapter_name = __getobj__.adapter_name

    def exec_query(sql, name = nil, binds = [], prepare: false)
      record_query(sql, binds) if name == "Durababble SQL"
      __getobj__.exec_query(sql, name, binds, prepare:)
    end

    def exec(sql)
      record_query(sql, [])
      __getobj__.raw_connection.exec(sql)
    end

    def exec_params(sql, params)
      record_query(sql, params)
      __getobj__.raw_connection.exec_params(sql, params)
    end

    def transaction(requires_new: true)
      __getobj__.transaction(requires_new:) { yield(self) }
    end

    private

    def record_query(sql, params)
      return unless @recording

      normalized = sql.strip
      return unless normalized.match?(/\A(?:SELECT|INSERT|UPDATE|DELETE)\b/i)

      @recorded_queries << [normalized, params]
    end
  end

  module QueryPlanAssertions
    extend self

    def explain_plan(connection, sql, params)
      explain_sql = sql.gsub(/\s+FOR UPDATE(?:\s+OF\s+[\w",\s.]+)?\s+SKIP LOCKED\b/i, "")
      connection.exec("SET enable_seqscan = off")
      json = connection.exec_params("EXPLAIN (FORMAT JSON) #{explain_sql}", params).first.fetch("QUERY PLAN")
      JSON.parse(json).first.fetch("Plan")
    ensure
      connection.exec("RESET enable_seqscan")
    end

    def assert_indexes_only!(plan, allowed_indexes:, allow_post_filter_indexes: [], sql:)
      scan_nodes = plan_nodes(plan).select do |node|
        node.fetch("Node Type", "").match?(/Seq Scan|Index Scan|Index Only Scan|Bitmap Heap Scan|Bitmap Index Scan|Table Scan/)
      end
      scan_nodes.each do |node|
        node_type = node.fetch("Node Type", "")
        index_name = node["Index Name"]
        post_filter_allowed = index_allowed?(index_name, allow_post_filter_indexes)
        next if index_scan?(node_type) && index_allowed?(index_name, allowed_indexes) && (!post_filter?(node) || post_filter_allowed)

        raise Minitest::Assertion,
          "expected every scan to be an allowed index scan#{" with no post filter" if allow_post_filter_indexes.empty?} (allowed: #{allowed_indexes.join(", ")}; post-filter allowed: #{allow_post_filter_indexes.join(", ")}), got #{node_type} #{index_name.inspect} for:\n#{sql}\nscan node:\n#{JSON.pretty_generate(node)}\nplan:\n#{JSON.pretty_generate(plan)}"
      end
    end

    def assert_no_table_scan!(plan, sql:)
      scans = plan_nodes(plan).select { |node| node.fetch("Node Type", "").match?(/Seq Scan|Table Scan|Bitmap Heap Scan/i) }
      return if scans.empty?

      raise Minitest::Assertion,
        "expected no table scan, got #{scans.map { |node| node.fetch("Node Type") }.join(", ")} for:\n#{sql}\nplan:\n#{JSON.pretty_generate(plan)}"
    end

    def assert_index_scan!(plan, index:, without_post_filter: false, sql:)
      node = plan_nodes(plan).find do |candidate|
        index_scan?(candidate.fetch("Node Type", "")) && candidate.fetch("Index Name", nil) == index
      end
      unless node
        raise Minitest::Assertion,
          "expected index scan using #{index} for:\n#{sql}\nplan:\n#{JSON.pretty_generate(plan)}"
      end

      if without_post_filter && post_filter?(node)
        raise Minitest::Assertion,
          "expected index scan using #{index} with no post filter for:\n#{sql}\nindex node:\n#{JSON.pretty_generate(node)}"
      end

      node
    end

    def assert_index_scan_with_post_filter!(plan, index:, sql:)
      node = assert_index_scan!(plan, index:, sql:)
      unless post_filter?(node)
        raise Minitest::Assertion,
          "expected index scan using #{index} to have a post filter for:\n#{sql}\nindex node:\n#{JSON.pretty_generate(node)}"
      end

      node
    end

    def assert_bounded_limit!(plan, sql:)
      return unless sql.match?(/\bLIMIT\s+1\b/i)

      limit = plan_nodes(plan).find { |node| node.fetch("Node Type", "") == "Limit" }
      unless limit
        raise Minitest::Assertion,
          "expected LIMIT 1 query to retain a bounded Limit node for:\n#{sql}\nplan:\n#{JSON.pretty_generate(plan)}"
      end

      plan_rows = limit.fetch("Plan Rows", 0).to_i
      return if plan_rows <= 1

      raise Minitest::Assertion,
        "expected LIMIT 1 query to estimate at most one row, got #{plan_rows} for:\n#{sql}\nlimit node:\n#{JSON.pretty_generate(limit)}"
    end

    def assert_skip_locked_shape!(sql:)
      return unless sql.match?(/\bFOR\s+UPDATE\b/i)
      return unless sql.match?(/\bLIMIT\s+1\b/i)
      return if sql.match?(/\bSKIP\s+LOCKED\b/i)

      raise Minitest::Assertion,
        "expected hot FOR UPDATE query to use SKIP LOCKED so workers do not block each other:\n#{sql}"
    end

    def plan_nodes(plan)
      [plan] + Array(plan["Plans"]).flat_map { |child| plan_nodes(child) }
    end

    def post_filter?(node)
      node.key?("Filter") || node.key?("Remote Filter")
    end

    def index_scan?(node_type)
      node_type.match?(/\AIndex(?: Only)? Scan\z/)
    end

    def index_allowed?(index_name, allowed_indexes)
      return false unless index_name

      allowed_indexes.any? do |allowed|
        tails = [48, 47, 40, 32].map { |length| allowed[-[allowed.length, length].min..] }
        index_name == allowed || index_name.end_with?("_#{allowed}") || tails.any? { |tail| index_name.end_with?("_#{tail}") }
      end
    end
  end

  test "rejects bitmap heap scans as table scans in plan assertions" do
    plan = {
      "Node Type" => "Bitmap Heap Scan",
      "Relation Name" => "workflows",
      "Plans" => [{ "Node Type" => "Bitmap Index Scan", "Index Name" => "workflows_queue_idx" }],
    }

    assert_raises_matching(Minitest::Assertion, /Bitmap Heap Scan/) do
      QueryPlanAssertions.assert_no_table_scan!(plan, sql: "SELECT * FROM workflows")
    end
  end

  test "store query registry entries remain executable" do
    refute_empty Durababble::StoreQueries::QUERIES

    Durababble::StoreQueries::QUERIES.each do |id, query|
      assert_equal id, query.id
      assert_includes [:postgres, :mysql], query.backend
      assert_respond_to query.builder, :call
    end
  end

  test "uncovered query lists are explicit and reference registered queries" do
    expected_postgres = [
      :pg_claim_object_command,
      :pg_complete_fence,
      :pg_complete_object_command,
      :pg_enqueue_object_command,
      :pg_fail_fence,
      :pg_insert_fence,
      :pg_lock_object_command,
      :pg_lock_object_command_for_worker,
      :pg_mark_workflow_waiting,
      :pg_object_state,
      :pg_outbox_message,
      :pg_read_fence,
      :pg_step_attempts_for,
      :pg_steps_for,
      :pg_waits_for_workflow,
      :pg_workflow,
    ]
    expected_mysql = [
      :mysql_ack_outbox,
      :mysql_claim_selected_outbox,
      :mysql_claim_selected_workflow,
      :mysql_claim_workflow_update,
      :mysql_current_workflow_lease,
      :mysql_heartbeat_latest_attempt,
      :mysql_heartbeat_step_row,
      :mysql_heartbeat_step_workflow,
      :mysql_insert_outbox,
      :mysql_insert_step_attempt,
      :mysql_outbox_by_key,
      :mysql_outbox_message,
      :mysql_running_step_exists,
      :mysql_step_attempts_for,
      :mysql_steps_for,
      :mysql_supersede_running_step_attempts,
      :mysql_upsert_step_running,
      :mysql_workflow,
      :mysql_workflow_locked_until,
    ]

    assert_equal expected_postgres, Durababble::StoreQueries.uncovered_query_ids(:postgres)
    assert_equal expected_mysql, Durababble::StoreQueries.uncovered_query_ids(:mysql)
    assert_empty expected_postgres - Durababble::StoreQueries.query_ids(:postgres)
    assert_empty expected_mysql - Durababble::StoreQueries.query_ids(:mysql)
  end

  test "hot query inventory names assertion and benchmark coverage" do
    Durababble::StoreQueries.hot_query_coverage.each do |name, metadata|
      refute_empty metadata.fetch(:methods), "missing methods for #{name}"
      refute_empty metadata.fetch(:indexes), "missing intended indexes for #{name}"
      refute_empty metadata.fetch(:assertions), "missing plan assertions for #{name}"
      refute_empty metadata.fetch(:benchmarks), "missing benchmark coverage for #{name}"
    end
  end

  test "store implementation routes hot SQL through the top-level query registry" do
    store_path = File.join(File.expand_path("../..", __dir__), "lib/durababble/store/postgres.rb")
    source = File.read(store_path)
    hot_methods = source.scan(/^    def (claim_runnable_workflow|claim_workflow|heartbeat|workflow_owned\?|heartbeat_step|current_workflow_lease|steal_expired_leases!|record_step_started|record_wait|enqueue_outbox|claim_outbox|ack_outbox|save_object_state|complete_timer_waits|record_step_completed_without_transaction|update_latest_attempt_serialized)\b(.*?)(?=^    def |\n  end\nend\z)/m)
    refute_empty hot_methods

    hot_methods.each do |method_name, body|
      refute_match(/execute_params\(\s*(?:"|<<~SQL)/, body, "#{method_name} SQL must be defined in Durababble::StoreQueries")
    end
  end

  test "accepts index-only scans as valid index scans" do
    plan = { "Node Type" => "Index Only Scan", "Index Name" => "workflows_queue_idx" }

    QueryPlanAssertions.assert_indexes_only!(
      plan,
      allowed_indexes: ["workflows_queue_idx"],
      sql: "SELECT id FROM workflows",
    )
    assert true
  end

  test "keeps hot-path store operations on explicit indexes in production-sized tables" do
    connection = migrated_recording_connection
    seed_large_workflow_fixture(connection, schema)

    operations = {
      "claim_runnable_workflow" => {
        call: -> { store.claim_runnable_workflow(worker_id: "plan-worker", lease_seconds: 60) },
        allowed_indexes: ["workflows_pkey", "workflows_queue_idx", "workflows_runnable_due_idx", "workflows_expired_lease_idx", "workflows_pending_created_idx", "workflows_failed_due_idx", "workflows_canceling_created_idx"],
        allow_post_filter_indexes: ["workflows_queue_idx", "workflows_runnable_due_idx", "workflows_pending_created_idx", "workflows_failed_due_idx", "workflows_canceling_created_idx", "workflows_expired_lease_idx"],
      },
      "claim_workflow" => {
        call: -> { store.claim_workflow(workflow_id: "pending-target", worker_id: "plan-worker", lease_seconds: 60) },
        allowed_indexes: ["workflows_pkey", "workflows_pkey"],
        allow_post_filter_indexes: ["workflows_pkey"],
      },
      "heartbeat" => {
        call: -> { store.heartbeat(workflow_id: "running-owned", worker_id: "owner", lease_seconds: 60) },
        allowed_indexes: ["workflows_pkey"],
        allow_post_filter_indexes: ["workflows_pkey"],
      },
      "workflow_owned" => {
        call: -> { store.workflow_owned?(workflow_id: "running-owned", worker_id: "owner") },
        allowed_indexes: ["workflows_pkey"],
        allow_post_filter_indexes: ["workflows_pkey"],
      },
      "release_worker_leases" => {
        call: -> { store.release_worker_leases!(worker_id: "owner") },
        allowed_indexes: ["workflows_worker_lease_idx", "workflows_pending_created_idx", "outbox_worker_lease_idx", "inbox_worker_lease_idx", "target_activations_worker_lease_idx"],
        allow_post_filter_indexes: ["workflows_pending_created_idx"],
      },
      "heartbeat_step" => {
        call: -> { store.heartbeat_step(workflow_id: "running-owned", position: 0, worker_id: "owner", lease_seconds: 60, cursor: { "offset" => 1 }) },
        allowed_indexes: ["workflows_pkey", "steps_pkey", "step_attempts_workflow_position_status_started_idx"],
        allow_post_filter_indexes: ["workflows_pkey", "steps_pkey", "step_attempts_workflow_position_status_started_idx"],
      },
      "current_workflow_lease" => {
        call: -> { store.current_workflow_lease("running-owned") },
        allowed_indexes: ["workflows_pkey"],
        allow_post_filter_indexes: ["workflows_pkey"],
      },
      "steal_expired_leases" => {
        call: -> { store.steal_expired_leases!(now: Time.now) },
        allowed_indexes: ["workflows_expired_lease_idx"],
      },
      "record_step_started" => {
        call: -> { store.record_step_started(workflow_id: "running-owned", position: 1, name: "next") },
        allowed_indexes: ["workflows_pkey", "workflow_history_pkey", "step_attempts_workflow_position_status_started_idx", "steps_pkey"],
        allow_post_filter_indexes: ["step_attempts_workflow_position_status_started_idx", "step_attempts_workflow_started_position_idx"],
      },
      "record_step_completed" => {
        call: -> { store.record_step_completed(workflow_id: "running-owned", position: 0, result: { "ok" => true }) },
        allowed_indexes: ["workflows_pkey", "workflow_history_pkey", "steps_pkey", "step_attempts_pkey", "step_attempts_workflow_position_status_started_idx", "step_attempts_workflow_started_position_idx"],
        allow_post_filter_indexes: ["step_attempts_workflow_position_status_started_idx", "step_attempts_workflow_started_position_idx"],
      },
      "record_wait" => {
        call: lambda do
          store.record_wait(
            workflow_id: "running-owned",
            position: 2,
            name: "timer",
            wait_request: Durababble::WaitRequest.new(
              kind: "timer",
              wake_at: Time.now + 60,
              event_key: nil,
              context: { "step" => 2 },
            ),
          )
        end,
        allowed_indexes: ["steps_pkey", "workflows_pkey", "workflow_history_pkey", "waits_workflow_status_idx", "step_attempts_pkey", "step_attempts_workflow_position_status_started_idx", "step_attempts_workflow_started_position_idx"],
        allow_post_filter_indexes: ["step_attempts_workflow_position_status_started_idx", "step_attempts_workflow_started_position_idx"],
      },
      "request_workflow_cancellation" => {
        call: -> { store.request_workflow_cancellation(workflow_id: "running-owned", reason: "query plan") },
        allowed_indexes: ["workflows_pkey", "waits_workflow_status_idx", "steps_pkey", "step_attempts_workflow_started_position_idx", "step_attempts_workflow_position_status_started_idx"],
        allow_post_filter_indexes: ["workflows_pkey", "steps_pkey", "step_attempts_workflow_started_position_idx", "step_attempts_workflow_position_status_started_idx"],
      },
      "wake_due_timers" => {
        call: -> { store.wake_due_timers(now: Time.now + 120) },
        allowed_indexes: ["waits_timer_pending_idx", "waits_pkey", "steps_pkey", "workflows_pkey", "workflow_history_pkey", "step_attempts_pkey", "step_attempts_workflow_started_position_idx", "step_attempts_workflow_position_status_started_idx"],
        allow_post_filter_indexes: ["step_attempts_workflow_started_position_idx", "step_attempts_workflow_position_status_started_idx"],
      },
      "waits_for" => {
        call: -> { store.waits_for("running-owned") },
        allowed_indexes: ["waits_workflow_created_idx"],
      },
      "with_fence_existing" => {
        call: -> { store.with_fence(workflow_id: "running-owned", key: "completed-fence", timeout: 1) { "unused" } },
        allowed_indexes: ["fences_pkey", "fences_pkey"],
      },
      "enqueue_outbox" => {
        call: -> { store.enqueue_outbox(workflow_id: "running-owned", topic: "topic", payload: { "x" => 1 }, key: "new-outbox-key") },
        allowed_indexes: ["outbox_key_key", "outbox_key_key"],
      },
      "claim_outbox" => {
        call: -> { store.claim_outbox(worker_id: "plan-worker", lease_seconds: 60) },
        allowed_indexes: ["outbox_pkey", "outbox_queue_idx", "outbox_expired_lease_idx"],
        allow_post_filter_indexes: ["outbox_queue_idx"],
      },
      "ack_outbox" => {
        call: -> { store.ack_outbox("processing-outbox", worker_id: "owner") },
        allowed_indexes: ["outbox_pkey"],
        allow_post_filter_indexes: ["outbox_pkey"],
      },
      "outbox_message" => {
        call: -> { store.outbox_message("pending-outbox") },
        allowed_indexes: ["outbox_pkey"],
        allow_post_filter_indexes: ["outbox_pkey"],
      },
      "workflow" => {
        call: -> { store.workflow("running-owned") },
        allowed_indexes: ["workflows_pkey"],
        allow_post_filter_indexes: ["workflows_pkey"],
      },
      "steps_for" => {
        call: -> { store.steps_for("running-owned") },
        allowed_indexes: ["steps_pkey"],
      },
      "step_attempts_for" => {
        call: -> { store.step_attempts_for("running-owned") },
        allowed_indexes: ["step_attempts_workflow_started_position_idx"],
      },
      "object_state" => {
        call: -> { store.object_state(object_type: "counter", object_id: "object-1") },
        allowed_indexes: ["durable_objects_pkey"],
      },
      "save_object_state" => {
        call: -> { store.save_object_state(object_type: "counter", object_id: "object-1", state: { "count" => 3 }) },
        allowed_indexes: ["durable_objects_pkey"],
      },
    }

    seen_query_ids = []
    operations.each do |name, expectation|
      queries, query_ids = recorded_queries_for(store, connection) { expectation.fetch(:call).call }
      refute_empty(queries, "expected #{name} to issue SQL")
      seen_query_ids.concat(query_ids)

      queries.each do |sql, params|
        plan = QueryPlanAssertions.explain_plan(connection, sql, params)
        QueryPlanAssertions.assert_skip_locked_shape!(sql:)
        QueryPlanAssertions.assert_bounded_limit!(plan, sql:)
        QueryPlanAssertions.assert_no_table_scan!(plan, sql:)
        QueryPlanAssertions.assert_indexes_only!(
          plan,
          allowed_indexes: expectation.fetch(:allowed_indexes),
          allow_post_filter_indexes: expectation.fetch(:allow_post_filter_indexes, []),
          sql:,
        )
      end
    end

    missing_query_ids = Durababble::StoreQueries.plan_required_ids(:postgres) - seen_query_ids.uniq
    assert_empty(missing_query_ids, "query-plan operations did not exercise registered PostgreSQL/YSQL queries: #{missing_query_ids.join(", ")}")
  ensure
    @durababble_store&.drop_schema!
    @durababble_store&.close
    @durababble_store = nil
    @durababble_schema = nil
  end

  private

  def migrated_recording_connection
    skip_without_yugabyte!
    require "pg"

    active_record_class = Durababble::Store.send(:active_record_class_for, durababble_yugabyte_database_url)
    connection = RecordingConnection.new(active_record_class.connection_pool.lease_connection)
    @durababble_schema = "durababble_plan_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    @durababble_store = Durababble::Store.from_active_record(connection:, schema:, owner: active_record_class)
    @durababble_store.migrate!
    connection
  end

  def recorded_queries_for(store, connection, &block)
    query_ids = []
    connection.transaction do
      connection.record do
        query_ids = store.send(:record_store_query_ids, &block)
      end
      raise ActiveRecord::Rollback
    end
    [connection.recorded_queries.dup, query_ids]
  end

  def seed_large_workflow_fixture(connection, schema)
    quoted_schema = PG::Connection.quote_ident(schema)
    serialized_empty = Durababble::Store::SERIALIZER.dump({}).unpack1("H*")
    serialized_count = Durababble::Store::SERIALIZER.dump({ "count" => 1 }).unpack1("H*")
    serialized_result = Durababble::Store::SERIALIZER.dump({ "done" => true }).unpack1("H*")
    serialized_wait_context = Durababble::Store::SERIALIZER.dump({ "waiting" => true }).unpack1("H*")
    serialized_outbox = Durababble::Store::SERIALIZER.dump({ "message" => true }).unpack1("H*")

    connection.exec(<<~SQL)
      INSERT INTO #{quoted_schema}.workflows (id, name, status, input, result, locked_by, locked_until, created_at, updated_at)
      SELECT 'completed-' || i, 'demo', 'completed', decode('#{serialized_empty}', 'hex'), decode('#{serialized_result}', 'hex'), NULL, NULL, now() - (i || ' seconds')::interval, now()
      FROM generate_series(1, 2000) AS i;

      INSERT INTO #{quoted_schema}.workflows (id, name, status, input, locked_by, locked_until, created_at, updated_at)
      SELECT 'waiting-' || i, 'demo', 'waiting', decode('#{serialized_empty}', 'hex'), NULL, NULL, now() - (i || ' seconds')::interval, now()
      FROM generate_series(1, 2000) AS i;

      INSERT INTO #{quoted_schema}.workflows (id, name, status, input, locked_by, locked_until, created_at, updated_at)
      SELECT 'pending-' || i, 'demo', 'pending', decode('#{serialized_empty}', 'hex'), NULL, NULL, now() - (i || ' seconds')::interval, now()
      FROM generate_series(1, 2000) AS i;

      INSERT INTO #{quoted_schema}.workflows (id, name, status, input, locked_by, locked_until, next_run_at, created_at, updated_at)
      SELECT 'failed-' || i, 'demo', 'failed', decode('#{serialized_empty}', 'hex'), NULL, NULL, now() - interval '1 minute', now() - (i || ' seconds')::interval, now()
      FROM generate_series(1, 2000) AS i;

      INSERT INTO #{quoted_schema}.workflows (id, name, status, input, locked_by, locked_until, created_at, updated_at)
      SELECT 'running-active-' || i, 'demo', 'running', decode('#{serialized_empty}', 'hex'), 'other', now() + interval '5 minutes', now() - (i || ' seconds')::interval, now()
      FROM generate_series(1, 2000) AS i;

      INSERT INTO #{quoted_schema}.workflows (id, name, status, input, locked_by, locked_until, created_at, updated_at)
      SELECT 'running-expired-' || i, 'demo', 'running', decode('#{serialized_empty}', 'hex'), 'stale', now() - interval '5 minutes', now() - (i || ' seconds')::interval, now()
      FROM generate_series(1, 2000) AS i;

      INSERT INTO #{quoted_schema}.workflows (id, name, status, input, locked_by, locked_until, created_at, updated_at)
      VALUES
        ('pending-target', 'demo', 'pending', decode('#{serialized_count}', 'hex'), NULL, NULL, now() - interval '2 hours', now()),
        ('running-owned', 'demo', 'running', decode('#{serialized_count}', 'hex'), 'owner', now() + interval '5 minutes', now() - interval '3 hours', now());

      INSERT INTO #{quoted_schema}.steps (workflow_id, position, name, status, result, started_at, completed_at, updated_at)
      SELECT 'running-owned', i, 'step-' || i, CASE WHEN i = 0 THEN 'running' ELSE 'completed' END, decode('#{serialized_result}', 'hex'), now() - (i || ' seconds')::interval, now(), now()
      FROM generate_series(0, 200) AS i;

      INSERT INTO #{quoted_schema}.step_attempts (id, workflow_id, position, name, status, result, started_at, completed_at)
      SELECT 'attempt-' || i, 'running-owned', i % 10, 'step-' || (i % 10), CASE WHEN i % 10 = 0 THEN 'running' ELSE 'completed' END, decode('#{serialized_result}', 'hex'), now() - (i || ' seconds')::interval, now()
      FROM generate_series(1, 3000) AS i;

      INSERT INTO #{quoted_schema}.waits (id, workflow_id, position, kind, event_key, wake_at, context, status, created_at)
      SELECT 'timer-wait-' || i, 'waiting-' || i, 0, 'timer', NULL, now() - interval '1 minute', decode('#{serialized_wait_context}', 'hex'), 'pending', now() - (i || ' seconds')::interval
      FROM generate_series(1, 2000) AS i;

      INSERT INTO #{quoted_schema}.waits (id, workflow_id, position, kind, event_key, wake_at, context, status, created_at)
      SELECT 'event-wait-' || i, 'waiting-' || i, 0, 'event', CASE WHEN i = 1 THEN 'target-event' ELSE 'other-event' END, NULL, decode('#{serialized_wait_context}', 'hex'), 'pending', now() - (i || ' seconds')::interval
      FROM generate_series(1, 2000) AS i;

      INSERT INTO #{quoted_schema}.waits (id, workflow_id, position, kind, event_key, wake_at, context, status, created_at)
      SELECT 'owned-wait-' || i, 'running-owned', i, 'timer', NULL, now() + interval '1 hour', decode('#{serialized_wait_context}', 'hex'), 'pending', now() - (i || ' seconds')::interval
      FROM generate_series(1, 200) AS i;

      INSERT INTO #{quoted_schema}.fences (workflow_id, key, status, result, completed_at)
      VALUES ('running-owned', 'completed-fence', 'completed', decode('#{serialized_result}', 'hex'), now());

      INSERT INTO #{quoted_schema}.outbox (id, workflow_id, topic, payload, key, status, locked_by, locked_until, created_at)
      SELECT 'pending-outbox-' || i, 'running-owned', 'topic', decode('#{serialized_outbox}', 'hex'), 'pending-key-' || i, 'pending', NULL, NULL, now() - (i || ' seconds')::interval
      FROM generate_series(1, 2000) AS i;

      INSERT INTO #{quoted_schema}.outbox (id, workflow_id, topic, payload, key, status, locked_by, locked_until, created_at)
      SELECT 'processed-outbox-' || i, 'running-owned', 'topic', decode('#{serialized_outbox}', 'hex'), 'processed-key-' || i, 'processed', NULL, NULL, now() - (i || ' seconds')::interval
      FROM generate_series(1, 2000) AS i;

      INSERT INTO #{quoted_schema}.outbox (id, workflow_id, topic, payload, key, status, locked_by, locked_until, created_at)
      VALUES
        ('pending-outbox', 'running-owned', 'topic', decode('#{serialized_outbox}', 'hex'), 'pending-outbox-key', 'pending', NULL, NULL, now() - interval '2 hours'),
        ('processing-outbox', 'running-owned', 'topic', decode('#{serialized_outbox}', 'hex'), 'processing-outbox-key', 'processing', 'owner', now() + interval '5 minutes', now() - interval '1 hour');

      INSERT INTO #{quoted_schema}.durable_objects (object_type, object_id, state, created_at, updated_at)
      VALUES ('counter', 'object-1', decode('#{serialized_result}', 'hex'), now() - interval '2 hours', now());

    SQL
  end
end
