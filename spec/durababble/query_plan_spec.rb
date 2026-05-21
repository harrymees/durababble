# frozen_string_literal: true

require "spec_helper"
require "delegate"
require "json"

RSpec.describe "Durababble query plans", :integration do
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

    def exec(sql)
      record_query(sql, [])
      __getobj__.exec(sql)
    end

    def exec_params(sql, params)
      record_query(sql, params)
      __getobj__.exec_params(sql, params)
    end

    def transaction
      if __getobj__.transaction_status == PG::PQTRANS_IDLE
        __getobj__.transaction { yield(self) }
      else
        savepoint = "durababble_plan_#{object_id.abs}"
        __getobj__.exec("SAVEPOINT #{savepoint}")
        begin
          result = yield(self)
        rescue Exception
          __getobj__.exec("ROLLBACK TO SAVEPOINT #{savepoint}")
          __getobj__.exec("RELEASE SAVEPOINT #{savepoint}")
          raise
        else
          __getobj__.exec("RELEASE SAVEPOINT #{savepoint}")
          result
        end
      end
    end

    private

    def record_query(sql, params)
      return unless @recording

      normalized = sql.strip
      return unless normalized.match?(/\A(?:SELECT|UPDATE|DELETE)\b/i)

      @recorded_queries << [normalized, params]
    end
  end

  module QueryPlanAssertions
    module_function

    def explain_plan(connection, sql, params)
      json = connection.exec_params("EXPLAIN (FORMAT JSON) #{sql}", params).first.fetch("QUERY PLAN")
      JSON.parse(json).first.fetch("Plan")
    end

    def expect_indexes_only!(plan, allowed_indexes:, allow_post_filter_indexes: [], sql:)
      scan_nodes = plan_nodes(plan).select { |node| node.fetch("Node Type", "").match?(/Seq Scan|Index Scan|Index Only Scan|Bitmap Heap Scan|Bitmap Index Scan|Table Scan/) }
      scan_nodes.each do |node|
        node_type = node.fetch("Node Type", "")
        index_name = node["Index Name"]
        post_filter_allowed = allow_post_filter_indexes.include?(index_name)
        next if index_scan?(node_type) && allowed_indexes.include?(index_name) && (!post_filter?(node) || post_filter_allowed)

        raise RSpec::Expectations::ExpectationNotMetError,
              "expected every scan to be an allowed index scan#{allow_post_filter_indexes.empty? ? " with no post filter" : ""} (allowed: #{allowed_indexes.join(", ")}; post-filter allowed: #{allow_post_filter_indexes.join(", ")}), got #{node_type} #{index_name.inspect} for:\n#{sql}\nscan node:\n#{JSON.pretty_generate(node)}\nplan:\n#{JSON.pretty_generate(plan)}"
      end
    end

    def expect_no_table_scan!(plan, sql:)
      scans = plan_nodes(plan).select { |node| node.fetch("Node Type", "").match?(/Seq Scan|Table Scan|Bitmap Heap Scan/i) }
      return if scans.empty?

      raise RSpec::Expectations::ExpectationNotMetError,
            "expected no table scan, got #{scans.map { |node| node.fetch("Node Type") }.join(", ")} for:\n#{sql}\nplan:\n#{JSON.pretty_generate(plan)}"
    end

    def expect_index_scan!(plan, index:, without_post_filter: false, sql:)
      node = plan_nodes(plan).find do |candidate|
        index_scan?(candidate.fetch("Node Type", "")) && candidate.fetch("Index Name", nil) == index
      end
      raise RSpec::Expectations::ExpectationNotMetError, "expected index scan using #{index} for:\n#{sql}\nplan:\n#{JSON.pretty_generate(plan)}" unless node

      if without_post_filter && post_filter?(node)
        raise RSpec::Expectations::ExpectationNotMetError,
              "expected index scan using #{index} with no post filter for:\n#{sql}\nindex node:\n#{JSON.pretty_generate(node)}"
      end

      node
    end

    def expect_index_scan_with_post_filter!(plan, index:, sql:)
      node = expect_index_scan!(plan, index:, sql:)
      raise RSpec::Expectations::ExpectationNotMetError, "expected index scan using #{index} to have a post filter for:\n#{sql}\nindex node:\n#{JSON.pretty_generate(node)}" unless post_filter?(node)

      node
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
  end

  it "rejects bitmap heap scans as table scans in plan assertions" do
    plan = {
      "Node Type" => "Bitmap Heap Scan",
      "Relation Name" => "workflows",
      "Plans" => [{ "Node Type" => "Bitmap Index Scan", "Index Name" => "workflows_queue_idx" }]
    }

    expect { QueryPlanAssertions.expect_no_table_scan!(plan, sql: "SELECT * FROM workflows") }.to raise_error(RSpec::Expectations::ExpectationNotMetError, /Bitmap Heap Scan/)
  end

  it "accepts index-only scans as valid index scans" do
    plan = { "Node Type" => "Index Only Scan", "Index Name" => "workflows_queue_idx" }

    expect do
      QueryPlanAssertions.expect_indexes_only!(plan, allowed_indexes: ["workflows_queue_idx"], sql: "SELECT id FROM workflows")
    end.not_to raise_error
  end

  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_plan_test_#{Process.pid}" }
  let(:connection) { RecordingConnection.new(PG.connect(database_url)) }
  let(:store) { Durababble::Store.new(connection, schema:) }

  after do
    store&.drop_schema!
    store&.close
  end

  it "keeps hot-path store operations on explicit indexes in production-sized tables" do
    store.migrate!
    seed_large_workflow_fixture(connection, schema)

    operations = {
      "claim_runnable_workflow" => {
        call: -> { store.claim_runnable_workflow(worker_id: "plan-worker", lease_seconds: 60) },
        allowed_indexes: ["workflows_pkey", "workflows_queue_idx", "workflows_runnable_due_idx", "workflows_expired_lease_idx"],
        allow_post_filter_indexes: ["workflows_queue_idx", "workflows_runnable_due_idx"]
      },
      "claim_workflow" => {
        call: -> { store.claim_workflow(workflow_id: "pending-target", worker_id: "plan-worker", lease_seconds: 60) },
        allowed_indexes: ["workflows_pkey", "workflows_pkey"],
        allow_post_filter_indexes: ["workflows_pkey"]
      },
      "heartbeat" => {
        call: -> { store.heartbeat(workflow_id: "running-owned", worker_id: "owner", lease_seconds: 60) },
        allowed_indexes: ["workflows_pkey"],
        allow_post_filter_indexes: ["workflows_pkey"]
      },
      "heartbeat_step" => {
        call: -> { store.heartbeat_step(workflow_id: "running-owned", position: 0, worker_id: "owner", lease_seconds: 60, cursor: { "offset" => 1 }) },
        allowed_indexes: ["workflows_pkey", "steps_pkey", "step_attempts_workflow_position_status_started_idx"],
        allow_post_filter_indexes: ["workflows_pkey", "steps_pkey", "step_attempts_workflow_position_status_started_idx"]
      },
      "current_workflow_lease" => {
        call: -> { store.current_workflow_lease("running-owned") },
        allowed_indexes: ["workflows_pkey"],
        allow_post_filter_indexes: ["workflows_pkey"]
      },
      "steal_expired_leases" => {
        call: -> { store.steal_expired_leases!(now: Time.now) },
        allowed_indexes: ["workflows_expired_lease_idx"]
      },
      "record_step_started" => {
        call: -> { store.record_step_started(workflow_id: "running-owned", position: 1, name: "next") },
        allowed_indexes: ["step_attempts_workflow_position_status_started_idx", "steps_pkey"],
        allow_post_filter_indexes: ["step_attempts_workflow_position_status_started_idx", "step_attempts_workflow_started_position_idx"]
      },
      "record_step_completed" => {
        call: -> { store.record_step_completed(workflow_id: "running-owned", position: 0, result: { "ok" => true }) },
        allowed_indexes: ["steps_pkey", "step_attempts_pkey", "step_attempts_workflow_position_status_started_idx", "step_attempts_workflow_started_position_idx"],
        allow_post_filter_indexes: ["step_attempts_workflow_position_status_started_idx", "step_attempts_workflow_started_position_idx"]
      },
      "record_wait" => {
        call: -> { store.record_wait(workflow_id: "running-owned", position: 2, name: "timer", wait_request: Durababble::WaitRequest.new(kind: "timer", wake_at: Time.now + 60, event_key: nil, context: { "step" => 2 })) },
        allowed_indexes: ["steps_pkey", "workflows_pkey", "step_attempts_pkey", "step_attempts_workflow_position_status_started_idx", "step_attempts_workflow_started_position_idx"],
        allow_post_filter_indexes: ["step_attempts_workflow_position_status_started_idx", "step_attempts_workflow_started_position_idx"]
      },
      "wake_due_timers" => {
        call: -> { store.wake_due_timers(now: Time.now + 120) },
        allowed_indexes: ["waits_timer_pending_idx", "waits_pkey", "steps_pkey", "workflows_pkey", "step_attempts_pkey", "step_attempts_workflow_started_position_idx", "step_attempts_workflow_position_status_started_idx"],
        allow_post_filter_indexes: ["step_attempts_workflow_started_position_idx", "step_attempts_workflow_position_status_started_idx"]
      },
      "signal_event" => {
        call: -> { store.signal_event("target-event", payload: { "seen" => true }) },
        allowed_indexes: ["waits_event_pending_idx", "waits_pkey", "steps_pkey", "step_attempts_pkey", "step_attempts_workflow_started_position_idx", "step_attempts_workflow_position_status_started_idx", "workflows_pkey"],
        allow_post_filter_indexes: ["step_attempts_workflow_started_position_idx", "step_attempts_workflow_position_status_started_idx"]
      },
      "waits_for" => {
        call: -> { store.waits_for("running-owned") },
        allowed_indexes: ["waits_workflow_created_idx"]
      },
      "with_fence_existing" => {
        call: -> { store.with_fence(workflow_id: "running-owned", key: "completed-fence", timeout: 1) { "unused" } },
        allowed_indexes: ["fences_pkey", "fences_pkey"]
      },
      "enqueue_outbox" => {
        call: -> { store.enqueue_outbox(workflow_id: "running-owned", topic: "topic", payload: { "x" => 1 }, key: "new-outbox-key") },
        allowed_indexes: ["outbox_key_key", "outbox_key_key"]
      },
      "claim_outbox" => {
        call: -> { store.claim_outbox(worker_id: "plan-worker", lease_seconds: 60) },
        allowed_indexes: ["outbox_pkey", "outbox_queue_idx", "outbox_expired_lease_idx"],
        allow_post_filter_indexes: ["outbox_queue_idx"]
      },
      "ack_outbox" => {
        call: -> { store.ack_outbox("processing-outbox", worker_id: "owner") },
        allowed_indexes: ["outbox_pkey"],
        allow_post_filter_indexes: ["outbox_pkey"]
      },
      "outbox_message" => {
        call: -> { store.outbox_message("pending-outbox") },
        allowed_indexes: ["outbox_pkey"],
        allow_post_filter_indexes: ["outbox_pkey"]
      },
      "workflow" => {
        call: -> { store.workflow("running-owned") },
        allowed_indexes: ["workflows_pkey"],
        allow_post_filter_indexes: ["workflows_pkey"]
      },
      "steps_for" => {
        call: -> { store.steps_for("running-owned") },
        allowed_indexes: ["steps_pkey"]
      },
      "step_attempts_for" => {
        call: -> { store.step_attempts_for("running-owned") },
        allowed_indexes: ["step_attempts_workflow_started_position_idx"]
      }
    }

    operations.each do |name, expectation|
      queries = recorded_queries_for(connection) { expectation.fetch(:call).call }
      expect(queries).not_to be_empty, "expected #{name} to issue SQL"

      queries.each do |sql, params|
        plan = QueryPlanAssertions.explain_plan(connection, sql, params)
        QueryPlanAssertions.expect_no_table_scan!(plan, sql:)
        QueryPlanAssertions.expect_indexes_only!(
          plan,
          allowed_indexes: expectation.fetch(:allowed_indexes),
          allow_post_filter_indexes: expectation.fetch(:allow_post_filter_indexes, []),
          sql:
        )
      end
    end
  end

  def recorded_queries_for(connection)
    connection.transaction do
      connection.record { yield }
      raise PG::RollbackTransaction
    end
    connection.recorded_queries.dup
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

      INSERT INTO #{quoted_schema}.workflows (id, name, status, input, locked_by, locked_until, created_at, updated_at)
      SELECT 'failed-' || i, 'demo', 'failed', decode('#{serialized_empty}', 'hex'), NULL, NULL, now() - (i || ' seconds')::interval, now()
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

    SQL
  end
end
