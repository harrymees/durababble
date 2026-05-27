# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "json"

class DurababbleMysqlQueryPlanTest < DurababbleTestCase
  test "uses indexes for hot MySQL queue wait activation and inbox predicates" do
    backend = durababble_store_backends.find(&:mysql?)
    skip("MySQL-backed Durababble tests require a MySQL database URL") unless backend

    with_durababble_store(backend, "plan") do
      seed_mysql_plan_fixture

      now = Time.now.utc
      expectations = {
        "pending workflow claim probe" => {
          sql: query_sql(:claim_pending_workflow, name_sql: ""),
          params: ["default"],
          expected_key_fragment: "workflows_queue",
        },
        "failed workflow claim probe" => {
          sql: query_sql(:claim_failed_workflow, name_sql: ""),
          params: ["default"],
          expected_key_fragment: "workflows_queue",
        },
        "canceling workflow claim probe" => {
          sql: query_sql(:claim_canceling_workflow, name_sql: ""),
          params: ["default"],
          expected_key_fragment: "workflows_queue",
        },
        "expired workflow claim probe" => {
          sql: query_sql(:claim_expired_workflow, name_sql: ""),
          params: ["default"],
          expected_key_fragment: "workflows_expired_lease",
        },
        "pending outbox claim probe" => {
          sql: query_sql(:claim_pending_outbox),
          expected_key_fragment: "outbox_queue",
        },
        "expired outbox claim probe" => {
          sql: query_sql(:claim_expired_outbox),
          expected_key_fragment: "outbox_expired_lease",
        },
        "timer wait wake probe" => {
          sql: query_sql(:complete_timer_waits, limit: 100),
          params: [now],
          expected_key_fragment: "waits_timer_pending",
        },
        "pending target activation claim probe" => {
          sql: query_sql(:claim_pending_target_activation, filter_sql: "AND target_kind IN (?) AND target_type IN (?)"),
          params: ["default", now, "object", "counter"],
          expected_key_fragment: "target_activations_queue",
        },
        "expired target activation claim probe" => {
          sql: query_sql(:claim_expired_target_activation, filter_sql: "AND target_kind IN (?) AND target_type IN (?)"),
          params: ["default", now, "object", "counter"],
          expected_key_fragment: "target_activations_expired",
        },
        "inbox mailbox claim probe" => {
          sql: query_sql(:inbox_claim_rows_for_update, limit: 10),
          params: ["object", "counter", "object-1"],
          expected_key_fragment: "inbox_target",
        },
        "inbox mailbox head probe" => {
          sql: query_sql(:inbox_head_for_update),
          params: ["object", "counter", "object-1"],
          expected_key_fragment: "inbox_target",
        },
        "current object activation lease probe" => {
          sql: query_sql(:current_object_activation_lease),
          params: ["counter", "object-1"],
          expected_key_fragment: "PRIMARY",
        },
        "current object inbox lease probe" => {
          sql: query_sql(:current_object_lease),
          params: ["counter", "object-1"],
          expected_key_fragment: "inbox_target",
        },
        "inbox idempotency probe" => {
          sql: query_sql(:existing_inbox_message_for_idempotency),
          params: [inbox_idempotency_hash("idempotency-1", target_kind: "object", target_type: "counter", target_id: "object-1")],
          expected_key_fragment: "inbox_idempotency_hash",
        },
        "workflow lease count probe" => {
          sql: query_sql(:count_workflow_leases, index: mysql_index_name("workflows", "worker_lease")),
          params: ["other"],
          expected_key_fragment: "workflows_worker_lease",
        },
        "workflow lease release probe" => {
          sql: query_sql(:release_workflow_leases, index: mysql_index_name("workflows", "worker_lease")),
          params: ["other"],
          expected_key_fragment: "workflows_worker_lease",
        },
        "outbox lease count probe" => {
          sql: query_sql(:count_outbox_leases, index: mysql_index_name("outbox", "worker_lease")),
          params: ["owner"],
          expected_key_fragment: "outbox_worker_lease",
        },
        "outbox lease release probe" => {
          sql: query_sql(:release_outbox_leases, index: mysql_index_name("outbox", "worker_lease")),
          params: ["owner"],
          expected_key_fragment: "outbox_worker_lease",
        },
        "inbox lease count probe" => {
          sql: query_sql(:count_inbox_leases, index: mysql_index_name("inbox", "worker_lease")),
          params: ["owner"],
          expected_key_fragment: "inbox_worker_lease",
        },
        "inbox lease release probe" => {
          sql: query_sql(:release_inbox_leases, index: mysql_index_name("inbox", "worker_lease")),
          params: ["owner"],
          expected_key_fragment: "inbox_worker_lease",
        },
        "target activation lease count probe" => {
          sql: query_sql(:count_target_activation_leases, index: mysql_index_name("target_activations", "worker_lease")),
          params: ["owner"],
          expected_key_fragment: "target_activations_worker_lease",
        },
        "target activation lease release probe" => {
          sql: query_sql(:release_target_activation_leases, index: mysql_index_name("target_activations", "worker_lease")),
          params: ["owner"],
          expected_key_fragment: "target_activations_worker_lease",
        },
      }

      expectations.each do |name, expectation|
        assert_mysql_plan_uses_index(
          name,
          expectation.fetch(:sql),
          params: expectation.fetch(:params, []),
          expected_key_fragment: expectation.fetch(:expected_key_fragment),
        )
      end
    end
  end

  private

  def table(name)
    store.send(:table, name)
  end

  def execute(sql)
    store.send(:execute, sql)
  end

  def query_sql(id, **locals)
    store.send(:store_query_sql, id, **locals)
  end

  def mysql_index_name(table_name, suffix)
    store.send(:index_name, table_name, suffix)
  end

  def execute_params(sql, params)
    store.send(:execute_params, sql, params)
  end

  def explain_json(sql, params = [])
    row = execute_params("EXPLAIN FORMAT=JSON #{sql}", params).first
    JSON.parse(row.fetch("EXPLAIN"))
  end

  def assert_mysql_plan_uses_index(name, sql, params:, expected_key_fragment:)
    plan = explain_json(sql, params)
    access_paths = mysql_access_paths(plan)
    refute_empty(access_paths, "expected #{name} to have table access nodes: #{JSON.pretty_generate(plan)}")
    assert(
      access_paths.all? { |path| path.fetch("access_type") != "ALL" },
      "expected #{name} to avoid full table scans: #{JSON.pretty_generate(plan)}",
    )
    assert(
      access_paths.any? { |path| path.fetch("key", "").include?(expected_key_fragment) },
      "expected #{name} to use index containing #{expected_key_fragment.inspect}, got #{access_paths.inspect}: #{JSON.pretty_generate(plan)}",
    )
  end

  def mysql_access_paths(node)
    case node
    when Hash
      current = []
      current << node.slice("table_name", "access_type", "key", "rows") if node.key?("access_type")
      current + node.values.flat_map { |value| mysql_access_paths(value) }
    when Array
      node.flat_map { |value| mysql_access_paths(value) }
    else
      []
    end
  end

  def mysql_literal(value)
    case value
    when String
      "'#{value.gsub("'", "''")}'"
    when Time
      "'#{value.utc.strftime("%Y-%m-%d %H:%M:%S.%6N")}'"
    when NilClass
      "NULL"
    else
      value.to_s
    end
  end

  def serialized_literal(value)
    "X'#{Durababble::Store::SERIALIZER.dump(value).unpack1("H*")}'"
  end

  def inbox_idempotency_hash(idempotency_key, target_kind:, target_type:, target_id:)
    Digest::SHA256.hexdigest(Durababble::Store::SERIALIZER.dump({
      "target_kind" => target_kind,
      "target_type" => target_type,
      "target_id" => target_id,
      "idempotency_key" => idempotency_key,
    }))
  end

  def seed_mysql_plan_fixture
    empty = serialized_literal({})
    result = serialized_literal({ "done" => true })
    wait_context = serialized_literal({ "waiting" => true })
    inbox_payload = serialized_literal({ "command" => true })
    outbox = serialized_literal({ "message" => true })
    now = Time.now.utc

    execute("START TRANSACTION")
    1.upto(500) do |i|
      created_at = mysql_literal(now - i)
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, result, created_at, updated_at) VALUES ('completed-#{i}', 'demo', 'completed', #{empty}, #{result}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, created_at, updated_at) VALUES ('waiting-#{i}', 'demo', 'waiting', #{empty}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, created_at, updated_at) VALUES ('pending-#{i}', 'demo', 'pending', #{empty}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, next_run_at, created_at, updated_at) VALUES ('failed-#{i}', 'demo', 'failed', #{empty}, #{mysql_literal(now - 60)}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, next_run_at, created_at, updated_at) VALUES ('canceling-#{i}', 'demo', 'canceling', #{empty}, NULL, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, locked_by, locked_until, created_at, updated_at) VALUES ('running-active-#{i}', 'demo', 'running', #{empty}, 'other', #{mysql_literal(now + 300)}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, locked_by, locked_until, created_at, updated_at) VALUES ('running-expired-#{i}', 'demo', 'running', #{empty}, 'stale', #{mysql_literal(now - 300)}, #{created_at}, #{created_at})")
      timer_wake_at = i == 1 ? now - 60 : now + 3600
      execute("INSERT INTO #{table("waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status, created_at) VALUES ('timer-wait-#{i}', 'waiting-#{i}', 0, 'timer', NULL, #{mysql_literal(timer_wake_at)}, #{wait_context}, 'pending', #{created_at})")
      execute("INSERT INTO #{table("waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status, created_at) VALUES ('event-wait-#{i}', 'waiting-#{i}', 0, 'event', #{mysql_literal(i == 1 ? "target-event" : "other-event")}, NULL, #{wait_context}, 'pending', #{created_at})")
      execute("INSERT INTO #{table("outbox")} (id, workflow_id, topic, payload, `key`, status, created_at) VALUES ('pending-outbox-#{i}', 'waiting-#{i}', 'topic', #{outbox}, 'pending-key-#{i}', 'pending', #{created_at})")
      execute("INSERT INTO #{table("outbox")} (id, workflow_id, topic, payload, `key`, status, locked_by, locked_until, created_at) VALUES ('processing-outbox-#{i}', 'waiting-#{i}', 'topic', #{outbox}, 'processing-key-#{i}', 'processing', 'owner', #{mysql_literal(now - 60)}, #{created_at})")
      activation_ready_at = i == 1 ? now - 60 : now + 3600
      execute("INSERT INTO #{table("target_activations")} (target_kind, target_type, target_id, status, ready_at, created_at, updated_at) VALUES ('object', 'counter', 'pending-object-#{i}', 'pending', #{mysql_literal(activation_ready_at)}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("target_activations")} (target_kind, target_type, target_id, status, ready_at, locked_by, locked_until, created_at, updated_at) VALUES ('object', 'counter', 'expired-object-#{i}', 'running', #{created_at}, 'owner', #{mysql_literal(now - 60)}, #{created_at}, #{created_at})")
      idempotency_key = i == 1 ? "idempotency-1" : nil
      idempotency_hash = idempotency_key && inbox_idempotency_hash(idempotency_key, target_kind: "object", target_type: "counter", target_id: "object-1")
      execute("INSERT INTO #{table("inbox")} (id, target_kind, target_type, target_id, sequence, message_kind, method_name, operation_id, idempotency_key, idempotency_hash, shape_hash, payload, status, ready_at, locked_by, locked_until, created_at, updated_at) VALUES ('inbox-object-1-#{i}', 'object', 'counter', 'object-1', #{i}, 'ask', 'increment', 'op-object-1-#{i}', #{mysql_literal(idempotency_key)}, #{mysql_literal(idempotency_hash)}, 'shape', #{inbox_payload}, #{mysql_literal(i == 1 ? "running" : "pending")}, #{created_at}, #{mysql_literal(i == 1 ? "owner" : nil)}, #{mysql_literal(i == 1 ? now + 300 : nil)}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("inbox")} (id, target_kind, target_type, target_id, sequence, message_kind, method_name, operation_id, idempotency_key, shape_hash, payload, status, ready_at, created_at, updated_at) VALUES ('inbox-other-#{i}', 'object', 'counter', 'other-#{i}', 1, 'ask', 'increment', 'op-other-#{i}', NULL, 'shape', #{inbox_payload}, 'pending', #{created_at}, #{created_at}, #{created_at})")
    end
    execute("INSERT INTO #{table("target_activations")} (target_kind, target_type, target_id, status, ready_at, locked_by, locked_until, created_at, updated_at) VALUES ('object', 'counter', 'object-1', 'running', #{mysql_literal(now - 3600)}, 'owner', #{mysql_literal(now + 300)}, #{mysql_literal(now - 3600)}, #{mysql_literal(now)})")
    execute("INSERT INTO #{table("durable_objects")} (object_type, object_id, state, created_at, updated_at) VALUES ('counter', 'object-1', #{result}, #{mysql_literal(now - 3600)}, #{mysql_literal(now)})")
    execute("INSERT INTO #{table("durable_object_commands")} (id, object_type, object_id, method_name, args, kwargs, status, created_at) VALUES ('object-command-pending', 'counter', 'object-1', 'increment', #{empty}, #{empty}, 'pending', #{mysql_literal(now - 3600)})")
    execute("COMMIT")
  rescue StandardError
    begin
      execute("ROLLBACK")
    rescue
      nil
    end
    raise
  end
end
