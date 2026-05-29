# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "json"

class DurababbleMysqlQueryPlanTest < DurababbleTestCase
  MYSQL_ALLOWED_ACCESS_TYPES = ["const", "eq_ref", "ref", "range"].freeze
  MYSQL_DEFAULT_MAX_ROWS_EXAMINED_PER_SCAN = 600

  test "uses indexes for hot MySQL queue wait activation and inbox predicates" do
    backend = durababble_store_backends.find(&:mysql?)
    skip("MySQL-backed Durababble tests require a MySQL database URL") unless backend

    with_durababble_store(backend, "plan") do
      seed_mysql_plan_fixture

      now = Time.now.utc
      expectations = {
        "workflow claim probe" => {
          sql: query_sql(:claim_runnable_workflow, name_sql: ""),
          params: ["default", "worker-1"],
          expected_key_fragment: "workflows_claim",
          # The owner-fence NOT EXISTS adds a correlated PRIMARY-key lookup on
          # durable_objects (eq_ref), which is the optimal shape for that probe.
          expected_access_types: ["range", "eq_ref"],
          max_rows_examined_per_scan: 4_000,
        },
        "expired workflow lease count probe" => {
          sql: query_sql(:count_expired_workflow_leases),
          params: [now],
          expected_key_fragment: "workflows_expired_lease",
          expected_access_types: ["range"],
        },
        "expired workflow lease recovery probe" => {
          sql: query_sql(:steal_expired_leases),
          params: [now],
          expected_key_fragment: "workflows_expired_lease",
          expected_access_types: ["range"],
        },
        "outbox claim probe" => {
          sql: query_sql(:claim_outbox),
          expected_key_fragment: "outbox_claim",
          expected_access_types: ["range"],
        },
        "target activation claim probe" => {
          sql: query_sql(:claim_target_activation, filter_sql: "AND target_kind IN (?) AND target_type IN (?)"),
          params: ["default", now, "object", "counter"],
          expected_key_fragment: "target_activations_claim",
          expected_access_types: ["range"],
        },
        "inbox mailbox claim probe" => {
          sql: query_sql(:inbox_claim_rows_for_update, limit: 10),
          params: ["default", "object", "counter", "object-1"],
          expected_key_fragment: "inbox_target",
        },
        "inbox mailbox head probe" => {
          sql: query_sql(:inbox_head_for_update),
          params: ["default", "object", "counter", "object-1"],
          expected_key_fragment: "inbox_target",
        },
        "current object lease probe" => {
          sql: query_sql(:current_object_lease),
          params: ["counter", "object-1"],
          expected_key_fragment: "PRIMARY",
          expected_access_types: ["const"],
          max_rows_examined_per_scan: 1,
        },
        "inbox idempotency probe" => {
          sql: query_sql(:existing_inbox_message_for_idempotency),
          params: [inbox_idempotency_hash("idempotency-1", target_kind: "object", target_type: "counter", target_id: "object-1")],
          expected_key_fragment: "inbox_idempotency_hash",
          expected_access_types: ["const"],
          max_rows_examined_per_scan: 1,
        },
        "workflow lease release probe" => {
          sql: query_sql(:release_workflow_leases, index: mysql_index_name("workflows", "worker_lease")),
          params: ["other"],
          expected_key_fragment: "workflows_worker_lease",
          expected_access_types: ["range"],
        },
        "outbox lease release probe" => {
          sql: query_sql(:release_outbox_leases, index: mysql_index_name("outbox", "worker_lease")),
          params: ["owner"],
          expected_key_fragment: "outbox_worker_lease",
          expected_access_types: ["range"],
        },
        "inbox lease release probe" => {
          sql: query_sql(:release_inbox_leases, index: mysql_index_name("inbox", "worker_lease")),
          params: ["owner"],
          expected_key_fragment: "inbox_worker_lease",
          expected_access_types: ["range"],
        },
        "target activation lease release probe" => {
          sql: query_sql(:release_target_activation_leases, index: mysql_index_name("target_activations", "worker_lease")),
          params: ["owner"],
          expected_key_fragment: "target_activations_worker_lease",
          expected_access_types: ["range"],
        },
      }

      expectations.each do |name, expectation|
        assert_mysql_plan_uses_index(
          name,
          expectation.fetch(:sql),
          params: expectation.fetch(:params, []),
          expected_key_fragment: expectation.fetch(:expected_key_fragment),
          expected_access_types: expectation.fetch(:expected_access_types, ["ref"]),
          max_rows_examined_per_scan: expectation.fetch(:max_rows_examined_per_scan, MYSQL_DEFAULT_MAX_ROWS_EXAMINED_PER_SCAN),
          allow_filesort: expectation.fetch(:allow_filesort, false),
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

  def assert_mysql_plan_uses_index(name, sql, params:, expected_key_fragment:, expected_access_types:, max_rows_examined_per_scan:, allow_filesort:)
    plan = explain_json(sql, params)
    access_paths = mysql_access_paths(plan)
    refute_empty(access_paths, "expected #{name} to have table access nodes: #{JSON.pretty_generate(plan)}")
    assert_equal(
      expected_access_types,
      access_paths.map { |path| path.fetch("access_type") },
      "expected #{name} access type shape to stay bounded, got #{access_paths.inspect}: #{JSON.pretty_generate(plan)}",
    )
    assert(
      access_paths.all? { |path| MYSQL_ALLOWED_ACCESS_TYPES.include?(path.fetch("access_type")) },
      "expected #{name} to avoid full scans and full index scans, got #{access_paths.inspect}: #{JSON.pretty_generate(plan)}",
    )
    assert(
      access_paths.any? { |path| path.fetch("key", "").include?(expected_key_fragment) },
      "expected #{name} to use index containing #{expected_key_fragment.inspect}, got #{access_paths.inspect}: #{JSON.pretty_generate(plan)}",
    )
    assert_mysql_key_parts!(name, access_paths, expected_key_fragment, plan)
    assert_mysql_rows_examined!(name, access_paths, max_rows_examined_per_scan, plan)
    assert_mysql_filesort_shape!(name, plan, allow_filesort:)
    assert_no_unexpected_mysql_temporary_table!(name, plan)
  end

  def mysql_access_paths(node)
    case node
    when Hash
      current = []
      current << node.slice(
        "table_name",
        "access_type",
        "key",
        "used_key_parts",
        "rows_examined_per_scan",
        "rows_produced_per_join",
        "using_temporary_table",
      ) if node.key?("access_type")
      current + node.values.flat_map { |value| mysql_access_paths(value) }
    when Array
      node.flat_map { |value| mysql_access_paths(value) }
    else
      []
    end
  end

  def assert_mysql_key_parts!(name, access_paths, expected_key_fragment, plan)
    expected_parts = mysql_expected_key_parts(expected_key_fragment)
    return if expected_parts.empty?

    matching_path = access_paths.find { |path| path.fetch("key", "").include?(expected_key_fragment) }
    used_parts = Array(matching_path&.fetch("used_key_parts", []))
    missing_parts = expected_parts - used_parts
    assert_empty(
      missing_parts,
      "expected #{name} to use key parts #{expected_parts.inspect}, got #{used_parts.inspect}: #{JSON.pretty_generate(plan)}",
    )
  end

  def mysql_expected_key_parts(expected_key_fragment)
    case expected_key_fragment
    when "workflows_claim"
      ["worker_pool", "queue_available_at"]
    when "workflows_expired_lease"
      ["status", "locked_until"]
    when "outbox_claim"
      ["queue_available_at"]
    when "target_activations_claim"
      ["worker_pool", "target_kind", "target_type", "queue_available_at"]
    when "inbox_target"
      ["target_kind", "target_type", "target_id"]
    when "inbox_idempotency_hash"
      ["idempotency_hash"]
    when /worker_lease/
      ["status", "locked_by"]
    else
      []
    end
  end

  def assert_mysql_rows_examined!(name, access_paths, max_rows_examined_per_scan, plan)
    over_budget = access_paths.select do |path|
      rows = path["rows_examined_per_scan"]
      rows && rows.to_i > max_rows_examined_per_scan
    end
    assert_empty(
      over_budget,
      "expected #{name} to examine at most #{max_rows_examined_per_scan} rows per scan, got #{access_paths.inspect}: #{JSON.pretty_generate(plan)}",
    )
  end

  def assert_mysql_filesort_shape!(name, plan, allow_filesort:)
    filesorts = mysql_plan_values(plan, "using_filesort")
    return if allow_filesort

    assert_empty(filesorts.select { |value| value == true }, "expected #{name} not to require filesort: #{JSON.pretty_generate(plan)}")
  end

  def assert_no_unexpected_mysql_temporary_table!(name, plan)
    temporary_tables = mysql_plan_values(plan, "using_temporary_table")
    unexpected = temporary_tables.reject { |value| [false, "for update"].include?(value) }
    assert_empty(unexpected, "expected #{name} not to require a temporary table: #{JSON.pretty_generate(plan)}")
  end

  def mysql_plan_values(node, key)
    case node
    when Hash
      current = node.key?(key) ? [node.fetch(key)] : []
      current + node.values.flat_map { |value| mysql_plan_values(value, key) }
    when Array
      node.flat_map { |value| mysql_plan_values(value, key) }
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
    inbox_payload = serialized_literal({ "command" => true })
    outbox = serialized_literal({ "message" => true })
    now = Time.now.utc

    execute("START TRANSACTION")
    1.upto(500) do |i|
      created_at = mysql_literal(now - i)
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, result, created_at, updated_at) VALUES ('completed-#{i}', 'demo', 'completed', #{empty}, #{result}, #{created_at}, #{created_at})")
      waiting_next_run_at = i == 1 ? now - 60 : now + 3600
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, next_run_at, created_at, updated_at) VALUES ('waiting-#{i}', 'demo', 'waiting', #{empty}, #{mysql_literal(waiting_next_run_at)}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, created_at, updated_at) VALUES ('pending-#{i}', 'demo', 'pending', #{empty}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, next_run_at, created_at, updated_at) VALUES ('failed-#{i}', 'demo', 'failed', #{empty}, #{mysql_literal(now - 60)}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, next_run_at, created_at, updated_at) VALUES ('canceling-#{i}', 'demo', 'canceling', #{empty}, NULL, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, locked_by, locked_until, created_at, updated_at) VALUES ('running-active-#{i}', 'demo', 'running', #{empty}, 'other', #{mysql_literal(now + 300)}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, locked_by, locked_until, created_at, updated_at) VALUES ('running-expired-#{i}', 'demo', 'running', #{empty}, 'stale', #{mysql_literal(now - 300)}, #{created_at}, #{created_at})")
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
    # Seed a live object lease so the `current_object_lease` probe finds a matching row;
    # without `locked_by`/`locked_until` populated, the planner short-circuits to
    # "Impossible WHERE" and the EXPLAIN tree has no access-path nodes to inspect.
    execute("INSERT INTO #{table("durable_objects")} (object_type, object_id, state, locked_by, locked_until, created_at, updated_at) VALUES ('counter', 'object-1', #{result}, 'owner', #{mysql_literal(now + 300)}, #{mysql_literal(now - 3600)}, #{mysql_literal(now)})")
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
