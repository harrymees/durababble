# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "json"

class DurababbleMysqlQueryPlanTest < DurababbleTestCase
  test "uses indexes for the same hot store queue and wait predicates as the Yugabyte plan suite" do
    backend = durababble_store_backends.find(&:mysql?)
    skip("MySQL-backed Durababble tests require a MySQL database URL") unless backend

    with_durababble_store(backend, "plan") do
      seed_mysql_plan_fixture

      expectations = {
        "pending workflow claim probe" => [
          "SELECT id, created_at FROM #{table("workflows")} WHERE status = 'pending' AND (next_run_at IS NULL OR next_run_at <= NOW(6)) ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED",
          "workflows_queue",
        ],
        "failed workflow claim probe" => [
          "SELECT id, created_at FROM #{table("workflows")} WHERE status = 'failed' AND next_run_at IS NOT NULL AND next_run_at <= NOW(6) ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED",
          "workflows_queue",
        ],
        "expired workflow claim probe" => [
          "SELECT id, created_at FROM #{table("workflows")} WHERE status = 'running' AND locked_until < NOW(6) ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED",
          "workflows_queue",
        ],
        "pending outbox claim probe" => [
          "SELECT id, created_at FROM #{table("outbox")} WHERE status = 'pending' ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED",
          "outbox_queue",
        ],
        "expired outbox claim probe" => [
          "SELECT id, created_at FROM #{table("outbox")} WHERE status = 'processing' AND locked_until < NOW(6) ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED",
          "outbox_queue",
        ],
        "event wait wake probe" => [
          "SELECT w.* FROM #{table("waits")} AS w JOIN #{table("workflows")} AS wf ON wf.id = w.workflow_id WHERE w.status = 'pending' AND wf.status = 'waiting' AND kind = 'event' AND event_key = 'target-event' FOR UPDATE SKIP LOCKED",
          "waits_event_pending",
        ],
        "timer wait wake probe" => [
          "SELECT w.* FROM #{table("waits")} AS w JOIN #{table("workflows")} AS wf ON wf.id = w.workflow_id WHERE w.status = 'pending' AND wf.status = 'waiting' AND kind = 'timer' AND wake_at <= NOW(6) FOR UPDATE SKIP LOCKED",
          "waits_timer_pending",
        ],
      }

      expectations.each do |name, (sql, expected_key_fragment)|
        plan = explain_json(sql)
        access_paths = mysql_access_paths(plan)
        refute_empty access_paths, "expected #{name} to have table access nodes: #{JSON.pretty_generate(plan)}"
        assert(
          access_paths.all? { |path| path.fetch("access_type") != "ALL" },
          "expected #{name} to avoid full table scans: #{JSON.pretty_generate(plan)}",
        )
        assert(
          access_paths.any? { |path| path.fetch("key", "").include?(expected_key_fragment) },
          "expected #{name} to use index containing #{expected_key_fragment.inspect}, got #{access_paths.inspect}: #{JSON.pretty_generate(plan)}",
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

  def explain_json(sql)
    row = execute("EXPLAIN FORMAT=JSON #{sql}").first
    JSON.parse(row.fetch("EXPLAIN"))
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

  def seed_mysql_plan_fixture
    empty = serialized_literal({})
    result = serialized_literal({ "done" => true })
    wait_context = serialized_literal({ "waiting" => true })
    outbox = serialized_literal({ "message" => true })
    now = Time.now.utc

    execute("START TRANSACTION")
    1.upto(500) do |i|
      created_at = mysql_literal(now - i)
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, result, created_at, updated_at) VALUES ('completed-#{i}', 'demo', 'completed', #{empty}, #{result}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, created_at, updated_at) VALUES ('waiting-#{i}', 'demo', 'waiting', #{empty}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, created_at, updated_at) VALUES ('pending-#{i}', 'demo', 'pending', #{empty}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, next_run_at, created_at, updated_at) VALUES ('failed-#{i}', 'demo', 'failed', #{empty}, #{mysql_literal(now - 60)}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, locked_by, locked_until, created_at, updated_at) VALUES ('running-active-#{i}', 'demo', 'running', #{empty}, 'other', #{mysql_literal(now + 300)}, #{created_at}, #{created_at})")
      execute("INSERT INTO #{table("workflows")} (id, name, status, input, locked_by, locked_until, created_at, updated_at) VALUES ('running-expired-#{i}', 'demo', 'running', #{empty}, 'stale', #{mysql_literal(now - 300)}, #{created_at}, #{created_at})")
      timer_wake_at = i == 1 ? now - 60 : now + 3600
      execute("INSERT INTO #{table("waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status, created_at) VALUES ('timer-wait-#{i}', 'waiting-#{i}', 0, 'timer', NULL, #{mysql_literal(timer_wake_at)}, #{wait_context}, 'pending', #{created_at})")
      event_key = i == 1 ? "target-event" : "other-event"
      execute("INSERT INTO #{table("waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status, created_at) VALUES ('event-wait-#{i}', 'waiting-#{i}', 0, 'event', #{mysql_literal(event_key)}, NULL, #{wait_context}, 'pending', #{created_at})")
      execute("INSERT INTO #{table("outbox")} (id, workflow_id, topic, payload, `key`, status, created_at) VALUES ('pending-outbox-#{i}', 'waiting-#{i}', 'topic', #{outbox}, 'pending-key-#{i}', 'pending', #{created_at})")
      execute("INSERT INTO #{table("outbox")} (id, workflow_id, topic, payload, `key`, status, locked_by, locked_until, created_at) VALUES ('processing-outbox-#{i}', 'waiting-#{i}', 'topic', #{outbox}, 'processing-key-#{i}', 'processing', 'owner', #{mysql_literal(now - 60)}, #{created_at})")
    end
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
