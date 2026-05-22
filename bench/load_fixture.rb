#!/usr/bin/env ruby
# frozen_string_literal: true

require "pg"
require "securerandom"
$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "durababble"

database_url = ENV.fetch("DURABABBLE_DATABASE_URL")
schema = ENV.fetch("DURABABBLE_BENCH_SCHEMA")
rows = Integer(ENV.fetch("DURABABBLE_BENCH_FIXTURE_SIZE"))
seed = Integer(ENV.fetch("DURABABBLE_BENCH_SEED", "12345"))
rng = Random.new(seed)
store = Durababble::Store.connect(database_url:, schema:)
store.send(:execute, "SET client_min_messages TO warning") unless store.is_a?(Durababble::MysqlStore)
store.migrate!

if store.is_a?(Durababble::MysqlStore)
  puts "loading #{rows} historical workflows/waits/outbox rows into #{schema}"
  table = ->(name) { store.send(:table, name) }
  rows.times do |i|
    status = i.even? ? "completed" : "running"
    workflow_id = "fixture-wf-%08d" % i
    locked_until = status == "running" ? Time.now.utc + 3600 : nil
    store.send(:execute_params,
      "INSERT IGNORE INTO #{table.call("workflows")} (id, name, status, input, result, locked_by, locked_until) VALUES (?, ?, ?, ?, ?, ?, ?)",
      [workflow_id, "fixture", status, store.send(:dump_serialized, { "i" => i, "seed" => seed }), store.send(:dump_serialized, { "done" => true, "i" => i }), status == "running" ? "fixture-worker" : nil, locked_until]
    )
    store.send(:execute_params,
      "INSERT IGNORE INTO #{table.call("steps")} (workflow_id, position, name, status, result, started_at, completed_at) VALUES (?, 0, 'fixture_step', 'completed', ?, NOW(6), NOW(6))",
      [workflow_id, store.send(:dump_serialized, { "i" => i })]
    )
    if (i % 5).zero?
      store.send(:execute_params,
        "INSERT IGNORE INTO #{table.call("waits")} (id, workflow_id, position, kind, event_key, wake_at, context, status) VALUES (?, ?, 1, 'timer', NULL, ?, ?, 'pending')",
        ["fixture-wait-%08d" % i, workflow_id, Time.now.utc + 86_400 + rng.rand(3600), store.send(:dump_serialized, { "i" => i })]
      )
    end
    if (i % 7).zero?
      store.send(:execute_params,
        "INSERT IGNORE INTO #{table.call("outbox")} (id, workflow_id, topic, payload, `key`, status, locked_by, locked_until) VALUES (?, ?, 'fixture.topic', ?, ?, 'processed', NULL, NULL)",
        ["fixture-outbox-%08d" % i, workflow_id, store.send(:dump_serialized, { "i" => i }), "fixture:#{i}"]
      )
    end
  end
  store.close
  puts "fixture load complete"
  exit
end

connection = PG.connect(database_url)
connection.exec("SET client_min_messages TO warning")
q_schema = PG::Connection.quote_ident(schema)

puts "loading #{rows} historical workflows/waits/outbox rows into #{schema}"
connection.transaction do |conn|
  rows.times do |i|
    status = i.even? ? "completed" : "running"
    workflow_id = "fixture-wf-%08d" % i
    locked_until = status == "running" ? Time.now.utc + 3600 : nil
    conn.exec_params(
      "INSERT INTO #{q_schema}.workflows (id, name, status, input, result, locked_by, locked_until) VALUES ($1,$2,$3,$4::bytea,$5::bytea,$6,$7::timestamptz) ON CONFLICT (id) DO NOTHING",
      [workflow_id, "fixture", status, store.send(:dump_serialized, { "i" => i, "seed" => seed }), store.send(:dump_serialized, { "done" => true, "i" => i }), status == "running" ? "fixture-worker" : nil, locked_until&.iso8601]
    )
    conn.exec_params(
      "INSERT INTO #{q_schema}.steps (workflow_id, position, name, status, result, started_at, completed_at) VALUES ($1,0,'fixture_step','completed',$2::bytea,now(),now()) ON CONFLICT DO NOTHING",
      [workflow_id, store.send(:dump_serialized, { "i" => i })]
    )
    if (i % 5).zero?
      conn.exec_params(
        "INSERT INTO #{q_schema}.waits (id, workflow_id, position, kind, event_key, wake_at, context, status) VALUES ($1,$2,1,'timer',NULL,$3::timestamptz,$4::bytea,'pending') ON CONFLICT DO NOTHING",
        ["fixture-wait-%08d" % i, workflow_id, (Time.now.utc + 86_400 + rng.rand(3600)).iso8601, store.send(:dump_serialized, { "i" => i })]
      )
    end
    if (i % 7).zero?
      conn.exec_params(
        "INSERT INTO #{q_schema}.outbox (id, workflow_id, topic, payload, key, status, locked_by, locked_until) VALUES ($1,$2,'fixture.topic',$3::bytea,$4,'processed',NULL,NULL) ON CONFLICT (key) DO NOTHING",
        ["fixture-outbox-%08d" % i, workflow_id, store.send(:dump_serialized, { "i" => i }), "fixture:#{i}"]
      )
    end
  end
end
connection.close
store.close
puts "fixture load complete"
