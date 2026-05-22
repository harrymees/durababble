#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "securerandom"
$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "durababble"

schema = ENV.fetch("DURABABBLE_BENCH_SCHEMA")
database_url = ENV.fetch("DURABABBLE_DATABASE_URL")
store = Durababble::Store.connect(database_url:, schema:)
store.send(:execute, "SET client_min_messages TO warning") unless store.is_a?(Durababble::MysqlStore)
store.migrate!

STDIN.each_line do |line|
  request = JSON.parse(line)
  command = request.fetch("command")
  payload = request.fetch("payload")
  result = case command
           when "ping"
             { "pong" => true, "i" => payload["i"] }
           when "enqueue_claim"
             id = store.enqueue_workflow(name: "rpc", input: payload)
             claimed = store.claim_workflow(workflow_id: id, worker_id: "rpc-worker", lease_seconds: 30)
             { "id" => id, "claimed" => !claimed.nil? }
           when "enqueue_claim_batch"
             count = Integer(payload.fetch("count"))
             start = Integer(payload.fetch("start", 0))
             ids = count.times.map { SecureRandom.uuid }
             if store.is_a?(Durababble::MysqlStore)
               values_sql = ids.each_index.map { "(?, ?, 'pending', ?, ?)" }.join(", ")
               insert_params = ids.each_with_index.flat_map do |id, offset|
                 [id, "rpc", store.send(:dump_serialized, payload.merge("i" => start + offset)), Time.now.utc]
               end
               store.send(:execute_params, <<~SQL, insert_params)
                 INSERT INTO #{store.send(:table, "workflows")} (id, name, status, input, created_at)
                 VALUES #{values_sql}
               SQL
               id_placeholders = ids.map { "?" }.join(", ")
               store.send(:execute_params, <<~SQL, ["rpc-worker"] + ids)
                 UPDATE #{store.send(:table, "workflows")}
                 SET status = 'running', locked_by = ?, locked_until = DATE_ADD(NOW(6), INTERVAL 30 SECOND), updated_at = NOW(6)
                 WHERE id IN (#{id_placeholders})
               SQL
               { "ids" => ids, "claimed" => ids.length }
             else
               values_sql = ids.each_index.map do |index|
                 base = index * 4
                 "($#{base + 1}, $#{base + 2}, 'pending', $#{base + 3}::bytea, $#{base + 4}::timestamptz)"
               end.join(", ")
               insert_params = ids.each_with_index.flat_map do |id, offset|
                 [id, "rpc", store.send(:dump_serialized, payload.merge("i" => start + offset)), Time.now.utc.iso8601(6)]
               end
               q_schema = PG::Connection.quote_ident(schema)
               store.send(:execute_params, <<~SQL, insert_params)
                 INSERT INTO #{q_schema}.workflows (id, name, status, input, created_at)
                 VALUES #{values_sql}
               SQL
               id_placeholders = ids.each_index.map { |index| "$#{index + 2}" }.join(", ")
               updated = store.send(:execute_params, <<~SQL, ["rpc-worker"] + ids)
                 UPDATE #{q_schema}.workflows
                 SET status = 'running', locked_by = $1, locked_until = now() + interval '30 seconds', updated_at = now()
                 WHERE id IN (#{id_placeholders})
               SQL
               { "ids" => ids, "claimed" => updated.cmd_tuples }
             end
           else
             raise ArgumentError, "unknown command #{command}"
           end
  STDOUT.puts(JSON.generate({ ok: true, result: }))
  STDOUT.flush
rescue StandardError => e
  STDOUT.puts(JSON.generate({ ok: false, error: "#{e.class}: #{e.message}" }))
  STDOUT.flush
end
