#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "durababble"

schema = ENV.fetch("DURABABBLE_BENCH_SCHEMA")
database_url = ENV.fetch("DURABABBLE_DATABASE_URL")
store = Durababble::Store.connect(database_url:, schema:)
store.send(:execute, "SET client_min_messages TO warning")
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
           else
             raise ArgumentError, "unknown command #{command}"
           end
  STDOUT.puts(JSON.generate({ ok: true, result: }))
  STDOUT.flush
rescue StandardError => e
  STDOUT.puts(JSON.generate({ ok: false, error: "#{e.class}: #{e.message}" }))
  STDOUT.flush
end
