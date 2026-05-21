# frozen_string_literal: true

require "spec_helper"

RSpec.describe Durababble::Store, :integration do
  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_test_#{Process.pid}" }
  let(:store) { described_class.connect(database_url:, schema:) }

  after do
    store&.drop_schema!
    store&.close
  end

  it "migrates and persists workflow plus step state in Yugabyte" do
    store.migrate!
    workflow_id = store.create_workflow(name: "demo", input: { "count" => 1 })

    store.record_step_started(workflow_id:, position: 0, name: "add_one")
    store.record_step_completed(workflow_id:, position: 0, result: { "count" => 2 })
    store.complete_workflow(workflow_id, result: { "count" => 2 })

    workflow = store.workflow(workflow_id)
    expect(workflow.fetch("status")).to eq("completed")
    expect(workflow.fetch("result")).to eq({ "count" => 2 })
    expect(store.steps_for(workflow_id).first.fetch("status")).to eq("completed")
  end

  it "stores runtime values as Paquito bytea payloads instead of JSONB" do
    store.migrate!
    workflow_id = store.create_workflow(name: "demo", input: { "count" => 1 })
    store.complete_workflow(workflow_id, result: { "count" => 2 })

    columns = PG.connect(database_url) do |connection|
      connection.exec_params(<<~SQL, [schema]).map { |row| [row.fetch("table_name"), row.fetch("column_name"), row.fetch("data_type")] }
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1
          AND column_name IN ('input', 'result', 'context', 'payload')
        ORDER BY table_name, column_name
      SQL
    end

    expect(columns).to include(
      ["workflows", "input", "bytea"],
      ["workflows", "result", "bytea"],
      ["steps", "result", "bytea"],
      ["waits", "context", "bytea"],
      ["outbox", "payload", "bytea"]
    )
    expect(columns).not_to include(a_collection_including("jsonb"))

    encoded_input = PG.connect(database_url) do |connection|
      connection.exec_params("SELECT input FROM #{PG::Connection.quote_ident(schema)}.workflows WHERE id = $1", [workflow_id]).first.fetch("input")
    end
    payload = PG::Connection.unescape_bytea(encoded_input)
    expect(payload.bytes.first).to eq(1)
    expect(Durababble::Store::SERIALIZER.load(payload)).to eq({ "count" => 1 })
  end

  it "migrates legacy JSONB runtime columns into Paquito bytea columns" do
    connection = PG.connect(database_url)
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
      ["legacy", "demo", "completed", '{"count":1}', '{"count":2}']
    )
    connection.close

    store.migrate!

    expect(store.workflow("legacy").fetch("input")).to eq({ "count" => 1 })
    expect(store.workflow("legacy").fetch("result")).to eq({ "count" => 2 })
    data_type = PG.connect(database_url) do |verify|
      verify.exec_params(<<~SQL, [schema, "workflows", "input"]).first.fetch("data_type")
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
      SQL
    end
    expect(data_type).to eq("bytea")
  end
end
