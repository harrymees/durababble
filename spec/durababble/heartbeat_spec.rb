# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe "Durababble step heartbeats", :integration do
  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_heartbeat_test_#{Process.pid}_#{SecureRandom.hex(4)}" }
  let(:store) { Durababble::Store.connect(database_url:, schema:) }

  after do
    store&.drop_schema!
    store&.close
  end

  it "extends the workflow lease and stores an opaque cursor during a running step" do
    store.migrate!
    test_store = store
    observed = {}
    workflow = Durababble::Workflow.define("heartbeat-extension") do
      step("long-step") do |ctx, heartbeat|
        observed[:cursor_before] = heartbeat.cursor
        before = Time.parse(test_store.workflow(ctx.fetch("workflow_id")).fetch("locked_until"))
        heartbeat.record({ "offset" => 10 })
        after = Time.parse(test_store.workflow(ctx.fetch("workflow_id")).fetch("locked_until"))
        observed[:extended] = after > before
        { "done" => true }
      end
    end

    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})
    store.send(:execute_params, "UPDATE #{quoted_schema}.workflows SET input = $2::bytea WHERE id = $1", [workflow_id, serialized({ "workflow_id" => workflow_id })])

    run = described_engine(lease_seconds: 3_600).resume(workflow, workflow_id:)

    expect(run.status).to eq("completed")
    expect(observed).to include(cursor_before: nil, extended: true)
    expect(store.steps_for(workflow_id).first.fetch("heartbeat_cursor")).to eq({ "offset" => 10 })
  end

  it "passes the last heartbeat cursor into the next step invocation after lease expiry recovery" do
    store.migrate!
    attempts = []
    workflow = Durababble::Workflow.define("heartbeat-cursor-resume") do
      step("download") do |_ctx, heartbeat|
        attempts << heartbeat.cursor
        if attempts.length == 1
          heartbeat.record({ "page" => 42 })
          raise Durababble::InjectedCrash, "crash after heartbeat"
        end
        { "resumed_from" => heartbeat.cursor.fetch("page") }
      end
    end

    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})
    expect { described_engine(lease_seconds: 1).resume(workflow, workflow_id:) }.to raise_error(Durababble::InjectedCrash)

    store.steal_expired_leases!(now: Time.now + 2)
    recovered = described_engine(worker_id: "recover", lease_seconds: 60).resume(workflow, workflow_id:)

    expect(recovered.status).to eq("completed")
    expect(recovered.result).to eq({ "resumed_from" => 42 })
    expect(attempts).to eq([nil, { "page" => 42 }])
  end

  it "rejects a zombie heartbeat after the worker misses its lease deadline" do
    store.migrate!
    test_store = store
    test_schema = quoted_schema
    workflow = Durababble::Workflow.define("zombie-heartbeat") do
      step("work") do |ctx, heartbeat|
        test_store.send(:execute_params, "UPDATE #{test_schema}.workflows SET locked_until = now() - interval '1 second' WHERE id = $1", [ctx.fetch("workflow_id")])
        heartbeat.record({ "too_late" => true })
        { "should_not" => "complete" }
      end
    end

    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})
    store.send(:execute_params, "UPDATE #{quoted_schema}.workflows SET input = $2::bytea WHERE id = $1", [workflow_id, serialized({ "workflow_id" => workflow_id })])

    expect do
      described_engine(worker_id: "zombie", lease_seconds: 1).resume(workflow, workflow_id:)
    end.to raise_error(Durababble::LeaseConflict, /expired or moved/)

    row = store.workflow(workflow_id)
    expect(row).to include("status" => "running", "locked_by" => "zombie")
    expect(Time.parse(row.fetch("locked_until"))).to be < Time.now
    expect(store.steps_for(workflow_id).first.fetch("heartbeat_cursor")).to be_nil
  end

  def described_engine(worker_id: "owner", lease_seconds: 60)
    Durababble::Engine.new(store:, worker_id:, lease_seconds:)
  end

  def quoted_schema
    PG::Connection.quote_ident(schema)
  end

  def serialized(value)
    store.send(:dump_serialized, value)
  end
end
