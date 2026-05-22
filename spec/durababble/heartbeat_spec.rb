# frozen_string_literal: true

require "spec_helper"
require "securerandom"

RSpec.describe "Durababble step heartbeats", :integration do
  durababble_store_backends.each do |backend|
    context "with #{backend.name}" do
      let(:backend_descriptor) { backend }
      let(:schema) { "#{backend_descriptor.default_schema_prefix}_heartbeat_test_#{Process.pid}_#{SecureRandom.hex(4)}" }
      let(:store) { Durababble::Store.connect(database_url: backend_descriptor.database_url, schema:) }

      after do
        store&.drop_schema!
        store&.close
      end

      it "extends the workflow lease and stores an opaque cursor during a running step" do
        store.migrate!
        test_store = store
        parse_lease_time = ->(value) { value.is_a?(Time) ? value : Time.parse(value) }
        observed = {}
        workflow = durababble_test_workflow("heartbeat-extension") do
          test_step("long-step") do |ctx, heartbeat|
            observed[:cursor_before] = heartbeat.cursor
            before = parse_lease_time.call(test_store.workflow(ctx.fetch("workflow_id")).fetch("locked_until"))
            heartbeat.record({ "offset" => 10 })
            after = parse_lease_time.call(test_store.workflow(ctx.fetch("workflow_id")).fetch("locked_until"))
            observed[:extended] = after > before
            { "done" => true }
          end
        end

        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})
        update_workflow_input(workflow_id, { "workflow_id" => workflow_id })

        run = described_engine(lease_seconds: 3_600).resume(workflow, workflow_id:)

        expect(run.status).to eq("completed")
        expect(run.result).to eq({ "done" => true })
        expect(observed).to include(cursor_before: nil, extended: true)
        expect(store.steps_for(workflow_id).first.fetch("heartbeat_cursor")).to eq({ "offset" => 10 })
      end

      it "passes the last heartbeat cursor into the next step invocation after lease expiry recovery" do
        store.migrate!
        attempts = []
        workflow = durababble_test_workflow("heartbeat-cursor-resume") do
          test_step("download") do |_ctx, heartbeat|
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
        expire_lease = ->(workflow_id) { expire_workflow_lease(workflow_id, test_store) }
        workflow = durababble_test_workflow("zombie-heartbeat") do
          test_step("work") do |ctx, heartbeat|
            expire_lease.call(ctx.fetch("workflow_id"))
            heartbeat.record({ "too_late" => true })
            { "should_not" => "complete" }
          end
        end

        workflow_id = store.enqueue_workflow(name: workflow.name, input: {})
        update_workflow_input(workflow_id, { "workflow_id" => workflow_id })

        expect do
          described_engine(worker_id: "zombie", lease_seconds: 1).resume(workflow, workflow_id:)
        end.to raise_error(Durababble::LeaseConflict, /expired or moved/)

        row = store.workflow(workflow_id)
        expect(row).to include("status" => "running", "locked_by" => "zombie")
        expect(parse_time(row.fetch("locked_until"))).to be < Time.now
        expect(store.steps_for(workflow_id).first.fetch("heartbeat_cursor")).to be_nil
      end
    end
  end

  def described_engine(worker_id: "owner", lease_seconds: 60)
    Durababble::Engine.new(store:, worker_id:, lease_seconds:)
  end

  def update_workflow_input(workflow_id, input)
    payload = store.send(:dump_serialized, input)
    if backend_descriptor.mysql?
      store.send(:execute_params, "UPDATE #{table("workflows")} SET input = ? WHERE id = ?", [payload, workflow_id])
    else
      store.send(:execute_params, "UPDATE #{table("workflows")} SET input = $2::bytea WHERE id = $1", [workflow_id, payload])
    end
  end

  def expire_workflow_lease(workflow_id, target_store = store)
    if backend_descriptor.mysql?
      target_store.send(:execute_params, "UPDATE #{table("workflows")} SET locked_until = DATE_SUB(UTC_TIMESTAMP(6), INTERVAL 1 HOUR) WHERE id = ?", [workflow_id])
    else
      target_store.send(:execute_params, "UPDATE #{table("workflows")} SET locked_until = now() - interval '1 hour' WHERE id = $1", [workflow_id])
    end
  end

  def table(name)
    store.send(:table, name)
  end

  def parse_time(value)
    value.is_a?(Time) ? value : Time.parse(value)
  end
end
