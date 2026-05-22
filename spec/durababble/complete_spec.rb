# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Durababble complete durable execution support", :integration do
  durababble_store_backends.each do |backend|
    context "with #{backend.name}" do
      let(:schema) { "#{backend.default_schema_prefix}_complete_test_#{Process.pid}_#{object_id.abs}" }
      let(:store) { Durababble::Store.connect(database_url: backend.database_url, schema:) }

      after do
        store&.drop_schema!
        store&.close
      end

      it "implements the guarantee matrix explicitly" do
        store.migrate!

        # Guarantee: workflows start pending, are claimable once, and leases prevent concurrent workers.
        workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 2 })
        claim = store.claim_runnable_workflow(worker_id: "worker-a", lease_seconds: 60)
        expect(claim.fetch("id")).to eq(workflow_id)
        expect(store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 60)).to be_nil

        # Guarantee: heartbeats extend a live lease and stale leases can be stolen.
        before = parse_time(store.workflow(workflow_id).fetch("locked_until"))
        store.heartbeat(workflow_id:, worker_id: "worker-a", lease_seconds: 120)
        after = parse_time(store.workflow(workflow_id).fetch("locked_until"))
        expect(after).to be > before
        expect(store.steal_expired_leases!(now: Time.now + 121)).to eq(1)
        stolen = store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 60)
        expect(stolen.fetch("id")).to eq(workflow_id)

        # Guarantee: execution is resumable, completed steps are not re-run, and attempts are append-only.
        events = []
        run = engine(worker_id: "worker-b").resume(counter_workflow(events:), workflow_id:)
        expect(run.status).to eq("completed")
        expect(run.result).to eq({ "count" => 6 })
        expect(events).to eq(%w[increment double])
        again = engine(worker_id: "worker-b").resume(counter_workflow(events:), workflow_id:)
        expect(again.result).to eq({ "count" => 6 })
        expect(events).to eq(%w[increment double])
        expect(store.step_attempts_for(workflow_id).map { |a| a.fetch("status") }).to eq(%w[completed completed])

        # Guarantee: idempotency fences return the first persisted result and do not re-run the side effect.
        side_effects = 0
        first = store.with_fence(workflow_id:, key: "charge:1") do
          side_effects += 1
          { "charge_id" => "ch_1" }
        end
        second = store.with_fence(workflow_id:, key: "charge:1") do
          side_effects += 1
          { "charge_id" => "ch_2" }
        end
        expect(first).to eq({ "charge_id" => "ch_1" })
        expect(second).to eq({ "charge_id" => "ch_1" })
        expect(side_effects).to eq(1)

        # Guarantee: outbox messages are durable, unique by idempotency key, claimable once, and acknowledgeable.
        outbox_id = store.enqueue_outbox(workflow_id:, topic: "email", payload: { "to" => "recipient@example.com" }, key: "email:1")
        expect(store.enqueue_outbox(workflow_id:, topic: "email", payload: { "to" => "duplicate" }, key: "email:1")).to eq(outbox_id)
        message = store.claim_outbox(worker_id: "mailer", lease_seconds: 60)
        expect(message.fetch("id")).to eq(outbox_id)
        expect(store.claim_outbox(worker_id: "other", lease_seconds: 60)).to be_nil
        store.ack_outbox(outbox_id, worker_id: "mailer")
        expect(store.outbox_message(outbox_id).fetch("status")).to eq("processed")
      end

      it "implements timers, external event waits, and worker polling" do
        store.migrate!
        workflow = durababble_test_workflow("waits") do
          test_step("wait_for_time") do |ctx|
            Durababble.wait_until(Time.now + 3600, ctx.merge("after_timer" => true))
          end
          test_step("wait_for_event") do |ctx|
            Durababble.wait_event("approval:#{ctx.fetch("request_id")}", ctx.merge("after_event_wait" => true))
          end
          test_step("finish") { |ctx| ctx.merge("finished" => true) }
        end

        worker = Durababble::Worker.new(store:, workflows: { "waits" => workflow }, worker_id: "worker-a")
        workflow_id = store.enqueue_workflow(name: "waits", input: { "request_id" => "r1" })

        expect(worker.tick).to eq(:worked)
        expect(store.workflow(workflow_id).fetch("status")).to eq("waiting")
        expect(store.waits_for(workflow_id).first.fetch("kind")).to eq("timer")

        expect(store.wake_due_timers(now: Time.now)).to eq(0)
        expect(store.wake_due_timers(now: Time.now + 3601)).to eq(1)
        expect(worker.tick).to eq(:worked)
        expect(store.workflow(workflow_id).fetch("status")).to eq("waiting")
        expect(store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }).to eq(%w[completed waiting])
        expect(store.waits_for(workflow_id).last.fetch("event_key")).to eq("approval:r1")

        expect(store.signal_event("approval:other", payload: {})).to eq(0)
        expect(store.signal_event("approval:r1", payload: { "approved" => true })).to eq(1)
        expect(worker.run_until_idle).to eq(1)
        expect(store.workflow(workflow_id).fetch("status")).to eq("completed")
        expect(store.workflow(workflow_id).fetch("result")).to include("finished" => true, "approved" => true)
        expect(store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }).to eq(%w[completed completed completed])
      end

      it "implements the crash matrix explicitly" do
        store.migrate!

        # Crash: after enqueue before work is claimed -> worker later completes it.
        enqueue_crash_id = store.enqueue_workflow(name: "counter", input: { "count" => 1 })
        expect(engine.resume(counter_workflow, workflow_id: enqueue_crash_id).result).to eq({ "count" => 4 })

        # Crash: after lease claim before step start -> expired lease is stolen and completed.
        claim_crash_id = store.enqueue_workflow(name: "counter", input: { "count" => 2 })
        store.claim_runnable_workflow(worker_id: "dead-worker", lease_seconds: 1)
        expect(store.claim_runnable_workflow(worker_id: "live-worker", lease_seconds: 60)).to be_nil
        store.steal_expired_leases!(now: Time.now + 2)
        expect(engine(worker_id: "live-worker").resume(counter_workflow, workflow_id: claim_crash_id).result).to eq({ "count" => 6 })

        # Crash: after step start before completion -> expired running step is retried.
        started_id = store.enqueue_workflow(name: "counter", input: { "count" => 3 })
        expect { engine(crash_after: :step_started).resume(counter_workflow, workflow_id: started_id) }.to raise_error(Durababble::InjectedCrash)
        expect(store.steps_for(started_id).first.fetch("status")).to eq("running")
        store.steal_expired_leases!(now: Time.now + 61)
        expect(engine(worker_id: "recover").resume(counter_workflow, workflow_id: started_id).result).to eq({ "count" => 8 })

        # Crash: after step completion before workflow completion -> completed step is not rerun.
        events = []
        completed_step_id = store.enqueue_workflow(name: "counter", input: { "count" => 4 })
        expect { engine(crash_after: :step_completed).resume(counter_workflow(events:), workflow_id: completed_step_id) }.to raise_error(Durababble::InjectedCrash)
        expect(store.steps_for(completed_step_id).first.fetch("status")).to eq("completed")
        store.steal_expired_leases!(now: Time.now + 61)
        expect(engine(worker_id: "recover").resume(counter_workflow(events:), workflow_id: completed_step_id).result).to eq({ "count" => 10 })
        expect(events.count("increment")).to eq(1)

        # Crash: while waiting -> wait row survives and signal resumes from the waiting step.
        waiting = durababble_test_workflow("waiting") do
          test_step("wait") { |ctx| Durababble.wait_event("event:#{ctx.fetch("id")}", ctx) }
          test_step("done") { |ctx| ctx.merge("done" => true) }
        end
        waiting_id = store.enqueue_workflow(name: "waiting", input: { "id" => "w1" })
        expect { engine(crash_after: :wait_recorded).resume(waiting, workflow_id: waiting_id) }.to raise_error(Durababble::InjectedCrash)
        expect(store.workflow(waiting_id).fetch("status")).to eq("waiting")
        expect(store.signal_event("event:w1", payload: { "signal" => true })).to eq(1)
        expect(engine(worker_id: "recover").resume(waiting, workflow_id: waiting_id).result).to include("done" => true, "signal" => true)

        # Crash: after outbox insert before external delivery -> outbox is still claimable once.
        outbox_workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 5 })
        outbox_id = store.enqueue_outbox(workflow_id: outbox_workflow_id, topic: "notify", payload: { "id" => 1 }, key: "notify:1")
        expect(store.claim_outbox(worker_id: "sender", lease_seconds: 60).fetch("id")).to eq(outbox_id)
      end
    end
  end

  def engine(worker_id: "worker-a", crash_after: nil)
    Durababble::Engine.new(store:, worker_id:, crash_after:)
  end

  def counter_workflow(events: nil)
    durababble_test_workflow("counter") do
      test_step("increment") do |ctx|
        events << "increment" if events
        { "count" => ctx.fetch("count") + 1 }
      end
      test_step("double") do |ctx|
        events << "double" if events
        { "count" => ctx.fetch("count") * 2 }
      end
    end
  end

  def parse_time(value)
    value.is_a?(Time) ? value : Time.parse(value)
  end
end
