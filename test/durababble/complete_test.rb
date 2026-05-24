# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleCompleteTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "implements the guarantee matrix explicitly with #{backend.name}" do
      with_durababble_store(backend, "complete_test") do |store|
        workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 2 })
        claim = store.claim_runnable_workflow(worker_id: "worker-a", lease_seconds: 60)
        assert_equal workflow_id, claim.fetch("id")
        assert_nil store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 60)

        before = parse_time(store.workflow(workflow_id).fetch("locked_until"))
        store.heartbeat(workflow_id:, worker_id: "worker-a", lease_seconds: 120)
        after = parse_time(store.workflow(workflow_id).fetch("locked_until"))
        assert_operator after, :>, before
        assert_equal 1, store.steal_expired_leases!(now: Time.now + 121)
        stolen = store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: 60)
        assert_equal workflow_id, stolen.fetch("id")

        events = []
        run = engine(worker_id: "worker-b").resume(counter_workflow(events:), workflow_id:)
        assert_equal "completed", run.status
        assert_equal({ "count" => 6 }, run.result)
        assert_equal ["increment", "double"], events
        again = engine(worker_id: "worker-b").resume(counter_workflow(events:), workflow_id:)
        assert_equal({ "count" => 6 }, again.result)
        assert_equal ["increment", "double"], events
        assert_equal ["completed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }

        side_effects = 0
        first = store.with_fence(workflow_id:, key: "charge:1") do
          side_effects += 1
          { "charge_id" => "ch_1" }
        end
        second = store.with_fence(workflow_id:, key: "charge:1") do
          side_effects += 1
          { "charge_id" => "ch_2" }
        end
        assert_equal({ "charge_id" => "ch_1" }, first)
        assert_equal({ "charge_id" => "ch_1" }, second)
        assert_equal 1, side_effects

        outbox_id = store.enqueue_outbox(
          workflow_id:,
          topic: "email",
          payload: { "to" => "recipient@example.com" },
          key: "email:1",
        )
        duplicate_id = store.enqueue_outbox(
          workflow_id:,
          topic: "email",
          payload: { "to" => "duplicate" },
          key: "email:1",
        )
        assert_equal outbox_id, duplicate_id
        message = store.claim_outbox(worker_id: "mailer", lease_seconds: 60)
        assert_equal outbox_id, message.fetch("id")
        assert_nil store.claim_outbox(worker_id: "other", lease_seconds: 60)
        store.ack_outbox(outbox_id, worker_id: "mailer")
        assert_equal "processed", store.outbox_message(outbox_id).fetch("status")
      end
    end

    test "implements timers, external event waits, and worker polling with #{backend.name}" do
      with_durababble_store(backend, "complete_test") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "waits"

          def execute(input)
            timed = wait_until(Time.now + 3600, input.merge("after_timer" => true))
            evented = wait_event("approval:#{timed.fetch("request_id")}", timed.merge("after_event_wait" => true))
            finish(evented)
          end

          step def finish(ctx)
            ctx.merge("finished" => true)
          end
        end

        worker = Durababble::Worker.new(store:, workflows: { "waits" => workflow }, worker_id: "worker-a")
        workflow_id = store.enqueue_workflow(name: "waits", input: { "request_id" => "r1" })

        assert_equal :worked, worker.tick
        assert_equal "waiting", store.workflow(workflow_id).fetch("status")
        assert_equal "timer", store.waits_for(workflow_id).first.fetch("kind")

        assert_equal 0, store.wake_due_timers(now: Time.now)
        assert_equal 1, store.wake_due_timers(now: Time.now + 3601)
        assert_equal :worked, worker.tick
        assert_equal "waiting", store.workflow(workflow_id).fetch("status")
        assert_empty store.step_attempts_for(workflow_id)
        assert_equal "approval:r1", store.waits_for(workflow_id).last.fetch("event_key")

        assert_equal 0, store.signal_event("approval:other", payload: {})
        assert_equal 1, store.signal_event("approval:r1", payload: { "approved" => true })
        assert_equal 1, worker.run_until_idle
        assert_equal "completed", store.workflow(workflow_id).fetch("status")
        assert_hash_includes store.workflow(workflow_id).fetch("result"), "finished" => true, "approved" => true
        assert_equal(
          ["completed"],
          store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") },
        )
      end
    end

    test "implements the crash matrix explicitly with #{backend.name}" do
      with_durababble_store(backend, "complete_test") do |store|
        enqueue_crash_id = store.enqueue_workflow(name: "counter", input: { "count" => 1 })
        assert_equal({ "count" => 4 }, engine.resume(counter_workflow, workflow_id: enqueue_crash_id).result)

        claim_crash_id = store.enqueue_workflow(name: "counter", input: { "count" => 2 })
        store.claim_runnable_workflow(worker_id: "dead-worker", lease_seconds: 1)
        assert_nil store.claim_runnable_workflow(worker_id: "live-worker", lease_seconds: 60)
        store.steal_expired_leases!(now: Time.now + 2)
        assert_equal(
          { "count" => 6 },
          engine(worker_id: "live-worker").resume(counter_workflow, workflow_id: claim_crash_id).result,
        )

        started_id = store.enqueue_workflow(name: "counter", input: { "count" => 3 })
        assert_raises(Durababble::InjectedCrash) do
          engine(crash_after: :step_started).resume(counter_workflow, workflow_id: started_id)
        end
        assert_equal "running", store.steps_for(started_id).first.fetch("status")
        store.steal_expired_leases!(now: Time.now + 61)
        assert_equal({ "count" => 8 }, engine(worker_id: "recover").resume(counter_workflow, workflow_id: started_id).result)

        events = []
        completed_step_id = store.enqueue_workflow(name: "counter", input: { "count" => 4 })
        assert_raises(Durababble::InjectedCrash) do
          engine(crash_after: :step_completed).resume(counter_workflow(events:), workflow_id: completed_step_id)
        end
        assert_equal "completed", store.steps_for(completed_step_id).first.fetch("status")
        store.steal_expired_leases!(now: Time.now + 61)
        assert_equal(
          { "count" => 10 },
          engine(worker_id: "recover").resume(counter_workflow(events:), workflow_id: completed_step_id).result,
        )
        assert_equal 1, events.count("increment")

        waiting = Class.new(Durababble::Workflow) do
          workflow_name "waiting"

          def execute(input)
            done(wait_event("event:#{input.fetch("id")}", input))
          end

          step def done(ctx)
            ctx.merge("done" => true)
          end
        end
        waiting_id = store.enqueue_workflow(name: "waiting", input: { "id" => "w1" })
        assert_raises(Durababble::InjectedCrash) do
          engine(crash_after: :wait_recorded).resume(waiting, workflow_id: waiting_id)
        end
        assert_equal "waiting", store.workflow(waiting_id).fetch("status")
        assert_equal 1, store.signal_event("event:w1", payload: { "signal" => true })
        result = engine(worker_id: "recover").resume(waiting, workflow_id: waiting_id).result
        assert_hash_includes result, "done" => true, "signal" => true

        outbox_workflow_id = store.enqueue_workflow(name: "counter", input: { "count" => 5 })
        outbox_id = store.enqueue_outbox(
          workflow_id: outbox_workflow_id,
          topic: "notify",
          payload: { "id" => 1 },
          key: "notify:1",
        )
        assert_equal outbox_id, store.claim_outbox(worker_id: "sender", lease_seconds: 60).fetch("id")
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
