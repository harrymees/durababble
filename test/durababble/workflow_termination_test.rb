# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowTerminationTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "terminates a running workflow without delivering cancellation cleanup with #{backend.name}" do
      with_durababble_store(backend, "workflow_termination_running") do |store|
        work_runs = 0
        cleanup_runs = 0
        started = Queue.new
        release = Queue.new
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "terminate-running"

          define_method(:execute) do |input|
            run_work(input)
          rescue Durababble::CancellationError
            cleanup_after_cancel(input)
          end

          define_method(:run_work) do |input|
            work_runs += 1
            started << true
            release.pop
            input.merge("revived" => true)
          end
          step :run_work

          define_method(:cleanup_after_cancel) do |input|
            cleanup_runs += 1
            input.merge("cleaned" => true)
          end
          step :cleanup_after_cancel
        end
        workflow_id = workflow.enqueue({}, store:)
        update_workflow_input(store, workflow_id, { "workflow_id" => workflow_id }, backend)

        owner = Thread.new do
          owner_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
          Durababble::Engine.new(store: owner_store, worker_id: "terminating-owner", lease_seconds: 60).resume(workflow, workflow_id:)
        ensure
          owner_store&.close
        end
        started.pop
        operator_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
        terminated = workflow.handle(workflow_id, store: operator_store).terminate(reason: "operator hard stop")
        release << true
        run = owner.value

        assert_equal("terminated", terminated.status)
        assert_equal("terminated", run.status)
        assert_nil(run.result)
        assert_equal("operator hard stop", run.error)
        assert_equal(1, work_runs)
        assert_equal(0, cleanup_runs)
        assert_equal(["canceled"], store.steps_for(workflow_id).map { |step| step.fetch("status") })
        assert_equal(["canceled"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") })
        assert_equal(["step_scheduled", "step_started", "workflow_terminated"], store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") })
      ensure
        release << true if defined?(release)
        owner&.kill if defined?(owner) && owner&.alive?
        operator_store&.close
      end
    end

    test "terminates a waiting workflow and ignores late timer wakeups with #{backend.name}" do
      with_durababble_store(backend, "workflow_termination_waiting") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "terminate-waiting"

          def execute(input)
            park(input)
          end

          def park(input)
            Durababble.wait_until(Time.now + 3600, input.merge("slept" => true))
          end
          step :park
        end
        workflow_id = workflow.enqueue({ "id" => "wait" }, store:)

        waiting = Durababble::Engine.new(store:, worker_id: "wait-owner").resume(workflow, workflow_id:)
        assert_equal "waiting", waiting.status

        terminated = workflow.handle(workflow_id, store:).terminate(reason: "stop wait")
        recovered = Durababble::Engine.new(store:, worker_id: "recover-after-terminate").resume(workflow, workflow_id:)

        assert_equal "terminated", terminated.status
        assert_equal "terminated", recovered.status
        assert_equal "stop wait", recovered.error
        assert_equal ["canceled"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
        assert_equal ["canceled"], store.steps_for(workflow_id).map { |step| step.fetch("status") }
        assert_equal ["canceled"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
        assert_equal 0, store.wake_due_timers(now: Time.now + 3601)
      end
    end

    test "termination is idempotent and does not overwrite already terminal workflows with #{backend.name}" do
      with_durababble_store(backend, "workflow_termination_terminal") do |store|
        pending_workflow = durababble_test_workflow("terminate-idempotent") do
          test_step("done") { |ctx| ctx }
        end
        pending_id = pending_workflow.enqueue({}, store:)

        first = pending_workflow.handle(pending_id, store:).terminate
        second = pending_workflow.handle(pending_id, store:).terminate(reason: "second")

        assert_equal "terminated", first.status
        assert_equal "workflow terminated", first.error
        assert_equal "terminated", second.status
        assert_equal "workflow terminated", second.error
        assert_equal ["workflow_terminated"], store.workflow_history_for(pending_id).map { |event| event.fetch("kind") }

        completed_workflow = durababble_test_workflow("terminate-completed") do
          test_step("done") { |ctx| ctx.merge("done" => true) }
        end
        completed_id = completed_workflow.enqueue({}, store:)
        completed = Durababble::Engine.new(store:).resume(completed_workflow, workflow_id: completed_id)
        completed_after_terminate = completed_workflow.handle(completed_id, store:).terminate(reason: "too late")

        assert_equal "completed", completed_after_terminate.status
        assert_equal completed.result, completed_after_terminate.result
        refute_includes store.workflow_history_for(completed_id).map { |event| event.fetch("kind") }, "workflow_terminated"

        failed_workflow = durababble_test_workflow("terminate-failed") do
          test_step("boom") { |_ctx| raise "already failed" }
        end
        failed_id = failed_workflow.enqueue({}, store:)
        failed = Durababble::Engine.new(store:).resume(failed_workflow, workflow_id: failed_id)
        failed_after_terminate = failed_workflow.handle(failed_id, store:).terminate(reason: "too late")

        assert_equal "failed", failed.status
        assert_equal "failed", failed_after_terminate.status
        assert_includes failed_after_terminate.error, "already failed"
      end
    end

    test "termination wins over pending cooperative cancellation cleanup with #{backend.name}" do
      with_durababble_store(backend, "workflow_termination_canceling") do |store|
        cleanup_runs = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "terminate-canceling"

          def execute(input)
            run_work(input)
          rescue Durababble::CancellationError => e
            cleanup_after_cancel(input.merge("reason" => e.reason))
          end

          def run_work(input)
            input
          end
          step :run_work

          define_method(:cleanup_after_cancel) do |input|
            cleanup_runs += 1
            input.merge("cleaned" => true)
          end
          step :cleanup_after_cancel
        end
        workflow_id = workflow.enqueue({}, store:)

        canceling = workflow.handle(workflow_id, store:).cancel(reason: "cooperate")
        terminated = workflow.handle(workflow_id, store:).terminate(reason: "hard stop after cancel")
        recovered = Durababble::Engine.new(store:, worker_id: "recover-canceling").resume(workflow, workflow_id:)

        assert_equal "canceling", canceling.status
        assert_equal "terminated", terminated.status
        assert_equal "terminated", recovered.status
        assert_equal 0, cleanup_runs
        assert_equal "cooperate", store.workflow_cancellation(workflow_id).fetch("reason")
      end
    end

    test "dead letters queued workflow messages and rejects new commands after termination with #{backend.name}" do
      with_durababble_store(backend, "workflow_termination_commands") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "terminate-command-target"

          def execute(input)
            input
          end

          expose_command def approve(reason:)
            { "approved" => reason }
          end
        end
        workflow_id = workflow.enqueue({ "id" => "cmd" }, store:)
        payload = { "method" => "approve", "args" => [], "kwargs" => { "reason" => "operator" } }
        command_id = store.enqueue_workflow_command(
          workflow_id:,
          workflow_name: workflow.workflow_name,
          method_name: "approve",
          payload:,
          idempotency_key: "approve-once",
        )
        signal_id = store.enqueue_inbox_message(
          target_kind: "workflow",
          target_type: workflow.workflow_name,
          target_id: workflow_id,
          message_kind: "workflow_signal",
          payload: { "signal" => "poke" },
          idempotency_key: "signal-once",
        )
        refute_nil store.target_activation(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id)

        workflow.handle(workflow_id, store:).terminate(reason: "command hard stop")

        assert_hash_includes store.inbox_message(command_id), "status" => "dead_lettered", "error" => "command hard stop"
        assert_hash_includes store.inbox_message(signal_id), "status" => "dead_lettered", "error" => "command hard stop"
        assert_nil store.target_activation(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id)
        assert_raises_matching(Durababble::Error, /terminal/) do
          store.enqueue_workflow_command(workflow_id:, workflow_name: workflow.workflow_name, method_name: "approve", payload:)
        end
      end
    end

    test "dead letters late workflow command completion after terminal state with #{backend.name}" do
      with_durababble_store(backend, "workflow_termination_command_completion_fence") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "terminate-command-completion-fence"

          def execute(input)
            input
          end

          expose_command def approve
            { "approved" => true }
          end
        end
        workflow_id = workflow.enqueue({ "id" => "cmd" }, store:)
        payload = { "method" => "approve", "args" => [], "kwargs" => {} }
        command_id = store.enqueue_workflow_command(
          workflow_id:,
          workflow_name: workflow.workflow_name,
          method_name: "approve",
          payload:,
        )
        claimed = store.claim_inbox_messages(
          target_kind: "workflow",
          target_type: workflow.workflow_name,
          target_id: workflow_id,
          worker_id: "command-worker",
        )

        assert_equal [command_id], claimed.map { |command| command.fetch("id") }

        store.complete_workflow(workflow_id, result: { "done" => true })
        store.complete_workflow_command(
          message_id: command_id,
          workflow_id:,
          result: { "approved" => true },
          worker_id: "command-worker",
        )

        assert_hash_includes(
          store.inbox_message(command_id),
          "status" => "dead_lettered",
          "error" => "workflow #{workflow_id} is completed",
        )
      end
    end

    test "later terminal writes cannot revive a terminated workflow with #{backend.name}" do
      with_durababble_store(backend, "workflow_termination_fences") do |store|
        workflow = durababble_test_workflow("terminate-fence") do
          test_step("done") { |ctx| ctx.merge("done" => true) }
        end
        workflow_id = workflow.enqueue({}, store:)

        workflow.handle(workflow_id, store:).terminate(reason: "fenced")
        store.complete_workflow(workflow_id, result: { "done" => true })
        store.fail_workflow(workflow_id, error: "late failure")
        store.cancel_workflow(workflow_id, reason: "late cancel")

        row = store.workflow(workflow_id)
        assert_hash_includes row, "status" => "terminated", "result" => nil, "error" => "fenced"
      end
    end
  end

  private

  def update_workflow_input(store, workflow_id, input, backend)
    payload = store.send(:dump_serialized, input)
    if backend.mysql?
      store.send(:execute_params, "UPDATE #{store.send(:table, "workflows")} SET input = ? WHERE id = ?", [payload, workflow_id])
    else
      store.send(:execute_params, "UPDATE #{store.send(:table, "workflows")} SET input = $2::bytea WHERE id = $1", [workflow_id, payload])
    end
  end
end
