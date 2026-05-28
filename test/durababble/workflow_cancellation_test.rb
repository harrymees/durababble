# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowCancellationTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "cancels before start and runs cleanup durably with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        work_runs = 0
        cleanup_runs = 0
        workflow = cancelable_workflow(
          "cancel-before-start",
          work: ->(input, _heartbeat) {
            work_runs += 1
            input.merge("worked" => true)
          },
          cleanup: ->(input) {
            cleanup_runs += 1
            { "cleaned" => true, "reason" => input.fetch("reason") }
          },
        )
        handle = workflow.start({ "id" => "prestart" }, store:)
        workflow_id = handle.workflow_id

        cancel_run = handle.cancel(reason: "user requested")
        assert_equal "canceling", cancel_run.status
        run = Durababble::Engine.new(store:).resume(workflow, workflow_id:)

        assert_equal "canceled", run.status
        assert_equal({ "cleaned" => true, "reason" => "user requested" }, run.result)
        assert_equal 0, work_runs
        assert_equal 1, cleanup_runs
      end
    end

    test "cancels while waiting and ignores later timer wakeups with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        cleanup_runs = 0
        workflow = cancelable_workflow(
          "cancel-waiting",
          work: ->(input, _heartbeat) {
            Durababble.wait_until(Time.now + 3600, input)
          },
          cleanup: ->(input) {
            cleanup_runs += 1
            { "cleaned" => input.fetch("reason") }
          },
        )
        workflow_id = workflow.enqueue({ "id" => "wait" }, store:)

        waiting = Durababble::Engine.new(store:).resume(workflow, workflow_id:)
        assert_equal "waiting", waiting.status

        workflow.handle(workflow_id, store:).cancel(reason: "no longer needed")
        assert_equal "canceled", store.wait_snapshots_for(workflow_id).first.fetch("status")
        run = Durababble::Engine.new(store:).resume(workflow, workflow_id:)
        woken = store.wake_due_timers(now: Time.now + 3601)

        assert_equal "canceled", run.status
        assert_equal 0, woken
        assert_equal 1, cleanup_runs
      end
    end

    test "runs ensure cleanup when cancellation unwinds after a step boundary with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        cleanup_runs = 0
        finalize_runs = 0
        workflow = nil
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "cancel-ensure-cleanup"

          define_method(:execute) do |input|
            workspace = nil
            begin
              workspace = create_workspace(input)
              copy_rows(workspace)
              finalize_export(workspace)
            ensure
              delete_workspace(workspace) if workspace
            end
          end

          define_method(:create_workspace) do |input|
            { "workflow_id" => input.fetch("workflow_id"), "workspace_id" => "tmp-#{input.fetch("id")}" }
          end
          step :create_workspace

          define_method(:copy_rows) do |workspace|
            workflow.handle(workspace.fetch("workflow_id"), store:).cancel(reason: "customer canceled")
            workspace.merge("copied" => true)
          end
          step :copy_rows

          define_method(:finalize_export) do |workspace|
            finalize_runs += 1
            workspace.merge("finalized" => true)
          end
          step :finalize_export

          define_method(:delete_workspace) do |workspace|
            cleanup_runs += 1
            { "deleted" => workspace.fetch("workspace_id") }
          end
          step :delete_workspace
        end
        workflow_id = workflow.enqueue({ "id" => "ensure" }, store:)
        update_workflow_input(store, workflow_id, { "id" => "ensure", "workflow_id" => workflow_id }, backend)

        run = Durababble::Engine.new(store:).resume(workflow, workflow_id:)
        completed_steps = store.steps_for(workflow_id).select { |step| step.fetch("status") == "completed" }.map { |step| step.fetch("name") }

        assert_equal "canceled", run.status
        assert_equal 1, cleanup_runs
        assert_equal 0, finalize_runs
        assert_equal ["create_workspace", "copy_rows", "delete_workspace"], completed_steps
      end
    end

    test "cancels during retry backoff without waiting for the retry due time with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        work_runs = 0
        cleanup_runs = 0
        workflow = cancelable_workflow(
          "cancel-backoff",
          retry_policy: { initial_interval: 60, maximum_attempts: 5 },
          work: ->(_input, _heartbeat) {
            work_runs += 1
            raise "temporary"
          },
          cleanup: ->(input) {
            cleanup_runs += 1
            { "cleaned" => input.fetch("reason") }
          },
        )
        workflow_id = workflow.enqueue({}, store:)

        scheduled = Durababble::Engine.new(store:).resume(workflow, workflow_id:)
        assert_equal "pending", scheduled.status
        refute_nil store.workflow(workflow_id).fetch("next_run_at")

        workflow.handle(workflow_id, store:).cancel(reason: "stop retrying")
        assert_nil store.workflow(workflow_id).fetch("next_run_at")
        run = Durababble::Engine.new(store:, worker_id: "cancel-worker").resume(workflow, workflow_id:)

        assert_equal "canceled", run.status
        assert_equal 1, work_runs
        assert_equal 1, cleanup_runs
      end
    end

    test "cancels a running heartbeating step at the heartbeat yield point with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        work_runs = 0
        cleanup_runs = 0
        workflow = nil
        workflow = cancelable_workflow(
          "cancel-heartbeat",
          work: ->(input, heartbeat) {
            work_runs += 1
            workflow.handle(input.fetch("workflow_id"), store:).cancel(reason: "heartbeat stop")
            heartbeat.record({ "offset" => 10 })
            input
          },
          cleanup: ->(input) {
            cleanup_runs += 1
            { "cleaned" => input.fetch("reason") }
          },
        )
        workflow_id = workflow.enqueue({}, store:)
        update_workflow_input(store, workflow_id, { "workflow_id" => workflow_id }, backend)

        run = Durababble::Engine.new(store:, lease_seconds: 60).resume(workflow, workflow_id:)

        assert_equal "canceled", run.status
        assert_equal 1, work_runs
        assert_equal 1, cleanup_runs
        assert_equal ["canceled", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "keeps the workflow lease through long-running cancellation cleanup with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        observations = []
        workflow = nil
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "cancel-cleanup-lease"

          define_method(:execute) do |input|
            run_work(input)
          rescue Durababble::CancellationError => e
            cleanup_after_cancel(input.merge("reason" => e.reason))
          end

          define_method(:run_work) do |input|
            workflow.handle(input.fetch("workflow_id"), store:).cancel(reason: "keep cleaning")
            row = store.workflow(input.fetch("workflow_id"))
            observations << {
              phase: "request",
              status: row.fetch("status"),
              locked_by: row.fetch("locked_by"),
              locked_until: row.fetch("locked_until"),
            }
            step_context.heartbeat.record({ "phase" => "work" })
          end
          step :run_work

          define_method(:cleanup_after_cancel) do |input|
            before = store.workflow(input.fetch("workflow_id"))
            sleep 0.02
            step_context.heartbeat.record({ "phase" => "cleanup" })
            after = store.workflow(input.fetch("workflow_id"))
            observations << {
              phase: "cleanup",
              before_status: before.fetch("status"),
              before_locked_by: before.fetch("locked_by"),
              before_locked_until: before.fetch("locked_until"),
              after_status: after.fetch("status"),
              after_locked_by: after.fetch("locked_by"),
              after_locked_until: after.fetch("locked_until"),
            }
            { "cleaned" => input.fetch("reason") }
          end
          step :cleanup_after_cancel
        end
        workflow_id = workflow.enqueue({}, store:)
        update_workflow_input(store, workflow_id, { "workflow_id" => workflow_id }, backend)

        run = Durababble::Engine.new(store:, worker_id: "cleanup-owner", lease_seconds: 5).resume(workflow, workflow_id:)

        request = observations.fetch(0)
        cleanup = observations.fetch(1)
        assert_equal "canceled", run.status
        assert_equal "running", request.fetch(:status)
        assert_equal "cleanup-owner", request.fetch(:locked_by)
        assert_equal "running", cleanup.fetch(:before_status)
        assert_equal "cleanup-owner", cleanup.fetch(:before_locked_by)
        assert_equal "running", cleanup.fetch(:after_status)
        assert_equal "cleanup-owner", cleanup.fetch(:after_locked_by)
        assert_operator timestamp_value(cleanup.fetch(:after_locked_until)), :>, timestamp_value(cleanup.fetch(:before_locked_until))
        assert_nil store.workflow(workflow_id).fetch("locked_by")
        assert_equal ["canceled", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "does not repeat completed cleanup after crash and recovery with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        cleanup_runs = 0
        workflow = cancelable_workflow(
          "cancel-cleanup-crash",
          work: ->(input, _heartbeat) {
            input
          },
          cleanup: ->(input) {
            cleanup_runs += 1
            { "cleaned" => input.fetch("reason") }
          },
        )
        workflow_id = workflow.enqueue({}, store:)
        workflow.handle(workflow_id, store:).cancel(reason: "recover cleanup")

        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(store:, worker_id: "crashy", lease_seconds: 1, crash_after: :step_completed).resume(workflow, workflow_id:)
        end
        store.steal_expired_leases!(now: Time.now + 2)
        run = Durababble::Engine.new(store:, worker_id: "recover").resume(workflow, workflow_id:)

        assert_equal "canceled", run.status
        assert_equal 1, cleanup_runs
        assert_equal ["completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "resumes cancellation cleanup after a crash before cleanup body runs with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        cleanup_runs = 0
        workflow = cancelable_workflow(
          "cancel-cleanup-start-crash",
          work: ->(input, _heartbeat) { input },
          cleanup: ->(input) {
            cleanup_runs += 1
            { "cleaned" => input.fetch("reason") }
          },
        )
        workflow_id = workflow.enqueue({}, store:)
        workflow.handle(workflow_id, store:).cancel(reason: "recover started cleanup")

        assert_raises(Durababble::InjectedCrash) do
          Durababble::Engine.new(store:, worker_id: "crashy", lease_seconds: 1, crash_after: :step_started).resume(workflow, workflow_id:)
        end
        assert_equal "running", store.workflow(workflow_id).fetch("status")
        assert_equal "crashy", store.workflow(workflow_id).fetch("locked_by")

        assert_equal 1, store.steal_expired_leases!(now: Time.now + 2)
        assert_equal "canceling", store.workflow(workflow_id).fetch("status")
        run = Durababble::Engine.new(store:, worker_id: "recover").resume(workflow, workflow_id:)

        assert_equal "canceled", run.status
        assert_equal 1, cleanup_runs
        assert_equal ["failed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "marks cleanup failure as workflow failure with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        workflow = cancelable_workflow(
          "cancel-cleanup-fails",
          work: ->(input, _heartbeat) {
            input
          },
          cleanup: ->(_input) {
            raise "cleanup failed"
          },
        )
        workflow_id = workflow.enqueue({}, store:)
        workflow.handle(workflow_id, store:).cancel(reason: "fail cleanup")

        run = Durababble::Engine.new(store:).resume(workflow, workflow_id:)

        assert_equal "failed", run.status
        assert_includes run.error, "RuntimeError: cleanup failed"
      end
    end

    test "retries cleanup steps durably before canceling with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        cleanup_runs = 0
        workflow = cancelable_workflow(
          "cancel-cleanup-retry",
          work: ->(input, _heartbeat) { input },
          cleanup_retry_policy: { initial_interval: 10, maximum_attempts: 2 },
          cleanup: ->(input) {
            cleanup_runs += 1
            raise "cleanup transient" if cleanup_runs == 1

            { "cleaned" => input.fetch("reason"), "attempts" => cleanup_runs }
          },
        )
        workflow_id = workflow.enqueue({}, store:)
        workflow.handle(workflow_id, store:).cancel(reason: "retry cleanup")

        scheduled = Durababble::Engine.new(store:, worker_id: "first").resume(workflow, workflow_id:)
        assert_equal "canceling", scheduled.status
        retry_due_at = store.workflow(workflow_id).fetch("next_run_at")
        refute_nil retry_due_at

        workflow.handle(workflow_id, store:).cancel(reason: "duplicate retry cleanup")
        assert_equal retry_due_at, store.workflow(workflow_id).fetch("next_run_at")

        store.make_workflow_due!(workflow_id, now: Time.now + 11)
        run = Durababble::Engine.new(store:, worker_id: "second").resume(workflow, workflow_id:)

        assert_equal "canceled", run.status
        assert_equal({ "cleaned" => "retry cleanup", "attempts" => 2 }, run.result)
        assert_equal ["failed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
      end
    end

    test "duplicate cancellation requests preserve the first durable reason with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        workflow = cancelable_workflow("cancel-duplicate", work: ->(input, _heartbeat) { input }, cleanup: ->(input) { input })
        workflow_id = workflow.enqueue({}, store:)

        first = workflow.handle(workflow_id, store:).cancel(reason: "first")
        second = workflow.handle(workflow_id, store:).cancel(reason: "second")

        assert_equal "canceling", first.status
        assert_equal "canceling", second.status
        assert_equal "first", store.workflow_cancellation(workflow_id).fetch("reason")
      end
    end

    test "stores cancellation metadata on the workflow row with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_storage_test") do |store|
        workflow = cancelable_workflow("cancel-storage", work: ->(input, _heartbeat) { input }, cleanup: ->(input) { input })
        workflow_id = workflow.enqueue({}, store:)

        workflow.handle(workflow_id, store:).cancel(reason: "stored on workflow")
        store.mark_workflow_cancellation_delivered(workflow_id:)

        row = store.workflow(workflow_id)
        cancellation = store.workflow_cancellation(workflow_id)

        assert_equal "stored on workflow", row.fetch("cancel_reason")
        refute_nil row.fetch("cancel_requested_at")
        refute_nil row.fetch("cancel_delivered_at")
        assert_equal workflow_id, cancellation.fetch("workflow_id")
        assert_equal row.fetch("cancel_reason"), cancellation.fetch("reason")
        refute_workflow_cancellations_table(store, backend)
      end
    end

    test "terminal workflow cancellation is a no-op unless already canceled with #{backend.name}" do
      with_durababble_store(backend, "workflow_cancellation_test") do |store|
        completed_workflow = durababble_test_workflow("terminal-completed") do
          test_step("done") { |ctx| ctx.merge("done" => true) }
        end
        completed_id = completed_workflow.enqueue({}, store:)
        Durababble::Engine.new(store:).resume(completed_workflow, workflow_id: completed_id)

        completed_cancel = completed_workflow.handle(completed_id, store:).cancel(reason: "too late")
        assert_equal "completed", completed_cancel.status
        assert_nil store.workflow_cancellation(completed_id)

        canceled_workflow = cancelable_workflow("terminal-canceled", work: ->(input, _heartbeat) { input }, cleanup: ->(input) { input })
        canceled_id = canceled_workflow.enqueue({}, store:)
        canceled_workflow.handle(canceled_id, store:).cancel(reason: "first")
        Durababble::Engine.new(store:).resume(canceled_workflow, workflow_id: canceled_id)

        canceled_again = canceled_workflow.handle(canceled_id, store:).cancel(reason: "second")
        assert_equal "canceled", canceled_again.status
        assert_equal "first", store.workflow_cancellation(canceled_id).fetch("reason")
      end
    end
  end

  private

  def refute_workflow_cancellations_table(store, backend)
    exists = if backend.mysql?
      store.send(:execute_params, <<~SQL, [store.send(:raw_table_name, "workflow_cancellations")]).first
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = ?
      SQL
    else
      store.send(:execute_params, <<~SQL, [schema, "workflow_cancellations"]).first
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = $1 AND table_name = $2
      SQL
    end

    assert_nil(exists)
  end

  def cancelable_workflow(name, work:, cleanup:, retry_policy: nil, cleanup_retry_policy: nil)
    Class.new(Durababble::Workflow) do
      workflow_name name

      define_method(:execute) do |input|
        run_work(input)
      rescue Durababble::CancellationError => e
        cleanup_after_cancel(input.merge("reason" => e.reason))
      end

      define_method(:run_work) do |input|
        work.call(input, step_context.heartbeat)
      end
      step :run_work, retry: retry_policy

      define_method(:cleanup_after_cancel) do |input|
        cleanup.call(input)
      end
      step :cleanup_after_cancel, retry: cleanup_retry_policy
    end
  end

  def update_workflow_input(store, workflow_id, input, backend)
    payload = store.send(:dump_serialized, input)
    if backend.mysql?
      store.send(:execute_params, "UPDATE #{store.send(:table, "workflows")} SET input = ? WHERE id = ?", [payload, workflow_id])
    else
      store.send(:execute_params, "UPDATE #{store.send(:table, "workflows")} SET input = $2::bytea WHERE id = $1", [workflow_id, payload])
    end
  end

  def timestamp_value(value)
    return value if value.is_a?(Time)

    Time.parse(value.to_s)
  end
end
