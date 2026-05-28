# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkflowTest < DurababbleTestCase
  class ApiTestChargeFailed < StandardError; end

  class ApiTestOrderWorkflow < Durababble::Workflow
    expose def status
      "queryable"
    end

    expose_command def cancel(reason:)
      reason
    end

    def execute(input)
      charged = charge(input)
      finish(charged)
    end

    step retry: { maximum_attempts: 3, initial_interval: 1 }
    def charge(input)
      raise ApiTestChargeFailed, "declined" if input.fetch("decline", false)

      input.merge("idempotency_key" => step_context.idempotency_key)
    end

    step def finish(input)
      input.merge("finished" => true)
    end
  end

  class ApiTestApprovalWorkflow < Durababble::Workflow
    workflow_name "api-test-approval-workflow"

    def execute(input)
      wait_for_approval(input)
    end

    step def wait_for_approval(input)
      Durababble.wait_until(Time.now + 3600, input.merge("waiting_for" => "approve"))
    end

    expose_command def approve(reason:)
      { "approved_by" => reason }
    end

    expose_command def reject(reason:)
      raise ApiTestChargeFailed, reason
    end
  end

  class ApiTestQueryableWorkflow < Durababble::Workflow
    class << self
      attr_accessor :step_started, :release_step, :current_instance_id, :query_entered, :query_release
    end

    workflow_name "api-test-queryable-workflow"

    expose def snapshot(prefix:, metadata: {})
      {
        "prefix" => prefix,
        "state" => @state,
        "metadata" => metadata,
        "instance_id" => object_id,
      }
    end

    expose def query_step_forbidden
      hold({})
    end

    expose def query_wait_forbidden
      wait_until(Time.now + 1, {})
    end

    expose def blocking_snapshot
      self.class.query_entered << true
      self.class.query_release.pop
      { "state" => @state }
    end

    def execute(input)
      @state = input.fetch("state")
      self.class.current_instance_id = object_id
      hold(input)
    end

    step def hold(input)
      self.class.step_started << true
      self.class.release_step.pop
      input
    end
  end

  class WorkflowQueryFakeClient
    attr_reader :requests

    def initialize(&handler)
      @handler = handler
      @requests = []
    end

    def request(command, payload)
      @requests << [command, payload]
      @handler.call(command, payload)
    end
  end

  class TerminalRaceCommandStore
    attr_reader :enqueued

    def initialize
      @enqueued = false
    end

    def migrate!; end

    def workflow(workflow_id)
      { "id" => workflow_id, "status" => "running", "next_run_at" => nil }
    end

    def enqueue_workflow_command(workflow_id:, workflow_name:, method_name:, payload:, idempotency_key:, max_attempts: 1)
      raise Durababble::Error, "workflow #{workflow_id} is terminal"
    end

    def deliver_target_message(**)
      @enqueued = true
    end

    def enqueue_inbox_message(**)
      @enqueued = true
      "raced-command"
    end

    def wait_for_inbox_message(message_id, poll_interval: 0.05, timeout: 10)
      raise Durababble::CommandTimeout, "timed out waiting for inbox message #{message_id}"
    end
  end

  class MaterializedWorkflowMailboxPoolStore
    attr_reader :commands, :deliveries, :waits

    def initialize(worker_pool:)
      @worker_pool = worker_pool
      @commands = []
      @deliveries = []
      @waits = []
    end

    def enqueue_workflow_command(**kwargs)
      @commands << kwargs
      "msg-#{@commands.length}"
    end

    def inbox_message(message_id)
      { "id" => message_id, "worker_pool" => @worker_pool }
    end

    def deliver_target_message(**kwargs)
      @deliveries << kwargs
      true
    end

    def wait_for_inbox_message(message_id, timeout: 10, **)
      @waits << { message_id:, timeout: }
      "waited:#{message_id}"
    end
  end

  class WorkerPoolEnqueueStore
    attr_reader :enqueued

    def initialize
      @enqueued = []
    end

    def enqueue_workflow(name:, input:, id: nil, worker_pool: "default")
      @enqueued << { name:, input:, id:, worker_pool: }
      id || "wf-#{@enqueued.length}"
    end
  end

  test "registers class-oriented workflow steps and public exposed methods" do
    assert_equal "api_test_order_workflow", ApiTestOrderWorkflow.workflow_name
    assert_equal [:charge, :finish], ApiTestOrderWorkflow.step_order
    assert_equal 3, ApiTestOrderWorkflow.step_definition(:charge).retry_policy.maximum_attempts
    assert_hash_includes ApiTestOrderWorkflow.exposed_queries, status: true
    assert_includes ApiTestOrderWorkflow.exposed_commands.keys, :cancel
  end

  test "does not expose the removed Workflow.define DSL" do
    assert_not_respond_to Durababble::Workflow, :define
  end

  test "derives fallback names for anonymous workflows" do
    anonymous_workflow = Class.new(Durababble::Workflow)

    assert_match(/\A\d+\z/, anonymous_workflow.workflow_name)
  end

  test "ignores unknown pending durable macros while preserving method order identity" do
    odd_workflow = Class.new(Durababble::Workflow)
    odd_workflow.instance_variable_set(:@pending_durable_macro, [:unknown, {}])
    odd_workflow.class_eval { def ignored_macro = true }
    odd_workflow.class_eval do
      def repeat(input) = input
      step :repeat
      step :repeat
    end

    assert_equal [:repeat], odd_workflow.step_order
  end

  test "rejects idempotency keys on exposed workflow queries before routing" do
    assert_raises_matching(ArgumentError, /does not accept idempotency_key/) do
      ApiTestQueryableWorkflow.handle("wf-1", store: Object.new).snapshot(prefix: "wf", idempotency_key: "query-key")
    end
  end

  test "uses the store's atomic workflow command enqueue path" do
    store = TerminalRaceCommandStore.new
    error = assert_raises(Durababble::Error) do
      ApiTestApprovalWorkflow.handle("wf-race", store:).approve(reason: "late")
    end

    assert_match(/terminal/, error.message)
    refute store.enqueued
  end

  test "workflow commands deliver through the persisted mailbox worker pool" do
    store = MaterializedWorkflowMailboxPoolStore.new(worker_pool: "materialized")

    result = ApiTestApprovalWorkflow.handle("wf-1", store:, worker_pool: "requested").approve(reason: "ok")

    assert_equal "waited:msg-1", result
    assert_equal(
      [
        {
          workflow_id: "wf-1",
          workflow_name: "api-test-approval-workflow",
          method_name: "approve",
          payload: { "method" => "approve", "args" => [], "kwargs" => { reason: "ok" } },
          idempotency_key: nil,
          max_attempts: 1,
        },
      ],
      store.commands,
    )
    assert_equal(
      [
        { message_id: "msg-1", timeout: 10 },
      ],
      store.waits,
    )
    assert_equal(
      [
        {
          worker_pool: "materialized",
          target_kind: "workflow",
          target_type: "api-test-approval-workflow",
          target_id: "wf-1",
        },
      ],
      store.deliveries,
    )
  end

  test "worker pool class helpers enqueue through the store without per-pool engines" do
    store = WorkerPoolEnqueueStore.new

    Durababble::Engine.stub(:new, ->(*) { raise "unexpected engine construction" }) do
      assert_equal "wf-pool-one", ApiTestOrderWorkflow.enqueue({ "request_id" => "one" }, id: "wf-pool-one", store:, worker_pool: "critical")
      handle = ApiTestOrderWorkflow.start({ "request_id" => "two" }, store:, worker_pool: "bulk")

      assert_match(/\A[0-9a-f-]{36}\z/, handle.workflow_id)
    end

    generated_id = store.enqueued.fetch(1).fetch(:id)
    assert_equal(
      [
        { name: "api_test_order_workflow", input: { "request_id" => "one" }, id: "wf-pool-one", worker_pool: "critical" },
        { name: "api_test_order_workflow", input: { "request_id" => "two" }, id: generated_id, worker_pool: "bulk" },
      ],
      store.enqueued,
    )
  end

  test "workflow command replay validates completed command result shape" do
    execution = replay_execution_for(ApiTestApprovalWorkflow)
    event = workflow_command_history_event(
      "workflow_command_completed",
      method_name: "approve",
      kwargs: { reason: "operator" },
      result: { "approved_by" => "operator" },
    )

    assert_nil execution.send(:replay_workflow_command_event, event)

    event["payload"]["result"] = { "approved_by" => "someone-else" }
    error = assert_raises(Durababble::ReplayDivergenceError) do
      execution.send(:replay_workflow_command_event, event)
    end
    assert_match(/different result/, error.message)
  end

  test "workflow command replay requires a recorded result for completed commands" do
    execution = replay_execution_for(ApiTestApprovalWorkflow)
    event = workflow_command_history_event(
      "workflow_command_completed",
      method_name: "approve",
      kwargs: { reason: "operator" },
    )
    refute(event.fetch("payload").key?("result"), "fixture should omit the result so the assertion is exercised")

    error = assert_raises(Durababble::ReplayDivergenceError) do
      execution.send(:replay_workflow_command_event, event)
    end
    assert_match(/no recorded result/, error.message)
  end

  test "workflow command replay accepts a matching recorded nil result" do
    execution = replay_execution_for(ApiTestApprovalWorkflow)
    execution.instance_variable_get(:@workflow).define_singleton_method(:approve) { |**| nil }
    event = workflow_command_history_event(
      "workflow_command_completed",
      method_name: "approve",
      kwargs: { reason: "operator" },
    )
    event["payload"]["result"] = nil

    assert_nil(execution.send(:replay_workflow_command_event, event))
  end

  test "workflow command replay validates failed command error shape" do
    execution = replay_execution_for(ApiTestApprovalWorkflow)
    event = workflow_command_history_event(
      "workflow_command_failed",
      method_name: "reject",
      kwargs: { reason: "not approved" },
      error: "DurababbleWorkflowTest::ApiTestChargeFailed: not approved",
    )

    assert_nil execution.send(:replay_workflow_command_event, event)

    event["error"] = "RuntimeError: different"
    error = assert_raises(Durababble::ReplayDivergenceError) do
      execution.send(:replay_workflow_command_event, event)
    end
    assert_match(/different error/, error.message)
  end

  test "workflow command replay rejects unknown or unexpectedly successful commands" do
    execution = replay_execution_for(ApiTestApprovalWorkflow)
    unknown = workflow_command_history_event("workflow_command_completed", method_name: "missing")

    error = assert_raises(Durababble::ReplayDivergenceError) do
      execution.send(:replay_workflow_command_event, unknown)
    end
    assert_match(/unknown workflow command missing/, error.message)

    successful = workflow_command_history_event(
      "workflow_command_failed",
      method_name: "approve",
      kwargs: { reason: "operator" },
      error: "RuntimeError: expected",
    )
    error = assert_raises(Durababble::ReplayDivergenceError) do
      execution.send(:replay_workflow_command_event, successful)
    end
    assert_match(/expected workflow command approve to fail/, error.message)
  end

  durababble_store_backends.each do |backend|
    test "routes exposed workflow queries to this runtime's active owner without durable mutation with #{backend.name}" do
      with_durababble_store(backend, "workflow_query_local_owner") do |store|
        store.migrate!
        reset_queryable_workflow_controls
        runtime = Durababble::WorkerRuntime.start(
          store:,
          workflows: [ApiTestQueryableWorkflow],
          worker_pool: "default",
          poll_interval: 0.01,
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: ApiTestQueryableWorkflow.workflow_name,
          input: { "state" => "local-owner" },
        )
        wait_until do
          ApiTestQueryableWorkflow.step_started.pop(true)
        rescue
          nil
        end
        store.workflow_rpc_client_factory = ->(_worker_id, worker_pool:) { raise "local query should not open a gRPC client for #{worker_pool}" }

        before_history = store.workflow_history_for(workflow_id)
        before_inbox = store.inbox_messages_for(target_kind: "workflow", target_type: ApiTestQueryableWorkflow.workflow_name, target_id: workflow_id)
        result = ApiTestQueryableWorkflow.handle(workflow_id, store:).snapshot(
          prefix: "local",
          metadata: { "nested" => ["value", 7] },
        )

        assert_equal(
          {
            "prefix" => "local",
            "state" => "local-owner",
            "metadata" => { "nested" => ["value", 7] },
            "instance_id" => ApiTestQueryableWorkflow.current_instance_id,
          },
          result,
        )
        assert_equal(before_history, store.workflow_history_for(workflow_id))
        assert_equal(before_inbox, store.inbox_messages_for(target_kind: "workflow", target_type: ApiTestQueryableWorkflow.workflow_name, target_id: workflow_id))
      ensure
        ApiTestQueryableWorkflow.release_step&.push(true)
        wait_until(timeout: 2) { store.workflow(workflow_id).fetch("status") == "completed" } if store && workflow_id
        runtime&.shutdown(timeout: 2)
      end
    end

    test "routes exposed workflow queries to a remote owner over gRPC and preserves kwargs with #{backend.name}" do
      with_durababble_store(backend, "workflow_query_remote_owner") do |store|
        store.migrate!
        reset_queryable_workflow_controls
        runtime = Durababble::WorkerRuntime.start(
          store:,
          workflows: [ApiTestQueryableWorkflow],
          worker_pool: "default",
          poll_interval: 0.01,
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: ApiTestQueryableWorkflow.workflow_name,
          input: { "state" => "remote-owner" },
        )
        wait_until do
          ApiTestQueryableWorkflow.step_started.pop(true)
        rescue
          nil
        end
        caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)

        result = ApiTestQueryableWorkflow.handle(workflow_id, store: caller_store).snapshot(
          prefix: "remote",
          metadata: { "nested" => [{ "a" => 1 }, "b"] },
        )

        assert_equal(
          {
            "prefix" => "remote",
            "state" => "remote-owner",
            "metadata" => { "nested" => [{ "a" => 1 }, "b"] },
            "instance_id" => ApiTestQueryableWorkflow.current_instance_id,
          },
          result,
        )
        assert_empty(caller_store.inbox_messages_for(target_kind: "workflow", target_type: ApiTestQueryableWorkflow.workflow_name, target_id: workflow_id))
      ensure
        caller_store&.close
        ApiTestQueryableWorkflow.release_step&.push(true)
        wait_until(timeout: 2) { store.workflow(workflow_id).fetch("status") == "completed" } if store && workflow_id
        runtime&.shutdown(timeout: 2)
      end
    end

    test "exposed workflow queries do not start workflows without an active owner with #{backend.name}" do
      with_durababble_store(backend, "workflow_query_no_owner") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(
          name: ApiTestQueryableWorkflow.workflow_name,
          input: { "state" => "pending" },
        )
        store.workflow_rpc_client_factory = ->(_worker_id, worker_pool:) { raise "query should not route without an owner in #{worker_pool}" }

        assert_raises_matching(Durababble::WorkflowRpc::NoActiveLease, /no active lease/) do
          ApiTestQueryableWorkflow.handle(workflow_id, store:).snapshot(prefix: "missing")
        end

        assert_hash_includes store.workflow(workflow_id), "status" => "pending", "locked_by" => nil
        assert_nil store.current_workflow_lease(workflow_id)
        assert_empty store.workflow_history_for(workflow_id)
        assert_empty store.inbox_messages_for(target_kind: "workflow", target_type: ApiTestQueryableWorkflow.workflow_name, target_id: workflow_id)
      end
    end

    test "exposed workflow queries report expired leases without enqueueing inbox work with #{backend.name}" do
      with_durababble_store(backend, "workflow_query_expired_owner") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(
          name: ApiTestQueryableWorkflow.workflow_name,
          input: { "state" => "expired" },
        )
        store.claim_workflow(workflow_id:, worker_id: "expired-owner", lease_seconds: 1)
        sleep(1.1)

        assert_raises_matching(Durababble::WorkflowRpc::NoActiveLease, /no active lease/) do
          ApiTestQueryableWorkflow.handle(workflow_id, store:).snapshot(prefix: "expired")
        end
        assert_empty store.inbox_messages_for(target_kind: "workflow", target_type: ApiTestQueryableWorkflow.workflow_name, target_id: workflow_id)
      end
    end

    test "exposed workflow queries surface unreachable active owners with #{backend.name}" do
      with_durababble_store(backend, "workflow_query_unreachable_owner") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(
          name: ApiTestQueryableWorkflow.workflow_name,
          input: { "state" => "unreachable" },
        )
        store.claim_workflow(workflow_id:, worker_id: "unreachable-owner", lease_seconds: 60)
        attempts = 0
        store.workflow_rpc_client_factory = lambda do |_worker_id, worker_pool:|
          WorkflowQueryFakeClient.new do
            attempts += 1
            raise Durababble::WorkflowRpc::NodeUnavailable, "owner unavailable in #{worker_pool}"
          end
        end

        assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /owner unavailable/) do
          ApiTestQueryableWorkflow.handle(workflow_id, store:).snapshot(prefix: "unreachable")
        end
        assert_equal 4, attempts
        assert_empty store.inbox_messages_for(target_kind: "workflow", target_type: ApiTestQueryableWorkflow.workflow_name, target_id: workflow_id)
      end
    end

    test "exposed workflow queries reroute after owner handoff with #{backend.name}" do
      with_durababble_store(backend, "workflow_query_handoff") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(
          name: ApiTestQueryableWorkflow.workflow_name,
          input: { "state" => "handoff" },
        )
        store.claim_workflow(workflow_id:, worker_id: "worker-a", lease_seconds: 60)
        clients = {}
        clients["worker-a"] = WorkflowQueryFakeClient.new do |_command, _payload|
          store.release_worker_leases!(worker_id: "worker-a")
          store.claim_workflow(workflow_id:, worker_id: "worker-b", lease_seconds: 60)
          raise Durababble::WorkflowRpc::StaleLease, "worker-a lost ownership"
        end
        clients["worker-b"] = WorkflowQueryFakeClient.new do |command, payload|
          assert_equal("workflow_rpc", command)
          assert_equal("snapshot", payload.fetch("command"))
          assert_equal(
            { "workflow_id" => workflow_id, "method" => "snapshot", "args" => [], "kwargs" => { prefix: "handoff" }, "rpc_kind" => "workflow_query" },
            payload.fetch("payload"),
          )
          { "owner" => "worker-b" }
        end
        store.workflow_rpc_client_factory = lambda do |worker_id, worker_pool:|
          _ = worker_pool
          clients.fetch(worker_id)
        end

        assert_equal({ "owner" => "worker-b" }, ApiTestQueryableWorkflow.handle(workflow_id, store:).snapshot(prefix: "handoff"))
        assert_equal 1, clients.fetch("worker-a").requests.length
        assert_equal 1, clients.fetch("worker-b").requests.length
        assert_empty store.inbox_messages_for(target_kind: "workflow", target_type: ApiTestQueryableWorkflow.workflow_name, target_id: workflow_id)
      end
    end

    test "exposed workflow queries route RPC clients with the active lease worker pool for #{backend.name}" do
      with_durababble_store(backend, "workflow_query_lease_pool") do |store|
        store.migrate!
        workflow_id = store.enqueue_workflow(
          name: ApiTestQueryableWorkflow.workflow_name,
          input: { "state" => "pool" },
          worker_pool: "priority",
        )
        store.claim_workflow(workflow_id:, worker_id: "worker-a", lease_seconds: 60, worker_pool: "priority")
        pools = []
        store.workflow_rpc_client_factory = lambda do |_address, worker_pool:|
          pools << worker_pool
          WorkflowQueryFakeClient.new { |_command, _payload| { "pool" => worker_pool } }
        end

        assert_equal({ "pool" => "priority" }, ApiTestQueryableWorkflow.handle(workflow_id, store:, worker_pool: "stale").snapshot(prefix: "pool"))
        assert_equal ["priority"], pools
      end
    end

    test "exposed workflow query context does not bleed into running workflow tasks with #{backend.name}" do
      with_durababble_store(backend, "workflow_query_context_isolation") do |store|
        store.migrate!
        reset_queryable_workflow_controls
        runtime = Durababble::WorkerRuntime.start(
          store:,
          workflows: [ApiTestQueryableWorkflow],
          worker_pool: "default",
          poll_interval: 0.01,
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: ApiTestQueryableWorkflow.workflow_name,
          input: { "state" => "query-isolated" },
        )
        wait_until do
          ApiTestQueryableWorkflow.step_started.pop(true)
        rescue
          nil
        end

        result_queue = Queue.new
        query_thread = Thread.new do
          result_queue << [:ok, ApiTestQueryableWorkflow.handle(workflow_id, store:).blocking_snapshot]
        rescue StandardError => e
          result_queue << [:error, e]
        end
        wait_until do
          ApiTestQueryableWorkflow.query_entered.pop(true)
        rescue
          nil
        end

        ApiTestQueryableWorkflow.release_step << true
        wait_until(timeout: 2) { store.workflow(workflow_id).fetch("status") == "completed" }
        ApiTestQueryableWorkflow.query_release << true
        status, value = result_queue.pop
        query_thread.join

        flunk(value.full_message) if status == :error
        assert_equal(:ok, status)
        assert_equal({ "state" => "query-isolated" }, value)
        assert_hash_includes(store.workflow(workflow_id), "status" => "completed")
      ensure
        ApiTestQueryableWorkflow.query_release&.push(true)
        ApiTestQueryableWorkflow.release_step&.push(true)
        query_thread&.kill if query_thread&.alive?
        runtime&.shutdown(timeout: 2)
      end
    end

    test "exposed workflow query bodies cannot schedule workflow steps or waits with #{backend.name}" do
      with_durababble_store(backend, "workflow_query_durable_boundary_guard") do |store|
        store.migrate!
        reset_queryable_workflow_controls
        runtime = Durababble::WorkerRuntime.start(
          store:,
          workflows: [ApiTestQueryableWorkflow],
          worker_pool: "default",
          poll_interval: 0.01,
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: ApiTestQueryableWorkflow.workflow_name,
          input: { "state" => "guard" },
        )
        wait_until do
          ApiTestQueryableWorkflow.step_started.pop(true)
        rescue
          nil
        end

        assert_raises_matching(Durababble::Error, /cannot call workflow steps from an exposed query/) do
          ApiTestQueryableWorkflow.handle(workflow_id, store:).query_step_forbidden
        end
        assert_raises_matching(Durababble::Error, /cannot schedule workflow waits from an exposed query/) do
          ApiTestQueryableWorkflow.handle(workflow_id, store:).query_wait_forbidden
        end
      ensure
        ApiTestQueryableWorkflow.release_step&.push(true)
        wait_until(timeout: 2) { store.workflow(workflow_id).fetch("status") == "completed" } if store && workflow_id
        runtime&.shutdown(timeout: 2)
      end
    end

    test "runs exposed workflow commands from durable inbox activations and returns results with #{backend.name}" do
      with_durababble_store(backend, "workflow_commands") do |store|
        worker = Durababble::Worker.new(
          store:,
          workflows: { ApiTestApprovalWorkflow.workflow_name => ApiTestApprovalWorkflow },
          worker_id: "command-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: ApiTestApprovalWorkflow.workflow_name,
          input: { "request_id" => "approval-request" },
        )

        assert_equal(:worked, worker.tick)
        assert_equal("waiting", store.workflow(workflow_id).fetch("status"))

        result_queue = Queue.new
        caller = Thread.new do
          caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
          begin
            result_queue << [:ok, ApiTestApprovalWorkflow.handle(workflow_id, store: caller_store).approve(reason: "operator")]
          rescue StandardError => e
            result_queue << [:error, e]
          ensure
            caller_store.close
          end
        end

        wait_until { store.target_activation(target_kind: "workflow", target_type: ApiTestApprovalWorkflow.workflow_name, target_id: workflow_id) }
        workflow_messages = store.inbox_messages_for(
          target_kind: "workflow",
          target_type: ApiTestApprovalWorkflow.workflow_name,
          target_id: workflow_id,
        )
        assert_equal(1, workflow_messages.length)
        assert_hash_includes(
          workflow_messages.first,
          "message_kind" => "workflow_command",
          "method_name" => "approve",
          "payload" => { "method" => "approve", "args" => [], "kwargs" => { reason: "operator" } },
        )
        assert_equal(:worked, worker.tick)
        status, value = result_queue.pop
        caller.join
        assert_equal(:ok, status)
        assert_equal({ "approved_by" => "operator" }, value)

        assert_hash_includes(store.workflow(workflow_id), "status" => "waiting")
        assert_equal(["pending"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") })
        assert_nil(store.target_activation(target_kind: "workflow", target_type: ApiTestApprovalWorkflow.workflow_name, target_id: workflow_id))
        assert_hash_includes(store.inbox_message(workflow_messages.first.fetch("id")), "status" => "completed", "result" => { "approved_by" => "operator" })
        assert_includes(store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }, "workflow_command_completed")
      ensure
        caller&.kill if caller&.alive?
      end
    end

    test "delivers workflow commands into the active deterministic execution with #{backend.name}" do
      with_durababble_store(backend, "workflow_active_commands") do |store|
        store.migrate!
        execute_object_ids = []
        command_object_ids = []
        finish_object_ids = []
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "active-approval-command"

          define_method(:execute) do |_input|
            execute_object_ids << object_id
            @approved = false
            ready = wait_condition(timeout: 60) { @approved }
            finish(ready)
          end

          define_method(:finish) do |ready|
            finish_object_ids << object_id
            { "ready" => ready, "approved" => @approved }
          end
          step :finish

          define_method(:approve) do
            @approved = true
            command_object_ids << object_id
            { "approved" => @approved }
          end
          expose_command :approve
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.workflow_name => workflow },
          worker_id: "active-command-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})

        assert_equal(:worked, worker.tick)
        assert_equal("waiting", store.workflow(workflow_id).fetch("status"))

        result_queue = Queue.new
        caller = Thread.new do
          caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
          begin
            result_queue << [:ok, workflow.handle(workflow_id, store: caller_store).approve]
          rescue StandardError => e
            result_queue << [:error, e]
          ensure
            caller_store.close
          end
        end

        wait_until { store.target_activation(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id) }
        assert_equal(:worked, worker.tick)
        status, command_result = result_queue.pop
        caller.join

        assert_equal(:ok, status)
        assert_equal(true, command_result.fetch("approved"))
        completed = store.workflow(workflow_id)
        assert_hash_includes(completed, "status" => "completed")
        assert_equal(
          { "ready" => true, "approved" => true },
          completed.fetch("result"),
        )
        assert_equal(command_object_ids.last, execute_object_ids.last)
        assert_equal(command_object_ids.last, finish_object_ids.last)
        assert_equal(2, execute_object_ids.length)
        assert_equal(1, command_object_ids.length)
        assert_nil(store.target_activation(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id))

        history = store.workflow_history_for(workflow_id)
        assert_equal(
          ["step_scheduled", "step_waiting", "workflow_command_completed", "step_scheduled", "step_started", "step_completed"],
          history.map { |event| event.fetch("kind") },
        )
        command_event = history.detect { |event| event.fetch("kind") == "workflow_command_completed" }
        assert_hash_includes(
          command_event.fetch("payload"),
          "method" => "approve",
          "args" => [],
          "kwargs" => {},
          "result" => command_result,
        )
      ensure
        caller&.kill if caller&.alive?
      end
    end

    test "retries exposed workflow commands according to their retry policy with #{backend.name}" do
      with_durababble_store(backend, "workflow_command_retry_policy") do |store|
        store.migrate!
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "retrying-approval-command"

          def execute(_input)
            @attempts = 0
            wait_condition(timeout: 60) { @approved }
          end

          expose_command retry: { maximum_attempts: 2, schedule: [0] }
          def approve
            @attempts += 1
            raise "transient approval failure" if @attempts == 1

            @approved = true
            { "attempts" => @attempts }
          end
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.workflow_name => workflow },
          worker_id: "retrying-command-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})

        assert_equal(:worked, worker.tick)

        result_queue = Queue.new
        caller = Thread.new do
          caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
          begin
            result_queue << [:ok, workflow.handle(workflow_id, store: caller_store).approve]
          rescue StandardError => e
            result_queue << [:error, e]
          ensure
            caller_store.close
          end
        end

        wait_until { store.target_activation(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id) }
        assert_equal(:worked, worker.tick)
        status, value = result_queue.pop
        caller.join

        assert_equal(:ok, status)
        assert_equal({ "attempts" => 2 }, value)
        message = store.inbox_messages_for(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id).first
        assert_hash_includes(message, "status" => "completed", "attempts" => 2, "max_attempts" => 2)
        assert_equal(
          ["workflow_command_failed", "workflow_command_completed"],
          store.workflow_history_for(workflow_id).select { |event| event.fetch("kind").start_with?("workflow_command_") }.map { |event| event.fetch("kind") },
        )
      ensure
        caller&.kill if caller&.alive?
      end
    end

    test "delivers a workflow command that satisfies a no-timeout wait_condition with #{backend.name}" do
      with_durababble_store(backend, "workflow_polling_wait_commands") do |store|
        store.migrate!
        execute_runs = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "polling-approval-command"

          define_method(:execute) do |_input|
            execute_runs += 1
            @approved = false
            ready = wait_condition { @approved }
            finish(ready)
          end

          define_method(:finish) { |ready| { "ready" => ready, "approved" => @approved } }
          step :finish

          define_method(:approve) do
            @approved = true
            { "approved" => @approved }
          end
          expose_command :approve
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.workflow_name => workflow },
          worker_id: "polling-command-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})

        # First tick: the single-task no-timeout wait suspends the workflow.
        assert_equal(:worked, worker.tick)
        assert_equal("waiting", store.workflow(workflow_id).fetch("status"))

        result_queue = Queue.new
        caller = Thread.new do
          caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
          begin
            result_queue << [:ok, workflow.handle(workflow_id, store: caller_store).approve]
          rescue StandardError => e
            result_queue << [:error, e]
          ensure
            caller_store.close
          end
        end

        wait_until { store.target_activation(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id) }
        # The approve command is delivered mid-wait, raising WorkflowCommandDelivered so
        # the polling wait_condition re-evaluates its block and the workflow completes.
        assert_equal(:worked, worker.tick)
        status, command_result = result_queue.pop
        caller.join

        assert_equal(:ok, status)
        assert_equal(true, command_result.fetch("approved"))
        assert_equal(2, execute_runs)
        completed = store.workflow(workflow_id)
        assert_hash_includes(completed, "status" => "completed")
        assert_equal({ "ready" => true, "approved" => true }, completed.fetch("result"))
        assert_equal(
          ["step_scheduled", "step_waiting", "workflow_command_completed", "step_scheduled", "step_started", "step_completed"],
          store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") },
        )
      ensure
        caller&.kill if caller&.alive?
      end
    end

    test "delivers a workflow command that satisfies a wait deferred behind a concurrent task with #{backend.name}" do
      with_durababble_store(backend, "workflow_deferred_wait_commands") do |store|
        store.migrate!
        execute_runs = 0
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "deferred-approval-command"

          # The wait runs concurrently with a sibling step, so the workflow has more than one
          # task and the wait DEFERS suspension instead of suspending immediately. The recorded
          # step_waiting therefore lands in history before the sibling's events and before the
          # delivered command — the exact ordering that used to strand the workflow in "waiting".
          define_method(:execute) do |_input|
            execute_runs += 1
            @approved = false
            results = Async do |task|
              approval = task.async { wait_condition { @approved } }
              sibling = task.async { do_work }
              { "approved" => approval.wait, "worked" => sibling.wait }
            end.wait
            finish(results)
          end

          define_method(:do_work) do
            sleep(0.01)
            { "worked" => true }
          end
          step :do_work

          define_method(:finish) { |results| { "results" => results, "approved" => @approved } }
          step :finish

          define_method(:approve) do
            @approved = true
            { "approved" => @approved }
          end
          expose_command :approve
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.workflow_name => workflow },
          worker_id: "deferred-command-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})

        # First tick: the deferred wait suspends the workflow once the sibling step finishes.
        assert_equal(:worked, worker.tick)
        assert_equal("waiting", store.workflow(workflow_id).fetch("status"))

        result_queue = Queue.new
        caller = Thread.new do
          caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
          begin
            result_queue << [:ok, workflow.handle(workflow_id, store: caller_store).approve]
          rescue StandardError => e
            result_queue << [:error, e]
          ensure
            caller_store.close
          end
        end

        wait_until { store.target_activation(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id) }
        # Second tick: the command is delivered at a safe point after the sibling replays, the
        # deferred wait re-evaluates its now-satisfied condition, and the workflow completes.
        assert_equal(:worked, worker.tick)
        status, command_result = result_queue.pop
        caller.join

        assert_equal(:ok, status)
        assert_equal(true, command_result.fetch("approved"))
        assert_equal(2, execute_runs)
        completed = store.workflow(workflow_id)
        assert_hash_includes(completed, "status" => "completed")
        assert_equal(
          { "results" => { "approved" => true, "worked" => { "worked" => true } }, "approved" => true },
          completed.fetch("result"),
        )
        assert_nil(store.target_activation(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id))
        history_kinds = store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }
        assert_includes(history_kinds, "workflow_command_completed")
        assert_operator(
          history_kinds.index("step_waiting"),
          :<,
          history_kinds.index("workflow_command_completed"),
          "the deferred wait must be recorded before the command it stranded behind it",
        )
      ensure
        caller&.kill if caller&.alive?
      end
    end

    test "returns exposed workflow command errors through the ask row with #{backend.name}" do
      with_durababble_store(backend, "workflow_command_errors") do |store|
        store.migrate!
        worker = Durababble::Worker.new(
          store:,
          workflows: { ApiTestApprovalWorkflow.workflow_name => ApiTestApprovalWorkflow },
          worker_id: "command-error-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(
          name: ApiTestApprovalWorkflow.workflow_name,
          input: { "request_id" => "approval-error-request" },
        )

        assert_equal(:worked, worker.tick)
        result_queue = Queue.new
        caller = Thread.new do
          caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
          begin
            ApiTestApprovalWorkflow.handle(workflow_id, store: caller_store).reject(reason: "no")
            result_queue << [:ok, nil]
          rescue StandardError => e
            result_queue << [:error, e]
          ensure
            caller_store.close
          end
        end

        wait_until { store.target_activation(target_kind: "workflow", target_type: ApiTestApprovalWorkflow.workflow_name, target_id: workflow_id) }
        assert_equal(:worked, worker.tick)
        status, error = result_queue.pop
        caller.join
        assert_equal(:error, status)
        assert_kind_of(Durababble::Error, error)
        assert_match(/ApiTestChargeFailed: no/, error.message)

        message = store.inbox_messages_for(target_kind: "workflow", target_type: ApiTestApprovalWorkflow.workflow_name, target_id: workflow_id).first
        assert_hash_includes(message, "status" => "dead_lettered")
        assert_nil(store.target_activation(target_kind: "workflow", target_type: ApiTestApprovalWorkflow.workflow_name, target_id: workflow_id))
        assert_includes(store.workflow_history_for(workflow_id).map { |event| event.fetch("kind") }, "workflow_command_failed")
      ensure
        caller&.kill if caller&.alive?
      end
    end
  end

  private

  def reset_queryable_workflow_controls
    ApiTestQueryableWorkflow.step_started = Queue.new
    ApiTestQueryableWorkflow.release_step = Queue.new
    ApiTestQueryableWorkflow.query_entered = Queue.new
    ApiTestQueryableWorkflow.query_release = Queue.new
    ApiTestQueryableWorkflow.current_instance_id = nil
  end

  def replay_execution_for(workflow_class)
    Durababble::WorkflowExecution.allocate.tap do |execution|
      execution.instance_variable_set(:@workflow_id, "wf-replay")
      execution.instance_variable_set(:@workflow_class, workflow_class)
      execution.instance_variable_set(:@workflow, workflow_class.new)
    end
  end

  def workflow_command_history_event(kind, method_name:, kwargs: {}, result: nil, error: nil)
    payload = { "method" => method_name.to_s, "args" => [], "kwargs" => kwargs }
    payload["result"] = result unless result.nil?
    event = { "kind" => kind, "name" => method_name.to_s, "payload" => payload }
    event["error"] = error unless error.nil?
    event
  end

  def wait_until(timeout: 10)
    deadline = Time.now + timeout
    loop do
      value = yield
      return value if value
      raise "condition not met before timeout" if Time.now >= deadline

      sleep(0.01)
    end
  end
end
