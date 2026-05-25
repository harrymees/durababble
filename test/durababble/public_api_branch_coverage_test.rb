# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababblePublicApiBranchCoverageTest < DurababbleTestCase
  class BranchTestWorkflow < Durababble::Workflow
    workflow_name "branch_test_workflow"

    def execute(input)
      [echo(input.fetch("plain")), kw_echo(value: input.fetch("keyword"))]
    end

    expose def labeled_status(prefix:)
      "#{prefix}:#{@__durababble_ref_workflow_id}"
    end

    expose_command def note(message:)
      message
    end

    step def echo(input)
      input
    end

    step def kw_echo(value:)
      value
    end
  end

  class BranchTestPendingWorkflow < Durababble::Workflow
    expose
    def query_with_pending_macro
      "query"
    end

    expose_command
    def command_with_pending_macro
      "command"
    end

    step
    def step_with_pending_macro(input)
      input
    end
  end

  class BranchTestDurableObject < Durababble::DurableObject
    object_type "branch_account"

    def initialize_state
      { "value" => 0 }
    end

    expose
    def formatted(prefix:)
      "#{prefix}:#{current_state.fetch("value", 0)}"
    end

    expose_command
    def add(amount:)
      update_state("value" => current_state.fetch("value", 0) + amount)
    end

    expose_command retry: { maximum_attempts: 2, schedule: [0] }
    def flaky_add(amount:)
      state = current_state
      raise "try again" if command_context.attempt_number == 1

      update_state("value" => state.fetch("value", 0) + amount, "attempts" => command_context.attempt_number - 1)
    end
  end

  durababble_store_backends.each do |backend|
    test "covers explicit and pending workflow macros plus keyword step invocation with #{backend.name}" do
      with_durababble_store(backend, "public_api_branch_workflow") do |store|
        run = Durababble::Engine.new(store:, migrate: false).run(
          BranchTestWorkflow,
          input: { "plain" => "plain", "keyword" => "keyword" },
        )

        assert_equal "completed", run.status
        assert_equal ["plain", "keyword"], run.result
        assert_equal(
          [
            ["echo", "completed"],
            ["kw_echo", "completed"],
          ],
          store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )
      end
    end
  end

  test "registers explicit and pending workflow macros" do
    assert_hash_includes BranchTestPendingWorkflow.exposed_queries, query_with_pending_macro: true
    assert_includes BranchTestPendingWorkflow.exposed_commands, :command_with_pending_macro
    assert_includes BranchTestPendingWorkflow.step_order, :step_with_pending_macro
  end

  durababble_store_backends.each do |backend|
    test "exposes workflow refs for keyword queries and command events with #{backend.name}" do
      with_durababble_store(backend, "public_api_branch_workflow_ref") do |store|
        workflow_id = store.enqueue_workflow(name: BranchTestWorkflow.workflow_name, input: { "plain" => "plain", "keyword" => "keyword" })
        store.mark_workflow_running(workflow_id, worker_id: "command-worker", lease_seconds: 60)
        ref = BranchTestWorkflow.ref(workflow_id, store:)

        assert_respond_to(ref, :labeled_status)
        assert_equal("status:#{workflow_id}", ref.labeled_status(prefix: "status"))

        result_queue = Queue.new
        caller = Thread.new do
          caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
          begin
            result_queue << [:ok, BranchTestWorkflow.ref(workflow_id, store: caller_store).note(message: "hello", idempotency_key: "note:hello")]
          rescue StandardError => e
            result_queue << [:error, e]
          ensure
            caller_store.close
          end
        end

        wait_until { store.target_activation(target_kind: "workflow", target_type: BranchTestWorkflow.workflow_name, target_id: workflow_id) }
        messages = store.inbox_messages_for(
          target_kind: "workflow",
          target_type: BranchTestWorkflow.workflow_name,
          target_id: workflow_id,
        )
        assert_equal(1, messages.length)
        assert_hash_includes(
          messages.first,
          "message_kind" => "workflow_command",
          "method_name" => "note",
          "payload" => { "method" => "note", "args" => [], "kwargs" => { message: "hello" } },
          "idempotency_key" => "note:hello",
        )

        drained = Durababble::Engine.new(store:, worker_id: "command-worker", migrate: false)
          .drain_workflow_inbox(BranchTestWorkflow, workflow_id:)
        status, value = result_queue.pop
        caller.join

        assert_equal(1, drained)
        assert_equal(:ok, status)
        assert_equal("hello", value)
        assert_hash_includes(store.inbox_message(messages.first.fetch("id")), "status" => "completed", "result" => "hello")
        assert_raises(NoMethodError) { ref.not_exposed }
      ensure
        caller&.kill if caller&.alive?
      end
    end
  end

  test "raises when a workflow step is called outside workflow execution" do
    assert_raises_matching(Durababble::Error, /outside workflow execution/) do
      BranchTestWorkflow.new.echo("outside")
    end
  end

  durababble_store_backends.each do |backend|
    test "covers durable object query, command, retry, and missing methods with #{backend.name}" do
      with_durababble_store(backend, "public_api_branch_object") do |store|
        account = BranchTestDurableObject.ref("acct-1", store:)

        assert_equal "branch_account", BranchTestDurableObject.object_type
        assert_respond_to account, :formatted
        assert_equal "balance:0", account.formatted(prefix: "balance")
        assert_equal({ "value" => 3 }, account.add(amount: 3))
        assert_equal({ "value" => 7, "attempts" => 1 }, account.flaky_add(amount: 4))
        assert_equal "balance:7", account.formatted(prefix: "balance")
        assert_equal({ "value" => 7, "attempts" => 1 }, store.object_state(object_type: "branch_account", object_id: "acct-1"))
        assert_equal(
          [
            ["add", "completed", 1],
            ["flaky_add", "completed", 2],
          ],
          store.inbox_messages_for(target_kind: "object", target_type: "branch_account", target_id: "acct-1").map do |message|
            [message.fetch("method_name"), message.fetch("status"), message.fetch("attempts")]
          end,
        )
        assert_raises(NoMethodError) { account.not_exposed }
      end
    end
  end

  test "updates transient durable object state without a store" do
    transient = BranchTestDurableObject.new
    assert_equal({ "value" => 9 }, transient.update_state("value" => 9))
  end

  test "configures a default store when no previous store exists" do
    backend = durababble_store_backends.first
    schema_name = "#{backend.default_schema_prefix}_configure_#{Process.pid}_#{SecureRandom.hex(4)}"
    Durababble.default_store = nil

    configured = Durababble.configure(database_url: backend.database_url, schema: schema_name)

    assert_same(configured, Durababble.default_store)
    assert_equal(schema_name, Durababble.default_store.schema)
    assert_kind_of(Durababble::Store, Durababble.default_store)
  ensure
    Durababble.default_store&.drop_schema!
    Durababble.default_store&.close
    Durababble.default_store = nil
  end

  test "closes a previously configured default store before replacing it" do
    backend = durababble_store_backends.first
    old_schema = "#{backend.default_schema_prefix}_configure_old_#{Process.pid}_#{SecureRandom.hex(4)}"
    new_schema = "#{backend.default_schema_prefix}_configure_new_#{Process.pid}_#{SecureRandom.hex(4)}"
    old_store = Durababble.configure(database_url: backend.database_url, schema: old_schema)
    old_pool = old_store.instance_variable_get(:@owner).connection_pool

    new_store = Durababble.configure(database_url: backend.database_url, schema: new_schema)

    refute(old_pool.active_connection?)
    assert_same(new_store, Durababble.default_store)
    assert_equal(new_schema, Durababble.default_store.schema)
  ensure
    old_store&.drop_schema!
    old_store&.close
    Durababble.default_store&.drop_schema!
    Durababble.default_store&.close
    Durababble.default_store = nil
  end

  private

  def wait_until(timeout: 2)
    deadline = Time.now + timeout
    loop do
      value = yield
      return value if value
      raise "condition not met before timeout" if Time.now >= deadline

      sleep(0.01)
    end
  end
end
