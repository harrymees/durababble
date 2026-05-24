# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababblePublicApiBranchCoverageTest < DurababbleTestCase
  class BranchTestWorkflow < Durababble::Workflow
    workflow_name "branch_test_workflow"

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

  class BranchTestStore
    attr_reader :events, :completed_commands, :failed_commands, :migrations, :closed

    def initialize
      @state = nil
      @commands = {}
      @next_command_id = 0
      @events = []
      @completed_commands = []
      @failed_commands = []
      @migrations = 0
      @closed = false
    end

    def migrate!
      @migrations += 1
    end

    def close
      @closed = true
    end

    def signal_event(event_key, payload:)
      @events << [event_key, payload]
      1
    end

    def object_state(object_type:, object_id:)
      @state
    end

    def save_object_state(object_type:, object_id:, state:)
      @state = state
    end

    def enqueue_object_command(object_type:, object_id:, method_name:, args:, kwargs:)
      @next_command_id += 1
      command_id = "cmd-#{@next_command_id}"
      @commands[command_id] = { object_type:, object_id:, method_name:, args:, kwargs: }
      command_id
    end

    def claim_object_command(command_id:, worker_id:)
      @commands.fetch(command_id).merge(worker_id:)
    end

    def complete_object_command(command_id:, result:, object_type: nil, object_id: nil, state: Durababble::Store::NO_OBJECT_STATE, worker_id: nil)
      save_object_state(object_type:, object_id:, state:) unless state.equal?(Durababble::Store::NO_OBJECT_STATE)
      @completed_commands << [command_id, result]
    end

    def fail_object_command(command_id:, error:, worker_id: nil)
      @failed_commands << [command_id, error]
    end
  end

  class BranchTestExecution
    attr_reader :step_context, :calls

    def initialize
      @step_context = :context
      @calls = []
    end

    def call_step(instance, method_name:, args:, kwargs:)
      @calls << [instance.class, method_name, args, kwargs]
      yield
    end
  end

  test "covers explicit and pending workflow macros plus keyword step invocation" do
    assert_hash_includes BranchTestPendingWorkflow.exposed_queries, query_with_pending_macro: true
    assert_includes BranchTestPendingWorkflow.exposed_commands, :command_with_pending_macro
    assert_includes BranchTestPendingWorkflow.step_order, :step_with_pending_macro

    workflow = BranchTestWorkflow.new
    execution = BranchTestExecution.new
    workflow.__durababble_execution__ = execution

    assert_equal "plain", workflow.echo("plain")
    assert_equal "keyword", workflow.kw_echo(value: "keyword")
    assert_equal [:echo, :kw_echo], execution.calls.map { |(_klass, method_name, _args, _kwargs)| method_name }
  end

  test "exposes workflow refs for keyword queries and command events and rejects missing methods" do
    store = BranchTestStore.new
    ref = BranchTestWorkflow.ref("wf-123", store:)

    assert_respond_to ref, :labeled_status
    assert_equal "status:wf-123", ref.labeled_status(prefix: "status")
    assert_equal 1, ref.note(message: "hello")
    assert_equal(
      [
        ["workflow:wf-123:command:note", { "method" => "note", "args" => [], "kwargs" => { message: "hello" } }],
      ],
      store.events,
    )
    assert_raises(NoMethodError) { ref.not_exposed }
  end

  test "raises when a workflow step is called outside workflow execution" do
    assert_raises_matching(Durababble::Error, /outside workflow execution/) do
      BranchTestWorkflow.new.echo("outside")
    end
  end

  test "covers durable object query, command, retry, nil-store update, and missing methods" do
    store = BranchTestStore.new
    account = BranchTestDurableObject.ref("acct-1", store:)

    assert_equal "branch_account", BranchTestDurableObject.object_type
    assert_respond_to account, :formatted
    assert_equal "balance:0", account.formatted(prefix: "balance")
    assert_equal({ "value" => 3 }, account.add(amount: 3))
    assert_equal({ "value" => 7, "attempts" => 1 }, account.flaky_add(amount: 4))
    assert_equal "balance:7", account.formatted(prefix: "balance")
    assert_equal 1, store.failed_commands.length
    assert_equal 2, store.completed_commands.length
    assert_raises(NoMethodError) { account.not_exposed }

    transient = BranchTestDurableObject.new
    assert_equal({ "value" => 9 }, transient.update_state("value" => 9))
  end

  test "configures a default store when no previous store exists" do
    Durababble::Store.expects(:connect).returns(BranchTestStore.new)
    Durababble.default_store = nil

    Durababble.configure(database_url: "postgresql://example.invalid/db", schema: "branch_test")

    assert_kind_of(BranchTestStore, Durababble.default_store)
  ensure
    Durababble.default_store = nil
  end

  test "closes a previously configured default store before replacing it" do
    old_store = BranchTestStore.new
    Durababble::Store.expects(:connect).returns(BranchTestStore.new)
    Durababble.default_store = old_store

    Durababble.configure(database_url: "postgresql://example.invalid/db", schema: "branch_test")

    assert(old_store.closed)
    assert_kind_of(BranchTestStore, Durababble.default_store)
  ensure
    Durababble.default_store = nil
  end
end
