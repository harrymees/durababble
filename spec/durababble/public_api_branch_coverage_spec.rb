# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Durababble public API branch contracts" do
  class BranchSpecWorkflow < Durababble::Workflow
    workflow_name "branch_spec_workflow"

    expose def labeled_status(prefix:)
      "#{prefix}:#{@__durababble_ref_workflow_id}"
    end

    expose_command def cancel(reason:)
      reason
    end

    step def echo(input)
      input
    end

    step def kw_echo(value:)
      value
    end
  end

  class BranchSpecPendingWorkflow < Durababble::Workflow
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

  class BranchSpecDurableObject < Durababble::DurableObject
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
      if command_context.attempt_number == 1
        raise "try again"
      end

      update_state("value" => state.fetch("value", 0) + amount, "attempts" => command_context.attempt_number - 1)
    end
  end

  class BranchSpecStore
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

  class BranchSpecExecution
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

  it "covers explicit and pending workflow macros plus keyword step invocation" do
    expect(BranchSpecPendingWorkflow.exposed_queries).to include(query_with_pending_macro: true)
    expect(BranchSpecPendingWorkflow.exposed_commands).to include(:command_with_pending_macro)
    expect(BranchSpecPendingWorkflow.step_order).to include(:step_with_pending_macro)

    workflow = BranchSpecWorkflow.new
    execution = BranchSpecExecution.new
    workflow.__durababble_execution__ = execution

    expect(workflow.echo("plain")).to eq("plain")
    expect(workflow.kw_echo(value: "keyword")).to eq("keyword")
    expect(execution.calls.map { |(_, method_name, _, _)| method_name }).to eq(%i[echo kw_echo])
  end

  it "exposes workflow refs for keyword queries and command events and rejects missing methods" do
    store = BranchSpecStore.new
    ref = BranchSpecWorkflow.ref("wf-123", store:)

    expect(ref).to respond_to(:labeled_status)
    expect(ref.labeled_status(prefix: "status")).to eq("status:wf-123")
    expect(ref.cancel(reason: "nope")).to eq(1)
    expect(store.events).to eq([
      ["workflow:wf-123:command:cancel", { "method" => "cancel", "args" => [], "kwargs" => { reason: "nope" } }]
    ])
    expect { ref.not_exposed }.to raise_error(NoMethodError)
  end

  it "raises when a workflow step is called outside workflow execution" do
    expect { BranchSpecWorkflow.new.echo("outside") }.to raise_error(Durababble::Error, /outside workflow execution/)
  end

  it "covers durable object query, command, retry, nil-store update, and missing methods" do
    store = BranchSpecStore.new
    account = BranchSpecDurableObject.ref("acct-1", store:)

    expect(BranchSpecDurableObject.object_type).to eq("branch_account")
    expect(account).to respond_to(:formatted)
    expect(account.formatted(prefix: "balance")).to eq("balance:0")
    expect(account.add(amount: 3)).to eq("value" => 3)
    expect(account.flaky_add(amount: 4)).to eq("value" => 7, "attempts" => 1)
    expect(account.formatted(prefix: "balance")).to eq("balance:7")
    expect(store.failed_commands.length).to eq(1)
    expect(store.completed_commands.length).to eq(2)
    expect { account.not_exposed }.to raise_error(NoMethodError)

    transient = BranchSpecDurableObject.new
    expect(transient.update_state("value" => 9)).to eq("value" => 9)
  end

  it "configures a default store when no previous store exists" do
    allow(Durababble::Store).to receive(:connect).and_return(BranchSpecStore.new)
    Durababble.default_store = nil

    Durababble.configure(database_url: "postgresql://example.invalid/db", schema: "branch_spec")

    expect(Durababble.default_store).to be_a(BranchSpecStore)
  ensure
    Durababble.default_store = nil
  end

  it "closes a previously configured default store before replacing it" do
    old_store = BranchSpecStore.new
    allow(Durababble::Store).to receive(:connect).and_return(BranchSpecStore.new)
    Durababble.default_store = old_store

    Durababble.configure(database_url: "postgresql://example.invalid/db", schema: "branch_spec")

    expect(old_store.closed).to be(true)
    expect(Durababble.default_store).to be_a(BranchSpecStore)
  ensure
    Durababble.default_store = nil
  end
end
