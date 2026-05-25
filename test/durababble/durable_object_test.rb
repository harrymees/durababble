# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleDurableObjectTest < DurababbleTestCase
  AccountState = Data.define(:balance_cents) do
    def credit(amount)
      with(balance_cents: balance_cents + amount)
    end

    def debit(amount)
      with(balance_cents: balance_cents - amount)
    end
  end

  class ApiTestAccount < Durababble::DurableObject
    def initialize_state
      AccountState.new(balance_cents: 0)
    end

    expose_command retry: { maximum_attempts: 2 }
    def credit(amount_cents)
      update_state(current_state.credit(amount_cents))
    end

    expose_command def debit(amount_cents)
      raise ArgumentError, "insufficient funds" if current_state.balance_cents < amount_cents

      update_state(current_state.debit(amount_cents))
    end

    expose def balance
      current_state.balance_cents
    end
  end

  class EmptyMailboxStore
    attr_reader :claimed

    def initialize
      @claimed = []
    end

    def claim_inbox_messages(**kwargs)
      @claimed << kwargs
      []
    end
  end

  class ClaimlessTestObject < Durababble::DurableObject
    expose_command def mutate
      update_state({ "mutated" => true })
    end
  end

  class BranchCommandStore
    attr_reader :completed, :failed, :retried

    def initialize(complete_result: ActiveRecord::Result.empty(affected_rows: 1), messages: nil, state: { "value" => 1 })
      @complete_result = complete_result
      @state = state
      @completed = []
      @failed = []
      @retried = []
      @messages = messages || [
        {
          "id" => "cmd-1",
          "target_kind" => "object",
          "target_type" => "clean_command_object",
          "target_id" => "clean",
          "message_kind" => "ask",
          "method_name" => "read_only",
          "payload" => { "method_name" => "read_only", "args" => [], "kwargs" => {} },
          "attempts" => 1,
        },
      ]
    end

    def claim_inbox_messages(**)
      message = @messages.shift
      message ? [message] : []
    end

    def object_state(object_type:, object_id:) = @state

    def complete_object_command(command_id:, result:, **kwargs)
      @completed << [command_id, result]
      @state = kwargs.fetch(:state) if kwargs.key?(:state) && !kwargs.fetch(:state).equal?(Durababble::Store::NO_OBJECT_STATE)
      @complete_result
    end

    def fail_object_command(command_id:, error:, worker_id:, terminal: false)
      @failed << [command_id, error, worker_id, terminal]
    end

    def retry_object_command(command_id:, error:, worker_id:, ready_at:)
      @retried << [command_id, error, worker_id, ready_at]
    end
  end

  class AskWaitStore
    attr_reader :enqueued, :waits

    def initialize
      @enqueued = []
      @waits = []
    end

    def migrate! = true

    def enqueue_object_command(**kwargs)
      @enqueued << kwargs
      "cmd-#{@enqueued.length}"
    end

    def deliver_target_message(**) = false

    def wait_for_inbox_message(message_id, poll_interval: 0.05, timeout: 10)
      @waits << { message_id:, poll_interval:, timeout: }
      "waited:#{message_id}"
    end
  end

  class CleanCommandObject < Durababble::DurableObject
    expose_command def read_only
      "unchanged"
    end
  end

  class LongScheduleAskObject < Durababble::DurableObject
    expose_command retry: { maximum_attempts: 3, schedule: [11, 13] }
    def eventually
      "ok"
    end
  end

  class ExponentialBackoffAskObject < Durababble::DurableObject
    expose_command retry: { maximum_attempts: 4, initial_interval: 2, backoff_coefficient: 3, maximum_interval: 10 }
    def eventually
      "ok"
    end
  end

  class UnboundedAskObject < Durababble::DurableObject
    expose_command retry: { maximum_attempts: nil, schedule: [11] }
    def eventually
      "ok"
    end
  end

  class WakeTestObject < Durababble::DurableObject
    def on_wake(payload:)
      update_state(payload.merge("woken" => true))
      "awake"
    end
  end

  class NoWakeTestObject < Durababble::DurableObject
  end

  class RetryStateTestCounter < Durababble::DurableObject
    def initialize_state
      { "count" => 0 }
    end

    expose_command retry: { maximum_attempts: 2, schedule: [0] }
    def increment_with_transient_failure
      update_state({ "count" => current_state.fetch("count") + 1 })
      raise "transient after state update" if command_context.attempt_number == 1

      current_state
    end

    expose_command def increment_and_fail
      update_state({ "count" => current_state.fetch("count") + 1 })
      raise "permanent after state update"
    end

    expose def count
      current_state.fetch("count")
    end
  end

  test "does not execute a durable object command when no mailbox row can be claimed" do
    store = EmptyMailboxStore.new
    executor = Durababble::DurableObjectExecutor.new(
      store:,
      objects: { ClaimlessTestObject.object_type => ClaimlessTestObject },
      worker_id: "worker-a",
      lease_seconds: 9,
    )

    assert_equal 0, executor.drain_object_inbox(ClaimlessTestObject.object_type, object_id: "object-1")
    assert_equal(
      [
        {
          target_kind: "object",
          target_type: ClaimlessTestObject.object_type,
          target_id: "object-1",
          worker_id: "worker-a",
          lease_seconds: 9,
          limit: 1,
        },
      ],
      store.claimed,
    )
  end

  test "derives fallback object types, ignores unknown macros, and saves state through the store" do
    anonymous_object = Class.new(Durababble::DurableObject)
    assert_match(/\A\d+\z/, anonymous_object.object_type)

    odd_object = Class.new(Durababble::DurableObject)
    odd_object.instance_variable_set(:@pending_durable_macro, [:unknown, {}])
    odd_object.class_eval { def ignored_macro = true }

    save_store = Object.new
    saved = []
    save_store.define_singleton_method(:save_object_state) { |**kwargs| saved << kwargs }
    object = anonymous_object.new(durable_id: "obj-1", store: save_store)
    object.update_state({ "saved" => true })
    assert_equal [{ object_type: anonymous_object.object_type, object_id: "obj-1", state: { "saved" => true } }], saved
  end

  test "completes read-only commands and fails them when the completion lease is lost" do
    clean_command_store = BranchCommandStore.new
    clean_executor = Durababble::DurableObjectExecutor.new(
      store: clean_command_store,
      objects: { CleanCommandObject.object_type => CleanCommandObject },
      worker_id: "worker-a",
      lease_seconds: 30,
    )
    assert_equal 1, clean_executor.drain_object_inbox(CleanCommandObject.object_type, object_id: "clean")
    assert_equal 1, clean_command_store.completed.length

    lost_lease_store = BranchCommandStore.new(complete_result: ActiveRecord::Result.empty(affected_rows: 0))
    lost_executor = Durababble::DurableObjectExecutor.new(
      store: lost_lease_store,
      objects: { CleanCommandObject.object_type => CleanCommandObject },
      worker_id: "worker-a",
      lease_seconds: 30,
    )
    assert_raises(Durababble::LeaseConflict) do
      lost_executor.drain_object_inbox(CleanCommandObject.object_type, object_id: "clean")
    end
    assert_empty lost_lease_store.failed
  end

  test "fails unsupported and unknown object mailbox messages without running user commands" do
    unsupported = BranchCommandStore.new(messages: [
      {
        "id" => "msg-unsupported",
        "target_kind" => "object",
        "target_type" => CleanCommandObject.object_type,
        "target_id" => "clean",
        "message_kind" => "mystery",
        "payload" => {},
        "attempts" => 1,
      },
    ])
    unsupported_executor = Durababble::DurableObjectExecutor.new(
      store: unsupported,
      objects: [CleanCommandObject],
      worker_id: "worker-a",
      lease_seconds: 30,
    )
    assert_equal 1, unsupported_executor.drain_object_inbox(CleanCommandObject.object_type, object_id: "clean")
    assert_equal [["msg-unsupported", "Durababble::Error: unsupported object inbox message mystery", "worker-a", true]], unsupported.failed

    unknown = BranchCommandStore.new(messages: [
      {
        "id" => "msg-unknown",
        "target_kind" => "object",
        "target_type" => CleanCommandObject.object_type,
        "target_id" => "clean",
        "message_kind" => "ask",
        "method_name" => "missing",
        "payload" => { "method_name" => "missing", "args" => [], "kwargs" => {} },
        "attempts" => 1,
      },
    ])
    unknown_executor = Durababble::DurableObjectExecutor.new(
      store: unknown,
      objects: [CleanCommandObject],
      worker_id: "worker-a",
      lease_seconds: 30,
    )
    assert_equal 1, unknown_executor.drain_object_inbox(CleanCommandObject.object_type, object_id: "clean")
    assert_equal [["msg-unknown", "Durababble::WorkflowRpc::UnknownCommand: missing", "worker-a", true]], unknown.failed
  end

  test "handles object wake messages with and without lifecycle handlers" do
    wake = BranchCommandStore.new(
      messages: [
        {
          "id" => "wake-1",
          "target_kind" => "object",
          "target_type" => WakeTestObject.object_type,
          "target_id" => "wake-object",
          "message_kind" => "wake",
          "payload" => { "reason" => "timer" },
          "attempts" => 1,
        },
      ],
      state: nil,
    )
    wake_executor = Durababble::DurableObjectExecutor.new(
      store: wake,
      objects: [WakeTestObject],
      worker_id: "worker-a",
      lease_seconds: 30,
    )
    assert_equal 1, wake_executor.drain_object_inbox(WakeTestObject.object_type, object_id: "wake-object")
    assert_equal [["wake-1", "awake"]], wake.completed
    assert_equal({ "reason" => "timer", "woken" => true }, wake.object_state(object_type: WakeTestObject.object_type, object_id: "wake-object"))

    no_wake = BranchCommandStore.new(messages: [
      {
        "id" => "wake-2",
        "target_kind" => "object",
        "target_type" => NoWakeTestObject.object_type,
        "target_id" => "wake-object",
        "message_kind" => "wake",
        "payload" => {},
        "attempts" => 1,
      },
    ])
    no_wake_executor = Durababble::DurableObjectExecutor.new(
      store: no_wake,
      objects: [NoWakeTestObject],
      worker_id: "worker-a",
      lease_seconds: 30,
    )
    assert_equal 1, no_wake_executor.drain_object_inbox(NoWakeTestObject.object_type, object_id: "wake-object")
    assert_equal [["wake-2", nil]], no_wake.completed
  end

  test "covers object at, unknown tell, and unbounded retry metadata branches" do
    assert_instance_of Durababble::DurableObjectRef, CleanCommandObject.at("clean", store: Object.new)
    assert_instance_of Durababble::DurableObjectRef, CleanCommandObject.at("clean", engine: Durababble::Engine.new(store: Object.new, migrate: false))
    assert_raises(NoMethodError) { CleanCommandObject.tell("clean", :missing, store: Object.new) }
    assert_raises(ArgumentError) { CleanCommandObject.at("clean", store: Object.new, engine: Durababble::Engine.new(store: Object.new, migrate: false)) }

    unbounded = Durababble::RetryPolicy.new(maximum_attempts: nil)
    assert_nil CleanCommandObject.send(:inbox_max_attempts, unbounded)
    assert_nil CleanCommandObject.ref("clean", store: Object.new).send(:inbox_max_attempts, unbounded)
  end

  test "durable object handles and tells use default and explicit engines" do
    explicit_store = AskWaitStore.new
    explicit_engine = Durababble::Engine.new(store: explicit_store, migrate: false)
    assert_equal("waited:cmd-1", CleanCommandObject.at("object-1", engine: explicit_engine).read_only)
    assert_equal("cmd-2", CleanCommandObject.tell("object-1", :read_only, engine: explicit_engine))
    assert_equal(["ask", "tell"], explicit_store.enqueued.map { |command| command.fetch(:message_kind) })

    default_store = AskWaitStore.new
    Durababble.default_engine = Durababble::Engine.new(store: default_store, migrate: false)
    assert_equal("waited:cmd-1", CleanCommandObject.at("object-2").read_only)
    assert_equal("cmd-2", CleanCommandObject.tell("object-2", :read_only))
    assert_equal(["ask", "tell"], default_store.enqueued.map { |command| command.fetch(:message_kind) })
  ensure
    Durababble.default_store = nil
  end

  test "waits long enough for synchronous asks to exhaust finite retry policies" do
    long_schedule_store = AskWaitStore.new
    assert_equal "waited:cmd-1", LongScheduleAskObject.ref("object-1", store: long_schedule_store).eventually
    assert_equal 3, long_schedule_store.enqueued.first.fetch(:max_attempts)
    assert_equal 34, long_schedule_store.waits.first.fetch(:timeout)

    exponential_store = AskWaitStore.new
    assert_equal "waited:cmd-1", ExponentialBackoffAskObject.ref("object-1", store: exponential_store).eventually
    assert_equal 4, exponential_store.enqueued.first.fetch(:max_attempts)
    assert_equal 28, exponential_store.waits.first.fetch(:timeout)
  end

  test "does not impose the default ask wait timeout on unbounded retry policies" do
    store = AskWaitStore.new
    assert_equal "waited:cmd-1", UnboundedAskObject.ref("object-1", store:).eventually
    assert_nil store.enqueued.first.fetch(:max_attempts)
    assert_nil store.waits.first.fetch(:timeout)
  end

  durababble_store_backends.each do |backend|
    test "runs exposed object asks on a worker and leaves caller-side state untouched with #{backend.name}" do
      with_durababble_store(backend, "durable_object_api") do |store|
        worker = object_worker(store, ApiTestAccount)

        assert_not_respond_to(ApiTestAccount, :step)
        assert_equal(0, ApiTestAccount.ref("acct-1", store:).balance)

        caller = call_object_command_async(backend, ApiTestAccount, "acct-1", [:credit, 500])
        wait_for_object_activation(ApiTestAccount, "acct-1")
        assert_nil(store.object_state(object_type: ApiTestAccount.object_type, object_id: "acct-1"))
        run_worker_until_result(worker, caller.fetch(:queue))

        status, value = caller.fetch(:queue).pop
        caller.fetch(:thread).join
        assert_equal(:ok, status)
        assert_equal(500, value.balance_cents)
        assert_equal(500, ApiTestAccount.ref("acct-1", store:).balance)
      ensure
        caller&.fetch(:thread)&.kill if caller&.fetch(:thread)&.alive?
      end
    end

    test "orders tells before later asks for the same object with #{backend.name}" do
      with_durababble_store(backend, "durable_object_tell_order") do |store|
        worker = object_worker(store, ApiTestAccount)

        tell_id = ApiTestAccount.tell("acct-2", :credit, 500, store:)
        caller = call_object_command_async(backend, ApiTestAccount, "acct-2", [:debit, 125])
        wait_for_object_activation(ApiTestAccount, "acct-2")
        run_worker_until_result(worker, caller.fetch(:queue))

        status, value = caller.fetch(:queue).pop
        caller.fetch(:thread).join
        assert_equal(:ok, status)
        assert_equal(375, value.balance_cents)
        assert_equal(375, ApiTestAccount.ref("acct-2", store:).balance)

        messages = store.inbox_messages_for(target_kind: "object", target_type: ApiTestAccount.object_type, target_id: "acct-2")
        assert_equal([tell_id, messages.last.fetch("id")], messages.map { |message| message.fetch("id") })
        assert_equal(["completed", "completed"], messages.map { |message| message.fetch("status") })
      ensure
        caller&.fetch(:thread)&.kill if caller&.fetch(:thread)&.alive?
      end
    end

    test "persists durable object state atomically with command completion with #{backend.name}" do
      with_durababble_store(backend, "durable_object_api") do |store|
        worker = object_worker(store, RetryStateTestCounter)
        counter = RetryStateTestCounter.ref("counter-1", store:)

        caller = call_object_command_async(backend, RetryStateTestCounter, "counter-1", [:increment_with_transient_failure])
        wait_for_object_activation(RetryStateTestCounter, "counter-1")
        run_worker_until_result(worker, caller.fetch(:queue))
        status, result = caller.fetch(:queue).pop
        caller.fetch(:thread).join
        assert_equal(:ok, status)
        assert_equal({ "count" => 1 }, result)
        assert_equal(1, counter.count)

        failing = call_object_command_async(backend, RetryStateTestCounter, "counter-1", [:increment_and_fail])
        wait_for_object_activation(RetryStateTestCounter, "counter-1")
        run_worker_until_result(worker, failing.fetch(:queue))
        failed_status, error = failing.fetch(:queue).pop
        failing.fetch(:thread).join
        assert_equal(:error, failed_status)
        assert_match(/permanent after state update/, error.message)
        assert_equal(1, counter.count)
      ensure
        caller&.fetch(:thread)&.kill if caller&.fetch(:thread)&.alive?
        failing&.fetch(:thread)&.kill if failing&.fetch(:thread)&.alive?
      end
    end

    test "rejects state mutation from exposed queries with #{backend.name}" do
      with_durababble_store(backend, "durable_object_api") do |store|
        unsafe_object = Class.new(Durababble::DurableObject) do
          object_type "unsafe_query_object"

          def initialize_state
            { "count" => 0 }
          end

          expose def bump_from_query
            update_state({ "count" => current_state.fetch("count") + 1 })
          end

          expose def snapshot
            current_state
          end
        end
        object = unsafe_object.ref("object-1", store:)

        assert_raises_matching(Durababble::Error, /cannot update durable object state from an exposed query/) do
          object.bump_from_query
        end
        assert_equal({ "count" => 0 }, object.snapshot)
      end
    end

    test "keeps a command idempotency key stable across retry with #{backend.name}" do
      with_durababble_store(backend, "durable_object_api") do |store|
        seen_keys = []
        retrying_object = Class.new(Durababble::DurableObject) do
          object_type "retrying_object"

          def initialize_state
            { "committed_attempts" => 0 }
          end

          define_method(:write_with_retry) do |value|
            seen_keys << command_context.idempotency_key
            update_state({
              "committed_attempts" => current_state.fetch("committed_attempts") + 1,
              "value" => value,
            })
            raise "transient command failure" if command_context.attempt_number == 1

            current_state
          end
          expose_command :write_with_retry, retry: { maximum_attempts: 2, schedule: [0] }

          expose def snapshot
            current_state
          end
        end
        object = retrying_object.ref("object-1", store:)
        worker = object_worker(store, retrying_object)

        caller = call_object_command_async(backend, retrying_object, "object-1", [:write_with_retry, "persisted"])
        wait_for_object_activation(retrying_object, "object-1")
        run_worker_until_result(worker, caller.fetch(:queue))
        status, result = caller.fetch(:queue).pop
        caller.fetch(:thread).join

        assert_equal(:ok, status)
        assert_equal({ "committed_attempts" => 1, "value" => "persisted" }, result)
        assert_equal(2, seen_keys.length)
        assert_equal(1, seen_keys.uniq.length)
        assert_equal(result, object.snapshot)
        assert_equal(
          2,
          store.inbox_messages_for(target_kind: "object", target_type: retrying_object.object_type, target_id: "object-1").first.fetch("attempts").to_i,
        )
      ensure
        caller&.fetch(:thread)&.kill if caller&.fetch(:thread)&.alive?
      end
    end

    test "dead-lettered object mailbox heads block later messages with #{backend.name}" do
      with_durababble_store(backend, "durable_object_blocked_head") do |store|
        failing_object = Class.new(Durababble::DurableObject) do
          object_type "blocked_head_object"

          expose_command def fail_first
            raise "blocked head"
          end

          expose_command def second
            update_state({ "ran" => true })
          end
        end
        worker = object_worker(store, failing_object)

        first = failing_object.tell("object-1", :fail_first, store:)
        second = failing_object.tell("object-1", :second, store:)

        assert_equal :worked, worker.tick
        messages = store.inbox_messages_for(target_kind: "object", target_type: failing_object.object_type, target_id: "object-1")
        assert_equal [first, second], messages.map { |message| message.fetch("id") }
        assert_equal ["dead_lettered", "pending"], messages.map { |message| message.fetch("status") }
        assert_nil store.object_state(object_type: failing_object.object_type, object_id: "object-1")
        assert_equal :idle, worker.tick
      end
    end
  end

  private

  def object_worker(store, *objects)
    Durababble::Worker.new(store:, workflows: {}, objects:, worker_id: "object-worker", lease_seconds: 30, migrate: false)
  end

  def call_object_command_async(backend, object_class, object_id, command)
    method_name, *args = command
    result_queue = Queue.new
    caller = Thread.new do
      caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
      begin
        result_queue << [:ok, object_class.ref(object_id, store: caller_store).public_send(method_name, *args)]
      rescue StandardError => e
        result_queue << [:error, e]
      ensure
        caller_store.close
      end
    end
    { thread: caller, queue: result_queue }
  end

  def wait_for_object_activation(object_class, object_id, timeout: 2)
    deadline = Time.now + timeout
    loop do
      activation = store.target_activation(target_kind: "object", target_type: object_class.object_type, target_id: object_id)
      return activation if activation
      raise "object activation not created before timeout" if Time.now >= deadline

      sleep(0.01)
    end
  end

  def run_worker_until_result(worker, result_queue, timeout: 3)
    deadline = Time.now + timeout
    loop do
      return unless result_queue.empty?

      worker.tick
      raise "object command did not complete before timeout" if Time.now >= deadline

      sleep(0.01)
    end
  end
end
