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

    def object_state(object_type:, object_id:, worker_pool: "default") = @state

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
    attr_reader :enqueued, :waits, :deliveries, :migrations

    def initialize
      @enqueued = []
      @waits = []
      @deliveries = []
      @migrations = 0
    end

    def migrate!
      @migrations += 1
      true
    end

    def enqueue_object_command(**kwargs)
      @enqueued << kwargs
      "cmd-#{@enqueued.length}"
    end

    def deliver_target_message(**kwargs)
      @deliveries << kwargs
      false
    end

    def wait_for_inbox_message(message_id, poll_interval: 0.05, timeout: 10)
      @waits << { message_id:, poll_interval:, timeout: }
      "waited:#{message_id}"
    end
  end

  class QueryRoutingClient
    attr_reader :calls

    def initialize(result: "remote-result", error: nil)
      @result = result
      @error = error
      @calls = []
    end

    def call_transient(**kwargs)
      @calls << kwargs
      raise @error if @error

      @result
    end
  end

  class QueryRoutingStore
    attr_reader :object_state_reads, :client
    attr_accessor :local_worker_id, :local_transient_handler

    def initialize(lease: nil, messages: [], state: { "value" => "local" }, client: QueryRoutingClient.new)
      @lease = lease
      @messages = messages
      @state = state
      @client = client
      @object_state_reads = 0
      @local_worker_id = nil
      @local_transient_handler = nil
    end

    def current_object_lease(_object_type, _object_id, worker_pool: "default") = @lease

    def rpc_client_factory = ->(_worker_id) { @client }

    def inbox_messages_for(**) = @messages

    def object_state(object_type:, object_id:, worker_pool: "default")
      @object_state_reads += 1
      @state
    end
  end

  class HandoffRoutingStore < QueryRoutingStore
    def initialize(leases:, clients:)
      super(lease: nil)
      @leases = leases
      @clients = clients
    end

    def current_object_lease(_object_type, _object_id, worker_pool: "default")
      @leases.length > 1 ? @leases.shift : @leases.first
    end

    def rpc_client_factory = ->(worker_id) { @clients.fetch(worker_id) }
  end

  class LeaseLossStore
    def initialize(store, worker_id:)
      @store = store
      @worker_id = worker_id
      @lease_checks = 0
    end

    def current_object_lease(object_type, object_id, worker_pool: "default")
      @lease_checks += 1
      @store.release_worker_leases!(worker_id: @worker_id) if @lease_checks == 2
      @store.current_object_lease(object_type, object_id, worker_pool:)
    end

    def method_missing(method_name, *args, **kwargs, &block)
      @store.public_send(method_name, *args, **kwargs, &block)
    end

    def respond_to_missing?(method_name, include_private = false)
      @store.respond_to?(method_name, include_private) || super
    end
  end

  class CleanCommandObject < Durababble::DurableObject
    expose_command def read_only
      "unchanged"
    end
  end

  class QueryRoutingObject < Durababble::DurableObject
    object_type "query_routing_object"

    def initialize_state
      { "value" => "initial" }
    end

    expose def value(prefix: nil)
      [prefix, current_state.fetch("value")].compact.join(":")
    end
  end

  class ReadGateObject < Durababble::DurableObject
    object_type "read_gate_object"

    def initialize_state
      { "value" => "initial" }
    end

    expose_command def set(value)
      update_state({ "value" => value })
    end

    expose_command def fail_head
      raise "blocked head"
    end

    expose def value
      current_state.fetch("value")
    end

    expose def unsafe_set_from_query(value)
      update_state({ "value" => value })
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

  class PersistedNilStateObject < Durababble::DurableObject
    object_type "persisted_nil_state_object"

    def initialize_state
      { "initialized" => true }
    end

    expose def snapshot
      current_state
    end
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

  class CloudflareCounterExample < Durababble::DurableObject
    object_type "cloudflare_counter_example"

    def initialize_state
      { "value" => 0 }
    end

    expose_command def increment(amount = 1)
      update_state("value" => current_state.fetch("value") + amount)
    end

    expose_command def decrement(amount = 1)
      update_state("value" => current_state.fetch("value") - amount)
    end

    expose def value
      current_state.fetch("value")
    end
  end

  class CloudflareRpcTargetExample < Durababble::DurableObject
    object_type "cloudflare_rpc_target_example"

    def initialize_state
      { "sessions" => {} }
    end

    expose_command def connect(session_id, metadata)
      update_state(
        "sessions" => current_state.fetch("sessions").merge(
          session_id => metadata.merge("operation_id" => command_context.idempotency_key),
        ),
      )
    end

    expose def metadata_for(session_id)
      current_state.fetch("sessions").fetch(session_id)
    end
  end

  class CloudflareBatcherExample < Durababble::DurableObject
    object_type "cloudflare_batcher_example"

    def initialize_state
      { "messages" => [], "flushes" => [] }
    end

    expose_command def add(message)
      update_state(current_state.merge("messages" => current_state.fetch("messages") + [message]))
    end

    def on_wake(payload:)
      messages = current_state.fetch("messages")
      update_state(
        "messages" => [],
        "flushes" => current_state.fetch("flushes") + [
          {
            "messages" => messages,
            "reason" => payload.fetch("reason"),
          },
        ],
      )
    end

    expose def snapshot
      current_state
    end
  end

  class CloudflareTtlExample < Durababble::DurableObject
    object_type "cloudflare_ttl_example"

    def initialize_state
      { "value" => nil, "expires_at" => nil, "expired" => false }
    end

    expose_command def put(value, expires_at:)
      update_state("value" => value, "expires_at" => expires_at, "expired" => false)
    end

    def on_wake(payload:)
      expires_at = current_state.fetch("expires_at")
      return current_state unless expires_at && payload.fetch("now") >= expires_at

      update_state("value" => nil, "expires_at" => nil, "expired" => true)
    end

    expose def snapshot
      current_state
    end
  end

  class CloudflareKvCoordinatorExample < Durababble::DurableObject
    object_type "cloudflare_kv_coordinator_example"

    def initialize_state
      { "kv" => {}, "versions" => {} }
    end

    expose_command def put(key, value)
      update_state(
        "kv" => current_state.fetch("kv").merge(key => value),
        "versions" => current_state.fetch("versions").merge(key => command_context.idempotency_key),
      )
    end

    expose def get(key)
      current_state.fetch("kv").fetch(key)
    end

    expose def version_for(key)
      current_state.fetch("versions").fetch(key)
    end
  end

  class CloudflareRoomExample < Durababble::DurableObject
    object_type "cloudflare_room_example"

    def initialize_state
      { "sessions" => {}, "messages" => [] }
    end

    expose_command def join(session_id, metadata)
      update_state(current_state.merge("sessions" => current_state.fetch("sessions").merge(session_id => metadata)))
    end

    expose_command def broadcast(body, from:)
      update_state(
        current_state.merge(
          "messages" => current_state.fetch("messages") + [
            {
              "from" => from,
              "body" => body,
              "member_count" => current_state.fetch("sessions").length,
            },
          ],
        ),
      )
    end

    expose def members
      current_state.fetch("sessions")
    end

    expose def transcript
      current_state.fetch("messages")
    end
  end

  class CloudflareReadableStreamExample < Durababble::DurableObject
    object_type "cloudflare_readable_stream_example"

    def initialize_state
      { "chunks" => [], "cursor" => 0 }
    end

    expose_command def append_chunks(chunks)
      update_state(current_state.merge("chunks" => current_state.fetch("chunks") + chunks))
    end

    expose_command def read_next(limit)
      cursor = current_state.fetch("cursor")
      next_cursor = [cursor + limit, current_state.fetch("chunks").length].min
      chunk = current_state.fetch("chunks")[cursor...next_cursor]
      update_state(current_state.merge("cursor" => next_cursor))
      chunk
    end

    expose def cursor
      current_state.fetch("cursor")
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
          worker_pool: "default",
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
    assert_equal [{ worker_pool: "default", object_type: anonymous_object.object_type, object_id: "obj-1", state: { "saved" => true } }], saved
  end

  test "memoizes nil initialized durable object state" do
    nil_state_object = Class.new(Durababble::DurableObject) do
      attr_reader :initializations

      def initialize(*args, **kwargs)
        @initializations = 0
        super
      end

      def initialize_state
        @initializations += 1
        nil
      end
    end
    object = nil_state_object.new

    assert_nil object.current_state
    assert_nil object.current_state
    assert_equal 1, object.initializations
  end

  durababble_store_backends.each do |backend|
    test "does not reinitialize persisted nil durable object state with #{backend.name}" do
      with_durababble_store(backend, "nil_object_state") do |store|
        store.save_object_state(object_type: PersistedNilStateObject.object_type, object_id: "nil-state", state: nil)

        assert_nil PersistedNilStateObject.handle("nil-state", store:).snapshot
      end
    end
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
    assert_instance_of Durababble::DurableObjectRef, CleanCommandObject.handle("clean", store: Object.new)
    assert_instance_of Durababble::DurableObjectRef, CleanCommandObject.at("clean", engine: Durababble::Engine.new(store: Object.new))
    assert_not_respond_to CleanCommandObject, :ref
    assert_raises(NoMethodError) { CleanCommandObject.tell("clean", :missing, store: Object.new) }
    assert_raises(ArgumentError) { CleanCommandObject.at("clean", store: Object.new, engine: Durababble::Engine.new(store: Object.new)) }

    unbounded = Durababble::RetryPolicy.new(maximum_attempts: nil)
    assert_nil unbounded.maximum_attempts_limit
    assert_equal 3, Durababble::RetryPolicy.new(maximum_attempts: 3).maximum_attempts_limit
  end

  test "durable object handles and tells use default and explicit engines" do
    explicit_store = AskWaitStore.new
    explicit_engine = Durababble::Engine.new(store: explicit_store)
    assert_equal("waited:cmd-1", CleanCommandObject.at("object-1", engine: explicit_engine).read_only)
    assert_equal("cmd-2", CleanCommandObject.tell("object-1", :read_only, engine: explicit_engine))
    assert_equal(["ask", "tell"], explicit_store.enqueued.map { |command| command.fetch(:message_kind) })
    assert_equal(0, explicit_store.migrations)

    default_store = AskWaitStore.new
    Durababble.default_engine = Durababble::Engine.new(store: default_store)
    assert_equal("waited:cmd-1", CleanCommandObject.at("object-2").read_only)
    assert_equal("cmd-2", CleanCommandObject.tell("object-2", :read_only))
    assert_equal(["ask", "tell"], default_store.enqueued.map { |command| command.fetch(:message_kind) })
    assert_equal(0, default_store.migrations)
  ensure
    Durababble.default_store = nil
  end

  test "waits long enough for synchronous asks to exhaust finite retry policies" do
    long_schedule_store = AskWaitStore.new
    assert_equal "waited:cmd-1", LongScheduleAskObject.handle("object-1", store: long_schedule_store).eventually
    assert_equal 3, long_schedule_store.enqueued.first.fetch(:max_attempts)
    assert_equal 34, long_schedule_store.waits.first.fetch(:timeout)

    exponential_store = AskWaitStore.new
    assert_equal "waited:cmd-1", ExponentialBackoffAskObject.handle("object-1", store: exponential_store).eventually
    assert_equal 4, exponential_store.enqueued.first.fetch(:max_attempts)
    assert_equal 28, exponential_store.waits.first.fetch(:timeout)
  end

  test "does not impose the default ask wait timeout on unbounded retry policies" do
    store = AskWaitStore.new
    assert_equal "waited:cmd-1", UnboundedAskObject.handle("object-1", store:).eventually
    assert_nil store.enqueued.first.fetch(:max_attempts)
    assert_nil store.waits.first.fetch(:timeout)
  end

  test "durable object handles use configured worker pool and idempotency defaults" do
    store = AskWaitStore.new
    handle = CleanCommandObject.handle("object-1", store:, worker_pool: "priority", idempotency_key: "default-key")

    assert_equal "waited:cmd-1", handle.read_only
    assert_equal "default-key", store.enqueued.first.fetch(:idempotency_key)
    assert_equal "priority", store.deliveries.first.fetch(:worker_pool)

    assert_equal "waited:cmd-2", handle.read_only(idempotency_key: "override-key")
    assert_equal "override-key", store.enqueued.last.fetch(:idempotency_key)
    assert_equal "priority", store.deliveries.last.fetch(:worker_pool)
  end

  test "routes exposed object queries through the active owner instead of reading caller state" do
    client = QueryRoutingClient.new(result: "owner-value")
    store = QueryRoutingStore.new(
      lease: { "worker_id" => "owner-node", "locked_until" => Time.now + 30 },
      state: { "value" => "caller-stale" },
      client:,
    )

    assert_equal "owner-value", QueryRoutingObject.handle("object-1", store:, worker_pool: "priority").value(prefix: "seen")
    assert_equal 0, store.object_state_reads
    assert_equal(
      [
        {
          worker_pool: "priority",
          class_name: QueryRoutingObject.object_type,
          object_id: "object-1",
          method: "value",
          args: { "args" => [], "kwargs" => { prefix: "seen" } },
        },
      ],
      client.calls,
    )
  end

  test "uses a local transient fast path when the current runtime owns the object" do
    client = QueryRoutingClient.new(result: "remote-value")
    store = QueryRoutingStore.new(
      lease: { "worker_id" => "owner-node", "locked_until" => Time.now + 30 },
      state: { "value" => "caller-stale" },
      client:,
    )
    local_calls = []
    store.local_worker_id = "owner-node"
    store.local_transient_handler = lambda do |request:, args:|
      local_calls << [request.class_name, request["object_id"], request["method"], args]
      "local-owner-value"
    end

    assert_equal "local-owner-value", QueryRoutingObject.handle("object-1", store:).value(prefix: "seen")
    assert_empty client.calls
    assert_equal 0, store.object_state_reads
    assert_equal [[QueryRoutingObject.object_type, "object-1", "value", { "args" => [], "kwargs" => { prefix: "seen" } }]], local_calls
  end

  test "refreshes object ownership once when a transient RPC reports a stale lease" do
    old_client = QueryRoutingClient.new(error: Durababble::WorkflowRpc::StaleLease.new("object moved"))
    new_client = QueryRoutingClient.new(result: "new-owner-value")
    store = HandoffRoutingStore.new(
      leases: [
        { "worker_id" => "old-node", "locked_until" => Time.now + 30 },
        { "worker_id" => "new-node", "locked_until" => Time.now + 30 },
      ],
      clients: {
        "old-node" => old_client,
        "new-node" => new_client,
      },
    )

    assert_equal "new-owner-value", QueryRoutingObject.handle("object-1", store:).value
    assert_equal 1, old_client.calls.length
    assert_equal 1, new_client.calls.length
  end

  test "does not fall back to persisted object state when the owner RPC is unavailable" do
    store = QueryRoutingStore.new(
      lease: { "worker_id" => "owner-node", "locked_until" => Time.now + 30 },
      state: { "value" => "caller-stale" },
      client: QueryRoutingClient.new(error: Durababble::WorkflowRpc::NodeUnavailable.new("owner-node unavailable")),
    )

    assert_raises_matching(Durababble::WorkflowRpc::NodeUnavailable, /unavailable/) do
      QueryRoutingObject.handle("object-1", store:).value
    end
    assert_equal 0, store.object_state_reads
  end

  test "rejects caller-local object queries when a mailbox head is pending" do
    store = QueryRoutingStore.new(
      lease: nil,
      messages: [
        {
          "id" => "msg-1",
          "status" => "pending",
          "message_kind" => "tell",
          "sequence" => 1,
        },
      ],
      state: { "value" => "caller-stale" },
    )

    assert_raises_matching(Durababble::ObjectReadBlocked, /pending mailbox head msg-1/) do
      QueryRoutingObject.handle("object-1", store:).value
    end
    assert_equal 0, store.object_state_reads
  end

  durababble_store_backends.each do |backend|
    test "routes exposed object reads to an active owner with #{backend.name}" do
      with_durababble_store(backend, "durable_object_read_route") do |store|
        store.save_object_state(object_type: ReadGateObject.object_type, object_id: "object-1", state: { "value" => "caller-stale" })
        store.rearm_target_activation(
          target_kind: "object",
          target_type: ReadGateObject.object_type,
          target_id: "object-1",
          ready_at: Time.now,
        )
        store.claim_target_activation(worker_id: "owner-node", lease_seconds: 30, target_kinds: ["object"], target_types: [ReadGateObject.object_type])
        client = QueryRoutingClient.new(result: "owner-local")
        store.rpc_client_factory = ->(address) do
          assert_equal("owner-node", address)
          client
        end

        assert_equal "owner-local", ReadGateObject.handle("object-1", store:).value
        assert_equal 1, client.calls.length
        assert_equal ReadGateObject.object_type, client.calls.first.fetch(:class_name)
      end
    end

    test "owner-local object transient handler validates ownership and blocks running mailbox heads with #{backend.name}" do
      with_durababble_store(backend, "durable_object_read_gate") do |store|
        store.save_object_state(object_type: ReadGateObject.object_type, object_id: "object-1", state: { "value" => "persisted" })
        message_id = store.enqueue_object_command(
          object_type: ReadGateObject.object_type,
          object_id: "object-1",
          method_name: "set",
          args: ["new"],
          kwargs: {},
        )
        store.claim_target_activation(worker_id: "owner-node", lease_seconds: 30, target_kinds: ["object"], target_types: [ReadGateObject.object_type])
        store.claim_object_command(command_id: message_id, worker_id: "owner-node", lease_seconds: 30)
        handler = Durababble::DurableObjectTransientHandler.new(store:, objects: [ReadGateObject], node_id: "owner-node")
        request = Class.new do
          attr_reader :class_name

          def initialize(class_name, object_id)
            @class_name = class_name
            @object_id = object_id
          end

          def [](key)
            case key
            when "method"
              "value"
            when "object_id"
              @object_id
            end
          end
        end.new(ReadGateObject.object_type, "object-1")

        assert_raises_matching(Durababble::ObjectReadBlocked, /running mailbox head #{message_id}/) do
          handler.call(request:, args: { "args" => [], "kwargs" => {} })
        end

        stale_handler = Durababble::DurableObjectTransientHandler.new(store:, objects: [ReadGateObject], node_id: "stale-node")
        assert_raises_matching(Durababble::WorkflowRpc::StaleLease, /stale-node no longer owns/) do
          stale_handler.call(request:, args: { "args" => [], "kwargs" => {} })
        end
      end
    end

    test "owner-local transient handler rejects lease loss after a read with #{backend.name}" do
      with_durababble_store(backend, "durable_object_read_lease_loss") do |store|
        store.save_object_state(object_type: ReadGateObject.object_type, object_id: "object-1", state: { "value" => "persisted" })
        store.rearm_target_activation(
          target_kind: "object",
          target_type: ReadGateObject.object_type,
          target_id: "object-1",
          ready_at: Time.now,
        )
        store.claim_target_activation(worker_id: "owner-node", lease_seconds: 30, target_kinds: ["object"], target_types: [ReadGateObject.object_type])
        handler = Durababble::DurableObjectTransientHandler.new(store: LeaseLossStore.new(store, worker_id: "owner-node"), objects: [ReadGateObject], node_id: "owner-node")
        request = Class.new do
          attr_reader :class_name

          def initialize(class_name, object_id)
            @class_name = class_name
            @object_id = object_id
          end

          def [](key)
            case key
            when "method"
              "value"
            when "object_id"
              @object_id
            end
          end
        end.new(ReadGateObject.object_type, "object-1")

        assert_raises_matching(Durababble::WorkflowRpc::NoActiveLease, /no active owner/) do
          handler.call(request:, args: { "args" => [], "kwargs" => {} })
        end
      end
    end

    test "dead-lettered object mailbox heads block transient reads with #{backend.name}" do
      with_durababble_store(backend, "durable_object_dead_letter_read_gate") do |store|
        worker = object_worker(store, ReadGateObject)
        ReadGateObject.tell("object-1", :fail_head, store:)
        wait_for_object_activation(ReadGateObject, "object-1")
        assert_equal :worked, worker.tick

        assert_raises_matching(Durababble::ObjectReadBlocked, /dead_lettered mailbox head/) do
          ReadGateObject.handle("object-1", store:).value
        end
      end
    end

    test "owner-local transient reads keep mutation prohibition with #{backend.name}" do
      with_durababble_store(backend, "durable_object_transient_mutation_gate") do |store|
        store.save_object_state(object_type: ReadGateObject.object_type, object_id: "object-1", state: { "value" => "persisted" })
        store.rearm_target_activation(
          target_kind: "object",
          target_type: ReadGateObject.object_type,
          target_id: "object-1",
          ready_at: Time.now,
        )
        store.claim_target_activation(worker_id: "owner-node", lease_seconds: 30, target_kinds: ["object"], target_types: [ReadGateObject.object_type])
        handler = Durababble::DurableObjectTransientHandler.new(store:, objects: [ReadGateObject], node_id: "owner-node")
        request = Class.new do
          attr_reader :class_name

          def initialize(class_name, object_id)
            @class_name = class_name
            @object_id = object_id
          end

          def [](key)
            case key
            when "method"
              "unsafe_set_from_query"
            when "object_id"
              @object_id
            end
          end
        end.new(ReadGateObject.object_type, "object-1")

        assert_raises_matching(Durababble::Error, /cannot update durable object state from an exposed query/) do
          handler.call(request:, args: { "args" => ["mutated"], "kwargs" => {} })
        end
        assert_equal({ "value" => "persisted" }, store.object_state(object_type: ReadGateObject.object_type, object_id: "object-1"))
      end
    end

    test "runs exposed object asks on a worker and leaves caller-side state untouched with #{backend.name}" do
      with_durababble_store(backend, "durable_object_api") do |store|
        worker = object_worker(store, ApiTestAccount)

        assert_not_respond_to(ApiTestAccount, :step)
        assert_equal(0, ApiTestAccount.at("acct-1", store:).balance)

        caller = call_object_command_async(backend, ApiTestAccount, "acct-1", [:credit, 500])
        wait_for_object_activation(ApiTestAccount, "acct-1")
        assert_nil(store.object_state(object_type: ApiTestAccount.object_type, object_id: "acct-1"))
        run_worker_until_result(worker, caller.fetch(:queue))

        status, value = caller.fetch(:queue).pop
        caller.fetch(:thread).join
        assert_equal(:ok, status)
        assert_equal(500, value.balance_cents)
        assert_equal(500, ApiTestAccount.at("acct-1", store:).balance)
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
        assert_equal(375, ApiTestAccount.handle("acct-2", store:).balance)

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
        counter = RetryStateTestCounter.handle("counter-1", store:)

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
        assert_equal({ "count" => 1 }, store.object_state(object_type: RetryStateTestCounter.object_type, object_id: "counter-1"))
        assert_raises_matching(Durababble::ObjectReadBlocked, /dead_lettered mailbox head/) do
          counter.count
        end
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
        object = unsafe_object.handle("object-1", store:)

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
        object = retrying_object.handle("object-1", store:)
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

    test "ports Cloudflare counter example to persisted local behavior with #{backend.name}" do
      with_durababble_store(backend, "cloudflare_counter_example") do |store|
        worker = object_worker(store, CloudflareCounterExample)
        counter = CloudflareCounterExample.at("global", store:)

        assert_equal(0, counter.value)

        CloudflareCounterExample.tell("global", :increment, 3, store:)
        CloudflareCounterExample.tell("global", :decrement, 1, store:)
        wait_for_object_activation(CloudflareCounterExample, "global")
        assert_equal(1, worker.run_until_idle)
        assert_equal(2, counter.value)

        messages = store.inbox_messages_for(target_kind: "object", target_type: CloudflareCounterExample.object_type, target_id: "global")
        assert_equal(["increment", "decrement"], messages.map { |message| message.fetch("method_name") })
        assert_equal(["completed", "completed"], messages.map { |message| message.fetch("status") })
      end
    end

    test "ports Cloudflare RpcTarget metadata example to persisted local behavior with #{backend.name}" do
      with_durababble_store(backend, "cloudflare_rpc_target_example") do |store|
        worker = object_worker(store, CloudflareRpcTargetExample)

        CloudflareRpcTargetExample.tell(
          "socket-worker",
          :connect,
          "session-1",
          { "country" => "US", "plan" => "pro" },
          store:,
        )
        wait_for_object_activation(CloudflareRpcTargetExample, "socket-worker")
        assert_equal(1, worker.run_until_idle)

        metadata = CloudflareRpcTargetExample.at("socket-worker", store:).metadata_for("session-1")
        assert_hash_includes(metadata, "country" => "US", "plan" => "pro")
        assert_match(/\Adurababble:v1:object:cloudflare_rpc_target_example:socket-worker:command:/, metadata.fetch("operation_id"))
      end
    end

    test "ports Cloudflare alarm batcher example to persisted local behavior with #{backend.name}" do
      with_durababble_store(backend, "cloudflare_batcher_example") do |store|
        worker = object_worker(store, CloudflareBatcherExample)

        CloudflareBatcherExample.tell("batcher", :add, "first", store:)
        CloudflareBatcherExample.tell("batcher", :add, "second", store:)
        wait_for_object_activation(CloudflareBatcherExample, "batcher")
        assert_equal(1, worker.run_until_idle)
        assert_equal({ "messages" => ["first", "second"], "flushes" => [] }, CloudflareBatcherExample.at("batcher", store:).snapshot)

        store.enqueue_inbox_message(
          target_kind: "object",
          target_type: CloudflareBatcherExample.object_type,
          target_id: "batcher",
          message_kind: "wake",
          payload: { "reason" => "alarm" },
        )
        wait_for_object_activation(CloudflareBatcherExample, "batcher")
        assert_equal(1, worker.run_until_idle)
        assert_equal(
          {
            "messages" => [],
            "flushes" => [{ "messages" => ["first", "second"], "reason" => "alarm" }],
          },
          CloudflareBatcherExample.at("batcher", store:).snapshot,
        )
      end
    end

    test "ports Cloudflare TTL cache example to persisted local behavior with #{backend.name}" do
      with_durababble_store(backend, "cloudflare_ttl_example") do |store|
        worker = object_worker(store, CloudflareTtlExample)

        CloudflareTtlExample.tell("cache-key", :put, "cached", expires_at: 100, store:)
        wait_for_object_activation(CloudflareTtlExample, "cache-key")
        assert_equal(1, worker.run_until_idle)

        store.enqueue_inbox_message(
          target_kind: "object",
          target_type: CloudflareTtlExample.object_type,
          target_id: "cache-key",
          message_kind: "wake",
          payload: { "now" => 99 },
        )
        wait_for_object_activation(CloudflareTtlExample, "cache-key")
        assert_equal(1, worker.run_until_idle)
        assert_equal({ "value" => "cached", "expires_at" => 100, "expired" => false }, CloudflareTtlExample.at("cache-key", store:).snapshot)

        store.enqueue_inbox_message(
          target_kind: "object",
          target_type: CloudflareTtlExample.object_type,
          target_id: "cache-key",
          message_kind: "wake",
          payload: { "now" => 101 },
        )
        wait_for_object_activation(CloudflareTtlExample, "cache-key")
        assert_equal(1, worker.run_until_idle)
        assert_equal({ "value" => nil, "expires_at" => nil, "expired" => true }, CloudflareTtlExample.at("cache-key", store:).snapshot)
      end
    end

    test "ports Cloudflare KV coordinator example to persisted local behavior with #{backend.name}" do
      with_durababble_store(backend, "cloudflare_kv_coordinator_example") do |store|
        worker = object_worker(store, CloudflareKvCoordinatorExample)

        CloudflareKvCoordinatorExample.tell("namespace", :put, "feature:enabled", true, store:, idempotency_key: "kv-put-1")
        wait_for_object_activation(CloudflareKvCoordinatorExample, "namespace")
        assert_equal(1, worker.run_until_idle)

        reopened = Durababble::Store.connect(database_url: backend.database_url, schema:)
        begin
          kv = CloudflareKvCoordinatorExample.at("namespace", store: reopened)
          assert_equal(true, kv.get("feature:enabled"))
          assert_match(/\Adurababble:v1:object:cloudflare_kv_coordinator_example:namespace:command:/, kv.version_for("feature:enabled"))
        ensure
          reopened.close
        end
      end
    end

    test "ports Cloudflare WebSocket room example to persisted local behavior with #{backend.name}" do
      with_durababble_store(backend, "cloudflare_room_example") do |store|
        worker = object_worker(store, CloudflareRoomExample)

        CloudflareRoomExample.tell("lobby", :join, "session-a", { "name" => "Ada" }, store:)
        CloudflareRoomExample.tell("lobby", :join, "session-b", { "name" => "Grace" }, store:)
        CloudflareRoomExample.tell("lobby", :broadcast, "hello", from: "session-a", store:)
        wait_for_object_activation(CloudflareRoomExample, "lobby")
        assert_equal(1, worker.run_until_idle)

        room = CloudflareRoomExample.at("lobby", store:)
        assert_equal(["session-a", "session-b"], room.members.keys)
        assert_equal([{ "from" => "session-a", "body" => "hello", "member_count" => 2 }], room.transcript)
      end
    end

    test "ports Cloudflare readable stream example to persisted local behavior with #{backend.name}" do
      with_durababble_store(backend, "cloudflare_readable_stream_example") do |store|
        worker = object_worker(store, CloudflareReadableStreamExample)

        CloudflareReadableStreamExample.tell("feed", :append_chunks, ["a", "b", "c"], store:)
        wait_for_object_activation(CloudflareReadableStreamExample, "feed")
        assert_equal(1, worker.run_until_idle)

        reader = call_object_command_async(backend, CloudflareReadableStreamExample, "feed", [:read_next, 2])
        wait_for_object_activation(CloudflareReadableStreamExample, "feed")
        run_worker_until_result(worker, reader.fetch(:queue))
        status, chunk = reader.fetch(:queue).pop
        reader.fetch(:thread).join

        assert_equal(:ok, status)
        assert_equal(["a", "b"], chunk)
        assert_equal(2, CloudflareReadableStreamExample.at("feed", store:).cursor)
      ensure
        reader&.fetch(:thread)&.kill if reader&.fetch(:thread)&.alive?
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
        result_queue << [:ok, object_class.handle(object_id, store: caller_store).public_send(method_name, *args)]
      rescue StandardError => e
        result_queue << [:error, e]
      ensure
        caller_store.close
      end
    end
    { thread: caller, queue: result_queue }
  end

  def wait_for_object_activation(object_class, object_id, timeout: 10)
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
