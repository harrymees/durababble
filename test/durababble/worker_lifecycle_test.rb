# typed: false
# frozen_string_literal: true

require_relative "../test_helper"
require "thread"
require "stringio"
require "logger"

class DurababbleWorkerLifecycleTest < DurababbleTestCase
  class RuntimeBranchStore
    attr_reader :closed, :released

    def initialize(tick_results: [])
      @tick_results = tick_results.dup
      @closed = false
      @released = []
    end

    def migrate!; end

    def close
      @closed = true
    end

    def release_worker_leases!(worker_id:)
      @released << worker_id
    end

    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil, worker_pool: "default", excluding_workflow_ids: nil)
      result = @tick_results.shift
      raise result if result.is_a?(Exception)

      result
    end

    def claim_target_activation(worker_id:, lease_seconds:, target_kinds:, target_types:, worker_pool: "default")
      nil
    end
  end

  class ForcedUnavailableDeliveryClient
    def deliver_message(**)
      raise Durababble::Rpc::Unavailable, "forced delivery failure"
    end
  end

  class IdlePollObservedStore
    def initialize(store)
      @store = store
      @idle_polls = Queue.new
    end

    def claim_runnable_workflow(**kwargs)
      @store.claim_runnable_workflow(**kwargs).tap do |result|
        @idle_polls << true unless result
      end
    end

    def await_idle_poll(timeout:)
      @idle_polls.pop(timeout:)
    end

    def method_missing(method_name, *args, **kwargs, &block)
      return @store.public_send(method_name, *args, **kwargs, &block) if @store.respond_to?(method_name)

      super
    end

    def respond_to_missing?(method_name, include_private = false)
      @store.respond_to?(method_name, include_private) || super
    end
  end

  class SaturatedDeliveryWorker
    attr_reader :claim_attempts, :delivery_claims

    def initialize(runtime:)
      @runtime = runtime
      @claim_attempts = 0
      @delivery_claims = 0
      @slow_completed = Queue.new
      @delivery_completed = Queue.new
    end

    def claim_work(excluding_target_keys: nil)
      @claim_attempts += 1
      return if @claim_attempts > 1

      Durababble::Worker::WorkItem.new(:workflow, ["default", "workflow", "slow", "wf-1"], :slow)
    end

    def delivery_work(worker_pool:, target_kind:, target_type:, target_id:)
      @delivery_claims += 1
      Durababble::Worker::WorkItem.new(
        :delivery,
        [worker_pool, target_kind, target_type, target_id].map(&:to_s).freeze,
        { worker_pool:, target_kind:, target_type:, target_id: },
      )
    end

    def perform_work(work_item)
      if work_item.payload == :slow
        @runtime.send(
          :enqueue_delivery,
          worker_pool: "default",
          target_kind: "workflow",
          target_class: "slow",
          target_id: "wf-2",
        )
        Kernel.sleep(0.05)
        @slow_completed << true
      else
        @delivery_completed << true
      end
    end

    def slow_completed?
      !@slow_completed.empty?
    end

    def delivery_completed?
      !@delivery_completed.empty?
    end
  end

  class YieldingScheduledWorker
    attr_reader :performed

    def initialize
      @claims = [
        Durababble::Worker::WorkItem.new(:workflow, ["default", "workflow", "yielding", "first"], :first),
        Durababble::Worker::WorkItem.new(:workflow, ["default", "workflow", "yielding", "second"], :second),
      ]
      @performed = Queue.new
      @release_first = Queue.new
    end

    def claim_work(excluding_target_keys: nil)
      @claims.shift
    end

    def perform_work(work_item)
      @performed << work_item.payload
      case work_item.payload
      when :first
        @release_first.pop
      when :second
        @release_first << true
      end
    end
  end

  class AsyncLifecycleStore < RuntimeBranchStore
    def initialize
      super(tick_results: [])
    end

    def claim_runnable_workflow(**)
      nil
    end
  end

  def setup
    @durababble_backend = durababble_store_backends.first
    @durababble_schema = "#{@durababble_backend.default_schema_prefix}_worker_lifecycle_test_#{Process.pid}_#{SecureRandom.hex(4)}"
    @durababble_store = Durababble::Store.connect(database_url:, schema:)
    @durababble_store.migrate!
  end

  def teardown
    @runtime_store&.close if defined?(@runtime_store) && @runtime_store
    @durababble_store&.drop_schema!
    @durababble_store&.close
    @runtime_store = nil
    @durababble_store = nil
    @durababble_schema = nil
    @durababble_backend = nil
  end

  test "requires either a store or database url" do
    assert_raises_matching(ArgumentError, /store: or database_url/) do
      Durababble::WorkerRuntime.new(workflows: {}, worker_pool: "default")
    end
  end

  test "requires rpc address configuration" do
    store = RuntimeBranchStore.new
    assert_raises_matching(ArgumentError, /rpc_host/) do
      Durababble::WorkerRuntime.new(store:, workflows: {}, worker_pool: "default", rpc_host: nil)
    end
    assert_raises_matching(ArgumentError, /rpc_port/) do
      Durababble::WorkerRuntime.new(store:, workflows: {}, worker_pool: "default", rpc_port: nil)
    end
  end

  test "requires positive concurrency" do
    store = RuntimeBranchStore.new

    assert_raises_matching(ArgumentError, /concurrency/) do
      Durababble::WorkerRuntime.new(store:, workflows: {}, worker_pool: "default", concurrency: 0)
    end
    assert_raises_matching(ArgumentError, /concurrency/) do
      Durababble::WorkerRuntime.new(store:, workflows: {}, worker_pool: "default", concurrency: "many")
    end

    runtime = Durababble::WorkerRuntime.new(store:, workflows: {}, worker_pool: "default", concurrency: 3)
    assert_equal 3, runtime.concurrency
  end

  test "refuses to start an object-only concurrent runtime without fiber isolation" do
    object = Class.new(Durababble::DurableObject) do
      object_type "runtime_isolation_guard_object"

      define_method(:ping) { true }
      expose_command :ping
    end
    runtime = Durababble::WorkerRuntime.new(
      store: RuntimeBranchStore.new,
      workflows: {},
      objects: [object],
      worker_pool: "default",
      concurrency: 2,
      migrate: false,
    )

    error = nil
    with_isolation_level(:thread) do
      Async do
        runtime.start
      rescue Durababble::ConfigurationError => e
        error = e
      end
    end
    assert_instance_of(Durababble::ConfigurationError, error)
    assert_match(/isolation_level = :fiber/, error.message)

    assert_equal false, runtime.running?
  end

  test "requires a caller-owned Async task to start" do
    runtime = Durababble::WorkerRuntime.new(
      store: RuntimeBranchStore.new,
      workflows: {},
      worker_pool: "default",
      migrate: false,
    )

    error = assert_raises(Durababble::ConfigurationError) { runtime.start }
    assert_match(/active Async task/, error.message)
    assert_equal(false, runtime.running?)
  end

  test "is idempotent when started more than once and stopped more than once" do
    store = RuntimeBranchStore.new
    runtime = Durababble::WorkerRuntime.new(
      store:,
      workflows: {},
      worker_pool: "default",
      poll_interval: 0.01,
      migrate: false,
    )

    with_started_runtime(runtime) do
      assert_equal runtime.rpc_address, Durababble::WorkerIdentity.address_for(runtime.worker_id)
      assert_match(/\Adefault-[0-9a-f]{12}@#{Regexp.escape(runtime.rpc_address)}\z/, runtime.worker_id)
      first_task = runtime.wait(timeout: 0.05)
      assert(first_task.is_a?(Async::Task) || first_task.nil?, "expected wait to return a task or nil")
      assert_same runtime, runtime.start
      assert_equal :stopped, runtime.shutdown(timeout: 1)
      assert_equal :stopped, runtime.shutdown(timeout: 1)
      assert_nil runtime.wait

      runtime.close
      assert_equal false, store.closed
    end
  end

  test "can run inside a caller-owned Async task" do
    store = AsyncLifecycleStore.new
    runtime = Durababble::WorkerRuntime.new(
      store:,
      workflows: {},
      worker_pool: "default",
      poll_interval: 0.01,
      migrate: false,
    )

    Async do |task|
      run_task = runtime.start_async(parent: task)
      assert_instance_of(Async::Task, run_task)
      assert_equal(true, runtime.running?)
      assert_nil(runtime.wait(timeout: 0.01))
      assert_equal(:stopped, runtime.shutdown(timeout: 1))
      run_task.wait
    end

    assert_equal(false, runtime.running?)
    assert_nil(runtime.wait)
  ensure
    runtime&.shutdown(timeout: 1)
  end

  test "records lease conflicts from the polling loop without releasing leases" do
    store = RuntimeBranchStore.new(tick_results: [Durababble::LeaseConflict.new("moved")])
    runtime = Durababble::WorkerRuntime.new(
      store:,
      workflows: {},
      worker_pool: "default",
      worker_id: "runtime-branch",
      poll_interval: 0.01,
      migrate: false,
    )

    with_started_runtime(runtime) do
      runtime.wait(timeout: 1)

      assert_kind_of Durababble::LeaseConflict, runtime.last_error
      assert_equal :stopped, runtime.shutdown(timeout: 1)
      assert_empty store.released
    end
  end

  test "records unexpected polling errors without closing caller stores" do
    store = RuntimeBranchStore.new(tick_results: [RuntimeError.new("boom")])
    runtime = Durababble::WorkerRuntime.new(
      store:,
      workflows: {},
      worker_pool: "default",
      poll_interval: 0.01,
      migrate: false,
    )

    with_started_runtime(runtime) do
      eventually(timeout: 3) { assert_kind_of RuntimeError, runtime.last_error }
      runtime.close

      assert_kind_of RuntimeError, runtime.last_error
      assert_equal "boom", runtime.last_error.message
      assert_equal false, store.closed
    end
  end

  test "logs unexpected polling errors so recurring failures are not silent" do
    log_output = StringIO.new
    previous_logger = Durababble.logger
    Durababble.logger = Logger.new(log_output)
    store = RuntimeBranchStore.new(tick_results: [RuntimeError.new("boom"), RuntimeError.new("boom")])
    runtime = Durababble::WorkerRuntime.new(
      store:,
      workflows: {},
      worker_pool: "default",
      poll_interval: 0.01,
      migrate: false,
    )

    with_started_runtime(runtime) do
      eventually(timeout: 3) { assert_match(/boom/, log_output.string) }
    end
  ensure
    runtime&.close
    Durababble.logger = previous_logger
  end

  test "uses one pool-backed store for worker and rpc paths when it owns the store" do
    runtime = Durababble::WorkerRuntime.new(
      database_url:,
      schema:,
      workflows: {},
      worker_pool: "default",
      poll_interval: 0.01,
      migrate: false,
    )
    with_started_runtime(runtime) do
      rpc_server = runtime.instance_variable_get(:@rpc_server)
      rpc_server_store = rpc_server&.instance_variable_get(:@store)

      refute_nil(rpc_server_store)
      assert_same(runtime.store, rpc_server_store)
      owner = runtime.store.instance_variable_get(:@owner)
      runtime.close
      refute(owner.connection_pool.active_connection?)
    end
  ensure
    runtime&.close
  end

  test "starts a background worker and gracefully completes in-flight work during shutdown" do
    completed = Queue.new
    workflow = durababble_test_workflow("runtime-graceful") do
      test_step("finish") do |ctx|
        completed << true
        ctx.merge("done" => true)
      end
    end
    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.name => workflow },
      worker_pool: "default",
      worker_id: "runtime-a",
      poll_interval: 0.01,
      migrate: false,
    )
    with_started_runtime(runtime) do
      assert_equal true, completed.pop

      assert_equal :stopped, runtime.shutdown(timeout: 1)

      assert_equal false, runtime.running?
      assert_hash_includes(
        store.workflow(workflow_id),
        "status" => "completed",
        "result" => { "done" => true },
        "locked_by" => nil,
        "locked_until" => nil,
      )
      assert_equal ["completed"], store.steps_for(workflow_id).map { |step| step.fetch("status") }
    end
  end

  test "runs multiple workflow tasks concurrently inside one runtime" do
    active = 0
    max_active = 0
    release = false
    started = Queue.new
    workflow = durababble_test_workflow("runtime-concurrent-workflows") do
      test_step("hold") do |ctx|
        active += 1
        max_active = [max_active, active].max
        started << ctx.fetch("id")
        sleep(0.01) until release
        ctx.merge("done" => true)
      ensure
        active -= 1
      end
    end
    workflow_ids = [
      store.enqueue_workflow(name: workflow.name, input: { "id" => "a" }),
      store.enqueue_workflow(name: workflow.name, input: { "id" => "b" }),
    ]

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.name => workflow },
      worker_pool: "default",
      worker_id: "runtime-concurrent",
      concurrency: 2,
      poll_interval: 0.01,
      migrate: false,
    )
    Async do
      runtime.start
      begin
        eventually(timeout: 3) { assert_equal(["a", "b"], queue_values(started, 2).sort) }
        assert_operator(max_active, :>=, 2)
        release = true
        eventually(timeout: 3) do
          assert_equal(["completed", "completed"], workflow_ids.map { |workflow_id| store.workflow(workflow_id).fetch("status") })
        end
        assert_equal(:stopped, runtime.shutdown(timeout: 1))
      ensure
        release = true
        runtime&.shutdown(timeout: 1)
      end
    end
  end

  test "waits for active work instead of spinning when queued deliveries arrive while saturated" do
    runtime = Durababble::WorkerRuntime.new(
      store: RuntimeBranchStore.new,
      workflows: {},
      worker_pool: "default",
      concurrency: 1,
      poll_interval: 5,
      migrate: false,
    )
    worker = SaturatedDeliveryWorker.new(runtime:)
    Async do |task|
      loop_task = task.async { |async_task| runtime.send(:run_loop, async_task, worker) }

      eventually(timeout: 1) { assert(worker.slow_completed?) }
      eventually(timeout: 1) { assert(worker.delivery_completed?) }
      runtime.shutdown(timeout: 1)
      loop_task.wait
    ensure
      runtime&.shutdown(timeout: 1)
    end
  end

  test "clears each active target after concurrently scheduled child work completes" do
    runtime = Durababble::WorkerRuntime.new(
      store: RuntimeBranchStore.new,
      workflows: {},
      worker_pool: "default",
      concurrency: 2,
      poll_interval: 0.01,
      migrate: false,
    )
    worker = YieldingScheduledWorker.new
    active_targets = {}

    Async do |task|
      assert_equal(true, runtime.send(:poll_once, task, worker, active_targets))
      assert_equal(true, runtime.send(:poll_once, task, worker, active_targets))
    end

    assert_equal([:first, :second], queue_values(worker.performed, 2))
    assert_empty(active_targets, "completed child tasks should clear their own active target keys")
  end

  test "a single concurrent runtime can make workflow-to-object progress without another process" do
    object = Class.new(Durababble::DurableObject) do
      object_type "runtime_concurrent_object"

      define_method(:record) do |value|
        update_state({ "value" => value })
        { "recorded" => value }
      end
      expose_command :record
    end
    caller_store = Durababble::Store.connect(database_url:, schema:)
    def caller_store.wait_for_inbox_message(message_id, poll_interval: 0.005, timeout: 2)
      super(message_id, poll_interval:, timeout:)
    end
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "runtime-concurrent-object-call"

      define_method(:execute) do |input|
        persist_to_object(input.fetch("object_id"))
      end

      define_method(:persist_to_object) do |object_id|
        object.handle(object_id, store: caller_store).record("ok")
      end
      step :persist_to_object
    end
    workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "object_id" => "object-1" })

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.workflow_name => workflow },
      objects: [object],
      worker_pool: "default",
      concurrency: 2,
      poll_interval: 0.01,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    with_started_runtime(runtime) do
      eventually(timeout: 3) do
        assert_hash_includes(store.workflow(workflow_id), "status" => "completed", "result" => { "recorded" => "ok" })
      end
      assert_equal({ "value" => "ok" }, store.object_state(object_type: object.object_type, object_id: "object-1"))
    end
  ensure
    caller_store&.close
  end

  test "does not re-enter an in-flight workflow when a duplicate activation is claimed by the same runtime" do
    release = false
    started = Queue.new
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "runtime-duplicate-activation"

      define_method(:execute) do |input|
        hold(input)
      end

      define_method(:hold) do |input|
        started << input.fetch("id")
        sleep(0.01) until release
        { "done" => true }
      end
      step :hold

      expose_command def approve
        { "approved" => true }
      end
    end
    workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "id" => "wf" })

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.workflow_name => workflow },
      worker_pool: "default",
      worker_id: "runtime-duplicate",
      concurrency: 2,
      poll_interval: 0.01,
      migrate: false,
    )
    with_started_runtime(runtime) do
      assert_equal("wf", started.pop)
      message_id = store.enqueue_workflow_command(
        workflow_id:,
        workflow_name: workflow.workflow_name,
        method_name: "approve",
        payload: { "method" => "approve", "args" => [], "kwargs" => {} },
      )

      sleep(0.25)
      assert_equal(0, started.length)
      assert_equal(1, store.step_attempts_for(workflow_id).length)

      release = true
      eventually(timeout: 3) do
        assert_hash_includes(store.workflow(workflow_id), "status" => "completed", "result" => { "done" => true })
        assert_hash_includes(store.inbox_message(message_id), "status" => "completed", "result" => { "approved" => true })
      end
    end
  ensure
    release = true
  end

  test "does not renew an expired in-flight workflow lease by reclaiming its own active target" do
    release = false
    started = Queue.new
    workflow = durababble_test_workflow("runtime-active-lease-not-renewed") do
      test_step("hold") do |ctx|
        started << true
        sleep(0.01) until release
        ctx.merge("done" => true)
      end
    end
    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.name => workflow },
      worker_pool: "default",
      worker_id: "runtime-active-lease-not-renewed",
      concurrency: 2,
      poll_interval: 0.01,
      lease_seconds: 1,
      migrate: false,
    )
    Async do
      runtime.start
      begin
        assert_equal(true, started.pop)
        original_locked_until = parse_time(store.workflow(workflow_id).fetch("locked_until"))

        eventually(timeout: 3) { assert_operator(Time.now, :>, original_locked_until + 0.15) }
        sleep(0.15)

        current_locked_until = parse_time(store.workflow(workflow_id).fetch("locked_until"))
        assert_operator(
          current_locked_until,
          :<=,
          original_locked_until + 0.05,
          "active workflow lease should not be renewed by duplicate in-process scheduling",
        )
      ensure
        runtime&.shutdown(timeout: 0.05)
        release = true
        runtime&.wait(timeout: 1)
        runtime&.shutdown(timeout: 1)
      end
    end
  end

  test "keeps commands for one object id serialized while other runtime slots are available" do
    active = 0
    max_active = 0
    entered = Queue.new
    object = Class.new(Durababble::DurableObject) do
      object_type "runtime_serial_object"

      define_method(:append) do |value|
        active += 1
        max_active = [max_active, active].max
        entered << value
        sleep(0.02)
        values = current_state || []
        update_state(values + [value])
        values + [value]
      ensure
        active -= 1
      end
      expose_command :append
    end
    first_id = object.tell("shared", :append, 1, store:)
    second_id = object.tell("shared", :append, 2, store:)

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: {},
      objects: [object],
      worker_pool: "default",
      concurrency: 4,
      poll_interval: 0.01,
      migrate: false,
    )
    with_started_runtime(runtime) do
      eventually(timeout: 3) do
        assert_hash_includes(store.inbox_message(first_id), "status" => "completed")
        assert_hash_includes(store.inbox_message(second_id), "status" => "completed")
      end
      assert_equal([1, 2], queue_values(entered, 2))
      assert_equal(1, max_active)
      assert_equal([1, 2], store.object_state(object_type: object.object_type, object_id: "shared"))
    end
  end

  test "shutdown timeout releases every in-flight workflow lease from a concurrent runtime" do
    release = false
    started = Queue.new
    attempts = Hash.new(0)
    workflow = durababble_test_workflow("runtime-concurrent-timeout") do
      test_step("hold") do |ctx|
        attempts[ctx.fetch("id")] += 1
        started << ctx.fetch("id")
        sleep(0.01) until release
        ctx.merge("attempt" => attempts.fetch(ctx.fetch("id")))
      end
    end
    workflow_ids = [
      store.enqueue_workflow(name: workflow.name, input: { "id" => "a" }),
      store.enqueue_workflow(name: workflow.name, input: { "id" => "b" }),
    ]

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.name => workflow },
      worker_pool: "default",
      worker_id: "runtime-concurrent-timeout",
      concurrency: 2,
      poll_interval: 0.01,
      lease_seconds: 60,
      migrate: false,
    )
    Async do
      runtime.start
      begin
        eventually(timeout: 3) { assert_equal(["a", "b"], queue_values(started, 2).sort) }

        assert_equal(:timeout, runtime.shutdown(timeout: 0.05))
        workflow_ids.each do |workflow_id|
          assert_hash_includes(store.workflow(workflow_id), "status" => "pending", "locked_by" => nil, "locked_until" => nil)
          assert_equal(["running"], store.steps_for(workflow_id).map { |step| step.fetch("status") })
        end

        release = true
        runtime.wait(timeout: 1)
        recovery = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "concurrent-recovery",
          lease_seconds: 60,
          migrate: false,
        )
        assert_equal(2, recovery.run_until_idle(max_ticks: 4))
        workflow_ids.each do |workflow_id|
          assert_hash_includes(store.workflow(workflow_id), "status" => "completed", "result" => { "id" => store.workflow(workflow_id).fetch("input").fetch("id"), "attempt" => 2 })
          assert_equal(["failed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") })
        end
      ensure
        release = true
        runtime&.shutdown(timeout: 1)
      end
    end
  end

  test "stops claiming work on shutdown timeout, revokes owned leases, and lets another worker retry incomplete steps" do
    entered = Queue.new
    release_zombie = Queue.new
    attempts = Queue.new
    workflow = durababble_test_workflow("runtime-timeout") do
      test_step("maybe-stuck") do |ctx|
        attempt = attempts.length + 1
        attempts << attempt
        entered << attempt
        release_zombie.pop if attempt == 1
        ctx.merge("attempt" => attempt)
      end
    end
    workflow_id = store.enqueue_workflow(name: workflow.name, input: {})

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.name => workflow },
      worker_pool: "default",
      worker_id: "runtime-timeout",
      poll_interval: 0.01,
      lease_seconds: 60,
      migrate: false,
    )
    Async do
      runtime.start
      begin
        assert_equal(1, entered.pop)

        assert_equal(:timeout, runtime.shutdown(timeout: 0.05))

        revoked = store.workflow(workflow_id)
        assert_hash_includes(revoked, "status" => "pending", "locked_by" => nil, "locked_until" => nil)
        assert_equal("running", store.steps_for(workflow_id).first.fetch("status"))

        release_zombie << true
        runtime.wait(timeout: 1)

        recovery = Durababble::Worker.new(
          store:,
          workflows: { workflow.name => workflow },
          worker_id: "recovery",
          lease_seconds: 60,
          migrate: false,
        )
        assert_equal(1, recovery.run_until_idle(max_ticks: 2))

        row = store.workflow(workflow_id)
        assert_hash_includes(row, "status" => "completed", "result" => { "attempt" => 2 })
        assert_equal(["failed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") })
      ensure
        release_zombie << true if runtime&.running?
        runtime&.shutdown(timeout: 1)
      end
    end
  end

  test "shutdown timeout revokes object inbox and activation leases for immediate recovery" do
    entered = Queue.new
    release_zombie = Queue.new
    blocking_object = Class.new(Durababble::DurableObject) do
      object_type "runtime_timeout_object"

      define_method(:record) do
        entered << command_context.attempt_number
        release_zombie.pop if command_context.attempt_number == 1
        update_state({ "attempt" => command_context.attempt_number })
      end
      expose_command :record, retry: { maximum_attempts: 2, schedule: [0] }
    end

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: {},
      objects: [blocking_object],
      worker_pool: "default",
      poll_interval: 0.01,
      lease_seconds: 60,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    Async do
      runtime.start
      begin
        message_id = blocking_object.tell("object-1", :record, store:)
        assert_equal(1, entered.pop)
        assert_hash_includes(store.inbox_message(message_id), "status" => "running", "locked_by" => runtime.worker_id)
        assert_hash_includes(
          store.target_activation(target_kind: "object", target_type: blocking_object.object_type, target_id: "object-1"),
          "status" => "running",
          "locked_by" => runtime.worker_id,
        )

        assert_equal(:timeout, runtime.shutdown(timeout: 0.05))
        assert_hash_includes(store.inbox_message(message_id), "status" => "pending", "locked_by" => nil, "locked_until" => nil)
        assert_hash_includes(
          store.target_activation(target_kind: "object", target_type: blocking_object.object_type, target_id: "object-1"),
          "status" => "pending",
          "locked_by" => nil,
          "locked_until" => nil,
        )

        release_zombie << true
        runtime.wait(timeout: 1)

        recovery = Durababble::Worker.new(
          store:,
          workflows: {},
          objects: [blocking_object],
          worker_id: "object-recovery",
          lease_seconds: 60,
          migrate: false,
        )
        assert_equal(1, recovery.run_until_idle(max_ticks: 2))
        assert_hash_includes(store.inbox_message(message_id), "status" => "completed", "result" => { "attempt" => 2 })
        assert_equal({ "attempt" => 2 }, store.object_state(object_type: blocking_object.object_type, object_id: "object-1"))
      ensure
        release_zombie << true if runtime&.running?
        runtime&.shutdown(timeout: 1)
      end
    end
  end

  test "only claims workflow names served by this runtime's worker pool" do
    pool_workflow = durababble_test_workflow("pool-a-work") do
      test_step("finish") { |ctx| ctx.merge("pool" => "a") }
    end
    other_workflow = durababble_test_workflow("pool-b-work") do
      test_step("finish") { |ctx| ctx.merge("pool" => "b") }
    end
    pool_workflow_id = store.enqueue_workflow(name: pool_workflow.name, input: {}, worker_pool: "pool-a")
    other_workflow_id = store.enqueue_workflow(name: other_workflow.name, input: {})

    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { pool_workflow.name => pool_workflow },
      worker_pool: "pool-a",
      worker_id: "pool-a-1",
      poll_interval: 0.01,
      migrate: false,
    )
    with_started_runtime(runtime) do
      eventually(timeout: 2) { assert_equal "completed", store.workflow(pool_workflow_id).fetch("status") }
      assert_equal :stopped, runtime.shutdown(timeout: 1)

      assert_hash_includes store.workflow(pool_workflow_id), "status" => "completed"
      assert_hash_includes store.workflow(other_workflow_id), "status" => "pending", "locked_by" => nil
    end
  end

  test "wakes the active leaseholder runtime through DeliverMessage instead of waiting for the next poll" do
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "runtime-rpc-command"

      def execute(_input)
        wait_condition(timeout: 60) { @approved_by }
      end

      expose_command def approve(reason:)
        @approved_by = reason
        { "approved_by" => reason }
      end
    end
    runtime = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.workflow_name => workflow },
      worker_pool: "default",
      poll_interval: 10,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    caller_store = nil
    with_started_runtime(runtime) do
      assert_equal(runtime.rpc_address, Durababble::WorkerIdentity.address_for(runtime.worker_id))

      workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {})
      store.claim_workflow(workflow_id:, worker_id: runtime.worker_id, lease_seconds: 30)
      assert_hash_includes(
        store.workflow(workflow_id),
        "status" => "running",
        "locked_by" => runtime.worker_id,
      )

      caller_store = Durababble::Store.connect(database_url:, schema:)
      started_at = Time.now
      result = workflow.handle(workflow_id, store: caller_store).approve(reason: "operator")
      elapsed = Time.now - started_at

      assert_equal({ "approved_by" => "operator" }, result)
      assert_operator(elapsed, :<, 3)
      assert_nil(
        store.target_activation(
          target_kind: "workflow",
          target_type: workflow.workflow_name,
          target_id: workflow_id,
        ),
      )
    end
  ensure
    caller_store&.close
  end

  test "address-routed DeliverMessage wakes a non-default pool runtime" do
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "runtime-rpc-pool-command"

      def execute(_input)
        wait_condition(timeout: 60) { @approved_by }
      end

      expose_command def approve(reason:)
        @approved_by = reason
        { "approved_by" => reason }
      end
    end
    observed_runtime_store = IdlePollObservedStore.new(runtime_store)
    runtime = Durababble::WorkerRuntime.new(
      store: observed_runtime_store,
      workflows: { workflow.workflow_name => workflow },
      worker_pool: "pool-a",
      poll_interval: 10,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    with_started_runtime(runtime) do
      assert(
        observed_runtime_store.await_idle_poll(timeout: 2),
        "runtime did not enter its idle poll before command setup",
      )

      workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {}, worker_pool: "pool-a")
      store.claim_workflow(workflow_id:, worker_id: runtime.worker_id, lease_seconds: 30, worker_pool: "pool-a")
      message_id = store.enqueue_workflow_command(
        workflow_id:,
        workflow_name: workflow.workflow_name,
        method_name: "approve",
        payload: { "method" => "approve", "args" => [], "kwargs" => { reason: "operator" } },
      )

      Durababble::Rpc::Client.new(address: runtime.rpc_address).deliver_message(
        worker_pool: "default",
        target_kind: "workflow",
        target_class: workflow.workflow_name,
        target_id: workflow_id,
      )
      sleep(0.05)
      assert_hash_includes(store.inbox_message(message_id), "status" => "pending")

      Durababble::Rpc::Client.new(address: runtime.rpc_address).deliver_message(
        worker_pool: "pool-a",
        target_kind: "workflow",
        target_class: workflow.workflow_name,
        target_id: workflow_id,
      )

      eventually(timeout: 3) do
        assert_hash_includes(
          store.inbox_message(message_id),
          "status" => "completed",
          "result" => { "approved_by" => "operator" },
        )
      end
    end
  end

  test "activation fallback forwards a failed command wake to the active leaseholder" do
    workflow = Class.new(Durababble::Workflow) do
      workflow_name "runtime-rpc-forward-command"

      def execute(_input)
        wait_condition(timeout: 60) { @approved_by }
      end

      expose_command def approve(reason:)
        @approved_by = reason
        { "approved_by" => reason }
      end
    end
    runtime_a = Durababble::WorkerRuntime.new(
      store: runtime_store,
      workflows: { workflow.workflow_name => workflow },
      worker_pool: "pool-a",
      poll_interval: 10,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    runtime_b_store = Durababble::Store.connect(database_url:, schema:)
    runtime_b = Durababble::WorkerRuntime.new(
      store: runtime_b_store,
      workflows: { workflow.workflow_name => workflow },
      worker_pool: "pool-a",
      poll_interval: 0.01,
      migrate: false,
      rpc_host: "127.0.0.1",
      rpc_port: 0,
    )
    caller_store = nil
    with_started_runtime(runtime_a, runtime_b) do
      workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: {}, worker_pool: "pool-a")
      store.claim_workflow(workflow_id:, worker_id: runtime_a.worker_id, lease_seconds: 30, worker_pool: "pool-a")
      caller_store = Durababble::Store.connect(database_url:, schema:)
      caller_store.rpc_client_factory = ->(_address) { ForcedUnavailableDeliveryClient.new }
      def caller_store.wait_for_inbox_message(message_id, poll_interval: 0.01, timeout: 3)
        super
      end

      started_at = Time.now
      result = workflow.handle(workflow_id, store: caller_store).approve(reason: "operator")
      elapsed = Time.now - started_at

      assert_equal({ "approved_by" => "operator" }, result)
      assert_operator(elapsed, :<, 3)
      assert_nil(
        store.target_activation(
          target_kind: "workflow",
          target_type: workflow.workflow_name,
          target_id: workflow_id,
        ),
      )
    end
  ensure
    caller_store&.close
    runtime_b_store&.close
  end

  private

  def database_url
    backend_descriptor.database_url
  end

  def runtime_store
    @runtime_store ||= Durababble::Store.connect(database_url:, schema:)
  end

  def eventually(timeout:)
    deadline = Time.now + timeout
    loop do
      yield
      return
    rescue Minitest::Assertion
      raise if Time.now >= deadline

      sleep(0.01)
    end
  end

  def with_started_runtime(*runtimes)
    Async do
      runtimes.each(&:start)
      yield
    ensure
      runtimes.reverse_each { |runtime| runtime&.shutdown(timeout: 1) }
    end
  end

  def queue_values(queue, count)
    return [] if queue.length < count

    count.times.map { queue.pop(true) }
  end

  def parse_time(value)
    value.is_a?(Time) ? value : Time.parse(value.to_s)
  end

  def with_isolation_level(level)
    previous = ActiveSupport::IsolatedExecutionState.isolation_level
    ActiveSupport::IsolatedExecutionState.isolation_level = level
    yield
  ensure
    ActiveSupport::IsolatedExecutionState.isolation_level = previous
  end
end
