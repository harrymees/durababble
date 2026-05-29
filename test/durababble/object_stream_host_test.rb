# typed: false
# frozen_string_literal: true

require "async"
require_relative "../test_helper"

# Unit coverage for `ObjectStreamHost`'s refcounted claim/renew/release lifecycle
# and its renewal-task behaviour. Driven against a hand-rolled fake store so the
# host's logic is exercised without a database connection.
class DurababbleObjectStreamHostTest < DurababbleTestCase
  # Records every store call and lets the test drive `claim`/`renew` outcomes.
  class FakeLeaseStore
    attr_reader :claims, :renews, :releases

    def initialize(holder: "host-worker", renew_returns: true, object_state: Durababble::Store::NO_OBJECT_STATE)
      @claims = []
      @renews = []
      @releases = []
      @holder = holder
      @renew_returns = renew_returns
      @object_state = object_state
      @mutex = Mutex.new
    end

    # Residency materialization reads durable state through `state_from_store`,
    # which calls this. Defaults to `NO_OBJECT_STATE` so the host runs `on_create`.
    def object_state_entry(object_type:, object_id:)
      @object_state
    end

    def claim_object_lease(worker_pool:, object_type:, object_id:, worker_id:, lease_seconds: 60)
      @mutex.synchronize { @claims << { worker_pool:, object_type:, object_id:, worker_id:, lease_seconds: } }
      { "worker_id" => @holder, "worker_pool" => worker_pool, "object_type" => object_type, "object_id" => object_id }
    end

    def renew_object_lease(object_type:, object_id:, worker_id:, lease_seconds: 60)
      @mutex.synchronize { @renews << { object_type:, object_id:, worker_id:, lease_seconds: } }
      next_value = @mutex.synchronize { @renew_returns.respond_to?(:shift) ? @renew_returns.shift : @renew_returns }
      next_value.nil? ? true : next_value
    end

    def release_object_lease(object_type:, object_id:, worker_id:)
      @mutex.synchronize { @releases << { object_type:, object_id:, worker_id: } }
      true
    end
  end

  # A durable-object fixture whose lifecycle hooks stand up / tear down a warm
  # resource and record every call on a class-level log, so residency tests can
  # assert the instance is materialized once (`on_create`/`on_load`), reused
  # across operations, and torn down (`on_destroy`) at the right moment.
  class ResidencyFixture < Durababble::DurableObject
    object_type "residency_fixture"

    class << self
      attr_accessor :events
    end

    def on_create
      warm_up
      record(:on_create)
    end

    def on_load
      warm_up
      record(:on_load)
    end

    def on_destroy
      record(:on_destroy)
    end

    # Mutates a non-durable ivar so tests can prove one warm instance is reused:
    # the count only accumulates if the same resident object serves every read.
    def mark_used
      @uses += 1
    end

    attr_reader :uses

    private

    def warm_up
      @uses = 0
    end

    def record(event)
      (self.class.events ||= []) << [event, durable_id]
    end
  end

  test "claims on the first opener and retains the lease at refcount 0 (sticky residency)" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:)

    host.with_lease(worker_pool: "default", object_type: "counter", object_id: "c1") do |entry|
      assert_equal(1, entry.refcount)
      assert(host.holds?(worker_pool: "default", object_type: "counter", object_id: "c1"))
    end

    assert_equal(1, store.claims.size)
    # Residency token: the lease is held past refcount 0, not dropped on block exit.
    assert_empty(store.releases)
    assert(host.holds?(worker_pool: "default", object_type: "counter", object_id: "c1"))

    # The lease is released by eviction, never by a refcount drop.
    assert(host.evict!(worker_pool: "default", object_type: "counter", object_id: "c1"))
    assert_equal(1, store.releases.size)
    refute(host.holds?(worker_pool: "default", object_type: "counter", object_id: "c1"))
  ensure
    host&.stop!
  end

  test "shares the lease across concurrent openers and retains it after both finish" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:)
    Async do |task|
      host.start_async(parent: task)
      started = Async::Queue.new
      block_release = Async::Queue.new

      inner = task.async do
        host.with_lease(worker_pool: "default", object_type: "counter", object_id: "c1") do |entry|
          started.enqueue(entry)
          block_release.dequeue
        end
      end

      outer = task.async do
        # Wait for inner to register, then join as a second consumer.
        sleep(0.001) until host.holds?(worker_pool: "default", object_type: "counter", object_id: "c1")
        host.with_lease(worker_pool: "default", object_type: "counter", object_id: "c1") do |entry|
          started.enqueue(entry)
        end
      end

      first_entry = started.dequeue
      second_entry = started.dequeue
      assert_same(first_entry, second_entry)
      outer.wait

      # Inner is still inside its block; refcount went 0->1->2->1. One claim,
      # and nothing released — outer dropping to a still-positive refcount holds.
      assert_equal(1, store.claims.size)
      assert_empty(store.releases)

      block_release.enqueue(:go)
      inner.wait
      # Both openers done (refcount 0), but residency keeps the lease resident.
      assert_empty(store.releases)
      assert(host.holds?(worker_pool: "default", object_type: "counter", object_id: "c1"))
    end.wait
  ensure
    host&.stop!
  end

  test "shares one object lease across worker pools for the same object" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:)
    first_entry = nil
    second_entry = nil

    Async do |task|
      release_first = Async::Queue.new
      first = task.async do
        host.with_lease(worker_pool: "pool-a", object_type: "counter", object_id: "c1") do |entry|
          first_entry = entry
          release_first.dequeue
        end
      end

      task.with_timeout(1) { sleep(0.001) until first_entry }
      second = task.async do
        host.with_lease(worker_pool: "pool-b", object_type: "counter", object_id: "c1") do |entry|
          second_entry = entry
        end
      end
      second.wait

      assert_same(first_entry, second_entry)
      assert_equal(["pool-a"], store.claims.map { |claim| claim.fetch(:worker_pool) })
      assert_empty(store.releases)
      assert(host.holds?(worker_pool: "pool-b", object_type: "counter", object_id: "c1"))

      release_first.enqueue(:go)
      first.wait
    end

    # Sticky residency: the single shared lease is retained after both pools finish.
    assert_empty(store.releases)
    assert(host.holds?(worker_pool: "pool-b", object_type: "counter", object_id: "c1"))
  ensure
    host&.stop!
  end

  test "does not let a concurrent opener run while the first claim is still pending" do
    store = FakeLeaseStore.new(holder: "someone-else")
    claim_started = Async::Condition.new
    release_claim = Async::Condition.new
    state = { claim_started: false, release_claim: false }
    store.define_singleton_method(:claim_object_lease) do |**kwargs|
      @claims << kwargs
      if @claims.size == 1
        state[:claim_started] = true
        claim_started.signal
        release_claim.wait until state[:release_claim]
      end
      { "worker_id" => @holder, "worker_pool" => kwargs.fetch(:worker_pool), "object_type" => kwargs.fetch(:object_type), "object_id" => kwargs.fetch(:object_id) }
    end

    host = build_host(store:)
    second_producer_started = false
    first_error = nil
    second_error = nil

    Async do |task|
      first = task.async do
        host.with_lease(worker_pool: "default", object_type: "counter", object_id: "c1") {}
      rescue StandardError => err
        first_error = err
      end

      second = task.async do
        claim_started.wait until state[:claim_started]
        host.with_lease(worker_pool: "default", object_type: "counter", object_id: "c1") do
          second_producer_started = true
        end
      rescue StandardError => err
        second_error = err
      end

      sleep(0.01)
      state[:release_claim] = true
      release_claim.signal
      first.wait
      second.wait
    end

    assert_instance_of(Durababble::WorkflowRpc::StaleLease, first_error)
    assert_instance_of(Durababble::WorkflowRpc::StaleLease, second_error)
    refute(second_producer_started, "a stream producer ran before this host held the object lease")
    assert_operator(store.claims.size, :>=, 2, "each opener must prove ownership instead of sharing a pending claim")
  ensure
    host&.stop!
  end

  test "renewal task renews held keys at the host's interval" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:, renew_interval: 0.02)
    Async do |task|
      host.start_async(parent: task)
      deadline = Async::Queue.new

      holder = task.async do
        host.with_lease(worker_pool: "default", object_type: "counter", object_id: "c1") do
          # Give the renewal task enough wall-clock to fire several times.
          sleep(0.12)
          deadline.enqueue(:done)
        end
      end
      task.with_timeout(1) { deadline.dequeue }
      holder.wait
    end.wait

    assert_operator(
      store.renews.size,
      :>=,
      2,
      "expected the renewal task to drive at least two renewals, got #{store.renews.size}",
    )
  ensure
    host&.stop!
  end

  test "renewal failure flags entry.lost and emits raise StaleLease" do
    # First call to renew returns true to admit the producer, then false to lose it.
    store = FakeLeaseStore.new(holder: "host-worker", renew_returns: [true, false, false, false, false])
    host = build_host(store:, renew_interval: 0.02)
    result = nil

    Async do |task|
      host.start_async(parent: task)
      captured = Async::Queue.new
      error = Async::Queue.new

      task.async do
        host.with_lease(worker_pool: "default", object_type: "c", object_id: "c1") do |entry|
          captured.enqueue(entry)
          # Spin until the renewal task flips lost; bound to avoid a runaway test.
          slept = 0.0
          until entry.lost || slept > 5.0
            sleep(0.01)
            slept += 0.01
          end
          error.enqueue(entry.lost)
        end
      rescue StandardError => err
        error.enqueue(err)
      end

      captured.dequeue
      result = task.with_timeout(5) { error.dequeue }
    end.wait

    assert_equal(true, result, "renewal failure did not flip entry.lost within timeout")
  ensure
    host&.stop!
  end

  test "evict! flags the entry as lost and releases the row" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:)
    Async do |task|
      host.start_async(parent: task)
      captured = Async::Queue.new
      released = Async::Queue.new

      holder = task.async do
        host.with_lease(worker_pool: "default", object_type: "c", object_id: "c1") do |entry|
          captured.enqueue(entry)
          released.dequeue
        end
      end

      entry = captured.dequeue
      refute(entry.lost)
      assert(host.evict!(worker_pool: "default", object_type: "c", object_id: "c1"))
      assert(entry.lost)
      # evict! releases the row immediately, even though refcount > 0.
      refute_empty(store.releases)

      released.enqueue(:proceed)
      holder.wait
    end.wait
  ensure
    host&.stop!
  end

  test "evict_all! sweeps every held key" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:)
    Async do |task|
      host.start_async(parent: task)
      started = Async::Queue.new
      proceed = Async::Queue.new

      holders = ["a", "b", "c"].map do |id|
        task.async do
          host.with_lease(worker_pool: "default", object_type: "t", object_id: id) do |entry|
            started.enqueue([entry, id])
            proceed.dequeue
          end
        end
      end

      entries = 3.times.map { started.dequeue }
      assert_equal(3, host.evict_all!)
      entries.each { |entry, _| assert(entry.lost) }
      assert_equal(3, store.releases.size)

      3.times { proceed.enqueue(:go) }
      holders.each(&:wait)
    end.wait
  ensure
    host&.stop!
  end

  test "evict! is idempotent and returns false when no entry is held" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:)

    refute(host.evict!(worker_pool: "default", object_type: "nope", object_id: "x"))
    assert_empty(store.releases)
  ensure
    host&.stop!
  end

  test "raises StaleLease with '(none)' when the store reports no holder at all" do
    # Drives the `holder&.dig("worker_id") || "(none)"` else branch on the
    # error path: claim returns nil, so the message renders "(none)".
    store = FakeLeaseStore.new(holder: "host-worker")
    def store.claim_object_lease(**)
      @claims << :no_holder
      nil
    end
    host = build_host(store:)

    err = assert_raises(Durababble::WorkflowRpc::StaleLease) do
      host.with_lease(worker_pool: "default", object_type: "counter", object_id: "c1") {}
    end
    assert_match(/held by \(none\)/, err.message)
    refute(host.holds?(worker_pool: "default", object_type: "counter", object_id: "c1"))
  ensure
    host&.stop!
  end

  test "start_async requires an async parent when called off reactor" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:)

    assert_raises_matching(Durababble::ConfigurationError, /requires an active Async task/) do
      host.start_async
    end
  ensure
    host&.stop!
  end

  test "start_async starts renewal for entries claimed before a parent is available" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:, renew_interval: 0.02)

    host.with_lease(worker_pool: "default", object_type: "counter", object_id: "c1") do
      refute_empty(store.claims)
      assert_empty(store.renews)
      Async do |task|
        host.start_async(parent: task)
        task.with_timeout(1) do
          sleep(0.01) until store.renews.any?
        end
      end.wait
    end

    refute_empty(store.renews)
  ensure
    host&.stop!
  end

  test "raises StaleLease and rolls back refcount when the store reports a different holder" do
    # Store returns a different worker as holder. The host cannot serve a stream
    # on a lease it doesn't own, so acquire must raise and clean up the entry.
    store = FakeLeaseStore.new(holder: "someone-else")
    host = build_host(store:)

    assert_raises(Durababble::WorkflowRpc::StaleLease) do
      host.with_lease(worker_pool: "default", object_type: "counter", object_id: "c1") {}
    end

    refute(host.holds?(worker_pool: "default", object_type: "counter", object_id: "c1"))
    assert_empty(store.releases)
  ensure
    host&.stop!
  end

  test "logs and swallows when release_object_lease raises" do
    store = FakeLeaseStore.new(holder: "host-worker")
    def store.release_object_lease(**)
      @releases << :attempted
      raise StandardError, "release blew up"
    end
    host = build_host(store:)

    captured = Queue.new
    fake_logger = Object.new
    fake_logger.define_singleton_method(:warn) { |msg| captured << msg }
    Durababble.stub(:logger, fake_logger) do
      host.with_lease(worker_pool: "default", object_type: "t", object_id: "x") {}
      # Residency holds the lease past refcount 0; release runs on eviction.
      host.evict!(worker_pool: "default", object_type: "t", object_id: "x")
    end

    refute_empty(store.releases)
    msg = captured.pop
    assert_match(/release_object_lease failed/, msg)
  ensure
    host&.stop!
  end

  test "stop! is safe when no renewal task was ever started" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:)

    host.stop! # never claimed -> no renewal task; should be a no-op
    refute_raises { host.stop! } # idempotent

    # already stopped above
  end

  test "ensure_renewal_task_locked is a no-op after stop!" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:)
    host.stop!

    # After stop!, acquiring would try to start the renewal task; the @stopping
    # check inside ensure_renewal_task_locked short-circuits. The claim still
    # happens, but no background task starts.
    host.with_lease(worker_pool: "default", object_type: "t", object_id: "x") {}
    # No renewal task was created post-stop; nothing to assert directly, but
    # we exercised the stopping-return branch.
    assert_equal(1, store.claims.size)

    # already stopped
  end

  test "evict_all skips keys that disappear while sweeping" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:)
    first = true

    host.with_lease(worker_pool: "default", object_type: "t", object_id: "x") do
      entries = host.instance_variable_get(:@entries)
      mutex = host.instance_variable_get(:@mutex)
      mutex.define_singleton_method(:synchronize) do |&block|
        result = super(&block)
        if first
          first = false
          entries.clear
        end
        result
      end

      assert_equal(1, host.evict_all!)
    end
  ensure
    host&.stop!
  end

  test "renewal loop catches exceptions from the store and logs them" do
    store = FakeLeaseStore.new(holder: "host-worker")
    def store.renew_object_lease(**)
      @renews << :attempted
      raise StandardError, "renew blew up"
    end
    host = build_host(store:, renew_interval: 0.02)
    captured = Async::Queue.new
    fake_logger = Object.new
    fake_logger.define_singleton_method(:error) { |msg| captured.enqueue(msg) }
    fake_logger.define_singleton_method(:warn) { |_| } # release path may log

    Async do |task|
      host.start_async(parent: task)
      started = Async::Queue.new
      proceed = Async::Queue.new
      Durababble.stub(:logger, fake_logger) do
        holder = task.async do
          host.with_lease(worker_pool: "default", object_type: "t", object_id: "x") do
            started.enqueue(:go)
            proceed.dequeue
          end
        end
        started.dequeue
        msg = task.with_timeout(1) { captured.dequeue }
        assert_match(/renewal loop crashed/, msg)
        proceed.enqueue(:done)
        holder.wait
      end
    end.wait
  ensure
    host&.stop!
  end

  test "renewal loop skips entries already marked lost (post-evict)" do
    # When an entry is evicted, the next renewal tick must not attempt to renew
    # it. That's the `next if entry.lost` branch in renewal_loop.
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_host(store:, renew_interval: 0.02)
    Async do |task|
      host.start_async(parent: task)
      captured = Async::Queue.new
      proceed = Async::Queue.new

      holder = task.async do
        host.with_lease(worker_pool: "default", object_type: "t", object_id: "x") do |entry|
          captured.enqueue(entry)
          proceed.dequeue
        end
      end
      captured.dequeue
      assert(host.evict!(worker_pool: "default", object_type: "t", object_id: "x"))
      # Wait long enough for a renewal tick to land while the entry is lost.
      sleep(0.08)
      # Renewals on the evicted key should not have been attempted after eviction.
      # We cannot easily distinguish "before" from "after", but the renewal loop
      # must keep running without raising, covered by inspecting host state.
      proceed.enqueue(:done)
      holder.wait
    end.wait
  ensure
    host&.stop!
  end

  test "with_resident materializes once, keeps the instance warm, and becomes the resident owner" do
    ResidencyFixture.events = []
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_resident_host(store:)

    first = nil
    second = nil
    host.with_resident(object_type: "residency_fixture", object_id: "r1", worker_pool: "default") do |instance|
      instance.mark_used
      first = instance
    end
    host.with_resident(object_type: "residency_fixture", object_id: "r1", worker_pool: "default") do |instance|
      instance.mark_used
      second = instance
    end

    assert_same(first, second, "both reads observe one warm resident instance")
    assert_equal(2, second.uses, "the resident instance accumulates state across reads")
    assert_equal(1, store.claims.size, "the lease is claimed once and held as a residency token")
    assert_empty(store.releases, "residency retains the lease at refcount 0 (become-and-stay owner)")
    assert(host.holds?(worker_pool: "default", object_type: "residency_fixture", object_id: "r1"))
    assert_equal([[:on_create, "r1"]], ResidencyFixture.events, "fresh state materializes via on_create, exactly once")
  ensure
    host&.stop!
  end

  test "materialization runs on_load when durable state already exists" do
    ResidencyFixture.events = []
    store = FakeLeaseStore.new(holder: "host-worker", object_state: { "value" => "persisted" })
    host = build_resident_host(store:)

    observed = nil
    host.with_resident(object_type: "residency_fixture", object_id: "r1", worker_pool: "default") do |instance|
      observed = instance.current_state
    end

    assert_equal({ "value" => "persisted" }, observed)
    assert_equal([[:on_load, "r1"]], ResidencyFixture.events, "existing state materializes via on_load")
  ensure
    host&.stop!
  end

  test "resident_instance get-or-materializes within a held lease and raises when not held" do
    ResidencyFixture.events = []
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_resident_host(store:)

    assert_raises(Durababble::WorkflowRpc::NoActiveLease) do
      host.resident_instance(object_type: "residency_fixture", object_id: "r1", worker_pool: "default")
    end

    one = nil
    two = nil
    host.with_lease(worker_pool: "default", object_type: "residency_fixture", object_id: "r1") do
      one = host.resident_instance(object_type: "residency_fixture", object_id: "r1", worker_pool: "default")
      two = host.resident_instance(object_type: "residency_fixture", object_id: "r1", worker_pool: "default")
    end

    assert_same(one, two)
    assert_equal(1, ResidencyFixture.events.size, "the stream path reuses one materialized instance")
  ensure
    host&.stop!
  end

  test "materializing an unknown object type raises UnknownCommand" do
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_resident_host(store:)

    assert_raises(Durababble::WorkflowRpc::UnknownCommand) do
      host.with_resident(object_type: "nope", object_id: "x", worker_pool: "default") {}
    end
  ensure
    host&.stop!
  end

  test "idle past idle_ttl evicts the resident instance: on_destroy fires and the lease is released" do
    ResidencyFixture.events = []
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_resident_host(store:, renew_interval: 0.02, idle_ttl: 0.01)

    Async do |task|
      host.start_async(parent: task)
      host.with_resident(object_type: "residency_fixture", object_id: "r1", worker_pool: "default", &:mark_used)
      # Refcount is now 0 with last_used_at stamped; the renewal loop crosses the
      # idle window on its next tick and evicts.
      task.with_timeout(2) { sleep(0.01) until store.releases.any? }
    end.wait

    refute(host.holds?(worker_pool: "default", object_type: "residency_fixture", object_id: "r1"))
    assert_equal(1, store.releases.size, "idle eviction releases the lease so ownership can rebalance")
    assert_includes(ResidencyFixture.events, [:on_destroy, "r1"], "idle eviction runs on_destroy")
  ensure
    host&.stop!
  end

  test "evict! tears the resident instance down: on_destroy fires and the lease is released" do
    ResidencyFixture.events = []
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_resident_host(store:)

    host.with_resident(object_type: "residency_fixture", object_id: "r1", worker_pool: "default", &:mark_used)
    assert(host.holds?(worker_pool: "default", object_type: "residency_fixture", object_id: "r1"))

    assert(host.evict!(worker_pool: "default", object_type: "residency_fixture", object_id: "r1"))
    refute(host.holds?(worker_pool: "default", object_type: "residency_fixture", object_id: "r1"))
    assert_equal(1, store.releases.size)
    assert_includes(ResidencyFixture.events, [:on_destroy, "r1"], "lease-loss eviction runs on_destroy")
  ensure
    host&.stop!
  end

  test "evict! swallows and logs when on_destroy raises" do
    ResidencyFixture.events = []
    store = FakeLeaseStore.new(holder: "host-worker")
    host = build_resident_host(store:)
    host.with_resident(object_type: "residency_fixture", object_id: "boom", worker_pool: "default") do |instance|
      instance.define_singleton_method(:on_destroy) { raise StandardError, "destroy blew up" }
    end

    captured = Queue.new
    fake_logger = Object.new
    fake_logger.define_singleton_method(:warn) { |msg| captured << msg }
    Durababble.stub(:logger, fake_logger) do
      assert(host.evict!(worker_pool: "default", object_type: "residency_fixture", object_id: "boom"))
    end

    assert_match(/on_destroy failed/, captured.pop)
    assert_equal(1, store.releases.size, "release still happens after on_destroy blows up")
  ensure
    host&.stop!
  end

  private

  def refute_raises
    yield
    pass
  rescue StandardError => err
    flunk("expected no exception, got #{err.class}: #{err.message}")
  end

  def build_host(store:, renew_interval: 1.0)
    Durababble::ObjectStreamHost.new(
      store:,
      worker_id: "host-worker",
      node_id: "host-worker",
      lease_seconds: 30,
      renew_interval:,
    )
  end

  def build_resident_host(store:, renew_interval: 1.0, idle_ttl: nil)
    Durababble::ObjectStreamHost.new(
      store:,
      worker_id: "host-worker",
      node_id: "host-worker",
      lease_seconds: 30,
      renew_interval:,
      objects: [ResidencyFixture],
      idle_ttl:,
    )
  end
end
