# typed: true
# frozen_string_literal: true

require "durababble"
require "durababble/store/sqlite"
require_relative "core"
require_relative "store_inspection"

module Durababble
  module Deterministic
    # The store DST runs against. It is a *real* SqliteStore — the same SQL
    # orchestration the production MySQL/Postgres adapters run, proven by the
    # backend conformance suite — wrapped so it behaves deterministically under
    # the virtual scheduler:
    #
    #   * #current_time returns the scheduler's integer tick (fed to dura_now()),
    #     so every created_at / lease deadline is a virtual tick, not wall time.
    #   * #timestamp_or_nil collapses any Time (the `now: Time.now` defaults that
    #     pepper the SQL adapters) to the current tick, and passes integers
    #     through, so lease math compares ticks to ticks.
    #   * #seconds_scale is 1, so INTERVAL n SECOND renders as `+ n` ticks.
    #   * #generate_uuid draws monotonic, lexically-sortable ids so a seed
    #     replays byte-identically.
    #   * #with_fence and #wait_for_inbox_message never sleep wall-clock — under a
    #     single-threaded virtual clock a polling loop would spin forever.
    #
    # On top of that it emits the same trace events VirtualYugabyte used to (now
    # under the actor name "store"), implements the StoreInspection read
    # interface the invariant harness consumes, and reports the same #summary
    # shape. It replaces VirtualYugabyte wholesale: instead of a hand-written
    # model of the store contract, DST now exercises the contract itself.
    class DeterministicSqliteStore < SqliteStore
      include StoreInspection

      #: untyped
      attr_reader :scheduler, :fault_plan

      MIGRATED_TEMPLATE_MUTEX = Mutex.new
      MIGRATED_TEMPLATES = {}
      CONNECTION_NAME_MUTEX = Mutex.new

      class << self
        #: (scheduler: untyped, ?fault_plan: untyped, ?schema: String) -> DeterministicSqliteStore
        def build(scheduler:, fault_plan: nil, schema: "durababble")
          connection_name = next_connection_name
          connection_class = Class.new(ActiveRecord::Base) do
            self.abstract_class = true
            self.connection_class = true
          end
          connection_class.instance_variable_set(Store::GENERATED_CONNECTION_CONST_IVAR, connection_name)
          Store::GENERATED_CONNECTION_CLASSES[connection_name] = connection_class
          Durababble.const_set(connection_name, connection_class)
          begin
            connection_class.establish_connection(adapter: "sqlite3", database: ":memory:", pool: 1)
            connection_pool = connection_class.connection_pool
            disable_query_cache_dirtying!(connection_pool)
            store = new(connection_pool, scheduler:, fault_plan:, schema:, owner: connection_class)
            store.send(:load_migrated_template!, migrated_template(schema))
            store
          rescue StandardError
            Store.send(:remove_active_record_class_const, connection_class)
            raise
          end
        end

        private

        #: () -> String
        def next_connection_name
          CONNECTION_NAME_MUTEX.synchronize do
            @connection_name_sequence = @connection_name_sequence.to_i + 1
            "DeterministicSqliteStoreConnection#{Process.pid}#{object_id}#{@connection_name_sequence}"
          end
        end

        # Query caching is never enabled for the private in-memory DST store; clearing
        # every ActiveRecord pool on each write only adds harness overhead.
        #: (untyped) -> void
        def disable_query_cache_dirtying!(connection_pool)
          connection_pool.define_singleton_method(:dirties_query_cache) { false }
        end

        #: (String) -> SqliteStore
        def migrated_template(schema)
          MIGRATED_TEMPLATE_MUTEX.synchronize do
            MIGRATED_TEMPLATES[schema] ||= begin
              template = SqliteStore.build_in_memory(schema:)
              template.migrate!
            end
          end
        end
      end

      #: (ActiveRecord::ConnectionAdapters::ConnectionPool, scheduler: untyped, ?fault_plan: untyped, schema: String, ?owner: Object?) -> void
      def initialize(connection_pool, scheduler:, schema:, fault_plan: nil, owner: nil)
        @scheduler = scheduler
        @fault_plan = fault_plan || FaultPlan.new(scheduler:)
        @side_effects = 0
        @object_wakes_delivered = 0
        @uuid_seq = 0
        @injected_workflows = {} #: Hash[String, Hash[String, Object?]]
        @injected_steps = {} #: Hash[String, Hash[Integer, Hash[String, Object?]]]
        @injected_attempts = {} #: Hash[String, Array[Hash[String, Object?]]]
        @injected_waits = {} #: Hash[String, Hash[String, Object?]]
        @injected_outbox = {} #: Hash[String, Hash[String, Object?]]
        @injected_fences = [] #: Array[Hash[String, Object?]]
        @injected_inbox = {} #: Hash[String, Hash[String, Object?]]
        @injected_target_activations = [] #: Array[Hash[String, Object?]]
        @write_crash_percent = 0
        @txn_depth = 0
        @crashes_armed = false
        super(connection_pool, schema:, owner:)
      end

      # --- Seed-driven crash-after-write fuzz mode ---------------------------
      # Off unless a scenario opts in. When enabled, durable writes performed
      # inside a #crashable block draw from the seeded RNG and may raise
      # InjectedCrash *after* the write lands — modelling a worker that dies
      # between two state transitions. The window depends on transaction depth:
      #
      #   * :after_statement — an autocommitted write (depth 0); the row is durable.
      #   * :after_commit    — the outermost transaction just committed; all its
      #                        writes are durable.
      #   * :mid_transaction — a write inside an open transaction; the raise
      #                        propagates out, so ActiveRecord ROLLBACKs every
      #                        write in that transaction (nothing lands).
      #
      # This is the systematic version of the hand-placed engine crash points:
      # it exercises every inter-write window without foreknowledge, so a
      # non-atomic write (e.g. the original step-failure bug) gets caught when a
      # :mid_transaction crash that should roll back both writes instead strands
      # a half-applied state.

      #: (percent: Integer) -> void
      def enable_write_crashes!(percent:)
        @write_crash_percent = percent
      end

      # Arms crash injection for the duration of the block. Callers (the sim
      # worker around #resume) must rescue InjectedCrash — the virtual scheduler
      # does not, so an unguarded crash would abort the whole run. Setup writes
      # (enqueue, timer wakes) run unarmed and never crash.
      #: [T] () { () -> T } -> T
      def crashable(&block)
        previous = @crashes_armed
        @crashes_armed = true
        block.call
      ensure
        @crashes_armed = previous
      end

      # Routes SecureRandom.uuid through the store's monotonic id seam for the
      # duration of the block, then restores it. The base store stamps several
      # ids (workflow, wait, step-attempt, inbox) with SecureRandom.uuid rather
      # than the overridable generate_uuid seam; under simulation those random
      # ids leak into trace events (workflow_claimed id:, wait_completed
      # wait_id:) and break run-to-run digest determinism. Drawing them from the
      # same monotonic counter makes a seed replay byte-identical and keeps id
      # order equal to insertion order. The scheduler is single-threaded, so a
      # process-global stub is safe for the length of one run.
      #: [T] () { () -> T } -> T
      def with_deterministic_uuids(&block)
        singleton_class = SecureRandom.singleton_class
        original = SecureRandom.method(:uuid)
        restore_singleton_uuid = singleton_class.public_method_defined?(:uuid, false)
        store = self
        singleton_class.__send__(:remove_method, :uuid) if restore_singleton_uuid
        SecureRandom.define_singleton_method(:uuid) { store.send(:generate_uuid) }
        block.call
      ensure
        if singleton_class&.public_method_defined?(:uuid, false)
          singleton_class.__send__(:remove_method, :uuid)
        end
        SecureRandom.define_singleton_method(:uuid, original) if restore_singleton_uuid
      end

      # --- Deterministic clock / identity seams ------------------------------

      #: () -> Integer
      def current_time = scheduler.time

      # --- Store contract, wrapped with trace + fault hooks ------------------

      #: (name: String, input: Object?, ?id: String?, ?worker_pool: String) -> String
      def enqueue_workflow(name:, input:, id: nil, worker_pool: "default")
        id = super
        trace_event("enqueue_workflow", id:, name:)
        id
      end

      #: (worker_id: String, lease_seconds: Integer, ?workflow_names: Array[String]?, ?worker_pool: String) -> Object?
      def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil, worker_pool: "default")
        claimed = super
        trace_event("workflow_claimed", id: claimed.fetch("id"), worker: worker_id) if claimed
        claimed
      end

      #: (workflow_id: String, worker_id: String, lease_seconds: Integer, ?worker_pool: String) -> Object?
      def claim_workflow(workflow_id:, worker_id:, lease_seconds:, worker_pool: "default")
        # Re-affirming a lease we already hold is not a fresh claim; mirror
        # VirtualYugabyte and only trace the pending/expired -> running edge so
        # the per-seed workflow_claimed count stays stable.
        already_owned = execute_store_query(:claim_workflow_already_owned, [workflow_id, worker_pool, worker_id]).first
        claimed = super
        trace_event("workflow_claimed", id: workflow_id, worker: worker_id) if claimed && !already_owned
        claimed
      end

      #: (workflow_id: String, worker_id: String, lease_seconds: Integer) -> Object?
      def heartbeat(workflow_id:, worker_id:, lease_seconds:)
        result = super
        trace_event("heartbeat", id: workflow_id, worker: worker_id) if affected?(result)
        result
      end

      #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, worker_id: String, lease_seconds: Integer, cursor: Object?) -> Object?
      def heartbeat_step(workflow_id:, worker_id:, lease_seconds:, cursor:, command_id: nil, position: nil)
        renewed = super
        trace_event("step_heartbeat", id: workflow_id, command_id: normalize_command_id(command_id, position), worker: worker_id, cursor:) if renewed
        renewed
      end

      #: (?now: Object?) -> Integer
      def steal_expired_leases!(now: nil)
        count = super(now: now.nil? ? scheduler.time : now)
        trace_event("steal_expired", count:) if count.positive?
        count
      end

      #: (String, result: Object?, ?worker_id: String?) -> Object
      def complete_workflow(workflow_id, result:, worker_id: nil)
        out = super
        trace_event("complete_workflow", id: workflow_id, result:)
        out
      end

      #: (String, reason: String, ?result: Object?, ?worker_id: String?) -> Object
      def cancel_workflow(workflow_id, reason:, result: nil, worker_id: nil)
        out = super
        trace_event("cancel_workflow", id: workflow_id, reason:, result:)
        out
      end

      #: (String, error: String, ?worker_id: String?) -> Object
      def fail_workflow(workflow_id, error:, worker_id: nil)
        out = super
        trace_event("fail_workflow", id: workflow_id, error:)
        out
      end

      #: (workflow_id: String, command_id: Integer, name: String, ?args: Array[Object?], ?kwargs: Hash[Symbol, Object?], ?metadata: Hash[String, Object?], ?worker_id: String?) -> Object?
      def record_step_scheduled(workflow_id:, command_id:, name:, args: [], kwargs: {}, metadata: {}, worker_id: nil)
        out = super
        trace_event("step_scheduled", id: workflow_id, command_id:, name:)
        out
      end

      #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, name: String, ?worker_id: String?) -> Object?
      def record_step_started(workflow_id:, name:, command_id: nil, position: nil, worker_id: nil)
        attempt_id = super
        trace_event("step_started", id: workflow_id, command_id: normalize_command_id(command_id, position), name:)
        attempt_id
      end

      #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, result: Object?, ?worker_id: String?) -> Object?
      def record_step_completed(workflow_id:, result:, command_id: nil, position: nil, worker_id: nil)
        out = super
        trace_event("step_completed", id: workflow_id, command_id: normalize_command_id(command_id, position), result:)
        fault_plan.after(:record_step_completed)
        out
      end

      #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, error: String, ?worker_id: String?, ?terminal: bool, ?error_class: String?, ?error_message: String?) -> Object?
      def record_step_failed(workflow_id:, error:, command_id: nil, position: nil, worker_id: nil, terminal: false, error_class: nil, error_message: nil)
        out = super
        trace_event("step_failed", id: workflow_id, command_id: normalize_command_id(command_id, position), error:, terminal:)
        out
      end

      # The atomic retry write (record_step_failed_without_transaction +
      # schedule_workflow_retry) bypasses the public record_step_failed wrapper,
      # so emit the "step_failed" trace here to keep the retry path's trace
      # coherent with the exhausted path. schedule_workflow_retry still traces
      # "workflow_retry_scheduled" from inside the transaction via its own wrapper.
      #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, error: String, worker_id: String, run_at: Object?) -> Object?
      def record_step_failed_and_schedule_retry(workflow_id:, error:, worker_id:, run_at:, command_id: nil, position: nil)
        out = super
        trace_event("step_failed", id: workflow_id, command_id: normalize_command_id(command_id, position), error:, terminal: false)
        out
      end

      #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, error: String, ?worker_id: String?) -> Object?
      def record_step_canceled(workflow_id:, error:, command_id: nil, position: nil, worker_id: nil)
        out = super
        trace_event("step_canceled", id: workflow_id, command_id: normalize_command_id(command_id, position), error:)
        out
      end

      #: (workflow_id: String, ?command_id: Integer?, ?position: Integer?, name: String, wait_request: WaitRequest, ?suspend_workflow: bool, ?worker_id: String?) -> Object?
      def record_wait(workflow_id:, name:, wait_request:, command_id: nil, position: nil, suspend_workflow: true, worker_id: nil)
        wait_id = super
        trace_event("wait_recorded", id: workflow_id, wait_id:, kind: wait_request.kind, event_key: wait_request.event_key)
        fault_plan.after(:record_wait)
        wait_id
      end

      #: (worker_id: String, lease_seconds: Integer) -> Object?
      def claim_outbox(worker_id:, lease_seconds:)
        message = super
        trace_event("outbox_claimed", id: message.fetch("id"), worker: worker_id) if message
        message
      end

      #: (workflow_id: String, topic: String, payload: Object?, key: String) -> Object?
      def enqueue_outbox(workflow_id:, topic:, payload:, key:)
        # Idempotent re-enqueue (same key) is a no-op, so it neither traces nor
        # fires the durability fault — matching VirtualYugabyte's early return.
        existed = execute_store_query(:outbox_by_key, [key]).first
        id = super
        unless existed
          trace_event("outbox_enqueued", id:, key:, topic:)
          fault_plan.after(:enqueue_outbox)
        end
        id
      end

      #: (String, worker_id: String) -> Object?
      def ack_outbox(outbox_id, worker_id:)
        result = super
        trace_event("outbox_processed", id: outbox_id, worker: worker_id) if affected?(result)
        result
      end

      # The async command-delivery completion (#69). super runs the full
      # transaction (mark the inbox message completed + reconcile the wakeup row),
      # so the delivery is durably committed before `fault_plan.after` can raise.
      # A crash here therefore models a worker that crashed *after* a command was
      # durably delivered but before it moved on to the next command / cleaned up
      # its activation lease — recovery must resume the remaining mailbox without
      # re-delivering the committed command.
      #: (message_id: String, workflow_id: String, result: Object?, worker_id: String) -> Object?
      def complete_workflow_command(message_id:, workflow_id:, result:, worker_id:)
        out = super
        if out
          trace_event("workflow_command_completed", id: workflow_id, message_id:)
          fault_plan.after(:complete_workflow_command)
        end
        out
      end

      # Crash window A in the async command-delivery drain loop (#69): the
      # worker has *durably* leased the wakeup row (claim_target_activation
      # commits its own transaction) but has not yet claimed any inbox message.
      # Firing the fault here models a worker that died holding only the
      # activation lease. Because the lease is committed, recovery cannot
      # proceed until `locked_until` passes the virtual clock — then another
      # worker reclaims the expired activation (claim_expired_target_activation)
      # and drains the still-pending mailbox. Gated on an actual claim so an
      # empty poll neither traces nor faults.
      #: (worker_id: String, lease_seconds: Integer, ?target_kinds: Array[String]?, ?target_types: Array[String]?, ?now: Object?, ?worker_pool: String) -> Object?
      def claim_target_activation(worker_id:, lease_seconds:, target_kinds: nil, target_types: nil, now: Time.now, worker_pool: "default")
        out = super
        fault_plan.after(:claim_target_activation) if out
        out
      end

      # Crash window B: the worker has durably leased *both* the activation and
      # the inbox head message (claim_inbox_messages commits its own
      # transaction marking the row running+leased) but has not yet completed
      # the command. A crash here leaves the message `running` with a live
      # lease; recovery waits for the lease to expire, then re-claims the
      # expired-but-running head (inbox_row_claimable? treats it as reclaimable)
      # and completes it — exactly once. Gated on a non-empty claim.
      #: (target_kind: String, target_type: String, target_id: String, worker_id: String, ?lease_seconds: Integer, ?limit: Integer, ?now: Object?, ?worker_pool: String) -> Array[Hash[String, Object?]]
      def claim_inbox_messages(target_kind:, target_type:, target_id:, worker_id:, lease_seconds: 60, limit: 1, now: Time.now, worker_pool: "default")
        out = super
        fault_plan.after(:claim_inbox_messages) unless out.empty?
        out
      end

      #: (worker_pool: String, object_type: String?, object_id: String?, name: String, wake_at: Object?, payload: Object?) -> Object?
      def upsert_object_wakeup_without_transaction(worker_pool:, object_type:, object_id:, name:, wake_at:, payload:)
        out = super
        trace_event("object_wake_scheduled", object_id:, name:, wake_at:) if affected?(out)
        out
      end

      #: (worker_pool: String, object_type: String?, object_id: String?, name: String) -> Object?
      def delete_object_wakeup_without_transaction(worker_pool:, object_type:, object_id:, name:)
        out = super
        trace_event("object_wake_canceled", object_id:, name:) if affected?(out) && !@delivering_object_wakeups
        out
      end

      #: (worker_pool: String, object_type: String?, object_id: String?) -> Object?
      def delete_all_object_wakeups_without_transaction(worker_pool:, object_type:, object_id:)
        removed = pending_object_wakeups.count do |wakeup|
          wakeup.fetch("worker_pool") == worker_pool && wakeup.fetch("object_type") == object_type && wakeup.fetch("object_id") == object_id
        end
        out = super
        trace_event("object_wake_canceled_all", object_id:, removed:) if removed.positive? && affected?(out)
        out
      end

      #: (Array[Hash[String, Object?]]) -> Integer
      def deliver_due_object_wakeups(wakeups)
        previous = @delivering_object_wakeups
        @delivering_object_wakeups = true
        count = super
        wakeups.each do |wakeup|
          trace_event("object_wake_delivered", object_id: wakeup.fetch("object_id"), name: wakeup.fetch("name"))
        end
        @object_wakes_delivered += count
        count
      ensure
        @delivering_object_wakeups = previous
      end

      #: (workflow_id: String, worker_id: String, run_at: Object?) -> Object?
      def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
        result = super
        trace_event("workflow_retry_scheduled", id: workflow_id, run_at:) if result
        result
      end

      #: (workflow_id: String, reason: String) -> Object?
      def request_workflow_cancellation(workflow_id:, reason:)
        row = super
        trace_event("workflow_cancel_requested", id: workflow_id, reason:, status: row.fetch("status")) if row
        row
      end

      #: (workflow_id: String) -> Object?
      def mark_workflow_cancellation_delivered(workflow_id:)
        result = super
        trace_event("workflow_cancel_delivered", id: workflow_id) if affected?(result)
        result
      end

      # Virtual-time fence: same SQL state machine as MysqlStore#with_fence, but
      # with the wall-clock polling loop removed. Under a single-threaded virtual
      # clock a holder cannot make progress while we sleep, so a fence we cannot
      # immediately acquire is either already resolved (return/raise its outcome)
      # or held by a worker that crashed mid-block. The latter leaves the row
      # `running`; once its lease (`locked_until`) is past the virtual clock we
      # reclaim it atomically via claim_expired_fence and run the block ourselves.
      #
      # A holder can be made to crash *while holding* the fence via
      # `fault_plan.after(:fence_acquired)`: it raises after the row is locked but
      # before the side effect runs, so the row stays `running` for a reclaimer to
      # take over — exercising the bug-2 reclaim path without foreknowledge.
      #: (workflow_id: String, key: String, ?poll_interval: Numeric, ?timeout: Numeric) { () -> Object? } -> Object?
      def with_fence(workflow_id:, key:, poll_interval: 0.05, timeout: 10, &block)
        token = generate_uuid
        execute_store_query(:insert_fence, [workflow_id, key, token, timeout])

        if execute_store_query(:lock_fence_for_worker, [workflow_id, key, token]).first
          trace_event("fence_acquired", id: workflow_id, key:)
          fault_plan.after(:fence_acquired)
          return run_fenced_block(workflow_id:, key:, token:, &block)
        end

        row = execute_store_query(:read_fence, [workflow_id, key]).first
        decoded = decode_row(row) if row
        case decoded&.fetch("status")
        when "completed"
          decoded.fetch("result")
        when "failed"
          raise Durababble::Error, decoded.fetch("error")
        when "running"
          unless reclaim_expired_fence(workflow_id:, key:, token:, timeout:)
            raise Durababble::FenceTimeout, "fence #{key} for #{workflow_id} held by another worker and not yet reclaimable"
          end

          trace_event("fence_reclaimed", id: workflow_id, key:)
          run_fenced_block(workflow_id:, key:, token:, &block)
        else
          raise Durababble::FenceTimeout, "fence #{key} for #{workflow_id} held by another worker and not yet reclaimable"
        end
      end

      # Runs the fenced side effect and records its outcome. Mirrors the inherited
      # helper but counts the (exactly-once) side effect and emits the trace events
      # the scenarios assert on. An InjectedCrash is a process death, not a block
      # failure: it must propagate without marking the fence `failed`, so the row
      # stays `running` and reclaimable.
      #: (workflow_id: String, key: String, token: String) { () -> Object? } -> Object?
      def run_fenced_block(workflow_id:, key:, token:, &block)
        @side_effects += 1
        result = block.call
        execute_store_query(:complete_fence, [dump_serialized(result), workflow_id, key, token])
        trace_event("fence_completed", id: workflow_id, key:, result:)
        result
      rescue Durababble::InjectedCrash
        raise
      rescue StandardError => e
        execute_store_query(:fail_fence, ["#{e.class}: #{e.message}", workflow_id, key, token])
        raise
      end

      # The bug-2 reclaim seam: atomically take over a 'running' fence whose
      # lease has expired (claim_expired_fence is a single UPDATE; affected_rows
      # == 1 means we won the takeover). Kept as its own method so the DST
      # mutation test can revert the fix in one place and confirm
      # fence_holder_crash_and_reclaim goes red. Mirrors the inherited
      # with_fence's inline reclaim.
      #: (workflow_id: String, key: String, token: String, timeout: Numeric) -> bool
      def reclaim_expired_fence(workflow_id:, key:, token:, timeout:)
        execute_store_query(:claim_expired_fence, [token, timeout, workflow_id, key]).affected_rows == 1
      end

      # Single-shot, non-blocking: the production loop sleeps wall-clock until the
      # message resolves, which never terminates under virtual time.
      #: (String, ?poll_interval: Numeric, ?timeout: Numeric?) -> Object?
      def wait_for_inbox_message(message_id, poll_interval: 0.05, timeout: 10)
        message = inbox_message(message_id)
        raise KeyError, "inbox message not found: #{message_id}" unless message

        case message.fetch("status")
        when "completed"
          message["result"]
        when "failed", "dead_lettered"
          raise Durababble::Error, message["error"] || "inbox message #{message_id} failed"
        else
          raise Durababble::CommandTimeout, "inbox message #{message_id} not ready (no blocking under virtual time)"
        end
      end

      # --- Test-only corruption overlay --------------------------------------
      # The bug_* harness fixtures fabricate impossible store shapes (orphaned
      # steps, duplicate completions, stuck fences) to prove the invariant
      # checkers fire. The real schema's NOT NULL / FOREIGN KEY constraints would
      # reject those rows, so injected rows live in an overlay that the
      # StoreInspection readers merge over the genuine DB rows — the checkers see
      # the same shapes VirtualYugabyte's ivar-poking used to produce.

      #: (Hash[String, Object?]) -> void
      def inject_workflow(row)
        @injected_workflows[row.fetch("id").to_s] = row
      end

      # The harness keys steps as {workflow_id => {slot => row}} and flags any row
      # whose slot/owning-workflow disagree with the row's own position/workflow_id
      # ("inconsistent identity"). To fabricate those, a fixture may pass
      # "__group_id" / "__position_key" meta-fields to place a row in a slot that
      # deliberately mismatches its contents.
      #: (Hash[String, Object?]) -> void
      def inject_step(row)
        row = row #: as untyped
        group = (row["__group_id"] || row.fetch("workflow_id")).to_s
        slot = row["__position_key"] || row.fetch("position")
        clean = row.reject { |key, _| key == "__group_id" || key == "__position_key" }
        (@injected_steps[group] ||= {})[slot] = clean
      end

      #: (Hash[String, Object?]) -> void
      def inject_attempt(row)
        (@injected_attempts[row.fetch("workflow_id").to_s] ||= []) << row
      end

      #: (Hash[String, Object?]) -> void
      def inject_wait(row)
        @injected_waits[row.fetch("id").to_s] = row
      end

      #: (Hash[String, Object?]) -> void
      def inject_outbox(row)
        @injected_outbox[row.fetch("id").to_s] = row
      end

      #: (Hash[String, Object?]) -> void
      def inject_fence(row)
        @injected_fences << row
      end

      #: (Hash[String, Object?]) -> void
      def inject_inbox(row)
        @injected_inbox[row.fetch("id").to_s] = row
      end

      #: (Hash[String, Object?]) -> void
      def inject_target_activation(row)
        @injected_target_activations << row
      end

      # --- StoreInspection ---------------------------------------------------

      #: () -> Hash[String, Hash[String, Object?]]
      def all_workflows
        base = select_all("workflows").to_h { |row| [row.fetch("id"), row] }
        base.merge(@injected_workflows)
      end

      #: () -> Hash[String, Hash[Integer, Hash[String, Object?]]]
      def all_steps
        grouped = Hash.new { |_hash, _key| {} }
        by_workflow = {} #: Hash[String, Hash[Integer, Hash[String, Object?]]]
        select_all("steps").each do |row|
          (by_workflow[row.fetch("workflow_id")] ||= {})[row.fetch("position")] = with_command_id(row)
        end
        @injected_steps.each do |workflow_id, steps|
          target = (by_workflow[workflow_id] ||= {})
          steps.each { |position, row| target[position] = row }
        end
        by_workflow.each { |workflow_id, steps| grouped[workflow_id] = steps }
        grouped
      end

      #: () -> Hash[String, Array[Hash[String, Object?]]]
      def all_attempts
        grouped = Hash.new { |_hash, _key| [] }
        by_workflow = {} #: Hash[String, Array[Hash[String, Object?]]]
        # Order chronologically: the base store stamps attempt ids with
        # SecureRandom.uuid (not the monotonic seam), so id order is not
        # insertion order. started_at is the integer tick; sequential attempts
        # for a step start on strictly later ticks, so this puts the latest
        # attempt last for the harness's `attempts.last` comparison. The random
        # id is a stable secondary key for any same-tick ties.
        select_all("step_attempts", order: "started_at, id").each do |row|
          (by_workflow[row.fetch("workflow_id")] ||= []) << with_command_id(row)
        end
        @injected_attempts.each do |workflow_id, attempts|
          (by_workflow[workflow_id] ||= []).concat(attempts)
        end
        by_workflow.each { |workflow_id, attempts| grouped[workflow_id] = attempts }
        grouped
      end

      #: () -> Hash[String, Hash[String, Object?]]
      def all_waits
        base = select_all("waits").to_h { |row| [row.fetch("id"), row] }
        base.merge(@injected_waits)
      end

      #: () -> Hash[String, Hash[String, Object?]]
      def all_outbox
        base = select_all("outbox").to_h { |row| [row.fetch("id"), row] }
        base.merge(@injected_outbox)
      end

      #: () -> Array[Hash[String, Object?]]
      def all_fences
        select_all("fences", order: "workflow_id, `key`") + @injected_fences
      end

      #: () -> Hash[String, Hash[String, Object?]]
      def all_inbox
        base = select_all("inbox").to_h { |row| [row.fetch("id"), row] }
        base.merge(@injected_inbox)
      end

      #: () -> Array[Hash[String, Object?]]
      def all_target_activations
        select_all("target_activations", order: "worker_pool, target_kind, target_type, target_id") + @injected_target_activations
      end

      #: () -> Array[Hash[String, Object?]]
      def pending_object_wakeups
        select_all("object_wakeups", order: "worker_pool, object_type, object_id, name")
      end

      #: () -> Hash[Symbol, Object?]
      def summary
        workflows = select_all("workflows")
        {
          completed_workflows: workflows.count { |row| row.fetch("status") == "completed" },
          canceled_workflows: workflows.count { |row| row.fetch("status") == "canceled" },
          side_effects: @side_effects,
          processed_outbox: select_all("outbox").count { |row| row.fetch("status") == "processed" },
          workflows: workflows.length,
          object_wakes_delivered: @object_wakes_delivered,
        }
      end

      private

      # Track transaction depth so the crash proxy can tell an autocommitted
      # write from one inside an open transaction, and only fire :after_commit
      # when the outermost transaction has actually committed.
      #: (**Object?) { () -> Object? } -> Object?
      def transaction(**options, &block)
        @txn_depth += 1
        begin
          result = super
        rescue StandardError
          @txn_depth -= 1
          raise
        end
        @txn_depth -= 1
        maybe_crash_after_write(:after_commit) if @txn_depth.zero?
        result
      end

      #: (String, Array[Object?]) -> untyped
      def execute_store_query_sql(sql, params)
        result = super
        maybe_crash_after_write(@txn_depth.positive? ? :mid_transaction : :after_statement) if wrote?(result)
        result
      end

      #: (Object?) -> bool
      def wrote?(result)
        result = result #: as untyped
        result.respond_to?(:affected_rows) && result.affected_rows.to_i.positive?
      end

      #: (Symbol) -> void
      def maybe_crash_after_write(window)
        return if @write_crash_percent.zero?
        return unless @crashes_armed
        return unless scheduler.rng.chance(@write_crash_percent)

        trace_event("store_crash", window:)
        raise InjectedCrash, "injected store crash #{window}"
      end

      # Collapse the wall-clock interval to ticks: integer ticks pass through,
      # and any Time (the `now: Time.now` defaults) becomes the current tick.
      #: (Object?) -> Integer?
      def timestamp_or_nil(time)
        return if time.nil?
        return time if time.is_a?(Integer)

        scheduler.time
      end

      # One tick == one second, so INTERVAL n SECOND renders as `+ n` (not the
      # base store's microsecond `+ n * 1_000_000`).
      #: () -> Integer
      def seconds_scale
        1
      end

      # Monotonic, zero-padded so lexical ordering equals insertion order — the
      # claim_candidate tie-break and step_attempts inspection both rely on it.
      #: () -> String
      def generate_uuid
        @uuid_seq += 1
        format("d%011d", @uuid_seq)
      end

      # FIFO among claim candidates by integer tick, with a stable id tie-break.
      # (The base compares created_at.to_s, which mis-orders integer ticks: "9"
      # sorts after "10".)
      #: (Array[Hash[String, Object?]]) -> Hash[String, Object?]?
      def claim_candidate(candidates)
        candidates.min_by { |row| [row.fetch("created_at").to_i, claim_tiebreak(row)] }
      end

      #: (Hash[String, Object?]) -> String
      def claim_tiebreak(row)
        row["id"]&.to_s || [row["target_kind"], row["target_type"], row["target_id"], row["key"]].join("/")
      end

      # Emit wait completions for the harness's `wait_completed` assertions; the
      # shared finish path otherwise writes silently.
      #: (Array[Hash[String, Object?]], Hash[String, Object?]) -> Integer
      def finish_completed_waits(waits, payload)
        count = super
        waits.each do |wait|
          wait = wait #: as untyped
          trace_event("wait_completed", id: wait.fetch("workflow_id"), wait_id: wait.fetch("id"), payload:)
        end
        count
      end

      #: (String, ?order: String?) -> Array[Hash[String, Object?]]
      def select_all(name, order: nil)
        sql = "SELECT * FROM #{table(name)}"
        sql = "#{sql} ORDER BY #{order}" if order
        execute_store_query_sql(sql, []).to_a.map { |row| decode_row(row) }
      end

      #: (Object?) -> bool
      def affected?(result)
        result = result #: as untyped
        result.respond_to?(:affected_rows) && result.affected_rows.to_i.positive?
      end

      #: (String, **Object?) -> void
      def trace_event(name, **fields)
        scheduler.trace.event(scheduler.time, "store", name, fields)
      end
    end
  end
end
