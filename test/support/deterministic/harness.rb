# typed: true
# frozen_string_literal: true

require_relative "sim_worker"

module Durababble
  module Deterministic
    class Harness
      #: untyped
      attr_reader :scenario, :seed, :scheduler, :network, :store, :workflows, :violations

      #: (scenario: untyped, seed: untyped, scheduler: untyped, network: untyped, store: untyped) -> void
      def initialize(scenario:, seed:, scheduler:, network:, store:)
        @scenario = scenario
        @seed = seed
        @scheduler = scheduler
        @network = network
        @store = store
        @workflows = {}
        @violations = []
        @checks = []
        @expect_settled = false
        @expected_side_effects = nil
        @expected_processed_outbox = nil
        @monitor_transitions = false
        @previous_transition_snapshot = nil
        @transition_violation_keys = {}
      end

      # Opt into online transition checks. Final-state invariants are still run,
      # but this catches illegal intermediate transitions that later cleanup
      # could otherwise hide.
      #: () -> void
      def monitor_transitions!
        @monitor_transitions = true
      end

      # Declare that, once the scheduler drains, every workflow must be either
      # terminal or *legitimately* parked by replay history (a pending event wait,
      # or a timer / retry scheduled beyond the simulation horizon). Opt-in because some
      # scenarios intentionally leave runnable work behind. Enables the
      # liveness/termination checker.
      #: () -> void
      def expect_settled!
        @expect_settled = true
      end

      # Declare the exact number of fenced side effects expected by end of run.
      #: (untyped) -> void
      def expect_side_effects(count)
        @expected_side_effects = count
      end

      # Declare the exact number of processed outbox messages expected by end of run.
      #: (untyped) -> void
      def expect_processed_outbox(count)
        @expected_processed_outbox = count
      end

      #: (untyped, ticks: untyped, ?crash_percent: untyped) -> untyped
      def add_workers(ids, ticks:, crash_percent: 0)
        ids.each do |id|
          SimWorker.new(id:, scheduler:, network:, store:, workflows:, crash_percent:).start(ticks:)
        end
      end

      #: (untyped) { (?) -> untyped } -> untyped
      def check(description, &block)
        @checks << [description, block]
      end

      # Mirrors the lease holder's in-memory allocation for scenarios that append
      # history directly through the store rather than via the engine.
      #: (untyped) -> Integer
      def next_event_index(workflow_id)
        Durababble::WorkflowReplayHistory.next_event_index_after(store.workflow_history_for(workflow_id))
      end

      #: () -> void
      def prepare_for_run!
        @previous_transition_snapshot = transition_snapshot if @monitor_transitions
      end

      #: (actor: untyped, name: untyped, time: untyped) -> void
      def observe_transition!(actor:, name:, time:)
        return unless @monitor_transitions

        previous = @previous_transition_snapshot || transition_snapshot
        current = transition_snapshot
        verify_transition!(previous, current, actor:, event_name: name, time:)
        @previous_transition_snapshot = current
      end

      #: () -> untyped
      def verify!
        @checks.each do |description, block|
          violations << "check failed: #{description}" unless block.call
        rescue StandardError => e
          violations << "check errored: #{description}: #{e.class}: #{e.message}"
        end

        verify_store_invariants!
      end

      private

      #: () -> untyped
      def verify_store_invariants!
        workflows_state = store.all_workflows
        steps_state = store.all_steps
        attempts_state = store.all_attempts
        waits_state = store.all_waits
        outbox_state = store.all_outbox
        fences_state = store.all_fences
        inbox_state = store.all_inbox
        activations_state = store.all_target_activations

        verify_workflow_invariants!(workflows_state)
        verify_step_invariants!(workflows_state, steps_state, attempts_state, waits_state)
        verify_wait_invariants!(workflows_state, steps_state, waits_state)
        verify_outbox_invariants!(workflows_state, outbox_state)
        verify_fence_invariants!(fences_state)
        verify_inbox_invariants!(inbox_state)
        verify_inbox_sequence_invariants!(inbox_state)
        verify_activation_invariants!(activations_state)
        verify_activation_inbox_consistency!(inbox_state, activations_state)
        verify_liveness!(workflows_state, waits_state) if @expect_settled
        verify_effect_expectations!
      end

      #: () -> Hash[Symbol, untyped]
      def transition_snapshot
        workflows = store.all_workflows
        {
          time: scheduler.time,
          workflows: workflows,
          outbox: store.all_outbox,
          inbox: store.all_inbox,
          activations: store.all_target_activations.to_h { |activation| [target_key(activation), activation] },
          histories: workflows.keys.to_h do |workflow_id|
            [workflow_id, store.workflow_history_for(workflow_id)]
          rescue KeyError
            [workflow_id, []]
          end,
        }
      end

      #: (Hash[Symbol, untyped], Hash[Symbol, untyped], actor: untyped, event_name: untyped, time: untyped) -> void
      def verify_transition!(previous, current, actor:, event_name:, time:)
        verify_terminal_workflow_transitions!(previous, current, actor:, event_name:, time:)
        verify_backoff_claim_transitions!(previous, current, actor:, event_name:, time:)
        verify_outbox_transitions!(previous, current, actor:, event_name:, time:)
        verify_inbox_transitions!(previous, current, actor:, event_name:, time:)
        verify_activation_transitions!(previous, current, actor:, event_name:, time:)
        verify_workflow_command_history_transitions!(previous, current, actor:, event_name:, time:)
      end

      #: (Hash[Symbol, untyped], Hash[Symbol, untyped], actor: untyped, event_name: untyped, time: untyped) -> void
      def verify_terminal_workflow_transitions!(previous, current, actor:, event_name:, time:)
        previous.fetch(:workflows).each do |workflow_id, before|
          next unless terminal_workflow_row?(before)

          after = current.fetch(:workflows)[workflow_id]
          if after.nil?
            transition_violation!("terminal workflow #{workflow_id} disappeared", actor:, event_name:, time:)
            next
          end

          next if terminal_workflow_fingerprint(before) == terminal_workflow_fingerprint(after)

          transition_violation!(
            "terminal workflow #{workflow_id} mutated from #{before.fetch("status").inspect} to #{after.fetch("status").inspect}",
            actor:,
            event_name:,
            time:,
          )
        end
      end

      #: (Hash[Symbol, untyped], Hash[Symbol, untyped], actor: untyped, event_name: untyped, time: untyped) -> void
      def verify_backoff_claim_transitions!(previous, current, actor:, event_name:, time:)
        previous.fetch(:workflows).each do |workflow_id, before|
          after = current.fetch(:workflows)[workflow_id]
          next unless after
          next unless after.fetch("status") == WorkflowStatus::RUNNING
          next unless backoff_protected?(previous, workflow_id, before, time)

          transition_violation!(
            "workflow #{workflow_id} became running before retry/backoff due time",
            actor:,
            event_name:,
            time:,
          )
        end
      end

      #: (Hash[Symbol, untyped], Hash[Symbol, untyped], actor: untyped, event_name: untyped, time: untyped) -> void
      def verify_outbox_transitions!(previous, current, actor:, event_name:, time:)
        previous.fetch(:outbox).each do |outbox_id, before|
          after = current.fetch(:outbox)[outbox_id]
          next unless after
          next unless before.fetch("status") != OutboxStatus::PROCESSED && after.fetch("status") == OutboxStatus::PROCESSED
          next if before.fetch("status") == OutboxStatus::PROCESSING && live_row_lease?(before, time)

          transition_violation!(
            "outbox #{outbox_id} was processed without a live processing lease",
            actor:,
            event_name:,
            time:,
          )
        end
      end

      #: (Hash[Symbol, untyped], Hash[Symbol, untyped], actor: untyped, event_name: untyped, time: untyped) -> void
      def verify_inbox_transitions!(previous, current, actor:, event_name:, time:)
        terminal_statuses = [InboxStatus::COMPLETED, InboxStatus::FAILED, InboxStatus::DEAD_LETTERED]
        previous.fetch(:inbox).each do |message_id, before|
          after = current.fetch(:inbox)[message_id]
          next unless after
          next unless before.fetch("status") == InboxStatus::RUNNING
          next unless terminal_statuses.include?(after.fetch("status"))
          next if live_row_lease?(before, time)
          next if before["locked_by"] != actor

          transition_violation!(
            "inbox #{message_id} moved to #{after.fetch("status")} without a live command lease",
            actor:,
            event_name:,
            time:,
          )
        end
      end

      #: (Hash[Symbol, untyped], Hash[Symbol, untyped], actor: untyped, event_name: untyped, time: untyped) -> void
      def verify_activation_transitions!(previous, current, actor:, event_name:, time:)
        previous.fetch(:activations).each do |key, before|
          next unless before.fetch("status") == "running"
          next if live_row_lease?(before, time)
          next if before["locked_by"] != actor

          after = current.fetch(:activations)[key]
          next if after && after.fetch("status") == "running"

          transition_violation!(
            "expired target activation #{key.join("/")} was completed or removed instead of reclaimed",
            actor:,
            event_name:,
            time:,
          )
        end
      end

      #: (Hash[Symbol, untyped], Hash[Symbol, untyped], actor: untyped, event_name: untyped, time: untyped) -> void
      def verify_workflow_command_history_transitions!(previous, current, actor:, event_name:, time:)
        current.fetch(:histories).each do |workflow_id, after_history|
          before_history = previous.fetch(:histories).fetch(workflow_id, [])
          before_count = before_history.length
          next if after_history.length <= before_count

          after_history.drop(before_count).each do |event|
            next unless ["workflow_command_completed", "workflow_command_failed"].include?(event.fetch("kind"))

            before_workflow = previous.fetch(:workflows)[workflow_id]
            if before_workflow && terminal_workflow_row?(before_workflow)
              transition_violation!(
                "terminal workflow #{workflow_id} appended #{event.fetch("kind")} history",
                actor:,
                event_name:,
                time:,
              )
              next
            end

            message_id = event["attempt_id"]
            before_message = previous.fetch(:inbox)[message_id]
            inbox_owner = before_message&.fetch("locked_by", nil)
            visible_owner = visible_live_workflow_owner(previous.fetch(:workflows)[workflow_id], time) ||
              visible_live_workflow_owner(current.fetch(:workflows)[workflow_id], time)
            next if inbox_owner.nil?
            next if inbox_owner != actor

            if visible_owner.nil?
              transition_violation!(
                "workflow #{workflow_id} appended #{event.fetch("kind")} for inbox owner #{inbox_owner.inspect} without a live workflow lease",
                actor:,
                event_name:,
                time:,
              )
              next
            end
            next if visible_owner == inbox_owner

            transition_violation!(
              "workflow #{workflow_id} appended #{event.fetch("kind")} for inbox owner #{inbox_owner.inspect} while workflow lease was held by #{visible_owner.inspect}",
              actor:,
              event_name:,
              time:,
            )
          end
        end
      end

      #: (String, actor: untyped, event_name: untyped, time: untyped) -> void
      def transition_violation!(message, actor:, event_name:, time:)
        key = [message, actor, event_name, time]
        return if @transition_violation_keys[key]

        @transition_violation_keys[key] = true
        violations << "transition violation after #{actor}/#{event_name} at t=#{time}: #{message}"
      end

      #: (Hash[String, Object?]?) -> bool
      def terminal_workflow_row?(row)
        return false unless row

        status = row.fetch("status")
        status == WorkflowStatus::COMPLETED ||
          status == WorkflowStatus::CANCELED ||
          status == WorkflowStatus::TERMINATED ||
          (status == WorkflowStatus::FAILED && row["next_run_at"].nil?)
      end

      #: (Hash[String, Object?]) -> Array[Object?]
      def terminal_workflow_fingerprint(row)
        row.values_at("status", "result", "error", "cancel_reason", "locked_by", "locked_until", "next_run_at")
      end

      #: (Hash[Symbol, untyped], String, Hash[String, Object?], untyped) -> bool
      def backoff_protected?(snapshot, workflow_id, row, time)
        status = row.fetch("status")
        due_at = row["next_run_at"]
        return true if status == WorkflowStatus::FAILED && due_at.nil?
        return false unless [WorkflowStatus::PENDING, WorkflowStatus::CANCELING, WorkflowStatus::FAILED].include?(status)
        return false if due_at.nil?
        return false if status != WorkflowStatus::FAILED && workflow_command_pending?(snapshot, workflow_id)

        timestamp_gt?(due_at, time)
      end

      #: (Hash[Symbol, untyped], String) -> bool
      def workflow_command_pending?(snapshot, workflow_id)
        terminal_statuses = [InboxStatus::COMPLETED, InboxStatus::FAILED, InboxStatus::DEAD_LETTERED]
        snapshot.fetch(:inbox).values.any? do |message|
          message.fetch("target_kind") == "workflow" &&
            message.fetch("target_id") == workflow_id &&
            !terminal_statuses.include?(message.fetch("status"))
        end
      end

      #: (Hash[String, Object?]?, untyped) -> bool
      def live_row_lease?(row, time)
        return false unless row
        return false if row["locked_by"].nil? || row["locked_until"].nil?

        !timestamp_lt?(row.fetch("locked_until"), time)
      end

      #: (Hash[String, Object?]?, untyped) -> Object?
      def visible_live_workflow_owner(row, time)
        return unless row
        return unless row.fetch("status") == WorkflowStatus::RUNNING
        return unless live_row_lease?(row, time)

        row["locked_by"]
      end

      #: (untyped, untyped) -> bool
      def timestamp_gt?(left, right)
        timestamp_value(left) > timestamp_value(right)
      end

      #: (untyped, untyped) -> bool
      def timestamp_lt?(left, right)
        timestamp_value(left) < timestamp_value(right)
      end

      #: (untyped) -> Numeric
      def timestamp_value(value)
        case value
        when Time
          value.to_r
        else
          value.to_i
        end
      end

      # An inbox message claimed (`running`) by a worker that then crashed leaves
      # the row leased; once the scheduler drains, no worker remains to reclaim
      # it past `locked_until`. Symmetric to the stuck-fence/stuck-outbox checks:
      # the same crashed-holder-never-reclaimed bug class applied to the inbox
      # lease. Only judged when the backend models a lease (`locked_until`).
      #: (untyped) -> untyped
      def verify_inbox_invariants!(inbox_state)
        final_time = scheduler.time
        inbox_state.each_value do |message|
          inbox_id = message.fetch("id")
          status = message.fetch("status")
          violations << "inbox #{inbox_id} has unknown status #{status.inspect}" unless INBOX_STATUSES.include?(status)
          locked_by = message["locked_by"]
          locked_until = message["locked_until"]
          if locked_by.nil? != locked_until.nil?
            violations << "inbox #{inbox_id} has partial lease"
          end
          if status != InboxStatus::RUNNING
            violations << "#{status} inbox #{inbox_id} still locked" if locked_by || locked_until
            next
          end

          if locked_by.nil? || locked_until.nil?
            violations << "running inbox #{inbox_id} has no complete lease"
            next
          end

          if locked_until < final_time
            violations << "stuck inbox #{inbox_id} held by #{locked_by.inspect} with expired lease never reclaimed"
          end
        end
      end

      # Mailbox sequencing is the durable FIFO contract: every target has a
      # positive, unique, contiguous sequence stream. Gaps strand later messages
      # behind a missing head; duplicates make delivery order ambiguous.
      #: (untyped) -> untyped
      def verify_inbox_sequence_invariants!(inbox_state)
        by_target = Hash.new { |hash, key| hash[key] = [] }
        inbox_state.each_value do |message|
          next if message["target_type"].nil?

          by_target[target_key(message)] << message
        end

        by_target.each do |key, messages|
          sequences = []
          messages.each do |message|
            sequence = parse_sequence(message["sequence"])
            if sequence.nil?
              violations << "inbox #{message.fetch("id")} for #{key.join("/")} has invalid sequence #{message["sequence"].inspect}"
              next
            end
            if sequence <= 0
              violations << "inbox #{message.fetch("id")} for #{key.join("/")} has non-positive sequence #{sequence}"
            end
            sequences << sequence
          end
          next if sequences.empty?

          duplicates = sequences.tally.select { |_sequence, count| count > 1 }.keys
          unless duplicates.empty?
            violations << "mailbox #{key.join("/")} has duplicate inbox sequence(s) #{duplicates.sort.join(",")}"
          end

          positive = sequences.select(&:positive?)
          next if positive.empty?

          expected = (1..positive.max).to_a
          missing = expected - positive
          unless missing.empty?
            violations << "mailbox #{key.join("/")} has non-contiguous inbox sequence(s), missing #{missing.join(",")}"
          end
        end
      end

      # A target_activation (the #69 wakeup-coordination row) still `running` at
      # end of run whose lease has expired and was never reclaimed is the same
      # crashed-holder-never-reclaimed bug class as the stuck fence/outbox/inbox:
      # a worker claimed the activation, crashed, and the unified target
      # activation claim path never took it over, so the target is wedged with
      # pending inbox work nobody will ever drain. Only judged when the backend
      # models a lease (`locked_until`).
      #: (untyped) -> untyped
      def verify_activation_invariants!(activations_state)
        final_time = scheduler.time
        activations_state.each do |activation|
          status = activation.fetch("status")
          target = "#{activation["target_kind"]}/#{activation["target_type"]}/#{activation["target_id"]}"
          violations << "target activation #{target} has unknown status #{status.inspect}" unless ACTIVATION_STATUSES.include?(status)
          locked_by = activation["locked_by"]
          locked_until = activation["locked_until"]
          if locked_by.nil? != locked_until.nil?
            violations << "target activation #{target} has partial lease"
          end
          if status != "running"
            violations << "#{status} target activation #{target} still locked" if locked_by || locked_until
            next
          end

          if locked_by.nil? || locked_until.nil?
            violations << "running target activation #{target} has no complete lease"
            next
          end

          if locked_until < final_time
            violations << "stuck target activation #{target} held by #{locked_by.inspect} with expired lease never reclaimed"
          end
        end
      end

      # Activation/inbox consistency (the #69 wakeup contract). `reconcile_target_activation`
      # maintains the invariant that a target has a `target_activations` row iff its
      # inbox head (lowest-`sequence` row among pending/failed/running/dead_lettered —
      # i.e. INBOX_HEAD_STATUSES, matching `inbox_head_for_update`) is *not* dead-lettered.
      # Both are written/reconciled inside the same transaction as the inbox change,
      # so at end of run an activatable head with no activation can only mean a lost
      # wakeup: a mailbox with claimable work that no worker will ever be woken to
      # drain. (A dead-lettered head intentionally wedges the FIFO mailbox and gets no
      # activation, so it is not flagged. The crashed-holder case — a `running`
      # activation whose lease expired — is covered by verify_activation_invariants!.)
      #: (untyped, untyped) -> untyped
      def verify_activation_inbox_consistency!(inbox_state, activations_state)
        activation_keys = activations_state.map { |activation| target_key(activation) }

        by_target = Hash.new { |hash, key| hash[key] = [] }
        inbox_state.each_value do |message|
          next if message["target_type"].nil?

          by_target[target_key(message)] << message
        end

        by_target.each do |key, messages|
          head_candidates = messages.select { |message| INBOX_HEAD_STATUSES.include?(message.fetch("status")) }
          head = head_candidates.min_by { |message| message.fetch("sequence") }
          next if head.nil? || head.fetch("status") == InboxStatus::DEAD_LETTERED

          unless activation_keys.include?(key)
            violations << "inbox head for #{key.join("/")} is #{head.fetch("status")} but no target activation exists (lost wakeup — mailbox never drained)"
          end
        end

        # Reverse direction of the IFF: an activation whose mailbox has no
        # activatable (non-dead-lettered) inbox head is an *orphaned* wakeup — a
        # worker will be woken to drain a mailbox with nothing claimable.
        # reconcile_target_activation deletes the activation when the head is
        # completed/absent (the "else" branch) or dead-lettered, so a surviving
        # activation here means a reconcile was skipped (e.g. a crash between the
        # inbox write and the reconcile) and never repaired. This is the dual of
        # the lost-wakeup check above and the same crashed-holder bug class.
        activations_state.each do |activation|
          key = target_key(activation)
          activatable = by_target[key].select { |message| INBOX_HEAD_STATUSES.include?(message.fetch("status")) }
          head = activatable.min_by { |message| message.fetch("sequence") }

          if head.nil?
            violations << "target activation #{key.join("/")} exists but its mailbox has no activatable head (orphaned wakeup — never reconciled away)"
          elsif head.fetch("status") == InboxStatus::DEAD_LETTERED
            violations << "target activation #{key.join("/")} exists but its inbox head is dead-lettered (should have been reconciled away)"
          end
        end
      end

      # The composite identity of a target shared by the inbox and target_activations
      # tables: (worker_pool, target_kind, target_type, target_id).
      #: (untyped) -> Array[untyped]
      def target_key(row)
        [row["worker_pool"] || "default", row.fetch("target_kind"), row.fetch("target_type"), row.fetch("target_id")]
      end

      # A fence still `running` at end of run whose lease has expired and was
      # never reclaimed is a stuck fence — the crashed-holder-never-reclaimed
      # bug class. Only judged when the backend models a lease (`locked_until`);
      # the legacy in-memory store does not and is skipped.
      #: (untyped) -> untyped
      def verify_fence_invariants!(fences_state)
        final_time = scheduler.time
        fences_state.each do |fence|
          status = fence.fetch("status")
          label = "#{fence.fetch("workflow_id")}/#{fence.fetch("key")}"
          violations << "fence #{label} has unknown status #{status.inspect}" unless FENCE_STATUSES.include?(status)
          locked_by = fence["locked_by"]
          locked_until = fence["locked_until"]
          if locked_by.nil? != locked_until.nil?
            violations << "fence #{label} has partial lease"
          end
          next unless status == "running"

          if locked_by.nil? || locked_until.nil?
            violations << "running fence #{label} has no complete lease"
            next
          end

          if locked_until < final_time
            violations << "stuck fence #{label} held by #{locked_by.inspect} with expired lease never reclaimed"
          end
        end
      end

      # Liveness/termination: once the scheduler drains, every workflow must be
      # terminal or legitimately parked. Flags abandoned-but-runnable work and
      # workflows wedged mid-flight (running/canceling at end of run).
      #: (untyped, untyped) -> untyped
      def verify_liveness!(workflows_state, waits_state)
        final_time = scheduler.time
        workflows_state.each do |id, row|
          status = row.fetch("status")
          next if status == WorkflowStatus::COMPLETED || status == WorkflowStatus::CANCELED

          case status
          when "running"
            violations << "workflow #{id} left running at end of run (abandoned lease, no path forward)"
          when "canceling"
            violations << "workflow #{id} stuck canceling at end of run"
          when "pending"
            if due?(row.fetch("next_run_at"), final_time)
              violations << "workflow #{id} left pending and runnable at end of run (abandoned work)"
            end
          when "failed"
            next_run_at = row.fetch("next_run_at")
            if !next_run_at.nil? && next_run_at <= final_time
              violations << "workflow #{id} left failed with a due retry at end of run (abandoned retry)"
            end
          when "waiting"
            pending = waits_state.values.select { |wait| wait.fetch("workflow_id") == id && wait.fetch("status") == "pending" }
            if pending.empty?
              violations << "workflow #{id} waiting with no pending replay wait at end of run"
            elsif pending.any? { |wait| wait.fetch("kind") == "timer" && !wait.fetch("wake_at").nil? && wait.fetch("wake_at") <= final_time }
              violations << "workflow #{id} waiting on a timer past its wake_at at end of run (workflow was never reclaimed)"
            end
          end
        end
      end

      # Exactly-once effects: when a scenario declares an expected count, enforce
      # it as a shared invariant rather than a per-scenario `check`.
      #: () -> untyped
      def verify_effect_expectations!
        return if @expected_side_effects.nil? && @expected_processed_outbox.nil?

        summary = store.summary
        unless @expected_side_effects.nil?
          actual = summary.fetch(:side_effects)
          violations << "expected #{@expected_side_effects} side effect(s) but observed #{actual}" if actual != @expected_side_effects
        end
        return if @expected_processed_outbox.nil?

        actual = summary.fetch(:processed_outbox)
        violations << "expected #{@expected_processed_outbox} processed outbox message(s) but observed #{actual}" if actual != @expected_processed_outbox
      end

      #: (untyped, untyped) -> untyped
      def due?(next_run_at, final_time)
        next_run_at.nil? || next_run_at <= final_time
      end

      #: (untyped) -> Integer?
      def parse_sequence(value)
        Integer(value)
      rescue ArgumentError, TypeError
        nil
      end

      WORKFLOW_STATUSES = WorkflowStatus::ALL
      STEP_STATUSES = StepStatus::ALL
      ATTEMPT_STATUSES = AttemptStatus::ALL
      WAIT_STATUSES = WaitStatus::ALL
      OUTBOX_STATUSES = OutboxStatus::ALL
      FENCE_STATUSES = ["running", "completed", "failed"].freeze
      # Every valid persisted inbox status, including the retained `completed`
      # terminal state — used to flag genuinely unknown statuses.
      INBOX_STATUSES = InboxStatus::ALL
      # Mailbox head candidates: the lowest-`sequence` row among these is the head
      # (matches `inbox_head_for_update`). Excludes `completed`, which is retained
      # in the table but is never an activatable head.
      INBOX_HEAD_STATUSES = (InboxStatus::ACTIVATABLE + [InboxStatus::DEAD_LETTERED]).freeze
      # target_activations rows only ever hold these two statuses (see store_queries):
      # `pending` (enqueued/reconciled, unleased) and `running` (claimed, leased).
      ACTIVATION_STATUSES = ["pending", "running"].freeze
      TERMINAL_WORKFLOW_STATUSES = [WorkflowStatus::COMPLETED, WorkflowStatus::CANCELED, WorkflowStatus::TERMINATED, WorkflowStatus::FAILED].freeze

      #: (untyped) -> untyped
      def verify_workflow_invariants!(workflows_state)
        workflows_state.each do |id, row|
          status = row.fetch("status")
          violations << "workflow #{id} has unknown status #{status.inspect}" unless WORKFLOW_STATUSES.include?(status)
          if row.fetch("locked_by").nil? != row.fetch("locked_until").nil?
            violations << "workflow #{id} has partial lease"
          end
          if status == "running"
            violations << "running workflow #{id} has no lease" unless row.fetch("locked_by") && row.fetch("locked_until")
          elsif row.fetch("locked_by") || row.fetch("locked_until")
            violations << "#{status} workflow #{id} still locked"
          end
        end
      end

      #: (untyped, untyped, untyped) -> untyped
      def verify_step_invariants!(workflows_state, steps_state, attempts_state, waits_state)
        # Steps backed by wait history are control-flow waits (workflow-level
        # wait_condition / wait_until / sleep): they go scheduled -> waiting ->
        # completed/canceled WITHOUT ever recording a step_started, so they carry
        # no attempt by design (the canonical history is step_scheduled,
        # step_waiting -- no step_started). Only step-runner dispatched steps
        # record an attempt, so the "must have an attempt" rule below applies to
        # them, not to wait commands.
        wait_positions = Hash.new { |hash, key| hash[key] = {} }
        waits_state.each_value { |wait| wait_positions[wait.fetch("workflow_id")][wait.fetch("position")] = true }
        steps_state.each do |workflow_id, steps|
          violations << "steps exist for missing workflow #{workflow_id}" unless workflows_state.key?(workflow_id)
          completed_positions = steps.values.select { |step| step.fetch("status") == "completed" }.map { |step| step.fetch("position") }
          if completed_positions.uniq.length != completed_positions.length
            violations << "duplicate completed step positions for #{workflow_id}"
          end
          steps.each do |position, step|
            status = step.fetch("status")
            violations << "step #{workflow_id}/#{position} has unknown status #{status.inspect}" unless STEP_STATUSES.include?(status)
            if step.fetch("workflow_id") != workflow_id || step.fetch("position") != position
              violations << "step #{workflow_id}/#{position} has inconsistent identity"
            end

            # A `scheduled` step is durably recorded but not yet started, so it
            # legitimately has no attempt yet (a worker crashed between
            # record_step_scheduled and record_step_started). Terminal cleanup can
            # also cancel that not-yet-started step before any attempt exists.
            attempts = attempts_state[workflow_id].select { |attempt| attempt.fetch("position") == position }
            workflow = workflows_state[workflow_id]
            terminal_workflow = TERMINAL_WORKFLOW_STATUSES.include?(workflow&.fetch("status"))
            scheduled = status == StepStatus::SCHEDULED
            wait_backed = wait_positions[workflow_id].key?(position)
            canceled_before_start = status == StepStatus::CANCELED && terminal_workflow
            if attempts.empty? && !scheduled && !wait_backed && !canceled_before_start
              violations << "step #{workflow_id}/#{position} has no attempt history"
            end
            if terminal_workflow && (scheduled || AttemptStatus.live?(status))
              violations << "#{workflows_state.fetch(workflow_id).fetch("status")} workflow #{workflow_id} has live step #{position}"
            end

            next if attempts.empty?

            latest = attempts.last
            if latest.fetch("name") != step.fetch("name")
              violations << "step #{workflow_id}/#{position} name #{step.fetch("name").inspect} does not match latest attempt #{latest.fetch("name").inspect}"
            end
            if latest.fetch("status") != status
              violations << "step #{workflow_id}/#{position} status #{status.inspect} does not match latest attempt #{latest.fetch("status").inspect}"
            end
          end
        end

        attempts_state.each do |workflow_id, attempts|
          violations << "attempts exist for missing workflow #{workflow_id}" unless workflows_state.key?(workflow_id)
          live_attempts = Hash.new { |hash, key| hash[key] = [] }
          attempts.each do |attempt|
            position = attempt.fetch("position")
            status = attempt.fetch("status")
            violations << "attempt #{attempt.fetch("id")} has unknown status #{status.inspect}" unless ATTEMPT_STATUSES.include?(status)
            if attempt.fetch("workflow_id") != workflow_id
              violations << "attempt #{attempt.fetch("id")} has inconsistent workflow id"
            end
            live_attempts[position] << attempt if AttemptStatus.live?(status)
            workflow = workflows_state[workflow_id]
            if workflow&.fetch("status") == "completed" && status == "running"
              violations << "completed workflow #{workflow_id} has running attempt #{attempt.fetch("id")}"
            end
            unless steps_state[workflow_id].key?(position)
              violations << "attempt #{attempt.fetch("id")} references missing step #{workflow_id}/#{position}"
            end
          end
          live_attempts.each do |position, live|
            next if live.length <= 1

            ids = live.map { |attempt| attempt.fetch("id") }.join(",")
            violations << "workflow #{workflow_id} step #{position} has multiple live attempts #{ids}"
          end
        end
      end

      #: (untyped, untyped, untyped) -> untyped
      def verify_wait_invariants!(workflows_state, steps_state, waits_state)
        waits_state.each_value do |wait|
          wait_id = wait.fetch("id")
          workflow_id = wait.fetch("workflow_id")
          position = wait.fetch("position")
          status = wait.fetch("status")
          violations << "wait #{wait_id} has unknown status #{status.inspect}" unless WAIT_STATUSES.include?(status)
          violations << "wait #{wait_id} references missing workflow #{workflow_id}" unless workflows_state.key?(workflow_id)
          unless steps_state[workflow_id].key?(position)
            violations << "wait #{wait_id} references missing step #{workflow_id}/#{position}"
          end
          step = steps_state[workflow_id][position]
          if status == "completed" && step && step.fetch("status") != "completed"
            violations << "completed wait #{wait_id} has non-completed step #{workflow_id}/#{position}"
          end
        end
      end

      #: (untyped, untyped) -> untyped
      def verify_outbox_invariants!(workflows_state, outbox_state)
        final_time = scheduler.time
        outbox_state.each_value do |message|
          outbox_id = message.fetch("id")
          status = message.fetch("status")
          violations << "outbox #{outbox_id} has unknown status #{status.inspect}" unless OUTBOX_STATUSES.include?(status)
          unless workflows_state.key?(message.fetch("workflow_id"))
            violations << "outbox #{outbox_id} references missing workflow #{message.fetch("workflow_id")}"
          end
          next unless status == "processing"

          locked_by = message.fetch("locked_by")
          locked_until = message.fetch("locked_until")
          if locked_by.nil? || locked_until.nil?
            violations << "processing outbox #{outbox_id} has no lease"
            next
          end

          # Symmetric to the stuck-fence checker: once the scheduler has drained,
          # no worker remains to reclaim a lease. An outbox message still
          # `processing` with an expired `locked_until` is stuck forever — the
          # crashed-holder-never-reclaimed bug class applied to the outbox lease.
          if locked_until < final_time
            violations << "stuck outbox #{outbox_id} for workflow #{message.fetch("workflow_id")} held by #{locked_by.inspect} with expired lease never reclaimed"
          end
        end
      end
    end
  end
end
