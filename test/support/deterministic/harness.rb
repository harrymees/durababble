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
      end

      # Declare that, once the scheduler drains, every workflow must be either
      # terminal or *legitimately* parked (a pending event wait, or a timer /
      # retry scheduled beyond the simulation horizon). Opt-in because some
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

        verify_workflow_invariants!(workflows_state)
        verify_step_invariants!(workflows_state, steps_state, attempts_state)
        verify_wait_invariants!(workflows_state, steps_state, waits_state)
        verify_outbox_invariants!(workflows_state, outbox_state)
        verify_fence_invariants!(fences_state)
        verify_liveness!(workflows_state, waits_state) if @expect_settled
        verify_effect_expectations!
      end

      # A fence still `running` at end of run whose lease has expired and was
      # never reclaimed is a stuck fence — the crashed-holder-never-reclaimed
      # bug class. Only judged when the backend models a lease (`locked_until`);
      # the legacy in-memory store does not and is skipped.
      #: (untyped) -> untyped
      def verify_fence_invariants!(fences_state)
        final_time = scheduler.time
        fences_state.each do |fence|
          next unless fence.fetch("status") == "running"

          locked_until = fence["locked_until"]
          next if locked_until.nil?

          if locked_until < final_time
            violations << "stuck fence #{fence.fetch("workflow_id")}/#{fence.fetch("key")} held by #{fence["locked_by"].inspect} with expired lease never reclaimed"
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
              violations << "workflow #{id} waiting with no pending wait at end of run"
            elsif pending.any? { |wait| wait.fetch("kind") == "timer" && !wait.fetch("wake_at").nil? && wait.fetch("wake_at") <= final_time }
              violations << "workflow #{id} waiting on a timer past its wake_at at end of run (timer never fired)"
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

      WORKFLOW_STATUSES = WorkflowStatus::ALL
      STEP_STATUSES = StepStatus::ALL
      ATTEMPT_STATUSES = AttemptStatus::ALL
      WAIT_STATUSES = WaitStatus::ALL
      OUTBOX_STATUSES = OutboxStatus::ALL
      TERMINAL_WORKFLOW_STATUSES = [WorkflowStatus::COMPLETED, WorkflowStatus::CANCELED, WorkflowStatus::FAILED].freeze

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
      def verify_step_invariants!(workflows_state, steps_state, attempts_state)
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
            # record_step_scheduled and record_step_started). Every other status
            # implies at least one attempt.
            attempts = attempts_state[workflow_id].select { |attempt| attempt.fetch("position") == position }
            scheduled = status == StepStatus::SCHEDULED
            if attempts.empty? && !scheduled
              violations << "step #{workflow_id}/#{position} has no attempt history"
            end
            if TERMINAL_WORKFLOW_STATUSES.include?(workflows_state[workflow_id]&.fetch("status")) && (scheduled || AttemptStatus.live?(status))
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
        outbox_state.each_value do |message|
          outbox_id = message.fetch("id")
          status = message.fetch("status")
          violations << "outbox #{outbox_id} has unknown status #{status.inspect}" unless OUTBOX_STATUSES.include?(status)
          unless workflows_state.key?(message.fetch("workflow_id"))
            violations << "outbox #{outbox_id} references missing workflow #{message.fetch("workflow_id")}"
          end
          next unless status == "processing"

          if message.fetch("locked_by").nil? || message.fetch("locked_until").nil?
            violations << "processing outbox #{outbox_id} has no lease"
          end
        end
      end
    end
  end
end
