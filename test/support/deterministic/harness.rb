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
        workflows_state = store.instance_variable_get(:@workflows)
        steps_state = store.instance_variable_get(:@steps)
        attempts_state = store.instance_variable_get(:@attempts)
        waits_state = store.instance_variable_get(:@waits)
        outbox_state = store.instance_variable_get(:@outbox)

        verify_workflow_invariants!(workflows_state)
        verify_step_invariants!(workflows_state, steps_state, attempts_state)
        verify_wait_invariants!(workflows_state, steps_state, waits_state)
        verify_outbox_invariants!(workflows_state, outbox_state)
      end

      WORKFLOW_STATUSES = WorkflowStatus::ALL
      STEP_STATUSES = StepStatus::ALL
      ATTEMPT_STATUSES = AttemptStatus::ALL
      WAIT_STATUSES = WaitStatus::ALL
      OUTBOX_STATUSES = OutboxStatus::ALL
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

            attempts = attempts_state[workflow_id].select { |attempt| attempt.fetch("position") == position }
            if attempts.empty?
              violations << "step #{workflow_id}/#{position} has no attempt history"
            end
            if TERMINAL_WORKFLOW_STATUSES.include?(workflows_state[workflow_id]&.fetch("status")) && AttemptStatus.live?(status)
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
