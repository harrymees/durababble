# typed: true
# frozen_string_literal: true

module Durababble
  module WorkflowStatus
    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"
    CANCELING = "canceling"
    CANCELED = "canceled"
    FAILED = "failed"
    COMPLETED = "completed"

    ALL = [PENDING, RUNNING, WAITING, CANCELING, CANCELED, FAILED, COMPLETED].freeze
    COMPLETED_STATUSES = [COMPLETED, CANCELED].freeze
    SUSPENDED_OR_RUNNABLE = [PENDING, WAITING, CANCELING].freeze
    RPC_NOT_RUNNING = [COMPLETED, WAITING].freeze

    class << self
      #: (String | Hash[String, Object?]) -> bool
      def completed?(row_or_status)
        COMPLETED_STATUSES.include?(status_of(row_or_status))
      end

      #: (Hash[String, Object?]) -> bool
      def terminal?(row)
        completed?(row) || final_failed?(row)
      end

      #: (Hash[String, Object?]) -> bool
      def final_failed?(row)
        row.fetch("status") == FAILED && row["next_run_at"].nil?
      end

      #: (String | Hash[String, Object?]) -> bool
      def suspended_or_runnable?(row_or_status)
        SUSPENDED_OR_RUNNABLE.include?(status_of(row_or_status))
      end

      #: (String | Hash[String, Object?]) -> bool
      def rpc_not_running?(row_or_status)
        RPC_NOT_RUNNING.include?(status_of(row_or_status))
      end

      #: (String | Hash[String, Object?]) -> bool
      def running?(row_or_status)
        status_of(row_or_status) == RUNNING
      end

      private

      #: (String | Hash[String, Object?]) -> String
      def status_of(row_or_status)
        row_or_status.is_a?(Hash) ? row_or_status.fetch("status").to_s : row_or_status
      end
    end
  end

  module StepStatus
    RUNNING = "running"
    WAITING = "waiting"
    CANCELED = "canceled"
    FAILED = "failed"
    COMPLETED = "completed"

    ALL = [RUNNING, WAITING, CANCELED, FAILED, COMPLETED].freeze
    LIVE = [RUNNING, WAITING].freeze
  end

  module AttemptStatus
    RUNNING = "running"
    WAITING = "waiting"
    CANCELED = "canceled"
    FAILED = "failed"
    COMPLETED = "completed"

    ALL = [RUNNING, WAITING, CANCELED, FAILED, COMPLETED].freeze
    LIVE = [RUNNING, WAITING].freeze

    class << self
      #: (String | Hash[String, Object?]) -> bool
      def live?(row_or_status)
        LIVE.include?(status_of(row_or_status))
      end

      private

      #: (String | Hash[String, Object?]) -> String
      def status_of(row_or_status)
        row_or_status.is_a?(Hash) ? row_or_status.fetch("status").to_s : row_or_status
      end
    end
  end

  module WaitStatus
    PENDING = "pending"
    CANCELED = "canceled"
    COMPLETED = "completed"

    ALL = [PENDING, CANCELED, COMPLETED].freeze
  end

  module OutboxStatus
    PENDING = "pending"
    PROCESSING = "processing"
    PROCESSED = "processed"

    ALL = [PENDING, PROCESSING, PROCESSED].freeze
  end

  module InboxStatus
    PENDING = "pending"
    FAILED = "failed"
    RUNNING = "running"
    DEAD_LETTERED = "dead_lettered"

    ACTIVATABLE = [PENDING, FAILED, RUNNING].freeze

    class << self
      #: (String | Hash[String, Object?]) -> bool
      def activatable?(row_or_status)
        ACTIVATABLE.include?(status_of(row_or_status))
      end

      #: (String | Hash[String, Object?]) -> bool
      def dead_lettered?(row_or_status)
        status_of(row_or_status) == DEAD_LETTERED
      end

      #: (String | Hash[String, Object?]) -> bool
      def running?(row_or_status)
        status_of(row_or_status) == RUNNING
      end

      private

      #: (String | Hash[String, Object?]) -> String
      def status_of(row_or_status)
        row_or_status.is_a?(Hash) ? row_or_status.fetch("status").to_s : row_or_status
      end
    end
  end
end
