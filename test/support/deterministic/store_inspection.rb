# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    # Backend-neutral read interface the invariant harness uses to inspect
    # persisted state. Any store used under DST (the in-memory VirtualYugabyte
    # historically, the DeterministicSqliteStore going forward) implements these
    # methods so the harness never reaches into store internals via
    # `instance_variable_get`.
    #
    # All methods return *decoded* rows: plain Ruby hashes whose keys are the
    # strings the harness reads ("status", "locked_by", "locked_until",
    # "position", ...). Implementations should return read-only snapshots; the
    # harness never mutates them.
    #
    # Expected shapes:
    #   all_workflows -> { workflow_id => row }
    #   all_steps     -> { workflow_id => { position => step_row } }   (missing key -> {})
    #   all_attempts  -> { workflow_id => [attempt_row, ...] }          (insertion order; missing key -> [])
    #   all_waits     -> { wait_id => wait_row }
    #   all_outbox    -> { outbox_id => message_row }
    #   all_fences    -> [ fence_row, ... ]   each with "workflow_id","key","status",
    #                                         and (where the backend models leases)
    #                                         "locked_by"/"locked_until"
    #   all_inbox     -> { inbox_id => message_row }   target-oriented (no workflow_id);
    #                                         "status","locked_by"/"locked_until" where
    #                                         the backend models leases
    module StoreInspection
      #: () -> untyped
      def all_workflows = raise(NotImplementedError, "#{self.class}#all_workflows")
      #: () -> untyped
      def all_steps = raise(NotImplementedError, "#{self.class}#all_steps")
      #: () -> untyped
      def all_attempts = raise(NotImplementedError, "#{self.class}#all_attempts")
      #: () -> untyped
      def all_waits = raise(NotImplementedError, "#{self.class}#all_waits")
      #: () -> untyped
      def all_outbox = raise(NotImplementedError, "#{self.class}#all_outbox")
      #: () -> untyped
      def all_fences = raise(NotImplementedError, "#{self.class}#all_fences")
      #: () -> untyped
      def all_inbox = raise(NotImplementedError, "#{self.class}#all_inbox")
    end
  end
end
