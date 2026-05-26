# typed: true
# frozen_string_literal: true

module Durababble
  # Shared rendering for the single `error` string Durababble persists on a
  # failed workflow, step, or object command. Embedding a capped backtrace keeps
  # failures diagnosable from storage alone instead of just `"Class: message"`,
  # while the column stays bounded.
  module ErrorFormatting
    # Number of backtrace frames retained in a persisted failure. The innermost
    # frames (where the error was raised) sit at the top, so capping the tail
    # keeps the column bounded without losing the frames that matter for
    # debugging.
    ERROR_BACKTRACE_LIMIT = 50

    extend self

    #: (Exception) -> String
    def format_error(error)
      message = "#{error.class}: #{error.message}"
      backtrace = error.backtrace
      return message if backtrace.nil? || backtrace.empty?

      [message, *backtrace.first(ERROR_BACKTRACE_LIMIT)].join("\n")
    end
  end
end
