# typed: true
# frozen_string_literal: true

require_relative "execution_context"

Fiber.attr_accessor(:durababble_determinism_allow_depth)

module Durababble
  module WorkflowDeterminism
    RANDOM_METHODS = [:rand, :srand, :bytes, :uuid, :hex, :random_bytes, :random_number, :urlsafe_base64, :base64].freeze
    SLEEP_METHODS = [:sleep].freeze
    TIME_METHODS = [:now, :today].freeze
    PROCESS_METHODS = [:clock_gettime, :times].freeze
    KERNEL_IO_METHODS = [:`, :exec, :open, :popen, :spawn, :system].freeze
    FILE_SYSTEM_METHODS = [
      :atime,
      :birthtime,
      :blockdev?,
      :chardev?,
      :children,
      :chmod,
      :chown,
      :ctime,
      :delete,
      :directory?,
      :empty?,
      :entries,
      :exist?,
      :exists?,
      :file?,
      :foreach,
      :ftype,
      :glob,
      :lstat,
      :mkdir,
      :mtime,
      :open,
      :readlink,
      :realpath,
      :rename,
      :rmdir,
      :stat,
      :symlink?,
      :truncate,
      :utime,
    ].freeze
    IO_METHODS = [
      :binread,
      :binwrite,
      :copy_stream,
      :foreach,
      :gets,
      :open,
      :popen,
      :print,
      :printf,
      :puts,
      :read,
      :read_nonblock,
      :readlines,
      :readpartial,
      :sysread,
      :syswrite,
      :write,
      :write_nonblock,
    ].freeze
    INTERNAL_CALLSITE_FRAGMENTS = [
      "#{File::SEPARATOR}lib#{File::SEPARATOR}durababble#{File::SEPARATOR}",
      "#{File::SEPARATOR}gems#{File::SEPARATOR}async-",
      "#{File::SEPARATOR}gems#{File::SEPARATOR}console-",
    ].freeze

    class << self
      #: (workflow_id: untyped) { -> untyped } -> untyped
      def enforce(workflow_id:, &block)
        trace = TracePoint.new(:call, :c_call) do |event|
          check_event!(workflow_id, event)
        end

        enable_trace(trace, &block)
      end

      #: () { -> untyped } -> untyped
      def allow_host_operations(&block)
        fiber = Fiber.current #: as untyped
        previous = fiber.durababble_determinism_allow_depth.to_i
        fiber.durababble_determinism_allow_depth = previous + 1
        block.call
      ensure
        fiber.durababble_determinism_allow_depth = previous
      end

      private

      #: (untyped, untyped, ?locations: Array[Thread::Backtrace::Location]?) -> void
      def check_event!(workflow_id, event, locations: nil)
        return unless WorkflowExecutionContext.current
        return if allowed_host_operation?

        violation, callsite_filtered = violation_candidate_for(event)
        return unless violation

        # TracePoint runs for every call; capture callsites only after cheap filtering.
        locations ||= caller_locations(1, 24) || []
        callsite = callsite_location(locations)
        return if callsite_filtered && !unsafe_callsite?(callsite)

        location = callsite ? " at #{callsite.path}:#{callsite.lineno}" : ""
        raise DeterminismError, "workflow #{workflow_id} orchestration cannot call #{violation}#{location}; move host I/O, wall-clock time, randomness, or blocking sleeps into a durable step"
      end

      #: (untyped) { -> untyped } -> untyped
      def enable_trace(trace, &block)
        begin
          trace.enable(target_thread: Thread.current)
        rescue ArgumentError
          trace.enable
        end

        block.call
      ensure
        trace.disable
      end

      #: () -> bool
      def allowed_host_operation?
        fiber = Fiber.current #: as untyped
        fiber.durababble_determinism_allow_depth.to_i.positive?
      end

      #: (untyped, callsite: untyped) -> String?
      def violation_for(event, callsite:)
        violation, callsite_filtered = violation_candidate_for(event)
        return unless violation
        return if callsite_filtered && !unsafe_callsite?(callsite)

        violation
      end

      #: (untyped) -> [String, bool]?
      def violation_candidate_for(event)
        method_id = event.method_id
        receiver = event.self
        defined_class = event.defined_class

        return ["Kernel##{method_id}", true] if SLEEP_METHODS.include?(method_id) && kernel_receiver?(receiver, defined_class)
        return ["Kernel##{method_id}", true] if KERNEL_IO_METHODS.include?(method_id) && kernel_receiver?(receiver, defined_class)
        return ["Kernel##{method_id}", true] if RANDOM_METHODS.include?(method_id) && kernel_receiver?(receiver, defined_class)
        return ["#{receiver}.#{method_id}", false] if RANDOM_METHODS.include?(method_id) && module_receiver_named?(receiver, "Random", "SecureRandom")
        return ["Random.new", false] if method_id == :initialize && class_named?(defined_class, "Random::Base")
        return ["Time.new", false] if method_id == :initialize && defined_class == Time
        return ["#{receiver}.#{method_id}", true] if TIME_METHODS.include?(method_id) && module_receiver_named?(receiver, "Time", "Date", "DateTime")
        return ["#{receiver}.#{method_id}", true] if PROCESS_METHODS.include?(method_id) && module_receiver_named?(receiver, "Process")
        return ["#{receiver}.#{method_id}", true] if IO_METHODS.include?(method_id) && io_receiver?(receiver)
        return ["#{receiver}.#{method_id}", true] if FILE_SYSTEM_METHODS.include?(method_id) && file_system_receiver?(receiver)

        nil
      end

      #: (Thread::Backtrace::Location?) -> bool
      def unsafe_callsite?(callsite)
        return true unless callsite

        path = callsite.path.to_s
        INTERNAL_CALLSITE_FRAGMENTS.none? { |fragment| path.include?(fragment) }
      end

      #: (Array[Thread::Backtrace::Location]) -> Thread::Backtrace::Location?
      def callsite_location(locations)
        locations.find do |location|
          path = location.path.to_s
          path != __FILE__ && !path.start_with?("<internal:")
        end
      end

      #: (untyped, untyped) -> bool
      def kernel_receiver?(receiver, defined_class)
        receiver == Kernel || defined_class == Kernel || defined_class.to_s.include?("Kernel")
      end

      #: (untyped, *String) -> bool
      def module_receiver_named?(receiver, *names)
        names.include?(receiver.to_s)
      end

      #: (untyped, String) -> bool
      def class_named?(defined_class, name)
        defined_class.respond_to?(:name) && defined_class.name.to_s == name
      end

      #: (untyped) -> bool
      def io_receiver?(receiver)
        return true if receiver.is_a?(IO)
        return true if receiver == IO || receiver == File

        !!(receiver.is_a?(Class) && receiver.ancestors.include?(IO))
      end

      #: (untyped) -> bool
      def file_system_receiver?(receiver)
        receiver == File || receiver == Dir
      end
    end
  end
end
