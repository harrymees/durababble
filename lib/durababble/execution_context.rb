# typed: true
# frozen_string_literal: true

require "async"

Fiber.attr_accessor(:durababble_workflow_execution) unless Fiber.method_defined?(:durababble_workflow_execution)
Fiber.attr_accessor(:durababble_step_context) unless Fiber.method_defined?(:durababble_step_context)
Fiber.attr_accessor(:durababble_object_command_context) unless Fiber.method_defined?(:durababble_object_command_context)
Fiber.attr_accessor(:durababble_object_query_context) unless Fiber.method_defined?(:durababble_object_query_context)
Fiber.attr_accessor(:durababble_stream_writer) unless Fiber.method_defined?(:durababble_stream_writer)
Fiber.attr_accessor(:durababble_workflow_query_context) unless Fiber.method_defined?(:durababble_workflow_query_context)

module Durababble
  StepContext = Data.define(:workflow_id, :step_index, :attempt_number, :idempotency_key, :heartbeat)

  Heartbeat = Data.define(:cursor, :recorder) do
    #: (?Object?) -> Object?
    def record(cursor = self.cursor)
      recorder.call(cursor)
    end

    alias_method :heartbeat, :record
  end

  module WorkflowExecutionContext
    class << self
      #: () -> WorkflowExecution?
      def current
        fiber = Fiber.current #: as untyped
        fiber.durababble_workflow_execution #: as WorkflowExecution?
      end

      #: (Object?) { () -> Object? } -> Object?
      def with_current(execution, &block)
        fiber = Fiber.current #: as untyped
        previous = fiber.durababble_workflow_execution
        fiber.durababble_workflow_execution = execution
        block.call
      ensure
        fiber.durababble_workflow_execution = previous
      end
    end
  end

  module StepExecutionContext
    class << self
      #: () -> StepContext?
      def current
        fiber = Fiber.current #: as untyped
        fiber.durababble_step_context #: as StepContext?
      end

      #: (StepContext?) { () -> Object? } -> Object?
      def with_current(context, &block)
        fiber = Fiber.current #: as untyped
        previous = fiber.durababble_step_context
        fiber.durababble_step_context = context
        block.call
      ensure
        fiber.durababble_step_context = previous
      end
    end
  end

  module ObjectCommandExecutionContext
    class << self
      #: () -> Object?
      def current
        fiber = Fiber.current #: as untyped
        fiber.durababble_object_command_context
      end

      #: (Object?) { () -> Object? } -> Object?
      def with_current(object, &block)
        fiber = Fiber.current #: as untyped
        previous = fiber.durababble_object_command_context
        fiber.durababble_object_command_context = object
        block.call
      ensure
        fiber.durababble_object_command_context = previous
      end
    end
  end

  # Points at the active streaming-result writer (a `ResultStream::Writer` for a
  # local snapshot producer, or an `Rpc::StreamWriter`/lease-checking wrapper for
  # a server-side producer). The exposed_stream method body runs in the same
  # fiber as the producer, so `Durababble.stream_cancelled?` can read it to learn
  # whether the consumer has gone away.
  module StreamExecutionContext
    class << self
      #: () -> untyped
      def current
        fiber = Fiber.current #: as untyped
        fiber.durababble_stream_writer
      end

      #: (untyped) { () -> Object? } -> Object?
      def with_current(writer, &block)
        fiber = Fiber.current #: as untyped
        previous = fiber.durababble_stream_writer
        fiber.durababble_stream_writer = writer
        block.call
      ensure
        fiber.durababble_stream_writer = previous
      end
    end
  end

  module WorkflowQueryContext
    class << self
      #: () -> bool
      def current
        fiber = Fiber.current #: as untyped
        !!fiber.durababble_workflow_query_context
      end

      #: (bool) { () -> Object? } -> Object?
      def with_current(context, &block)
        fiber = Fiber.current #: as untyped
        previous = fiber.durababble_workflow_query_context
        fiber.durababble_workflow_query_context = context
        block.call
      ensure
        fiber.durababble_workflow_query_context = previous
      end
    end
  end

  module ObjectQueryExecutionContext
    class << self
      #: () -> Object?
      def current
        fiber = Fiber.current #: as untyped
        fiber.durababble_object_query_context
      end

      #: (Object?) { () -> Object? } -> Object?
      def with_current(object, &block)
        fiber = Fiber.current #: as untyped
        previous = fiber.durababble_object_query_context
        fiber.durababble_object_query_context = object
        block.call
      ensure
        fiber.durababble_object_query_context = previous
      end
    end
  end

  module AsyncTaskWorkflowContextPatch
    #: () { () -> Object? } -> Object?
    def schedule(&block)
      task = self #: as untyped
      step_context = StepExecutionContext.current
      query_context = WorkflowQueryContext.current
      object_context = ObjectCommandExecutionContext.current
      object_query_context = ObjectQueryExecutionContext.current
      stream_writer = StreamExecutionContext.current
      if task.transient?
        execution = WorkflowExecutionContext.current
        return super(&block) unless execution || step_context || query_context || object_context || object_query_context || stream_writer

        return super do
          WorkflowExecutionContext.with_current(nil) do
            StepExecutionContext.with_current(step_context) do
              WorkflowQueryContext.with_current(query_context) do
                ObjectCommandExecutionContext.with_current(object_context) do
                  ObjectQueryExecutionContext.with_current(object_query_context) do
                    StreamExecutionContext.with_current(stream_writer) { block.call }
                  end
                end
              end
            end
          end
        end
      end

      execution = WorkflowExecutionContext.current
      return super(&block) unless execution || step_context || query_context || object_context || object_query_context || stream_writer

      workflow_task = self #: as untyped
      execution&.register_workflow_task(workflow_task)
      super do
        WorkflowExecutionContext.with_current(execution) do
          StepExecutionContext.with_current(step_context) do
            WorkflowQueryContext.with_current(query_context) do
              ObjectCommandExecutionContext.with_current(object_context) do
                ObjectQueryExecutionContext.with_current(object_query_context) do
                  StreamExecutionContext.with_current(stream_writer) { block.call }
                end
              end
            end
          end
        end
      ensure
        execution&.unregister_workflow_task(workflow_task)
      end
    end

    #: () -> Object?
    def wait
      execution = WorkflowExecutionContext.current
      return super() unless execution

      execution.block_current_workflow_task { super() }
    end
  end

  Async::Task.prepend(AsyncTaskWorkflowContextPatch) unless Async::Task < AsyncTaskWorkflowContextPatch
end
