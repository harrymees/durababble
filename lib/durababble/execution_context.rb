# typed: true
# frozen_string_literal: true

require "async"

Fiber.attr_accessor(:durababble_workflow_execution) unless Fiber.method_defined?(:durababble_workflow_execution)
Fiber.attr_accessor(:durababble_step_context) unless Fiber.method_defined?(:durababble_step_context)
Fiber.attr_accessor(:durababble_object_command_context) unless Fiber.method_defined?(:durababble_object_command_context)

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

  module AsyncTaskWorkflowContextPatch
    #: () { () -> Object? } -> Object?
    def schedule(&block)
      task = self #: as untyped
      step_context = StepExecutionContext.current
      object_context = ObjectCommandExecutionContext.current
      if task.transient?
        execution = WorkflowExecutionContext.current
        return super(&block) unless execution || step_context || object_context

        return super do
          WorkflowExecutionContext.with_current(nil) do
            StepExecutionContext.with_current(step_context) do
              ObjectCommandExecutionContext.with_current(object_context) { block.call }
            end
          end
        end
      end

      execution = WorkflowExecutionContext.current
      return super(&block) unless execution || step_context || object_context

      workflow_task = self #: as untyped
      execution&.register_workflow_task(workflow_task)
      super do
        WorkflowExecutionContext.with_current(execution) do
          StepExecutionContext.with_current(step_context) do
            ObjectCommandExecutionContext.with_current(object_context) { block.call }
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
