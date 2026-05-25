# typed: true
# frozen_string_literal: true

require "async"

Fiber.attr_accessor(:durababble_workflow_execution) unless Fiber.method_defined?(:durababble_workflow_execution)
Fiber.attr_accessor(:durababble_step_context) unless Fiber.method_defined?(:durababble_step_context)

module Durababble
  StepContext = Data.define(:workflow_id, :step_index, :attempt_number, :idempotency_key, :heartbeat)

  Heartbeat = Data.define(:cursor, :recorder) do
    #: (?untyped) -> untyped
    def record(cursor = self.cursor)
      recorder.call(cursor)
    end

    alias_method :heartbeat, :record
  end

  module WorkflowExecutionContext
    class << self
      #: () -> untyped
      def current
        fiber = Fiber.current #: as untyped
        fiber.durababble_workflow_execution
      end

      #: (untyped) { (?) -> untyped } -> untyped
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
      #: () -> untyped
      def current
        fiber = Fiber.current #: as untyped
        fiber.durababble_step_context
      end

      #: (untyped) { (?) -> untyped } -> untyped
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

  module AsyncTaskWorkflowContextPatch
    #: () { (?) -> untyped } -> untyped
    def schedule(&block)
      task = self #: as untyped
      step_context = StepExecutionContext.current
      if task.transient?
        execution = WorkflowExecutionContext.current
        return super(&block) unless execution || step_context

        return super do
          WorkflowExecutionContext.with_current(nil) do
            StepExecutionContext.with_current(step_context) { block.call }
          end
        end
      end

      execution = WorkflowExecutionContext.current
      return super(&block) unless execution || step_context

      execution&.register_workflow_task(self)
      super do
        WorkflowExecutionContext.with_current(execution) do
          StepExecutionContext.with_current(step_context) { block.call }
        end
      ensure
        execution&.unregister_workflow_task(self)
      end
    end

    #: () -> untyped
    def wait
      execution = WorkflowExecutionContext.current
      return super() unless execution

      execution.block_current_workflow_task { super() }
    end
  end

  Async::Task.prepend(AsyncTaskWorkflowContextPatch) unless Async::Task < AsyncTaskWorkflowContextPatch
end
