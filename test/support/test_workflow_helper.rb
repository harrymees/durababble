# typed: false
# frozen_string_literal: true

module DurababbleTestWorkflowHelper
  def durababble_test_workflow(name, &definition)
    Class.new(Durababble::Workflow) do
      workflow_name name

      def execute(input)
        self.class.step_order.reduce(input) { |ctx, method_name| public_send(method_name, ctx) }
      end

      class << self
        def test_step(name, retry_policy: nil, &block)
          define_method(name) do |ctx|
            if block.arity >= 2
              block.call(ctx, step_context.heartbeat)
            else
              block.call(ctx)
            end
          end
          step(name, retry: retry_policy)
        end
      end

      class_eval(&definition) if definition
    end
  end
end
