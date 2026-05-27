# typed: true
# frozen_string_literal: true

module Durababble
  class WorkflowQueryRegistry
    #: () -> void
    def initialize
      @mutex = Mutex.new
      @entries = {}
    end

    #: (String, singleton(Workflow), Workflow) { () -> Object? } -> Object?
    def register(workflow_id, workflow_class, workflow, &block)
      @mutex.synchronize do
        @entries[workflow_id] = {
          workflow_class:,
          workflow:,
        }
      end
      block.call
    ensure
      @mutex.synchronize do
        entry = @entries[workflow_id]
        @entries.delete(workflow_id) if entry && entry.fetch(:workflow).equal?(workflow)
      end
    end

    #: (workflow_id: String, method_name: Symbol, args: Array[Object?], kwargs: Hash[Symbol, Object?]) -> Object?
    def call(workflow_id:, method_name:, args:, kwargs:)
      entry = @mutex.synchronize { @entries[workflow_id] }
      raise WorkflowRpc::NoActiveLease, "workflow #{workflow_id} is not active on this runtime" unless entry

      workflow_class = entry.fetch(:workflow_class)
      method_name = method_name.to_sym
      unless workflow_class.exposed_queries.key?(method_name)
        raise WorkflowRpc::UnknownCommand, "unknown workflow query #{method_name}"
      end

      workflow = entry.fetch(:workflow)
      workflow.__durababble_with_query_context__ do
        kwargs.empty? ? workflow.public_send(method_name, *args) : workflow.public_send(method_name, *args, **kwargs)
      end
    end
  end
end
