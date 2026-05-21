# frozen_string_literal: true

require "spec_helper"

RSpec.describe Durababble::Workflow do
  class ApiSpecChargeFailed < StandardError; end

  class ApiSpecOrderWorkflow < Durababble::Workflow
    expose def status
      "queryable"
    end

    expose_command def cancel(reason:)
      reason
    end

    def execute(input)
      charged = charge(input)
      finish(charged)
    end

    step retry: { maximum_attempts: 3, initial_interval: 1 }
    def charge(input)
      raise ApiSpecChargeFailed, "declined" if input.fetch("decline", false)

      input.merge("idempotency_key" => step_context.idempotency_key)
    end

    step def finish(input)
      input.merge("finished" => true)
    end
  end

  it "registers class-oriented workflow steps and public exposed methods" do
    expect(ApiSpecOrderWorkflow.workflow_name).to eq("api_spec_order_workflow")
    expect(ApiSpecOrderWorkflow.step_order).to eq(%i[charge finish])
    expect(ApiSpecOrderWorkflow.step_definition(:charge).retry_policy.maximum_attempts).to eq(3)
    expect(ApiSpecOrderWorkflow.exposed_queries).to include(status: true)
    expect(ApiSpecOrderWorkflow.exposed_commands.keys).to include(:cancel)
  end

  it "does not expose the removed Workflow.define DSL" do
    expect(described_class).not_to respond_to(:define)
  end
end
