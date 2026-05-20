# frozen_string_literal: true

require "spec_helper"

RSpec.describe Durababble::Workflow do
  it "defines ordered steps with names and callables" do
    workflow = described_class.define("checkout") do
      step("reserve_inventory") { |ctx| ctx.fetch(:sku) }
      step("charge_card") { "charged" }
    end

    expect(workflow.name).to eq("checkout")
    expect(workflow.steps.map(&:name)).to eq(["reserve_inventory", "charge_card"])
    expect(workflow.steps.first.call({ sku: "sku-1" })).to eq("sku-1")
  end
end
