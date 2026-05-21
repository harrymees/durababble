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

  it "can define an empty workflow without a DSL block" do
    workflow = described_class.define(:empty)

    expect(workflow.name).to eq("empty")
    expect(workflow.steps).to eq([])
  end

  it "rejects steps without executable handlers" do
    workflow = described_class.new("broken")

    expect { workflow.step("missing_handler") }.to raise_error(ArgumentError, /step requires a block/)
  end
end
