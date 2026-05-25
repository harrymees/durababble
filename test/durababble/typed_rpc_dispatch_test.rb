# frozen_string_literal: true

require "open3"
require "test_helper"

class TypedRpcDispatchTest < DurababbleTestCase
  RBS_LOAD_PATH = ["-I", "sig", "-I", "test/fixtures/rbs"].freeze

  def test_public_rbs_parses_directly
    assert_rbs_success("parse", "sig/durababble.rbs")
  end

  def test_workflow_handle_dispatch_methods_are_target_specific
    ancestors_output = assert_rbs_success(*RBS_LOAD_PATH, "ancestors", "DurababbleTypeFixtures::TypedWorkflow")
    assert_includes(ancestors_output, "::Durababble::Workflow[::String, bool, ::DurababbleTypeFixtures::TypedWorkflowDispatch]")

    at_output = assert_rbs_success(*RBS_LOAD_PATH, "method", "--singleton", "DurababbleTypeFixtures::TypedWorkflow", "at")
    assert_includes(at_output, "workflow_handle[Input, Output, Dispatch]")
    handle_output = assert_rbs_success(*RBS_LOAD_PATH, "method", "--singleton", "DurababbleTypeFixtures::TypedWorkflow", "handle")
    assert_includes(handle_output, "workflow_handle[Input, Output, Dispatch]")
    assert_rbs_missing_method(*RBS_LOAD_PATH, "method", "--singleton", "DurababbleTypeFixtures::TypedWorkflow", "ref")

    approve_output = assert_rbs_success(*RBS_LOAD_PATH, "method", "DurababbleTypeFixtures::TypedWorkflowDispatch", "approve")
    assert_includes(approve_output, "(reason: ::String) -> bool")

    describe_output = assert_rbs_success(*RBS_LOAD_PATH, "method", "DurababbleTypeFixtures::TypedWorkflowDispatch", "describe")
    assert_includes(describe_output, "(::String prefix) -> ::String")

    assert_rbs_missing_method(*RBS_LOAD_PATH, "method", "DurababbleTypeFixtures::TypedWorkflowDispatch", "missing")
    refute_includes(approve_output, "(::Integer")
  end

  def test_durable_object_handle_dispatch_methods_are_target_specific
    ancestors_output = assert_rbs_success(*RBS_LOAD_PATH, "ancestors", "DurababbleTypeFixtures::TypedObject")
    assert_includes(ancestors_output, "::Durababble::DurableObject[::String, ::Hash[::String, ::Integer], ::DurababbleTypeFixtures::TypedObjectDispatch]")

    at_output = assert_rbs_success(*RBS_LOAD_PATH, "method", "--singleton", "DurababbleTypeFixtures::TypedObject", "at")
    assert_includes(at_output, "durable_object_handle[Id, State, Dispatch]")
    handle_output = assert_rbs_success(*RBS_LOAD_PATH, "method", "--singleton", "DurababbleTypeFixtures::TypedObject", "handle")
    assert_includes(handle_output, "durable_object_handle[Id, State, Dispatch]")
    assert_rbs_missing_method(*RBS_LOAD_PATH, "method", "--singleton", "DurababbleTypeFixtures::TypedObject", "ref")

    balance_output = assert_rbs_success(*RBS_LOAD_PATH, "method", "DurababbleTypeFixtures::TypedObjectDispatch", "balance")
    assert_includes(balance_output, "() -> ::Integer")

    credit_output = assert_rbs_success(*RBS_LOAD_PATH, "method", "DurababbleTypeFixtures::TypedObjectDispatch", "credit")
    assert_includes(credit_output, "(::Integer amount_cents, ?memo: ::String) -> ::Integer")

    assert_rbs_missing_method(*RBS_LOAD_PATH, "method", "DurababbleTypeFixtures::TypedObjectDispatch", "missing")
    refute_includes(credit_output, "(::String amount_cents")
  end

  private

  def assert_rbs_success(*args)
    stdout, stderr, status = rbs(*args)
    assert(status.success?, "expected rbs #{args.join(" ")} to pass\nstdout:\n#{stdout}\nstderr:\n#{stderr}")
    stdout
  end

  def assert_rbs_missing_method(*args)
    stdout, stderr, status = rbs(*args)
    assert(status.success?, "expected rbs #{args.join(" ")} to inspect missing method cleanly\nstdout:\n#{stdout}\nstderr:\n#{stderr}")
    assert_includes(stdout, "Cannot find method")
  end

  def rbs(*args)
    Open3.capture3({ "BUNDLE_GEMFILE" => File.expand_path("../../Gemfile", __dir__) }, "bundle", "exec", "rbs", *args)
  end
end
