# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleOperatorAppTest < DurababbleTestCase
  class FakeStore
    attr_reader :schema

    def initialize
      @schema = "operator_test"
      @workflow = {
        "id" => "wf-1",
        "name" => "nightly_import",
        "status" => "running",
        "input" => { "shop" => "secret-shop-value" },
        "result" => nil,
        "error" => nil,
        "locked_by" => "worker-a",
        "locked_until" => Time.now + 30,
        "next_run_at" => nil,
        "created_at" => Time.now - 60,
        "updated_at" => Time.now,
      }
    end

    def migrate!
      self
    end

    def list_workflows(status: nil, limit: 50)
      rows = [@workflow]
      rows = rows.select { |workflow| workflow.fetch("status") == status } if status
      rows.first(limit)
    end

    def workflow(workflow_id)
      raise KeyError, workflow_id unless workflow_id == @workflow.fetch("id")

      @workflow
    end

    def steps_for(_workflow_id)
      [
        { "position" => 0, "name" => "fetch_page", "status" => "completed", "started_at" => Time.now - 20, "completed_at" => Time.now - 10, "error" => nil },
        { "position" => 1, "name" => "transform", "status" => "running", "started_at" => Time.now - 5, "completed_at" => nil, "error" => nil },
      ]
    end

    def step_attempts_for(_workflow_id)
      [{ "position" => 1, "name" => "transform", "status" => "running", "started_at" => Time.now - 5, "completed_at" => nil, "error" => nil }]
    end

    def waits_for(_workflow_id)
      [{ "position" => 2, "kind" => "event", "status" => "pending", "event_key" => "approval", "wake_at" => nil, "completed_at" => nil }]
    end

    def list_outbox_messages(workflow_id:, limit: 50)
      [{ "workflow_id" => workflow_id, "topic" => "imports", "key" => "imports:wf-1", "status" => "pending", "locked_by" => nil, "locked_until" => nil, "processed_at" => nil }].first(limit)
    end

    def list_durable_objects(limit: 50)
      [{ "object_type" => "account", "object_id" => "acct-1", "state" => { "balance" => 10 }, "locked_by" => nil, "locked_until" => nil, "created_at" => Time.now - 40, "updated_at" => Time.now - 2 }].first(limit)
    end

    def object_state(object_type:, object_id:)
      raise KeyError, "#{object_type}/#{object_id}" unless object_type == "account" && object_id == "acct-1"

      { "balance" => 10 }
    end

    def list_object_commands(object_type:, object_id:, limit: 50)
      [{ "object_type" => object_type, "object_id" => object_id, "method_name" => "credit", "status" => "completed", "locked_by" => nil, "locked_until" => nil, "created_at" => Time.now - 4, "completed_at" => Time.now - 3, "error" => nil }].first(limit)
    end
  end

  def app
    Durababble::Operator::App.new(store: FakeStore.new)
  end

  test "renders workflow overview with mount-aware links and redacted payload summaries" do
    status, headers, body = call_app(app, path: "/workflows", script_name: "/durababble/operator")

    assert_equal 200, status
    assert_equal "text/html; charset=utf-8", headers.fetch("content-type")
    html = body.join
    assert_includes html, "Durababble Operator"
    assert_includes html, "operator_test"
    assert_includes html, "/durababble/operator/workflows/wf-1"
    assert_includes html, "nightly_import"
    refute_includes html, "secret-shop-value"
  end

  test "renders workflow detail, object list, and object detail" do
    workflow_status, = call_app(app, path: "/workflows/wf-1")
    object_list_status, = call_app(app, path: "/objects")
    object_detail_status, _headers, object_detail_body = call_app(app, path: "/objects/account/acct-1")

    assert_equal 200, workflow_status
    assert_equal 200, object_list_status
    assert_equal 200, object_detail_status
    assert_includes object_detail_body.join, "Command History"
  end

  test "can sit behind host auth middleware without owning authentication" do
    mounted = app
    auth_middleware = lambda do |env|
      next [401, { "content-type" => "text/plain" }, ["unauthorized"]] unless env["operator.user"]

      mounted.call(env)
    end

    unauthorized, = call_app(auth_middleware, path: "/workflows")
    authorized, _headers, body = call_app(auth_middleware, path: "/workflows", extra_env: { "operator.user" => "alice" })

    assert_equal 401, unauthorized
    assert_equal 200, authorized
    assert_includes body.join, "Workflows"
  end

  test "returns plain health response and not found pages" do
    health_status, health_headers, health_body = call_app(app, path: "/health")
    missing_status, _headers, missing_body = call_app(app, path: "/missing")

    assert_equal 200, health_status
    assert_equal "text/plain; charset=utf-8", health_headers.fetch("content-type")
    assert_equal "ok\n", health_body.join
    assert_equal 404, missing_status
    assert_includes missing_body.join, "Page not found"
  end

  private

  def call_app(rack_app, path:, script_name: "", query_string: "", extra_env: {})
    rack_app.call({
      "REQUEST_METHOD" => "GET",
      "SCRIPT_NAME" => script_name,
      "PATH_INFO" => path,
      "QUERY_STRING" => query_string,
    }.merge(extra_env))
  end
end
