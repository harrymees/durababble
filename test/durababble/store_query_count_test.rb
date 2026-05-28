# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleStoreQueryCountTest < DurababbleTestCase
  module SqlQueryCounter
    def execute_store_query_sql(sql, params)
      if (recorder = instance_variable_get(:@durababble_sql_query_count_recorder))
        recorder << sql
      end

      super
    end

    private :execute_store_query_sql
  end

  durababble_store_backends.each do |backend|
    test "workflow and lease hot paths stay within SQL query count budgets with #{backend.name}" do
      with_durababble_store(backend, "query_counts_workflow") do
        install_sql_query_counter

        workflow_id = assert_sql_query_budget("enqueue_workflow", mysql: 1, postgres: 1) do
          store.enqueue_workflow(name: "query-count-enqueue", input: { "ok" => true })
        end
        assert_hash_includes store.workflow(workflow_id), "status" => "pending"
        store.claim_workflow(workflow_id:, worker_id: "cleanup-worker", lease_seconds: 30)

        runnable_id = store.enqueue_workflow(name: "query-count-claim", input: { "ok" => true })
        claimed = assert_sql_query_budget("claim_runnable_workflow", mysql: 3, postgres: 1) do
          store.claim_runnable_workflow(worker_id: "claim-worker", lease_seconds: 30)
        end
        assert_hash_includes claimed, "id" => runnable_id, "status" => "running"

        targeted_id = store.enqueue_workflow(name: "query-count-targeted-claim", input: { "ok" => true })
        targeted = assert_sql_query_budget("claim_workflow", mysql: 3, postgres: 2) do
          store.claim_workflow(workflow_id: targeted_id, worker_id: "target-worker", lease_seconds: 30)
        end
        assert_hash_includes targeted, "id" => targeted_id, "status" => "running"

        heartbeat_id = store.create_workflow(name: "query-count-heartbeat", input: {}, worker_id: "heartbeat-worker", lease_seconds: 30)
        heartbeat = assert_sql_query_budget("heartbeat", mysql: 1, postgres: 1) do
          store.heartbeat(workflow_id: heartbeat_id, worker_id: "heartbeat-worker", lease_seconds: 30)
        end
        assert_equal 1, heartbeat.affected_rows.to_i

        prepare_release_worker_leases_fixture
        released = assert_sql_query_budget("release_worker_leases", mysql: 9, postgres: 5) do
          store.release_worker_leases!(worker_id: "release-worker")
        end
        assert_equal 1, released.fetch("workflows")
        assert_equal 1, released.fetch("outbox")
        assert_equal 1, released.fetch("inbox")
        assert_equal 1, released.fetch("target_activations")
        assert_equal 1, released.fetch("durable_objects")
      end
    end

    test "step wait and timer hot paths stay within SQL query count budgets with #{backend.name}" do
      with_durababble_store(backend, "query_counts_steps") do
        install_sql_query_counter

        started_workflow = store.create_workflow(name: "query-count-step-start", input: {})
        started_index = next_event_index(started_workflow)
        attempt_id = assert_sql_query_budget("record_step_started", mysql: 4, postgres: 5) do
          store.record_step_started(
            workflow_id: started_workflow,
            command_id: 0,
            name: "step",
            event_index: started_index,
          )
        end
        refute_nil attempt_id

        completed_workflow = store.create_workflow(name: "query-count-step-complete", input: {})
        store.record_step_started(
          workflow_id: completed_workflow,
          command_id: 0,
          name: "step",
          event_index: next_event_index(completed_workflow),
        )
        completed_index = next_event_index(completed_workflow)
        assert_sql_query_budget("record_step_completed", mysql: 3, postgres: 4) do
          store.record_step_completed(
            workflow_id: completed_workflow,
            command_id: 0,
            result: { "done" => true },
            event_index: completed_index,
          )
        end
        assert_hash_includes store.steps_for(completed_workflow).first, "status" => "completed", "result" => { "done" => true }

        wait_workflow = store.create_workflow(name: "query-count-record-wait", input: {})
        store.record_step_started(
          workflow_id: wait_workflow,
          command_id: 0,
          name: "timer",
          event_index: next_event_index(wait_workflow),
        )
        wait_index = next_event_index(wait_workflow)
        wait_id = assert_sql_query_budget("record_wait", mysql: 3, postgres: 4) do
          store.record_wait(
            workflow_id: wait_workflow,
            command_id: 0,
            name: "timer",
            wait_request: Durababble.wait_until(Time.utc(2026, 2, 1, 0, 0, 0), { "timer" => true }),
            suspend_workflow: false,
            event_index: wait_index,
          )
        end
        refute_nil wait_id

        assert_sql_query_budget("wait_snapshots_for", mysql: 2, postgres: 2) do
          snapshots = store.wait_snapshots_for(wait_workflow)
          assert_equal ["pending"], snapshots.map { |wait| wait.fetch("status") }
        end
      end
    end

    test "outbox mailbox and object hot paths stay within SQL query count budgets with #{backend.name}" do
      with_durababble_store(backend, "query_counts_mailbox") do
        install_sql_query_counter

        outbox_workflow = store.enqueue_workflow(name: "query-count-outbox", input: {})
        outbox_id = assert_sql_query_budget("enqueue_outbox", mysql: 3, postgres: 3) do
          store.enqueue_outbox(workflow_id: outbox_workflow, topic: "events", payload: { "ok" => true }, key: "query-count-outbox")
        end
        assert_hash_includes store.outbox_message(outbox_id), "status" => "pending"
        store.claim_outbox(worker_id: "cleanup-outbox-worker", lease_seconds: 30)

        claim_outbox_workflow = store.enqueue_workflow(name: "query-count-claim-outbox", input: {})
        claim_outbox_id = store.enqueue_outbox(workflow_id: claim_outbox_workflow, topic: "events", payload: { "ok" => true }, key: "query-count-claim-outbox")
        claimed_outbox = assert_sql_query_budget("claim_outbox", mysql: 3, postgres: 1) do
          store.claim_outbox(worker_id: "outbox-worker", lease_seconds: 30)
        end
        assert_hash_includes claimed_outbox, "id" => claim_outbox_id, "status" => "processing"

        enqueued_command_id = assert_sql_query_budget("enqueue_object_command", mysql: 5, postgres: 5) do
          enqueue_object_command("enqueue-object")
        end
        assert_hash_includes store.inbox_message(enqueued_command_id), "status" => "pending"

        direct_claim_command_id = enqueue_object_command("direct-claim-object")
        direct_claim = assert_sql_query_budget("claim_object_command", mysql: 5, postgres: 4) do
          store.claim_object_command(command_id: direct_claim_command_id, worker_id: "object-worker", lease_seconds: 30)
        end
        assert_hash_includes direct_claim, "id" => direct_claim_command_id, "status" => "running"

        inbox_claim_command_id = enqueue_object_command("inbox-claim-object")
        inbox_messages = assert_sql_query_budget("claim_inbox_messages", mysql: 4, postgres: 3) do
          store.claim_inbox_messages(
            target_kind: "object",
            target_type: "counter",
            target_id: "inbox-claim-object",
            worker_id: "inbox-worker",
            lease_seconds: 30,
            limit: 1,
          )
        end
        assert_equal [inbox_claim_command_id], inbox_messages.map { |message| message.fetch("id") }

        target_activation_command_id = enqueue_object_command("target-activation-claim-object", object_type: "query-count-target")
        target_activation = assert_sql_query_budget("claim_target_activation", mysql: 3, postgres: 2) do
          store.claim_target_activation(worker_id: "target-activation-worker", lease_seconds: 30, target_kinds: ["object"], target_types: ["query-count-target"])
        end
        assert_hash_includes target_activation, "target_kind" => "object", "target_type" => "query-count-target", "target_id" => "target-activation-claim-object", "status" => "running"
        assert_hash_includes store.inbox_message(target_activation_command_id), "status" => "pending"

        completed = assert_sql_query_budget("complete_object_command", mysql: 6, postgres: 6) do
          store.complete_object_command(
            command_id: inbox_claim_command_id,
            object_type: "counter",
            object_id: "inbox-claim-object",
            state: { "count" => 1 },
            result: { "count" => 1 },
            worker_id: "inbox-worker",
          )
        end
        assert_equal 1, completed.affected_rows.to_i
        assert_equal({ "count" => 1 }, store.object_state(object_type: "counter", object_id: "inbox-claim-object"))
      end
    end
  end

  private

  def install_sql_query_counter
    store.singleton_class.prepend(SqlQueryCounter) unless store.singleton_class.ancestors.include?(SqlQueryCounter)
  end

  def assert_sql_query_budget(name, mysql:, postgres:)
    limit = backend_descriptor.mysql? ? mysql : postgres
    queries = []
    previous = store.instance_variable_get(:@durababble_sql_query_count_recorder)
    store.instance_variable_set(:@durababble_sql_query_count_recorder, queries)
    result = yield
    assert_operator(
      queries.length,
      :<=,
      limit,
      "#{name} issued #{queries.length} SQL statements, expected at most #{limit}\n#{format_queries(queries)}",
    )
    result
  ensure
    store.instance_variable_set(:@durababble_sql_query_count_recorder, previous)
  end

  def format_queries(queries)
    queries.each_with_index.map do |sql, index|
      "#{index + 1}. #{sql.gsub(/\s+/, " ").strip}"
    end.join("\n")
  end

  def prepare_release_worker_leases_fixture
    store.create_workflow(name: "query-count-release-workflow", input: {}, worker_id: "release-worker", lease_seconds: 30)

    workflow_id = store.enqueue_workflow(name: "query-count-release-outbox", input: {})
    store.enqueue_outbox(workflow_id:, topic: "events", payload: { "ok" => true }, key: "query-count-release-outbox")
    store.claim_outbox(worker_id: "release-worker", lease_seconds: 30)

    command_id = enqueue_object_command("release-object")
    store.claim_object_command(command_id:, worker_id: "release-worker", lease_seconds: 30)
    store.claim_target_activation(worker_id: "release-worker", lease_seconds: 30, target_kinds: ["object"], target_types: ["counter"])
  end

  def enqueue_object_command(object_id, object_type: "counter")
    store.enqueue_object_command(
      object_type:,
      object_id:,
      method_name: "increment",
      args: [1],
      kwargs: {},
    )
  end
end
