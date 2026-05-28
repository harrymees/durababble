# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababblePayloadLimitTest < DurababbleTestCase
  durababble_store_backends.each do |backend|
    test "enforces workflow input limits after serialization with #{backend.name}" do
      with_durababble_store(backend, "payload_workflow_input") do |store|
        payload = payload_value("workflow-input")
        size = serialized_size(payload)

        with_payload_limit(:workflow_input, size + 1) do
          id = store.enqueue_workflow(name: "payload-input-under", input: payload)
          assert_equal payload, store.workflow(id).fetch("input")
        end

        with_payload_limit(:workflow_input, size) do
          id = store.enqueue_workflow(name: "payload-input-at", input: payload)
          assert_equal payload, store.workflow(id).fetch("input")
        end

        before = count_rows(store, "workflows")
        error = with_payload_limit(:workflow_input, size - 1) do
          assert_raises(Durababble::PayloadTooLarge) do
            store.enqueue_workflow(name: "payload-input-over", input: payload)
          end
        end
        assert_limit_error(error, surface: :workflow_input, context: "workflow payload-input-over")
        assert_equal before, count_rows(store, "workflows")
      end
    end

    test "enforces workflow result limits without replacing running rows with #{backend.name}" do
      with_durababble_store(backend, "payload_workflow_result") do |store|
        payload = payload_value("workflow-result")
        size = serialized_size(payload)

        with_payload_limit(:workflow_result, size + 1) do
          id = store.create_workflow(name: "payload-result-under", input: {})
          store.complete_workflow(id, result: payload)
          assert_equal payload, store.workflow(id).fetch("result")
        end

        with_payload_limit(:workflow_result, size) do
          id = store.create_workflow(name: "payload-result-at", input: {})
          store.complete_workflow(id, result: payload)
          assert_equal payload, store.workflow(id).fetch("result")
        end

        id = store.create_workflow(name: "payload-result-over", input: {})
        error = with_payload_limit(:workflow_result, size - 1) do
          assert_raises(Durababble::PayloadTooLarge) do
            store.complete_workflow(id, result: payload)
          end
        end
        assert_limit_error(error, surface: :workflow_result, context: "workflow #{id} result")
        row = store.workflow(id)
        assert_equal "running", row.fetch("status")
        assert_nil row["result"]
      end
    end

    test "enforces step output limits before step and history completion writes with #{backend.name}" do
      with_durababble_store(backend, "payload_step_output") do |store|
        payload = payload_value("step-output")
        size = serialized_size(payload)

        with_payload_limit(:step_output, size + 1) do
          id = prepare_step(store, "payload-step-under")
          store.record_step_completed(workflow_id: id, command_id: 0, result: payload, worker_id: "worker", event_index: next_event_index(id, store:))
          assert_equal payload, store.steps_for(id).first.fetch("result")
        end

        with_payload_limit(:step_output, size) do
          id = prepare_step(store, "payload-step-at")
          store.record_step_completed(workflow_id: id, command_id: 0, result: payload, worker_id: "worker", event_index: next_event_index(id, store:))
          assert_equal payload, store.steps_for(id).first.fetch("result")
        end

        id = prepare_step(store, "payload-step-over")
        error = with_payload_limit(:step_output, size - 1) do
          assert_raises(Durababble::PayloadTooLarge) do
            store.record_step_completed(workflow_id: id, command_id: 0, result: payload, worker_id: "worker", event_index: next_event_index(id, store:))
          end
        end
        assert_limit_error(error, surface: :step_output, context: "workflow #{id} command 0")
        assert_equal ["step_scheduled", "step_started"], store.workflow_history_for(id).map { |event| event.fetch("kind") }
        assert_equal "running", store.steps_for(id).first.fetch("status")
      end
    end

    test "enforces durable object state limits without replacing existing state with #{backend.name}" do
      with_durababble_store(backend, "payload_object_state") do |store|
        payload = payload_value("object-state")
        size = serialized_size(payload)

        with_payload_limit(:object_state, size + 1) do
          store.save_object_state(object_type: "PayloadObject", object_id: "under", state: payload)
          assert_equal payload, store.object_state(object_type: "PayloadObject", object_id: "under")
        end

        with_payload_limit(:object_state, size) do
          store.save_object_state(object_type: "PayloadObject", object_id: "at", state: payload)
          assert_equal payload, store.object_state(object_type: "PayloadObject", object_id: "at")
        end

        original = { "body" => "small" }
        store.save_object_state(object_type: "PayloadObject", object_id: "over", state: original)
        error = with_payload_limit(:object_state, size - 1) do
          assert_raises(Durababble::PayloadTooLarge) do
            store.save_object_state(object_type: "PayloadObject", object_id: "over", state: payload)
          end
        end
        assert_limit_error(error, surface: :object_state, context: "PayloadObject/over")
        assert_equal original, store.object_state(object_type: "PayloadObject", object_id: "over")
      end
    end

    test "enforces inbox payload limits and rolls back enqueue bookkeeping with #{backend.name}" do
      with_durababble_store(backend, "payload_inbox") do |store|
        argument = payload_value("inbox-argument")
        payload = object_command_payload(argument)
        size = serialized_size(payload)

        with_payload_limit(:inbox_payload, size + 1) do
          id = store.enqueue_object_command(object_type: "PayloadObject", object_id: "under", method_name: "write", args: [argument], kwargs: {})
          assert_equal payload, store.inbox_message(id).fetch("payload")
        end

        with_payload_limit(:inbox_payload, size) do
          id = store.enqueue_object_command(object_type: "PayloadObject", object_id: "at", method_name: "write", args: [argument], kwargs: {})
          assert_equal payload, store.inbox_message(id).fetch("payload")
        end

        before = ["inbox", "mailbox_sequences", "target_activations"].to_h { |table| [table, count_rows(store, table)] }
        error = with_payload_limit(:inbox_payload, size - 1) do
          assert_raises(Durababble::PayloadTooLarge) do
            store.enqueue_object_command(object_type: "PayloadObject", object_id: "over", method_name: "write", args: [argument], kwargs: {})
          end
        end
        assert_limit_error(error, surface: :inbox_payload, context: "object PayloadObject/over ask")
        assert_equal before, before.keys.to_h { |table| [table, count_rows(store, table)] }
      end
    end

    test "rolls back object state when inbox result serialization is too large with #{backend.name}" do
      with_durababble_store(backend, "payload_inbox_result") do |store|
        id = store.enqueue_object_command(object_type: "PayloadObject", object_id: "result-over", method_name: "write", args: [], kwargs: {})
        store.claim_object_command(command_id: id, worker_id: "worker")
        result = payload_value("inbox-result")
        size = serialized_size(result)

        error = with_payload_limit(:inbox_payload, size - 1) do
          assert_raises(Durababble::PayloadTooLarge) do
            store.complete_object_command(
              command_id: id,
              result:,
              object_type: "PayloadObject",
              object_id: "result-over",
              state: { "saved" => true },
              worker_id: "worker",
            )
          end
        end
        assert_limit_error(error, surface: :inbox_payload, context: "inbox message #{id} result")
        assert_nil store.object_state(object_type: "PayloadObject", object_id: "result-over")
        assert_equal "running", store.inbox_message(id).fetch("status")
      end
    end

    test "enforces object wakeup payload limits when scheduling a wakeup with #{backend.name}" do
      with_durababble_store(backend, "payload_object_wakeup") do |store|
        payload = payload_value("object-wakeup")
        size = serialized_size(payload)
        wake_at = Time.utc(2026, 5, 25, 12, 0, 0)

        under_id = store.enqueue_object_command(object_type: "PayloadObject", object_id: "wakeup-under", method_name: "sleep", args: [], kwargs: {})
        store.claim_object_command(command_id: under_id, worker_id: "worker")
        with_payload_limit(:inbox_payload, size + 1) do
          store.complete_object_command(
            command_id: under_id,
            result: nil,
            object_type: "PayloadObject",
            object_id: "wakeup-under",
            wakeup_changes: [Durababble::ObjectWakeupChange.new(:schedule, "timer", wake_at, payload)],
            worker_id: "worker",
          )
        end
        assert_equal 1, count_rows(store, "object_wakeups")

        over_id = store.enqueue_object_command(object_type: "PayloadObject", object_id: "wakeup-over", method_name: "sleep", args: [], kwargs: {})
        store.claim_object_command(command_id: over_id, worker_id: "worker")
        before = ["object_wakeups", "inbox"].to_h { |table| [table, count_rows(store, table)] }
        error = with_payload_limit(:inbox_payload, size - 1) do
          assert_raises(Durababble::PayloadTooLarge) do
            store.complete_object_command(
              command_id: over_id,
              result: nil,
              object_type: "PayloadObject",
              object_id: "wakeup-over",
              wakeup_changes: [Durababble::ObjectWakeupChange.new(:schedule, "timer", wake_at, payload)],
              worker_id: "worker",
            )
          end
        end
        assert_limit_error(error, surface: :inbox_payload, context: "object wakeup PayloadObject/wakeup-over (timer)")
        # The command transaction rolls back, so no wakeup row is persisted and the sweeper never sees a
        # poison-pill payload; the command is left running for ordinary retry handling.
        assert_equal before, before.keys.to_h { |table| [table, count_rows(store, table)] }
        assert_equal "running", store.inbox_message(over_id).fetch("status")
      end
    end
  end

  private

  def payload_value(label)
    { "label" => label, "body" => "x" * 64 }
  end

  def object_command_payload(argument)
    { "method_name" => "write", "args" => [argument], "kwargs" => {} }
  end

  def serialized_size(value)
    Durababble::Store::SERIALIZER.dump(value).bytesize
  end

  def with_payload_limit(surface, value)
    configured = Durababble.instance_variable_defined?(:@payload_limits)
    previous = Durababble.instance_variable_get(:@payload_limits) if configured
    Durababble.payload_limits = { surface => value }
    yield
  ensure
    if configured
      Durababble.instance_variable_set(:@payload_limits, previous)
    elsif Durababble.instance_variable_defined?(:@payload_limits)
      Durababble.remove_instance_variable(:@payload_limits)
    end
  end

  def prepare_step(store, name)
    workflow_id = store.create_workflow(name:, input: {}, worker_id: "worker")
    store.record_step_scheduled(workflow_id:, command_id: 0, name: "write", worker_id: "worker", event_index: next_event_index(workflow_id, store:))
    store.record_step_started(workflow_id:, command_id: 0, name: "write", worker_id: "worker", event_index: next_event_index(workflow_id, store:))
    workflow_id
  end

  def count_rows(store, table)
    store.send(:execute_params, "SELECT COUNT(*) AS count FROM #{store.send(:table, table)}", []).first.fetch("count").to_i
  end

  def assert_limit_error(error, surface:, context:)
    assert_equal(surface, error.surface)
    assert_equal(context, error.context)
    assert_match(/#{surface.to_s.tr("_", " ")} payload/, error.message)
    assert_match(/#{Regexp.escape(context)}/, error.message)
    refute_match(/x{16}/, error.message)
  end
end
