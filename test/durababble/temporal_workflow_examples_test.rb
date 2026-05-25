# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleTemporalWorkflowExamplesTest < DurababbleTestCase
  class TemporalExampleActivityError < StandardError; end

  durababble_store_backends.each do |backend|
    test "ports a Temporal-style order workflow with activity retry and durable timer with #{backend.name}" do
      with_temporal_example_store(backend, "temporal_order") do |store|
        attempts = Hash.new(0)
        wake_at = Time.utc(2026, 1, 1, 0, 0, 0)
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "temporal-order-fulfillment"

          define_method(:execute) do |order|
            payment = charge_card(order)
            reservation = reserve_inventory(payment)
            label = create_shipping_label(reservation)
            send_confirmation(wait_until(wake_at, label.merge("ready_to_notify" => true)))
          end

          define_method(:charge_card) do |order|
            attempts[:charge_card] += 1
            raise TemporalExampleActivityError, "transient payment error" if attempts[:charge_card] == 1

            order.merge(
              "payment_id" => "pay-#{order.fetch("order_id")}",
              "charge_idempotency_key" => step_context.idempotency_key,
            )
          end
          step :charge_card, retry: { maximum_attempts: 2, schedule: [0] }

          step def reserve_inventory(payment)
            payment.merge("reservation_id" => "res-#{payment.fetch("sku")}")
          end

          step def create_shipping_label(reservation)
            reservation.merge("label_id" => "ship-#{reservation.fetch("order_id")}")
          end

          step def send_confirmation(label)
            label.merge("confirmation_sent" => true)
          end
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.workflow_name => workflow },
          worker_id: "temporal-order-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "order_id" => "A100", "sku" => "sku-1" })

        assert_equal :worked, worker.tick
        assert_hash_includes store.workflow(workflow_id), "status" => "pending", "locked_by" => nil
        assert_equal ["failed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }

        store.make_workflow_due!(workflow_id, now: Time.now + 1)
        assert_equal :worked, worker.tick
        assert_hash_includes store.workflow(workflow_id), "status" => "waiting"
        assert_equal ["failed", "completed", "completed", "completed"], store.step_attempts_for(workflow_id).map { |attempt| attempt.fetch("status") }
        assert_equal ["pending"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
        assert_equal 0, store.wake_due_timers(now: wake_at - 1)

        assert_equal 1, store.wake_due_timers(now: wake_at + 1)
        assert_equal :worked, worker.tick

        result = store.workflow(workflow_id).fetch("result")
        assert_hash_includes(
          store.workflow(workflow_id),
          "status" => "completed",
          "result" => result,
        )
        assert_hash_includes(
          result,
          "order_id" => "A100",
          "payment_id" => "pay-A100",
          "reservation_id" => "res-sku-1",
          "label_id" => "ship-A100",
          "ready_to_notify" => true,
          "confirmation_sent" => true,
        )
        assert_equal "durababble:v1:workflow:#{workflow_id}:step:0", result.fetch("charge_idempotency_key")
        assert_equal ["completed"], store.waits_for(workflow_id).map { |wait| wait.fetch("status") }
      end
    end

    test "ports a Temporal saga compensation workflow with durable compensating steps with #{backend.name}" do
      with_temporal_example_store(backend, "temporal_saga") do |store|
        effects = []
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "temporal-saga-compensation"

          define_method(:execute) do |booking|
            reserved = reserve_seat(booking)
            charged = charge_card(reserved)
            issue_ticket(charged)
          rescue StandardError
            refund_card(booking)
            release_seat(booking)
            raise
          end

          define_method(:reserve_seat) do |booking|
            effects << "reserve"
            booking.merge("seat_id" => "seat-1")
          end
          step :reserve_seat

          define_method(:charge_card) do |booking|
            effects << "charge"
            booking.merge("charge_id" => "charge-1")
          end
          step :charge_card

          define_method(:issue_ticket) do |_booking|
            effects << "ticket"
            raise TemporalExampleActivityError, "ticket printer unavailable"
          end
          step :issue_ticket

          define_method(:refund_card) do |booking|
            effects << "refund"
            booking.merge("refunded" => true)
          end
          step :refund_card

          define_method(:release_seat) do |booking|
            effects << "release"
            booking.merge("released" => true)
          end
          step :release_seat
        end

        run = Durababble::Engine.new(store:, worker_id: "temporal-saga-worker", migrate: false).run(workflow, input: { "trip_id" => "trip-1" })

        assert_equal "failed", run.status
        assert_match(/TemporalExampleActivityError: ticket printer unavailable/, run.error)
        assert_equal ["reserve", "charge", "ticket", "refund", "release"], effects
        assert_equal(
          [
            ["reserve_seat", "completed"],
            ["charge_card", "completed"],
            ["issue_ticket", "failed"],
            ["refund_card", "completed"],
            ["release_seat", "completed"],
          ],
          store.steps_for(run.id).map { |step| [step.fetch("name"), step.fetch("status")] },
        )
      end
    end

    test "ports Temporal signal/update idempotency to durable workflow commands with #{backend.name}" do
      with_temporal_example_store(backend, "temporal_commands") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "temporal-approval-command"

          def execute(request)
            sleep(3600, request.merge("waiting" => true))
          end

          expose_command def approve(reason:)
            { "approved" => true, "reason" => reason }
          end
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: { workflow.workflow_name => workflow },
          worker_id: "temporal-command-worker",
          migrate: false,
        )
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "request_id" => "approval-1" })
        assert_equal(:worked, worker.tick)
        assert_hash_includes(store.workflow(workflow_id), "status" => "waiting")

        first = call_workflow_command_async(backend, workflow, workflow_id, :approve, reason: "operator", idempotency_key: "approve-once")
        wait_for_activation("workflow", workflow.workflow_name, workflow_id)
        assert_equal(:worked, worker.tick)
        status, value = first.fetch(:queue).pop
        first.fetch(:thread).join
        assert_equal(:ok, status)
        assert_equal({ "approved" => true, "reason" => "operator" }, value)

        second = call_workflow_command_async(backend, workflow, workflow_id, :approve, reason: "operator", idempotency_key: "approve-once")
        duplicate_status, duplicate_value = second.fetch(:queue).pop
        second.fetch(:thread).join
        assert_equal(:ok, duplicate_status)
        assert_equal(value, duplicate_value)
        assert_equal(1, store.inbox_messages_for(target_kind: "workflow", target_type: workflow.workflow_name, target_id: workflow_id).length)

        assert_raises(Durababble::IdempotencyKeyConflict) do
          workflow.handle(workflow_id, store:).approve(reason: "different", idempotency_key: "approve-once")
        end
      ensure
        first&.fetch(:thread)&.kill if first&.fetch(:thread)&.alive?
        second&.fetch(:thread)&.kill if second&.fetch(:thread)&.alive?
      end
    end

    test "ports Temporal entity workflow state to a durable object mailbox with #{backend.name}" do
      with_temporal_example_store(backend, "temporal_entity") do |store|
        cart = Class.new(Durababble::DurableObject) do
          object_type "temporal_entity_cart"

          def initialize_state
            { "items" => [], "checked_out" => false }
          end

          expose_command def add_item(sku)
            update_state(current_state.merge("items" => current_state.fetch("items") + [sku]))
          end

          expose_command def checkout
            update_state(current_state.merge("checked_out" => true))
          end

          expose def snapshot
            current_state
          end
        end
        worker = Durababble::Worker.new(
          store:,
          workflows: {},
          objects: [cart],
          worker_id: "temporal-entity-worker",
          migrate: false,
        )

        tell_id = cart.tell("cart-1", :add_item, "sku-1", store:)
        checkout = call_object_command_async(backend, cart, "cart-1", :checkout)
        wait_for_activation("object", cart.object_type, "cart-1")
        run_worker_until_result(worker, checkout.fetch(:queue))
        status, result = checkout.fetch(:queue).pop
        checkout.fetch(:thread).join

        assert_equal(:ok, status)
        assert_equal({ "items" => ["sku-1"], "checked_out" => true }, result)
        assert_equal(result, cart.handle("cart-1", store:).snapshot)
        messages = store.inbox_messages_for(target_kind: "object", target_type: cart.object_type, target_id: "cart-1")
        assert_equal([tell_id, messages.last.fetch("id")], messages.map { |message| message.fetch("id") })
        assert_equal(["completed", "completed"], messages.map { |message| message.fetch("status") })
      ensure
        checkout&.fetch(:thread)&.kill if checkout&.fetch(:thread)&.alive?
      end
    end
  end

  private

  def with_temporal_example_store(backend, schema_suffix, &block)
    attempts = 0
    begin
      with_durababble_store(backend, schema_suffix, migrate: false) do |store|
        migrate_with_yugabyte_catalog_retry(store, backend)
        block.call(store)
      end
    rescue ActiveRecord::SerializationFailure => e
      attempts += 1
      retry if backend.postgres? && attempts < 3 && yugabyte_catalog_conflict?(e)

      raise
    end
  end

  def migrate_with_yugabyte_catalog_retry(store, backend)
    attempts = 0
    begin
      store.migrate!
    rescue ActiveRecord::SerializationFailure => e
      attempts += 1
      retry if backend.postgres? && attempts < 3 && yugabyte_catalog_conflict?(e)

      raise
    end
  end

  def yugabyte_catalog_conflict?(error)
    error.message.include?("Catalog Version Mismatch") || error.message.include?("could not serialize access due to concurrent update")
  end

  def call_workflow_command_async(backend, workflow_class, workflow_id, method_name, **kwargs)
    result_queue = Queue.new
    caller = Thread.new do
      caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
      begin
        result_queue << [:ok, workflow_class.handle(workflow_id, store: caller_store).public_send(method_name, **kwargs)]
      rescue StandardError => e
        result_queue << [:error, e]
      ensure
        caller_store.close
      end
    end
    { thread: caller, queue: result_queue }
  end

  def call_object_command_async(backend, object_class, object_id, method_name, *args)
    result_queue = Queue.new
    caller = Thread.new do
      caller_store = Durababble::Store.connect(database_url: backend.database_url, schema:)
      begin
        result_queue << [:ok, object_class.handle(object_id, store: caller_store).public_send(method_name, *args)]
      rescue StandardError => e
        result_queue << [:error, e]
      ensure
        caller_store.close
      end
    end
    { thread: caller, queue: result_queue }
  end

  def wait_for_activation(target_kind, target_type, target_id, timeout: 2)
    deadline = Time.now + timeout
    loop do
      activation = store.target_activation(target_kind:, target_type:, target_id:)
      return activation if activation
      raise "target activation not created before timeout" if Time.now >= deadline

      sleep(0.01)
    end
  end

  def run_worker_until_result(worker, result_queue, timeout: 3)
    deadline = Time.now + timeout
    loop do
      return unless result_queue.empty?

      worker.tick
      raise "command did not complete before timeout" if Time.now >= deadline

      sleep(0.01)
    end
  end
end
