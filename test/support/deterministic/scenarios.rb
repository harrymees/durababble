# typed: true
# frozen_string_literal: true

require "digest"

require_relative "harness"

module Durababble
  module Deterministic
    module Scenarios
      extend self
      include Kernel

      #: (untyped) -> untyped
      def fetch(name)
        method(name).to_proc
      rescue NameError
        raise ArgumentError, "unknown deterministic scenario: #{name}"
      end

      #: (untyped, untyped) { (?) -> untyped } -> untyped
      def workflow_rpc_client(_h, _node_id, &block)
        Object.new.tap do |client|
          client.define_singleton_method(:request) do |command, payload|
            raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

            block.call(payload)
          end
        end
      end

      #: (untyped, untyped, ?faults: untyped) { (?) -> untyped } -> untyped
      def grpc_workflow_rpc_client(h, node_id, faults: [], &block)
        fault_queue = faults.dup
        transport_fault = method(:grpc_transport_fault!)
        workflow_response = method(:grpc_workflow_rpc_response)
        remote_error_response = method(:grpc_remote_error_response)
        handler_for = method(:workflow_rpc_handler)
        Object.new.tap do |client|
          client.define_singleton_method(:request) do |command, payload|
            raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.call_transient", target: node_id)
            server_payload = {
              "workflow_id" => payload.fetch("workflow_id"),
              "expected_worker_id" => node_id,
              "command" => payload.fetch("command"),
              "payload" => payload.fetch("payload", {}),
            }
            fault = fault_queue.shift
            transport_fault.call(h, fault, target: node_id)
            response_context = {
              h:,
              node_id:,
              workflow_id: payload.fetch("workflow_id"),
              handler_for:,
              remote_error_response:,
              handler_block: block,
            }
            response = workflow_response.call(response_context, server_payload)
            if fault == "duplicate_response"
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.duplicate_response", target: node_id)
              workflow_response.call(response_context, server_payload)
            end
            if fault == "response_timeout"
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.response_timeout", target: node_id)
              raise Durababble::WorkflowRpc::NodeUnavailable, "#{node_id} response timed out"
            end

            Durababble::Rpc::Client.decode_transient_response(response)
          rescue Durababble::WorkflowRpc::StaleLease
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.decode_moved", target: node_id)
            raise
          rescue Durababble::WorkflowRpc::NoActiveLease
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.decode_not_running", target: node_id)
            raise
          end
        end
      end

      #: (untyped, untyped) -> untyped
      def grpc_workflow_rpc_response(context, payload)
        h = context.fetch(:h)
        node_id = context.fetch(:node_id)
        workflow_id = context.fetch(:workflow_id)
        handler_block = context.fetch(:handler_block)
        handler_for = context.fetch(:handler_for)
        remote_error_response = context.fetch(:remote_error_response)

        result = handler_block ? handler_block.call(payload) : handler_for.call(h, node_id).call(payload)
        Durababble::Rpc::Proto::TransientResponse.new(ok: Durababble::Rpc.dump(result))
      rescue Durababble::WorkflowRpc::NoActiveLease
        h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.not_running", target: node_id)
        Durababble::Rpc::Proto::TransientResponse.new(not_running: true)
      rescue Durababble::WorkflowRpc::StaleLease => e
        lease = h.store.current_workflow_lease(workflow_id)
        if lease && lease.fetch("worker_id") != node_id
          h.scheduler.trace.event(
            h.scheduler.time,
            "grpc",
            "grpc.lease_moved",
            from: node_id,
            to: lease.fetch("worker_id"),
          )
          Durababble::Rpc::Proto::TransientResponse.new(
            moved: Durababble::Rpc::Proto::LeaseMoved.new(
              new_node_id: lease.fetch("worker_id"),
              new_rpc_address: "virtual://#{lease.fetch("worker_id")}",
            ),
          )
        else
          remote_error_response.call(e)
        end
      rescue StandardError => e
        remote_error_response.call(e)
      end

      #: (untyped, untyped, target: untyped) -> untyped
      def grpc_transport_fault!(h, fault, target:)
        case fault
        when nil, "success", "response_timeout", "duplicate_response"
          nil
        when "timeout"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.timeout", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} timed out"
        when "deadline_exceeded"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.deadline_exceeded", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} deadline exceeded"
        when "connection_reset"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.rst", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} reset the stream"
        when "eof"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.eof", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} closed the stream"
        when "unavailable"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.unavailable", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} unavailable"
        else
          raise ArgumentError, "unknown gRPC fault #{fault}"
        end
      end

      #: (untyped, untyped, target: untyped, fault: untyped) { (?) -> untyped } -> untyped
      def grpc_faulty_unary(h, method_name, target:, fault:, &block)
        h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.#{method_name}.request", target:, fault:)
        case fault
        when "drop"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.drop", method: method_name, target:)
          :dropped
        when "duplicate"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.duplicate", method: method_name, target:)
          block.call
          block.call
          :ok
        else
          grpc_transport_fault!(h, fault, target:)
          block.call
          :ok
        end
      end

      #: (untyped, untyped, workflow_id: untyped) -> untyped
      def call_grpc_service_method(service, method_name, workflow_id:)
        case method_name
        when "awaken_batch"
          service.awaken_batch(
            Durababble::Rpc::Proto::AwakenBatchRequest.new(
              worker_pool: "default",
              workflow_ids: [workflow_id],
            ),
            nil,
          )
        when "deliver_message"
          service.deliver_message(
            Durababble::Rpc::Proto::DeliverMessageRequest.new(
              worker_pool: "default",
              target_kind: "workflow",
              target_id: workflow_id,
            ),
            nil,
          )
        when "evict_lease"
          service.evict_lease(
            Durababble::Rpc::Proto::EvictLeaseRequest.new(
              worker_pool: "default",
              target_kind: "workflow",
              target_id: workflow_id,
            ),
            nil,
          )
        else
          raise ArgumentError, "unknown gRPC service method #{method_name}"
        end
      end

      #: (untyped) -> untyped
      def grpc_remote_error_response(error)
        Durababble::Rpc::Proto::TransientResponse.new(
          err: Durababble::Rpc::Proto::RemoteError.new(
            klass: error.class.name,
            message: error.message,
            backtrace: error.backtrace || [],
          ),
        )
      end

      #: (untyped, untyped) { (?) -> untyped } -> untyped
      def workflow_rpc_handler(h, node_id, &handler_block)
        Durababble::WorkflowRpc::Handler.new(store: h.store, node_id:, handlers: {
          "status" => handler_block || ->(payload) { { "node" => node_id, "payload" => payload } },
        })
      end

      #: () -> untyped
      def counter_workflow
        workflow_class("counter") do
          test_step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
          test_step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
        end
      end

      #: (untyped, ?retry_policy: untyped) ?{ (untyped, ?untyped) -> untyped } -> untyped
      def test_step(name, retry_policy: nil, &block)
        nil
      end

      #: (untyped) ?{ (?) -> untyped } -> untyped
      def workflow_class(name, &definition)
        workflow = Class.new(Durababble::Workflow)
        workflow.workflow_name(name)
        workflow.define_method(:execute) do |input|
          instance = self #: as untyped
          instance.class.step_order.reduce(input) { |ctx, method_name| instance.public_send(method_name, ctx) }
        end
        workflow.define_singleton_method(:test_step) do |step_name, retry_policy: nil, &block|
          workflow_class = self #: as untyped
          workflow_class.define_method(step_name) do |ctx|
            instance = self #: as untyped
            if block.arity >= 2
              block.call(ctx, instance.step_context.heartbeat)
            else
              block.call(ctx)
            end
          end
          workflow_class.step(step_name, retry: retry_policy)
        end
        workflow.class_eval(&definition) if definition
        workflow
      end

      #: (untyped) ?{ (?) -> untyped } -> untyped
      def durable_object_class(type, &definition)
        object_class = Class.new(Durababble::DurableObject)
        object_class.object_type(type)
        object_class.class_eval(&definition) if definition
        object_class
      end

      #: () -> untyped
      def alarm_object_class
        durable_object_class("alarm") do
          define_method(:initialize_state) { { "wakes" => [], "armed" => [] } }
          define_method(:arm) do |specs|
            specs.each { |spec| schedule_wake(name: spec.fetch("name"), at: spec.fetch("at"), payload: spec.fetch("payload")) }
            update_state(current_state.merge("armed" => current_state.fetch("armed") + specs.map { |spec| spec.fetch("name") }))
          end
          expose_command(:arm)
          define_method(:on_wake) do |name:, payload:| # rubocop:disable Lint/UnusedBlockArgument
            update_state(current_state.merge("wakes" => current_state.fetch("wakes") | [name]))
          end
          define_method(:snapshot) { current_state }
          expose(:snapshot)
        end
      end

      #: (untyped, untyped) { (untyped) -> untyped } -> untyped
      def run(seed, scenario, &block)
        trace = Trace.new
        scheduler = Scheduler.new(seed:, trace:)
        network = VirtualNetwork.new(scheduler:, drop_percent: scenario == "chaos" ? 5 : 0)
        store = DeterministicSqliteStore.build(scheduler:)
        begin
          store.with_deterministic_uuids do
            harness = Harness.new(scenario:, seed:, scheduler:, network:, store:)
            trace.event(0, "dst", "begin", scenario:, seed:)
            block.call(harness)
            scheduler.run
            harness.verify!
            trace.event(scheduler.time, "dst", "end", scenario:, seed:)
            trace_s = trace.to_s
            Result.new(scenario:, seed:, trace: trace_s, digest: Digest::SHA256.hexdigest(trace_s), violations: harness.violations, summary: store.summary)
          end
        ensure
          store.close
        end
      end
    end
  end
end

Dir[File.join(__dir__, "scenarios", "*.rb")].sort.each do |path|
  require_relative "scenarios/#{File.basename(path, ".rb")}"
end
