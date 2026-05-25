# typed: true
# frozen_string_literal: true

module Durababble
  module WorkflowRpc
    class Error < Durababble::Error; end
    class WorkflowNotRunning < Error; end
    class NoActiveLease < WorkflowNotRunning; end
    class NodeUnavailable < Error; end
    class StaleLease < Error; end
    class UnknownCommand < Error; end

    class << self
      #: (untyped) -> untyped
      def remote_error_from_message(message)
        klass_name, parsed_message = message.split(": ", 2)
        remote_error_from_fields(klass_name, parsed_message || message)
      end

      #: (untyped, untyped) -> untyped
      def remote_error_from_fields(klass_name, message)
        case klass_name
        when "Durababble::WorkflowRpc::StaleLease", "WorkflowRpc::StaleLease", "StaleLease"
          StaleLease.new(message)
        when "Durababble::WorkflowRpc::NoActiveLease", "WorkflowRpc::NoActiveLease", "NoActiveLease"
          NoActiveLease.new(message)
        when "Durababble::WorkflowRpc::WorkflowNotRunning", "WorkflowRpc::WorkflowNotRunning", "WorkflowNotRunning"
          WorkflowNotRunning.new(message)
        when "Durababble::WorkflowRpc::NodeUnavailable", "WorkflowRpc::NodeUnavailable", "NodeUnavailable"
          NodeUnavailable.new(message)
        when "Durababble::WorkflowRpc::UnknownCommand", "WorkflowRpc::UnknownCommand", "UnknownCommand"
          UnknownCommand.new(message)
        end
      end
    end

    class LeaseStarter
      #: (store: untyped, worker_ids: untyped, ?lease_seconds: untyped, ?await_attempts: untyped, ?await_sleep: untyped) -> void
      def initialize(store:, worker_ids:, lease_seconds: 60, await_attempts: 3, await_sleep: ->(_attempt) {})
        @store = store
        @worker_ids = worker_ids
        @lease_seconds = lease_seconds
        @await_attempts = await_attempts
        @await_sleep = await_sleep
      end

      #: (workflow_id: untyped) -> untyped
      def call(workflow_id:)
        Observability.trace("durababble.workflow_rpc.lease_start", "durababble.workflow.id" => workflow_id) do
          @worker_ids.each do |worker_id|
            claimed = @store.claim_workflow(workflow_id:, worker_id:, lease_seconds: @lease_seconds)
            return await_started!(workflow_id) if claimed
          end
          await_started!(workflow_id)
        end
      end

      private

      #: (untyped) -> untyped
      def await_started!(workflow_id)
        @await_attempts.times do |attempt|
          lease = @store.current_workflow_lease(workflow_id)
          return lease if lease

          @await_sleep.call(attempt)
        end
        raise NoActiveLease, "workflow #{workflow_id} could not be started with an active lease"
      end
    end

    class Router
      #: (store: untyped, rpc_clients: untyped, ?retry_on_stale: untyped, ?start_workflow: untyped) -> void
      def initialize(store:, rpc_clients:, retry_on_stale: false, start_workflow: nil)
        @store = store
        @rpc_clients = rpc_clients
        @retry_on_stale = retry_on_stale
        @start_workflow = start_workflow
      end

      #: (workflow_id: untyped, command: untyped, ?payload: untyped) -> untyped
      def request(workflow_id:, command:, payload: {})
        attributes = {
          "durababble.workflow.id" => workflow_id,
          "durababble.rpc.command" => command,
        }
        Observability.trace("durababble.workflow_rpc.route", attributes) do
          attempts = 0

          begin
            route_once(workflow_id:, command:, payload:)
          rescue StaleLease, NoActiveLease, NodeUnavailable => e
            Observability.count("durababble.workflow_rpc.reroutes", attributes.merge("error.type" => e.class.name))
            raise unless @retry_on_stale

            attempts += 1
            raise if attempts > 3

            start_workflow!(workflow_id) if e.is_a?(NoActiveLease)
            retry
          end
        end
      end

      private

      #: (workflow_id: untyped, command: untyped, payload: untyped) -> untyped
      def route_once(workflow_id:, command:, payload:)
        lease = @store.current_workflow_lease(workflow_id)
        raise inactive_workflow_error(workflow_id) unless lease

        worker_id = lease.fetch("worker_id")
        client = @rpc_clients.fetch(worker_id) do
          raise NodeUnavailable, "workflow #{workflow_id} is leased by unavailable node #{worker_id}"
        end
        client.request("workflow_rpc", {
          "workflow_id" => workflow_id,
          "expected_worker_id" => worker_id,
          "command" => command,
          "payload" => payload,
        })
      rescue Durababble::RpcClient::RemoteError => e
        raise translate_remote_error(e)
      end

      #: (untyped) -> untyped
      def start_workflow!(workflow_id)
        starter = @start_workflow || LeaseStarter.new(store: @store, worker_ids: @rpc_clients.keys)
        starter.call(workflow_id:)
      end

      #: (untyped) -> untyped
      def translate_remote_error(error)
        WorkflowRpc.remote_error_from_message(error.message) || error
      end

      #: (untyped) -> untyped
      def inactive_workflow_error(workflow_id)
        row = @store.workflow(workflow_id)
        return WorkflowNotRunning.new("workflow #{workflow_id} is #{row.fetch("status")}") if WorkflowStatus.rpc_not_running?(row)

        NoActiveLease.new("workflow #{workflow_id} has no active lease")
      end
    end

    class Handler
      #: (store: untyped, node_id: untyped, handlers: untyped) -> void
      def initialize(store:, node_id:, handlers:)
        @store = store
        @node_id = node_id
        @handlers = handlers
      end

      #: (untyped) -> untyped
      def call(payload)
        workflow_id = payload.fetch("workflow_id")
        expected_worker_id = payload.fetch("expected_worker_id")
        attributes = {
          "durababble.workflow.id" => workflow_id,
          "durababble.rpc.command" => payload.fetch("command"),
          "durababble.worker.id" => @node_id,
          "durababble.lease.owner" => expected_worker_id,
        }
        Observability.trace("durababble.workflow_rpc.handle", attributes) do
          raise StaleLease, "RPC expected #{expected_worker_id}, but reached #{@node_id}" unless expected_worker_id == @node_id

          assert_current_lease!(workflow_id)
          handler = @handlers.fetch(payload.fetch("command")) do
            raise UnknownCommand, "unknown workflow RPC command #{payload.fetch("command")}"
          end
          result = handler.call(payload.fetch("payload", {}))
          assert_current_lease!(workflow_id)
          result
        end
      end

      private

      #: (untyped) -> untyped
      def assert_current_lease!(workflow_id)
        lease = @store.current_workflow_lease(workflow_id)
        raise inactive_workflow_error(workflow_id) unless lease
        return if lease.fetch("worker_id") == @node_id

        raise StaleLease, "#{@node_id} no longer owns workflow #{workflow_id}; current owner is #{lease.fetch("worker_id")}"
      end

      #: (untyped) -> untyped
      def inactive_workflow_error(workflow_id)
        row = @store.workflow(workflow_id)
        return WorkflowNotRunning.new("workflow #{workflow_id} is #{row.fetch("status")}") if WorkflowStatus.rpc_not_running?(row)

        NoActiveLease.new("workflow #{workflow_id} has no active lease")
      end
    end
  end
end
