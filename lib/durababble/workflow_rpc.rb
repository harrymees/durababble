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
      #: (String) -> Error?
      def remote_error_from_message(message)
        klass_name, parsed_message = message.split(": ", 2)
        klass_name ||= message
        remote_error_from_fields(klass_name, parsed_message || message)
      end

      #: (String, String) -> Error?
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

      # The error to raise when a workflow has no current lease: terminal/not-running
      # workflows report WorkflowNotRunning, otherwise the lease is simply absent.
      #: (Store, String) -> Error
      def inactive_workflow_error(store, workflow_id)
        store = store #: as untyped
        row = store.workflow(workflow_id)
        return WorkflowNotRunning.new("workflow #{workflow_id} is #{row.fetch("status")}") if WorkflowStatus.rpc_not_running?(row)

        NoActiveLease.new("workflow #{workflow_id} has no active lease")
      end
    end

    class LeaseStarter
      # Base per-attempt spacing for the default lease-poll backoff. Kept small
      # because we are only waiting for an already-claimed lease row to become
      # visible, not for slow work to finish.
      DEFAULT_AWAIT_RETRY_STEP_SECONDS = 0.05

      #: (store: Store, worker_ids: Array[String], ?lease_seconds: Numeric, ?await_attempts: Integer, ?await_sleep: Object?) -> void
      def initialize(store:, worker_ids:, lease_seconds: 60, await_attempts: 3, await_sleep: nil)
        @store = store
        @worker_ids = worker_ids
        @lease_seconds = lease_seconds
        @await_attempts = await_attempts
        @await_sleep = await_sleep || default_await_sleep
      end

      #: (workflow_id: String) -> Hash[String, Object?]
      def call(workflow_id:)
        workflow = @store.workflow(workflow_id)
        worker_pool = workflow.fetch("worker_pool", "default").to_s
        Observability.trace("durababble.workflow_rpc.lease_start", "durababble.workflow.id" => workflow_id, "durababble.worker.pool" => worker_pool) do
          @worker_ids.each do |worker_id|
            claimed = @store.claim_workflow(workflow_id:, worker_id:, lease_seconds: @lease_seconds, worker_pool:)
            return await_started!(workflow_id, worker_pool:) if claimed
          end
          await_started!(workflow_id, worker_pool:)
        end
      end

      private

      #: (String, worker_pool: String) -> Hash[String, Object?]
      def await_started!(workflow_id, worker_pool:)
        last_attempt = @await_attempts - 1
        @await_attempts.times do |attempt|
          lease = @store.current_workflow_lease(workflow_id, worker_pool:)
          return lease if lease

          # No point sleeping after the final poll — we are about to give up.
          @await_sleep.call(attempt) unless attempt == last_attempt
        end
        raise NoActiveLease, "workflow #{workflow_id} could not be started with an active lease"
      end

      # Jittered, linearly growing sleep between lease polls. Routed through
      # Backoff so a fleet awaiting the same workflow does not poll in lockstep.
      # Runs on the worker host, outside workflow replay, so the randomness is
      # determinism-safe.
      #: () -> Proc
      def default_await_sleep
        ->(attempt) { Kernel.sleep(Backoff.linear(attempt + 1, step: DEFAULT_AWAIT_RETRY_STEP_SECONDS)) }
      end
    end

    class Router
      #: (store: Store, ?rpc_clients: Hash[String, Object], ?rpc_client_factory: Object?, ?retry_on_stale: bool, ?start_workflow: Object?, ?start_on_no_active_lease: bool) -> void
      def initialize(store:, rpc_clients: {}, rpc_client_factory: nil, retry_on_stale: false, start_workflow: nil, start_on_no_active_lease: true)
        @store = store
        @rpc_clients = rpc_clients
        @rpc_client_factory = rpc_client_factory
        @retry_on_stale = retry_on_stale
        @start_workflow = start_workflow
        @start_on_no_active_lease = start_on_no_active_lease
      end

      #: (workflow_id: String, command: String, ?payload: Hash[String, Object?]) -> Object?
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
            raise if e.is_a?(NoActiveLease) && !@start_on_no_active_lease

            attempts += 1
            raise if attempts > 3

            start_workflow!(workflow_id) if e.is_a?(NoActiveLease) && @start_on_no_active_lease
            retry
          end
        end
      end

      private

      #: (workflow_id: String, command: String, payload: Hash[String, Object?]) -> Object?
      def route_once(workflow_id:, command:, payload:)
        lease = @store.current_workflow_lease(workflow_id)
        raise WorkflowRpc.inactive_workflow_error(@store, workflow_id) unless lease

        worker_id = lease.fetch("worker_id").to_s
        worker_pool = lease.fetch("worker_pool", "default").to_s
        client = rpc_client_for(worker_id, workflow_id:, worker_pool:)
        client = client #: as untyped
        client.request("workflow_rpc", {
          "workflow_id" => workflow_id,
          "expected_worker_id" => worker_id,
          "command" => command,
          "payload" => payload,
        })
      rescue Durababble::Rpc::RemoteError => e
        raise translate_remote_error(e)
      end

      #: (String) -> Hash[String, Object?]
      def start_workflow!(workflow_id)
        starter = @start_workflow || LeaseStarter.new(store: @store, worker_ids: @rpc_clients.keys)
        starter = starter #: as untyped
        starter.call(workflow_id:)
      end

      #: (String, workflow_id: String, worker_pool: String) -> Object
      def rpc_client_for(worker_id, workflow_id:, worker_pool:)
        if @rpc_clients.respond_to?(:fetch)
          sentinel = Object.new
          client = @rpc_clients.fetch(worker_id, sentinel)
          return client unless client.equal?(sentinel)
        end

        factory = @rpc_client_factory
        factory = factory #: as untyped
        return factory.call(worker_id, worker_pool:) if factory

        raise NodeUnavailable, "workflow #{workflow_id} is leased by unavailable node #{worker_id}"
      end

      #: (Durababble::Rpc::RemoteError) -> StandardError
      def translate_remote_error(error)
        WorkflowRpc.remote_error_from_message(error.message) || error
      end
    end

    class LocalClient
      #: (store: Store, node_id: String, handlers: Hash[String, Object]) -> void
      def initialize(store:, node_id:, handlers:)
        @store = store
        @node_id = node_id
        @handlers = handlers
      end

      #: (String, Hash[String, Object?]) -> Object?
      def request(command, payload)
        raise UnknownCommand, command unless command == "workflow_rpc"

        with_store do |store|
          Handler.new(store:, node_id: @node_id, handlers: @handlers).call(payload)
        end
      end

      private

      #: () { (Store) -> Object? } -> Object?
      def with_store(&block)
        if @store.respond_to?(:with_dedicated_connection)
          @store.with_dedicated_connection(&block)
        else
          block.call(@store)
        end
      end
    end

    class Handler
      #: (store: Store, node_id: String, handlers: Hash[String, Object]) -> void
      def initialize(store:, node_id:, handlers:)
        @store = store
        @node_id = node_id
        @handlers = handlers
      end

      #: (Hash[String, Object?]) -> Object?
      def call(payload)
        workflow_id = payload.fetch("workflow_id").to_s
        expected_worker_id = payload.fetch("expected_worker_id")
        command = payload.fetch("command").to_s
        attributes = {
          "durababble.workflow.id" => workflow_id,
          "durababble.rpc.command" => command,
          "durababble.worker.id" => @node_id,
          "durababble.lease.owner" => expected_worker_id,
        }
        Observability.trace("durababble.workflow_rpc.handle", attributes) do
          raise StaleLease, "RPC expected #{expected_worker_id}, but reached #{@node_id}" unless expected_worker_id == @node_id

          assert_current_lease!(workflow_id)
          handler = @handlers.fetch(command) do
            raise UnknownCommand, "unknown workflow RPC command #{command}"
          end
          handler = handler #: as untyped
          result = handler.call(payload.fetch("payload", {}))
          assert_current_lease!(workflow_id) unless query_payload?(payload)
          result
        end
      end

      private

      #: (Hash[String, Object?]) -> bool
      def query_payload?(payload)
        payload_value = payload["payload"]
        payload_value.is_a?(Hash) && payload_value["rpc_kind"] == "workflow_query"
      end

      #: (String) -> void
      def assert_current_lease!(workflow_id)
        lease = @store.current_workflow_lease(workflow_id)
        raise WorkflowRpc.inactive_workflow_error(@store, workflow_id) unless lease
        return if lease.fetch("worker_id") == @node_id

        raise StaleLease, "#{@node_id} no longer owns workflow #{workflow_id}; current owner is #{lease.fetch("worker_id")}"
      end
    end
  end
end
