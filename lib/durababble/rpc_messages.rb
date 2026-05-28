# typed: true
# frozen_string_literal: true

module Durababble
  module Rpc
    # Plain Ruby value objects that replace the former protobuf message classes
    # (`Rpc::Proto::*`). They travel on the wire via `Rpc.dump`/`Rpc.load`
    # (Paquito/Marshal), Ruby-to-Ruby — durababble never used protobuf for
    # cross-language interop. Field names and access patterns mirror the previous
    # protobuf messages exactly (including `request["method"]`) so the `Service`
    # dispatch, the deterministic harness, and `decode_transient_response` are
    # unchanged except for the `Proto` -> `Messages` namespace.
    module Messages
      # `async-grpc` expects request/response classes to expose protobuf-like
      # `#encode` and `.decode` methods. Durababble's RPC contract is still
      # Ruby-to-Ruby Paquito value objects, so those hooks simply wrap the
      # existing serializer and let the gRPC layer provide framing/statuses.
      module WireMessage
        #: () -> String
        def encode
          message = self #: as Object
          Rpc.dump(message)
        end

        module ClassMethods
          #: (String) -> Object
          def decode(bytes)
            expected_class = self #: as Class
            message = Rpc.load(bytes)
            return message if message.is_a?(expected_class)

            Kernel.raise TypeError, "expected #{expected_class}, got #{message.class}"
          end
        end

        class << self
          #: (Class) -> void
          def included(base)
            base.extend(ClassMethods)
          end
        end
      end
      private_constant :WireMessage

      class AwakenBatchRequest
        include WireMessage

        #: String
        attr_reader :worker_pool
        #: Array[String]
        attr_reader :workflow_ids

        #: (?worker_pool: String, ?workflow_ids: Array[String]) -> void
        def initialize(worker_pool: "", workflow_ids: [])
          @worker_pool = worker_pool
          @workflow_ids = workflow_ids
        end
      end

      class AwakenBatchResponse
        include WireMessage
      end

      # Shared shape for the lease-eviction and message-delivery requests.
      class TargetRequest
        include WireMessage

        #: String
        attr_reader :worker_pool
        #: String
        attr_reader :target_kind
        #: String
        attr_reader :target_class
        #: String
        attr_reader :target_id
        # The worker the caller believes holds the lease; the receiver uses it to
        # fence recycled addresses (see #68/#69) before acting on the message.
        #: String
        attr_reader :expected_worker_id

        #: (?worker_pool: String, ?target_kind: String, ?target_class: String, ?target_id: String, ?expected_worker_id: String) -> void
        def initialize(worker_pool: "", target_kind: "", target_class: "", target_id: "", expected_worker_id: "")
          @worker_pool = worker_pool
          @target_kind = target_kind
          @target_class = target_class
          @target_id = target_id
          @expected_worker_id = expected_worker_id
        end
      end

      class EvictLeaseRequest < TargetRequest; end

      class EvictLeaseResponse
        include WireMessage
      end

      class DeliverMessageRequest < TargetRequest; end

      class DeliverMessageResponse
        include WireMessage
      end

      class TransientRequest
        include WireMessage

        #: String
        attr_reader :worker_pool
        #: String
        attr_reader :class_name
        # The durable object's logical id (e.g. `"acct-1"`). Named with the
        # `durable_` prefix so the attr_reader does not shadow `Object#object_id`,
        # which the Ruby VM uses for identity and which several stdlib paths
        # (`Hash`, `inspect`, `ObjectSpace`) rely on.
        #: String
        attr_reader :durable_object_id
        #: String
        attr_reader :workflow_id
        #: String?
        attr_reader :args
        #: Integer
        attr_reader :deadline_ms
        # The worker the caller believes holds the workflow lease; the receiver
        # raises StaleLease when it is not that node (see #68/#69).
        #: String
        attr_reader :expected_worker_id

        #: (?worker_pool: String, ?class_name: String, ?durable_object_id: String, ?workflow_id: String, ?method: String, ?args: String?, ?deadline_ms: Integer, ?expected_worker_id: String) -> void
        def initialize(worker_pool: "", class_name: "", durable_object_id: "", workflow_id: "", method: "", args: nil, deadline_ms: 0, expected_worker_id: "")
          @worker_pool = worker_pool
          @class_name = class_name
          @durable_object_id = durable_object_id
          @workflow_id = workflow_id
          @method = method
          @args = args
          @deadline_ms = deadline_ms
          @expected_worker_id = expected_worker_id
        end

        # The conceptual `method` field collides with `Object#method`, so — as with
        # the former protobuf message — it is read via `request["method"]`.
        #: (String | Symbol) -> Object?
        def [](key)
          key.to_s == "method" ? @method : public_send(key)
        end
      end

      class LeaseMoved
        include WireMessage

        #: String
        attr_reader :new_rpc_address
        #: String
        attr_reader :new_node_id

        #: (?new_rpc_address: String, ?new_node_id: String) -> void
        def initialize(new_rpc_address: "", new_node_id: "")
          @new_rpc_address = new_rpc_address
          @new_node_id = new_node_id
        end
      end

      class RemoteError
        include WireMessage

        #: String
        attr_reader :klass
        #: String
        attr_reader :message
        #: Array[String]
        attr_reader :backtrace

        #: (?klass: String, ?message: String, ?backtrace: Array[String]) -> void
        def initialize(klass: "", message: "", backtrace: [])
          @klass = klass
          @message = message
          @backtrace = backtrace
        end
      end

      # One message in a streaming-result RPC. The gRPC transport supplies the
      # stream framing; this value only discriminates application values from a
      # terminal application error.
      class StreamFrame
        include WireMessage

        #: Symbol
        attr_reader :kind
        #: Object?
        attr_reader :value
        #: RemoteError?
        attr_reader :error

        #: (?kind: Symbol, ?value: Object?, ?error: RemoteError?) -> void
        def initialize(kind: :value, value: nil, error: nil)
          @kind = kind
          @value = value
          @error = error
        end

        #: () -> bool
        def value? = @kind == :value

        #: () -> bool
        def error? = @kind == :error
      end

      # Discriminated response mirroring the former protobuf `oneof result`.
      # Exactly one of `ok`/`err`/`not_running`/`moved` is populated; `#result`
      # reports which, matching the protobuf oneof accessor.
      class TransientResponse
        include WireMessage

        #: String?
        attr_reader :ok
        #: RemoteError?
        attr_reader :err
        #: bool
        attr_reader :not_running
        #: LeaseMoved?
        attr_reader :moved

        #: (?ok: String?, ?err: RemoteError?, ?not_running: bool, ?moved: LeaseMoved?) -> void
        def initialize(ok: nil, err: nil, not_running: false, moved: nil)
          @ok = ok
          @err = err
          @not_running = not_running
          @moved = moved
        end

        # The `Service` only ever populates one of these fields, but if a
        # caller constructs a response with multiple set (e.g. ad-hoc test
        # fixtures), the short-circuit order is `:ok > :err > :not_running >
        # :moved`. Treat this as discriminator priority, not arbitrary truthy
        # selection.
        #: () -> Symbol?
        def result
          return :ok unless @ok.nil?
          return :err unless @err.nil?
          return :not_running if @not_running
          return :moved unless @moved.nil?

          nil
        end
      end
    end
  end
end
