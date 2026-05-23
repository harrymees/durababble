# typed: true
# frozen_string_literal: true

require "google/protobuf"
require "google/protobuf/descriptor_pb"
require "grpc"

module Durababble
  module Rpc
    module Proto
      class << self
        #: () -> untyped
        def pool = Google::Protobuf::DescriptorPool.generated_pool

        #: () -> untyped
        def install!
          return if pool.lookup("durababble.v1.AwakenBatchRequest")

          pool.add_serialized_file(file_descriptor.to_proto)
        end

        private

        #: () -> untyped
        def file_descriptor
          Google::Protobuf::FileDescriptorProto.new(
            name: "durababble/v1/durababble.proto",
            package: "durababble.v1",
            syntax: "proto3",
            message_type: [
              remote_error,
              lease_moved,
              awaken_batch_request,
              message("AwakenBatchResponse"),
              evict_lease_request,
              message("EvictLeaseResponse"),
              transient_request,
              transient_response,
              deliver_message_request,
              message("DeliverMessageResponse"),
            ],
          )
        end

        #: () -> untyped
        def remote_error
          message(
            "RemoteError",
            field("klass", 1, :TYPE_STRING),
            field("message", 2, :TYPE_STRING),
            field("backtrace", 3, :TYPE_STRING, label: :LABEL_REPEATED),
          )
        end

        #: () -> untyped
        def lease_moved
          message(
            "LeaseMoved",
            field("new_rpc_address", 1, :TYPE_STRING),
            field("new_node_id", 2, :TYPE_STRING),
          )
        end

        #: () -> untyped
        def awaken_batch_request
          message(
            "AwakenBatchRequest",
            field("worker_pool", 1, :TYPE_STRING),
            field("workflow_ids", 2, :TYPE_STRING, label: :LABEL_REPEATED),
          )
        end

        #: () -> untyped
        def evict_lease_request
          target_request("EvictLeaseRequest")
        end

        #: () -> untyped
        def deliver_message_request
          target_request("DeliverMessageRequest")
        end

        #: (untyped) -> untyped
        def target_request(name)
          message(
            name,
            field("worker_pool", 1, :TYPE_STRING),
            field("target_kind", 2, :TYPE_STRING),
            field("target_class", 3, :TYPE_STRING),
            field("target_id", 4, :TYPE_STRING),
          )
        end

        #: () -> untyped
        def transient_request
          message(
            "TransientRequest",
            field("worker_pool", 1, :TYPE_STRING),
            field("class_name", 2, :TYPE_STRING),
            field("object_id", 3, :TYPE_STRING),
            field("workflow_id", 4, :TYPE_STRING),
            field("method", 5, :TYPE_STRING),
            field("args", 6, :TYPE_BYTES),
            field("deadline_ms", 7, :TYPE_INT64),
          )
        end

        #: () -> untyped
        def transient_response
          Google::Protobuf::DescriptorProto.new(
            name: "TransientResponse",
            oneof_decl: [Google::Protobuf::OneofDescriptorProto.new(name: "result")],
            field: [
              field("ok", 1, :TYPE_BYTES, oneof_index: 0),
              field("err", 2, :TYPE_MESSAGE, type_name: ".durababble.v1.RemoteError", oneof_index: 0),
              field("not_running", 3, :TYPE_BOOL, oneof_index: 0),
              field("moved", 4, :TYPE_MESSAGE, type_name: ".durababble.v1.LeaseMoved", oneof_index: 0),
            ],
          )
        end

        #: (untyped, *untyped) -> untyped
        def message(name, *fields)
          Google::Protobuf::DescriptorProto.new(name:, field: fields)
        end

        #: (untyped, untyped, untyped, ?label: untyped, ?type_name: untyped, ?oneof_index: untyped) -> untyped
        def field(name, number, type, label: :LABEL_OPTIONAL, type_name: nil, oneof_index: nil)
          Google::Protobuf::FieldDescriptorProto.new(
            name:,
            number:,
            label:,
            type:,
            type_name:,
            oneof_index:,
          )
        end
      end

      install!

      MESSAGE_CLASSES = {
        AwakenBatchRequest: "durababble.v1.AwakenBatchRequest",
        AwakenBatchResponse: "durababble.v1.AwakenBatchResponse",
        DeliverMessageRequest: "durababble.v1.DeliverMessageRequest",
        DeliverMessageResponse: "durababble.v1.DeliverMessageResponse",
        EvictLeaseRequest: "durababble.v1.EvictLeaseRequest",
        EvictLeaseResponse: "durababble.v1.EvictLeaseResponse",
        LeaseMoved: "durababble.v1.LeaseMoved",
        RemoteError: "durababble.v1.RemoteError",
        TransientRequest: "durababble.v1.TransientRequest",
        TransientResponse: "durababble.v1.TransientResponse",
      }.freeze

      MESSAGE_CLASSES.each do |constant_name, message_name|
        const_set(constant_name, pool.lookup(message_name).msgclass)
      end

      class Service
        include GRPC::GenericService

        self.marshal_class_method = :encode
        self.unmarshal_class_method = :decode
        self.service_name = "durababble.v1.Durababble"

        rpc :AwakenBatch, AwakenBatchRequest, AwakenBatchResponse
        rpc :EvictLease, EvictLeaseRequest, EvictLeaseResponse
        rpc :CallTransient, TransientRequest, TransientResponse
        rpc :DeliverMessage, DeliverMessageRequest, DeliverMessageResponse
      end

      Stub = Service.rpc_stub_class
    end
  end
end
