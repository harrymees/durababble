# typed: true

module Concurrent
  module Promises
    def self.future(&blk); end
  end
end

module Async
  class Condition
    def initialize; end
    def wait; end
    def signal; end
  end

  class Task
    def self.current; end
    def async(&blk); end
    def wait; end
  end
end

module ActiveModel
  module Type
    class Value
      def initialize; end
    end
  end
end

module ActiveRecord
  class ActiveRecordError < StandardError; end
  class Deadlocked < ActiveRecordError; end
  class PreparedStatementInvalid < ActiveRecordError; end
  class SerializationFailure < ActiveRecordError; end

  class Base
    def self.abstract_class=(value); end
    def self.connection_class=(value); end
    def self.connection_pool; end
    def self.establish_connection(config); end
  end

  class Relation
    class QueryAttribute
      def initialize(name, value, type); end
    end
  end

  module Sanitization
    module ClassMethods
      def sanitize_sql_array(array); end
      def with_connection(&block); end
    end
  end
end

module Kernel
  def Async(&blk); end
end

module Google
  module Protobuf
    class DescriptorPool
      def self.generated_pool; end
      def build(&blk); end
      def lookup(name); end
    end

    class Descriptor
      def msgclass; end
    end

    class FileDescriptorProto
      def initialize(**kwargs); end
    end

    class DescriptorProto
      def initialize(**kwargs); end
    end

    class FieldDescriptorProto
      def initialize(**kwargs); end
    end

    class OneofDescriptorProto
      def initialize(**kwargs); end
    end
  end
end

module Prism
  module LexCompat
    class Result; end
  end
end

module GRPC
  class BadStatus < StandardError; end
  class BadStatus
    def details; end
  end

  class DeadlineExceeded < BadStatus; end
  class Unauthenticated < BadStatus; end
  class Unavailable < BadStatus; end

  module GenericService
    mixes_in_class_methods(ClassMethods)

    module ClassMethods
      attr_accessor :marshal_class_method, :unmarshal_class_method, :service_name

      def rpc(name, request_class, response_class); end
      def rpc_stub_class; end
    end
  end

  class RpcServer
    def initialize(**kwargs); end
    def add_http2_port(address, credentials); end
    def handle(service); end
    def run; end
    def stop; end
  end
end

module Paquito
  class SingleBytePrefixVersion
    def initialize(version, versions); end
    def dump(value); end
    def load(value); end
  end
end

module PG
  class TRDeadlockDetected < StandardError; end
  class TRSerializationFailure < StandardError; end

  def self.connect(database_url); end

  class Connection
    def self.quote_ident(identifier); end
    def self.unescape_bytea(value); end
  end
end

class Trilogy
  def initialize(**kwargs); end
  def close; end
  def query(sql); end
end

module Durababble
  class Store
    def decode_row(row); end
    def dump_serialized(value); end
    def update_latest_attempt_serialized(workflow_id:, command_id:, status:, serialized_result:, error:); end
  end

  module MysqlMigrations
    def dump_serialized(value); end
    def execute(sql); end
    def execute_params(sql, params); end
    def index_name(table_name, suffix); end
    def raw_table_name(name); end
    def table(name); end
    def table_prefix; end
  end

  module PostgresMigrations
    def dump_serialized(value); end
    def execute(sql); end
    def execute_params(sql, params); end
    def quoted_schema; end
    def schema; end
    def table(name); end
  end

  module Rpc
    module Proto
      class Message
        def initialize(**kwargs); end
        def self.decode(value); end
        def self.encode(value); end
      end

      class AwakenBatchRequest < Message; end
      class AwakenBatchResponse < Message; end
      class DeliverMessageRequest < Message; end
      class DeliverMessageResponse < Message; end
      class EvictLeaseRequest < Message; end
      class EvictLeaseResponse < Message; end
      class LeaseMoved < Message; end
      class RemoteError < Message; end
      class TransientRequest < Message; end
      class TransientResponse < Message; end
    end
  end
end
