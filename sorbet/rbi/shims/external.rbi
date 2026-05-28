# typed: true

module Concurrent
  module Promises
    def self.future(&blk); end
  end
end

module Async
  class TimeoutError < StandardError; end

  # `Async::Stop` (raised into a task by `#stop`) descends from `Async::Cancel`,
  # which descends from `Exception` and is deliberately not a `StandardError`.
  class Cancel < Exception; end
  class Stop < Cancel; end

  class Scheduler
    def interrupt; end
    def terminate; end
  end

  class Condition
    def initialize; end
    def wait; end
    def signal; end
  end

  class Queue
    class ClosedError < StandardError; end

    def initialize; end
    def push(value); end
    def enqueue(item); end
    def dequeue(timeout: nil); end
    def empty?; end
    def close; end
    def closed?; end
  end

  class Task
    def self.current; end
    def self.current?; end
    def async(*args, **kwargs, &blk); end
    def wait; end
    def stop(later = false); end
    def running?; end
    def reactor; end
    def transient?; end
    def with_timeout(duration, exception = nil, message = nil, &blk); end
  end

  class LimitedQueue < Queue
    def initialize(limit = nil); end
  end

  module HTTP
    class Endpoint
      def self.parse(string, **options); end
      def bound; end
      def protocol; end
      def scheme; end
    end

    class Client
      def initialize(endpoint, **options); end
      def post(path, headers = nil, body = nil); end
      def get(path, headers = nil, body = nil); end
      def close; end
    end

    class Server
      def initialize(app, endpoint, **options); end
      def run; end
    end

    module Protocol
      class HTTP1; end
      class HTTP2; end
    end
  end

  module GRPC
    class Client
      def initialize(http_client); end
      def stub(interface, service_name); end
      def call(request); end
      def close; end
    end

    class Service
      def initialize(interface, service_name); end
    end

    class Dispatcher
      def initialize(app = nil); end
      def register(service); end
    end
  end
end

module Protocol
  module GRPC
    class Error < StandardError
      def self.for(status_code, message = nil, metadata: {}); end
      def status_code; end
    end

    class Unauthenticated < Error; end
    class Unavailable < Error; end
    class DeadlineExceeded < Error; end
    class Cancelled < Error; end
    class Internal < Error; end
    class NotFound < Error; end

    class Interface
      def self.rpc(name, request, response); end
      def self.stream(message_class); end
      def initialize(name); end
      def path(method_name); end
    end

    module Body
      class ReadableBody
        def self.wrap(message, **options); end
        def read; end
        def close(error = nil); end
      end

      class WritableBody
        def initialize(**options); end
        def write(message, **options); end
        def close_write(error = nil); end
      end
    end

    module Metadata
      def self.assign_status!(headers, status:, message: nil, error: nil); end
      def self.extract_status(headers); end
      def self.extract_message(headers); end
    end

    module Methods
      def self.build_headers(metadata: {}, timeout: nil, content_type: nil); end
    end

    module Status
      OK = 0
      INTERNAL = 13
      UNAUTHENTICATED = 16
    end
  end

  module HTTP
    class Request
      def self.[](method, path, headers = nil, body = nil); end
    end

    class Response
      def self.[](status, headers = nil, body = nil); end
      def close(error = nil); end
      def headers; end
      def stream; end
    end

    module Body
      class Writable
        class Closed < StandardError; end
      end
    end

    module Middleware
      NotFound = nil
    end
  end

  module HTTP2
    class Error < StandardError
      CANCEL = 8
    end
  end
end

class Fiber
  def self.scheduler; end
end

module ActiveSupport
  module IsolatedExecutionState
    sig { returns(Symbol) }
    def self.isolation_level; end

    sig { params(value: Symbol).returns(Symbol) }
    def self.isolation_level=(value); end
  end
end

module ActiveRecord
  class ActiveRecordError < StandardError; end
  class Deadlocked < ActiveRecordError; end
  class PreparedStatementInvalid < ActiveRecordError; end
  class RecordNotUnique < ActiveRecordError; end
  class SerializationFailure < ActiveRecordError; end

  class Base
    def self.abstract_class=(value); end
    def self.connection_class=(value); end
    sig { returns(ActiveRecord::ConnectionAdapters::ConnectionPool) }
    def self.connection_pool; end
    def self.establish_connection(config); end
  end

  class Result
    def self.empty(affected_rows: nil); end
    def initialize(columns, rows, column_types = nil, affected_rows: nil); end
    def affected_rows; end
  end

  module Sanitization
    module ClassMethods
      def sanitize_sql_array(array); end
      def with_connection(&block); end
    end
  end

  module ConnectionAdapters
    class AbstractAdapter
      sig { returns(String) }
      def adapter_name; end

      sig { params(sql: String, name: T.nilable(String), binds: T::Array[T.nilable(Object)], prepare: T::Boolean).returns(ActiveRecord::Result) }
      def exec_query(sql, name = nil, binds = [], prepare: false); end

      sig { params(identifier: String).returns(String) }
      def quote_column_name(identifier); end

      sig do
        type_parameters(:Result)
          .params(requires_new: T::Boolean, options: T.nilable(Object), block: T.proc.returns(T.type_parameter(:Result)))
          .returns(T.type_parameter(:Result))
      end
      def transaction(requires_new: true, **options, &block); end
    end

    class ConnectionPool
      sig do
        type_parameters(:Result)
          .params(block: T.proc.params(connection: AbstractAdapter).returns(T.type_parameter(:Result)))
          .returns(T.type_parameter(:Result))
      end
      def with_connection(&block); end

      sig { returns(AbstractAdapter) }
      def lease_connection; end

      sig { void }
      def disconnect!; end

      sig { returns(T::Boolean) }
      def active_connection?; end
    end
  end
end

module Kernel
  def Async(&blk); end
  def Sync(&blk); end
end

module Prism
  module LexCompat
    class Result; end
  end
end

module OpenTelemetry
  def self.tracer_provider; end
  def self.meter_provider; end

  module Trace
    class Span; end
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

module SQLite3
  class Backup
    def initialize(destination, destination_name, source, source_name); end
    def step(pages); end
    def finish; end
  end

  class Database; end

  class ConstraintException < StandardError; end

  class Blob < String
    def initialize(value); end
  end
end

module Durababble
  class Store
    def current_workflow_lease(workflow_id, worker_pool: nil); end
    def decode_row(row); end
    def dump_serialized(value, surface: nil, context: nil); end
    def enqueue_inbox_message(**kwargs); end
    def inbox_message(message_id); end
    def load_serialized(value); end
    def reconcile_target_activation_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, now:); end
    def set_target_activation_pending_without_transaction(worker_pool:, target_kind:, target_type:, target_id:, ready_at:); end
    def update_latest_attempt_serialized(workflow_id:, command_id:, status:, serialized_result:, error:); end
  end

  module MysqlMigrations
    def dump_serialized(value, surface: nil, context: nil); end
    def execute(sql); end
    def execute_params(sql, params); end
    def index_name(table_name, suffix); end
    def quote_column_name(identifier); end
    def raw_table_name(name); end
    def table(name); end
    def table_prefix; end
  end

  module PostgresMigrations
    def dump_serialized(value, surface: nil, context: nil); end
    def execute(sql); end
    def execute_params(sql, params); end
    def quote_column_name(identifier); end
    def quoted_schema; end
    def schema; end
    def table(name); end
  end

  module SqliteMigrations
    def execute(sql); end
    def index_name(table_name, suffix); end
    def table(name); end
  end
end
