# typed: false
# frozen_string_literal: true

require "active_record"
require "durababble"

module DurababbleScriptedSqlSupport
  class << self
    def sql_result(rows = [], affected_rows: rows.length)
      columns = rows.flat_map(&:keys).uniq
      ActiveRecord::Result.new(columns, rows.map { |row| columns.map { |column| row[column] } }, affected_rows:)
    end

    def included(base)
      base.extend(ClassMethods)
    end
  end

  module ClassMethods
    def sql_result(rows = [], affected_rows: rows.length)
      DurababbleScriptedSqlSupport.sql_result(rows, affected_rows:)
    end
  end

  class ScriptedPgConnection
    attr_reader :exec_params_calls, :exec_calls, :closed

    def initialize(params_results: [], exec_results: [], finished: false)
      @params_results = params_results
      @exec_results = exec_results
      @exec_params_calls = []
      @exec_calls = []
      @finished = finished
      @closed = false
    end

    def adapter_name = "PostgreSQL"

    def exec_query(sql, name = nil, binds = [], prepare: false)
      params = binds
      if name == "Durababble SQL"
        @exec_params_calls << [sql, params]
        result = @params_results.shift || DurababbleScriptedSqlSupport.sql_result
        result.is_a?(Proc) ? result.call(sql, params) : result
      else
        @exec_calls << sql
        result = @exec_results.shift || DurababbleScriptedSqlSupport.sql_result
        result.is_a?(Proc) ? result.call(sql) : result
      end
    end

    def transaction(requires_new: true)
      yield
    end

    def quote(value)
      case value
      when nil
        "NULL"
      when true
        "TRUE"
      when false
        "FALSE"
      when Numeric
        value.to_s
      else
        "'#{value.to_s.gsub("'", "''")}'"
      end
    end

    def quote_column_name(identifier)
      %("#{identifier.to_s.gsub("\"", "\"\"")}")
    end

    def close
      @closed = true
    end
  end

  class ScriptedMysqlConnection
    attr_reader :queries

    def initialize(&result_for_query)
      @result_for_query = result_for_query
      @queries = []
    end

    def adapter_name = "Trilogy"

    def exec_query(sql, name = nil, binds = [], prepare: false)
      @queries << sql
      @result_for_query&.call(sql) || DurababbleScriptedSqlSupport.sql_result
    end

    def transaction(requires_new: true)
      yield
    end

    def quote(value)
      case value
      when nil
        "NULL"
      when true
        "TRUE"
      when false
        "FALSE"
      when Numeric
        value.to_s
      when Time
        "'#{value.utc.strftime("%Y-%m-%d %H:%M:%S.%6N")}'"
      else
        "'#{value.to_s.gsub("'", "''")}'"
      end
    end

    def quote_column_name(identifier)
      "`#{identifier.to_s.gsub("`", "``")}`"
    end

    def cast_bound_value(value)
      case value
      when true
        "1"
      when false
        "0"
      when Numeric
        value.to_s
      else
        value
      end
    end
  end

  class FlakyDeliveryClient
    attr_reader :deliveries

    def initialize(failures:)
      @failures = failures
      @deliveries = []
    end

    def deliver_message(**kwargs)
      @deliveries << kwargs
      if @failures.positive?
        @failures -= 1
        raise Durababble::Rpc::Unavailable, "temporarily unavailable"
      end

      true
    end
  end

  class MysqlMigrationProbeStore < Durababble::MysqlStore
    attr_reader :executed

    def initialize(schema:, columns: {})
      super(ScriptedMysqlConnection.new, schema:)
      @columns = columns
      @executed = []
    end

    def execute_params(sql, params)
      @executed << [:execute_params, sql, params]
      table = params.first
      column = params[1]
      rows = if sql.include?("information_schema.columns")
        @columns.fetch(table, []).include?(column) ? [{ "exists" => 1 }] : []
      else
        []
      end
      DurababbleScriptedSqlSupport.sql_result(rows)
    end

    def execute(sql)
      @executed << [:execute, sql]
      DurababbleScriptedSqlSupport.sql_result
    end
  end

  def sql_result(rows = [], affected_rows: rows.length)
    DurababbleScriptedSqlSupport.sql_result(rows, affected_rows:)
  end

  def pg_dump(value)
    Durababble::Store::SERIALIZER.dump(value).then { |bytes| "\\x#{bytes.unpack1("H*")}" }
  end

  private :sql_result, :pg_dump
end
