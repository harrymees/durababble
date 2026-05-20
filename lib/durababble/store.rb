# frozen_string_literal: true

require "json"
require "pg"
require "securerandom"

module Durababble
  class Store
    attr_reader :schema

    def self.connect(database_url:, schema: "durababble")
      new(PG.connect(database_url), schema:)
    end

    def initialize(connection, schema:)
      @connection = connection
      @schema = schema
    end

    def migrate!
      execute("CREATE SCHEMA IF NOT EXISTS #{quoted_schema}")
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("workflows")} (
          id text PRIMARY KEY,
          name text NOT NULL,
          status text NOT NULL,
          input jsonb NOT NULL DEFAULT '{}'::jsonb,
          result jsonb,
          error text,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now()
        )
      SQL
      execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS #{table("steps")} (
          workflow_id text NOT NULL REFERENCES #{table("workflows")}(id) ON DELETE CASCADE,
          position integer NOT NULL,
          name text NOT NULL,
          status text NOT NULL,
          result jsonb,
          error text,
          started_at timestamptz,
          completed_at timestamptz,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (workflow_id, position)
        )
      SQL
      self
    end

    def drop_schema!
      execute("DROP SCHEMA IF EXISTS #{quoted_schema} CASCADE")
    end

    def close
      @connection.close unless @connection.finished?
    end

    def create_workflow(name:, input:)
      id = SecureRandom.uuid
      execute_params(
        "INSERT INTO #{table("workflows")} (id, name, status, input) VALUES ($1, $2, 'running', $3::jsonb)",
        [id, name, dump_json(input)]
      )
      id
    end

    def mark_workflow_running(workflow_id)
      execute_params("UPDATE #{table("workflows")} SET status = 'running', error = NULL, updated_at = now() WHERE id = $1", [workflow_id])
    end

    def complete_workflow(workflow_id, result:)
      execute_params(
        "UPDATE #{table("workflows")} SET status = 'completed', result = $2::jsonb, error = NULL, updated_at = now() WHERE id = $1",
        [workflow_id, dump_json(result)]
      )
    end

    def fail_workflow(workflow_id, error:)
      execute_params(
        "UPDATE #{table("workflows")} SET status = 'failed', error = $2, updated_at = now() WHERE id = $1",
        [workflow_id, error]
      )
    end

    def record_step_started(workflow_id:, position:, name:)
      execute_params(<<~SQL, [workflow_id, position, name])
        INSERT INTO #{table("steps")} (workflow_id, position, name, status, started_at, updated_at)
        VALUES ($1, $2, $3, 'running', now(), now())
        ON CONFLICT (workflow_id, position) DO UPDATE
          SET status = 'running', error = NULL, started_at = COALESCE(#{table("steps")}.started_at, now()), updated_at = now()
      SQL
    end

    def record_step_completed(workflow_id:, position:, result:)
      execute_params(
        "UPDATE #{table("steps")} SET status = 'completed', result = $3::jsonb, error = NULL, completed_at = now(), updated_at = now() WHERE workflow_id = $1 AND position = $2",
        [workflow_id, position, dump_json(result)]
      )
    end

    def record_step_failed(workflow_id:, position:, error:)
      execute_params(
        "UPDATE #{table("steps")} SET status = 'failed', error = $3, updated_at = now() WHERE workflow_id = $1 AND position = $2",
        [workflow_id, position, error]
      )
    end

    def workflow(workflow_id)
      result = execute_params("SELECT * FROM #{table("workflows")} WHERE id = $1", [workflow_id])
      row = result.first
      raise KeyError, "workflow not found: #{workflow_id}" unless row

      decode_row(row)
    end

    def steps_for(workflow_id)
      execute_params("SELECT * FROM #{table("steps")} WHERE workflow_id = $1 ORDER BY position", [workflow_id]).map { |row| decode_row(row) }
    end

    private

    def execute(sql)
      @connection.exec(sql)
    end

    def execute_params(sql, params)
      @connection.exec_params(sql, params)
    end

    def table(name)
      "#{quoted_schema}.#{PG::Connection.quote_ident(name)}"
    end

    def quoted_schema
      PG::Connection.quote_ident(schema)
    end

    def dump_json(value)
      JSON.generate(value || {})
    end

    def decode_row(row)
      row.transform_values do |value|
        next value unless value.is_a?(String)

        begin
          JSON.parse(value)
        rescue JSON::ParserError
          value
        end
      end
    end
  end
end
