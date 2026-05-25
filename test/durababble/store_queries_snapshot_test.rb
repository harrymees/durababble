# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

# Renders every registered store query for each backend to a byte-exact text
# snapshot. This is the guardrail for the SQL-adapter DRY refactor: the MySQL
# snapshot must never change (it protects the query-plan tests that cannot run
# without a live database here), and the Postgres snapshot may only change
# deliberately (regenerate with UPDATE_SNAPSHOTS=1 and review the diff).
#
# Queries are rendered offline against a fake connection that only provides
# `quote_column_name`, so no database server is required.
class StoreQueriesSnapshotTest < DurababbleTestCase
  SNAPSHOT_DIR = File.expand_path("snapshots", __dir__)

  # Canned values for the handful of locals some query builders interpolate.
  # The exact values are irrelevant — only stability matters.
  LOCALS = {
    index: "<index>",
    name_sql: "<name_sql>",
    filter_sql: "<filter_sql>",
    name_filter: "<name_filter>",
    limit: 100,
    table_name: "workflows",
  }.freeze

  FakeConnection = Struct.new(:quote_char) do
    def quote_column_name(name)
      "#{quote_char}#{name}#{quote_char}"
    end
  end

  def fake_store(backend)
    case backend
    when :mysql
      Durababble::MysqlStore.new(FakeConnection.new("`"), schema: "durababble_mysql_snapshot")
    when :postgres
      Durababble::PostgresStore.new(FakeConnection.new('"'), schema: "durababble_pg_snapshot")
    else
      raise ArgumentError, backend.to_s
    end
  end

  def locals_for(query)
    query.builder.parameters.each_with_object({}) do |(kind, name), locals|
      next unless [:keyreq, :key].include?(kind)

      raise "no canned local for #{name.inspect} (query #{query.id})" unless LOCALS.key?(name)

      locals[name] = LOCALS.fetch(name)
    end
  end

  def render_snapshot(backend)
    store = fake_store(backend)
    Durababble::StoreQueries.query_ids(backend).map do |id|
      query = Durababble::StoreQueries::QUERIES.fetch(id)
      sql = Durababble::StoreQueries.sql(id, store, locals_for(query))
      "-- #{id}\n#{sql.strip}\n"
    end.join("\n")
  end

  def assert_snapshot(backend)
    actual = render_snapshot(backend)
    path = File.join(SNAPSHOT_DIR, "store_queries_#{backend}.sql")

    if ENV["UPDATE_SNAPSHOTS"] || !File.exist?(path)
      FileUtils.mkdir_p(SNAPSHOT_DIR)
      File.write(path, actual)
      skip("wrote snapshot #{path}; re-run without UPDATE_SNAPSHOTS to assert")
    end

    expected = File.read(path)
    assert_equal(
      expected,
      actual,
      "#{backend} store-query SQL changed. If intentional, regenerate with " \
        "UPDATE_SNAPSHOTS=1 and review the diff.",
    )
  end

  test "mysql store query SQL matches the committed snapshot" do
    assert_snapshot(:mysql)
  end

  test "postgres store query SQL matches the committed snapshot" do
    assert_snapshot(:postgres)
  end
end
