# typed: false
# frozen_string_literal: true

require "uri"

DurababbleStoreBackend = Data.define(:name, :database_url, :default_schema_prefix) do
  def sqlite?
    database_url.start_with?("sqlite://")
  end

  def mysql?
    database_url.start_with?("mysql://", "mysql2://", "trilogy://")
  end

  def postgres?
    !sqlite? && !mysql?
  end
end

def durababble_default_database_url
  ENV.fetch("DURABABBLE_DATABASE_URL") { durababble_mysql_database_url }
end

def durababble_mysql_database_url
  ENV.fetch("DURABABBLE_MYSQL_DATABASE_URL") do
    user = URI.encode_www_form_component(ENV.fetch("DURABABBLE_MYSQL_USERNAME", ENV.fetch("MYSQL_USER", "root")))
    password = ENV.fetch("DURABABBLE_MYSQL_PASSWORD", ENV.fetch("MYSQL_PASSWORD", nil))
    password = nil if password.to_s.empty?
    host = ENV.fetch("DURABABBLE_MYSQL_HOST", ENV.fetch("MYSQL_HOST", "127.0.0.1"))
    port = ENV.fetch("DURABABBLE_MYSQL_PORT", ENV.fetch("MYSQL_PORT", "3306"))
    database = ENV.fetch("DURABABBLE_MYSQL_DATABASE", "sidekick_server_test")
    credentials = password ? "#{user}:#{URI.encode_www_form_component(password)}" : user

    "mysql://#{credentials}@#{host}:#{port}/#{database}"
  end
end

def durababble_postgres_database_url
  ENV.fetch("DURABABBLE_POSTGRES_DATABASE_URL")
end

def durababble_postgres_enabled?
  !ENV["DURABABBLE_POSTGRES_DATABASE_URL"].to_s.empty?
end

def durababble_yugabyte_database_url
  ENV.fetch("DURABABBLE_YUGABYTE_DATABASE_URL")
end

def durababble_yugabyte_enabled?
  !ENV["DURABABBLE_YUGABYTE_DATABASE_URL"].to_s.empty?
end

def durababble_test_backend_names
  names = ENV.fetch("DURABABBLE_TEST_BACKENDS", "")
    .split(",")
    .map { |name| name.strip.downcase }
    .reject(&:empty?)
  unknown = names - ["mysql", "postgres", "yugabyte"]
  raise ArgumentError, "unknown DURABABBLE_TEST_BACKENDS value(s): #{unknown.join(", ")}" unless unknown.empty?

  names
end

def durababble_backend_selected?(name)
  names = durababble_test_backend_names
  names.empty? || names.include?(name)
end

def durababble_backend_requested?(name)
  durababble_test_backend_names.include?(name)
end

def durababble_store_backends
  backends = []

  if durababble_backend_selected?("mysql")
    backends << DurababbleStoreBackend.new(
      name: "mysql",
      database_url: durababble_mysql_database_url,
      default_schema_prefix: "durababble_mysql",
    )
  end

  if durababble_backend_requested?("postgres")
    backends << DurababbleStoreBackend.new(
      name: "postgres",
      database_url: durababble_postgres_database_url,
      default_schema_prefix: "durababble_pg",
    )
  end

  if durababble_backend_selected?("yugabyte") && (durababble_yugabyte_enabled? || durababble_backend_requested?("yugabyte"))
    backends << DurababbleStoreBackend.new(
      name: "yugabyte",
      database_url: durababble_yugabyte_database_url,
      default_schema_prefix: "durababble_yb",
    )
  end

  raise ArgumentError, "no Durababble store backends selected" if backends.empty?

  backends
end

# The in-memory SQLite store is test-only and single-serialized-connection. It
# is exercised by the backend conformance suite (proving it runs the same SQL
# contract as the production adapters) but is deliberately kept out of the
# broader integration suites, which assume real multi-connection backends. DST
# exercises it separately under simulation.
def durababble_sqlite_backend
  DurababbleStoreBackend.new(
    name: "sqlite",
    database_url: "sqlite://memory",
    default_schema_prefix: "durababble_sqlite",
  )
end

def durababble_conformance_store_backends
  [durababble_sqlite_backend, *durababble_store_backends]
end

def skip_without_yugabyte!
  skip("set DURABABBLE_YUGABYTE_DATABASE_URL to run Yugabyte-backed Durababble tests") unless durababble_yugabyte_enabled?
end
