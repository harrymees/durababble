# typed: false
# frozen_string_literal: true

require "uri"

DurababbleStoreBackend = Data.define(:name, :database_url, :default_schema_prefix) do
  def mysql?
    database_url.start_with?("mysql://", "mysql2://", "trilogy://")
  end

  def postgres?
    !mysql?
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

def durababble_yugabyte_database_url
  ENV.fetch("DURABABBLE_YUGABYTE_DATABASE_URL")
end

def durababble_yugabyte_enabled?
  !ENV["DURABABBLE_YUGABYTE_DATABASE_URL"].to_s.empty?
end

def durababble_store_backends
  backends = [
    DurababbleStoreBackend.new(
      name: "mysql",
      database_url: durababble_mysql_database_url,
      default_schema_prefix: "durababble_mysql",
    ),
  ]

  if durababble_yugabyte_enabled?
    backends << DurababbleStoreBackend.new(
      name: "yugabyte",
      database_url: durababble_yugabyte_database_url,
      default_schema_prefix: "durababble_yb",
    )
  end

  backends
end

def skip_without_yugabyte!
  skip("set DURABABBLE_YUGABYTE_DATABASE_URL to run Yugabyte-backed Durababble tests") unless durababble_yugabyte_enabled?
end
