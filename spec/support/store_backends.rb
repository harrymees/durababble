# frozen_string_literal: true

DurababbleStoreBackend = Data.define(:name, :database_url, :default_schema_prefix) do
  def mysql?
    database_url.start_with?("mysql://", "mysql2://")
  end

  def postgres?
    !mysql?
  end
end

def durababble_store_backends
  [
    DurababbleStoreBackend.new(
      name: "yugabyte",
      database_url: ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte"),
      default_schema_prefix: "durababble_yb"
    ),
    DurababbleStoreBackend.new(
      name: "mysql",
      database_url: ENV.fetch("DURABABBLE_MYSQL_DATABASE_URL", "mysql2://durababble:durababble@127.0.0.1:3306/durababble_test"),
      default_schema_prefix: "durababble_mysql"
    )
  ]
end
