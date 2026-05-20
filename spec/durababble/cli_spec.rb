# frozen_string_literal: true

require "open3"
require "spec_helper"

RSpec.describe "durababble CLI", :integration do
  let(:database_url) { ENV.fetch("DURABABBLE_DATABASE_URL", "postgresql://yugabyte@127.0.0.1:15433/yugabyte") }
  let(:schema) { "durababble_cli_test_#{Process.pid}_#{object_id.abs}" }
  let(:store) { Durababble::Store.connect(database_url:, schema:) }
  let(:exe) { File.expand_path("../../exe/durababble", __dir__) }

  after do
    store.drop_schema!
    store.close
  rescue StandardError
    nil
  end

  def run_cli(*args)
    Open3.capture3({ "DURABABBLE_DATABASE_URL" => database_url }, "mise", "exec", "--", "ruby", exe, *args, chdir: File.expand_path("../..", __dir__))
  end

  it "migrates, runs, inspects, and resumes the built-in counter workflow" do
    stdout, stderr, status = run_cli("migrate", "--schema", schema)
    expect(status).to be_success, stderr
    expect(stdout).to include("migrated #{schema}")

    stdout, stderr, status = run_cli("run-counter", "--schema", schema, "--count", "3")
    expect(status).to be_success, stderr
    run_id = stdout[/[0-9a-f-]{36}/]
    expect(run_id).not_to be_nil
    expect(stdout).to include("completed")
    expect(stdout).to include('"count"=>8').or include('"count" => 8')

    stdout, stderr, status = run_cli("inspect", run_id, "--schema", schema)
    expect(status).to be_success, stderr
    expect(stdout).to include(run_id)
    expect(stdout).to include("completed")

    stdout, stderr, status = run_cli("resume-counter", run_id, "--schema", schema)
    expect(status).to be_success, stderr
    expect(stdout).to include("completed")
    expect(stdout).to include('"count"=>8').or include('"count" => 8')
  end
end
