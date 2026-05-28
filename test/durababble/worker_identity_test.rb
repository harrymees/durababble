# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleWorkerIdentityTest < DurababbleTestCase
  test "build rejects malformed worker identity parts" do
    assert_raises_matching(ArgumentError, /id cannot be empty/) do
      Durababble::WorkerIdentity.build(id: "", address: "127.0.0.1:7000")
    end
    assert_raises_matching(ArgumentError, /address cannot be empty/) do
      Durababble::WorkerIdentity.build(id: "worker-a", address: "")
    end
    assert_raises_matching(ArgumentError, /id cannot contain @/) do
      Durababble::WorkerIdentity.build(id: "worker@a", address: "127.0.0.1:7000")
    end
  end

  test "extracts ids only from composite worker identities" do
    assert_equal("worker-a@127.0.0.1:7000", Durababble::WorkerIdentity.build(id: "worker-a", address: "127.0.0.1:7000"))
    assert_equal("worker-a", Durababble::WorkerIdentity.id_for("worker-a@127.0.0.1:7000"))
    assert_nil(Durababble::WorkerIdentity.id_for("127.0.0.1:7000"))
  end
end
