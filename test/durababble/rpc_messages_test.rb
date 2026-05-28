# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleRpcMessagesTest < DurababbleTestCase
  test "transient requests expose the method field and regular readers through brackets" do
    request = Durababble::Rpc::Messages::TransientRequest.new(
      worker_pool: "default",
      method: "status",
    )

    assert_equal("status", request["method"])
    assert_equal("default", request[:worker_pool])
  end
end
