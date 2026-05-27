# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def rpc_fault_injection(seed)
        run(seed, "rpc_fault_injection") do |h|
          outcomes = ["success", "timeout", "connection_error", "eof", "remote_error", "idle_disconnect_reconnect"]
          outcomes.rotate(h.scheduler.rng.int(outcomes.length)).each_with_index do |outcome, index|
            h.scheduler.schedule(actor: "rpc-client", delay: index * 3, name: "rpc:#{outcome}") do
              h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.request", id: index, outcome:)
              case outcome
              when "success"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.success", id: index)
              when "timeout"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.timeout", id: index)
              when "connection_error"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.connection_error", id: index)
              when "eof"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.eof", id: index)
              when "remote_error"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.remote_error", id: index)
              when "idle_disconnect_reconnect"
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.idle_disconnect", id: index)
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.reconnect", id: index)
                h.scheduler.trace.event(h.scheduler.time, "rpc", "rpc.success", id: index)
              end
            end
          end
          h.check("success path observed") { h.scheduler.trace.to_s.include?("rpc.success") }
          h.check("timeout fault observed") { h.scheduler.trace.to_s.include?("rpc.timeout") }
          h.check("connection fault observed") { h.scheduler.trace.to_s.include?("rpc.connection_error") }
          h.check("eof fault observed") { h.scheduler.trace.to_s.include?("rpc.eof") }
          h.check("remote error observed") { h.scheduler.trace.to_s.include?("rpc.remote_error") }
          h.check("idle disconnect recovery observed") { h.scheduler.trace.to_s.include?("rpc.reconnect") }
        end
      end
    end
  end
end
