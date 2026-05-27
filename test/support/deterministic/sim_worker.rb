# typed: true
# frozen_string_literal: true

require_relative "deterministic_sqlite_store"

module Durababble
  module Deterministic
    class SimWorker
      #: (id: untyped, scheduler: untyped, network: untyped, store: untyped, workflows: untyped, ?tick_interval: untyped, ?crash_percent: untyped) -> void
      def initialize(id:, scheduler:, network:, store:, workflows:, tick_interval: 20, crash_percent: 0)
        @id = id
        @scheduler = scheduler
        @network = network
        @store = store
        @workflows = workflows
        @tick_interval = tick_interval
        @crash_percent = crash_percent
      end

      #: (?ticks: untyped) -> untyped
      def start(ticks: 20)
        ticks.times do |tick|
          @scheduler.schedule(actor: @id, delay: 5 + tick * @tick_interval + @scheduler.rng.int(7), name: "worker_tick") { run_tick }
        end
      end

      #: () -> untyped
      def run_tick
        @network.send(source: @id, target: "db", type: "worker_tick") do
          if @scheduler.rng.chance(@crash_percent)
            @store.steal_expired_leases!(now: @scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
            @scheduler.trace.event(@scheduler.time, @id, "crash_before_tick")
            next
          end

          claimed = @store.claim_runnable_workflow(worker_id: @id, lease_seconds: Engine::DEFAULT_LEASE_SECONDS)
          next unless claimed

          workflow = @workflows.fetch(claimed.fetch("name"))
          # Arm the store crash proxy for the resume so a worker can die between
          # any two durable writes mid-workflow (not just at crash_before_tick).
          # The crash is caught here, modelling a process death; a later tick or
          # the reaper reclaims the lease and replay drives the workflow forward.
          @store.crashable do
            Engine.new(store: @store, worker_id: @id).resume(workflow, workflow_id: claimed.fetch("id"))
          end
        rescue LeaseConflict => e
          @scheduler.trace.event(@scheduler.time, @id, "lease_conflict", error: e.message)
        rescue InjectedCrash => e
          @scheduler.trace.event(@scheduler.time, @id, "crashed_mid_resume", error: e.message)
        end
      end
    end
  end
end
