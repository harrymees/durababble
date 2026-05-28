# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def generated_workflow_rpc_interleaving_fuzz(seed)
        run(seed, "generated_workflow_rpc_interleaving_fuzz") do |h|
          h.monitor_transitions!
          h.expect_settled!

          workflow_name = "generated-rpc-interleaving"
          workflow_id = "generated-rpc-#{seed}"
          terminal_statuses = ["completed", "failed", "canceled", "terminated"].freeze
          fault_hooks = [
            :record_step_completed,
            :record_wait,
            :enqueue_outbox,
            :complete_workflow_command,
            :claim_target_activation,
            :claim_inbox_messages,
            :fence_acquired,
          ].freeze
          selected_fault_hook = fault_hooks[h.scheduler.rng.int(fault_hooks.length)]
          h.store.fault_plan.fail_after(selected_fault_hook, message: "generated broad fuzz #{selected_fault_hook}")
          write_crash_percent = [0, 5, 10, 15][h.scheduler.rng.int(4)]
          h.scheduler.trace.event(
            h.scheduler.time,
            "generated",
            "generated_fault_plan",
            hook: selected_fault_hook,
            write_crash_percent:,
          )
          allow_step_failures = write_crash_percent.zero?

          plan = [
            { "kind" => "flaky", "name" => "generated_rpc_step_0_flaky", "failures" => allow_step_failures ? 1 + h.scheduler.rng.int(2) : 0 },
            { "kind" => "pure", "name" => "generated_rpc_step_1_pure" },
            { "kind" => "pure", "name" => "generated_rpc_step_2_pure" },
          ]
          extra_kinds = allow_step_failures ? ["pure", "flaky", "maybe_fail"].freeze : ["pure", "flaky"].freeze
          (2 + h.scheduler.rng.int(4)).times do |_offset|
            index = plan.length
            kind = extra_kinds[h.scheduler.rng.int(extra_kinds.length)]
            spec = { "kind" => kind, "name" => "generated_rpc_step_#{index}_#{kind}" }
            spec["failures"] = allow_step_failures ? 1 + h.scheduler.rng.int(2) : 0 if kind == "flaky"
            spec["fail"] = h.scheduler.rng.chance(35) if kind == "maybe_fail"
            plan << spec
          end

          attempts = Hash.new(0)
          workflow = Class.new(Durababble::Workflow)
          workflow.workflow_name(workflow_name)
          workflow.define_method(:snapshot) do |prefix:|
            {
              "prefix" => prefix,
              "released" => @released == true,
              "signals" => @signals || [],
              "state" => @state,
            }
          end
          workflow.expose(:snapshot)
          workflow.define_method(:poke) do |label:, release: false, fail: false|
            @signals ||= []
            @signals << label
            @released = true if release
            @state = "poke:#{label}"
            raise "generated command failure #{label}" if fail

            { "label" => label, "released" => @released == true }
          end
          workflow.expose_command(:poke, retry: { maximum_attempts: 2, schedule: [0] })
          workflow.define_method(:execute) do |input|
            instance = self #: as untyped
            @released = input.fetch("initially_released", false)
            @signals ||= []
            plan.reduce(input.merge("plan_length" => plan.length)) do |ctx, spec|
              instance.public_send(spec.fetch("name"), ctx)
            end.merge("signals" => @signals, "released" => @released == true)
          end
          plan.each do |spec|
            step_name = spec.fetch("name")
            case spec.fetch("kind")
            when "pure"
              workflow.define_method(step_name) do |ctx|
                @state = step_name
                h.scheduler.trace.event(h.scheduler.time, "generated", "generated_rpc_pure_step", step: step_name)
                ctx.merge(step_name => true)
              end
              workflow.step(step_name)
            when "flaky"
              workflow.define_method(step_name) do |ctx|
                key = "#{ctx.fetch("id")}:#{step_name}"
                attempts[key] += 1
                @state = "#{step_name}:#{attempts[key]}"
                h.scheduler.trace.event(
                  h.scheduler.time,
                  "generated",
                  "generated_rpc_retry_attempt",
                  attempt: attempts[key],
                  step: step_name,
                )
                raise "generated retry #{step_name} #{attempts[key]}" if attempts[key] <= spec.fetch("failures")

                ctx.merge(step_name => attempts[key])
              end
              workflow.step(
                step_name,
                retry: {
                  initial_interval: 8,
                  backoff_coefficient: 1,
                  maximum_interval: 8,
                  maximum_attempts: spec.fetch("failures") + 2,
                },
              )
            when "maybe_fail"
              workflow.define_method(step_name) do |ctx|
                @state = step_name
                h.scheduler.trace.event(
                  h.scheduler.time,
                  "generated",
                  "generated_rpc_maybe_fail_step",
                  fail: spec.fetch("fail"),
                  step: step_name,
                )
                raise "generated terminal failure #{step_name}" if spec.fetch("fail")

                ctx.merge(step_name => true)
              end
              workflow.step(step_name, retry: { maximum_attempts: 1 })
            end
          end
          h.workflows[workflow_name] = workflow

          wait_workflow_name = "generated-record-wait-primer"
          wait_workflow_id = "#{workflow_id}-wait-primer"
          wait_workflow = Class.new(Durababble::Workflow)
          wait_workflow.workflow_name(wait_workflow_name)
          wait_workflow.define_method(:execute) do |input|
            Durababble.wait_until(input.fetch("wake_at"), input.merge("waited" => true))
          end
          h.workflows[wait_workflow_name] = wait_workflow

          h.store.enqueue_workflow(
            name: workflow_name,
            input: {
              "id" => workflow_id,
              "initially_released" => false,
              "plan" => plan,
            },
            id: workflow_id,
          )
          h.store.enqueue_workflow(
            name: wait_workflow_name,
            input: { "id" => wait_workflow_id, "wake_at" => 48 + h.scheduler.rng.int(18) },
            id: wait_workflow_id,
          )
          h.scheduler.schedule(actor: "generated-wait-primer", delay: 1, name: "prime_record_wait") do
            h.store.crashable do
              Durababble::Engine.new(store: h.store, worker_id: "generated-wait-primer", lease_seconds: 30)
                .resume(wait_workflow, workflow_id: wait_workflow_id)
            end
          rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled, Durababble::LeaseConflict => e
            h.scheduler.trace.event(h.scheduler.time, "generated-wait-primer", "generated_wait_primer_yield", error: e.class.name)
          rescue Durababble::InjectedCrash => e
            h.scheduler.trace.event(h.scheduler.time, "generated-wait-primer", "generated_wait_primer_crashed", error: e.message)
          end
          h.scheduler.schedule(actor: "generated-crash-enabler", delay: 45, name: "enable_write_crashes") do
            h.store.enable_write_crashes!(percent: write_crash_percent)
            h.scheduler.trace.event(h.scheduler.time, "generated-crash-enabler", "generated_write_crashes_enabled", percent: write_crash_percent)
          end

          command_fault_hooks = [:claim_target_activation, :claim_inbox_messages, :complete_workflow_command].freeze
          if command_fault_hooks.include?(selected_fault_hook)
            primer_worker = "generated-command-primer"
            h.store.enqueue_workflow_command(
              workflow_id:,
              workflow_name:,
              method_name: "poke",
              payload: { "method" => "poke", "args" => [], "kwargs" => { label: "primer", release: true, fail: false } },
              idempotency_key: "generated-command-primer-#{seed}",
            )

            prime_command_delivery = lambda do |worker_id, lease_seconds|
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds:,
                target_kinds: ["workflow"],
                target_types: [workflow_name],
              )
              return unless activation

              target_id = activation.fetch("target_id")
              claimed = h.store.claim_workflow_for_activation(workflow_id: target_id, worker_id:, lease_seconds:)
              unless claimed
                h.scheduler.trace.event(h.scheduler.time, worker_id, "generated_command_primer_waiting_for_workflow", id: target_id)
                h.store.complete_target_activation(
                  target_kind: "workflow",
                  target_type: workflow_name,
                  target_id:,
                  worker_id:,
                  now: h.store.current_time + lease_seconds,
                )
                return
              end

              messages = h.store.claim_inbox_messages(
                target_kind: "workflow",
                target_type: workflow_name,
                target_id:,
                worker_id:,
                lease_seconds:,
                limit: 2,
              )
              messages.each do |message|
                h.store.complete_workflow_command(
                  message_id: message.fetch("id"),
                  workflow_id: target_id,
                  result: { "primer" => true },
                  worker_id:,
                )
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: workflow_name,
                target_id:,
                worker_id:,
              )
            end

            h.scheduler.schedule(actor: primer_worker, delay: 4, name: "prime_workflow_command_fault") do
              prime_command_delivery.call(primer_worker, 12)
            rescue Durababble::LeaseConflict => e
              h.scheduler.trace.event(h.scheduler.time, primer_worker, "generated_command_primer_yield", error: e.class.name)
            rescue Durababble::InjectedCrash => e
              h.scheduler.trace.event(h.scheduler.time, primer_worker, "generated_command_primer_crashed", error: e.message)
            end
            h.scheduler.schedule(actor: "#{primer_worker}-recovery", delay: 22, name: "recover_workflow_command_primer") do
              h.store.steal_expired_leases!(now: h.scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
              prime_command_delivery.call("#{primer_worker}-recovery", 12)
            rescue Durababble::LeaseConflict => e
              h.scheduler.trace.event(h.scheduler.time, "#{primer_worker}-recovery", "generated_command_primer_recovery_yield", error: e.class.name)
            rescue Durababble::InjectedCrash => e
              h.scheduler.trace.event(h.scheduler.time, "#{primer_worker}-recovery", "generated_command_primer_recovery_crashed", error: e.message)
            end
          end

          command_attempts = []
          enqueue_command_rpc = lambda do |actor, index, release:, fail:|
            handle = workflow.handle(workflow_id, store: h.store)
            handle.poke(
              label: "#{actor}-#{index}",
              release:,
              fail:,
              idempotency_key: "generated-rpc-command-#{seed}-#{index}",
            )
            h.scheduler.trace.event(h.scheduler.time, actor, "generated_command_rpc_completed", index:)
          rescue Durababble::CommandTimeout => e
            h.scheduler.trace.event(h.scheduler.time, actor, "generated_command_rpc_queued", error: e.class.name, index:)
          rescue Durababble::Error, KeyError => e
            h.scheduler.trace.event(h.scheduler.time, actor, "generated_command_rpc_rejected", error: "#{e.class}: #{e.message}", index:)
          ensure
            command_attempts << index
          end

          drain_commands = lambda do |worker_id, lease_seconds|
            3.times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds:,
                target_kinds: ["workflow"],
                target_types: [workflow_name],
              )
              break unless activation

              target_id = activation.fetch("target_id")
              claimed = h.store.claim_workflow_for_activation(workflow_id: target_id, worker_id:, lease_seconds:)
              if claimed
                h.store.crashable do
                  Durababble::Engine.new(store: h.store, worker_id:, lease_seconds:)
                    .resume(workflow, workflow_id: target_id, claimed:)
                end
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: workflow_name,
                target_id:,
                worker_id:,
              )
            rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled, Durababble::LeaseConflict => e
              h.scheduler.trace.event(h.scheduler.time, worker_id, "generated_command_drain_yield", error: e.class.name)
            end
          rescue Durababble::InjectedCrash => e
            h.scheduler.trace.event(h.scheduler.time, worker_id, "generated_command_drain_crashed", error: e.message)
          end

          rpc_faults = [
            "timeout",
            "deadline_exceeded",
            "connection_reset",
            "eof",
            "unavailable",
            "response_timeout",
            "duplicate_response",
          ].freeze
          simple_rpc = lambda do |actor, index|
            row = h.store.workflow(workflow_id)
            if !terminal_statuses.include?(row.fetch("status")) && row["next_run_at"].nil?
              h.store.claim_workflow(workflow_id:, worker_id: "generated-rpc-owner-#{index}", lease_seconds: 12)
            end
            lease = h.store.current_workflow_lease(workflow_id)
            raise Durababble::WorkflowRpc::NoActiveLease, "no generated RPC owner" unless lease

            node_id = lease.fetch("worker_id")
            fault = rpc_faults[(h.scheduler.rng.int(rpc_faults.length) + index) % rpc_faults.length]
            client = rpc_workflow_rpc_client(h, node_id, faults: [fault]) do |payload|
              handlers = {
                "summary" => lambda do |request|
                  current = h.store.workflow(workflow_id)
                  h.scheduler.trace.event(
                    h.scheduler.time,
                    "rpc",
                    "generated_simple_rpc_handler",
                    index:,
                    request: request.fetch("request"),
                    status: current.fetch("status"),
                  )
                  { "node" => node_id, "request" => request.fetch("request"), "status" => current.fetch("status") }
                end,
              }
              Durababble::WorkflowRpc::Handler.new(store: h.store, node_id:, handlers:).call(payload)
            end
            router = Durababble::WorkflowRpc::Router.new(store: h.store, rpc_clients: { node_id => client }, retry_on_stale: true)
            response = router.request(workflow_id:, command: "summary", payload: { "request" => index })
            h.scheduler.trace.event(h.scheduler.time, actor, "generated_simple_rpc_response", fault:, response:)
          rescue Durababble::WorkflowRpc::Error, Durababble::Rpc::Error, Durababble::Error, KeyError => e
            h.scheduler.trace.event(h.scheduler.time, actor, "generated_simple_rpc_rejected", error: "#{e.class}: #{e.message}", index:)
          end

          outbox_id = nil #: untyped
          claim_outbox = lambda do |actor|
            message = h.store.claim_outbox(worker_id: actor, lease_seconds: 20)
            outbox_id = message.fetch("id") if message
            h.scheduler.trace.event(h.scheduler.time, actor, "generated_outbox_claim", id: outbox_id) if message
          end
          ack_outbox = lambda do |actor|
            next unless outbox_id

            h.store.ack_outbox(outbox_id, worker_id: actor)
            h.scheduler.trace.event(h.scheduler.time, actor, "generated_outbox_ack", id: outbox_id)
          end

          h.scheduler.schedule(actor: "generated-simple-rpc-0", delay: 2, name: "simple_rpc") do
            simple_rpc.call("generated-simple-rpc-0", 0)
          end
          7.times do |i|
            command_delay = i.zero? ? 3 : 8 + h.scheduler.rng.int(90)
            h.scheduler.schedule(actor: "generated-command-rpc-#{i}", delay: command_delay, name: "command_rpc") do
              enqueue_command_rpc.call(
                "generated-command-rpc-#{i}",
                i,
                release: i.zero? || h.scheduler.rng.chance(35),
                fail: !i.zero? && h.scheduler.rng.chance(20),
              )
            end
            h.scheduler.schedule(actor: "generated-simple-rpc-#{i + 1}", delay: 10 + h.scheduler.rng.int(160), name: "simple_rpc") do
              simple_rpc.call("generated-simple-rpc-#{i + 1}", i + 1)
            end
          end

          h.scheduler.schedule(actor: "generated-outbox-producer", delay: 14, name: "enqueue_outbox") do
            outbox_id = h.store.enqueue_outbox(
              workflow_id:,
              topic: "generated",
              payload: { "seed" => seed },
              key: "generated-outbox-#{seed}",
            )
          rescue Durababble::InjectedCrash => e
            h.scheduler.trace.event(h.scheduler.time, "generated-outbox-producer", "generated_outbox_fault", error: e.message)
            outbox_id = h.store.enqueue_outbox(
              workflow_id:,
              topic: "generated",
              payload: { "seed" => seed, "retry" => true },
              key: "generated-outbox-#{seed}",
            )
          end
          h.scheduler.schedule(actor: "generated-outbox-claimer", delay: 34, name: "claim_outbox") do
            claim_outbox.call("generated-outbox-claimer")
          end
          h.scheduler.schedule(actor: "generated-outbox-acker", delay: 36, name: "ack_outbox") do
            ack_outbox.call("generated-outbox-claimer")
          end

          h.scheduler.schedule(actor: "generated-fence", delay: 16, name: "fence") do
            h.store.with_fence(workflow_id:, key: "generated-fence", timeout: 8) { { "seed" => seed } }
          rescue Durababble::InjectedCrash => e
            h.scheduler.trace.event(h.scheduler.time, "generated-fence", "generated_fence_fault", error: e.message)
          rescue Durababble::FenceTimeout => e
            h.scheduler.trace.event(h.scheduler.time, "generated-fence", "generated_fence_waited", error: e.message)
          end
          h.scheduler.schedule(actor: "generated-fence-recover", delay: 46, name: "recover_fence") do
            h.store.with_fence(workflow_id:, key: "generated-fence", timeout: 8) { { "seed" => seed, "recovered" => true } }
          rescue Durababble::FenceTimeout => e
            h.scheduler.trace.event(h.scheduler.time, "generated-fence-recover", "generated_fence_recover_waited", error: e.message)
          end

          h.add_workers(
            ["generated-worker-a", "generated-worker-b", "generated-worker-c"],
            ticks: 34,
            crash_percent: 10 + h.scheduler.rng.int(8),
          )
          10.times do |i|
            h.scheduler.schedule(actor: "generated-command-worker-#{i}", delay: 20 + i * 18 + h.scheduler.rng.int(9), name: "drain_commands") do
              drain_commands.call("generated-command-worker-#{i}", 12)
            end
            h.scheduler.schedule(actor: "generated-reaper-#{i}", delay: 28 + i * 22, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
            end
            h.scheduler.schedule(actor: "generated-timer-#{i}", delay: 40 + i * 20, name: "claim_due_wait_primer") do
              resume_workflow_once(h, actor: "generated-timer-#{i}", workflow: wait_workflow, workflow_id: wait_workflow_id)
            end
          end

          final_resume = lambda do |worker_id, target_workflow_name, target_workflow_id|
            row = h.store.workflow(target_workflow_id)
            return if terminal_statuses.include?(row.fetch("status"))

            Durababble::Engine.new(store: h.store, worker_id:, lease_seconds: 30)
              .resume(h.workflows.fetch(target_workflow_name), workflow_id: target_workflow_id)
          rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled, Durababble::LeaseConflict => e
            h.scheduler.trace.event(h.scheduler.time, worker_id, "generated_final_resume_yield", error: e.class.name, id: target_workflow_id)
          rescue Durababble::InjectedCrash => e
            h.scheduler.trace.event(h.scheduler.time, worker_id, "generated_final_resume_crashed", error: e.message, id: target_workflow_id)
          end
          h.scheduler.schedule(actor: "generated-final-command", delay: 260, name: "final_command_rpc") do
            row = h.store.workflow(workflow_id)
            unless terminal_statuses.include?(row.fetch("status"))
              enqueue_command_rpc.call("generated-final-command", 10_000, release: true, fail: false)
            end
          end
          h.scheduler.schedule(actor: "generated-settler", delay: 258, name: "disable_write_crashes") do
            h.store.enable_write_crashes!(percent: 0)
            h.scheduler.trace.event(h.scheduler.time, "generated-settler", "generated_write_crashes_disabled")
          end
          [270, 300, 330, 360].each_with_index do |delay, index|
            h.scheduler.schedule(actor: "generated-final-reaper-#{index}", delay:, name: "final_steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
            end
            h.scheduler.schedule(actor: "generated-final-timer-#{index}", delay: delay + 2, name: "final_claim_due_wait_primer") do
              resume_workflow_once(h, actor: "generated-final-timer-#{index}", workflow: wait_workflow, workflow_id: wait_workflow_id)
            end
            h.scheduler.schedule(actor: "generated-final-command-worker-#{index}", delay: delay + 4, name: "final_drain_commands") do
              drain_commands.call("generated-final-command-worker-#{index}", 30)
            end
            h.scheduler.schedule(actor: "generated-final-worker-#{index}", delay: delay + 8, name: "final_resume") do
              final_resume.call("generated-final-worker-#{index}", workflow_name, workflow_id)
              final_resume.call("generated-final-worker-#{index}", wait_workflow_name, wait_workflow_id)
            end
          end
          h.scheduler.schedule(actor: "generated-final-outbox-claimer", delay: 365, name: "final_claim_outbox") do
            claim_outbox.call("generated-final-outbox-claimer")
          end
          h.scheduler.schedule(actor: "generated-final-outbox-acker", delay: 367, name: "final_ack_outbox") do
            ack_outbox.call("generated-final-outbox-claimer")
          end

          h.check("generated broad fuzz injected its selected store fault") do
            h.scheduler.trace.to_s.include?("generated broad fuzz #{selected_fault_hook}")
          end
          h.check("generated broad fuzz attempted command RPCs and simple RPCs") do
            trace = h.scheduler.trace.to_s
            !command_attempts.empty? &&
              trace.include?("generated_command_rpc_queued") &&
              trace.include?("generated_simple_rpc_response")
          end
          h.check("generated broad fuzz exercised RPC transport fault handling") do
            trace = h.scheduler.trace.to_s
            rpc_faults.any? { |fault| trace.include?("rpc.#{fault}") } ||
              trace.include?("rpc.rst") ||
              trace.include?("rpc.response_timeout") ||
              trace.include?("rpc.duplicate_response")
          end
          h.check("generated broad fuzz exercised generated retry shape") do
            h.scheduler.trace.to_s.include?("generated_rpc_retry_attempt")
          end
          h.check("generated broad fuzz leaves the workflow terminal or parked by a durable wait/retry") do
            row = h.store.workflow(workflow_id)
            terminal_statuses.include?(row.fetch("status")) ||
              h.store.all_waits.values.any? { |wait| wait.fetch("workflow_id") == workflow_id && wait.fetch("status") == "pending" } ||
              !row["next_run_at"].nil?
          end
        end
      end
    end
  end
end
