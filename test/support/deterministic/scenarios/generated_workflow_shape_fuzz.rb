# typed: true
# frozen_string_literal: true

module Durababble
  module Deterministic
    module Scenarios
      #: (untyped) -> untyped
      def generated_workflow_shape_fuzz(seed)
        run(seed, "generated_workflow_shape_fuzz") do |h|
          h.expect_settled!

          retry_failures = 1 + h.scheduler.rng.int(2)
          retry_attempts = Hash.new(0)
          h.workflows["generated-timer-retry"] = workflow_class("generated-timer-retry") do
            test_step(
              "flaky",
              retry_policy: { initial_interval: 8, backoff_coefficient: 1, maximum_interval: 8, maximum_attempts: 4 },
            ) do |ctx|
              id = ctx.fetch("id")
              retry_attempts[id] += 1
              h.scheduler.trace.event(h.scheduler.time, "generated", "generated_retry_attempt", id:, attempt: retry_attempts[id])
              raise "generated retry #{retry_attempts[id]}" if retry_attempts[id] <= retry_failures

              ctx.merge("retried" => retry_attempts[id])
            end
            test_step("nap") { |ctx| Durababble.wait_until(ctx.fetch("wake_at"), ctx.merge("napped" => true)) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end

          command_workflow = workflow_class("generated-command-wait") do
            expose_command(:signal)
            define_method(:signal) do
              instance = self #: as untyped
              instance.instance_variable_set(:@signaled, true)
              { "signaled" => true }
            end
            define_method(:execute) do |input|
              instance = self #: as untyped
              instance.wait_condition(timeout: input.fetch("timeout")) { instance.instance_variable_get(:@signaled) == true }
              input.merge("released" => true)
            end
          end
          h.workflows["generated-command-wait"] = command_workflow

          shape_plan = Array.new(3 + h.scheduler.rng.int(3)) do |index|
            kind = index.zero? || h.scheduler.rng.chance(55) ? "retry" : "pure"
            {
              "kind" => kind,
              "name" => "generated_shape_#{index}_#{kind}",
              "failures" => kind == "retry" ? 1 + h.scheduler.rng.int(2) : 0,
            }
          end
          shape_attempts = Hash.new(0)
          shape_workflow = Class.new(Durababble::Workflow)
          shape_workflow.workflow_name("generated-step-chain")
          shape_workflow.define_method(:execute) do |input|
            instance = self #: as untyped
            shape_plan.reduce(input) { |ctx, spec| instance.public_send(spec.fetch("name"), ctx) }
          end
          shape_plan.each do |spec|
            step_name = spec.fetch("name")
            if spec.fetch("kind") == "retry"
              shape_workflow.define_method(step_name) do |ctx|
                key = "#{ctx.fetch("id")}:#{step_name}"
                shape_attempts[key] += 1
                h.scheduler.trace.event(
                  h.scheduler.time,
                  "generated",
                  "generated_shape_retry_attempt",
                  attempt: shape_attempts[key],
                  step: step_name,
                )
                raise "generated shape retry #{step_name} #{shape_attempts[key]}" if shape_attempts[key] <= spec.fetch("failures")

                ctx.merge(step_name => shape_attempts[key])
              end
              shape_workflow.step(step_name, retry: { initial_interval: 7, backoff_coefficient: 1, maximum_interval: 7, maximum_attempts: 5 })
            else
              shape_workflow.define_method(step_name) do |ctx|
                h.scheduler.trace.event(h.scheduler.time, "generated", "generated_shape_pure_step", step: step_name)
                ctx.merge(step_name => true)
              end
              shape_workflow.step(step_name)
            end
          end
          h.workflows["generated-step-chain"] = shape_workflow

          h.workflows["generated-cancelable"] = Class.new(Durababble::Workflow) do
            workflow_name "generated-cancelable"
            define_method(:execute) do |input|
              instance = self #: as untyped
              instance.wait_for_timer(input)
              input.merge("completed" => true)
            rescue Durababble::CancellationError => e
              instance.cleanup(input.merge("reason" => e.reason))
            end
            define_method(:wait_for_timer) { |input| Durababble.wait_until(input.fetch("wake_at"), input) }
            step :wait_for_timer
            define_method(:cleanup) do |input|
              h.scheduler.trace.event(h.scheduler.time, "generated", "generated_cleanup", id: input.fetch("id"))
              input.merge("cleaned" => true)
            end
            step :cleanup
          end

          h.workflows["generated-terminating"] = workflow_class("generated-terminating") do
            test_step("wait") { |ctx| Durababble.wait_until(ctx.fetch("wake_at"), ctx) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end

          workflows = [
            ["generated-timer-retry", "generated-timer-retry", { "id" => "timer-retry", "wake_at" => 80 + h.scheduler.rng.int(30) }],
            ["generated-command-wait", "generated-command-wait", { "id" => "command-wait", "timeout" => 120 + h.scheduler.rng.int(30) }],
            ["generated-step-chain", "generated-step-chain", { "id" => "step-chain", "shape" => shape_plan }],
            ["generated-cancelable", "generated-cancelable", { "id" => "cancelable", "wake_at" => 160 }],
            ["generated-terminating", "generated-terminating", { "id" => "terminating", "wake_at" => 180 }],
          ]
          workflow_ids = workflows.map do |name, id, input|
            [name, h.store.enqueue_workflow(name:, input:, id:)]
          end.to_h

          h.add_workers(["shape-worker-a", "shape-worker-b", "shape-worker-c"], ticks: 36, crash_percent: 10)

          h.scheduler.schedule(actor: "shape-client", delay: 5 + h.scheduler.rng.int(20), name: "signal_command_wait") do
            h.store.enqueue_workflow_command(
              workflow_id: workflow_ids.fetch("generated-command-wait"),
              workflow_name: "generated-command-wait",
              method_name: "signal",
              payload: { "method" => "signal", "args" => [], "kwargs" => {} },
            )
          end
          h.scheduler.schedule(actor: "shape-canceler", delay: 18 + h.scheduler.rng.int(25), name: "request_cancel") do
            h.store.crashable do
              h.store.request_workflow_cancellation(workflow_id: workflow_ids.fetch("generated-cancelable"), reason: "generated cancel")
            end
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "shape-canceler", "generated_cancel_crashed")
          end
          h.scheduler.schedule(actor: "shape-terminator", delay: 20 + h.scheduler.rng.int(30), name: "request_terminate") do
            h.store.crashable do
              h.store.request_workflow_termination(workflow_id: workflow_ids.fetch("generated-terminating"), reason: "generated terminate")
            end
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "shape-terminator", "generated_terminate_crashed")
          end

          deliver_commands = lambda do |worker_id|
            4.times do
              activation = h.store.claim_target_activation(
                worker_id:,
                lease_seconds: 12,
                target_kinds: ["workflow"],
                target_types: ["generated-command-wait"],
              )
              break if activation.nil?

              workflow_id = activation.fetch("target_id")
              claimed = h.store.claim_workflow_for_activation(workflow_id:, worker_id:, lease_seconds: 12)
              if claimed
                h.store.crashable do
                  Durababble::Engine.new(store: h.store, worker_id:, lease_seconds: 12)
                    .resume(command_workflow, workflow_id:, claimed:)
                end
              end
              h.store.complete_target_activation(
                target_kind: "workflow",
                target_type: "generated-command-wait",
                target_id: workflow_id,
                worker_id:,
              )
            rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled, Durababble::LeaseConflict => e
              h.scheduler.trace.event(h.scheduler.time, worker_id, "generated_command_delivery_yield", error: e.class.name)
            end
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, worker_id, "generated_command_delivery_crashed")
          end

          8.times do |i|
            h.scheduler.schedule(actor: "shape-command-worker-#{i}", delay: 20 + i * 18 + h.scheduler.rng.int(8), name: "deliver_commands") do
              deliver_commands.call("shape-command-worker-#{i}")
            end
            h.scheduler.schedule(actor: "shape-timer-#{i}", delay: 35 + i * 20, name: "wake_due_timers") do
              h.store.wake_due_timers(now: h.store.current_time + 90)
            end
            h.scheduler.schedule(actor: "shape-reaper-#{i}", delay: 30 + i * 20, name: "steal_expired") do
              h.store.steal_expired_leases!(now: h.scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
            end
          end

          terminal_statuses = ["completed", "failed", "canceled", "terminated"].freeze
          final_resume = lambda do |worker_id, workflow_name, workflow_id|
            return if terminal_statuses.include?(h.store.workflow(workflow_id).fetch("status"))

            Durababble::Engine.new(store: h.store, worker_id:, lease_seconds: 30)
              .resume(h.workflows.fetch(workflow_name), workflow_id:)
          rescue Durababble::WorkflowSuspended, Durababble::StepRetryScheduled, Durababble::LeaseConflict => e
            h.scheduler.trace.event(h.scheduler.time, worker_id, "generated_final_resume_yield", id: workflow_id, error: e.class.name)
          end

          h.scheduler.schedule(actor: "shape-final-signaler", delay: 225, name: "ensure_signal") do
            unless terminal_statuses.include?(h.store.workflow(workflow_ids.fetch("generated-command-wait")).fetch("status"))
              h.store.enqueue_workflow_command(
                workflow_id: workflow_ids.fetch("generated-command-wait"),
                workflow_name: "generated-command-wait",
                method_name: "signal",
                payload: { "method" => "signal", "args" => [], "kwargs" => {} },
                idempotency_key: "generated-final-signal",
              )
            end
          end
          h.scheduler.schedule(actor: "shape-final-canceler", delay: 226, name: "ensure_cancel") do
            h.store.request_workflow_cancellation(workflow_id: workflow_ids.fetch("generated-cancelable"), reason: "generated cancel")
          end
          h.scheduler.schedule(actor: "shape-final-terminator", delay: 227, name: "ensure_terminate") do
            h.store.request_workflow_termination(workflow_id: workflow_ids.fetch("generated-terminating"), reason: "generated terminate")
          end
          [230, 255, 280, 305].each_with_index do |delay, index|
            h.scheduler.schedule(actor: "shape-final-timer-#{index}", delay:, name: "final_wake_due_timers") do
              h.store.wake_due_timers(now: h.store.current_time + 1000)
            end
            h.scheduler.schedule(actor: "shape-final-command-worker-#{index}", delay: delay + 5, name: "final_deliver_commands") do
              deliver_commands.call("shape-final-command-worker-#{index}")
            end
            h.scheduler.schedule(actor: "shape-final-worker-#{index}", delay: delay + 10, name: "final_resumes") do
              h.store.steal_expired_leases!(now: h.scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
              workflow_ids.each do |workflow_name, workflow_id|
                final_resume.call("shape-final-worker-#{index}", workflow_name, workflow_id)
              end
            end
          end

          h.check("generated workflows all reached terminal states") do
            workflow_ids.values.all? { |workflow_id| terminal_statuses.include?(h.store.workflow(workflow_id).fetch("status")) }
          end
          h.check("generated workflow shapes exercised retries, commands, cancellation, and termination") do
            trace = h.scheduler.trace.to_s
            trace.include?("generated_retry_attempt") &&
              trace.include?("workflow_command_completed") &&
              trace.include?("workflow_cancel_requested") &&
              h.store.workflow_history_for(workflow_ids.fetch("generated-terminating")).any? do |event|
                event.fetch("kind") == "workflow_terminated"
              end
          end
          h.check("generated seed built a multi-step shape") do
            trace = h.scheduler.trace.to_s
            shape_plan.length >= 3 && trace.include?("generated_shape_retry_attempt")
          end
          h.check("generated command workflow delivered at most one signal") do
            h.store.workflow_history_for(workflow_ids.fetch("generated-command-wait")).count { |event| event.fetch("kind") == "workflow_command_completed" } <= 1
          end
          h.check("generated cancel cleanup ran or terminalized cleanly") do
            h.store.steps_for(workflow_ids.fetch("generated-cancelable")).none? { |step| step.fetch("status") == "waiting" }
          end
        end
      end
    end
  end
end
