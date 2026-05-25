#!/usr/bin/env ruby
# typed: false
# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "optparse"
require "rbconfig"
require "socket"
require "securerandom"
require "time"

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "durababble"

module Durababble
  module Benchmarks
    DEFAULT_DATABASE_URL = Durababble.default_database_url

    Operation = Struct.new(:name, :iterations, :warmup, :description, :block, :prepare, keyword_init: true)

    class Runner
      def initialize(profile:, database_url:, schema:, output_dir:, fixture_size:, seed:, samples:, keep_schema:, only: nil)
        @profile = profile
        @database_url = database_url
        @schema = schema
        @output_dir = output_dir
        @fixture_size = fixture_size
        @seed = seed
        @samples = samples
        @keep_schema = keep_schema
        @only = only
        @rng = Random.new(seed)
        @store = Durababble::Store.connect(database_url:, schema:)
        @store.send(:execute, "SET client_min_messages TO warning") unless @store.is_a?(Durababble::MysqlStore)
        @store.drop_schema!
        @store.migrate!
      end

      def run
        FileUtils.mkdir_p(@output_dir)
        start = Time.now.utc
        results = []
        puts "durababble benchmarks profile=#{@profile} schema=#{@schema} fixture_size=#{@fixture_size} samples=#{@samples}"

        operations.each do |operation|
          puts "\n== #{operation.name}: #{operation.description}"
          result = measure(operation)
          cleanup_after(operation)
          results << result
          puts format("   median=%0.3fms p95=%0.3fms ops/s=%0.1f iterations=%d", result[:median_ms], result[:p95_ms], result[:ops_per_second], result[:iterations])
        end

        finish = Time.now.utc
        report = {
          suite: "durababble",
          profile: @profile,
          started_at: start.iso8601,
          finished_at: finish.iso8601,
          duration_seconds: finish - start,
          schema: @schema,
          fixture_size: @fixture_size,
          seed: @seed,
          samples: @samples,
          environment: environment,
          operations: results,
        }
        stamp = start.strftime("%Y%m%dT%H%M%SZ")
        json_path = File.join(@output_dir, "durababble-bench-#{@profile}-#{stamp}.json")
        md_path = File.join(@output_dir, "durababble-bench-#{@profile}-#{stamp}.md")
        csv_path = File.join(@output_dir, "durababble-bench-#{@profile}-#{stamp}.csv")
        File.write(json_path, JSON.pretty_generate(report))
        File.write(md_path, markdown(report))
        File.write(csv_path, csv(report))
        puts "\nwrote #{json_path}"
        puts "wrote #{md_path}"
        puts "wrote #{csv_path}"
        report
      ensure
        @rpc&.close
        @bulk_due_timer_stores&.each(&:close)
        @store&.drop_schema! unless @keep_schema
        @store&.close
      end

      private

      def operations
        quick = @profile == "smoke"
        selected = [
          Operation.new(name: "enqueue_workflows", iterations: quick ? 100 : 2_000, warmup: quick ? 10 : 100, description: "insert pending workflows with Paquito input", block: method(:bench_enqueue)),
          Operation.new(name: "inline_run_workflow", iterations: quick ? 50 : 1_000, warmup: quick ? 5 : 50, description: "create, lease, and complete an inline workflow run", block: method(:bench_inline_run_workflow)),
          Operation.new(name: "claim_runnable_workflows", iterations: quick ? 100 : 2_000, warmup: quick ? 10 : 100, description: "claim pending workflows under distributed leases", block: method(:bench_claim)),
          Operation.new(name: "lease_heartbeat", iterations: quick ? 100 : 1_500, warmup: quick ? 10 : 100, description: "renew active workflow leases", block: method(:bench_heartbeat)),
          Operation.new(name: "lease_conflict_check", iterations: quick ? 100 : 1_500, warmup: quick ? 10 : 100, description: "check/respect another worker's live lease", block: method(:bench_lease_conflict)),
          Operation.new(name: "fenced_workflow_completion", iterations: quick ? 100 : 1_500, warmup: quick ? 10 : 100, description: "complete running workflows with a SQL lease-fenced status update", block: method(:bench_fenced_workflow_completion)),
          Operation.new(name: "fenced_workflow_failure", iterations: quick ? 100 : 1_500, warmup: quick ? 10 : 100, description: "fail running workflows with a SQL lease-fenced status update", block: method(:bench_fenced_workflow_failure)),
          Operation.new(name: "fenced_workflow_cancellation", iterations: quick ? 100 : 1_500, warmup: quick ? 10 : 100, description: "cancel running workflows with a SQL lease-fenced status update", block: method(:bench_fenced_workflow_cancellation)),
          Operation.new(name: "timer_wait_resume_workflow", iterations: quick ? 25 : 500, warmup: quick ? 3 : 25, description: "run into due timer wait, wake it, and resume remaining workflow steps", block: method(:bench_timer_wait_resume_workflow)),
          Operation.new(name: "worker_tick_execute_workflow", iterations: quick ? 50 : 1_000, warmup: quick ? 5 : 50, description: "worker tick claim + execute a runnable workflow", block: method(:bench_worker_tick_execute_workflow)),
          Operation.new(name: "worker_run_until_idle_batch", iterations: quick ? 10 : 100, warmup: quick ? 1 : 10, description: "worker drains a small batch through run_until_idle", block: method(:bench_worker_run_until_idle_batch)),
          Operation.new(name: "resume_skips_completed_step", iterations: quick ? 50 : 750, warmup: quick ? 5 : 50, description: "resume reconstructs context and skips already-completed steps", block: method(:bench_resume_skips_completed_step)),
          Operation.new(name: "read_workflow_state", iterations: quick ? 100 : 1_500, warmup: quick ? 10 : 100, description: "inspect workflow, steps, attempts, and waits for observability", block: method(:bench_read_workflow_state)),
          Operation.new(name: "failed_workflow_retry", iterations: quick ? 25 : 500, warmup: quick ? 3 : 25, description: "retry a failed workflow through the runnable queue", block: method(:bench_failed_workflow_retry)),
          Operation.new(name: "expired_workflow_lease_recovery", iterations: quick ? 50 : 1_000, warmup: quick ? 5 : 50, description: "detect and return expired workflow leases to pending", block: method(:bench_expired_workflow_lease_recovery)),
          Operation.new(name: "fence_first_execution", iterations: quick ? 50 : 1_000, warmup: quick ? 5 : 50, description: "acquire an idempotency fence, run side effect, and persist result", block: method(:bench_fence_first_execution)),
          Operation.new(name: "fence_cached_result", iterations: quick ? 50 : 1_000, warmup: quick ? 5 : 50, description: "read cached idempotency fence result without re-running side effect", block: method(:bench_fence_cached_result)),
          Operation.new(name: "outbox_claim_ack", iterations: quick ? 100 : 1_500, warmup: quick ? 10 : 100, description: "claim and acknowledge outbox messages", block: method(:bench_outbox_claim_ack)),
          Operation.new(name: "outbox_expired_reclaim", iterations: quick ? 50 : 1_000, warmup: quick ? 5 : 50, description: "reclaim an outbox message whose processing lease expired", block: method(:bench_outbox_expired_reclaim)),
          Operation.new(name: "durable_object_command_claim", iterations: quick ? 50 : 1_000, warmup: quick ? 5 : 50, description: "persist, claim, complete, and read durable object command state", block: method(:bench_durable_object_command_claim)),
          Operation.new(name: "large_table_claim_scan", iterations: quick ? 25 : 250, warmup: quick ? 3 : 25, description: "claim runnable rows with large completed/running table fixture", block: method(:bench_large_table_claim_scan)),
          Operation.new(name: "large_table_due_timer_scan", iterations: quick ? 25 : 250, warmup: quick ? 3 : 25, description: "wake due timers with many unrelated wait rows", block: method(:bench_large_table_due_timer_scan)),
          Operation.new(name: "bulk_due_timer_wake_parallel", iterations: quick ? 3 : 20, warmup: quick ? 1 : 3, description: "wake a large due timer set from two concurrent stores", prepare: method(:prepare_bulk_due_timer_wake), block: method(:bench_bulk_due_timer_wake_parallel)),
          Operation.new(name: "step_attempt_number_lookup", iterations: quick ? 50 : 750, warmup: quick ? 5 : 50, description: "compute a step attempt number with many prior attempts", block: method(:bench_step_attempt_number_lookup)),
          Operation.new(name: "command_rpc_ping", iterations: quick ? 50 : 500, warmup: quick ? 5 : 50, description: "JSON-line command RPC roundtrip to a separate Ruby process", block: method(:bench_rpc_ping)),
          Operation.new(name: "command_rpc_enqueue_claim", iterations: quick ? 25 : 250, warmup: quick ? 3 : 25, description: "separate process enqueue + lease claim command RPC", block: method(:bench_rpc_enqueue_claim)),
          Operation.new(name: "command_rpc_enqueue_claim_batch", iterations: quick ? 10 : 100, warmup: quick ? 1 : 10, description: "separate process batched enqueue + lease claim command RPC", block: method(:bench_rpc_enqueue_claim_batch)),
        ]
        @only ? selected.select { |operation| operation.name.match?(@only) } : selected
      end

      def measure(operation)
        operation.warmup.times do |i|
          operation.prepare&.call(i, warmup: true)
          operation.block.call(i, warmup: true)
        end
        GC.start(full_mark: true, immediate_sweep: true)
        samples = []
        operation.iterations.times do |i|
          operation.prepare&.call(i, warmup: false)
          before_alloc = GC.stat.fetch(:total_allocated_objects)
          started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          operation.block.call(i, warmup: false)
          elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
          after_alloc = GC.stat.fetch(:total_allocated_objects)
          samples << { seconds: elapsed, allocations: after_alloc - before_alloc }
        end
        summarize(operation, samples)
      end

      def cleanup_after(operation)
        if @store.is_a?(Durababble::MysqlStore)
          @store.send(:execute, <<~SQL)
            UPDATE #{@store.send(:table, "workflows")}
            SET status = 'completed', locked_by = NULL, locked_until = NULL, updated_at = NOW(6)
            WHERE name <> 'fixture' AND status <> 'completed'
          SQL
          @store.send(:execute, <<~SQL)
            UPDATE #{@store.send(:table, "outbox")}
            SET status = 'processed', locked_by = NULL, locked_until = NULL, processed_at = COALESCE(processed_at, NOW(6))
            WHERE status <> 'processed'
          SQL
          return
        end

        connection = PG.connect(@database_url)
        schema = PG::Connection.quote_ident(@schema)
        connection.exec(<<~SQL)
          UPDATE #{schema}.workflows
          SET status = 'completed', locked_by = NULL, locked_until = NULL, updated_at = now()
          WHERE name <> 'fixture' AND status <> 'completed'
        SQL
        connection.exec(<<~SQL)
          UPDATE #{schema}.outbox
          SET status = 'processed', locked_by = NULL, locked_until = NULL, processed_at = COALESCE(processed_at, now())
          WHERE status <> 'processed'
        SQL
      ensure
        connection&.close
      end

      def summarize(operation, samples)
        seconds = samples.map { |sample| sample.fetch(:seconds) }.sort
        allocations = samples.map { |sample| sample.fetch(:allocations) }.sort
        total = seconds.sum
        {
          name: operation.name,
          description: operation.description,
          iterations: operation.iterations,
          warmup_iterations: operation.warmup,
          total_seconds: total,
          ops_per_second: operation.iterations / total,
          min_ms: seconds.first * 1000.0,
          median_ms: percentile(seconds, 0.50) * 1000.0,
          p90_ms: percentile(seconds, 0.90) * 1000.0,
          p95_ms: percentile(seconds, 0.95) * 1000.0,
          p99_ms: percentile(seconds, 0.99) * 1000.0,
          max_ms: seconds.last * 1000.0,
          avg_allocations: allocations.sum.to_f / allocations.length,
          p95_allocations: percentile(allocations, 0.95),
          sample_count: samples.length,
        }
      end

      def percentile(values, quantile)
        return values.first if values.length == 1

        rank = quantile * (values.length - 1)
        lower = values[rank.floor]
        upper = values[rank.ceil]
        lower + (upper - lower) * (rank - rank.floor)
      end

      def bench_enqueue(i, warmup:)
        @store.enqueue_workflow(name: "bench_enqueue", input: { "i" => i, "warmup" => warmup, "payload" => "x" * 64 })
      end

      def bench_claim(i, warmup:)
        @store.enqueue_workflow(name: "bench_claim", input: { "i" => i, "warmup" => warmup })
        claimed = @store.claim_runnable_workflow(worker_id: "claim-worker", lease_seconds: 30)
        raise "workflow not claimed" unless claimed
      end

      def bench_inline_run_workflow(i, warmup:)
        run = Durababble::Engine.new(store: @store, worker_id: "inline-runner", lease_seconds: 30, migrate: false).run(noop_workflow, input: { "i" => i, "warmup" => warmup, "payload" => "x" * 64 })
        raise "inline workflow did not complete" unless run.status == "completed"
      end

      def bench_heartbeat(i, warmup:)
        id = @store.enqueue_workflow(name: "bench_heartbeat", input: { "i" => i })
        @store.claim_workflow(workflow_id: id, worker_id: "heartbeater", lease_seconds: 30)
        @store.heartbeat(workflow_id: id, worker_id: "heartbeater", lease_seconds: 30)
      end

      def bench_lease_conflict(i, warmup:)
        id = @store.enqueue_workflow(name: "bench_conflict", input: { "i" => i })
        @store.claim_workflow(workflow_id: id, worker_id: "owner", lease_seconds: 30)
        claimed = @store.claim_workflow(workflow_id: id, worker_id: "intruder", lease_seconds: 30)
        raise "lease conflict unexpectedly claimed" if claimed
      end

      def bench_fenced_workflow_completion(i, warmup:)
        id = @store.enqueue_workflow(name: "bench_fenced_complete", input: { "i" => i, "warmup" => warmup })
        @store.claim_workflow(workflow_id: id, worker_id: "status-owner", lease_seconds: 30)
        @store.complete_workflow(id, result: { "done" => true, "i" => i }, worker_id: "status-owner")
      end

      def bench_fenced_workflow_failure(i, warmup:)
        id = @store.enqueue_workflow(name: "bench_fenced_failure", input: { "i" => i, "warmup" => warmup })
        @store.claim_workflow(workflow_id: id, worker_id: "status-owner", lease_seconds: 30)
        @store.fail_workflow(id, error: "synthetic failure", worker_id: "status-owner")
      end

      def bench_fenced_workflow_cancellation(i, warmup:)
        id = @store.enqueue_workflow(name: "bench_fenced_cancel", input: { "i" => i, "warmup" => warmup })
        @store.claim_workflow(workflow_id: id, worker_id: "status-owner", lease_seconds: 30)
        @store.cancel_workflow(id, reason: "synthetic cancellation", result: { "canceled" => true }, worker_id: "status-owner")
      end

      def bench_timer_wait_resume_workflow(i, warmup:)
        workflow = timer_resume_workflow
        run = Durababble::Engine.new(store: @store, worker_id: "timer-runner", lease_seconds: 30, migrate: false).run(workflow, input: { "i" => i })
        raise "workflow did not wait" unless run.status == "waiting"

        @store.wake_due_timers(now: Time.now)
        resumed = Durababble::Engine.new(store: @store, worker_id: "timer-worker", lease_seconds: 30, migrate: false).resume(workflow, workflow_id: run.id)
        raise "engine did not resume timer workflow" unless resumed.status == "completed"
      end

      def bench_worker_tick_execute_workflow(i, warmup:)
        workflow = arithmetic_workflow("bench_worker_tick")
        @store.enqueue_workflow(name: workflow.name, input: { "i" => i, "value" => 1 })
        worked = worker_for("tick-worker", workflow).tick
        raise "worker did not execute workflow" unless worked == :worked
      end

      def bench_worker_run_until_idle_batch(i, warmup:)
        workflow = arithmetic_workflow("bench_worker_drain")
        10.times { |n| @store.enqueue_workflow(name: workflow.name, input: { "i" => i, "n" => n, "value" => 1 }) }
        worked = worker_for("drain-worker", workflow).run_until_idle(max_ticks: 20)
        raise "worker did not drain batch" unless worked == 10
      end

      def bench_resume_skips_completed_step(i, warmup:)
        workflow = arithmetic_workflow("bench_resume_skip")
        workflow_id = @store.enqueue_workflow(name: workflow.name, input: { "i" => i, "value" => 1 })
        @store.mark_workflow_running(workflow_id, worker_id: "resume-prep", lease_seconds: 30)
        @store.record_step_started(workflow_id:, position: 0, name: "add_one")
        @store.record_step_completed(workflow_id:, position: 0, result: { "i" => i, "value" => 2 })
        run = Durababble::Engine.new(store: @store, worker_id: "resume-prep", lease_seconds: 30, migrate: false).resume(workflow, workflow_id:)
        raise "resume did not complete" unless run.status == "completed" && run.result.fetch("value") == 4
      end

      def bench_read_workflow_state(i, warmup:)
        workflow_id = state_read_ids[i % state_read_ids.length]
        @store.workflow(workflow_id)
        @store.steps_for(workflow_id)
        @store.step_attempts_for(workflow_id)
        @store.waits_for(workflow_id)
      end

      def bench_failed_workflow_retry(i, warmup:)
        workflow_id = @store.enqueue_workflow(name: "bench_failed_retry", input: { "i" => i })
        @store.mark_workflow_running(workflow_id, worker_id: "failure-owner", lease_seconds: 30)
        @store.record_step_started(workflow_id:, position: 0, name: "flaky")
        @store.record_step_failed(workflow_id:, position: 0, error: "synthetic failure")
        @store.schedule_workflow_retry(workflow_id:, worker_id: "failure-owner", run_at: Time.now - 1)
        claimed = @store.claim_runnable_workflow(worker_id: "retry-worker", lease_seconds: 30)
        raise "failed workflow was not retry-claimable" unless claimed
      end

      def bench_expired_workflow_lease_recovery(i, warmup:)
        workflow_id = @store.enqueue_workflow(name: "bench_expired", input: { "i" => i })
        @store.claim_workflow(workflow_id:, worker_id: "expired-owner", lease_seconds: 30)
        expire_workflow_lease(workflow_id)
        recovered = @store.steal_expired_leases!(now: Time.now)
        raise "expired workflow lease was not recovered" if recovered < 1
      end

      def bench_fence_first_execution(i, warmup:)
        workflow_id = @store.enqueue_workflow(name: "bench_fence", input: { "i" => i })
        result = @store.with_fence(workflow_id:, key: "first:#{warmup}:#{i}:#{SecureRandom.hex(4)}", poll_interval: 0.001, timeout: 2) do
          { "side_effect" => i, "warmup" => warmup }
        end
        raise "fence result mismatch" unless result.fetch("side_effect") == i
      end

      def bench_fence_cached_result(i, warmup:)
        workflow_id = @store.enqueue_workflow(name: "bench_fence_cached", input: { "i" => i })
        key = "cached:#{warmup}:#{i}:#{SecureRandom.hex(4)}"
        expected = @store.with_fence(workflow_id:, key:, poll_interval: 0.001, timeout: 2) { { "cached" => i } }
        result = @store.with_fence(workflow_id:, key:, poll_interval: 0.001, timeout: 2) { raise "side effect reran" }
        raise "cached fence mismatch" unless result == expected
      end

      def bench_outbox_claim_ack(i, warmup:)
        workflow_id = @store.enqueue_workflow(name: "bench_outbox", input: { "i" => i })
        outbox_id = @store.enqueue_outbox(workflow_id:, topic: "bench.topic", payload: { "i" => i, "warmup" => warmup }, key: "bench:#{warmup}:#{i}:#{SecureRandom.hex(4)}")
        message = @store.claim_outbox(worker_id: "outbox-worker", lease_seconds: 30)
        raise "outbox not claimed" unless message && message.fetch("id") == outbox_id

        @store.ack_outbox(outbox_id, worker_id: "outbox-worker")
      end

      def bench_outbox_expired_reclaim(i, warmup:)
        workflow_id = @store.enqueue_workflow(name: "bench_outbox_expired", input: { "i" => i })
        outbox_id = @store.enqueue_outbox(workflow_id:, topic: "bench.topic", payload: { "i" => i, "warmup" => warmup }, key: "bench-expired:#{warmup}:#{i}:#{SecureRandom.hex(4)}")
        first = @store.claim_outbox(worker_id: "outbox-owner", lease_seconds: 30)
        raise "outbox not initially claimed" unless first && first.fetch("id") == outbox_id

        expire_outbox_lease(outbox_id)
        second = @store.claim_outbox(worker_id: "outbox-reclaimer", lease_seconds: 30)
        raise "expired outbox was not reclaimed" unless second && second.fetch("id") == outbox_id
      end

      def bench_durable_object_command_claim(i, warmup:)
        object_id = "bench-object-#{warmup}-#{i}-#{SecureRandom.hex(4)}"
        @store.save_object_state(object_type: "bench-counter", object_id:, state: { "count" => i })
        command_id = @store.enqueue_object_command(
          object_type: "bench-counter",
          object_id:,
          method_name: "increment",
          args: [1],
          kwargs: {},
        )
        command = @store.claim_object_command(command_id:, worker_id: "object-worker", lease_seconds: 30)
        raise "durable object command was not claimed" unless command && command.fetch("id") == command_id

        @store.complete_object_command(
          command_id:,
          object_type: "bench-counter",
          object_id:,
          state: { "count" => i + 1 },
          result: { "count" => i + 1 },
          worker_id: "object-worker",
        )
        state = @store.object_state(object_type: "bench-counter", object_id:)
        raise "durable object state did not persist" unless state.fetch("count") == i + 1
      end

      def bench_large_table_claim_scan(i, warmup:)
        ensure_large_fixture!
        @store.enqueue_workflow(name: "large_claim", input: { "i" => i, "warmup" => warmup })
        claimed = @store.claim_runnable_workflow(worker_id: "large-claim-worker", lease_seconds: 30)
        raise "large-table claim missed pending row" unless claimed
      end

      def bench_large_table_due_timer_scan(i, warmup:)
        ensure_large_fixture!
        workflow_id = @store.enqueue_workflow(name: "due_timer", input: { "i" => i })
        @store.mark_workflow_running(workflow_id, worker_id: "timer-worker", lease_seconds: 30)
        @store.record_step_started(workflow_id:, position: 0, name: "timer")
        @store.record_wait(
          workflow_id:,
          position: 0,
          name: "timer",
          wait_request: Durababble.wait_until(Time.now - 1, context: { "i" => i }),
        )
        woke = @store.wake_due_timers(now: Time.now)
        raise "due timer not woken" if woke < 1

        @store.complete_workflow(workflow_id, result: { "woke" => true })
      end

      def prepare_bulk_due_timer_wake(i, warmup:)
        @bulk_due_timer_expected = bulk_due_timer_count
        @bulk_due_timer_now = Time.now
        prefix = "bulk-timer-#{warmup ? "warmup" : "sample"}-#{i}"
        input = @store.send(:dump_serialized, {})
        context = @store.send(:dump_serialized, { "bulk" => true })
        rows = @bulk_due_timer_expected.times.map do |n|
          workflow_id = "#{prefix}-workflow-#{n}"
          {
            workflow: [workflow_id, "bulk_due_timer", "waiting", input],
            step: [workflow_id, 0, "timer", "waiting"],
            attempt: ["#{prefix}-attempt-#{n}", workflow_id, 0, "timer", "waiting"],
            wait: ["#{prefix}-wait-#{n}", workflow_id, 0, "timer", @bulk_due_timer_now - 1, context, "pending"],
          }
        end

        bulk_insert_rows("workflows", ["id", "name", "status", "input"], rows.map { |row| row.fetch(:workflow) }, casts: { "input" => "::bytea" })
        bulk_insert_rows("steps", ["workflow_id", "position", "name", "status"], rows.map { |row| row.fetch(:step) })
        bulk_insert_rows("step_attempts", ["id", "workflow_id", "position", "name", "status"], rows.map { |row| row.fetch(:attempt) })
        bulk_insert_rows("waits", ["id", "workflow_id", "position", "kind", "wake_at", "context", "status"], rows.map { |row| row.fetch(:wait) }, casts: { "wake_at" => "::timestamptz", "context" => "::bytea" })
      end

      def bench_bulk_due_timer_wake_parallel(i, warmup:)
        counts = bulk_due_timer_stores.map do |store|
          Thread.new { store.wake_due_timers(now: @bulk_due_timer_now) }
        end.map(&:value)
        woke = counts.sum
        raise "bulk due timer wake missed rows: woke #{woke}, expected #{@bulk_due_timer_expected}, counts=#{counts.inspect}" unless woke == @bulk_due_timer_expected
      end

      def bench_step_attempt_number_lookup(i, warmup:)
        ensure_step_attempt_fixture!
        attempt_number = @step_attempt_fixture_runner.send(:attempt_number_for, 0)
        raise "attempt number mismatch: #{attempt_number}" unless attempt_number == step_attempt_fixture_count
      end

      def bench_rpc_ping(i, warmup:)
        rpc.request("ping", "i" => i, "warmup" => warmup)
      end

      def bench_rpc_enqueue_claim(i, warmup:)
        response = rpc.request("enqueue_claim", "i" => i, "warmup" => warmup)
        raise "rpc enqueue_claim failed" unless response.fetch("claimed")
      end

      def bench_rpc_enqueue_claim_batch(i, warmup:)
        response = rpc.request("enqueue_claim_batch", "start" => i * 10, "count" => 10, "warmup" => warmup)
        raise "rpc enqueue_claim_batch failed" unless response.fetch("claimed") == 10
      end

      def worker_for(worker_id, workflow)
        @workers ||= {}
        @workers[[worker_id, workflow.name]] ||= Durababble::Worker.new(
          store: @store,
          workflows: { workflow.name => workflow },
          worker_id:,
          lease_seconds: 30,
          migrate: false,
        )
      end

      def arithmetic_workflow(name)
        @arithmetic_workflows ||= {}
        @arithmetic_workflows[name] ||= Class.new(Durababble::Workflow) do
          workflow_name name

          def execute(input)
            double(add_one(input))
          end

          step def add_one(ctx)
            ctx.merge("value" => ctx.fetch("value") + 1)
          end

          step def double(ctx)
            ctx.merge("value" => ctx.fetch("value") * 2)
          end
        end
      end

      def noop_workflow
        @noop_workflow ||= Class.new(Durababble::Workflow) do
          workflow_name "bench_inline_run"

          def execute(input)
            input
          end
        end
      end

      def event_resume_workflow(event_key)
        Class.new(Durababble::Workflow) do
          workflow_name "bench_event_resume"

          define_method(:execute) do |input|
            finish_after_event(wait_for_event(input))
          end

          define_method(:wait_for_event) do |ctx|
            Durababble.wait_event(event_key, context: ctx)
          end
          step :wait_for_event

          step def finish_after_event(ctx)
            ctx.merge("finished" => true)
          end
        end
      end

      def timer_resume_workflow
        @timer_resume_workflow ||= Class.new(Durababble::Workflow) do
          workflow_name "bench_timer_resume"

          def execute(input)
            finish_after_timer(wait_for_timer(input))
          end

          step def wait_for_timer(ctx)
            Durababble.wait_until(Time.now - 1, context: ctx)
          end

          step def finish_after_timer(ctx)
            ctx.merge("finished" => true)
          end
        end
      end

      def state_read_ids
        return @state_read_ids if @state_read_ids

        workflow = arithmetic_workflow("bench_state_read")
        @state_read_ids = 25.times.map do |i|
          id = @store.enqueue_workflow(name: workflow.name, input: { "i" => i, "value" => 1 })
          Durababble::Engine.new(store: @store, worker_id: "state-reader-prep", lease_seconds: 30, migrate: false).resume(workflow, workflow_id: id)
          id
        end
      end

      def ensure_step_attempt_fixture!
        return if @step_attempt_fixture_runner

        workflow_id = "step-attempt-fixture"
        bulk_insert_rows(
          "workflows",
          ["id", "name", "status", "input"],
          [[workflow_id, "step_attempt_fixture", "running", @store.send(:dump_serialized, {})]],
          casts: { "input" => "::bytea" },
        )
        rows = step_attempt_fixture_count.times.map do |n|
          ["step-attempt-fixture-#{n}", workflow_id, 0, "flaky", n == step_attempt_fixture_count - 1 ? "running" : "failed"]
        end
        bulk_insert_rows("step_attempts", ["id", "workflow_id", "position", "name", "status"], rows)

        @step_attempt_fixture_runner = Durababble::WorkflowStepRunner.new(
          store: @store,
          workflow_id:,
          worker_id: "attempt-bench",
          lease_seconds: 30,
          root_task: Object.new,
          futures: {},
          step_contexts: {},
          synchronize_store: ->(&block) { block.call },
          raise_if_cancel_requested: -> {},
          assert_workflow_lease: -> {},
          suspend_workflow_immediately: -> { true },
          retry_run_at: ->(delay) { Time.now + delay },
          crash: ->(_point) {},
        )
      end

      def bulk_due_timer_count
        @fixture_size.clamp(100, 5_000)
      end

      def step_attempt_fixture_count
        @fixture_size.clamp(100, 10_000)
      end

      def expire_workflow_lease(workflow_id)
        if @store.is_a?(Durababble::MysqlStore)
          @store.send(:execute_params, "UPDATE #{@store.send(:table, "workflows")} SET locked_until = DATE_SUB(NOW(6), INTERVAL 1 SECOND) WHERE id = ?", [workflow_id])
        else
          @store.send(:execute_params, "UPDATE #{quoted_schema}.workflows SET locked_until = now() - interval '1 second' WHERE id = $1", [workflow_id])
        end
      end

      def expire_outbox_lease(outbox_id)
        if @store.is_a?(Durababble::MysqlStore)
          @store.send(:execute_params, "UPDATE #{@store.send(:table, "outbox")} SET locked_until = DATE_SUB(NOW(6), INTERVAL 1 SECOND) WHERE id = ?", [outbox_id])
        else
          @store.send(:execute_params, "UPDATE #{quoted_schema}.outbox SET locked_until = now() - interval '1 second' WHERE id = $1", [outbox_id])
        end
      end

      def quoted_schema
        PG::Connection.quote_ident(@schema)
      end

      def bulk_due_timer_stores
        @bulk_due_timer_stores ||= 2.times.map { connect_store }
      end

      def connect_store
        store = Durababble::Store.connect(database_url: @database_url, schema: @schema)
        store.send(:execute, "SET client_min_messages TO warning") unless store.is_a?(Durababble::MysqlStore)
        store
      end

      def bulk_insert_rows(table_name, columns, rows, casts: {})
        rows.each_slice(100) do |slice|
          params = []
          values = slice.map do |row|
            placeholders = row.each_with_index.map do |value, index|
              params << value
              placeholder = @store.is_a?(Durababble::MysqlStore) ? "?" : "$#{params.length}"
              "#{placeholder}#{casts.fetch(columns.fetch(index), "") unless @store.is_a?(Durababble::MysqlStore)}"
            end
            "(#{placeholders.join(", ")})"
          end
          @store.send(:execute_params, "INSERT INTO #{@store.send(:table, table_name)} (#{columns.join(", ")}) VALUES #{values.join(", ")}", params)
        end
      end

      def ensure_large_fixture!
        return if @large_fixture_loaded

        puts "   loading large fixture rows=#{@fixture_size}"
        @store.close
        load_large_fixture
        @store = Durababble::Store.connect(database_url: @database_url, schema: @schema)
        @store.send(:execute, "SET client_min_messages TO warning") unless @store.is_a?(Durababble::MysqlStore)
        @large_fixture_loaded = true
      end

      def load_large_fixture
        ruby = RbConfig.ruby
        script = File.expand_path("load_fixture.rb", __dir__)
        env = {
          "DURABABBLE_DATABASE_URL" => @database_url,
          "DURABABBLE_BENCH_SCHEMA" => @schema,
          "DURABABBLE_BENCH_FIXTURE_SIZE" => @fixture_size.to_s,
          "DURABABBLE_BENCH_SEED" => @seed.to_s,
        }
        system(env, ruby, script, exception: true)
      end

      def rpc
        @rpc ||= Durababble::RpcClient.spawn(
          command: [RbConfig.ruby, File.expand_path("rpc_worker.rb", __dir__)],
          env: { "DURABABBLE_DATABASE_URL" => @database_url, "DURABABBLE_BENCH_SCHEMA" => @schema },
          timeout: 30,
        )
      end

      def environment
        {
          ruby_description: RUBY_DESCRIPTION,
          ruby_engine: RUBY_ENGINE,
          ruby_version: RUBY_VERSION,
          ruby_platform: RUBY_PLATFORM,
          yjit_enabled: defined?(RubyVM::YJIT) ? RubyVM::YJIT.enabled? : false,
          hostname: Socket.gethostname,
          pid: Process.pid,
          gc: GC.stat.slice(:heap_live_slots, :heap_free_slots, :total_allocated_objects),
          git_sha: git("rev-parse", "HEAD"),
          git_branch: git("branch", "--show-current"),
        }
      end

      def git(*args)
        out, status = Open3.capture2("git", *args, chdir: File.expand_path("..", __dir__))
        status.success? ? out.strip : nil
      end

      def markdown(report)
        lines = []
        lines << "# Durababble benchmark #{report.fetch(:profile)} #{report.fetch(:started_at)}"
        lines << ""
        lines << "- Git: `#{report.fetch(:environment).fetch(:git_sha)}`"
        lines << "- Ruby: `#{report.fetch(:environment).fetch(:ruby_description)}`"
        lines << "- Fixture size: `#{report.fetch(:fixture_size)}`"
        lines << "- Schema: `#{report.fetch(:schema)}`"
        lines << ""
        lines << "| Operation | ops/s | median ms | p95 ms | p99 ms | avg allocs |"
        lines << "| --- | ---: | ---: | ---: | ---: | ---: |"
        report.fetch(:operations).each do |op|
          lines << format("| `%s` | %.1f | %.3f | %.3f | %.3f | %.1f |", op.fetch(:name), op.fetch(:ops_per_second), op.fetch(:median_ms), op.fetch(:p95_ms), op.fetch(:p99_ms), op.fetch(:avg_allocations))
        end
        lines << ""
        lines.join("\n")
      end

      def csv(report)
        rows = [
          ["profile", "started_at", "git_sha", "operation", "iterations", "ops_per_second", "median_ms", "p95_ms", "p99_ms", "avg_allocations", "fixture_size"],
        ]
        report.fetch(:operations).each do |op|
          rows << [report.fetch(:profile), report.fetch(:started_at), report.fetch(:environment).fetch(:git_sha), op.fetch(:name), op.fetch(:iterations), op.fetch(:ops_per_second), op.fetch(:median_ms), op.fetch(:p95_ms), op.fetch(:p99_ms), op.fetch(:avg_allocations), report.fetch(:fixture_size)]
        end
        rows.map { |row| row.map { |field| csv_field(field) }.join(",") }.join("\n")
      end

      def csv_field(value)
        field = value.to_s
        return field unless field.match?(/[",\r\n]/)

        "\"#{field.gsub("\"", "\"\"")}\""
      end
    end
  end
end

options = {
  profile: ENV.fetch("DURABABBLE_BENCH_PROFILE", "smoke"),
  database_url: Durababble::Benchmarks::DEFAULT_DATABASE_URL,
  schema: ENV.fetch("DURABABBLE_BENCH_SCHEMA") do
    Durababble.workspace_schema("#{Dir.pwd}/bench/#{Time.now.utc.strftime("%Y%m%d%H%M%S")}_#{Process.pid}", prefix: "#{Durababble.default_schema}_bench")
  end,
  output_dir: ENV.fetch("DURABABBLE_BENCH_OUTPUT", File.expand_path("results", __dir__)),
  fixture_size: Integer(ENV.fetch("DURABABBLE_BENCH_FIXTURE_SIZE", ENV.fetch("DURABABBLE_BENCH_PROFILE", "smoke") == "full" ? "100000" : "2000")),
  seed: Integer(ENV.fetch("DURABABBLE_BENCH_SEED", "12345")),
  samples: Integer(ENV.fetch("DURABABBLE_BENCH_SAMPLES", "1")),
  keep_schema: ENV["DURABABBLE_BENCH_KEEP_SCHEMA"] == "1",
  only: nil,
}

OptionParser.new do |parser|
  parser.banner = "Usage: ruby bench/run.rb [options]"
  parser.on("--profile PROFILE", "smoke or full") { |value| options[:profile] = value }
  parser.on("--database-url URL", "YSQL connection URL") { |value| options[:database_url] = value }
  parser.on("--schema SCHEMA", "YSQL schema to create/drop") { |value| options[:schema] = value }
  parser.on("--output DIR", "result output directory") { |value| options[:output_dir] = value }
  parser.on("--fixture-size N", Integer, "large-table fixture rows") { |value| options[:fixture_size] = value }
  parser.on("--seed N", Integer, "fixture seed") { |value| options[:seed] = value }
  parser.on("--keep-schema", "do not drop benchmark schema at exit") { options[:keep_schema] = true }
  parser.on("--only REGEX", "only run operations whose names match REGEX") { |value| options[:only] = Regexp.new(value) }
end.parse!

runner = Durababble::Benchmarks::Runner.new(**options)
runner.run
