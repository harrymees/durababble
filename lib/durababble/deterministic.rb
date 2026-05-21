# frozen_string_literal: true

require "digest"

module Durababble
  module Deterministic
    Result = Data.define(:scenario, :seed, :trace, :digest, :violations, :summary)

    def self.prove(scenario, seed:)
      Scenarios.fetch(scenario).call(seed)
    end

    def self.search(scenario, seeds:)
      seeds.filter_map do |seed|
        result = prove(scenario, seed:)
        [seed, result.violations] unless result.violations.empty?
      end
    end

    class Rng
      MASK = (1 << 64) - 1

      def initialize(seed)
        @state = seed & MASK
      end

      def next_u64
        @state = (@state * 6_364_136_223_846_793_005 + 1_442_695_040_888_963_407) & MASK
      end

      def int(max)
        raise ArgumentError, "max must be positive" unless max.positive?

        next_u64 % max
      end

      def chance(percent)
        int(100) < percent
      end
    end

    class Trace
      attr_reader :lines

      def initialize
        @lines = []
      end

      def event(time, actor, name, fields = {})
        stable = fields.sort_by { |key, _| key.to_s }.map { |key, value| "#{key}=#{stable_value(value)}" }.join(" ")
        @lines << format("t=%06d actor=%s event=%s%s", time, actor, name, stable.empty? ? "" : " #{stable}")
      end

      def to_s
        @lines.join("\n")
      end

      private

      def stable_value(value)
        case value
        when Hash
          "{" + value.sort_by { |key, _| key.to_s }.map { |key, val| "#{key}:#{stable_value(val)}" }.join(",") + "}"
        when Array
          "[" + value.map { |val| stable_value(val) }.join(",") + "]"
        else
          value.inspect
        end
      end
    end

    class Scheduler
      attr_reader :time, :rng, :trace

      def initialize(seed:, trace: Trace.new)
        @rng = Rng.new(seed)
        @trace = trace
        @time = 0
        @seq = 0
        @events = []
      end

      def schedule(actor:, delay:, name:, &block)
        @seq += 1
        event = [@time + delay, @seq, actor, name, block]
        @events << event
        @events.sort_by! { |time, seq, _actor, _name, _block| [time, seq] }
        trace.event(@time, actor, "schedule", at: @time + delay, name:)
      end

      def run(max_events: 10_000)
        count = 0
        until @events.empty?
          raise "deterministic scheduler exceeded #{max_events} events" if count >= max_events

          event_time, _seq, actor, name, block = @events.shift
          @time = event_time
          trace.event(@time, actor, "run", name:)
          block.call
          count += 1
        end
      end
    end

    class VirtualNetwork
      def initialize(scheduler:, min_latency: 1, max_latency: 9, drop_percent: 0)
        @scheduler = scheduler
        @min_latency = min_latency
        @max_latency = max_latency
        @drop_percent = drop_percent
        @partitions = {}
      end

      def partition(source, target)
        @partitions[[source, target]] = true
        @scheduler.trace.event(@scheduler.time, "network", "partition", source:, target:)
      end

      def heal(source, target)
        @partitions.delete([source, target])
        @scheduler.trace.event(@scheduler.time, "network", "heal", source:, target:)
      end

      def send(source:, target:, type:, payload: {}, &handler)
        if @partitions[[source, target]] || @scheduler.rng.chance(@drop_percent)
          @scheduler.trace.event(@scheduler.time, "network", "network.drop", source:, target:, type:)
          return
        end

        delay = @min_latency + @scheduler.rng.int(@max_latency - @min_latency + 1)
        @scheduler.trace.event(@scheduler.time, "network", "network.send", source:, target:, type:, delay:)
        @scheduler.schedule(actor: target, delay:, name: "deliver:#{type}") do
          @scheduler.trace.event(@scheduler.time, "network", "deliver", source:, target:, type:)
          handler.call(payload)
        end
      end
    end

    class VirtualYugabyte
      attr_reader :scheduler

      def initialize(scheduler:)
        @scheduler = scheduler
        @id_seq = 0
        @workflows = {}
        @steps = Hash.new { |hash, key| hash[key] = {} }
        @attempts = Hash.new { |hash, key| hash[key] = [] }
        @waits = {}
        @fences = {}
        @outbox = {}
        @outbox_by_key = {}
        @side_effects = 0
        trace("init")
      end

      def migrate! = self
      def close = nil
      def drop_schema! = nil
      def current_time = scheduler.time

      def enqueue_workflow(name:, input:)
        id = next_id("wf")
        @workflows[id] = { "id" => id, "name" => name, "status" => "pending", "input" => deep(input), "result" => nil, "error" => nil, "locked_by" => nil, "locked_until" => nil, "next_run_at" => nil }
        trace("enqueue_workflow", id:, name:)
        id
      end

      def create_workflow(name:, input:)
        id = enqueue_workflow(name:, input:)
        mark_workflow_running(id)
        id
      end

      def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil)
        workflow = @workflows.values.select { |row| runnable?(row) && (!workflow_names || workflow_names.include?(row.fetch("name"))) }.min_by { |row| row.fetch("id") }
        return nil unless workflow

        claim_row(workflow, worker_id, lease_seconds)
      end

      def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
        row = @workflows.fetch(workflow_id)
        return deep(row) if row.fetch("status") == "running" && row.fetch("locked_by") == worker_id && !expired?(row)
        return nil unless row.fetch("status") == "pending" || row.fetch("status") == "failed" || (row.fetch("status") == "running" && (row.fetch("locked_by") == worker_id || expired?(row)))

        claim_row(row, worker_id, lease_seconds)
      end

      def heartbeat(workflow_id:, worker_id:, lease_seconds:)
        row = @workflows.fetch(workflow_id)
        if row.fetch("locked_by") == worker_id && row.fetch("status") == "running"
          row["locked_until"] = scheduler.time + lease_seconds
          trace("heartbeat", id: workflow_id, worker: worker_id)
        end
      end

      def heartbeat_step(workflow_id:, position:, worker_id:, lease_seconds:, cursor:)
        row = @workflows.fetch(workflow_id)
        return nil unless row.fetch("locked_by") == worker_id && row.fetch("status") == "running" && !expired?(row)

        row["locked_until"] = scheduler.time + lease_seconds
        step = @steps[workflow_id][position]
        return nil unless step&.fetch("status") == "running"

        step["heartbeat_cursor"] = deep(cursor)
        latest_attempt = @attempts[workflow_id].reverse.find { |attempt| attempt.fetch("position") == position && attempt.fetch("status") == "running" }
        latest_attempt["heartbeat_cursor"] = deep(cursor) if latest_attempt
        trace("step_heartbeat", id: workflow_id, position:, worker: worker_id, cursor:)
        row.fetch("locked_until")
      end

      def step_heartbeat_cursor(workflow_id:, position:)
        deep(@steps[workflow_id][position]&.fetch("heartbeat_cursor", nil))
      end

      def current_workflow_lease(workflow_id)
        row = @workflows.fetch(workflow_id)
        return nil unless row.fetch("status") == "running" && row.fetch("locked_by") && !expired?(row)

        { "workflow_id" => workflow_id, "worker_id" => row.fetch("locked_by"), "locked_until" => row.fetch("locked_until") }
      end

      def steal_expired_leases!(now: nil)
        now ||= scheduler.time
        count = 0
        @workflows.each_value do |row|
          next unless row.fetch("status") == "running" && row.fetch("locked_until") && row.fetch("locked_until") < now

          row["status"] = "pending"
          row["locked_by"] = nil
          row["locked_until"] = nil
          count += 1
          trace("steal_expired", id: row.fetch("id"))
        end
        count
      end

      def mark_workflow_running(workflow_id, worker_id: nil, lease_seconds: 60)
        row = @workflows.fetch(workflow_id)
        row["status"] = "running"
        row["error"] = nil
        if worker_id
          row["locked_by"] = worker_id
          row["locked_until"] = scheduler.time + lease_seconds
          row["next_run_at"] = nil
        end
        deep(row)
      end

      def complete_workflow(workflow_id, result:)
        row = @workflows.fetch(workflow_id)
        row["status"] = "completed"
        row["result"] = deep(result)
        row["error"] = nil
        row["locked_by"] = nil
        row["locked_until"] = nil
        row["next_run_at"] = nil
        trace("complete_workflow", id: workflow_id, result:)
      end

      def fail_workflow(workflow_id, error:)
        row = @workflows.fetch(workflow_id)
        row["status"] = "failed"
        row["error"] = error
        row["locked_by"] = nil
        row["locked_until"] = nil
        row["next_run_at"] = nil
        trace("fail_workflow", id: workflow_id, error:)
      end

      def record_step_started(workflow_id:, position:, name:)
        @attempts[workflow_id].each do |attempt|
          next unless attempt.fetch("position") == position && attempt.fetch("status") == "running"

          attempt["status"] = "failed"
          attempt["error"] = "superseded by retry"
        end
        previous_cursor = @steps[workflow_id][position]&.fetch("heartbeat_cursor", nil)
        @steps[workflow_id][position] = { "workflow_id" => workflow_id, "position" => position, "name" => name, "status" => "running", "result" => nil, "error" => nil, "heartbeat_cursor" => deep(previous_cursor) }
        @attempts[workflow_id] << { "id" => next_id("attempt"), "workflow_id" => workflow_id, "position" => position, "name" => name, "status" => "running", "result" => nil, "error" => nil, "heartbeat_cursor" => deep(previous_cursor) }
        trace("step_started", id: workflow_id, position:, name:)
      end

      def record_step_completed(workflow_id:, position:, result:)
        step = @steps[workflow_id].fetch(position)
        step["status"] = "completed"
        step["result"] = deep(result)
        update_latest_attempt(workflow_id, position, "completed", result, nil)
        trace("step_completed", id: workflow_id, position:, result:)
      end

      def record_step_failed(workflow_id:, position:, error:)
        step = @steps[workflow_id].fetch(position)
        step["status"] = "failed"
        step["error"] = error
        update_latest_attempt(workflow_id, position, "failed", nil, error)
        trace("step_failed", id: workflow_id, position:, error:)
      end

      def record_wait(workflow_id:, position:, name:, wait_request:)
        @steps[workflow_id][position] = { "workflow_id" => workflow_id, "position" => position, "name" => name, "status" => "waiting", "result" => deep(wait_request.context), "error" => nil, "heartbeat_cursor" => @steps[workflow_id][position]&.fetch("heartbeat_cursor", nil) }
        wait_id = next_id("wait")
        @waits[wait_id] = { "id" => wait_id, "workflow_id" => workflow_id, "position" => position, "kind" => wait_request.kind, "event_key" => wait_request.event_key, "wake_at" => wait_request.wake_at, "context" => deep(wait_request.context), "payload" => nil, "status" => "pending" }
        update_latest_attempt(workflow_id, position, "waiting", wait_request.context, nil)
        row = @workflows.fetch(workflow_id)
        row["status"] = "waiting"
        row["locked_by"] = nil
        row["locked_until"] = nil
        trace("wait_recorded", id: workflow_id, wait_id:, kind: wait_request.kind, event_key: wait_request.event_key)
        wait_id
      end

      def wake_due_timers(now: nil)
        now ||= scheduler.time
        complete_waits(@waits.values.select { |wait| wait.fetch("status") == "pending" && wait.fetch("kind") == "timer" && wait.fetch("wake_at") <= now }, {})
      end

      def signal_event(event_key, payload: {})
        complete_waits(@waits.values.select { |wait| wait.fetch("status") == "pending" && wait.fetch("kind") == "event" && wait.fetch("event_key") == event_key }, payload)
      end

      def waits_for(workflow_id)
        @waits.values.select { |wait| wait.fetch("workflow_id") == workflow_id }.sort_by { |wait| wait.fetch("id") }.map { |row| deep(row) }
      end

      def with_fence(workflow_id:, key:, poll_interval: 0.05, timeout: 10)
        fence_key = [workflow_id, key]
        existing = @fences[fence_key]
        return deep(existing.fetch("result")) if existing&.fetch("status") == "completed"
        raise Error, existing.fetch("error") if existing&.fetch("status") == "failed"
        raise FenceTimeout, "virtual fence already running: #{key}" if existing

        @side_effects += 1
        @fences[fence_key] = { "workflow_id" => workflow_id, "key" => key, "status" => "running", "result" => nil, "error" => nil }
        trace("fence_acquired", id: workflow_id, key:)
        result = yield
        @fences[fence_key]["status"] = "completed"
        @fences[fence_key]["result"] = deep(result)
        trace("fence_completed", id: workflow_id, key:, result:)
        deep(result)
      rescue StandardError => e
        @fences[fence_key]["status"] = "failed" if @fences[fence_key]
        @fences[fence_key]["error"] = "#{e.class}: #{e.message}" if @fences[fence_key]
        raise
      end

      def enqueue_outbox(workflow_id:, topic:, payload:, key:)
        return @outbox_by_key.fetch(key) if @outbox_by_key.key?(key)

        id = next_id("outbox")
        @outbox[id] = { "id" => id, "workflow_id" => workflow_id, "topic" => topic, "payload" => deep(payload), "key" => key, "status" => "pending", "locked_by" => nil, "locked_until" => nil }
        @outbox_by_key[key] = id
        trace("outbox_enqueued", id:, key:, topic:)
        id
      end

      def claim_outbox(worker_id:, lease_seconds:)
        row = @outbox.values.select { |message| message.fetch("status") == "pending" || (message.fetch("status") == "processing" && message.fetch("locked_until") < scheduler.time) }.min_by { |message| message.fetch("id") }
        return nil unless row

        row["status"] = "processing"
        row["locked_by"] = worker_id
        row["locked_until"] = scheduler.time + lease_seconds
        trace("outbox_claimed", id: row.fetch("id"), worker: worker_id)
        deep(row)
      end

      def ack_outbox(outbox_id, worker_id:)
        row = @outbox.fetch(outbox_id)
        return unless row.fetch("locked_by") == worker_id

        row["status"] = "processed"
        trace("outbox_processed", id: outbox_id, worker: worker_id)
      end

      def outbox_message(outbox_id) = deep(@outbox.fetch(outbox_id))
      def workflow(workflow_id) = deep(@workflows.fetch(workflow_id))
      def workflow_owned?(workflow_id:, worker_id:)
        row = @workflows.fetch(workflow_id)
        row.fetch("status") == "running" && row.fetch("locked_by") == worker_id && !expired?(row)
      end

      def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
        row = @workflows.fetch(workflow_id)
        return unless row.fetch("status") == "running" && row.fetch("locked_by") == worker_id

        row["status"] = "pending"
        row["locked_by"] = nil
        row["locked_until"] = nil
        row["next_run_at"] = run_at
        trace("workflow_retry_scheduled", id: workflow_id, run_at:)
      end

      def make_workflow_due!(workflow_id, now: scheduler.time)
        row = @workflows.fetch(workflow_id)
        row["next_run_at"] = nil
        trace("workflow_retry_due", id: workflow_id, now:)
      end
      def steps_for(workflow_id) = @steps[workflow_id].values.sort_by { |row| row.fetch("position") }.map { |row| deep(row) }
      def step_attempts_for(workflow_id) = @attempts[workflow_id].map { |row| deep(row) }

      def summary
        {
          completed_workflows: @workflows.values.count { |row| row.fetch("status") == "completed" },
          side_effects: @side_effects,
          processed_outbox: @outbox.values.count { |row| row.fetch("status") == "processed" },
          workflows: @workflows.length
        }
      end

      private

      def runnable?(row)
        due = row.fetch("next_run_at", nil).nil? || row.fetch("next_run_at") <= scheduler.time
        due && (row.fetch("status") == "pending" || row.fetch("status") == "failed" || (row.fetch("status") == "running" && expired?(row)))
      end

      def expired?(row)
        row.fetch("locked_until") && row.fetch("locked_until") < scheduler.time
      end

      def claim_row(row, worker_id, lease_seconds)
        row["status"] = "running"
        row["locked_by"] = worker_id
        row["locked_until"] = scheduler.time + lease_seconds
        row["next_run_at"] = nil
        trace("workflow_claimed", id: row.fetch("id"), worker: worker_id)
        deep(row)
      end

      def complete_waits(waits, payload)
        waits.each do |wait|
          wait["status"] = "completed"
          wait["payload"] = deep(payload)
          context = wait.fetch("context").merge(payload)
          record_step_completed(workflow_id: wait.fetch("workflow_id"), position: wait.fetch("position"), result: context)
          row = @workflows.fetch(wait.fetch("workflow_id"))
          row["status"] = "pending"
          row["locked_by"] = nil
          row["locked_until"] = nil
          trace("wait_completed", id: wait.fetch("workflow_id"), wait_id: wait.fetch("id"), payload:)
        end
        waits.length
      end

      def update_latest_attempt(workflow_id, position, status, result, error)
        attempt = @attempts[workflow_id].reverse.find { |row| row.fetch("position") == position && ["running", "waiting"].include?(row.fetch("status")) }
        return unless attempt

        attempt["status"] = status
        attempt["result"] = deep(result)
        attempt["error"] = error
      end

      def next_id(prefix)
        @id_seq += 1
        format("%s-%04d", prefix, @id_seq)
      end

      def deep(value)
        Marshal.load(Marshal.dump(value))
      end

      def trace(name, fields = {})
        scheduler.trace.event(scheduler.time, "virtual_yugabyte", name, fields)
      end
    end

    class SimWorker
      def initialize(id:, scheduler:, network:, store:, workflows:, tick_interval: 20, crash_percent: 0)
        @id = id
        @scheduler = scheduler
        @network = network
        @store = store
        @workflows = workflows
        @tick_interval = tick_interval
        @crash_percent = crash_percent
      end

      def start(ticks: 20)
        ticks.times do |tick|
          @scheduler.schedule(actor: @id, delay: 5 + tick * @tick_interval + @scheduler.rng.int(7), name: "worker_tick") { run_tick }
        end
      end

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
          Engine.new(store: @store, worker_id: @id).resume(workflow, workflow_id: claimed.fetch("id"))
        rescue LeaseConflict => e
          @scheduler.trace.event(@scheduler.time, @id, "lease_conflict", error: e.message)
        end
      end
    end

    module Scenarios
      module_function

      def fetch(name)
        method(name).to_proc
      rescue NameError
        raise ArgumentError, "unknown deterministic scenario: #{name}"
      end

      def multi_worker_counter(seed)
        run(seed, "multi_worker_counter") do |h|
          workflow = counter_workflow
          h.workflows["counter"] = workflow
          8.times do |i|
            h.network.send(source: "client-#{i}", target: "db", type: "enqueue") do
              h.store.enqueue_workflow(name: "counter", input: { "count" => i })
            end
          end
          h.add_workers(%w[worker-a worker-b worker-c], ticks: 18)
        end
      end

      def waits_fences_and_outbox(seed)
        run(seed, "waits_fences_and_outbox") do |h|
          h.workflows["counter"] = counter_workflow
          h.workflows["waiting"] = Durababble::Workflow.define("waiting") do
            step("wait") { |ctx| Durababble.wait_event("approval:#{ctx.fetch("id")}", ctx) }
            step("finish") { |ctx| ctx.merge("finished" => true) }
          end

          ids = []
          3.times { |i| ids << h.store.enqueue_workflow(name: "counter", input: { "count" => i }) }
          wait_id = h.store.enqueue_workflow(name: "waiting", input: { "id" => "req" })
          h.add_workers(%w[worker-a worker-b], ticks: 15)
          h.scheduler.schedule(actor: "client-signal", delay: 120, name: "signal") { h.store.signal_event("approval:req", payload: { "approved" => true }) }
          h.scheduler.schedule(actor: "client-fence", delay: 40, name: "fence") do
            h.store.with_fence(workflow_id: ids.first, key: "charge") { { "charge" => "ok" } }
            h.store.with_fence(workflow_id: ids.first, key: "charge") { { "charge" => "duplicate" } }
          end
          h.scheduler.schedule(actor: "client-outbox", delay: 70, name: "outbox") do
            outbox = h.store.enqueue_outbox(workflow_id: ids.first, topic: "email", payload: { "to" => "x" }, key: "email")
            message = h.store.claim_outbox(worker_id: "sender", lease_seconds: 20)
            h.store.ack_outbox(outbox, worker_id: message.fetch("locked_by"))
          end
        end
      end

      def lease_expiry(seed)
        run(seed, "lease_expiry") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => 3 })
          h.store.claim_workflow(workflow_id: id, worker_id: "crashed-worker", lease_seconds: 10)
          h.scheduler.schedule(actor: "reaper", delay: 20, name: "steal_expired") { h.store.steal_expired_leases! }
          h.add_workers(%w[replacement-worker], ticks: 5)
        end
      end

      def outbox_lease_expiry(seed)
        run(seed, "outbox_lease_expiry") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => 1 })
          outbox = h.store.enqueue_outbox(workflow_id: id, topic: "email", payload: { "to" => "x" }, key: "email")
          h.store.claim_outbox(worker_id: "crashed-sender", lease_seconds: 10)
          h.scheduler.schedule(actor: "sender-b", delay: 20, name: "recover_outbox") do
            message = h.store.claim_outbox(worker_id: "sender-b", lease_seconds: 10)
            h.store.ack_outbox(outbox, worker_id: message.fetch("locked_by"))
          end
        end
      end

      def timer_and_partition(seed)
        run(seed, "timer_and_partition") do |h|
          h.workflows["timer"] = Durababble::Workflow.define("timer") do
            step("sleep") { |ctx| Durababble.wait_until(ctx.fetch("wake_at"), ctx) }
            step("finish") { |ctx| ctx.merge("timer_done" => true) }
          end
          h.network.partition("partitioned-client", "db")
          h.network.send(source: "partitioned-client", target: "db", type: "enqueue") { h.store.enqueue_workflow(name: "timer", input: { "wake_at" => 50 }) }
          h.scheduler.schedule(actor: "network", delay: 10, name: "heal") { h.network.heal("partitioned-client", "db") }
          h.scheduler.schedule(actor: "healed-client", delay: 12, name: "enqueue") { h.store.enqueue_workflow(name: "timer", input: { "wake_at" => 50 }) }
          h.add_workers(%w[worker-a worker-b], ticks: 15)
          h.scheduler.schedule(actor: "timer", delay: 55, name: "wake_due_timers") { h.store.wake_due_timers }
        end
      end

      def bug_duplicate_completion(seed)
        run(seed, "bug_duplicate_completion") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id, worker_id: "bug", lease_seconds: 10)
          h.store.record_step_started(workflow_id: id, position: 0, name: "broken")
          h.store.complete_workflow(id, result: { "count" => seed })
        end
      end

      def rpc_fault_injection(seed)
        run(seed, "rpc_fault_injection") do |h|
          outcomes = %w[success timeout connection_error eof remote_error idle_disconnect_reconnect]
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

      def workflow_rpc_lease_change(seed)
        run(seed, "workflow_rpc_lease_change") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: id, worker_id: "worker-a", lease_seconds: 10)

          worker_a = workflow_rpc_client(h, "worker-a") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.lookup", target: "worker-a")
            h.store.steal_expired_leases!(now: 11)
            h.store.claim_workflow(workflow_id: id, worker_id: "worker-b", lease_seconds: 60)
            handler = workflow_rpc_handler(h, "worker-a")
            handler.call(payload)
          rescue Durababble::WorkflowRpc::StaleLease
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.stale_rejected", stale: "worker-a")
            raise
          end
          worker_b = workflow_rpc_client(h, "worker-b") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.retry_success", target: "worker-b")
            workflow_rpc_handler(h, "worker-b").call(payload)
          end
          router = Durababble::WorkflowRpc::Router.new(store: h.store, rpc_clients: { "worker-a" => worker_a, "worker-b" => worker_b }, retry_on_stale: true)
          h.scheduler.schedule(actor: "caller", delay: 5, name: "workflow_rpc") do
            router.request(workflow_id: id, command: "status", payload: { "request" => seed })
          end
          h.check("workflow lease moved to worker-b") { h.store.workflow(id).fetch("locked_by") == "worker-b" }
          h.check("stale holder rejected") { h.scheduler.trace.to_s.include?("workflow_rpc.stale_rejected") }
          h.check("retry reached new holder") { h.scheduler.trace.to_s.include?("workflow_rpc.retry_success") }
        end
      end

      def workflow_rpc_no_active_owner_recovery(seed)
        run(seed, "workflow_rpc_no_active_owner_recovery") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: id, worker_id: "worker-a", lease_seconds: 10)

          worker_a = workflow_rpc_client(h, "worker-a") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.lookup", target: "worker-a")
            h.store.steal_expired_leases!(now: 11)
            workflow_rpc_handler(h, "worker-a").call(payload)
          rescue Durababble::WorkflowRpc::NoActiveLease
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.no_active_holder_rejected", stale: "worker-a")
            raise
          end
          worker_b = workflow_rpc_client(h, "worker-b") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.internal_start_retry_success", target: "worker-b")
            workflow_rpc_handler(h, "worker-b").call(payload)
          end
          starter = Durababble::WorkflowRpc::LeaseStarter.new(store: h.store, worker_ids: ["worker-b"], lease_seconds: 60)
          router = Durababble::WorkflowRpc::Router.new(store: h.store, rpc_clients: { "worker-a" => worker_a, "worker-b" => worker_b }, retry_on_stale: true, start_workflow: starter)
          h.scheduler.schedule(actor: "caller", delay: 5, name: "workflow_rpc") do
            router.request(workflow_id: id, command: "status", payload: { "request" => seed })
          end
          h.check("stale no-active RPC rejected") { h.scheduler.trace.to_s.include?("workflow_rpc.no_active_holder_rejected") }
          h.check("workflow was started internally") { h.store.workflow(id).fetch("locked_by") == "worker-b" }
          h.check("RPC retried after internal start") { h.scheduler.trace.to_s.include?("workflow_rpc.internal_start_retry_success") }
        end
      end

      def workflow_rpc_shutdown_midflight(seed)
        run(seed, "workflow_rpc_shutdown_midflight") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: id, worker_id: "worker-a", lease_seconds: 60)
          worker_a = workflow_rpc_client(h, "worker-a") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.lookup", target: "worker-a")
            h.store.complete_workflow(id, result: { "shutdown" => true })
            workflow_rpc_handler(h, "worker-a") do
              h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.unowned_handler_ran")
              { "bad" => true }
            end.call(payload)
          rescue Durababble::WorkflowRpc::WorkflowNotRunning
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.shutdown_rejected", stale: "worker-a")
            raise
          end
          router = Durababble::WorkflowRpc::Router.new(store: h.store, rpc_clients: { "worker-a" => worker_a }, retry_on_stale: true)
          h.scheduler.schedule(actor: "caller", delay: 5, name: "workflow_rpc") do
            begin
              router.request(workflow_id: id, command: "status", payload: {})
            rescue Durababble::WorkflowRpc::WorkflowNotRunning
              h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.no_retry_after_shutdown")
            end
          end
          h.check("shutdown stale RPC rejected") { h.scheduler.trace.to_s.include?("workflow_rpc.shutdown_rejected") }
          h.check("unowned handler did not run") { !h.scheduler.trace.to_s.include?("workflow_rpc.unowned_handler_ran") }
        end
      end

      def workflow_rpc_client(_h, _node_id, &block)
        Object.new.tap do |client|
          client.define_singleton_method(:request) do |command, payload|
            raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

            block.call(payload)
          end
        end
      end

      def workflow_rpc_handler(h, node_id, &handler_block)
        Durababble::WorkflowRpc::Handler.new(store: h.store, node_id:, handlers: {
          "status" => (handler_block || ->(payload) { { "node" => node_id, "payload" => payload } })
        })
      end

      def workflow_durable_before_claim(seed)
        run(seed, "workflow_durable_before_claim") do |h|
          h.workflows["counter"] = counter_workflow
          h.scheduler.schedule(actor: "client", delay: h.scheduler.rng.int(20), name: "enqueue_then_crash") do
            h.store.enqueue_workflow(name: "counter", input: { "count" => 5 })
          end
          h.add_workers(%w[worker-a worker-b], ticks: 12)
          h.check("pending workflow eventually completed") { h.store.summary.fetch(:completed_workflows) == 1 }
        end
      end

      def lease_conflict(seed)
        run(seed, "lease_conflict") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => 1 })
          h.store.claim_workflow(workflow_id: id, worker_id: "owner", lease_seconds: 60)
          h.scheduler.schedule(actor: "intruder", delay: h.scheduler.rng.int(20), name: "resume_without_lease") do
            Durababble::Engine.new(store: h.store, worker_id: "intruder").resume(h.workflows.fetch("counter"), workflow_id: id)
          rescue LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "intruder", "lease_conflict_observed", workflow_id: id)
          end
          h.scheduler.schedule(actor: "owner", delay: 30 + h.scheduler.rng.int(10), name: "owner_resume") do
            Durababble::Engine.new(store: h.store, worker_id: "owner").resume(h.workflows.fetch("counter"), workflow_id: id)
          end
          h.check("lease conflict observed") { h.scheduler.trace.to_s.include?("lease_conflict_observed") }
          h.check("owner completed workflow") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      def heartbeat_extension(seed)
        run(seed, "heartbeat_extension") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => 2 })
          h.store.claim_workflow(workflow_id: id, worker_id: "owner", lease_seconds: 20)
          h.scheduler.schedule(actor: "owner", delay: 15 + h.scheduler.rng.int(5), name: "heartbeat") { h.store.heartbeat(workflow_id: id, worker_id: "owner", lease_seconds: 80) }
          h.scheduler.schedule(actor: "reaper", delay: 30, name: "steal_before_original_expiry") { h.store.steal_expired_leases! }
          h.scheduler.schedule(actor: "owner", delay: 35, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "owner").resume(h.workflows.fetch("counter"), workflow_id: id) }
          h.check("heartbeat prevented premature steal") { h.store.workflow(id).fetch("status") == "completed" }
          h.check("no lease steal occurred") { !h.scheduler.trace.to_s.include?("steal_expired") }
        end
      end

      def step_heartbeat_cursor_recovery(seed)
        run(seed, "step_heartbeat_cursor_recovery") do |h|
          attempts = []
          h.workflows["cursor"] = Durababble::Workflow.define("cursor") do
            step("download") do |_ctx, heartbeat|
              attempts << heartbeat.cursor
              if attempts.length == 1
                heartbeat.record({ "offset" => seed })
                raise InjectedCrash, "crash after step heartbeat"
              end

              h.scheduler.trace.event(h.scheduler.time, "worker", "step_heartbeat_resumed", cursor: heartbeat.cursor)
              { "resumed_from" => heartbeat.cursor.fetch("offset") }
            end
          end
          id = h.store.enqueue_workflow(name: "cursor", input: {})
          h.scheduler.schedule(actor: "crashing-worker", delay: h.scheduler.rng.int(5), name: "heartbeat_then_crash") do
            Durababble::Engine.new(store: h.store, worker_id: "crashing-worker", lease_seconds: 10).resume(h.workflows.fetch("cursor"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "crashing-worker", "step_heartbeat_crash", workflow_id: id)
          end
          h.scheduler.schedule(actor: "reaper", delay: 20, name: "steal") { h.store.steal_expired_leases! }
          h.scheduler.schedule(actor: "recover", delay: 25, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "recover").resume(h.workflows.fetch("cursor"), workflow_id: id) }
          h.check("cursor was provided on retry") { attempts == [nil, { "offset" => seed }] }
          h.check("workflow completed from cursor") { h.store.workflow(id).fetch("result") == { "resumed_from" => seed } }
        end
      end

      def step_retry_policy_recovery(seed)
        run(seed, "step_retry_policy_recovery") do |h|
          attempts = 0
          h.workflows["retry"] = Durababble::Workflow.define("retry") do
            step("flaky", retry_policy: { initial_interval: 10, backoff_coefficient: 2, maximum_interval: 15, maximum_attempts: 3 }) do |ctx|
              attempts += 1
              h.scheduler.trace.event(h.scheduler.time, "worker", "step_retry_attempt", attempt: attempts)
              raise "transient #{attempts}" if attempts < 3

              ctx.merge("attempts" => attempts)
            end
          end
          id = h.store.enqueue_workflow(name: "retry", input: {})
          h.scheduler.schedule(actor: "worker-a", delay: 1 + h.scheduler.rng.int(3), name: "first_attempt") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-a").resume(h.workflows.fetch("retry"), workflow_id: id)
          end
          h.scheduler.schedule(actor: "worker-b", delay: 8, name: "restart_before_due") do
            claimed = h.store.claim_runnable_workflow(worker_id: "worker-b", lease_seconds: Engine::DEFAULT_LEASE_SECONDS)
            h.scheduler.trace.event(h.scheduler.time, "worker-b", "step_retry_not_due") unless claimed
          end
          h.scheduler.schedule(actor: "worker-b", delay: 20, name: "second_attempt_after_restart") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-b").resume(h.workflows.fetch("retry"), workflow_id: id)
          end
          h.scheduler.schedule(actor: "worker-c", delay: 36, name: "final_attempt_after_restart") do
            Durababble::Engine.new(store: h.store, worker_id: "worker-c").resume(h.workflows.fetch("retry"), workflow_id: id)
          end
          h.check("retry waited for due time") { h.scheduler.trace.to_s.include?("step_retry_not_due") }
          h.check("workflow completed after durable retries") { h.store.workflow(id).fetch("status") == "completed" }
          h.check("attempt history records retries") { h.store.step_attempts_for(id).map { |a| a.fetch("status") } == %w[failed failed completed] }
        end
      end

      def completed_step_skip_after_crash(seed)
        run(seed, "completed_step_skip_after_crash") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.scheduler.schedule(actor: "crashing-worker", delay: h.scheduler.rng.int(5), name: "crash_after_step_completed") do
            Durababble::Engine.new(store: h.store, worker_id: "crashing-worker", crash_after: :step_completed).resume(h.workflows.fetch("counter"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "crashing-worker", "crashed_after_step_completed", workflow_id: id)
          end
          h.scheduler.schedule(actor: "reaper", delay: 70, name: "steal") { h.store.steal_expired_leases!(now: h.scheduler.time + 61) }
          h.scheduler.schedule(actor: "recover", delay: 80, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "recover").resume(h.workflows.fetch("counter"), workflow_id: id) }
          h.check("completed step was not re-started") { h.scheduler.trace.to_s.scan("step_started").length == 2 }
          h.check("workflow completed after recovery") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      def incomplete_step_retry_after_crash(seed)
        run(seed, "incomplete_step_retry_after_crash") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.scheduler.schedule(actor: "crashing-worker", delay: h.scheduler.rng.int(5), name: "crash_after_step_started") do
            Durababble::Engine.new(store: h.store, worker_id: "crashing-worker", crash_after: :step_started).resume(h.workflows.fetch("counter"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "crashing-worker", "crashed_after_step_started", workflow_id: id)
          end
          h.scheduler.schedule(actor: "reaper", delay: 70, name: "steal") { h.store.steal_expired_leases!(now: h.scheduler.time + 61) }
          h.scheduler.schedule(actor: "recover", delay: 80, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "recover").resume(h.workflows.fetch("counter"), workflow_id: id) }
          h.check("incomplete step was retried") { h.store.step_attempts_for(id).map { |attempt| attempt.fetch("status") } == %w[failed completed completed] }
          h.check("workflow completed after retry") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      def attempt_history_append_only(seed)
        run(seed, "attempt_history_append_only") do |h|
          h.workflows["flaky"] = Durababble::Workflow.define("flaky") do
            step("fail") { |_ctx| raise "boom" }
          end
          id = h.store.enqueue_workflow(name: "flaky", input: { "seed" => seed })
          3.times do |i|
            h.scheduler.schedule(actor: "worker-#{i}", delay: i * 20, name: "attempt") { Durababble::Engine.new(store: h.store, worker_id: "worker-#{i}").resume(h.workflows.fetch("flaky"), workflow_id: id) }
          end
          h.check("each retry appended an attempt") { h.store.step_attempts_for(id).length == 3 }
          h.check("attempts are failed terminal records") { h.store.step_attempts_for(id).all? { |a| a.fetch("status") == "failed" } }
        end
      end

      def concurrent_signal_once(seed)
        run(seed, "concurrent_signal_once") do |h|
          h.workflows["waiting"] = Durababble::Workflow.define("waiting") do
            step("wait") { |ctx| Durababble.wait_event("event:#{ctx.fetch("id")}", ctx) }
            step("done") { |ctx| ctx.merge("done" => true) }
          end
          id = h.store.enqueue_workflow(name: "waiting", input: { "id" => "sig" })
          h.scheduler.schedule(actor: "worker", delay: 1, name: "park") { Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("waiting"), workflow_id: id) }
          5.times do |i|
            h.scheduler.schedule(actor: "signaler-#{i}", delay: 20 + h.scheduler.rng.int(5), name: "signal") { h.store.signal_event("event:sig", payload: { "signaler" => i }) }
          end
          h.scheduler.schedule(actor: "worker", delay: 40, name: "resume") { Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("waiting"), workflow_id: id) }
          h.check("wait completed once") { h.scheduler.trace.to_s.scan("wait_completed").length == 1 }
          h.check("workflow completed after signal") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      def fenced_side_effect_once(seed)
        run(seed, "fenced_side_effect_once") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          5.times do |i|
            h.scheduler.schedule(actor: "caller-#{i}", delay: h.scheduler.rng.int(5), name: "fence") do
              h.store.with_fence(workflow_id: id, key: "charge") { { "winner" => i } }
            rescue FenceTimeout
              h.scheduler.trace.event(h.scheduler.time, "caller-#{i}", "fence_waited")
            end
          end
          h.check("side effect ran once") { h.store.summary.fetch(:side_effects) == 1 }
        end
      end

      def crash_after_enqueue(seed) = workflow_durable_before_claim(seed)
      def crash_after_lease_claim(seed) = lease_expiry(seed)
      def crash_after_step_started(seed) = incomplete_step_retry_after_crash(seed)
      def crash_after_step_completed(seed) = completed_step_skip_after_crash(seed)
      def crash_while_waiting_event(seed) = concurrent_signal_once(seed)
      def crash_after_outbox_insert(seed) = outbox_lease_expiry(seed)
      def crash_after_outbox_claim(seed) = outbox_lease_expiry(seed)

      def chaos(seed)
        run(seed, "chaos") do |h|
          h.workflows["counter"] = counter_workflow
          h.workflows["waiting"] = Durababble::Workflow.define("waiting") do
            step("wait") { |ctx| Durababble.wait_event("event:#{ctx.fetch("id")}", ctx) }
            step("done") { |ctx| ctx.merge("done" => true) }
          end

          12.times do |i|
            name = h.scheduler.rng.chance(25) ? "waiting" : "counter"
            input = name == "waiting" ? { "id" => "w#{i}" } : { "count" => i }
            h.network.send(source: "client-#{i}", target: "db", type: "enqueue") { h.store.enqueue_workflow(name:, input:) }
            h.scheduler.schedule(actor: "signal-#{i}", delay: 80 + h.scheduler.rng.int(200), name: "signal") { h.store.signal_event("event:w#{i}", payload: { "signaled" => true }) }
          end
          h.add_workers(%w[worker-a worker-b worker-c worker-d], ticks: 30, crash_percent: 15)
          8.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 60 + i * 50, name: "steal_expired") { h.store.steal_expired_leases!(now: h.scheduler.time + 61) }
          end
        end
      end

      def counter_workflow
        Durababble::Workflow.define("counter") do
          step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
          step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
        end
      end

      def run(seed, scenario)
        trace = Trace.new
        scheduler = Scheduler.new(seed:, trace:)
        network = VirtualNetwork.new(scheduler:, drop_percent: scenario == "chaos" ? 5 : 0)
        store = VirtualYugabyte.new(scheduler:)
        harness = Harness.new(scenario:, seed:, scheduler:, network:, store:)
        trace.event(0, "dst", "begin", scenario:, seed:)
        yield harness
        scheduler.run
        harness.verify!
        trace.event(scheduler.time, "dst", "end", scenario:, seed:)
        trace_s = trace.to_s
        Result.new(scenario:, seed:, trace: trace_s, digest: Digest::SHA256.hexdigest(trace_s), violations: harness.violations, summary: store.summary)
      end
    end

    class Harness
      attr_reader :scenario, :seed, :scheduler, :network, :store, :workflows, :violations

      def initialize(scenario:, seed:, scheduler:, network:, store:)
        @scenario = scenario
        @seed = seed
        @scheduler = scheduler
        @network = network
        @store = store
        @workflows = {}
        @violations = []
        @checks = []
      end

      def add_workers(ids, ticks:, crash_percent: 0)
        ids.each do |id|
          SimWorker.new(id:, scheduler:, network:, store:, workflows:, crash_percent:).start(ticks:)
        end
      end

      def check(description, &block)
        @checks << [description, block]
      end

      def verify!
        @checks.each do |description, block|
          violations << "check failed: #{description}" unless block.call
        rescue StandardError => e
          violations << "check errored: #{description}: #{e.class}: #{e.message}"
        end

        workflows_state = store.instance_variable_get(:@workflows)
        steps_state = store.instance_variable_get(:@steps)
        attempts_state = store.instance_variable_get(:@attempts)
        workflows_state.each do |id, row|
          if row.fetch("status") == "completed" && row.fetch("locked_by")
            violations << "completed workflow #{id} still locked"
          end
          completed_positions = steps_state[id].values.select { |step| step.fetch("status") == "completed" }.map { |step| step.fetch("position") }
          if completed_positions.uniq.length != completed_positions.length
            violations << "duplicate completed step positions for #{id}"
          end
          attempts_state[id].each do |attempt|
            if attempt.fetch("status") == "running" && row.fetch("status") == "completed"
              violations << "completed workflow #{id} has running attempt #{attempt.fetch("id")}"
            end
          end
        end
      end
    end
  end
end
