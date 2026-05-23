# typed: true
# frozen_string_literal: true

require "digest"

module Durababble
  module Deterministic
    Result = Data.define(:scenario, :seed, :trace, :digest, :violations, :summary)
    MutationReport = Data.define(:scenario, :seed, :digest, :violations)

    class << self
      #: (untyped, seed: untyped) -> untyped
      def prove(scenario, seed:)
        Scenarios.fetch(scenario).call(seed)
      end

      #: (untyped, seeds: untyped) -> untyped
      def search(scenario, seeds:)
        seeds.filter_map do |seed|
          result = prove(scenario, seed:)
          [seed, result.violations] unless result.violations.empty?
        end
      end

      #: (untyped, seeds: untyped) -> untyped
      def search_reports(scenario, seeds:)
        seeds.filter_map do |seed|
          result = prove(scenario, seed:)
          next if result.violations.empty?

          MutationReport.new(scenario: result.scenario, seed:, digest: result.digest, violations: result.violations)
        end
      end
    end

    class Rng
      MASK = (1 << 64) - 1

      #: (untyped) -> void
      def initialize(seed)
        @state = seed & MASK
      end

      #: () -> untyped
      def next_u64
        @state = (@state * 6_364_136_223_846_793_005 + 1_442_695_040_888_963_407) & MASK
      end

      #: (untyped) -> untyped
      def int(max)
        raise ArgumentError, "max must be positive" unless max.positive?

        next_u64 % max
      end

      #: (untyped) -> untyped
      def chance(percent)
        int(100) < percent
      end
    end

    class Trace
      #: untyped
      attr_reader :lines

      #: () -> void
      def initialize
        @lines = []
      end

      #: (untyped, untyped, untyped, ?untyped) -> untyped
      def event(time, actor, name, fields = {})
        stable = fields.sort_by { |key, _| key.to_s }.map { |key, value| "#{key}=#{stable_value(value)}" }.join(" ")
        @lines << format("t=%06d actor=%s event=%s%s", time, actor, name, stable.empty? ? "" : " #{stable}")
      end

      #: () -> untyped
      def to_s
        @lines.join("\n")
      end

      private

      #: (untyped) -> untyped
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
      #: untyped
      attr_reader :time, :rng, :trace

      #: (seed: untyped, ?trace: untyped) -> void
      def initialize(seed:, trace: Trace.new)
        @rng = Rng.new(seed)
        @trace = trace
        @time = 0
        @seq = 0
        @events = []
      end

      #: (actor: untyped, delay: untyped, name: untyped) { (?) -> untyped } -> untyped
      def schedule(actor:, delay:, name:, &block)
        @seq += 1
        event = [@time + delay, @seq, actor, name, block]
        @events << event
        @events.sort_by! { |time, seq, _actor, _name, _block| [time, seq] }
        trace.event(@time, actor, "schedule", at: @time + delay, name:)
      end

      #: (?max_events: untyped) -> untyped
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
      #: (untyped) -> untyped
      attr_accessor :duplicate_percent

      #: (scheduler: untyped, ?min_latency: untyped, ?max_latency: untyped, ?drop_percent: untyped) -> void
      def initialize(scheduler:, min_latency: 1, max_latency: 9, drop_percent: 0)
        @scheduler = scheduler
        @min_latency = min_latency
        @max_latency = max_latency
        @drop_percent = drop_percent
        @duplicate_percent = 0
        @partitions = {}
      end

      #: (untyped, untyped) -> untyped
      def partition(source, target)
        @partitions[[source, target]] = true
        @scheduler.trace.event(@scheduler.time, "network", "partition", source:, target:)
      end

      #: (untyped, untyped) -> untyped
      def heal(source, target)
        @partitions.delete([source, target])
        @scheduler.trace.event(@scheduler.time, "network", "heal", source:, target:)
      end

      #: (source: untyped, target: untyped, type: untyped, ?payload: untyped) { (?) -> untyped } -> untyped
      def send(source:, target:, type:, payload: {}, &handler)
        if @partitions[[source, target]] || @scheduler.rng.chance(@drop_percent)
          @scheduler.trace.event(@scheduler.time, "network", "network.drop", source:, target:, type:)
          return
        end

        delay = @min_latency + @scheduler.rng.int(@max_latency - @min_latency + 1)
        @scheduler.trace.event(@scheduler.time, "network", "network.send", source:, target:, type:, delay:)
        schedule_delivery(source:, target:, type:, payload:, delay:, duplicate: false, &handler)
        return unless @scheduler.rng.chance(@duplicate_percent)

        duplicate_delay = delay + 1 + @scheduler.rng.int(3)
        @scheduler.trace.event(@scheduler.time, "network", "network.duplicate", source:, target:, type:, delay: duplicate_delay)
        schedule_delivery(source:, target:, type:, payload:, delay: duplicate_delay, duplicate: true, &handler)
      end

      private

      #: (source: untyped, target: untyped, type: untyped, payload: untyped, delay: untyped, duplicate: untyped) { (?) -> untyped } -> untyped
      def schedule_delivery(source:, target:, type:, payload:, delay:, duplicate:, &handler)
        @scheduler.schedule(actor: target, delay:, name: duplicate ? "deliver_duplicate:#{type}" : "deliver:#{type}") do
          @scheduler.trace.event(@scheduler.time, "network", "deliver", source:, target:, type:)
          handler.call(payload)
        end
      end
    end

    class FaultPlan
      #: (scheduler: untyped) -> void
      def initialize(scheduler:)
        @scheduler = scheduler
        @after = Hash.new { |hash, key| hash[key] = [] }
        @counts = Hash.new(0)
      end

      #: (untyped, ?once: untyped, ?message: untyped) -> untyped
      def fail_after(operation, once: 1, message: nil)
        @after[operation.to_s] << { remaining: once, message: message || "injected fault after #{operation}" }
      end

      #: (untyped) -> untyped
      def after(operation)
        operation = operation.to_s
        @counts[operation] += 1
        fault = @after[operation].find { |candidate| candidate.fetch(:remaining).positive? }
        return unless fault

        fault[:remaining] -= 1
        @scheduler.trace.event(@scheduler.time, "fault", "fault.injected", operation:, count: @counts.fetch(operation), message: fault.fetch(:message))
        raise InjectedCrash, fault.fetch(:message)
      end
    end

    class VirtualYugabyte
      #: untyped
      attr_reader :scheduler, :fault_plan

      #: (scheduler: untyped, ?fault_plan: untyped, ?mutation: untyped) -> void
      def initialize(scheduler:, fault_plan: nil, mutation: nil)
        @scheduler = scheduler
        @fault_plan = fault_plan || FaultPlan.new(scheduler:)
        @mutation = mutation&.to_sym
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

      #: () -> untyped
      def migrate! = self
      #: () -> untyped
      def close = nil
      #: () -> untyped
      def drop_schema! = nil
      #: () -> untyped
      def current_time = scheduler.time

      #: (name: untyped, input: untyped) -> untyped
      def enqueue_workflow(name:, input:)
        id = next_id("wf")
        @workflows[id] = { "id" => id, "name" => name, "status" => "pending", "input" => deep(input), "result" => nil, "error" => nil, "locked_by" => nil, "locked_until" => nil, "next_run_at" => nil }
        trace("enqueue_workflow", id:, name:)
        id
      end

      #: (name: untyped, input: untyped) -> untyped
      def create_workflow(name:, input:)
        id = enqueue_workflow(name:, input:)
        mark_workflow_running(id)
        id
      end

      #: (worker_id: untyped, lease_seconds: untyped, ?workflow_names: untyped) -> untyped
      def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil)
        workflow = @workflows.values.select { |row| runnable?(row) && (!workflow_names || workflow_names.include?(row.fetch("name"))) }.min_by { |row| row.fetch("id") }
        return unless workflow

        claim_row(workflow, worker_id, lease_seconds)
      end

      #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
      def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
        row = @workflows.fetch(workflow_id)
        return deep(row) if row.fetch("status") == "running" && row.fetch("locked_by") == worker_id && !expired?(row)
        return unless row.fetch("status") == "pending" || row.fetch("status") == "failed" || (row.fetch("status") == "running" && (row.fetch("locked_by") == worker_id || expired?(row)))

        claim_row(row, worker_id, lease_seconds)
      end

      #: (workflow_id: untyped, worker_id: untyped, lease_seconds: untyped) -> untyped
      def heartbeat(workflow_id:, worker_id:, lease_seconds:)
        row = @workflows.fetch(workflow_id)
        if row.fetch("locked_by") == worker_id && row.fetch("status") == "running" && !expired?(row)
          row["locked_until"] = scheduler.time + lease_seconds
          trace("heartbeat", id: workflow_id, worker: worker_id)
        end
      end

      #: (workflow_id: untyped, position: untyped, worker_id: untyped, lease_seconds: untyped, cursor: untyped) -> untyped
      def heartbeat_step(workflow_id:, position:, worker_id:, lease_seconds:, cursor:)
        row = @workflows.fetch(workflow_id)
        return unless row.fetch("locked_by") == worker_id && row.fetch("status") == "running" && !expired?(row)

        row["locked_until"] = scheduler.time + lease_seconds
        step = @steps[workflow_id][position]
        return unless step&.fetch("status") == "running"

        step["heartbeat_cursor"] = deep(cursor)
        latest_attempt = @attempts[workflow_id].reverse.find { |attempt| attempt.fetch("position") == position && attempt.fetch("status") == "running" }
        latest_attempt["heartbeat_cursor"] = deep(cursor) if latest_attempt
        trace("step_heartbeat", id: workflow_id, position:, worker: worker_id, cursor:)
        row.fetch("locked_until")
      end

      #: (workflow_id: untyped, position: untyped) -> untyped
      def step_heartbeat_cursor(workflow_id:, position:)
        deep(@steps[workflow_id][position]&.fetch("heartbeat_cursor", nil))
      end

      #: (untyped) -> untyped
      def current_workflow_lease(workflow_id)
        row = @workflows.fetch(workflow_id)
        return unless row.fetch("status") == "running" && row.fetch("locked_by") && !expired?(row)

        { "workflow_id" => workflow_id, "worker_id" => row.fetch("locked_by"), "locked_until" => row.fetch("locked_until") }
      end

      #: (?now: untyped) -> untyped
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

      #: (untyped, ?worker_id: untyped, ?lease_seconds: untyped) -> untyped
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

      #: (untyped, result: untyped) -> untyped
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

      #: (untyped, error: untyped) -> untyped
      def fail_workflow(workflow_id, error:)
        row = @workflows.fetch(workflow_id)
        row["status"] = "failed"
        row["error"] = error
        row["locked_by"] = nil
        row["locked_until"] = nil
        row["next_run_at"] = nil
        trace("fail_workflow", id: workflow_id, error:)
      end

      #: (workflow_id: untyped, position: untyped, name: untyped) -> untyped
      def record_step_started(workflow_id:, position:, name:)
        if mutation?(:attempt_history_overwrite)
          @attempts[workflow_id].delete_if { |attempt| attempt.fetch("position") == position }
          trace("mutation_attempt_history_overwrite", id: workflow_id, position:)
        else
          @attempts[workflow_id].each do |attempt|
            next unless attempt.fetch("position") == position && attempt.fetch("status") == "running"

            attempt["status"] = "failed"
            attempt["error"] = "superseded by retry"
          end
        end
        previous_cursor = @steps[workflow_id][position]&.fetch("heartbeat_cursor", nil)
        @steps[workflow_id][position] = { "workflow_id" => workflow_id, "position" => position, "name" => name, "status" => "running", "result" => nil, "error" => nil, "heartbeat_cursor" => deep(previous_cursor) }
        @attempts[workflow_id] << { "id" => next_id("attempt"), "workflow_id" => workflow_id, "position" => position, "name" => name, "status" => "running", "result" => nil, "error" => nil, "heartbeat_cursor" => deep(previous_cursor) }
        trace("step_started", id: workflow_id, position:, name:)
      end

      #: (workflow_id: untyped, position: untyped, result: untyped) -> untyped
      def record_step_completed(workflow_id:, position:, result:)
        step = @steps[workflow_id].fetch(position)
        step["status"] = "completed"
        step["result"] = deep(result)
        unless mutation?(:stale_step_completion)
          update_latest_attempt(workflow_id, position, "completed", result, nil)
        end
        trace("step_completed", id: workflow_id, position:, result:)
        fault_plan.after(:record_step_completed)
      end

      #: (workflow_id: untyped, position: untyped, error: untyped) -> untyped
      def record_step_failed(workflow_id:, position:, error:)
        step = @steps[workflow_id].fetch(position)
        step["status"] = "failed"
        step["error"] = error
        update_latest_attempt(workflow_id, position, "failed", nil, error)
        trace("step_failed", id: workflow_id, position:, error:)
      end

      #: (workflow_id: untyped, position: untyped, name: untyped, wait_request: untyped) -> untyped
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
        fault_plan.after(:record_wait)
        wait_id
      end

      #: (?now: untyped) -> untyped
      def wake_due_timers(now: nil)
        now ||= scheduler.time
        complete_waits(@waits.values.select { |wait| wait.fetch("status") == "pending" && wait.fetch("kind") == "timer" && wait.fetch("wake_at") <= now }, {})
      end

      #: (untyped, ?payload: untyped) -> untyped
      def signal_event(event_key, payload: {})
        if mutation?(:miss_event_wake)
          trace("event_wake_missed", event_key:)
          return 0
        end

        waits = @waits.values.select do |wait|
          wait.fetch("kind") == "event" &&
            wait.fetch("event_key") == event_key &&
            (mutation?(:duplicate_event_wake) || wait.fetch("status") == "pending")
        end
        complete_waits(waits, payload)
      end

      #: (untyped) -> untyped
      def waits_for(workflow_id)
        @waits.values.select { |wait| wait.fetch("workflow_id") == workflow_id }.sort_by { |wait| wait.fetch("id") }.map { |row| deep(row) }
      end

      #: (workflow_id: untyped, key: untyped, ?poll_interval: untyped, ?timeout: untyped) { (?) -> untyped } -> untyped
      def with_fence(workflow_id:, key:, poll_interval: 0.05, timeout: 10, &block)
        fence_key = [workflow_id, key]
        existing = @fences[fence_key]
        return deep(existing.fetch("result")) if existing&.fetch("status") == "completed"
        raise Error, existing.fetch("error") if existing&.fetch("status") == "failed"
        raise FenceTimeout, "virtual fence already running: #{key}" if existing

        @side_effects += 1
        @fences[fence_key] = { "workflow_id" => workflow_id, "key" => key, "status" => "running", "result" => nil, "error" => nil }
        trace("fence_acquired", id: workflow_id, key:)
        result = block.call
        @fences[fence_key]["status"] = "completed"
        @fences[fence_key]["result"] = deep(result)
        trace("fence_completed", id: workflow_id, key:, result:)
        deep(result)
      rescue StandardError => e
        @fences[fence_key]["status"] = "failed" if @fences[fence_key]
        @fences[fence_key]["error"] = "#{e.class}: #{e.message}" if @fences[fence_key]
        raise
      end

      #: (workflow_id: untyped, topic: untyped, payload: untyped, key: untyped) -> untyped
      def enqueue_outbox(workflow_id:, topic:, payload:, key:)
        return @outbox_by_key.fetch(key) if @outbox_by_key.key?(key) && !mutation?(:outbox_duplicate_by_key)

        id = next_id("outbox")
        @outbox[id] = { "id" => id, "workflow_id" => workflow_id, "topic" => topic, "payload" => deep(payload), "key" => key, "status" => "pending", "locked_by" => nil, "locked_until" => nil }
        @outbox_by_key[key] ||= id
        trace("outbox_enqueued", id:, key:, topic:)
        fault_plan.after(:enqueue_outbox)
        id
      end

      #: (worker_id: untyped, lease_seconds: untyped) -> untyped
      def claim_outbox(worker_id:, lease_seconds:)
        claimable = @outbox.values.select do |message|
          message.fetch("status") == "pending" ||
            (!mutation?(:outbox_stuck_lease) && message.fetch("status") == "processing" && message.fetch("locked_until") < scheduler.time)
        end
        row = claimable.min_by { |message| message.fetch("id") }
        return unless row

        row["status"] = "processing"
        row["locked_by"] = worker_id
        row["locked_until"] = scheduler.time + lease_seconds
        trace("outbox_claimed", id: row.fetch("id"), worker: worker_id)
        deep(row)
      end

      #: (untyped, worker_id: untyped) -> untyped
      def ack_outbox(outbox_id, worker_id:)
        row = @outbox.fetch(outbox_id)
        return unless row.fetch("locked_by") == worker_id

        row["status"] = "processed"
        trace("outbox_processed", id: outbox_id, worker: worker_id)
      end

      #: (untyped) -> untyped
      def outbox_message(outbox_id) = deep(@outbox.fetch(outbox_id))
      #: () -> untyped
      def outbox_messages = @outbox.values.sort_by { |row| row.fetch("id") }.map { |row| deep(row) }
      #: (untyped) -> untyped
      def workflow(workflow_id) = deep(@workflows.fetch(workflow_id))

      #: (workflow_id: untyped, worker_id: untyped) -> untyped
      def workflow_owned?(workflow_id:, worker_id:)
        row = @workflows.fetch(workflow_id)
        return true if mutation?(:stale_lease_commit) && row.fetch("status") == "running" && row.fetch("locked_by") == worker_id

        row.fetch("status") == "running" && row.fetch("locked_by") == worker_id && !expired?(row)
      end

      #: (workflow_id: untyped, worker_id: untyped, run_at: untyped) -> untyped
      def schedule_workflow_retry(workflow_id:, worker_id:, run_at:)
        row = @workflows.fetch(workflow_id)
        return unless row.fetch("status") == "running" && row.fetch("locked_by") == worker_id

        row["status"] = "pending"
        row["locked_by"] = nil
        row["locked_until"] = nil
        row["next_run_at"] = run_at
        trace("workflow_retry_scheduled", id: workflow_id, run_at:)
      end

      #: (untyped, ?now: untyped) -> untyped
      def make_workflow_due!(workflow_id, now: scheduler.time)
        row = @workflows.fetch(workflow_id)
        row["next_run_at"] = nil
        trace("workflow_retry_due", id: workflow_id, now:)
      end

      #: (untyped) -> untyped
      def steps_for(workflow_id) = @steps[workflow_id].values.sort_by { |row| row.fetch("position") }.map { |row| deep(row) }
      #: (untyped) -> untyped
      def step_attempts_for(workflow_id) = @attempts[workflow_id].map { |row| deep(row) }

      #: () -> untyped
      def summary
        {
          completed_workflows: @workflows.values.count { |row| row.fetch("status") == "completed" },
          side_effects: @side_effects,
          processed_outbox: @outbox.values.count { |row| row.fetch("status") == "processed" },
          workflows: @workflows.length,
        }
      end

      private

      #: (untyped) -> untyped
      def runnable?(row)
        due = row.fetch("next_run_at", nil).nil? || row.fetch("next_run_at") <= scheduler.time
        due && (row.fetch("status") == "pending" || row.fetch("status") == "failed" || (row.fetch("status") == "running" && expired?(row)))
      end

      #: (untyped) -> untyped
      def expired?(row)
        row.fetch("locked_until") && row.fetch("locked_until") < scheduler.time
      end

      #: (untyped, untyped, untyped) -> untyped
      def claim_row(row, worker_id, lease_seconds)
        row["status"] = "running"
        row["locked_by"] = worker_id
        row["locked_until"] = scheduler.time + lease_seconds
        row["next_run_at"] = nil
        trace("workflow_claimed", id: row.fetch("id"), worker: worker_id)
        deep(row)
      end

      #: (untyped, untyped) -> untyped
      def complete_waits(waits, payload)
        completed = 0
        waits.each do |wait|
          row = @workflows.fetch(wait.fetch("workflow_id"))
          next unless row.fetch("status") == "waiting" || (mutation?(:duplicate_event_wake) && row.fetch("status") == "pending")

          wait["status"] = "completed"
          wait["payload"] = deep(payload)
          context = wait.fetch("context").merge(payload)
          record_step_completed(workflow_id: wait.fetch("workflow_id"), position: wait.fetch("position"), result: context)
          row["status"] = "pending"
          row["locked_by"] = nil
          row["locked_until"] = nil
          completed += 1
          trace("wait_completed", id: wait.fetch("workflow_id"), wait_id: wait.fetch("id"), payload:)
        end
        completed
      end

      #: (untyped, untyped, untyped, untyped, untyped) -> untyped
      def update_latest_attempt(workflow_id, position, status, result, error)
        attempt = @attempts[workflow_id].reverse.find { |row| row.fetch("position") == position && ["running", "waiting"].include?(row.fetch("status")) }
        return unless attempt

        attempt["status"] = status
        attempt["result"] = deep(result)
        attempt["error"] = error
      end

      #: (untyped) -> untyped
      def next_id(prefix)
        @id_seq += 1
        format("%s-%04d", prefix, @id_seq)
      end

      #: (untyped) -> untyped
      def deep(value)
        Marshal.load(Marshal.dump(value))
      end

      #: (untyped) -> untyped
      def mutation?(name)
        @mutation == name.to_sym
      end

      #: (untyped, ?untyped) -> untyped
      def trace(name, fields = {})
        scheduler.trace.event(scheduler.time, "virtual_yugabyte", name, fields)
      end
    end

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
          Engine.new(store: @store, worker_id: @id).resume(workflow, workflow_id: claimed.fetch("id"))
        rescue LeaseConflict => e
          @scheduler.trace.event(@scheduler.time, @id, "lease_conflict", error: e.message)
        end
      end
    end

    module Scenarios
      extend self
      include Kernel

      #: (untyped) -> untyped
      def fetch(name)
        method(name).to_proc
      rescue NameError
        raise ArgumentError, "unknown deterministic scenario: #{name}"
      end

      #: (untyped) -> untyped
      def multi_worker_counter(seed)
        run(seed, "multi_worker_counter") do |h|
          workflow = counter_workflow
          h.workflows["counter"] = workflow
          8.times do |i|
            h.network.send(source: "client-#{i}", target: "db", type: "enqueue") do
              h.store.enqueue_workflow(name: "counter", input: { "count" => i })
            end
          end
          h.add_workers(["worker-a", "worker-b", "worker-c"], ticks: 18)
        end
      end

      #: (untyped) -> untyped
      def waits_fences_and_outbox(seed)
        run(seed, "waits_fences_and_outbox") do |h|
          h.workflows["counter"] = counter_workflow
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_event("approval:#{ctx.fetch("id")}", ctx) }
            test_step("finish") { |ctx| ctx.merge("finished" => true) }
          end

          ids = []
          3.times { |i| ids << h.store.enqueue_workflow(name: "counter", input: { "count" => i }) }
          h.store.enqueue_workflow(name: "waiting", input: { "id" => "req" })
          h.add_workers(["worker-a", "worker-b"], ticks: 15)
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

      #: (untyped) -> untyped
      def lease_expiry(seed)
        run(seed, "lease_expiry") do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => 3 })
          h.store.claim_workflow(workflow_id: id, worker_id: "crashed-worker", lease_seconds: 10)
          h.scheduler.schedule(actor: "reaper", delay: 20, name: "steal_expired") { h.store.steal_expired_leases! }
          h.add_workers(["replacement-worker"], ticks: 5)
        end
      end

      #: (untyped) -> untyped
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

      #: (untyped) -> untyped
      def timer_and_partition(seed)
        run(seed, "timer_and_partition") do |h|
          h.workflows["timer"] = workflow_class("timer") do
            test_step("sleep") { |ctx| Durababble.wait_until(ctx.fetch("wake_at"), ctx) }
            test_step("finish") { |ctx| ctx.merge("timer_done" => true) }
          end
          h.network.partition("partitioned-client", "db")
          h.network.send(source: "partitioned-client", target: "db", type: "enqueue") { h.store.enqueue_workflow(name: "timer", input: { "wake_at" => 50 }) }
          h.scheduler.schedule(actor: "network", delay: 10, name: "heal") { h.network.heal("partitioned-client", "db") }
          h.scheduler.schedule(actor: "healed-client", delay: 12, name: "enqueue") { h.store.enqueue_workflow(name: "timer", input: { "wake_at" => 50 }) }
          h.add_workers(["worker-a", "worker-b"], ticks: 15)
          h.scheduler.schedule(actor: "timer", delay: 55, name: "wake_due_timers") { h.store.wake_due_timers }
        end
      end

      #: (untyped) -> untyped
      def bug_duplicate_completion(seed)
        run(seed, "bug_duplicate_completion") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.mark_workflow_running(id, worker_id: "bug", lease_seconds: 10)
          h.store.record_step_started(workflow_id: id, position: 0, name: "broken")
          h.store.complete_workflow(id, result: { "count" => seed })
        end
      end

      #: (untyped) -> untyped
      def bug_stale_step_completion(seed)
        run(seed, "bug_stale_step_completion", mutation: :stale_step_completion) do |h|
          h.workflows["counter"] = counter_workflow
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.scheduler.schedule(actor: "worker", delay: h.scheduler.rng.int(5), name: "complete_with_stale_attempt") do
            Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("counter"), workflow_id: id)
          end
        end
      end

      #: (untyped) -> untyped
      def bug_stale_lease_commit(seed)
        run(seed, "bug_stale_lease_commit", mutation: :stale_lease_commit) do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: id, worker_id: "stale-worker", lease_seconds: 10)
          h.scheduler.schedule(actor: "stale-worker", delay: 20, name: "commit_after_lease_expiry") do
            if h.store.workflow_owned?(workflow_id: id, worker_id: "stale-worker")
              h.store.complete_workflow(id, result: { "owner" => "stale-worker" })
              h.scheduler.trace.event(h.scheduler.time, "stale-worker", "stale_lease_commit_accepted", workflow_id: id)
            else
              h.scheduler.trace.event(h.scheduler.time, "stale-worker", "stale_lease_commit_rejected", workflow_id: id)
            end
          end
          h.check("stale lease commit was rejected") do
            !h.scheduler.trace.to_s.include?("stale_lease_commit_accepted")
          end
        end
      end

      #: (untyped) -> untyped
      def bug_missed_event_wake(seed)
        run(seed, "bug_missed_event_wake", mutation: :miss_event_wake) do |h|
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_event("miss:#{ctx.fetch("id")}", ctx) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          id = h.store.enqueue_workflow(name: "waiting", input: { "id" => seed.to_s })
          h.scheduler.schedule(actor: "worker", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("waiting"), workflow_id: id)
          end
          h.scheduler.schedule(actor: "signaler", delay: 20, name: "signal") do
            signaled = h.store.signal_event("miss:#{seed}", payload: { "ok" => true })
            h.scheduler.trace.event(h.scheduler.time, "signaler", "signal_result", signaled:)
          end
          h.scheduler.schedule(actor: "worker", delay: 30, name: "resume_after_signal") do
            Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("waiting"), workflow_id: id)
          rescue LeaseConflict
            h.scheduler.trace.event(h.scheduler.time, "worker", "resume_blocked_waiting", workflow_id: id)
          end
          h.check("event wake completed waiting workflow") do
            h.store.workflow(id).fetch("status") == "completed"
          end
        end
      end

      #: (untyped) -> untyped
      def bug_duplicated_event_wake(seed)
        run(seed, "bug_duplicated_event_wake", mutation: :duplicate_event_wake) do |h|
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_event("dup-wake:#{ctx.fetch("id")}", ctx) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          id = h.store.enqueue_workflow(name: "waiting", input: { "id" => seed.to_s })
          h.scheduler.schedule(actor: "worker", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("waiting"), workflow_id: id)
          end
          2.times do |i|
            h.scheduler.schedule(actor: "signaler-#{i}", delay: 20 + i, name: "signal") do
              h.store.signal_event("dup-wake:#{seed}", payload: { "signaler" => i })
            end
          end
          h.scheduler.schedule(actor: "worker", delay: 30, name: "resume") do
            Durababble::Engine.new(store: h.store, worker_id: "worker").resume(h.workflows.fetch("waiting"), workflow_id: id)
          end
          h.check("event wake completed only once") do
            h.scheduler.trace.to_s.scan("wait_completed").length == 1
          end
        end
      end

      #: (untyped) -> untyped
      def bug_outbox_duplicate_by_key(seed)
        run(seed, "bug_outbox_duplicate_by_key", mutation: :outbox_duplicate_by_key) do |h|
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          2.times do |i|
            h.scheduler.schedule(actor: "producer-#{i}", delay: i, name: "enqueue_outbox") do
              h.store.enqueue_outbox(
                workflow_id:,
                topic: "email",
                payload: { "producer" => i },
                key: "email:#{seed}",
              )
            end
          end
          h.check("outbox key stayed unique") do
            h.store.outbox_messages.count { |message| message.fetch("key") == "email:#{seed}" } == 1
          end
        end
      end

      #: (untyped) -> untyped
      def bug_outbox_stuck_lease(seed)
        run(seed, "bug_outbox_stuck_lease", mutation: :outbox_stuck_lease) do |h|
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.enqueue_outbox(workflow_id:, topic: "email", payload: { "seed" => seed }, key: "email:#{seed}")
          h.store.claim_outbox(worker_id: "crashed-sender", lease_seconds: 10)
          h.scheduler.schedule(actor: "sender-b", delay: 20, name: "recover_outbox") do
            message = h.store.claim_outbox(worker_id: "sender-b", lease_seconds: 10)
            if message
              h.store.ack_outbox(message.fetch("id"), worker_id: "sender-b")
            else
              h.scheduler.trace.event(h.scheduler.time, "sender-b", "outbox_reclaim_missed")
            end
          end
          h.check("outbox lease was reclaimed after expiry") do
            h.store.summary.fetch(:processed_outbox) == 1
          end
        end
      end

      #: (untyped) -> untyped
      def bug_attempt_history_corruption(seed)
        run(seed, "bug_attempt_history_corruption", mutation: :attempt_history_overwrite) do |h|
          h.workflows["flaky"] = workflow_class("flaky") do
            test_step("fail") { |_ctx| raise "boom #{seed}" }
          end
          id = h.store.enqueue_workflow(name: "flaky", input: { "seed" => seed })
          3.times do |i|
            h.scheduler.schedule(actor: "worker-#{i}", delay: i * 20, name: "attempt") do
              Durababble::Engine.new(store: h.store, worker_id: "worker-#{i}").resume(h.workflows.fetch("flaky"), workflow_id: id)
            end
          end
          h.check("attempt history stayed append-only") { h.store.step_attempts_for(id).length == 3 }
        end
      end

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

      #: (untyped) -> untyped
      def workflow_rpc_owner_state_matrix(seed)
        run(seed, "workflow_rpc_owner_state_matrix") do |h|
          moved_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: moved_id, worker_id: "worker-a", lease_seconds: 10)
          moved_worker_a = workflow_rpc_client(h, "worker-a") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.lookup", target: "worker-a")
            h.store.steal_expired_leases!(now: 11)
            h.store.claim_workflow(workflow_id: moved_id, worker_id: "worker-b", lease_seconds: 60)
            handler = workflow_rpc_handler(h, "worker-a")
            handler.call(payload)
          rescue Durababble::WorkflowRpc::StaleLease
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.stale_rejected", stale: "worker-a")
            raise
          end
          moved_worker_b = workflow_rpc_client(h, "worker-b") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.retry_success", target: "worker-b")
            workflow_rpc_handler(h, "worker-b").call(payload)
          end
          moved_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-a" => moved_worker_a, "worker-b" => moved_worker_b },
            retry_on_stale: true,
          )
          h.scheduler.schedule(actor: "caller", delay: 5, name: "workflow_rpc_moved") do
            moved_router.request(workflow_id: moved_id, command: "status", payload: { "request" => seed })
          end

          h.workflows["counter"] = counter_workflow
          no_active_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 1 })
          h.store.claim_workflow(workflow_id: no_active_id, worker_id: "worker-c", lease_seconds: 30)
          no_active_worker_c = workflow_rpc_client(h, "worker-c") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.lookup", target: "worker-a")
            h.store.steal_expired_leases!(now: 31)
            workflow_rpc_handler(h, "worker-c").call(payload)
          rescue Durababble::WorkflowRpc::NoActiveLease
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.no_active_holder_rejected", stale: "worker-c")
            raise
          end
          restarted_worker_d = workflow_rpc_client(h, "worker-d") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.internal_start_retry_success", target: "worker-d")
            workflow_rpc_handler(h, "worker-d").call(payload)
          end
          starter = Durababble::WorkflowRpc::LeaseStarter.new(store: h.store, worker_ids: ["worker-d"], lease_seconds: 60)
          no_active_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-c" => no_active_worker_c, "worker-d" => restarted_worker_d },
            retry_on_stale: true,
            start_workflow: starter,
          )
          h.scheduler.schedule(actor: "caller", delay: 15, name: "workflow_rpc_no_active") do
            no_active_router.request(workflow_id: no_active_id, command: "status", payload: { "request" => seed })
          end

          shutdown_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 2 })
          h.store.claim_workflow(workflow_id: shutdown_id, worker_id: "worker-e", lease_seconds: 60)
          shutdown_worker = workflow_rpc_client(h, "worker-e") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.lookup", target: "worker-e")
            h.store.complete_workflow(shutdown_id, result: { "shutdown" => true })
            workflow_rpc_handler(h, "worker-e") do
              h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.unowned_handler_ran")
              { "bad" => true }
            end.call(payload)
          rescue Durababble::WorkflowRpc::WorkflowNotRunning
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.shutdown_rejected", stale: "worker-e")
            raise
          end
          shutdown_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-e" => shutdown_worker },
            retry_on_stale: true,
          )
          h.scheduler.schedule(actor: "caller", delay: 25, name: "workflow_rpc_shutdown") do
            shutdown_router.request(workflow_id: shutdown_id, command: "status", payload: {})
          rescue Durababble::WorkflowRpc::WorkflowNotRunning
            h.scheduler.trace.event(h.scheduler.time, "workflow_rpc", "workflow_rpc.no_retry_after_shutdown")
          end

          h.check("workflow lease moved to worker-b") { h.store.workflow(moved_id).fetch("locked_by") == "worker-b" }
          h.check("stale holder rejected") { h.scheduler.trace.to_s.include?("workflow_rpc.stale_rejected") }
          h.check("retry reached new holder") { h.scheduler.trace.to_s.include?("workflow_rpc.retry_success") }
          h.check("stale no-active RPC rejected") { h.scheduler.trace.to_s.include?("workflow_rpc.no_active_holder_rejected") }
          h.check("workflow was started internally") { h.store.workflow(no_active_id).fetch("locked_by") == "worker-d" }
          h.check("RPC retried after internal start") { h.scheduler.trace.to_s.include?("workflow_rpc.internal_start_retry_success") }
          h.check("shutdown stale RPC rejected") { h.scheduler.trace.to_s.include?("workflow_rpc.shutdown_rejected") }
          h.check("unowned handler did not run") { !h.scheduler.trace.to_s.include?("workflow_rpc.unowned_handler_ran") }
        end
      end

      #: (untyped) -> untyped
      def grpc_workflow_rpc_response_matrix(seed)
        run(seed, "grpc_workflow_rpc_response_matrix") do |h|
          moved_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: moved_id, worker_id: "worker-a", lease_seconds: 10)
          worker_a = grpc_workflow_rpc_client(h, "worker-a") do |payload|
            h.store.steal_expired_leases!(now: 11)
            h.store.claim_workflow(workflow_id: moved_id, worker_id: "worker-b", lease_seconds: 60)
            workflow_rpc_handler(h, "worker-a").call(payload)
          end
          worker_b = grpc_workflow_rpc_client(h, "worker-b") do |payload|
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.retry_success", target: "worker-b")
            workflow_rpc_handler(h, "worker-b").call(payload)
          end
          moved_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-a" => worker_a, "worker-b" => worker_b },
            retry_on_stale: true,
          )
          h.scheduler.schedule(actor: "caller", delay: 5, name: "grpc_workflow_rpc") do
            moved_router.request(workflow_id: moved_id, command: "status", payload: { "request" => seed })
          end

          unavailable_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 1 })
          h.store.claim_workflow(workflow_id: unavailable_id, worker_id: "worker-a", lease_seconds: 60)
          unavailable_client = Object.new
          unavailable_client.define_singleton_method(:request) do |command, _payload|
            raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.unavailable", target: "worker-a")
            raise Durababble::WorkflowRpc::NodeUnavailable, "worker-a unavailable"
          end
          unavailable_router = Durababble::WorkflowRpc::Router.new(
            store: h.store,
            rpc_clients: { "worker-a" => unavailable_client },
            retry_on_stale: false,
          )
          h.scheduler.schedule(actor: "caller", delay: 1, name: "grpc_unavailable") do
            unavailable_router.request(workflow_id: unavailable_id, command: "status", payload: {})
          rescue Durababble::WorkflowRpc::NodeUnavailable
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.node_unavailable_observed")
          end

          not_running_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 2 })
          worker_b = grpc_workflow_rpc_client(h, "worker-b")
          h.scheduler.schedule(actor: "caller", delay: 15, name: "grpc_not_running") do
            worker_b.request("workflow_rpc", {
              "workflow_id" => not_running_id,
              "command" => "status",
              "payload" => {},
            })
          rescue Durababble::WorkflowRpc::NoActiveLease
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.not_running_observed")
          end
          h.check("gRPC CallTransient was used") { h.scheduler.trace.to_s.include?("grpc.call_transient") }
          h.check("gRPC moved response was emitted") { h.scheduler.trace.to_s.include?("grpc.lease_moved") }
          h.check("gRPC moved response decoded as stale lease") { h.scheduler.trace.to_s.include?("grpc.decode_moved") }
          h.check("gRPC retry reached the new owner") { h.scheduler.trace.to_s.include?("grpc.retry_success") }
          h.check("gRPC unavailable surfaced as node unavailable") do
            h.scheduler.trace.to_s.include?("grpc.node_unavailable_observed")
          end
          h.check("gRPC not_running response decoded as no active lease") do
            h.scheduler.trace.to_s.include?("grpc.not_running_observed")
          end
        end
      end

      #: (untyped) -> untyped
      def grpc_workflow_rpc_transport_fault_matrix(seed)
        run(seed, "grpc_workflow_rpc_transport_fault_matrix") do |h|
          faults = [
            "timeout",
            "deadline_exceeded",
            "connection_reset",
            "eof",
            "unavailable",
            "response_timeout",
            "duplicate_response",
          ].rotate(h.scheduler.rng.int(7))
          handler_calls = Hash.new(0)

          faults.each_with_index do |fault, index|
            id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + index })
            node_id = "worker-#{index}"
            h.store.claim_workflow(workflow_id: id, worker_id: node_id, lease_seconds: 60)
            client = grpc_workflow_rpc_client(h, node_id, faults: [fault]) do |payload|
              handler_calls[fault] += 1
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.handler", fault:, target: node_id)
              workflow_rpc_handler(h, node_id).call(payload)
            end
            router = Durababble::WorkflowRpc::Router.new(
              store: h.store,
              rpc_clients: { node_id => client },
              retry_on_stale: true,
            )
            h.scheduler.schedule(actor: "caller", delay: index * 4, name: "grpc_fault:#{fault}") do
              router.request(workflow_id: id, command: "status", payload: { "fault" => fault })
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.transport_retry_success", fault:)
            end
          end

          h.check("timeout transport fault was injected") { h.scheduler.trace.to_s.include?("grpc.timeout") }
          h.check("deadline transport fault was injected") { h.scheduler.trace.to_s.include?("grpc.deadline_exceeded") }
          h.check("RST transport fault was injected") { h.scheduler.trace.to_s.include?("grpc.rst") }
          h.check("EOF transport fault was injected") { h.scheduler.trace.to_s.include?("grpc.eof") }
          h.check("unavailable transport fault was injected") { h.scheduler.trace.to_s.include?("grpc.unavailable") }
          h.check("response-timeout fault was injected after handler execution") do
            h.scheduler.trace.to_s.include?("grpc.response_timeout")
          end
          h.check("duplicate response delivery was modeled") { h.scheduler.trace.to_s.include?("grpc.duplicate_response") }
          h.check("each transport fault retried to success") do
            h.scheduler.trace.to_s.scan("grpc.transport_retry_success").length == faults.length
          end
          h.check("lost response can duplicate a transient handler invocation") do
            handler_calls["response_timeout"] == 2
          end
          h.check("explicit duplicate response can duplicate a transient handler invocation") do
            handler_calls["duplicate_response"] == 2
          end
        end
      end

      #: (untyped) -> untyped
      def grpc_workflow_rpc_transport_fault_reroute(seed)
        run(seed, "grpc_workflow_rpc_transport_fault_reroute") do |h|
          faults = ["timeout", "deadline_exceeded", "connection_reset", "eof", "unavailable"]

          faults.each_with_index do |fault, index|
            id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + index })
            old_owner = "old-owner-#{index}"
            new_owner = "new-owner-#{index}"
            h.store.claim_workflow(workflow_id: id, worker_id: old_owner, lease_seconds: 10)
            old_client = Object.new
            transport_fault = method(:grpc_transport_fault!)
            old_client.define_singleton_method(:request) do |command, _payload|
              raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

              h.store.mark_workflow_running(id, worker_id: new_owner, lease_seconds: 60)
              transport_fault.call(h, fault, target: old_owner)
            end
            new_client = grpc_workflow_rpc_client(h, new_owner) do |payload|
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.transport_reroute_success", fault:)
              workflow_rpc_handler(h, new_owner).call(payload)
            end
            router = Durababble::WorkflowRpc::Router.new(
              store: h.store,
              rpc_clients: { old_owner => old_client, new_owner => new_client },
              retry_on_stale: true,
            )
            h.scheduler.schedule(actor: "caller", delay: index * 4, name: "grpc_reroute:#{fault}") do
              router.request(workflow_id: id, command: "status", payload: { "fault" => fault })
            end
          end

          h.check("transport failures caused fresh lease lookups and reroutes") do
            h.scheduler.trace.to_s.scan("grpc.transport_reroute_success").length == faults.length
          end
          h.check("reroute matrix included timeout") { h.scheduler.trace.to_s.include?("fault=\"timeout\"") }
          h.check("reroute matrix included RST") { h.scheduler.trace.to_s.include?("fault=\"connection_reset\"") }
          h.check("reroute matrix included EOF") { h.scheduler.trace.to_s.include?("fault=\"eof\"") }
        end
      end

      #: (untyped) -> untyped
      def grpc_wakeup_fault_matrix(seed)
        run(seed, "grpc_wakeup_fault_matrix") do |h|
          h.workflows["counter"] = counter_workflow
          active_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          recovery_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed + 1 })
          h.store.claim_workflow(workflow_id: active_id, worker_id: "worker-a", lease_seconds: 60)
          events = []
          service = Durababble::Rpc::Service.new(
            node_id: "worker-a",
            store: h.store,
            worker_pool: "default",
            workflow_handlers: {},
            transient_handler: nil,
            node_directory: Durababble::Rpc::NodeDirectory.new,
            authorize: nil,
            awaken_batch: ->(**event) { events << [:awaken_batch, event] },
            evict_lease: ->(**event) { events << [:evict_lease, event] },
            deliver_message: ->(**event) { events << [:deliver_message, event] },
          )
          operations = [
            ["awaken_batch", "drop"],
            ["awaken_batch", "duplicate"],
            ["deliver_message", "timeout"],
            ["deliver_message", "connection_reset"],
            ["deliver_message", "duplicate"],
            ["evict_lease", "eof"],
            ["evict_lease", "unavailable"],
          ].rotate(h.scheduler.rng.int(7))

          operations.each_with_index do |(method_name, fault), index|
            h.scheduler.schedule(actor: "caller", delay: index * 3, name: "grpc_wakeup:#{method_name}:#{fault}") do
              grpc_faulty_unary(h, method_name, target: "worker-a", fault:) do
                call_grpc_service_method(service, method_name, workflow_id: active_id)
              end
            rescue Durababble::WorkflowRpc::NodeUnavailable
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.wakeup_fault_observed", method: method_name, fault:)
            end
          end

          h.add_workers(["polling-worker"], ticks: 8)
          h.scheduler.schedule(actor: "reaper", delay: 80, name: "release_active_for_recovery") do
            h.store.steal_expired_leases!(now: h.scheduler.time + Engine::DEFAULT_LEASE_SECONDS + 1)
          end
          h.check("wakeup drop was injected") { h.scheduler.trace.to_s.include?("grpc.drop") }
          h.check("wakeup duplicate was injected") { h.scheduler.trace.to_s.include?("grpc.duplicate") }
          h.check("wakeup timeout was observed") { h.scheduler.trace.to_s.include?("grpc.wakeup_fault_observed") }
          h.check("polling completed workflow despite wakeup transport faults") do
            h.store.workflow(recovery_id).fetch("status") == "completed"
          end
          h.check("duplicate wakeups did not create durable duplicate effects") do
            events.count { |event, _| event == :awaken_batch } <= 2 &&
              events.count { |event, _| event == :deliver_message } <= 2
          end
        end
      end

      #: (untyped) -> untyped
      def grpc_service_contract(seed)
        run(seed, "grpc_service_contract") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: id, worker_id: "worker-a", lease_seconds: 10)
          events = []
          service = Durababble::Rpc::Service.new(
            node_id: "worker-a",
            store: h.store,
            worker_pool: "default",
            workflow_handlers: { "status" => ->(payload) { { "node" => "worker-a", "seed" => payload.fetch("seed") } } },
            transient_handler: ->(request:, args:) { { "method" => request["method"], "args" => args } },
            node_directory: Durababble::Rpc::NodeDirectory.new("worker-b" => "virtual://worker-b"),
            authorize: nil,
            awaken_batch: ->(**event) { events << [:awaken_batch, event] },
            evict_lease: ->(**event) { events << [:evict_lease, event] },
            deliver_message: ->(**event) { events << [:deliver_message, event] },
          )

          h.scheduler.schedule(actor: "caller", delay: 1, name: "grpc_awaken_batch") do
            service.awaken_batch(
              Durababble::Rpc::Proto::AwakenBatchRequest.new(worker_pool: "default", workflow_ids: [id]),
              nil,
            )
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.awaken_batch")
          end
          h.scheduler.schedule(actor: "caller", delay: 2, name: "grpc_evict_lease") do
            service.evict_lease(
              Durababble::Rpc::Proto::EvictLeaseRequest.new(
                worker_pool: "default",
                target_kind: "workflow",
                target_id: id,
              ),
              nil,
            )
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.evict_lease")
          end
          h.scheduler.schedule(actor: "caller", delay: 3, name: "grpc_deliver_message") do
            service.deliver_message(
              Durababble::Rpc::Proto::DeliverMessageRequest.new(
                worker_pool: "default",
                target_kind: "workflow",
                target_id: id,
              ),
              nil,
            )
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.deliver_message")
          end
          h.scheduler.schedule(actor: "caller", delay: 4, name: "grpc_call_transient") do
            response = service.call_transient(
              Durababble::Rpc::Proto::TransientRequest.new(
                worker_pool: "default",
                workflow_id: id,
                method: "status",
                args: Durababble::Rpc.dump({ "seed" => seed }),
              ),
              nil,
            )
            decoded = Durababble::Rpc::Client.decode_transient_response(response)
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.call_transient_ok", decoded:)
          end
          h.scheduler.schedule(actor: "caller", delay: 5, name: "grpc_call_object_transient") do
            response = service.call_transient(
              Durababble::Rpc::Proto::TransientRequest.new(
                worker_pool: "default",
                class_name: "Counter",
                object_id: "counter-1",
                method: "balance",
                args: Durababble::Rpc.dump({ "seed" => seed }),
              ),
              nil,
            )
            decoded = Durababble::Rpc::Client.decode_transient_response(response)
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.call_object_transient_ok", decoded:)
          end
          h.scheduler.schedule(actor: "caller", delay: 6, name: "grpc_stale_deliver_message") do
            before = events.length
            h.store.steal_expired_leases!(now: 11)
            h.store.claim_workflow(workflow_id: id, worker_id: "worker-b", lease_seconds: 60)
            service.deliver_message(
              Durababble::Rpc::Proto::DeliverMessageRequest.new(
                worker_pool: "default",
                target_kind: "workflow",
                target_id: id,
              ),
              nil,
            )
            h.scheduler.trace.event(
              h.scheduler.time,
              "grpc",
              events.length == before ? "grpc.deliver_message_stale_ack" : "grpc.deliver_message_stale_work",
            )
          end
          h.check("AwakenBatch was served") { h.scheduler.trace.to_s.include?("grpc.awaken_batch") }
          h.check("EvictLease was served") { h.scheduler.trace.to_s.include?("grpc.evict_lease") }
          h.check("DeliverMessage was served for the active owner") { h.scheduler.trace.to_s.include?("grpc.deliver_message") }
          h.check("CallTransient decoded a workflow response") { h.scheduler.trace.to_s.include?("grpc.call_transient_ok") }
          h.check("CallTransient decoded an object response") do
            h.scheduler.trace.to_s.include?("grpc.call_object_transient_ok")
          end
          h.check("stale DeliverMessage returned without doing work") do
            h.scheduler.trace.to_s.include?("grpc.deliver_message_stale_ack")
          end
        end
      end

      #: (untyped, untyped) { (?) -> untyped } -> untyped
      def workflow_rpc_client(_h, _node_id, &block)
        Object.new.tap do |client|
          client.define_singleton_method(:request) do |command, payload|
            raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

            block.call(payload)
          end
        end
      end

      #: (untyped, untyped, ?faults: untyped) { (?) -> untyped } -> untyped
      def grpc_workflow_rpc_client(h, node_id, faults: [], &block)
        fault_queue = faults.dup
        transport_fault = method(:grpc_transport_fault!)
        workflow_response = method(:grpc_workflow_rpc_response)
        remote_error_response = method(:grpc_remote_error_response)
        handler_for = method(:workflow_rpc_handler)
        Object.new.tap do |client|
          client.define_singleton_method(:request) do |command, payload|
            raise Durababble::WorkflowRpc::UnknownCommand, command unless command == "workflow_rpc"

            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.call_transient", target: node_id)
            server_payload = {
              "workflow_id" => payload.fetch("workflow_id"),
              "expected_worker_id" => node_id,
              "command" => payload.fetch("command"),
              "payload" => payload.fetch("payload", {}),
            }
            fault = fault_queue.shift
            transport_fault.call(h, fault, target: node_id)
            response_context = {
              h:,
              node_id:,
              workflow_id: payload.fetch("workflow_id"),
              handler_for:,
              remote_error_response:,
              handler_block: block,
            }
            response = workflow_response.call(response_context, server_payload)
            if fault == "duplicate_response"
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.duplicate_response", target: node_id)
              workflow_response.call(response_context, server_payload)
            end
            if fault == "response_timeout"
              h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.response_timeout", target: node_id)
              raise Durababble::WorkflowRpc::NodeUnavailable, "#{node_id} response timed out"
            end

            Durababble::Rpc::Client.decode_transient_response(response)
          rescue Durababble::WorkflowRpc::StaleLease
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.decode_moved", target: node_id)
            raise
          rescue Durababble::WorkflowRpc::NoActiveLease
            h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.decode_not_running", target: node_id)
            raise
          end
        end
      end

      #: (untyped, untyped) -> untyped
      def grpc_workflow_rpc_response(context, payload)
        h = context.fetch(:h)
        node_id = context.fetch(:node_id)
        workflow_id = context.fetch(:workflow_id)
        handler_block = context.fetch(:handler_block)
        handler_for = context.fetch(:handler_for)
        remote_error_response = context.fetch(:remote_error_response)

        result = handler_block ? handler_block.call(payload) : handler_for.call(h, node_id).call(payload)
        Durababble::Rpc::Proto::TransientResponse.new(ok: Durababble::Rpc.dump(result))
      rescue Durababble::WorkflowRpc::NoActiveLease
        h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.not_running", target: node_id)
        Durababble::Rpc::Proto::TransientResponse.new(not_running: true)
      rescue Durababble::WorkflowRpc::StaleLease => e
        lease = h.store.current_workflow_lease(workflow_id)
        if lease && lease.fetch("worker_id") != node_id
          h.scheduler.trace.event(
            h.scheduler.time,
            "grpc",
            "grpc.lease_moved",
            from: node_id,
            to: lease.fetch("worker_id"),
          )
          Durababble::Rpc::Proto::TransientResponse.new(
            moved: Durababble::Rpc::Proto::LeaseMoved.new(
              new_node_id: lease.fetch("worker_id"),
              new_rpc_address: "virtual://#{lease.fetch("worker_id")}",
            ),
          )
        else
          remote_error_response.call(e)
        end
      rescue StandardError => e
        remote_error_response.call(e)
      end

      #: (untyped, untyped, target: untyped) -> untyped
      def grpc_transport_fault!(h, fault, target:)
        case fault
        when nil, "success", "response_timeout", "duplicate_response"
          nil
        when "timeout"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.timeout", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} timed out"
        when "deadline_exceeded"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.deadline_exceeded", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} deadline exceeded"
        when "connection_reset"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.rst", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} reset the stream"
        when "eof"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.eof", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} closed the stream"
        when "unavailable"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.unavailable", target:)
          raise Durababble::WorkflowRpc::NodeUnavailable, "#{target} unavailable"
        else
          raise ArgumentError, "unknown gRPC fault #{fault}"
        end
      end

      #: (untyped, untyped, target: untyped, fault: untyped) { (?) -> untyped } -> untyped
      def grpc_faulty_unary(h, method_name, target:, fault:, &block)
        h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.#{method_name}.request", target:, fault:)
        case fault
        when "drop"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.drop", method: method_name, target:)
          :dropped
        when "duplicate"
          h.scheduler.trace.event(h.scheduler.time, "grpc", "grpc.duplicate", method: method_name, target:)
          block.call
          block.call
          :ok
        else
          grpc_transport_fault!(h, fault, target:)
          block.call
          :ok
        end
      end

      #: (untyped, untyped, workflow_id: untyped) -> untyped
      def call_grpc_service_method(service, method_name, workflow_id:)
        case method_name
        when "awaken_batch"
          service.awaken_batch(
            Durababble::Rpc::Proto::AwakenBatchRequest.new(
              worker_pool: "default",
              workflow_ids: [workflow_id],
            ),
            nil,
          )
        when "deliver_message"
          service.deliver_message(
            Durababble::Rpc::Proto::DeliverMessageRequest.new(
              worker_pool: "default",
              target_kind: "workflow",
              target_id: workflow_id,
            ),
            nil,
          )
        when "evict_lease"
          service.evict_lease(
            Durababble::Rpc::Proto::EvictLeaseRequest.new(
              worker_pool: "default",
              target_kind: "workflow",
              target_id: workflow_id,
            ),
            nil,
          )
        else
          raise ArgumentError, "unknown gRPC service method #{method_name}"
        end
      end

      #: (untyped) -> untyped
      def grpc_remote_error_response(error)
        Durababble::Rpc::Proto::TransientResponse.new(
          err: Durababble::Rpc::Proto::RemoteError.new(
            klass: error.class.name,
            message: error.message,
            backtrace: error.backtrace || [],
          ),
        )
      end

      #: (untyped, untyped) { (?) -> untyped } -> untyped
      def workflow_rpc_handler(h, node_id, &handler_block)
        Durababble::WorkflowRpc::Handler.new(store: h.store, node_id:, handlers: {
          "status" => handler_block || ->(payload) { { "node" => node_id, "payload" => payload } },
        })
      end

      #: (untyped) -> untyped
      def workflow_durable_before_claim(seed)
        run(seed, "workflow_durable_before_claim") do |h|
          h.workflows["counter"] = counter_workflow
          h.scheduler.schedule(actor: "client", delay: h.scheduler.rng.int(20), name: "enqueue_then_crash") do
            h.store.enqueue_workflow(name: "counter", input: { "count" => 5 })
          end
          h.add_workers(["worker-a", "worker-b"], ticks: 12)
          h.check("pending workflow eventually completed") { h.store.summary.fetch(:completed_workflows) == 1 }
        end
      end

      #: (untyped) -> untyped
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

      #: (untyped) -> untyped
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

      #: (untyped) -> untyped
      def zombie_workflow_heartbeat_after_expiry(seed)
        run(seed, "zombie_workflow_heartbeat_after_expiry") do |h|
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.claim_workflow(workflow_id: id, worker_id: "zombie", lease_seconds: 10)
          h.scheduler.schedule(actor: "zombie", delay: 20, name: "heartbeat_after_expiry") do
            h.store.heartbeat(workflow_id: id, worker_id: "zombie", lease_seconds: 60)
            if h.store.workflow_owned?(workflow_id: id, worker_id: "zombie")
              h.scheduler.trace.event(h.scheduler.time, "zombie", "zombie_heartbeat_renewed", workflow_id: id)
            else
              h.scheduler.trace.event(h.scheduler.time, "zombie", "zombie_heartbeat_rejected", workflow_id: id)
            end
          end
          h.check("expired heartbeat was rejected") { h.scheduler.trace.to_s.include?("zombie_heartbeat_rejected") }
          h.check("zombie did not regain ownership") { !h.store.workflow_owned?(workflow_id: id, worker_id: "zombie") }
        end
      end

      #: (untyped) -> untyped
      def stale_wait_signal_terminal_workflow(seed)
        run(seed, "stale_wait_signal_terminal_workflow") do |h|
          id = h.store.create_workflow(name: "waiting", input: { "seed" => seed })
          h.store.record_wait(
            workflow_id: id,
            position: 0,
            name: "wait",
            wait_request: Durababble.wait_event("stale:#{seed}", { "seed" => seed }),
          )
          h.store.complete_workflow(id, result: { "done" => true })
          h.scheduler.schedule(actor: "signaler", delay: h.scheduler.rng.int(5), name: "stale_signal") do
            signaled = h.store.signal_event("stale:#{seed}", payload: { "late" => true })
            event = signaled.zero? ? "stale_wait_ignored" : "stale_wait_completed"
            h.scheduler.trace.event(h.scheduler.time, "signaler", event, workflow_id: id, signaled:)
          end
          h.check("stale wait signal was ignored") { h.scheduler.trace.to_s.include?("stale_wait_ignored") }
          h.check("terminal workflow remained completed") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      #: (untyped) -> untyped
      def step_heartbeat_cursor_recovery(seed)
        run(seed, "step_heartbeat_cursor_recovery") do |h|
          attempts = []
          h.workflows["cursor"] = workflow_class("cursor") do
            test_step("download") do |_ctx, heartbeat|
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

      #: (untyped) -> untyped
      def step_retry_policy_recovery(seed)
        run(seed, "step_retry_policy_recovery") do |h|
          attempts = 0
          h.workflows["retry"] = workflow_class("retry") do
            test_step("flaky", retry_policy: { initial_interval: 10, backoff_coefficient: 2, maximum_interval: 15, maximum_attempts: 3 }) do |ctx|
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
          h.check("attempt history records retries") { h.store.step_attempts_for(id).map { |a| a.fetch("status") } == ["failed", "failed", "completed"] }
        end
      end

      #: (untyped) -> untyped
      def store_fault_after_step_completed(seed)
        run(seed, "store_fault_after_step_completed") do |h|
          h.workflows["counter"] = counter_workflow
          h.store.fault_plan.fail_after(:record_step_completed, message: "lost connection after durable step write")
          id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.scheduler.schedule(
            actor: "faulty-worker",
            delay: h.scheduler.rng.int(5),
            name: "fault_after_step_completed",
          ) do
            Durababble::Engine.new(
              store: h.store,
              worker_id: "faulty-worker",
              lease_seconds: 10,
            ).resume(h.workflows.fetch("counter"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "faulty-worker", "store_fault_observed", workflow_id: id)
          end
          h.scheduler.schedule(actor: "reaper", delay: 20, name: "steal") { h.store.steal_expired_leases! }
          h.scheduler.schedule(actor: "recover", delay: 25, name: "resume") do
            Durababble::Engine.new(store: h.store, worker_id: "recover").resume(
              h.workflows.fetch("counter"),
              workflow_id: id,
            )
          end
          h.check("fault was injected after step write") { h.scheduler.trace.to_s.include?("fault.injected") }
          h.check("completed step was not marked failed after store fault") do
            !h.store.step_attempts_for(id).map { |attempt| attempt.fetch("status") }.include?("failed")
          end
          h.check("workflow completed after recovering from durable step write") do
            h.store.workflow(id).fetch("status") == "completed"
          end
        end
      end

      #: (untyped) -> untyped
      def store_fault_after_wait_recorded(seed)
        run(seed, "store_fault_after_wait_recorded") do |h|
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_event("fault-wait:#{ctx.fetch("id")}", ctx) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          h.store.fault_plan.fail_after(:record_wait, message: "lost connection after durable wait write")
          id = h.store.enqueue_workflow(name: "waiting", input: { "id" => seed.to_s })
          h.scheduler.schedule(
            actor: "faulty-worker",
            delay: h.scheduler.rng.int(5),
            name: "fault_after_wait_recorded",
          ) do
            Durababble::Engine.new(
              store: h.store,
              worker_id: "faulty-worker",
              lease_seconds: 10,
            ).resume(h.workflows.fetch("waiting"), workflow_id: id)
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "faulty-worker", "store_fault_observed", workflow_id: id)
          end
          h.scheduler.schedule(actor: "signal", delay: 20, name: "signal") do
            h.store.signal_event("fault-wait:#{seed}", payload: { "ok" => true })
          end
          h.scheduler.schedule(actor: "recover", delay: 25, name: "resume") do
            Durababble::Engine.new(store: h.store, worker_id: "recover").resume(
              h.workflows.fetch("waiting"),
              workflow_id: id,
            )
          end
          h.check("fault was injected after wait write") { h.scheduler.trace.to_s.include?("fault.injected") }
          h.check("workflow completed after recovering from durable wait write") do
            h.store.workflow(id).fetch("status") == "completed"
          end
        end
      end

      #: (untyped) -> untyped
      def store_fault_after_outbox_enqueue(seed)
        run(seed, "store_fault_after_outbox_enqueue") do |h|
          workflow_id = h.store.enqueue_workflow(name: "counter", input: { "count" => seed })
          h.store.fault_plan.fail_after(:enqueue_outbox, message: "lost connection after durable outbox write")
          outbox_id = nil #: untyped
          h.scheduler.schedule(
            actor: "producer",
            delay: h.scheduler.rng.int(5),
            name: "fault_after_outbox_enqueue",
          ) do
            outbox_id = h.store.enqueue_outbox(
              workflow_id:,
              topic: "email",
              payload: { "seed" => seed },
              key: "email:#{seed}",
            )
          rescue InjectedCrash
            h.scheduler.trace.event(h.scheduler.time, "producer", "store_fault_observed", workflow_id:)
            outbox_id = h.store.enqueue_outbox(
              workflow_id:,
              topic: "email",
              payload: { "seed" => seed, "retry" => true },
              key: "email:#{seed}",
            )
          end
          h.scheduler.schedule(actor: "sender", delay: 20, name: "send") do
            message = h.store.claim_outbox(worker_id: "sender", lease_seconds: 10)
            h.store.ack_outbox(message.fetch("id"), worker_id: "sender") if message
          end
          h.check("fault was injected after outbox write") { h.scheduler.trace.to_s.include?("fault.injected") }
          h.check("retry reused the original outbox message") do
            h.store.outbox_message(outbox_id).fetch("payload") == { "seed" => seed }
          end
          h.check("outbox processed once after enqueue fault") { h.store.summary.fetch(:processed_outbox) == 1 }
        end
      end

      #: (untyped) -> untyped
      def duplicate_delivery_signal_and_outbox(seed)
        run(seed, "duplicate_delivery_signal_and_outbox") do |h|
          h.network.duplicate_percent = 100
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_event("dup:#{ctx.fetch("id")}", ctx) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end
          workflow_id = h.store.enqueue_workflow(name: "waiting", input: { "id" => seed.to_s })
          h.scheduler.schedule(actor: "worker", delay: 1, name: "park") do
            Durababble::Engine.new(store: h.store, worker_id: "worker").resume(
              h.workflows.fetch("waiting"),
              workflow_id:,
            )
          end
          h.network.send(source: "client-signal", target: "db", type: "signal", payload: {}) do
            h.store.signal_event("dup:#{seed}", payload: { "ok" => true })
          end
          h.network.send(source: "producer", target: "db", type: "outbox", payload: {}) do
            h.store.enqueue_outbox(
              workflow_id:,
              topic: "email",
              payload: { "seed" => seed },
              key: "dup-email:#{seed}",
            )
          end
          h.scheduler.schedule(actor: "sender", delay: 25, name: "send") do
            message = h.store.claim_outbox(worker_id: "sender", lease_seconds: 10)
            h.store.ack_outbox(message.fetch("id"), worker_id: "sender") if message
          end
          h.scheduler.schedule(actor: "worker", delay: 30, name: "resume") do
            Durababble::Engine.new(store: h.store, worker_id: "worker").resume(
              h.workflows.fetch("waiting"),
              workflow_id:,
            )
          end
          h.check("duplicate network delivery occurred") { h.scheduler.trace.to_s.include?("network.duplicate") }
          h.check("wait completed once despite duplicate signal") do
            h.scheduler.trace.to_s.scan("wait_completed").length == 1
          end
          h.check("outbox message was idempotent despite duplicate producer delivery") do
            h.store.summary.fetch(:processed_outbox) == 1
          end
          h.check("workflow completed after duplicate signal") do
            h.store.workflow(workflow_id).fetch("status") == "completed"
          end
        end
      end

      #: (untyped) -> untyped
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

      #: (untyped) -> untyped
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
          h.check("incomplete step was retried") { h.store.step_attempts_for(id).map { |attempt| attempt.fetch("status") } == ["failed", "completed", "completed"] }
          h.check("workflow completed after retry") { h.store.workflow(id).fetch("status") == "completed" }
        end
      end

      #: (untyped) -> untyped
      def attempt_history_append_only(seed)
        run(seed, "attempt_history_append_only") do |h|
          h.workflows["flaky"] = workflow_class("flaky") do
            test_step("fail") { |_ctx| raise "boom" }
          end
          id = h.store.enqueue_workflow(name: "flaky", input: { "seed" => seed })
          3.times do |i|
            h.scheduler.schedule(actor: "worker-#{i}", delay: i * 20, name: "attempt") { Durababble::Engine.new(store: h.store, worker_id: "worker-#{i}").resume(h.workflows.fetch("flaky"), workflow_id: id) }
          end
          h.check("each retry appended an attempt") { h.store.step_attempts_for(id).length == 3 }
          h.check("attempts are failed terminal records") { h.store.step_attempts_for(id).all? { |a| a.fetch("status") == "failed" } }
        end
      end

      #: (untyped) -> untyped
      def concurrent_signal_once(seed)
        run(seed, "concurrent_signal_once") do |h|
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_event("event:#{ctx.fetch("id")}", ctx) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
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

      #: (untyped) -> untyped
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

      #: (untyped) -> untyped
      def chaos(seed)
        run(seed, "chaos") do |h|
          h.workflows["counter"] = counter_workflow
          h.workflows["waiting"] = workflow_class("waiting") do
            test_step("wait") { |ctx| Durababble.wait_event("event:#{ctx.fetch("id")}", ctx) }
            test_step("done") { |ctx| ctx.merge("done" => true) }
          end

          12.times do |i|
            name = h.scheduler.rng.chance(25) ? "waiting" : "counter"
            input = name == "waiting" ? { "id" => "w#{i}" } : { "count" => i }
            h.network.send(source: "client-#{i}", target: "db", type: "enqueue") { h.store.enqueue_workflow(name:, input:) }
            h.scheduler.schedule(actor: "signal-#{i}", delay: 80 + h.scheduler.rng.int(200), name: "signal") { h.store.signal_event("event:w#{i}", payload: { "signaled" => true }) }
          end
          h.add_workers(["worker-a", "worker-b", "worker-c", "worker-d"], ticks: 30, crash_percent: 15)
          8.times do |i|
            h.scheduler.schedule(actor: "reaper", delay: 60 + i * 50, name: "steal_expired") { h.store.steal_expired_leases!(now: h.scheduler.time + 61) }
          end
        end
      end

      #: () -> untyped
      def counter_workflow
        workflow_class("counter") do
          test_step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
          test_step("double") { |ctx| { "count" => ctx.fetch("count") * 2 } }
        end
      end

      #: (untyped, ?retry_policy: untyped) ?{ (untyped, ?untyped) -> untyped } -> untyped
      def test_step(name, retry_policy: nil, &block)
        nil
      end

      #: (untyped) ?{ (?) -> untyped } -> untyped
      def workflow_class(name, &definition)
        workflow = Class.new(Durababble::Workflow)
        workflow.workflow_name(name)
        workflow.define_method(:execute) do |input|
          instance = self #: as untyped
          instance.class.step_order.reduce(input) { |ctx, method_name| instance.public_send(method_name, ctx) }
        end
        workflow.define_singleton_method(:test_step) do |step_name, retry_policy: nil, &block|
          workflow_class = self #: as untyped
          workflow_class.define_method(step_name) do |ctx|
            instance = self #: as untyped
            if block.arity >= 2
              block.call(ctx, instance.step_context.heartbeat)
            else
              block.call(ctx)
            end
          end
          workflow_class.step(step_name, retry: retry_policy)
        end
        workflow.class_eval(&definition) if definition
        workflow
      end

      #: (untyped, untyped) { (untyped) -> untyped } -> untyped
      def run(seed, scenario, mutation: nil, &block)
        trace = Trace.new
        scheduler = Scheduler.new(seed:, trace:)
        network = VirtualNetwork.new(scheduler:, drop_percent: scenario == "chaos" ? 5 : 0)
        store = VirtualYugabyte.new(scheduler:, mutation:)
        harness = Harness.new(scenario:, seed:, scheduler:, network:, store:)
        trace.event(0, "dst", "begin", scenario:, seed:)
        block.call(harness)
        scheduler.run
        harness.verify!
        trace.event(scheduler.time, "dst", "end", scenario:, seed:)
        trace_s = trace.to_s
        Result.new(scenario:, seed:, trace: trace_s, digest: Digest::SHA256.hexdigest(trace_s), violations: harness.violations, summary: store.summary)
      end
    end

    class Harness
      #: untyped
      attr_reader :scenario, :seed, :scheduler, :network, :store, :workflows, :violations

      #: (scenario: untyped, seed: untyped, scheduler: untyped, network: untyped, store: untyped) -> void
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

      #: (untyped, ticks: untyped, ?crash_percent: untyped) -> untyped
      def add_workers(ids, ticks:, crash_percent: 0)
        ids.each do |id|
          SimWorker.new(id:, scheduler:, network:, store:, workflows:, crash_percent:).start(ticks:)
        end
      end

      #: (untyped) { (?) -> untyped } -> untyped
      def check(description, &block)
        @checks << [description, block]
      end

      #: () -> untyped
      def verify!
        @checks.each do |description, block|
          violations << "check failed: #{description}" unless block.call
        rescue StandardError => e
          violations << "check errored: #{description}: #{e.class}: #{e.message}"
        end

        workflows_state = store.instance_variable_get(:@workflows)
        steps_state = store.instance_variable_get(:@steps)
        attempts_state = store.instance_variable_get(:@attempts)
        outbox_state = store.instance_variable_get(:@outbox)
        workflows_state.each do |id, row|
          if row.fetch("status") == "completed" && row.fetch("locked_by")
            violations << "completed workflow #{id} still locked"
          end
          completed_positions = steps_state[id].values.select { |step| step.fetch("status") == "completed" }.map { |step| step.fetch("position") }
          if completed_positions.uniq.length != completed_positions.length
            violations << "duplicate completed step positions for #{id}"
          end
          attempts_state[id].each do |attempt|
            if ["running", "waiting"].include?(attempt.fetch("status")) && ["completed", "failed"].include?(row.fetch("status"))
              violations << "#{row.fetch("status")} workflow #{id} has #{attempt.fetch("status")} attempt #{attempt.fetch("id")}"
            end
          end
        end
        outbox_state.values.group_by { |message| message.fetch("key") }.each do |key, messages|
          violations << "duplicate outbox key #{key}" if messages.length > 1
        end
      end
    end
  end
end
