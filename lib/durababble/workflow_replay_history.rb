# typed: true
# frozen_string_literal: true

require "time"

require_relative "durable_time"

module Durababble
  class WorkflowReplayHistory
    TERMINAL_KINDS = ["step_completed", "step_waiting", "step_canceled", "step_failed"].freeze
    WORKFLOW_COMMAND_KINDS = ["workflow_command_completed", "workflow_command_failed"].freeze

    class << self
      # The next physical event_index to append after the given history: one past
      # the highest recorded index, or 0 when empty. The lease holder allocates
      # from here so appends are a single plain insert; hot-path reports and
      # query-count tests call it to mirror that allocation exactly.
      #
      # workflow_history_for loads rows ORDER BY event_index, so the last indexed
      # row holds the highest index; scan back to it instead of over the whole
      # array. In production every row carries an index, so the first step finds it
      # (O(1)); the loop only walks past trailing rows in the legacy/synthetic case
      # where some events lack a persisted index. This is deliberately last+1, not
      # events.length: a failed append still advances the in-memory counter (see
      # allocate_event_index!), so a committed append can leave a hole in the
      # persisted indexes (e.g. a step's completion write fails transiently and a
      # failure row commits at the next index instead). length would then return an
      # index that already exists and collide on the (workflow_id, event_index) PK
      # after reload.
      #: (Array[Hash[String, Object?]]) -> Integer
      def next_event_index_after(events)
        events.reverse_each do |event|
          index = event["event_index"]
          return index.to_s.to_i + 1 if index
        end
        0
      end
    end

    #: Integer
    attr_reader :event_count

    #: (Array[Hash[String, Object?]]) -> void
    def initialize(events)
      @event_count = events.length
      @scheduled = {}
      @terminal = {}
      @terminal_events = []
      @workflow_command_events = []
      @workflow_command_index = 0
      @consumed_event_indexes = {}
      @resolution_index = 0
      events.each { |event| index_event(event) }
      # Distinct from @event_count, which is a coarse size budget (it reserves
      # ahead and counts remembered-but-unwritten events); the physical PK
      # sequence must come from here, advanced only by allocate_event_index!.
      @next_event_index = self.class.next_event_index_after(events)
      @terminal_events = @terminal.values.sort_by { |event| event.fetch("event_index").to_i }
      @workflow_command_events.sort_by! { |event| event.fetch("event_index").to_i }
      # Scheduled events remembered mid-replay carry no "event_index", so the set of
      # blocking indexes is fixed by the recorded history and can be computed once.
      @blocking_event_indexes = (@scheduled.values + @terminal_events)
        .filter_map { |event| event["event_index"]&.to_i }
        .sort
      # Blocking indexes are consumed monotonically as replay advances, so a single
      # cursor over the sorted array answers "still blocked?" in amortized O(1)
      # instead of rescanning the whole array on every safe point. The cursor marks
      # the first index not yet known to be consumed; everything before it is.
      @blocking_cursor = 0
    end

    #: (Integer) -> bool
    def terminal_recorded?(command_id)
      @terminal.key?(command_id)
    end

    #: (Integer) -> Hash[String, Object?]?
    def recorded_schedule(command_id)
      @scheduled[command_id]
    end

    #: (Integer, Hash[String, Object?]) -> bool
    def recorded_schedule_matches?(command_id, shape)
      recorded_schedule(command_id)&.fetch("payload") == shape
    end

    #: (workflow_id: String, command_id: Integer, shape: Hash[String, Object?]) -> bool
    def validate_scheduled_shape!(workflow_id:, command_id:, shape:)
      scheduled = recorded_schedule(command_id)
      return false unless scheduled

      if scheduled.fetch("payload") == shape
        consume_event!(scheduled)
        return true
      end

      message = "workflow #{workflow_id} replay reached command #{command_id} #{shape.fetch("name").inspect} " \
        "with a different durable command shape than recorded history"
      raise ReplayDivergenceError, message
    end

    #: (Integer, step_name: String, shape: Hash[String, Object?]) -> Hash[String, Object?]
    def remember_scheduled(command_id, step_name:, shape:)
      @scheduled[command_id] = {
        "kind" => "step_scheduled",
        "command_id" => command_id,
        "name" => step_name,
        "payload" => shape,
      }
      @event_count += 1
    end

    #: () { (Hash[String, Object?]) -> Object? } -> Integer
    def deliver_workflow_commands(&block)
      delivered = 0
      while @workflow_command_index < @workflow_command_events.length
        event = @workflow_command_events.fetch(@workflow_command_index)
        break unless workflow_command_event_deliverable?(event)

        @workflow_command_index += 1
        consume_event!(event)
        block.call(event)
        delivered += 1
      end
      delivered
    end

    #: () -> bool
    def blocked_recorded_workflow_command?
      return false if @workflow_command_index >= @workflow_command_events.length

      !workflow_command_event_deliverable?(@workflow_command_events.fetch(@workflow_command_index))
    end

    #: () -> bool
    def blocked_by_replay_history?
      advance_blocking_cursor!
      @blocking_cursor < @blocking_event_indexes.length
    end

    #: (Integer) -> void
    def reserve_events!(count)
      @event_count += count
    end

    # Hand out the next physical event_index for a workflow_history append and
    # advance the counter. Callers must invoke this inside synchronize_store so
    # allocation order matches the order appends hit the store, mirroring the
    # legacy SQL MAX(event_index)+1 path it replaces. A rolled-back or skipped
    # append leaves a harmless gap: replay orders by event_index and relies on
    # PK uniqueness, not contiguity.
    #: () -> Integer
    def allocate_event_index!
      index = @next_event_index
      @next_event_index += 1
      index
    end

    #: (workflow_id: String, next_command_id: Integer) -> void
    def validate_complete!(workflow_id:, next_command_id:)
      extra = @scheduled
        .keys
        .select { |command_id| command_id >= next_command_id }
        .sort
      return if extra.empty?

      rendered = extra.map { |command_id| "#{command_id}:#{@scheduled.fetch(command_id).fetch("name")}" }.join(", ")
      raise ReplayDivergenceError, "workflow #{workflow_id} replay completed without consuming durable command history: #{rendered}"
    end

    #: (Hash[Integer, CommandFuture]) { (Hash[String, Object?], CommandFuture) -> Object? } -> void
    def deliver_resolutions(futures, &block)
      while @resolution_index < @terminal_events.length
        event = @terminal_events.fetch(@resolution_index)
        break if workflow_command_event_before?(event)

        command_id = event.fetch("command_id").to_s.to_i
        future = futures[command_id]
        break unless future

        @resolution_index += 1
        consume_event!(event)
        block.call(event, future)
      end
    end

    #: (Hash[Integer, CommandFuture]) -> Integer?
    def next_undeliverable_command_id(futures)
      return if @resolution_index >= @terminal_events.length

      command_id = @terminal_events.fetch(@resolution_index).fetch("command_id").to_s.to_i
      command_id unless futures[command_id]
    end

    #: (Integer) -> wait_metadata?
    def waiting_timer(command_id)
      event = @terminal[command_id]
      return unless event&.fetch("kind") == "step_waiting"
      return if interrupted_wait_condition?(event)

      wait = waiting_event_payload(event)
      return unless wait.fetch("kind", nil) == "timer"

      wait
    end

    #: (Integer) -> wait_metadata?
    def waiting_timer_or_child_workflow(command_id)
      event = @terminal[command_id]
      return unless event&.fetch("kind") == "step_waiting"
      return if interrupted_wait_condition?(event)

      wait = waiting_event_payload(event)
      return unless ["timer", "child_workflow"].include?(wait.fetch("kind", nil))

      wait
    end

    #: () -> Object?
    def earliest_unresolved_timer_wake_at
      @terminal.keys.filter_map { |command_id| waiting_timer_or_child_workflow(command_id)&.fetch("wake_at", nil) }.min_by { |wake_at| comparable_time(wake_at) }
    end

    #: (Integer, name: String, wait_request: WaitRequest, ?event_index: Integer?) -> void
    def remember_step_waiting(command_id, name:, wait_request:, event_index: nil)
      @terminal[command_id] = {
        "kind" => "step_waiting",
        "command_id" => command_id,
        "name" => name,
        "event_index" => event_index,
        "payload" => step_waiting_payload(wait_request),
      }
    end

    #: (Integer, payload: Object?, ?reserved_history_event: bool) -> void
    def remember_step_completed(command_id, payload:, reserved_history_event: false)
      @terminal[command_id] = {
        "kind" => "step_completed",
        "command_id" => command_id,
        "payload" => payload,
      }
      return if reserved_history_event

      @event_count += 1
    end

    #: (Integer) -> void
    def forget_waiting_timer(command_id)
      event = @terminal[command_id]
      return unless event&.fetch("kind") == "step_waiting"

      @terminal.delete(command_id)
    end

    private

    #: (Object) -> Object
    def comparable_time(value)
      DurableTime.comparable(value)
    end

    #: (Hash[String, Object?]) -> void
    def index_event(event)
      @workflow_command_events << event if WORKFLOW_COMMAND_KINDS.include?(event.fetch("kind"))

      command_id = event["command_id"]&.to_s&.to_i
      return unless command_id

      case event.fetch("kind")
      when "step_scheduled"
        raise_duplicate_history!("step_scheduled", command_id) if @scheduled.key?(command_id)

        @scheduled[command_id] = event
      when *TERMINAL_KINDS
        # A step_failed event is terminal unless it explicitly carries a
        # "retrying" payload. Replay must not re-run the side effect of a
        # non-retrying failure; the workflow code is expected to observe it via
        # the exception path.
        return if retrying_step_failure?(event)

        existing = @terminal[command_id]
        raise_duplicate_history!("terminal", command_id) if duplicate_terminal_history?(existing, event)

        @terminal[command_id] = event
      end
    end

    #: (String, Integer) -> void
    def raise_duplicate_history!(kind, command_id)
      raise NonDeterminismError, "workflow replay history contains duplicate #{kind} history for command #{command_id}"
    end

    #: (Hash[String, Object?]?, Hash[String, Object?]) -> bool
    def duplicate_terminal_history?(existing, event)
      return false unless existing

      existing.fetch("kind") != "step_waiting" || event.fetch("kind") == "step_waiting"
    end

    #: (Hash[String, Object?]) -> bool
    def retrying_step_failure?(event)
      return false unless event.fetch("kind") == "step_failed"

      payload = event["payload"]
      payload.is_a?(Hash) && payload["retrying"] == true
    end

    #: (wait_history_event) -> wait_metadata
    def waiting_event_payload(event)
      payload = event.fetch("payload")
      if payload.is_a?(Hash) && payload["wait"].is_a?(Hash)
        wait = payload.fetch("wait") #: as untyped
        return {
          "kind" => wait["kind"],
          "event_key" => wait["event_key"],
          "wake_at" => wait["wake_at"],
          "context" => payload.fetch("context", wait.fetch("context", {})),
        }
      end

      command_id = event.fetch("command_id").to_s.to_i
      schedule_payload = recorded_schedule(command_id)&.fetch("payload", nil)
      schedule_wait = schedule_payload["wait"] if schedule_payload.is_a?(Hash)
      schedule_wait = schedule_wait #: as untyped
      {
        "kind" => schedule_wait&.fetch("kind", nil),
        "event_key" => schedule_wait&.fetch("event_key", nil),
        "wake_at" => schedule_wait&.fetch("wake_at", nil),
        "context" => payload,
      }
    end

    #: (Hash[String, Object?]) -> bool
    def interrupted_wait_condition?(event)
      schedule = recorded_schedule(event.fetch("command_id").to_s.to_i)
      return false unless schedule&.fetch("name", nil).to_s == "wait_condition"

      event_index = event["event_index"]
      return false unless event_index

      @workflow_command_events.any? { |workflow_command| workflow_command.fetch("event_index").to_s.to_i > event_index.to_s.to_i }
    end

    #: (WaitRequest) -> wait_event_payload
    def step_waiting_payload(wait_request)
      {
        "context" => wait_request.context,
        "wait" => {
          "kind" => wait_request.kind,
          "event_key" => wait_request.event_key,
          "wake_at" => wait_request.wake_at,
        },
      }
    end

    #: (Hash[String, Object?]) -> bool
    def workflow_command_event_deliverable?(event)
      event_index = event.fetch("event_index").to_s.to_i
      advance_blocking_cursor!
      # Every blocking index below event_index is consumed exactly when the first
      # unconsumed blocking index (the cursor) is at or past event_index. The array
      # is sorted, so this is the same answer the old per-call select produced.
      @blocking_cursor >= @blocking_event_indexes.length || @blocking_event_indexes.fetch(@blocking_cursor) >= event_index
    end

    #: (Hash[String, Object?]) -> bool
    def workflow_command_event_before?(event)
      return false if @workflow_command_index >= @workflow_command_events.length

      workflow_command = @workflow_command_events.fetch(@workflow_command_index)
      workflow_command_event_index = workflow_command.fetch("event_index").to_s.to_i
      event_index = event.fetch("event_index").to_s.to_i
      return false if workflow_command_event_index >= event_index

      workflow_command_event_deliverable?(workflow_command)
    end

    # Advance the cursor past every blocking index already consumed. Consumption is
    # monotonic, so the cursor only moves forward and the total work across a whole
    # replay is O(blocking indexes) rather than O(blocking indexes) per safe point.
    #: () -> void
    def advance_blocking_cursor!
      indexes = @blocking_event_indexes
      cursor = @blocking_cursor
      cursor += 1 while cursor < indexes.length && @consumed_event_indexes[indexes.fetch(cursor)]
      @blocking_cursor = cursor
    end

    #: (Hash[String, Object?]) -> void
    def consume_event!(event)
      event_index = event["event_index"]
      @consumed_event_indexes[event_index.to_s.to_i] = true if event_index
    end
  end
end
