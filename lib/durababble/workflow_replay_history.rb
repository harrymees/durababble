# typed: true
# frozen_string_literal: true

module Durababble
  class WorkflowReplayHistory
    TERMINAL_KINDS = ["step_completed", "step_waiting", "step_canceled", "step_failed"].freeze
    WORKFLOW_COMMAND_KINDS = ["workflow_command_completed", "workflow_command_failed"].freeze

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
      raise NonDeterminismError, message
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

    #: (workflow_id: String, next_command_id: Integer) -> void
    def validate_complete!(workflow_id:, next_command_id:)
      extra = @scheduled
        .keys
        .select { |command_id| command_id >= next_command_id }
        .sort
      return if extra.empty?

      rendered = extra.map { |command_id| "#{command_id}:#{@scheduled.fetch(command_id).fetch("name")}" }.join(", ")
      raise NonDeterminismError, "workflow #{workflow_id} replay completed without consuming durable command history: #{rendered}"
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

    private

    #: (Hash[String, Object?]) -> void
    def index_event(event)
      @workflow_command_events << event if WORKFLOW_COMMAND_KINDS.include?(event.fetch("kind"))

      command_id = event["command_id"]&.to_s&.to_i
      return unless command_id

      case event.fetch("kind")
      when "step_scheduled"
        @scheduled[command_id] = event
      when "step_failed"
        @terminal[command_id] = event if terminal_step_failure?(event)
      when *TERMINAL_KINDS
        return if retrying_step_failure?(event)

        @terminal[command_id] = event
      end
    end

    #: (Hash[String, Object?]) -> bool
    def retrying_step_failure?(event)
      return false unless event.fetch("kind") == "step_failed"

      payload = event["payload"]
      payload.is_a?(Hash) && payload["retrying"] == true
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

    #: (Hash[String, Object?]) -> bool
    def terminal_step_failure?(event)
      payload = event["payload"]
      payload.is_a?(Hash) && payload["terminal"] == true
    end
  end
end
