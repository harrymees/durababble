# typed: true
# frozen_string_literal: true

module Durababble
  class WorkflowReplayHistory
    TERMINAL_KINDS = ["step_completed", "step_waiting", "step_canceled"].freeze
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
      blocking_event_indexes.any? { |index| !@consumed_event_indexes[index] }
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
      when *TERMINAL_KINDS
        @terminal[command_id] = event
      end
    end

    #: (Hash[String, Object?]) -> bool
    def workflow_command_event_deliverable?(event)
      event_index = event.fetch("event_index").to_i
      blocking_event_indexes_before(event_index).all? { |index| @consumed_event_indexes[index] }
    end

    #: (Hash[String, Object?]) -> bool
    def workflow_command_event_before?(event)
      return false if @workflow_command_index >= @workflow_command_events.length

      workflow_command = @workflow_command_events.fetch(@workflow_command_index)
      workflow_command.fetch("event_index").to_i < event.fetch("event_index").to_i &&
        workflow_command_event_deliverable?(workflow_command)
    end

    #: (Integer) -> Array[Integer]
    def blocking_event_indexes_before(event_index)
      blocking_event_indexes.select { |index| index < event_index }
    end

    #: () -> Array[Integer]
    def blocking_event_indexes
      (@scheduled.values + @terminal_events).filter_map { |event| event["event_index"]&.to_i }
    end

    #: (Hash[String, Object?]) -> void
    def consume_event!(event)
      event_index = event["event_index"]
      @consumed_event_indexes[event_index.to_i] = true if event_index
    end
  end
end
