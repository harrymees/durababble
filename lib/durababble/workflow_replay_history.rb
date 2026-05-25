# typed: true
# frozen_string_literal: true

module Durababble
  class WorkflowReplayHistory
    TERMINAL_KINDS = ["step_completed", "step_waiting", "step_canceled"].freeze

    #: (Array[Hash[String, Object?]]) -> void
    def initialize(events)
      @scheduled = {}
      @terminal = {}
      @terminal_events = []
      @resolution_index = 0
      events.each { |event| index_event(event) }
      @terminal_events = @terminal.values.sort_by { |event| event.fetch("event_index").to_i }
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
      return true if scheduled.fetch("payload") == shape

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
        command_id = event.fetch("command_id").to_s.to_i
        future = futures[command_id]
        break unless future

        @resolution_index += 1
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
      command_id = event["command_id"]&.to_s&.to_i
      return unless command_id

      case event.fetch("kind")
      when "step_scheduled"
        @scheduled[command_id] = event
      when *TERMINAL_KINDS
        @terminal[command_id] = event
      end
    end
  end
end
