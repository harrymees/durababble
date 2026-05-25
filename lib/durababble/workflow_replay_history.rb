# typed: true
# frozen_string_literal: true

module Durababble
  class WorkflowReplayHistory
    TERMINAL_KINDS = ["step_completed", "step_canceled"].freeze

    #: (untyped) -> void
    def initialize(events)
      @scheduled = {}
      @terminal = {}
      @terminal_events = []
      @resolution_index = 0
      events.each { |event| index_event(event) }
      @terminal_events = @terminal.values.sort_by { |event| event.fetch("event_index").to_i }
    end

    #: (untyped) -> bool
    def terminal_recorded?(command_id)
      @terminal.key?(command_id)
    end

    #: (untyped) -> untyped
    def recorded_schedule(command_id)
      @scheduled[command_id]
    end

    #: (untyped, untyped) -> bool
    def recorded_schedule_matches?(command_id, shape)
      recorded_schedule(command_id)&.fetch("payload") == shape
    end

    #: (workflow_id: untyped, command_id: untyped, shape: untyped) -> bool
    def validate_scheduled_shape!(workflow_id:, command_id:, shape:)
      scheduled = recorded_schedule(command_id)
      return false unless scheduled
      return true if scheduled.fetch("payload") == shape

      message = "workflow #{workflow_id} replay reached command #{command_id} #{shape.fetch("name").inspect} " \
        "with a different durable command shape than recorded history"
      raise NonDeterminismError, message
    end

    #: (untyped, step_name: untyped, shape: untyped) -> untyped
    def remember_scheduled(command_id, step_name:, shape:)
      @scheduled[command_id] = {
        "kind" => "step_scheduled",
        "command_id" => command_id,
        "name" => step_name,
        "payload" => shape,
      }
    end

    #: (workflow_id: untyped, next_command_id: untyped) -> void
    def validate_complete!(workflow_id:, next_command_id:)
      extra = @scheduled
        .keys
        .select { |command_id| command_id >= next_command_id }
        .sort
      return if extra.empty?

      rendered = extra.map { |command_id| "#{command_id}:#{@scheduled.fetch(command_id).fetch("name")}" }.join(", ")
      raise NonDeterminismError, "workflow #{workflow_id} replay completed without consuming durable command history: #{rendered}"
    end

    #: (untyped) { (untyped, untyped) -> untyped } -> void
    def deliver_resolutions(futures, &block)
      while @resolution_index < @terminal_events.length
        event = @terminal_events.fetch(@resolution_index)
        command_id = event.fetch("command_id").to_i
        future = futures[command_id]
        break unless future

        @resolution_index += 1
        block.call(event, future)
      end
    end

    #: (untyped) -> untyped
    def next_undeliverable_command_id(futures)
      return if @resolution_index >= @terminal_events.length

      command_id = @terminal_events.fetch(@resolution_index).fetch("command_id").to_i
      command_id unless futures[command_id]
    end

    private

    #: (untyped) -> void
    def index_event(event)
      command_id = event["command_id"]&.to_i
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
