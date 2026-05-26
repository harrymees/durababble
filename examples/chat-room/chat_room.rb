# typed: false
# frozen_string_literal: true

require "json"
require "time"

require_relative "../../lib/durababble"

module ChatRoomExample
  SYSTEM_USER = {
    "user_id" => "system",
    "display_name" => "System",
  }.freeze

  class << self
    def configure(database_url: nil, schema: nil, workflow_worker_pool: "default", object_worker_pool: "default")
      @database_url = database_url
      @schema = schema
      @workflow_worker_pool = workflow_worker_pool
      @object_worker_pool = object_worker_pool
    end

    def reset_configuration!
      @database_url = nil
      @schema = nil
      @workflow_worker_pool = "default"
      @object_worker_pool = "default"
    end

    def workflow_worker_pool
      @workflow_worker_pool || "default"
    end

    def object_worker_pool
      @object_worker_pool || "default"
    end

    def with_store
      if @database_url
        store = Durababble::Store.connect(database_url: @database_url, schema: @schema || Durababble.default_schema)
        return yield store
      end

      yield Durababble.store
    ensure
      store&.close
    end
  end

  # ChatRoom is a Durababble::DurableObject keyed by room name. Each room owns
  # its own list of users, topic, and the durable message log. Every join,
  # leave, topic change, and chat post is an exposed command, so it survives
  # process restarts and gets persisted exactly once per idempotency_key.
  class ChatRoom < Durababble::DurableObject
    object_type "chat_room"

    def initialize_state
      {
        "topic" => "",
        "users" => {},
        "messages" => [],
        "next_message_id" => 1,
      }
    end

    expose_command def join(user_id, display_name)
      user_id = normalize_user_id(user_id)
      display_name = display_name.to_s.strip
      raise ArgumentError, "display_name cannot be empty" if display_name.empty?

      users = current_state.fetch("users").dup
      already = users[user_id]
      users[user_id] = { "display_name" => display_name, "joined_at" => now_iso }
      state = current_state.merge("users" => users)
      unless already
        state = append_message(state, SYSTEM_USER, "#{display_name} joined", "kind" => "join", "user_id" => user_id)
      end
      update_state(state)
      {
        "user_id" => user_id,
        "display_name" => display_name,
        "already_in_room" => !already.nil?,
      }
    end

    expose_command def leave(user_id)
      user_id = normalize_user_id(user_id)
      users = current_state.fetch("users").dup
      user = users.delete(user_id)
      return { "user_id" => user_id, "was_in_room" => false } unless user

      state = current_state.merge("users" => users)
      state = append_message(state, SYSTEM_USER, "#{user.fetch("display_name")} left", "kind" => "leave", "user_id" => user_id)
      update_state(state)
      { "user_id" => user_id, "was_in_room" => true, "display_name" => user.fetch("display_name") }
    end

    expose_command def post_message(user_id, text)
      user_id = normalize_user_id(user_id)
      text = text.to_s.strip
      raise ArgumentError, "message text cannot be empty" if text.empty?

      sender = current_state.fetch("users").fetch(user_id) do
        raise KeyError, "user #{user_id} is not in the room"
      end
      author = { "user_id" => user_id, "display_name" => sender.fetch("display_name") }
      state = append_message(current_state, author, text, "kind" => "chat")
      update_state(state)
      state.fetch("messages").last
    end

    expose_command def post_system_message(text, metadata = {})
      text = text.to_s.strip
      raise ArgumentError, "message text cannot be empty" if text.empty?

      kind = metadata.is_a?(Hash) ? metadata["kind"] || "system" : "system"
      meta = (metadata.is_a?(Hash) ? metadata : {}).merge("kind" => kind)
      state = append_message(current_state, SYSTEM_USER, text, meta)
      update_state(state)
      state.fetch("messages").last
    end

    expose_command def set_topic(user_id, topic)
      user_id = normalize_user_id(user_id)
      topic = topic.to_s.strip
      sender = if user_id == "system"
        SYSTEM_USER
      else
        user = current_state.fetch("users").fetch(user_id) do
          raise KeyError, "user #{user_id} is not in the room"
        end
        { "user_id" => user_id, "display_name" => user.fetch("display_name") }
      end
      state = current_state.merge("topic" => topic)
      announcement = topic.empty? ? "#{sender.fetch("display_name")} cleared the topic" : "#{sender.fetch("display_name")} set the topic to #{topic.inspect}"
      state = append_message(state, SYSTEM_USER, announcement, "kind" => "topic", "user_id" => sender.fetch("user_id"), "topic" => topic)
      update_state(state)
      { "topic" => topic }
    end

    expose def snapshot(since: 0)
      since = Integer(since || 0)
      messages = current_state.fetch("messages")
      visible = since.positive? ? messages.select { |message| message.fetch("id") >= since } : messages
      {
        "topic" => current_state.fetch("topic"),
        "users" => current_state.fetch("users"),
        "messages" => visible,
        "next_message_id" => current_state.fetch("next_message_id"),
      }
    end

    expose def messages_since(since)
      since = Integer(since || 0)
      current_state.fetch("messages").select { |message| message.fetch("id") >= since }
    end

    private

    def append_message(state, author, text, metadata = {})
      next_id = state.fetch("next_message_id")
      message = {
        "id" => next_id,
        "author" => author,
        "text" => text,
        "posted_at" => now_iso,
        "metadata" => metadata,
        "operation_id" => command_context&.idempotency_key,
      }
      state.merge(
        "messages" => state.fetch("messages") + [message],
        "next_message_id" => next_id + 1,
      )
    end

    def normalize_user_id(user_id)
      user_id = user_id.to_s.strip
      raise ArgumentError, "user_id cannot be empty" if user_id.empty?

      user_id
    end

    def now_iso
      Time.now.utc.iso8601
    end
  end

  # ScheduledAnnouncementWorkflow demonstrates durable orchestration on top of
  # the ChatRoom durable object. It records that an announcement was scheduled,
  # optionally sleeps via Durababble.sleep so progress survives process death,
  # then posts the announcement to the room. Each step is a durable RPC against
  # the room object, so the announcement is posted exactly once even if the
  # workflow worker restarts mid-way.
  class ScheduledAnnouncementWorkflow < Durababble::Workflow
    workflow_name "chat-room-scheduled-announcement"

    def execute(input)
      room_id = input.fetch("room").to_s.strip
      raise ArgumentError, "room cannot be empty" if room_id.empty?

      text = input.fetch("text").to_s
      raise ArgumentError, "text cannot be empty" if text.strip.empty?

      delay_seconds = Float(input.fetch("delay_seconds", 0))
      raise ArgumentError, "delay_seconds cannot be negative" if delay_seconds.negative?

      scheduled = record_request(room_id, text, delay_seconds)
      Durababble.sleep(delay_seconds) if delay_seconds.positive?
      posted = post_announcement(room_id, text)
      finalize(room_id, scheduled, posted)
    end

    step def record_request(room_id, text, delay_seconds)
      ChatRoomExample.with_store do |store|
        room = ChatRoomExample::ChatRoom.at(room_id, store:, worker_pool: ChatRoomExample.object_worker_pool)
        preview = delay_seconds.positive? ? "Announcement scheduled in #{format_delay(delay_seconds)}: #{text}" : "Announcement queued: #{text}"
        message = room.post_system_message(
          preview,
          { "kind" => "announcement_scheduled", "delay_seconds" => delay_seconds, "preview_text" => text },
          idempotency_key: step_context.idempotency_key,
        )
        { "scheduled_message_id" => message.fetch("id"), "preview" => preview }
      end
    end

    step def post_announcement(room_id, text)
      ChatRoomExample.with_store do |store|
        room = ChatRoomExample::ChatRoom.at(room_id, store:, worker_pool: ChatRoomExample.object_worker_pool)
        message = room.post_system_message(
          text,
          { "kind" => "announcement" },
          idempotency_key: step_context.idempotency_key,
        )
        { "message_id" => message.fetch("id"), "posted_at" => message.fetch("posted_at"), "text" => message.fetch("text") }
      end
    end

    step def finalize(room_id, scheduled, posted)
      {
        "status" => "finished",
        "room" => room_id,
        "scheduled_message_id" => scheduled.fetch("scheduled_message_id"),
        "announcement_message_id" => posted.fetch("message_id"),
        "posted_at" => posted.fetch("posted_at"),
        "text" => posted.fetch("text"),
      }
    end

    private

    def format_delay(seconds)
      seconds = Float(seconds)
      return "#{seconds.to_i}s" if seconds == seconds.to_i

      "#{seconds}s"
    end
  end
end
