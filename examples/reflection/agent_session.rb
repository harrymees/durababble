# typed: false
# frozen_string_literal: true

require "json"
require_relative "../../lib/durababble"

# Reflection ("data exhaust") end-to-end demo.
#
# A `reflect`-ing workflow owns the live shape of a multi-turn agent session and
# mutates it from its orchestration body. Any number of clients tail that tree
# over the ordinary streaming-RPC machinery — here the *real* `StreamDispatcher`
# and *real* `ResultStream`, with no engine or dispatcher changes. The nested
# schema (AgentSession -> messages[] -> tool_calls[]) is what lets a multi-turn
# session be represented faithfully.
#
# This script needs no database: reflection is in-process (Hub/Registry/Document
# /Mirror). The dispatcher only reads the workflow lease, which a tiny fake store
# supplies. Run it with:  ruby examples/reflection/agent_session.rb

NODE_ID = "owner-node-1"
WORKFLOW_ID = "session-42"

# Non-blocking sleep on the reactor. The body runs as an Async fiber, so we must
# not call `Workflow#sleep` (that schedules a *durable* sleep and needs an engine
# execution context); the small pauses below only space out token frames so the
# consumer fiber interleaves and you can watch the tree grow live.
def reactor_pause(seconds)
  Async::Task.current&.sleep(seconds)
end

# The reflected workflow. `reflect do … end` declares the streamed shape and opts
# the class in; the body mutates that shape through the `reflect` handle.
class AgentSession < Durababble::Workflow
  workflow_name "agent_session"

  reflect do
    signal :title
    signal :status
    model :messages do
      signal :role
      signal :content, delta: :append # incremental LLM token stream
      model :tool_calls do
        signal :name
        signal :arguments, delta: :append
        signal :result
      end
    end
  end

  # Orchestration body. In a real workflow each `step` would do the LLM/tool work
  # and the reflect ops would be interleaved with it; here we simulate two turns
  # of an agent planning a trip, streaming assistant tokens and a tool call.
  def execute(input)
    reflect.title = "Trip planning: #{input.fetch("destination")}"
    reflect.status = "running"

    user_turn("Plan a 3-day trip to #{input.fetch("destination")}.")
    assistant = assistant_turn
    stream_tokens(assistant, "Sure — let me look up some options for you. ")

    call = assistant.tool_calls.append(name: "search_attractions", arguments: "", result: "")
    stream_tokens_into(call, :arguments, %({"city": "#{input.fetch("destination")}", "days": 3}))
    call.result = "Fushimi Inari, Arashiyama, Gion"

    stream_tokens(assistant, "Here's a draft itinerary built around those. ")
    # Authoritative wholesale set: commits the final content (replay-safe).
    assistant.content = "Sure — here is a 3-day itinerary: Day 1 Fushimi Inari, " \
      "Day 2 Arashiyama, Day 3 Gion."

    user_turn("Can you add a food recommendation for day 2?")
    follow_up = assistant_turn
    stream_tokens(follow_up, "For day 2 in Arashiyama, try yudofu near Tenryu-ji.")

    reflect.status = "complete"
  end

  private

  def user_turn(content)
    reflect.messages.append(role: "user", content:)
  end

  def assistant_turn
    reflect.messages.append(role: "assistant", content: "")
  end

  def stream_tokens(message, text)
    stream_tokens_into(message, :content, text)
  end

  def stream_tokens_into(handle, field, text)
    text.scan(/\S+\s*/) do |token|
      handle.append(field, token)
      reactor_pause(0.01)
    end
  end
end

# Minimal store the dispatcher needs: it asserts this node owns the workflow
# lease before and during the stream. Reflection touches nothing else on it.
class FakeStore
  def initialize(node_id)
    @node_id = node_id
  end

  def current_workflow_lease(_workflow_id)
    { "worker_id" => @node_id }
  end
end

# Opens the reflection stream the way a remote client would: a `TransientRequest`
# routed through the real dispatcher, wrapped in a real `ResultStream`. Each
# yielded frame is applied to a fresh `Mirror`, reconstructing the owner's tree.
def tail_session(dispatcher, label:)
  request = Durababble::Rpc::Messages::TransientRequest.new(
    class_name: "agent_session",
    workflow_id: WORKFLOW_ID,
    method: Durababble::Reflection::STREAM_METHOD.to_s,
    expected_worker_id: NODE_ID,
  )
  mirror = Durababble::Reflection::Mirror.new
  stream = Durababble::ResultStream.new do |writer|
    dispatcher.call(request:, args: { "args" => [], "kwargs" => {} }, writer:)
  end

  frame_count = 0
  stream.each do |frame|
    frame_count += 1
    puts "  [#{label} frame #{frame_count}] #{describe_frame(frame)}"
    mirror.apply(frame)
  end
  mirror
end

def describe_frame(frame)
  case frame["t"]
  when "snapshot" then "snapshot (root id=#{frame.fetch("root").fetch("id")})"
  when "set" then "set    #{frame.fetch("id")}.#{frame.fetch("field")} = #{frame.fetch("value").inspect}"
  when "append" then "append #{frame.fetch("id")}.#{frame.fetch("field")} << #{frame.fetch("chunk").inspect}"
  when "child" then "child  #{frame.fetch("parent")}.#{frame.fetch("list")}[#{frame.fetch("index")}] = #{frame.fetch("node").fetch("id")}"
  else frame.inspect
  end
end

# Fresh process-global hub directory so reruns don't see a stale session.
Durababble::Reflection::Registry.reset!

store = FakeStore.new(NODE_ID)
dispatcher = Durababble::StreamDispatcher.new(
  store:,
  workflows: [AgentSession],
  objects: [],
  node_id: NODE_ID,
)

live_view = nil
late_view = nil

Sync do |task|
  # Owner side: run the orchestration body. The prepended hosting wrapper resolves
  # the hub, gates replay, and closes the hub (ending every subscriber's stream)
  # when the body returns. The brief head start lets the live tailer subscribe
  # first so it observes deltas as they happen rather than a single snapshot.
  body = task.async do
    reactor_pause(0.05)
    session = AgentSession.new
    session.instance_variable_set(:@__durababble_ref_workflow_id, WORKFLOW_ID)
    session.execute("destination" => "Kyoto")
  end

  puts "== live client (subscribes before/while the body runs) =="
  live_view = tail_session(dispatcher, label: "live").view
  body.wait

  # Late client: the session already completed, so it gets a hydration snapshot of
  # the final tree plus an immediate end-of-stream — same reconstruction, no deltas.
  puts "\n== late client (subscribes after completion) =="
  late_view = tail_session(dispatcher, label: "late").view
end

puts "\n== reconstructed session (live client) =="
puts JSON.pretty_generate(live_view)

if live_view == late_view
  puts "\nOK: live-delta and late-snapshot reconstructions are identical."
else
  warn "\nMISMATCH between live and late reconstructions!"
  warn JSON.pretty_generate(late_view)
  exit 1
end
