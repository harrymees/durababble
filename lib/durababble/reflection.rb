# typed: false
# frozen_string_literal: true

require "async"

require_relative "reflection/schema"
require_relative "reflection/document"
require_relative "reflection/hub"
require_relative "reflection/reflector"
require_relative "reflection/mirror"

module Durababble
  # Reflection ("data exhaust"): a workflow declares the *shape* it streams to
  # clients with `reflect do … end`, mutates that shape from its orchestration
  # body via the `reflect` handle, and any number of clients tail the live tree
  # over the ordinary `expose_stream` machinery — no engine or StreamDispatcher
  # changes. The owner node holds one `Hub`/`Document` per `{workflow, id}`; the
  # auto-exposed `__durababble_reflection__` stream subscribes to it and forwards
  # snapshot + delta frames down a `ResultStream` to each consumer's `Mirror`.
  #
  # See `Hub` for the replay/publish gate and `Document` for the wire codec.
  module Reflection
    # Stream method `reflect` auto-exposes. Routed like any other workflow stream:
    # `WorkflowRef#open_workflow_stream` → lease holder → `StreamDispatcher`.
    STREAM_METHOD = :__durababble_reflection__

    # Idle poll cadence for the subscriber loop: how often a parked consumer
    # wakes to re-check `stream_cancelled?` when no frames are flowing. A pushed
    # frame or END_OF_STREAM unblocks immediately; this only bounds how long an
    # otherwise-silent stream takes to notice the consumer went away.
    POLL_INTERVAL = 0.5 #: Float

    # Around-each-activation wrapper prepended onto every `reflect`-ing workflow
    # subclass. The orchestration body re-runs on every activation, so before it
    # runs we rebind the replay predicate and reset list cursors
    # (`begin_activation`); we close the hub only when the body returns normally,
    # which happens exactly once — at completion. Suspension, step retry, and
    # terminal failure all leave `execute` via an exception that propagates
    # untouched, so the hub (and its subscribers) survive across activations and
    # only a true completion ends their streams.
    module Hosting
      #: (*Object?, **Object?) ?{ () -> Object? } -> Object?
      def execute(*args, **kwargs, &block)
        hub = __durababble_reflection_hub__
        execution = WorkflowExecutionContext.current
        hub.begin_activation(replaying: execution ? -> { execution.replaying? } : nil)
        result = super
        hub.close
        result
      end
    end

    # Blocking dequeue with a ceiling so the subscriber loop can poll
    # `stream_cancelled?`. Mirrors the worker runtime's wakeup-queue pattern.
    #: (untyped, Numeric) -> Object?
    def self.dequeue_with_timeout(queue, timeout)
      task = Async::Task.current
      task.with_timeout(timeout) { queue.dequeue }
    rescue Async::TimeoutError
      nil
    end
  end

  class Workflow
    class << self
      # DSL: declare the reflected shape and opt this workflow into reflection.
      # Stores the compiled schema on the subclass, auto-exposes the reflection
      # stream, and prepends the per-activation hosting wrapper exactly once.
      #: () { () -> void } -> void
      def reflect(&block)
        @__durababble_reflection_schema = Reflection::Schema.build(&block)
        expose_stream(Reflection::STREAM_METHOD)
        prepend(Reflection::Hosting) unless self < Reflection::Hosting
      end

      #: () -> Reflection::Schema?
      def __durababble_reflection_schema
        @__durababble_reflection_schema
      end
    end

    # Live write handle over this session's reflected tree, rooted at the hub's
    # root node. Use from the orchestration body:
    #
    #   reflect.title = "Trip planning"
    #   msg = reflect.messages.append(role: "user", content: "Plan a trip")
    #
    # Every mutation is replay-gated by the hosting wrapper, so the same body
    # code is safe to re-run on every activation.
    #: () -> Reflection::NodeHandle
    def reflect
      hub = __durababble_reflection_hub__
      Reflection::NodeHandle.new(hub, hub.root)
    end

    # Resolves (or lazily creates) this session's `Hub` from the process-global
    # Registry. The orchestration body and the stream dispatcher both land here
    # with the same `{workflow_name, workflow_id}`, so they share one Hub with no
    # explicit wiring.
    #: () -> Reflection::Hub
    def __durababble_reflection_hub__
      schema = self.class.__durababble_reflection_schema
      raise Error, "#{self.class} did not declare a reflect schema" unless schema

      Reflection::Registry.fetch_or_create(
        type: self.class.workflow_name,
        id: __durababble_reflection_workflow_id__,
        schema:,
      )
    end

    # Auto-exposed reflection stream. Atomically captures the current state as
    # hydration frames and registers a subscriber queue, emits the snapshot, then
    # forwards every delta until the session ends (END_OF_STREAM) or the consumer
    # disconnects (`stream_cancelled?`). Always unsubscribes on the way out.
    #: () { (Object?) -> void } -> void
    def __durababble_reflection__
      hub = __durababble_reflection_hub__
      snapshot, queue = hub.subscribe
      begin
        snapshot.each { |frame| yield frame }
        until Durababble.stream_cancelled?
          frame = Reflection.dequeue_with_timeout(queue, Reflection::POLL_INTERVAL)
          next unless frame
          break if frame == Reflection::Hub::END_OF_STREAM

          yield frame
        end
      ensure
        hub.unsubscribe(queue)
      end
    end

    # Workflow id for hub keying. Inside the orchestration body the execution
    # context supplies it; on a dispatcher- or snapshot-built stream instance it
    # comes from the ref ivar the caller set.
    #: () -> String
    def __durababble_reflection_workflow_id__
      execution = WorkflowExecutionContext.current
      return execution.workflow_id if execution

      id = instance_variable_defined?(:@__durababble_ref_workflow_id) ? @__durababble_ref_workflow_id : nil
      raise Error, "reflection requires a workflow id" if id.nil? || id.to_s.empty?

      id
    end
  end
end
