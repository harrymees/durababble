# typed: false
# frozen_string_literal: true

require_relative "hub"

module Durababble
  module Reflection
    # Client-side reconstruction of the reflected tree. Consumes the same delta
    # frames the `Hub` publishes (snapshot first, then set/append/child) and
    # rebuilds a local mirror of the owner's `Document`. One Mirror per subscribed
    # stream; feed every frame the `ResultStream` yields into `#apply`.
    #
    # The mirror stores nodes in the exact `to_wire` shape the owner emits
    # (`{"id"=>, "fields"=>{}, "children"=>{list=>[node, ...]}}`) and keeps a
    # flat `wire_id -> node` index so every targeted op (`set`/`append`/`child`)
    # is O(1) regardless of tree depth. Reusing the wire shape means a `child`
    # frame's payload can be ingested verbatim.
    #
    # Unknown ids raise rather than silently no-op: a `set` against an id we never
    # ingested means a frame was dropped or arrived out of order, which is a
    # protocol bug worth surfacing loudly in a prototype.
    class Mirror
      #: bool
      attr_reader :ended

      #: () -> void
      def initialize
        @root = nil
        @index = {}
        @ended = false
      end

      # Applies one wire frame. Accepts the END_OF_STREAM sentinel too, so a
      # consumer can pump every value the stream yields straight through without
      # special-casing the terminator.
      #: (Object?) -> void
      def apply(frame)
        return mark_ended if frame == Hub::END_OF_STREAM
        raise ArgumentError, "expected a frame Hash, got #{frame.inspect}" unless frame.is_a?(Hash)

        case frame["t"]
        when "snapshot" then apply_snapshot(frame)
        when "set" then apply_set(frame)
        when "append" then apply_append(frame)
        when "child" then apply_child(frame)
        else raise ArgumentError, "unknown reflection frame type #{frame["t"].inspect}"
        end
      end

      #: () -> Hash[String, Object?]?
      def root
        @root
      end

      # Clean nested view with wire ids stripped: each node collapses to its
      # `fields` merged with its child lists (recursively viewed). Handy for
      # asserting on or printing the reconstructed session without the structural
      # bookkeeping.
      #: () -> Hash[String, Object?]?
      def view
        @root && view_node(@root)
      end

      private

      #: (Hash[String, Object?]) -> void
      def apply_snapshot(frame)
        @index = {}
        @root = frame.fetch("root")
        ingest(@root)
      end

      #: (Hash[String, Object?]) -> void
      def apply_set(frame)
        node = fetch_node(frame.fetch("id"))
        node.fetch("fields")[frame.fetch("field")] = frame.fetch("value")
      end

      #: (Hash[String, Object?]) -> void
      def apply_append(frame)
        node = fetch_node(frame.fetch("id"))
        fields = node.fetch("fields")
        field = frame.fetch("field")
        fields[field] = "#{fields[field]}#{frame.fetch("chunk")}"
      end

      #: (Hash[String, Object?]) -> void
      def apply_child(frame)
        parent = fetch_node(frame.fetch("parent"))
        list = (parent["children"] ||= {})[frame.fetch("list")] ||= []
        node = frame.fetch("node")
        list[frame.fetch("index")] = node
        ingest(node)
      end

      # Registers a node and its whole subtree into the flat index so later
      # targeted frames resolve in O(1). Called for the snapshot root and for
      # every freshly-arrived `child` payload.
      #: (Hash[String, Object?]) -> void
      def ingest(node)
        @index[node.fetch("id")] = node
        node.fetch("children", {}).each_value do |children|
          children.each { |child| ingest(child) }
        end
      end

      #: (String) -> Hash[String, Object?]
      def fetch_node(wire_id)
        @index.fetch(wire_id) do
          raise KeyError, "reflection frame targets unknown node #{wire_id.inspect}"
        end
      end

      #: (Hash[String, Object?]) -> Hash[String, Object?]
      def view_node(node)
        result = node.fetch("fields").dup
        node.fetch("children", {}).each do |list_name, children|
          result[list_name] = children.map { |child| view_node(child) }
        end
        result
      end

      #: () -> void
      def mark_ended
        @ended = true
      end
    end
  end
end
