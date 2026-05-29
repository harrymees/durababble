# typed: false
# frozen_string_literal: true

module Durababble
  module Reflection
    # Struct-like write handle over one `Node`, returned to the workflow body.
    # Every mutation is forwarded to the `Hub`, which applies it to the Document
    # and publishes it only when the body is running live (not replaying).
    #
    # Ergonomics mirror the schema:
    #   reflect.title = "Trip planning"      # scalar set
    #   reflect.status                       # scalar read-back
    #   msg = reflect.messages.append(role: "user", content: "Plan a trip")
    #   msg.append(:content, " token")       # incremental token-stream exhaust
    #   msg.content = "final authoritative"  # wholesale set (replay-safe commit)
    #   tc = msg.tool_calls.append(name: "search")
    #
    # Explicit `set`/`append`/`list` are always available; the method_missing
    # sugar above is a thin wrapper that consults the node's `ModelType`.
    class NodeHandle
      #: Node
      attr_reader :node

      #: (Hub, Node) -> void
      def initialize(hub, node)
        @hub = hub
        @node = node
      end

      #: () -> String
      def wire_id
        @node.wire_id
      end

      # Wholesale set of a scalar field. Idempotent end-to-end: re-setting the
      # same value publishes nothing, so this is the safe way to commit a step's
      # authoritative result from replayable body code.
      #: (Symbol, Object?) -> NodeHandle
      def set(field_name, value)
        @hub.set(@node, field_name, value)
        self
      end

      # Appends a chunk to a token-stream field. Live-only exhaust: skipped during
      # replay. Pair with a later `set` to commit the authoritative value.
      #: (Symbol, Object?) -> NodeHandle
      def append(field_name, chunk)
        @hub.append(@node, field_name, chunk)
        self
      end

      #: (Symbol) -> Object?
      def get(field_name)
        @node.fields[field_name.to_sym]
      end

      # Handle onto a nested list (`messages`, `tool_calls`, ...).
      #: (Symbol) -> ListHandle
      def list(list_name)
        ListHandle.new(@hub, @node, list_name.to_sym)
      end

      #: (Symbol, *Object?, **Object?) -> Object?
      def method_missing(name, *args, **kwargs, &block)
        type = @node.model_type
        str = name.to_s
        if str.end_with?("=") && type.signal?(str[0..-2])
          set(str[0..-2].to_sym, args.first)
        elsif type.model?(name)
          list(name)
        elsif type.signal?(name)
          get(name)
        else
          super
        end
      end

      #: (Symbol, ?bool) -> bool
      def respond_to_missing?(name, include_private = false)
        type = @node.model_type
        str = name.to_s
        (str.end_with?("=") && type.signal?(str[0..-2])) ||
          type.model?(name) ||
          type.signal?(name) ||
          super
      end
    end

    # Handle onto a nested list of child structs. `append` adds (or, on replay,
    # rebinds) the next child and returns its `NodeHandle`; indexed access and
    # `each` re-grab existing children so a later body section can keep streaming
    # into a message created earlier.
    class ListHandle
      include Enumerable

      #: (Hub, Node, Symbol) -> void
      def initialize(hub, parent_node, list_name)
        @hub = hub
        @parent = parent_node
        @list_name = list_name
      end

      # Appends the next child, seeding it with `initial_fields` at creation time.
      #: (**Object?) -> NodeHandle
      def append(**initial_fields)
        child = @hub.append_child(@parent, @list_name, initial_fields)
        NodeHandle.new(@hub, child)
      end

      #: () -> Integer
      def size
        @parent.children[@list_name].size
      end
      alias_method :length, :size

      #: (Integer) -> NodeHandle?
      def [](index)
        child = @parent.children[@list_name][index]
        child && NodeHandle.new(@hub, child)
      end

      #: () { (NodeHandle) -> void } -> void
      def each
        return enum_for(:each) unless block_given?

        @parent.children[@list_name].each { |child| yield NodeHandle.new(@hub, child) }
      end
    end
  end
end
