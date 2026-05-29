# typed: false
# frozen_string_literal: true

module Durababble
  module Reflection
    # Declarative description of the *shape* a workflow reflects onto clients.
    #
    # A schema is a tree of `ModelType`s. The root models the session itself
    # (scalar fields like `title`/`status`); nested `model` declarations are
    # ordered *lists* of child structs (e.g. `messages`, and within each message
    # `tool_calls`). That recursion is what lets a multi-turn agent session be
    # represented faithfully: AgentSession -> messages[] -> tool_calls[].
    #
    #   reflect do
    #     signal :title
    #     signal :status
    #     model :messages do
    #       signal :role
    #       signal :content, delta: :append   # token-stream field
    #       model :tool_calls do
    #         signal :name
    #         signal :arguments, delta: :append
    #         signal :result
    #       end
    #     end
    #   end
    #
    # `delta: :append` marks a scalar that is produced incrementally (LLM tokens)
    # and may be appended to as live "exhaust"; a plain signal is set wholesale.
    class Schema
      #: (ModelType) -> void
      def initialize(root)
        @root = root
      end

      #: ModelType
      attr_reader :root

      #: () { () -> void } -> Schema
      def self.build(&block)
        root = ModelType.new(name: nil)
        root.instance_eval(&block) if block
        new(root)
      end
    end

    # Describes a single scalar field on a model. `delta` is `nil` (set
    # wholesale) or `:append` (incrementally appended token-stream).
    SignalDef = Struct.new(:name, :delta, keyword_init: true) do
      #: () -> bool
      def append?
        delta == :append
      end
    end

    # One struct shape in the tree: a set of scalar `signals` plus nested list
    # `models`. The root has `name: nil`; nested models carry their list name.
    class ModelType
      #: Symbol?
      attr_reader :name
      #: Hash[Symbol, SignalDef]
      attr_reader :signals
      #: Hash[Symbol, ModelType]
      attr_reader :models

      #: (name: Symbol?) -> void
      def initialize(name:)
        @name = name
        @signals = {}
        @models = {}
      end

      # DSL: declare a scalar field. `delta: :append` opts the field into the
      # incremental token-stream path; otherwise it is set wholesale.
      #: (Symbol, ?delta: Symbol?) -> void
      def signal(field_name, delta: nil)
        @signals[field_name.to_sym] = SignalDef.new(name: field_name.to_sym, delta:)
      end

      # DSL: declare a nested ordered list of child structs.
      #: (Symbol) { () -> void } -> void
      def model(list_name, &block)
        child = ModelType.new(name: list_name.to_sym)
        child.instance_eval(&block) if block
        @models[list_name.to_sym] = child
      end

      #: (Symbol) -> bool
      def signal?(field_name)
        @signals.key?(field_name.to_sym)
      end

      #: (Symbol) -> bool
      def model?(list_name)
        @models.key?(list_name.to_sym)
      end
    end
  end
end
