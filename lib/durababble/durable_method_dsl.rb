# typed: true
# frozen_string_literal: true

module Durababble
  module DurableMethodDSL
    #: Hash[Symbol, bool]
    attr_reader :exposed_queries
    #: Hash[Symbol, RetryPolicy]
    attr_reader :exposed_commands
    #: Hash[Symbol, bool]
    attr_reader :exposed_streams

    #: (Class) -> void
    def initialize_durable_method_dsl(subclass)
      subclass.instance_variable_set(:@exposed_queries, {})
      subclass.instance_variable_set(:@exposed_commands, {})
      subclass.instance_variable_set(:@exposed_streams, {})
    end

    #: (Symbol) -> Object?
    def method_added(method_name)
      super
      apply_pending_durable_macro(method_name)
    end

    #: (?Symbol?) -> Symbol?
    def expose(method_name = nil)
      return register_exposed_query(method_name) if method_name

      set_pending_durable_macro(:expose)
    end

    #: (?Symbol?, **Object?) -> Symbol?
    def expose_command(method_name = nil, **options)
      if method_name
        return register_exposed_command(method_name, retry_policy: options.fetch(:retry_policy, options[:retry]))
      end

      set_pending_durable_macro(:expose_command, retry_policy: options.fetch(:retry_policy, options[:retry]))
    end

    #: (?Symbol?) -> Symbol?
    def expose_stream(method_name = nil)
      return register_exposed_stream(method_name) if method_name

      set_pending_durable_macro(:expose_stream)
    end

    private

    #: (Symbol) -> Symbol
    def register_exposed_query(method_name)
      @exposed_queries[method_name.to_sym] = true
      method_name
    end

    #: (Symbol, retry_policy: Object?) -> Symbol
    def register_exposed_command(method_name, retry_policy:)
      policy = retry_policy #: as untyped
      @exposed_commands[method_name.to_sym] = RetryPolicy.from(policy)
      method_name
    end

    #: (Symbol) -> Symbol
    def register_exposed_stream(method_name)
      @exposed_streams[method_name.to_sym] = true
      method_name
    end

    #: (Symbol, **Object?) -> nil
    def set_pending_durable_macro(kind, **options)
      @pending_durable_macro = [kind, options]
      nil
    end

    #: (Symbol) -> bool
    def apply_pending_durable_macro(method_name)
      return false if @__durababble_wrapping

      pending = consume_pending_durable_macro
      return false unless pending

      kind, options = pending
      handle_pending_durable_macro(kind, method_name, options)
      true
    end

    #: () -> [Symbol, Hash[Symbol, Object?]]?
    def consume_pending_durable_macro
      pending = @pending_durable_macro
      @pending_durable_macro = nil if pending
      pending
    end

    #: (Symbol, Symbol, Hash[Symbol, Object?]) -> Symbol?
    def handle_pending_durable_macro(kind, method_name, options)
      case kind
      when :expose
        register_exposed_query(method_name)
      when :expose_command
        register_exposed_command(method_name, retry_policy: options.fetch(:retry_policy, options[:retry]))
      when :expose_stream
        register_exposed_stream(method_name)
      end
    end

    #: (String) -> String
    def underscore(value)
      value.gsub(/([A-Z]+)([A-Z][a-z])/, "\\1_\\2")
        .gsub(/([a-z\d])([A-Z])/, "\\1_\\2")
        .tr("-", "_")
        .downcase
    end
  end
end
