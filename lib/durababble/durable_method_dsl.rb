# typed: true
# frozen_string_literal: true

module Durababble
  module DurableMethodDSL
    #: untyped
    attr_reader :exposed_queries, :exposed_commands

    #: (untyped) -> untyped
    def initialize_durable_method_dsl(subclass)
      subclass.instance_variable_set(:@exposed_queries, {})
      subclass.instance_variable_set(:@exposed_commands, {})
    end

    #: (untyped) -> untyped
    def method_added(method_name)
      super
      apply_pending_durable_macro(method_name)
    end

    #: (?untyped) -> untyped
    def expose(method_name = nil)
      return register_exposed_query(method_name) if method_name

      set_pending_durable_macro(:expose)
    end

    #: (?untyped, **untyped) -> untyped
    def expose_command(method_name = nil, **options)
      if method_name
        return register_exposed_command(method_name, retry_policy: options.fetch(:retry_policy, options[:retry]))
      end

      set_pending_durable_macro(:expose_command, retry_policy: options[:retry])
    end

    private

    #: (untyped) -> untyped
    def register_exposed_query(method_name)
      @exposed_queries[method_name.to_sym] = true
      method_name
    end

    #: (untyped, retry_policy: untyped) -> untyped
    def register_exposed_command(method_name, retry_policy:)
      @exposed_commands[method_name.to_sym] = RetryPolicy.from(retry_policy)
      method_name
    end

    #: (untyped, **untyped) -> untyped
    def set_pending_durable_macro(kind, **options)
      @pending_durable_macro = [kind, options]
      nil
    end

    #: (untyped) -> bool
    def apply_pending_durable_macro(method_name)
      return false if @__durababble_wrapping

      pending = consume_pending_durable_macro
      return false unless pending

      kind, options = pending
      handle_pending_durable_macro(kind, method_name, options)
      true
    end

    #: () -> untyped
    def consume_pending_durable_macro
      pending = @pending_durable_macro
      @pending_durable_macro = nil if pending
      pending
    end

    #: (untyped, untyped, untyped) -> untyped
    def handle_pending_durable_macro(kind, method_name, options)
      case kind
      when :expose
        register_exposed_query(method_name)
      when :expose_command
        register_exposed_command(method_name, retry_policy: options.fetch(:retry_policy, options[:retry]))
      end
    end

    #: (untyped) -> untyped
    def underscore(value)
      value.gsub(/([A-Z]+)([A-Z][a-z])/, "\\1_\\2")
        .gsub(/([a-z\d])([A-Z])/, "\\1_\\2")
        .tr("-", "_")
        .downcase
    end
  end
end
