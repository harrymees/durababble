# typed: true
# frozen_string_literal: true

module Durababble
  module AsyncDispatch
    class << self
      #: (untyped, untyped, untyped, untyped) -> untyped
      def call(workflow, method_name, args, kwargs)
        if kwargs.empty?
          workflow.send(method_name, *args)
        else
          workflow.send(method_name, *args, **kwargs)
        end
      end
    end
  end
end
