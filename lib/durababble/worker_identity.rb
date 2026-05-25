# typed: true
# frozen_string_literal: true

require "securerandom"

module Durababble
  class WorkerIdentity
    class << self
      #: (address: String, ?id: String?) -> String
      def generate(address:, id: nil)
        build(id: id || SecureRandom.hex(6), address:)
      end

      #: (id: String, address: String) -> String
      def build(id:, address:)
        normalized_id = String(id)
        normalized_address = String(address)
        raise ArgumentError, "worker identity id cannot be empty" if normalized_id.empty?
        raise ArgumentError, "worker identity address cannot be empty" if normalized_address.empty?
        raise ArgumentError, "worker identity id cannot contain @" if normalized_id.include?("@")

        "#{normalized_id}@#{normalized_address}"
      end

      #: (String) -> String
      def address_for(identity)
        value = String(identity)
        _id, separator, address = value.partition("@")
        separator.empty? ? value : address
      end

      #: (String) -> String?
      def id_for(identity)
        value = String(identity)
        id, separator, _address = value.partition("@")
        separator.empty? ? nil : id
      end
    end
  end
end
