# typed: true
# frozen_string_literal: true

module Durababble
  # Length-prefixed framing for streaming-result RPCs. Each frame is a 4-byte
  # big-endian unsigned length followed by that many payload bytes (the payload
  # is `Rpc.dump`-encoded, Paquito/Marshal). Streams write one frame per quack;
  # a graceful end is the body closing, and a producer error is carried as a
  # terminal frame whose decoded payload is an error `StreamFrame`.
  module FrameCodec
    LENGTH_BYTES = 4

    class << self
      # Encodes one frame: 4-byte big-endian length prefix + payload bytes.
      #: (String) -> String
      def frame(payload)
        bytes = payload.b
        [bytes.bytesize].pack("N") + bytes
      end
    end

    # Accumulates incoming byte chunks (which need not align with frame
    # boundaries) and pops complete frame payloads. A partial frame stays
    # buffered until the rest of its bytes arrive.
    class Buffer
      #: () -> void
      def initialize
        @buffer = +"".b
      end

      #: (String?) -> Buffer
      def <<(chunk)
        @buffer << chunk.b if chunk && !chunk.empty?
        self
      end

      # Pops the next complete frame payload, or nil if no full frame is buffered.
      #: () -> String?
      def shift
        return if @buffer.bytesize < LENGTH_BYTES

        length = @buffer.byteslice(0, LENGTH_BYTES).unpack1("N")
        total = LENGTH_BYTES + length
        return if @buffer.bytesize < total

        payload = @buffer.byteslice(LENGTH_BYTES, length)
        @buffer = @buffer.byteslice(total, @buffer.bytesize - total) || +"".b
        payload
      end
    end
  end
end
