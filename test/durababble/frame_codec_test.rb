# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleFrameCodecTest < DurababbleTestCase
  Codec = Durababble::FrameCodec

  test "frames carry a 4-byte big-endian length prefix" do
    framed = Codec.frame("hello")

    assert_equal 9, framed.bytesize
    assert_equal [5].pack("N"), framed.byteslice(0, 4)
    assert_equal "hello", framed.byteslice(4, 5)
  end

  test "buffer pops complete frames and keeps partial bytes buffered" do
    buffer = Codec::Buffer.new
    buffer << Codec.frame("a") << Codec.frame("bb")

    assert_equal "a", buffer.shift
    assert_equal "bb", buffer.shift
    assert_nil buffer.shift
  end

  test "buffer reassembles frames split across chunk boundaries" do
    framed = Codec.frame("split-me") + Codec.frame("second")
    buffer = Codec::Buffer.new

    # Feed one byte at a time; no full frame emerges until its bytes complete.
    decoded = []
    framed.each_byte do |byte|
      buffer << byte.chr
      while (payload = buffer.shift)
        decoded << payload
      end
    end

    assert_equal ["split-me", "second"], decoded
  end

  test "round-trips binary Rpc payloads including embedded length prefixes" do
    value = { "n" => 7, "blob" => "\x00\x00\x00\x05".b }
    buffer = Codec::Buffer.new
    buffer << Codec.frame(Durababble::Rpc.dump(value))

    assert_equal value, Durababble::Rpc.load(buffer.shift)
    assert_nil buffer.shift
  end

  test "handles zero-length payloads" do
    buffer = Codec::Buffer.new
    buffer << Codec.frame("")

    assert_equal "", buffer.shift
    assert_nil buffer.shift
  end
end
