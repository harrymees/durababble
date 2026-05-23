# typed: true
# frozen_string_literal: true

require "json"
require "open3"
require "rbconfig"

module Durababble
  class RpcClient
    class Error < Durababble::Error; end
    class RemoteError < Error; end
    class ConnectionError < Error; end
    class EOFError < ConnectionError; end
    class TimeoutError < ConnectionError; end

    DEFAULT_TIMEOUT = 5.0

    class << self
      #: (command: untyped, ?env: untyped, ?timeout: untyped) -> untyped
      def spawn(command:, env: {}, timeout: DEFAULT_TIMEOUT)
        open3 = Open3 #: as untyped
        stdin, stdout, wait_thread = open3.popen2e(env, *command)
        new(stdin:, stdout:, wait_thread:, timeout:, command:, env:)
      end
    end

    #: (stdin: untyped, stdout: untyped, wait_thread: untyped, ?timeout: untyped, ?command: untyped, ?env: untyped) -> void
    def initialize(stdin:, stdout:, wait_thread:, timeout: DEFAULT_TIMEOUT, command: nil, env: {})
      @stdin = stdin
      @stdout = stdout
      @wait_thread = wait_thread
      @timeout = timeout
      @command = command
      @env = env
    end

    #: (untyped, ?untyped) -> untyped
    def request(command, payload = {})
      recover_poisoned_connection
      reconnect_if_idle_connection_died
      write_request(command, payload)
      read_response(command)
    end

    #: () -> untyped
    def close
      @stdin.close unless @stdin.closed?
      return if @wait_thread.join(1)

      Process.kill("TERM", @wait_thread.pid)
      @wait_thread.join(1)
    rescue StandardError
      nil
    ensure
      close_streams
    end

    private

    #: () -> untyped
    def recover_poisoned_connection
      return unless @poisoned

      unless @command
        raise ConnectionError, "RPC connection timed out and cannot be reused without a spawn command"
      end

      close_streams
      open3 = Open3 #: as untyped
      @stdin, @stdout, @wait_thread = open3.popen2e(@env, *@command)
      @poisoned = false
    rescue SystemCallError, IOError => e
      raise ConnectionError, "failed to reconnect RPC worker after timeout: #{e.class}: #{e.message}"
    end

    #: () -> untyped
    def reconnect_if_idle_connection_died
      return unless @command
      return unless @wait_thread.join(0)

      close_streams
      open3 = Open3 #: as untyped
      @stdin, @stdout, @wait_thread = open3.popen2e(@env, *@command)
    rescue SystemCallError, IOError => e
      raise ConnectionError, "failed to reconnect RPC worker: #{e.class}: #{e.message}"
    end

    #: () -> untyped
    def close_streams
      @stdin.close if @stdin.respond_to?(:closed?) && !@stdin.closed?
      @stdout.close if @stdout.respond_to?(:closed?) && !@stdout.closed?
    rescue StandardError
      nil
    end

    #: (untyped, untyped) -> untyped
    def write_request(command, payload)
      @stdin.puts(JSON.generate({ command:, payload: }))
      @stdin.flush
    rescue SystemCallError, IOError => e
      raise ConnectionError, "failed to write RPC request #{command}: #{e.class}: #{e.message}"
    end

    #: (untyped) -> untyped
    def read_response(command)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @timeout
      loop do
        line = read_line_before(deadline, command)
        next unless line.start_with?("{")

        response = JSON.parse(line)
        next unless response.key?("ok")

        raise RemoteError, response.fetch("error") unless response.fetch("ok")

        return response.fetch("result")
      rescue JSON::ParserError => e
        raise ConnectionError, "invalid RPC JSON response for #{command}: #{e.message}"
      end
    end

    #: (untyped, untyped) -> untyped
    def read_line_before(deadline, command)
      if @stdout.respond_to?(:to_io)
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        timeout!(command) unless remaining.positive?
        ready = IO.select([@stdout.to_io], nil, nil, remaining)
        timeout!(command) unless ready
      end

      line = @stdout.gets
      raise EOFError, "RPC worker exited before response for #{command}" unless line

      line
    rescue IOError, SystemCallError => e
      raise ConnectionError, "failed to read RPC response for #{command}: #{e.class}: #{e.message}"
    end

    #: (untyped) -> untyped
    def timeout!(command)
      @poisoned = true
      raise TimeoutError, "RPC request #{command} timed out after #{@timeout}s"
    end
  end
end
