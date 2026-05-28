# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

require "async"
require "securerandom"

class DurababbleWorkflowDeterminismTest < DurababbleTestCase
  FakeTraceEvent = Struct.new(:method_id, :receiver, :defined_class, keyword_init: true) do
    define_method(:self) { receiver }
  end
  FakeCallsite = Struct.new(:path, :lineno, keyword_init: true)
  RandomBaseClass = Class.new do
    class << self
      def name
        "Random::Base"
      end
    end
  end

  UNSAFE_ORCHESTRATION_CASES = {
    "wall clock time" => lambda { Time.now },
    "time constructor" => lambda { Time.new },
    "kernel random" => lambda { rand(1_000_000) },
    "random constructor" => lambda { Random.new },
    "random class" => lambda { Random.rand(1_000_000) },
    "secure random UUID" => lambda { SecureRandom.uuid },
    "explicit blocking sleep" => lambda { Kernel.sleep(0) },
    "file read" => lambda { File.read("README.md", 1) },
    "file system probe" => lambda { File.exist?("README.md") },
    "directory read" => lambda { Dir.entries(".") },
  }.freeze

  test "classifies unsafe workflow orchestration calls" do
    assert_equal "Kernel#sleep", determinism_violation_for(:sleep, receiver: Kernel, defined_class: Kernel)
    assert_equal "Kernel#system", determinism_violation_for(:system, receiver: Kernel, defined_class: Kernel)
    assert_equal "Kernel#rand", determinism_violation_for(:rand, receiver: Kernel, defined_class: Kernel)
    assert_equal "Random.rand", determinism_violation_for(:rand, receiver: Random)
    assert_equal "SecureRandom.uuid", determinism_violation_for(:uuid, receiver: SecureRandom)
    assert_equal "Random.new", determinism_violation_for(:initialize, receiver: Object.new, defined_class: RandomBaseClass)
    assert_equal "Time.new", determinism_violation_for(:initialize, receiver: Time.allocate, defined_class: Time)
    assert_equal "Time.now", determinism_violation_for(:now, receiver: Time)
    assert_equal "Process.clock_gettime", determinism_violation_for(:clock_gettime, receiver: Process)
    assert_equal "IO.read", determinism_violation_for(:read, receiver: IO)
    assert_equal "File.binread", determinism_violation_for(:binread, receiver: File)
    assert_equal "Dir.entries", determinism_violation_for(:entries, receiver: Dir)
    assert_nil determinism_violation_for(:object_id, receiver: Object.new)
  end

  test "classifies violation candidates before callsite lookup" do
    safe_event = FakeTraceEvent.new(method_id: :object_id, receiver: Object.new, defined_class: Object)
    file_event = FakeTraceEvent.new(method_id: :read, receiver: File, defined_class: File)
    secure_random_event = FakeTraceEvent.new(method_id: :uuid, receiver: SecureRandom, defined_class: SecureRandom)

    assert_nil Durababble::WorkflowDeterminism.send(:violation_candidate_for, safe_event)
    assert_equal ["File.read", true], Durababble::WorkflowDeterminism.send(:violation_candidate_for, file_event)
    assert_equal ["SecureRandom.uuid", false], Durababble::WorkflowDeterminism.send(:violation_candidate_for, secure_random_event)
  end

  test "allows internal Durababble callsites while rejecting unknown callsites" do
    assert_nil determinism_violation_for(:sleep, receiver: Kernel, defined_class: Kernel, callsite: internal_callsite)
    assert_nil determinism_violation_for(:now, receiver: Time, callsite: internal_callsite)
    assert_nil determinism_violation_for(:clock_gettime, receiver: Process, callsite: internal_callsite)
    assert_nil determinism_violation_for(:read, receiver: IO, callsite: internal_callsite)
    assert_nil determinism_violation_for(:entries, receiver: Dir, callsite: internal_callsite)

    assert_equal "Kernel#sleep", determinism_violation_for(:sleep, receiver: Kernel, defined_class: Kernel, callsite: nil)
  end

  test "recognizes IO receiver shapes" do
    File.open("README.md", "rb") do |io|
      assert(Durababble::WorkflowDeterminism.send(:io_receiver?, io))
    end

    assert Durababble::WorkflowDeterminism.send(:io_receiver?, IO)
    assert Durababble::WorkflowDeterminism.send(:io_receiver?, File)
    assert Durababble::WorkflowDeterminism.send(:io_receiver?, Class.new(IO))
    refute Durababble::WorkflowDeterminism.send(:io_receiver?, String)
  end

  test "skips internal Ruby and determinism frames when reporting callsites" do
    locations = [
      FakeCallsite.new(path: "<internal:timev>", lineno: 265),
      FakeCallsite.new(path: File.expand_path("../../lib/durababble/workflow_determinism.rb", __dir__), lineno: 82),
      FakeCallsite.new(path: "/tmp/user_workflow.rb", lineno: 14),
    ]

    callsite = Durababble::WorkflowDeterminism.send(:callsite_location, locations)
    assert_equal "/tmp/user_workflow.rb", callsite.path
    assert_nil Durababble::WorkflowDeterminism.send(:callsite_location, locations.first(2))
  end

  test "enforces only inside workflow execution context and host allowances" do
    assert_equal "#", Durababble::WorkflowDeterminism.enforce(workflow_id: "no-context") { File.read("README.md", 1) }

    Durababble::WorkflowExecutionContext.with_current(Object.new) do
      assert_equal "#", Durababble::WorkflowDeterminism.enforce(workflow_id: "allowed-host") {
        Durababble::WorkflowDeterminism.allow_host_operations { File.read("README.md", 1) }
      }

      error = assert_raises(Durababble::DeterminismError) do
        Durababble::WorkflowDeterminism.enforce(workflow_id: "unsafe-host") { File.read("README.md", 1) }
      end
      assert_match(/workflow unsafe-host orchestration cannot call File.read/, error.message)
    end
  end

  test "falls back when target-thread TracePoint enable is unavailable" do
    trace = Object.new
    calls = []
    trace.define_singleton_method(:enable) do |**kwargs|
      calls << kwargs
      raise ArgumentError if kwargs.key?(:target_thread)
    end
    trace.define_singleton_method(:disable) { calls << :disable }

    result = Durababble::WorkflowDeterminism.send(:enable_trace, trace) { "enabled" }

    assert_equal "enabled", result
    assert_equal [{ target_thread: Thread.current }, {}, :disable], calls
  end

  test "checks trace events only in unsafe workflow orchestration context" do
    unsafe_event = FakeTraceEvent.new(method_id: :read, receiver: File, defined_class: File)
    safe_event = FakeTraceEvent.new(method_id: :object_id, receiver: Object.new, defined_class: Object)

    assert_nil Durababble::WorkflowDeterminism.send(:check_event!, "outside", unsafe_event, locations: [unsafe_callsite])

    Durababble::WorkflowExecutionContext.with_current(Object.new) do
      assert_nil Durababble::WorkflowDeterminism.allow_host_operations {
        Durababble::WorkflowDeterminism.send(:check_event!, "allowed", unsafe_event, locations: [unsafe_callsite])
      }
      assert_nil Durababble::WorkflowDeterminism.send(:check_event!, "safe", safe_event)
      assert_nil Durababble::WorkflowDeterminism.send(:check_event!, "safe", safe_event, locations: [unsafe_callsite])

      error = assert_raises(Durababble::DeterminismError) do
        Durababble::WorkflowDeterminism.send(:check_event!, "unsafe", unsafe_event, locations: [unsafe_callsite])
      end
      assert_match(%r{workflow unsafe orchestration cannot call File.read at /tmp/user_workflow.rb:12}, error.message)

      error = assert_raises(Durababble::DeterminismError) do
        Durababble::WorkflowDeterminism.send(:check_event!, "unknown", unsafe_event, locations: [])
      end
      assert_match(/workflow unknown orchestration cannot call File.read;/, error.message)
    end
  end

  durababble_store_backends.each do |backend|
    UNSAFE_ORCHESTRATION_CASES.each do |name, operation|
      test "rejects #{name} in workflow orchestration with #{backend.name}" do
        with_durababble_store(backend, "workflow_determinism_#{name.gsub(/\W+/, "_")}") do |store|
          workflow = Class.new(Durababble::Workflow) do
            workflow_name "unsafe-orchestration-#{name.gsub(/\W+/, "-")}"

            define_method(:execute) do |_input|
              operation.call
              "unreachable"
            end
          end

          run = Durababble::Engine.new(store:, worker_id: "determinism-worker").run(workflow, input: {})

          assert_equal "failed", run.status
          assert_match(/Durababble::DeterminismError/, run.error)
          assert_match(/orchestration cannot call/, run.error)
          assert_empty store.workflow_history_for(run.id)
        end
      end
    end

    test "rejects unsafe calls from raw Async workflow fibers with #{backend.name}" do
      with_durababble_store(backend, "workflow_determinism_async") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "unsafe-async-orchestration"

          def execute(_input)
            error = nil
            Async do
              File.read("README.md", 1)
            rescue StandardError => e
              error = e
            end.wait
            raise error if error
          end
        end

        run = Durababble::Engine.new(store:, worker_id: "determinism-async-worker").run(workflow, input: {})

        assert_equal "failed", run.status
        assert_match(/Durababble::DeterminismError/, run.error)
        assert_match(/orchestration cannot call/, run.error)
        assert_empty store.workflow_history_for(run.id)
      end
    end

    test "keeps durable sleep available in workflow orchestration with #{backend.name}" do
      with_durababble_store(backend, "workflow_determinism_durable_sleep") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "determinism-durable-sleep"

          def execute(input)
            sleep(input.fetch("duration"), input.merge("slept" => true))
          end
        end

        engine = Durababble::Engine.new(store:, worker_id: "determinism-sleep-worker")
        workflow_id = store.enqueue_workflow(name: workflow.workflow_name, input: { "duration" => 3600 })

        waiting = engine.resume(workflow, workflow_id:)
        assert_equal "waiting", waiting.status
        assert_equal [["sleep", "waiting"]], store.steps_for(workflow_id).map { |step| [step.fetch("name"), step.fetch("status")] }

        assert_equal 1, store.wake_due_timers(now: Time.now + 3601)
        completed = engine.resume(workflow, workflow_id:)
        assert_equal "completed", completed.status
        assert_equal({ "duration" => 3600, "slept" => true }, completed.result)
      end
    end

    test "keeps normal host Ruby semantics inside step bodies with #{backend.name}" do
      with_durababble_store(backend, "workflow_determinism_steps") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "determinism-step-host-semantics"

          def execute(input)
            host_operation(input)
          end

          def host_operation(input)
            Kernel.sleep(0)
            input.merge(
              "time_class" => Time.now.class.name,
              "random_integer" => rand(1_000_000).is_a?(Integer),
              "read_byte" => File.read("README.md", 1),
            )
          end
          step :host_operation
        end

        run = Durababble::Engine.new(store:, worker_id: "determinism-step-worker").run(workflow, input: { "ok" => true })

        assert_equal "completed", run.status
        assert_hash_includes(
          run.result,
          "ok" => true,
          "time_class" => "Time",
          "random_integer" => true,
          "read_byte" => "#",
        )
      end
    end

    test "does not alter host APIs after workflow determinism failures with #{backend.name}" do
      with_durababble_store(backend, "workflow_determinism_host_api") do |store|
        workflow = Class.new(Durababble::Workflow) do
          workflow_name "unsafe-host-api-check"

          def execute(_input)
            File.read("README.md", 1)
          end
        end

        run = Durababble::Engine.new(store:, worker_id: "determinism-host-worker").run(workflow, input: {})
        assert_equal "failed", run.status
        assert_match(/Durababble::DeterminismError/, run.error)
      end

      assert_equal "#", File.read("README.md", 1)
      assert_kind_of Integer, rand(1_000_000)
      assert_kind_of Time, Time.now
      assert_equal 0, Kernel.sleep(0)
    end
  end

  private

  def determinism_violation_for(method_id, receiver:, defined_class: receiver, callsite: unsafe_callsite)
    event = FakeTraceEvent.new(method_id:, receiver:, defined_class:)
    Durababble::WorkflowDeterminism.send(:violation_for, event, callsite:)
  end

  def unsafe_callsite
    FakeCallsite.new(path: "/tmp/user_workflow.rb", lineno: 12)
  end

  def internal_callsite
    FakeCallsite.new(path: File.expand_path("../../lib/durababble/engine.rb", __dir__), lineno: 42)
  end
end
