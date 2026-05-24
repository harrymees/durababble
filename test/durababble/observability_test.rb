# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

class DurababbleObservabilityTest < DurababbleTestCase
  TestSpan = Data.define(:name, :attributes, :exceptions) do
    def add_attributes(values)
      attributes.merge!(values)
    end

    def record_exception(error)
      exceptions << error
    end
  end

  class TestTracer
    attr_reader :spans

    def initialize
      @spans = []
    end

    def in_span(name, attributes: {})
      span = TestSpan.new(name:, attributes: attributes.dup, exceptions: [])
      @spans << span
      yield span
    end

    def reset!
      spans.clear
    end
  end

  class TestTracerProvider
    attr_reader :test_tracer

    def initialize
      @test_tracer = TestTracer.new
    end

    def tracer(*) = test_tracer
  end

  class TestInstrument
    attr_reader :name, :measurements

    def initialize(name)
      @name = name
      @measurements = []
    end

    def add(value, attributes: {})
      @measurements << { value:, attributes: }
    end

    def record(value, attributes: {})
      @measurements << { value:, attributes: }
    end
  end

  class TestMeter
    attr_reader :instruments

    def initialize
      @instruments = {}
    end

    def create_counter(name, **)
      instruments[name] ||= TestInstrument.new(name)
    end

    def create_histogram(name, **)
      instruments[name] ||= TestInstrument.new(name)
    end
  end

  class TestMeterProvider
    attr_reader :test_meter

    def initialize
      @test_meter = TestMeter.new
    end

    def meter(_name = nil, version: nil) = test_meter
  end

  class ObservedCounter < Durababble::DurableObject
    def initialize_state
      { "count" => 0 }
    end

    expose_command def add(amount)
      update_state("count" => current_state.fetch("count") + amount)
    end

    expose def count
      current_state.fetch("count")
    end
  end

  class RuntimeStore
    attr_reader :workflows, :steps, :attempts, :history

    def initialize
      @next_id = 0
      @workflows = {}
      @steps = Hash.new { |hash, key| hash[key] = [] }
      @attempts = Hash.new { |hash, key| hash[key] = [] }
      @history = Hash.new { |hash, key| hash[key] = [] }
    end

    def migrate! = self

    def enqueue_workflow(name:, input:)
      @next_id += 1
      id = "wf-#{@next_id}"
      @workflows[id] = { "id" => id, "name" => name, "status" => "pending", "input" => input, "created_at" => Time.now }
      id
    end

    def workflow(workflow_id) = @workflows.fetch(workflow_id)

    def claim_workflow(workflow_id:, worker_id:, lease_seconds:)
      row = @workflows.fetch(workflow_id)
      return if row.fetch("status") == "running" && row["locked_by"] != worker_id

      row.merge!("status" => "running", "locked_by" => worker_id, "locked_until" => Time.now + lease_seconds)
      row
    end

    def claim_runnable_workflow(worker_id:, lease_seconds:, workflow_names: nil)
      row = @workflows.values.find { |workflow| workflow.fetch("status") == "pending" && (!workflow_names || workflow_names.include?(workflow.fetch("name"))) }
      return unless row

      claim_workflow(workflow_id: row.fetch("id"), worker_id:, lease_seconds:)
    end

    def workflow_owned?(workflow_id:, worker_id:)
      row = @workflows.fetch(workflow_id)
      row.fetch("status") == "running" && row.fetch("locked_by") == worker_id
    end

    def workflow_cancellation(_workflow_id) = nil

    def step_attempts_for(workflow_id) = attempts[workflow_id]
    def workflow_history_for(workflow_id) = history[workflow_id]
    def step_heartbeat_cursor(workflow_id:, command_id: nil, position: nil) = nil

    def record_step_scheduled(workflow_id:, command_id:, name:, args: [], kwargs: {}, metadata: {})
      append_history(workflow_id, "kind" => "step_scheduled", "command_id" => command_id, "name" => name, "payload" => { "name" => name, "args" => args, "kwargs" => kwargs, "retry" => metadata.fetch("retry") })
    end

    def record_step_started(workflow_id:, name:, command_id: nil, position: nil)
      position = command_id || position
      step = { "workflow_id" => workflow_id, "position" => position, "name" => name, "status" => "running" }
      steps[workflow_id].delete_if { |row| row.fetch("position") == position }
      steps[workflow_id] << step
      attempts[workflow_id] << step.merge("id" => "attempt-#{attempts[workflow_id].length + 1}")
      append_history(workflow_id, "kind" => "step_started", "command_id" => position, "name" => name, "attempt_id" => attempts[workflow_id].last.fetch("id"))
    end

    def record_step_completed(workflow_id:, result:, command_id: nil, position: nil)
      position = command_id || position
      step = steps[workflow_id].find { |row| row.fetch("position") == position }
      step.merge!("status" => "completed", "result" => result)
      attempts[workflow_id].last.merge!("status" => "completed", "result" => result)
      append_history(workflow_id, "kind" => "step_completed", "command_id" => position, "payload" => result)
    end

    def record_step_failed(workflow_id:, error:, command_id: nil, position: nil)
      position = command_id || position
      step = steps[workflow_id].find { |row| row.fetch("position") == position }
      step.merge!("status" => "failed", "error" => error)
      attempts[workflow_id].last.merge!("status" => "failed", "error" => error)
      append_history(workflow_id, "kind" => "step_failed", "command_id" => position, "error" => error)
    end

    def complete_workflow(workflow_id, result:)
      workflows.fetch(workflow_id).merge!("status" => "completed", "result" => result, "locked_by" => nil)
    end

    def fail_workflow(workflow_id, error:)
      workflows.fetch(workflow_id).merge!("status" => "failed", "error" => error, "locked_by" => nil)
    end

    private

    def append_history(workflow_id, event)
      history[workflow_id] << event.merge("event_index" => history[workflow_id].length)
    end
  end

  class ObjectStore
    Result = Data.define(:cmd_tuples)

    def initialize
      @state = {}
      @commands = {}
    end

    def migrate! = self
    def object_state(object_type:, object_id:) = @state[[object_type, object_id]]

    def enqueue_object_command(object_type:, object_id:, method_name:, args:, kwargs:)
      command_id = "cmd-#{@commands.length + 1}"
      @commands[command_id] = { object_type:, object_id:, method_name:, args:, kwargs:, status: "pending" }
      command_id
    end

    def claim_object_command(command_id:, worker_id:)
      @commands.fetch(command_id).merge!(status: "running", worker_id:)
      { "id" => command_id }
    end

    def complete_object_command(command_id:, result:, object_type: nil, object_id: nil, state: nil, worker_id: nil)
      @state[[object_type, object_id]] = state if object_type && object_id && !state.equal?(Durababble::Store::NO_OBJECT_STATE)
      @commands.fetch(command_id).merge!(status: "completed", result:, worker_id:)
      Result.new(1)
    end

    def fail_object_command(command_id:, error:, worker_id:)
      @commands.fetch(command_id).merge!(status: "failed", error:, worker_id:)
    end
  end

  class FakeSqlResult
    def first = nil
    def map(&block) = [].map(&block)
    def to_a = []
    def cmd_tuples = 0
  end

  class FakeSqlConnection
    def exec(*) = FakeSqlResult.new
    def exec_params(*) = FakeSqlResult.new
    def transaction = yield
    def close = true
    def finished? = false
  end

  def setup
    super
    @tracer_provider = self.class.instance_variable_get(:@test_tracer_provider) || TestTracerProvider.new
    @meter_provider = self.class.instance_variable_get(:@test_meter_provider) || TestMeterProvider.new
    unless self.class.instance_variable_get(:@open_telemetry_providers_installed)
      OpenTelemetry.tracer_provider = @tracer_provider
      OpenTelemetry.meter_provider = @meter_provider
      self.class.instance_variable_set(:@test_tracer_provider, @tracer_provider)
      self.class.instance_variable_set(:@test_meter_provider, @meter_provider)
      self.class.instance_variable_set(:@open_telemetry_providers_installed, true)
    end
    @tracer_provider.test_tracer.reset!
    @meter_provider.test_meter.instruments.clear
    Durababble.configure_observability(
      enabled: true,
      attributes: { "service.name" => "durababble-test" },
    )
  end

  def teardown
    Durababble.configure_observability(enabled: false)
    super
  end

  test "disabled observability does not require OpenTelemetry providers" do
    Durababble.configure_observability(enabled: false)

    assert_equal false, Durababble.observability.enabled?
    assert_nil Durababble.observability.tracer
    assert_nil Durababble.observability.meter
    assert_nil Durababble.observability.instrument(:counter, "durababble.disabled")
    assert_nil Durababble::Observability.trace("durababble.disabled") { nil }
  end

  test "enabled observability uses OpenTelemetry global providers directly" do
    assert_same @tracer_provider.test_tracer, Durababble.observability.tracer
    assert_same @meter_provider.test_meter, Durababble.observability.meter
  end

  test "cleans attributes and annotates traced errors" do
    timestamp = Time.utc(2026, 5, 24, 17, 20, 0)

    error = assert_raises(RuntimeError) do
      Durababble::Observability.trace(
        "durababble.failure",
        {
          nil_value: nil,
          symbol_value: :queued,
          time_value: timestamp,
          object_value: { "id" => "wf-1" },
        },
      ) { raise "boom" }
    end

    assert_equal "boom", error.message
    span = @tracer_provider.test_tracer.spans.last
    refute_includes span.attributes.keys, "nil_value"
    assert_equal "queued", span.attributes.fetch("symbol_value")
    assert_equal "2026-05-24T17:20:00.000000Z", span.attributes.fetch("time_value")
    assert_equal "{\"id\" => \"wf-1\"}", span.attributes.fetch("object_value")
    assert_equal "RuntimeError", span.attributes.fetch("error.type")
    assert_equal ["RuntimeError"], span.exceptions.map { |exception| exception.class.name }
  end

  test "does not annotate durable control-flow suspensions as errors" do
    assert_raises(Durababble::WorkflowSuspended) do
      Durababble::Observability.trace("durababble.suspended") { raise Durababble::WorkflowSuspended }
    end

    span = @tracer_provider.test_tracer.spans.last
    refute_includes span.attributes.keys, "error.type"
    assert_empty span.exceptions

    nil_span_result = Durababble::Observability.send(:annotate_error, nil, RuntimeError.new("ignored"))
    assert_nil nil_span_result
  end

  test "labels unknown SQL shapes and MySQL stores conservatively" do
    assert_equal "unknown.unknown", Durababble::Observability.query_shape("/* comment */")
    assert_equal "mysql", Durababble::Observability.store_backend(Durababble::MysqlStore.allocate)
  end

  test "emits spans and metrics for runtime, object, worker, and store paths" do
    runtime_store = RuntimeStore.new
    engine = Durababble::Engine.new(store: runtime_store)
    workflow = durababble_test_workflow("observed-counter") do
      test_step("increment") { |ctx| { "count" => ctx.fetch("count") + 1 } }
    end

    run = engine.run(workflow, input: { "count" => 1 })
    assert_equal "completed", run.status

    worker_id = runtime_store.enqueue_workflow(name: workflow.workflow_name, input: { "count" => 4 })
    assert_equal "pending", runtime_store.workflow(worker_id).fetch("status")
    assert_equal :worked, Durababble::Worker.new(store: runtime_store, workflows: [workflow], worker_id: "worker-observed").tick

    object_store = ObjectStore.new
    counter = ObservedCounter.ref("counter-1", store: object_store)
    assert_equal({ "count" => 5 }, counter.add(5))
    assert_equal 5, counter.count

    sql_store = Durababble::Store.new(FakeSqlConnection.new, schema: "observability_test")
    sql_store.send(:execute_params, "SELECT * FROM observability_test.workflows WHERE id = $1", ["wf-1"])

    span_names = @tracer_provider.test_tracer.spans.map(&:name)
    assert_includes span_names, "durababble.workflow.start"
    assert_includes span_names, "durababble.workflow.execute"
    assert_includes span_names, "durababble.workflow.step"
    assert_includes span_names, "durababble.object.command"
    assert_includes span_names, "durababble.object.query"
    assert_includes span_names, "durababble.worker.tick"
    assert_includes span_names, "durababble.store.operation"

    step_span = @tracer_provider.test_tracer.spans.find { |span| span.name == "durababble.workflow.step" }
    assert_equal "increment", step_span.attributes.fetch("durababble.step.name")
    assert_equal "durababble-test", step_span.attributes.fetch("service.name")

    instruments = @meter_provider.test_meter.instruments
    assert_includes instruments.keys, "durababble.workflow.starts"
    assert_includes instruments.keys, "durababble.workflow.completions"
    assert_includes instruments.keys, "durababble.workflow.step.attempts"
    assert_includes instruments.keys, "durababble.store.operation.duration"
    assert_equal 1, instruments.fetch("durababble.workflow.starts").measurements.first.fetch(:value)

    store_span = @tracer_provider.test_tracer.spans.find { |span| span.name == "durababble.store.operation" }
    assert_equal "select.workflows", store_span.attributes.fetch("durababble.store.query_shape")
    assert_equal "postgresql", store_span.attributes.fetch("durababble.store.backend")
  end
end
