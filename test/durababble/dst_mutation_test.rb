# typed: false
# frozen_string_literal: true

require_relative "../test_helper"

require_relative "../support/deterministic"

# Mutation testing for the deterministic simulation suite.
#
# DST is only worth running if it actually catches the bugs it claims to. Each
# test here *reintroduces* a known crash-recovery bug at runtime (by overriding
# the exact method the fix lives in) and asserts that the pinning scenario goes
# red — i.e. the mutation is detected as a store-invariant violation or an
# outright crash during the run. A baseline assertion confirms the same scenario
# is clean once the original method is restored, so a green mutation test can
# only mean "DST has teeth," never "the scenario never fails."
#
# This guards against the failure mode where a checker or scenario silently
# rots into a no-op: if a fix is reverted in production code and no scenario
# notices, one of these tests fails.
class DurababbleDstMutationTest < DurababbleTestCase
  # The pinning scenarios are structurally deterministic (crash points are
  # hand-placed, not RNG-gated), so a small sweep is plenty; .any? short-circuits
  # on the first seed that detects the mutation.
  SEEDS = (1..20)

  test "non-atomic step-failure retry is caught by step_failure_crash_matrix" do
    # Bug 1, retry path: record_step_failed_and_schedule_retry must persist the
    # step failure AND reschedule the workflow (clearing the lease) in one
    # transaction. Drop the reschedule and the workflow is stranded running+leased
    # with a failed step; after the crash at :step_failed_recorded no reaper runs,
    # so the recovering worker hits LeaseConflict and the retry never completes.
    mutation = proc do |workflow_id:, error:, worker_id:, command_id: nil, position: nil, **|
      command_id = normalize_command_id(command_id, position)
      transaction do
        assert_workflow_lease_for_update!(workflow_id:, worker_id:)
        record_step_failed_without_transaction(workflow_id:, command_id:, error:, terminal: false)
        true # BUG: never schedule the retry — workflow stays running + leased
      end
    end

    assert_mutation_detected(
      "step_failure_crash_matrix",
      Durababble::SqlStore,
      :record_step_failed_and_schedule_retry,
      mutation,
    )
  end

  test "non-terminal exhausted step failure is caught by step_failure_crash_matrix" do
    # Bug 1, exhausted path: the final step failure is recorded as terminal so
    # replay does NOT re-run the step. Mis-classify it as retrying and replay
    # re-runs the (fenced) side effect, so the once-only effect fires twice.
    mutation = proc { |_event| true }

    assert_mutation_detected(
      "step_failure_crash_matrix",
      Durababble::WorkflowReplayHistory,
      :retrying_step_failure?,
      mutation,
    )
  end

  test "fence that is never reclaimed is caught by fence_holder_crash_and_reclaim" do
    # Bug 2: a worker that crashes holding a fence leaves the row `running`
    # forever unless another worker atomically takes it over past its lease via
    # claim_expired_fence. Refuse to reclaim and the side effect never runs and
    # the fence is stuck — both flagged. The reclaim is exercised under DST
    # through DeterministicSqliteStore#with_fence (which re-implements the
    # inherited with_fence under virtual time); its reclaim seam is the single
    # point that reverting the fix touches.
    mutation = proc { |**| false }

    assert_mutation_detected(
      "fence_holder_crash_and_reclaim",
      Durababble::Deterministic::DeterministicSqliteStore,
      :reclaim_expired_fence,
      mutation,
    )
  end

  private

  # Asserts (1) the scenario is clean across the seed sweep with the original
  # method in place, and (2) replacing `method` on `klass` with `mutation` makes
  # at least one seed go red. Restores the original method unconditionally.
  #: (String, Module, Symbol, Proc) -> void
  def assert_mutation_detected(scenario, klass, method, mutation)
    refute(
      detects_failure?(scenario, SEEDS),
      "baseline scenario #{scenario} already fails without any mutation — the test proves nothing",
    )

    with_mutation(klass, method, mutation) do
      assert(
        detects_failure?(scenario, SEEDS),
        "reverting #{klass}##{method} did not make #{scenario} fail — DST is not pinning this fix",
      )
    end

    refute(
      detects_failure?(scenario, SEEDS),
      "scenario #{scenario} still failing after the mutation was restored — leaked state",
    )
  end

  # A mutation is "detected" if any seed produces store-invariant violations or
  # raises during the run (an unrescued crash propagates out of Scheduler#run).
  #: (String, Range[Integer]) -> bool
  def detects_failure?(scenario, seeds)
    seeds.any? do |seed|
      !Durababble::Deterministic.prove(scenario, seed:).violations.empty?
    rescue StandardError
      true
    end
  end

  #: (Module, Symbol, Proc) { () -> void } -> void
  def with_mutation(klass, method, mutation)
    original = klass.instance_method(method)
    klass.send(:define_method, method, mutation)
    yield
  ensure
    klass.send(:define_method, method, original)
  end
end
