# typed: true
# frozen_string_literal: true

module Durababble
  # Centralized retry/backoff math. Every host-side retry sleep in the system
  # routes through here so the jitter policy lives in one place.
  #
  # Jitter matters because Durababble runs many workers against shared storage:
  # without it, a fleet that hits the same transient contention (a deadlock, an
  # undelivered activation) all wait the same fixed interval and retry in
  # lockstep, turning a brief hiccup into a synchronized thundering herd. Adding
  # randomness spreads those retries out so they decorrelate.
  #
  # These run on the worker host, outside workflow replay, so randomness here
  # does not violate determinism (see WorkflowDeterminism).
  module Backoff
    # Fraction of the base delay added as random jitter. 0.25 spreads a delay
    # across [base, base * 1.25] — enough to decorrelate retries while keeping a
    # predictable floor.
    DEFAULT_JITTER = 0.25

    extend self

    # A base delay plus up to `jitter * base` of randomness. Never returns less
    # than `base`, so callers keep their minimum spacing while still scattering.
    #: (Numeric, ?jitter: Float) -> Float
    def jittered(base, jitter: DEFAULT_JITTER)
      (base + (Kernel.rand * jitter * base)).to_f
    end

    # Jittered, linearly growing delay for in-process retry loops. `attempt` is
    # 1-based; the pre-jitter delay is `step * attempt`, optionally capped at
    # `max` before jitter is applied.
    #: (Integer, step: Numeric, ?max: Numeric?, ?jitter: Float) -> Float
    def linear(attempt, step:, max: nil, jitter: DEFAULT_JITTER)
      base = step * attempt
      base = max if max && base > max
      jittered(base, jitter:)
    end

    # Jittered, exponentially growing delay for in-process poll loops. `attempt`
    # is 1-based; the pre-jitter delay is `step * (factor ** (attempt - 1))`,
    # optionally capped at `max` before jitter is applied. Use this over `linear`
    # when a crowd polling the same shared row should taper its frequency fast.
    #: (Integer, step: Numeric, ?factor: Float, ?max: Numeric?, ?jitter: Float) -> Float
    def exponential(attempt, step:, factor: 2.0, max: nil, jitter: DEFAULT_JITTER)
      base = step * (factor**(attempt - 1))
      base = max if max && base > max
      jittered(base, jitter:)
    end
  end
end
