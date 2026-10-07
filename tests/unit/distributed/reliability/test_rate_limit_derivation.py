"""
AD-24 limits derived from the protocol rates they bound -- VOPR-style.

For seeded draws of the protocol's own settings (windows, heartbeat and
flush intervals, worker cores, the AD-37 throttle delay), every derived
limit is exercised against the limiter it configures, on a virtual clock:

* a legitimate sender at its protocol's maximum rate -- sends spaced exactly
  one interval apart, and the same spaced by seeded extra delays, from a
  seeded phase against the counter's windows -- is never refused, through
  many windows;
* a sender sustaining more than the limit's twice-the-protocol rate is
  refused within a few windows;
* the STRESSED per-client budget admits a worker's AD-37-throttled progress
  and refuses a submission storm;
* request-driven operations are unbounded unless configured, and a
  configured limit is the one enforced;
* every limit is an Env setting that overrides its derivation, and the
  tracked-client bound is two per connection the server holds.
"""

import math
import random
from dataclasses import dataclass

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.reliability import adaptive_rate_limiter as adaptive_rate_limiter_module
from hyperscale.distributed.reliability import sliding_window_counter as sliding_window_counter_module
from hyperscale.distributed.reliability.overload import HybridOverloadDetector, OverloadConfig, OverloadState
from hyperscale.distributed.reliability.priority import RequestPriority
from hyperscale.distributed.reliability.rate_limit_derivation import (
    SLIDING_WINDOW_ESTIMATE_BOUND,
    UNBOUNDED_REQUESTS,
    derive_heartbeat_max_requests,
    derive_max_tracked_clients,
    derive_operation_limits,
    derive_progress_update_max_requests,
    derive_rate_limit_window_seconds,
    derive_stressed_max_requests,
    derive_worker_cores,
)
from hyperscale.distributed.reliability.rate_limiting import AdaptiveRateLimitConfig, AdaptiveRateLimiter

SEEDS = range(12)
SIMULATED_WINDOWS = 8
# A flood sends this many times faster than the protocol's maximum: past
# the limit's estimate bound, so it must be refused.
FLOOD_RATE_MULTIPLIER = SLIDING_WINDOW_ESTIMATE_BOUND + 1
MILLISECONDS_PER_SECOND = 1000.0


@dataclass(slots=True)
class VirtualClock:
    """A monotonic clock the simulation advances."""

    now: float = 1000.0

    def monotonic(self) -> float:
        return self.now

    def monotonic_ns(self) -> int:
        return int(self.now * 1e9)

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def virtual_clock(monkeypatch: pytest.MonkeyPatch) -> VirtualClock:
    clock = VirtualClock()
    monkeypatch.setattr(adaptive_rate_limiter_module, "_DEFAULT_CLOCK", clock)
    monkeypatch.setattr(sliding_window_counter_module, "_DEFAULT_CLOCK", clock)
    return clock


def drawn_env(seed: int) -> Env:
    """The protocol's own settings, drawn from plausible ranges."""
    generator = random.Random(seed)
    return Env(
        OVERLOAD_SAMPLE_INTERVAL_SECONDS=generator.choice([0.5, 1.0, 2.0]),
        OVERLOAD_CURRENT_WINDOW=generator.choice([5, 10, 20]),
        MANAGER_HEARTBEAT_INTERVAL=generator.choice([1.0, 2.5, 5.0, 7.0]),
        WORKER_PROGRESS_FLUSH_INTERVAL=generator.choice([0.05, 0.1, 0.25, 0.5]),
        WORKER_MAX_CORES=generator.randint(1, 8),
        WORKER_BACKPRESSURE_THROTTLE_DELAY_MS=generator.choice([250, 500, 1000]),
    )


def healthy_limiter(env: Env) -> AdaptiveRateLimiter:
    return AdaptiveRateLimiter(
        HybridOverloadDetector(),
        config=AdaptiveRateLimitConfig.from_env(env, None),
        detector_sampled_externally=True,
    )


def stressed_limiter(env: Env) -> AdaptiveRateLimiter:
    detector = HybridOverloadDetector(env.get_overload_config())
    stressed_cpu_percent = (env.OVERLOAD_CPU_STRESSED + env.OVERLOAD_CPU_OVERLOADED) / 2 * 100
    detector.get_state(stressed_cpu_percent, 0.0)
    assert detector.current_state is OverloadState.STRESSED
    return AdaptiveRateLimiter(
        detector,
        config=AdaptiveRateLimitConfig.from_env(env, None),
        detector_sampled_externally=True,
    )


def pass_times(
    generator: random.Random,
    interval_seconds: float,
    span_seconds: float,
    jittered: bool,
) -> list[float]:
    """Times a loop that sleeps ``interval_seconds`` between passes sends
    at, from a seeded phase: exactly one interval apart, or later by seeded
    extra delays (scheduling, slow sends)."""
    times: list[float] = []
    elapsed = generator.uniform(0.0, interval_seconds)
    while elapsed < span_seconds:
        times.append(elapsed)
        elapsed += interval_seconds + (generator.uniform(0.0, interval_seconds) if jittered else 0.0)
    return times


async def refusals_for_passes(
    limiter: AdaptiveRateLimiter,
    clock: VirtualClock,
    operation: str,
    times: list[float],
    sends_per_pass: int,
    priority: RequestPriority,
) -> int:
    """Send ``sends_per_pass`` requests at each pass time; count refusals."""
    start = clock.now
    refusals = 0
    for send_time in times:
        clock.now = start + send_time
        for _ in range(sends_per_pass):
            refusals += not (await limiter.check("peer", operation, priority)).allowed
    return refusals


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("jittered", [False, True])
async def test_heartbeats_at_their_interval_are_never_refused(
    seed: int, jittered: bool, virtual_clock: VirtualClock
) -> None:
    env = drawn_env(seed)
    span_seconds = SIMULATED_WINDOWS * derive_rate_limit_window_seconds(env)
    times = pass_times(random.Random(seed), env.MANAGER_HEARTBEAT_INTERVAL, span_seconds, jittered)

    refusals = await refusals_for_passes(
        healthy_limiter(env), virtual_clock, "heartbeat", times, 1, RequestPriority.NORMAL
    )

    assert refusals == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("jittered", [False, True])
async def test_progress_for_every_core_each_flush_is_never_refused(
    seed: int, jittered: bool, virtual_clock: VirtualClock
) -> None:
    env = drawn_env(seed)
    span_seconds = SIMULATED_WINDOWS * derive_rate_limit_window_seconds(env)
    times = pass_times(random.Random(seed), env.WORKER_PROGRESS_FLUSH_INTERVAL, span_seconds, jittered)

    refusals = await refusals_for_passes(
        healthy_limiter(env),
        virtual_clock,
        "progress_update",
        times,
        derive_worker_cores(env),
        RequestPriority.NORMAL,
    )

    assert refusals == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize(
    ("operation", "interval_setting", "sends_per_pass"),
    [
        ("heartbeat", "MANAGER_HEARTBEAT_INTERVAL", lambda env: 1),
        ("progress_update", "WORKER_PROGRESS_FLUSH_INTERVAL", derive_worker_cores),
    ],
)
async def test_a_sender_past_the_limit_is_refused(
    seed: int,
    operation: str,
    interval_setting: str,
    sends_per_pass,
    virtual_clock: VirtualClock,
) -> None:
    env = drawn_env(seed)
    flood_interval = getattr(env, interval_setting) / FLOOD_RATE_MULTIPLIER
    span_seconds = SIMULATED_WINDOWS * derive_rate_limit_window_seconds(env)
    times = pass_times(random.Random(seed), flood_interval, span_seconds, jittered=False)

    refusals = await refusals_for_passes(
        healthy_limiter(env), virtual_clock, operation, times, sends_per_pass(env), RequestPriority.NORMAL
    )

    assert refusals > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_stressed_budget_admits_throttled_progress(seed: int, virtual_clock: VirtualClock) -> None:
    env = drawn_env(seed)
    throttled_interval = (
        env.WORKER_PROGRESS_FLUSH_INTERVAL + env.WORKER_BACKPRESSURE_THROTTLE_DELAY_MS / MILLISECONDS_PER_SECOND
    )
    span_seconds = SIMULATED_WINDOWS * derive_rate_limit_window_seconds(env)
    times = pass_times(random.Random(seed), throttled_interval, span_seconds, jittered=False)

    refusals = await refusals_for_passes(
        stressed_limiter(env),
        virtual_clock,
        "progress_update",
        times,
        derive_worker_cores(env),
        RequestPriority.NORMAL,
    )

    assert refusals == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_stressed_budget_refuses_a_submission_storm(seed: int, virtual_clock: VirtualClock) -> None:
    env = drawn_env(seed)
    window_seconds = derive_rate_limit_window_seconds(env)
    storm_size = FLOOD_RATE_MULTIPLIER * derive_stressed_max_requests(env)
    times = [window_seconds * index / storm_size for index in range(storm_size)]

    refusals = await refusals_for_passes(
        stressed_limiter(env), virtual_clock, "job_submit", times, 1, RequestPriority.HIGH
    )

    assert refusals > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["stats_update", "job_submit", "job_status", "cancel", "reconnect", "default"])
async def test_request_driven_operations_are_unbounded_unless_configured(
    operation: str, virtual_clock: VirtualClock
) -> None:
    env = Env()
    burst = FLOOD_RATE_MULTIPLIER * derive_progress_update_max_requests(env)

    refusals = await refusals_for_passes(
        healthy_limiter(env), virtual_clock, operation, [0.0], burst, RequestPriority.NORMAL
    )

    assert derive_operation_limits(env)[operation][0] == UNBOUNDED_REQUESTS
    assert refusals == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "setting"),
    [
        ("stats_update", "RATE_LIMIT_STATS_UPDATE_MAX_REQUESTS"),
        ("job_submit", "RATE_LIMIT_JOB_SUBMIT_MAX_REQUESTS"),
        ("job_status", "RATE_LIMIT_JOB_STATUS_MAX_REQUESTS"),
        ("workflow_dispatch", "RATE_LIMIT_WORKFLOW_DISPATCH_MAX_REQUESTS"),
        ("cancel", "RATE_LIMIT_CANCEL_MAX_REQUESTS"),
        ("reconnect", "RATE_LIMIT_RECONNECT_MAX_REQUESTS"),
        ("default", "RATE_LIMIT_DEFAULT_MAX_REQUESTS"),
        ("heartbeat", "RATE_LIMIT_HEARTBEAT_MAX_REQUESTS"),
        ("progress_update", "RATE_LIMIT_PROGRESS_UPDATE_MAX_REQUESTS"),
    ],
)
async def test_a_configured_limit_is_the_one_enforced(
    operation: str, setting: str, virtual_clock: VirtualClock
) -> None:
    configured_limit = 7
    env = Env(**{setting: configured_limit})

    limiter = healthy_limiter(env)
    admissions = [
        (await limiter.check("peer", operation, RequestPriority.NORMAL)).allowed
        for _ in range(configured_limit + 1)
    ]

    assert admissions == [True] * configured_limit + [False]


@pytest.mark.parametrize("seed", SEEDS)
def test_derivations_follow_the_protocol_settings(seed: int) -> None:
    env = drawn_env(seed)
    window_seconds = env.OVERLOAD_CURRENT_WINDOW * env.OVERLOAD_SAMPLE_INTERVAL_SECONDS
    cores = env.WORKER_MAX_CORES
    throttled_interval = (
        env.WORKER_PROGRESS_FLUSH_INTERVAL + env.WORKER_BACKPRESSURE_THROTTLE_DELAY_MS / MILLISECONDS_PER_SECOND
    )
    heartbeats = math.floor(window_seconds / env.MANAGER_HEARTBEAT_INTERVAL) + 1

    assert derive_rate_limit_window_seconds(env) == window_seconds
    assert derive_heartbeat_max_requests(env) == SLIDING_WINDOW_ESTIMATE_BOUND * heartbeats
    assert derive_progress_update_max_requests(env) == SLIDING_WINDOW_ESTIMATE_BOUND * cores * (
        math.floor(window_seconds / env.WORKER_PROGRESS_FLUSH_INTERVAL) + 1
    )
    assert derive_stressed_max_requests(env) == SLIDING_WINDOW_ESTIMATE_BOUND * (
        cores * (math.floor(window_seconds / throttled_interval) + 1) + heartbeats
    )
    assert all(window == window_seconds for _limit, window in derive_operation_limits(env).values())


def test_window_and_stressed_settings_override_their_derivations() -> None:
    env = Env(RATE_LIMIT_WINDOW_SECONDS=3.0, RATE_LIMIT_STRESSED_MAX_REQUESTS=11)

    config = AdaptiveRateLimitConfig.from_env(env, None)

    assert config.window_size_seconds == 3.0
    assert config.default_window_size == 3.0
    assert config.stressed_requests_per_window == 11


def test_tracked_clients_are_two_per_held_connection() -> None:
    env = Env()

    assert derive_max_tracked_clients(env, 4096) == 8192
    assert derive_max_tracked_clients(env, None) == UNBOUNDED_REQUESTS
    assert derive_max_tracked_clients(Env(RATE_LIMIT_MAX_TRACKED_CLIENTS=17), 4096) == 17
    assert AdaptiveRateLimitConfig.from_env(env, 4096).max_tracked_clients == 8192


def test_env_overload_settings_are_the_detector_defaults() -> None:
    """The window derives from the OVERLOAD_* settings the nodes' detectors
    run on; their defaults are the detector's own."""
    assert Env().get_overload_config() == OverloadConfig()


def test_a_bare_config_takes_the_derivations_of_the_env_defaults() -> None:
    env = Env()

    assert AdaptiveRateLimitConfig() == AdaptiveRateLimitConfig.from_env(env, None)
