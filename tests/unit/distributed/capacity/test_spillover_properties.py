"""
Spillover evaluation (AD-43) as a property over seeded capacity scenarios.

Every expectation comes from AD-43 (docs/architecture/AD_43.md):

* Part 4 -- a datacenter's wait for cores is the later of when its release
  schedule frees enough of them and when its queued and executing work
  drains over all its cores; a requirement beyond the datacenter waits as
  one for every core it has (a dispatcher grows a job onto cores as they
  free), so with every core free it waits for nothing.
* Part 7 -- the decision flow: no spillover when disabled, when the
  primary's capacity is stale, when it can serve the job immediately or
  its wait is within ``SPILLOVER_MAX_WAIT_SECONDS``; otherwise the fresh
  fallback that can serve immediately within
  ``SPILLOVER_MAX_LATENCY_PENALTY_MS`` with the least latency penalty,
  taken only when its wait is at most ``SPILLOVER_MIN_IMPROVEMENT_RATIO``
  of the primary's.
* Part 1 (problem statement) -- spillover moves a job to "DCs with
  available cores": a spillover target never holds fewer of the job's
  cores than the primary would give it.

The oracle is an independent transcription of that flow; metamorphic
properties pin the monotone inputs (a busier primary never stops a
spillover; a freer primary, a farther fallback, a stricter threshold
never start one).
"""

import itertools
import math
import random

import pytest

from hyperscale.distributed.capacity import SpilloverConfig, SpilloverEvaluator
from hyperscale.distributed.capacity.datacenter_capacity import DatacenterCapacity
from hyperscale.distributed.env import Env

NOW = 10_000.0
SEEDS = range(40)
SCENARIOS_PER_SEED = 50


class FixedClock:
    """The gate's monotonic clock, held at one reading per scenario."""

    def __init__(self, now: float) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now


def random_capacity(rng: random.Random, datacenter_id: str, config: SpilloverConfig) -> DatacenterCapacity:
    total_cores = rng.choice([0, 1, 2, 4, 8, 16, 32, 64])
    available_cores = rng.randint(0, total_cores)
    held_cores = total_cores - available_cores
    release_schedule: list[tuple[float, int]] = []
    while held_cores > 0 and rng.random() < 0.8:
        released_cores = rng.randint(1, held_cores)
        release_schedule.append((rng.uniform(0.0, 4.0 * config.max_wait_seconds), released_cores))
        held_cores -= released_cores
    staleness = config.capacity_staleness_threshold_seconds
    age = rng.choice([0.0, rng.uniform(0.0, staleness), staleness, rng.uniform(staleness, 3.0 * staleness)])
    return DatacenterCapacity(
        datacenter_id=datacenter_id,
        total_cores=total_cores,
        available_cores=available_cores,
        pending_workflow_count=rng.randint(0, 20),
        pending_duration_seconds=rng.choice([0.0, rng.uniform(0.0, 50.0 * config.max_wait_seconds)]),
        active_remaining_seconds=rng.choice([0.0, rng.uniform(0.0, 50.0 * config.max_wait_seconds)]),
        last_updated=NOW - age,
        release_schedule=tuple(sorted(release_schedule)),
    )


def random_config(rng: random.Random) -> SpilloverConfig:
    return SpilloverConfig(
        max_wait_seconds=rng.choice([0.0, 1.0, 10.0, 60.0]),
        max_latency_penalty_ms=rng.choice([0.0, 25.0, 100.0, 400.0]),
        min_improvement_ratio=rng.choice([0.0, 0.25, 0.5, 1.0]),
        spillover_enabled=rng.random() < 0.9,
        capacity_staleness_threshold_seconds=rng.choice([5.0, 30.0]),
    )


def random_scenario(rng: random.Random):
    config = random_config(rng)
    primary = random_capacity(rng, "dc-primary", config)
    # Half the scenarios draw latencies from a few values, so fallbacks tie.
    tied_latencies = rng.random() < 0.5
    fallbacks = [
        (
            random_capacity(rng, f"dc-fallback-{index}", config),
            rng.choice([20.0, 60.0, 120.0]) if tied_latencies else rng.uniform(1.0, 500.0),
        )
        for index in range(rng.randint(0, 5))
    ]
    primary_rtt_ms = rng.choice([20.0, 60.0]) if tied_latencies else rng.uniform(1.0, 300.0)
    return config, rng.randint(1, 80), primary, fallbacks, primary_rtt_ms


def reference_wait(capacity: DatacenterCapacity, cores_required: int) -> float:
    """AD-43 Part 4, transcribed: the later of the schedule bound and the
    drain bound, for at most every core the datacenter has."""
    if cores_required <= 0:
        return 0.0
    if capacity.total_cores <= 0:
        return math.inf
    needed_cores = min(cores_required, capacity.total_cores)
    if capacity.available_cores >= needed_cores:
        return 0.0
    schedule_bound = 0.0
    freed_cores = capacity.available_cores
    # Cores cannot all be free before the release that frees the last of
    # them -- nor, when the schedule falls short, before its last release.
    for release_offset, released_cores in capacity.release_schedule:
        freed_cores += released_cores
        schedule_bound = release_offset
        if freed_cores >= needed_cores:
            break
    drain_bound = (capacity.active_remaining_seconds + capacity.pending_duration_seconds) / capacity.total_cores
    return max(schedule_bound, drain_bound)


def cores_for_job(capacity: DatacenterCapacity, cores_required: int) -> int:
    """The cores a datacenter can ever devote to the job."""
    return min(cores_required, capacity.total_cores) if capacity.total_cores > 0 else 0


def reference_decision(
    config: SpilloverConfig,
    cores_required: int,
    primary: DatacenterCapacity,
    fallbacks: list[tuple[DatacenterCapacity, float]],
    primary_rtt_ms: float,
) -> tuple[bool, str, str | None]:
    """AD-43 Part 7's decision flow, transcribed."""

    def is_fresh(capacity: DatacenterCapacity) -> bool:
        return NOW - capacity.last_updated <= config.capacity_staleness_threshold_seconds

    def serves_now(capacity: DatacenterCapacity) -> bool:
        return capacity.available_cores >= cores_for_job(capacity, cores_required) if capacity.total_cores > 0 else (
            capacity.available_cores >= cores_required
        )

    if not config.spillover_enabled:
        return False, "spillover_disabled", None
    if not is_fresh(primary):
        return False, "capacity_stale", None
    if serves_now(primary):
        return False, "primary_has_capacity", None
    primary_wait = reference_wait(primary, cores_required)
    if primary_wait <= config.max_wait_seconds:
        return False, "primary_wait_acceptable", None
    eligible = [
        (rtt_ms - primary_rtt_ms, index, capacity)
        for index, (capacity, rtt_ms) in enumerate(fallbacks)
        if serves_now(capacity)
        and is_fresh(capacity)
        and rtt_ms - primary_rtt_ms <= config.max_latency_penalty_ms
        and cores_for_job(capacity, cores_required) >= max(cores_for_job(primary, cores_required), 1)
    ]
    if not eligible:
        return False, "no_spillover_with_capacity", None
    # The fallbacks arrive in the router's ranking (best first): among
    # equally near ones the better ranked is taken.
    _, _, chosen = min(eligible, key=lambda entry: (entry[0], entry[1]))
    if reference_wait(chosen, cores_required) > primary_wait * config.min_improvement_ratio:
        return False, "improvement_insufficient", chosen.datacenter_id
    return True, "spillover_improves_wait_time", chosen.datacenter_id


def evaluate(config, cores_required, primary, fallbacks, primary_rtt_ms):
    return SpilloverEvaluator(config, FixedClock(NOW)).evaluate(
        job_cores_required=cores_required,
        primary_capacity=primary,
        fallback_capacities=fallbacks,
        primary_rtt_ms=primary_rtt_ms,
    )


def with_fields(capacity: DatacenterCapacity, **changes) -> DatacenterCapacity:
    fields = {
        name: getattr(capacity, name)
        for name in (
            "datacenter_id",
            "total_cores",
            "available_cores",
            "pending_workflow_count",
            "pending_duration_seconds",
            "active_remaining_seconds",
            "last_updated",
            "release_schedule",
        )
    }
    fields.update(changes)
    return DatacenterCapacity(**fields)


def seeded_scenarios():
    for seed in SEEDS:
        rng = random.Random(seed)
        for scenario_index in range(SCENARIOS_PER_SEED):
            yield seed, scenario_index, rng, random_scenario(rng)


@pytest.mark.parametrize("seed", SEEDS)
def test_the_wait_for_cores_matches_ad43_part4_and_is_zero_exactly_when_servable(seed: int) -> None:
    rng = random.Random(seed)
    config = SpilloverConfig()
    for _ in range(200):
        capacity = random_capacity(rng, "dc", config)
        for cores_required in range(0, 2 * max(capacity.total_cores, 1) + 2):
            wait = capacity.estimated_wait_for_cores(cores_required)
            assert wait == reference_wait(capacity, cores_required), (capacity, cores_required)
            if cores_required > 0 and capacity.total_cores > 0:
                if capacity.can_serve_immediately(cores_required):
                    assert wait == 0.0, (capacity, cores_required)
                elif capacity.active_remaining_seconds + capacity.pending_duration_seconds > 0.0:
                    assert wait > 0.0, (capacity, cores_required)
                # A requirement beyond the datacenter waits as one for all of it.
                assert wait == capacity.estimated_wait_for_cores(min(cores_required, capacity.total_cores))


@pytest.mark.parametrize("seed", SEEDS)
def test_the_wait_is_monotone_in_its_inputs(seed: int) -> None:
    rng = random.Random(seed)
    config = SpilloverConfig()
    for _ in range(200):
        capacity = random_capacity(rng, "dc", config)
        waits = [capacity.estimated_wait_for_cores(cores) for cores in range(0, capacity.total_cores + 3)]
        assert waits == sorted(waits), capacity
        cores_required = rng.randint(1, max(capacity.total_cores, 1) + 2)
        busier = with_fields(
            capacity,
            active_remaining_seconds=capacity.active_remaining_seconds + rng.uniform(0.0, 1000.0),
            pending_duration_seconds=capacity.pending_duration_seconds + rng.uniform(0.0, 1000.0),
        )
        assert busier.estimated_wait_for_cores(cores_required) >= capacity.estimated_wait_for_cores(cores_required)
        if capacity.available_cores < capacity.total_cores:
            freer = with_fields(capacity, available_cores=capacity.available_cores + 1)
            assert freer.estimated_wait_for_cores(cores_required) <= capacity.estimated_wait_for_cores(
                cores_required
            )


def test_decisions_match_the_ad43_decision_flow() -> None:
    reasons_seen: set[str] = set()
    for seed, scenario_index, _, (config, cores_required, primary, fallbacks, primary_rtt_ms) in seeded_scenarios():
        decision = evaluate(config, cores_required, primary, fallbacks, primary_rtt_ms)
        expected = reference_decision(config, cores_required, primary, fallbacks, primary_rtt_ms)
        assert (decision.should_spillover, decision.reason, decision.spillover_dc) == expected, (
            seed,
            scenario_index,
        )
        assert decision.primary_dc == primary.datacenter_id
        reasons_seen.add(decision.reason)
    # The generator reaches every branch of the flow, or the oracle proves
    # nothing. "improvement_insufficient" is not among them: a candidate
    # must serve the job immediately, so its wait is zero and no ratio of
    # the primary's (at least zero) is below it.
    assert reasons_seen == {
        "spillover_disabled",
        "capacity_stale",
        "primary_has_capacity",
        "primary_wait_acceptable",
        "no_spillover_with_capacity",
        "spillover_improves_wait_time",
    }


def test_a_spillover_never_lands_somewhere_worse() -> None:
    spillovers = 0
    for seed, scenario_index, _, (config, cores_required, primary, fallbacks, primary_rtt_ms) in seeded_scenarios():
        decision = evaluate(config, cores_required, primary, fallbacks, primary_rtt_ms)
        if not decision.should_spillover:
            continue
        spillovers += 1
        target, target_rtt_ms = next(
            (capacity, rtt_ms) for capacity, rtt_ms in fallbacks if capacity.datacenter_id == decision.spillover_dc
        )
        context = (seed, scenario_index)
        assert not target.is_stale(NOW, config.capacity_staleness_threshold_seconds), context
        assert target.can_serve_immediately(cores_required), context
        assert cores_for_job(target, cores_required) >= cores_for_job(primary, cores_required), context
        assert target.available_cores > primary.available_cores, context
        assert target_rtt_ms - primary_rtt_ms <= config.max_latency_penalty_ms, context
        assert decision.latency_penalty_ms == target_rtt_ms - primary_rtt_ms, context
        assert decision.spillover_wait_seconds == target.estimated_wait_for_cores(cores_required), context
        assert decision.primary_wait_seconds == primary.estimated_wait_for_cores(cores_required), context
        assert decision.primary_wait_seconds > config.max_wait_seconds, context
        if math.isfinite(decision.primary_wait_seconds):
            # A primary of no cores waits forever: any fallback improves on it.
            assert decision.spillover_wait_seconds <= decision.primary_wait_seconds * config.min_improvement_ratio
        assert decision.spillover_wait_seconds <= decision.primary_wait_seconds, context
    assert spillovers > 0


def test_a_busier_primary_never_stops_a_spillover_and_a_freer_one_never_starts_one() -> None:
    for seed, scenario_index, rng, (config, cores_required, primary, fallbacks, primary_rtt_ms) in seeded_scenarios():
        decision = evaluate(config, cores_required, primary, fallbacks, primary_rtt_ms)
        busier_primary = with_fields(
            primary,
            active_remaining_seconds=primary.active_remaining_seconds + rng.uniform(0.0, 10_000.0),
            pending_duration_seconds=primary.pending_duration_seconds + rng.uniform(0.0, 10_000.0),
        )
        busier = evaluate(config, cores_required, busier_primary, fallbacks, primary_rtt_ms)
        if decision.should_spillover:
            assert busier.should_spillover and busier.spillover_dc == decision.spillover_dc, (seed, scenario_index)
        if primary.available_cores < primary.total_cores:
            freer_primary = with_fields(primary, available_cores=primary.available_cores + 1)
            freer = evaluate(config, cores_required, freer_primary, fallbacks, primary_rtt_ms)
            if freer.should_spillover:
                assert decision.should_spillover, (seed, scenario_index)


def test_a_farther_fallback_or_a_stricter_threshold_never_starts_a_spillover() -> None:
    for seed, scenario_index, rng, (config, cores_required, primary, fallbacks, primary_rtt_ms) in seeded_scenarios():
        decision = evaluate(config, cores_required, primary, fallbacks, primary_rtt_ms)
        context = (seed, scenario_index)
        if fallbacks:
            farther_index = rng.randrange(len(fallbacks))
            farther_fallbacks = [
                (capacity, rtt_ms + rng.uniform(0.0, 300.0) if index == farther_index else rtt_ms)
                for index, (capacity, rtt_ms) in enumerate(fallbacks)
            ]
            if evaluate(config, cores_required, primary, farther_fallbacks, primary_rtt_ms).should_spillover:
                assert decision.should_spillover, context
        stricter_configs = [
            SpilloverConfig(
                max_wait_seconds=config.max_wait_seconds + rng.uniform(0.0, 100.0),
                max_latency_penalty_ms=config.max_latency_penalty_ms,
                min_improvement_ratio=config.min_improvement_ratio,
                spillover_enabled=config.spillover_enabled,
                capacity_staleness_threshold_seconds=config.capacity_staleness_threshold_seconds,
            ),
            SpilloverConfig(
                max_wait_seconds=config.max_wait_seconds,
                max_latency_penalty_ms=config.max_latency_penalty_ms * rng.uniform(0.0, 1.0),
                min_improvement_ratio=config.min_improvement_ratio,
                spillover_enabled=config.spillover_enabled,
                capacity_staleness_threshold_seconds=config.capacity_staleness_threshold_seconds,
            ),
            SpilloverConfig(
                max_wait_seconds=config.max_wait_seconds,
                max_latency_penalty_ms=config.max_latency_penalty_ms,
                min_improvement_ratio=config.min_improvement_ratio * rng.uniform(0.0, 1.0),
                spillover_enabled=config.spillover_enabled,
                capacity_staleness_threshold_seconds=config.capacity_staleness_threshold_seconds,
            ),
        ]
        for stricter_config in stricter_configs:
            if evaluate(stricter_config, cores_required, primary, fallbacks, primary_rtt_ms).should_spillover:
                assert decision.should_spillover, (context, stricter_config)


def test_decisions_are_deterministic_and_independent_of_fallback_order_up_to_ties() -> None:
    for seed, scenario_index, rng, (config, cores_required, primary, fallbacks, primary_rtt_ms) in seeded_scenarios():
        first = evaluate(config, cores_required, primary, fallbacks, primary_rtt_ms)
        assert evaluate(config, cores_required, primary, fallbacks, primary_rtt_ms) == first
        shuffled = list(fallbacks)
        rng.shuffle(shuffled)
        reordered = evaluate(config, cores_required, primary, shuffled, primary_rtt_ms)
        context = (seed, scenario_index)
        assert (reordered.should_spillover, reordered.reason) == (first.should_spillover, first.reason), context
        assert reordered.latency_penalty_ms == first.latency_penalty_ms, context
        if reordered.spillover_dc != first.spillover_dc:
            # Only datacenters equally near may trade places.
            rtts = {capacity.datacenter_id: rtt_ms for capacity, rtt_ms in fallbacks}
            assert rtts[reordered.spillover_dc] == rtts[first.spillover_dc], context


def test_the_thresholds_are_the_env_settings_at_their_boundaries() -> None:
    env = Env(
        SPILLOVER_MAX_WAIT_SECONDS=12.0,
        SPILLOVER_MAX_LATENCY_PENALTY_MS=40.0,
        SPILLOVER_MIN_IMPROVEMENT_RATIO=0.25,
        CAPACITY_STALENESS_THRESHOLD_SECONDS=7.0,
    )
    evaluator = SpilloverEvaluator.from_env(env, FixedClock(NOW))
    # Primary: one of four cores free, the rest held by work that drains in
    # exactly the wait threshold, then just past it.
    for drain_seconds, expected_reason in (
        (4 * 12.0, "primary_wait_acceptable"),
        (4 * 12.0 + 1e-9, "spillover_improves_wait_time"),
    ):
        primary = DatacenterCapacity("dc-primary", 4, 1, 1, 0.0, drain_seconds, NOW - 7.0)
        fallback = DatacenterCapacity("dc-near", 4, 4, 0, 0.0, 0.0, NOW - 7.0)
        decision = evaluator.evaluate(4, primary, [(fallback, 50.0 + 40.0)], primary_rtt_ms=50.0)
        assert decision.reason == expected_reason
    primary = DatacenterCapacity("dc-primary", 4, 1, 1, 0.0, 1000.0, NOW)
    fallback = DatacenterCapacity("dc-near", 4, 4, 0, 0.0, 0.0, NOW)
    beyond_penalty = evaluator.evaluate(4, primary, [(fallback, 50.0 + 40.0 + 1e-6)], primary_rtt_ms=50.0)
    assert beyond_penalty.reason == "no_spillover_with_capacity"
    stale_primary = DatacenterCapacity("dc-primary", 4, 1, 1, 0.0, 1000.0, NOW - 7.0 - 1e-6)
    assert evaluator.evaluate(4, stale_primary, [(fallback, 50.0)], 50.0).reason == "capacity_stale"
    disabled = SpilloverEvaluator.from_env(Env(SPILLOVER_ENABLED=False), FixedClock(NOW))
    assert disabled.evaluate(4, primary, [(fallback, 50.0)], 50.0).reason == "spillover_disabled"


def test_a_job_never_spills_to_a_datacenter_too_small_to_hold_what_the_primary_would() -> None:
    """A job of ten cores whose primary has five free (and a hundred in all)
    once spilled to a three-core datacenter: idle, it 'served immediately'
    every core it had, so it waited for nothing -- and the job ran on three
    cores for good instead of growing to ten at home."""
    evaluator = SpilloverEvaluator(SpilloverConfig(max_wait_seconds=1.0), FixedClock(NOW))
    primary = DatacenterCapacity("dc-primary", 100, 5, 4, 400.0, 9000.0, NOW)
    small_idle = DatacenterCapacity("dc-small", 3, 3, 0, 0.0, 0.0, NOW)
    large_idle = DatacenterCapacity("dc-large", 16, 16, 0, 0.0, 0.0, NOW)

    decision = evaluator.evaluate(10, primary, [(small_idle, 10.0)], primary_rtt_ms=10.0)
    assert not decision.should_spillover
    assert decision.reason == "no_spillover_with_capacity"

    decision = evaluator.evaluate(10, primary, [(small_idle, 10.0), (large_idle, 60.0)], primary_rtt_ms=10.0)
    assert (decision.should_spillover, decision.spillover_dc) == (True, "dc-large")


def test_scenario_space_is_exercised() -> None:
    combinations = {
        (config.spillover_enabled, bool(fallbacks), primary.total_cores == 0)
        for _, _, _, (config, _, primary, fallbacks, _) in seeded_scenarios()
    }
    assert set(itertools.product([True, False], repeat=3)) <= combinations
