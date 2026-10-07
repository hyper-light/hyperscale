"""
AD-36 datacenter routing, as the gate routes a job.

The router scored with literal weights its config never reached, capped a
load factor that could not reach its cap, gave every datacenter without a
Vivaldi coordinate a fixed 100ms, cut every job to at most two primary
datacenters whatever it asked for, ran a hysteresis machine whose verdict
never reached the decision, and leaked a routing state per routed job.

* a datacenter's latency is a confidence-weighted blend of its Vivaldi RTT
  bound and its observed latency, with whatever weight the evidence lacks
  given to a conservative prior: the farthest latency evidenced for any
  datacenter -- so an unknown datacenter is as far as the farthest known,
  and with no evidence anywhere latency drops out of the ranking;
* an unconverged gate does not trust its own Vivaldi distances;
* the load factor takes its weights from the configuration and measures
  the queue against the datacenter's own cores;
* health order is never traded for latency; a job gets the datacenters it
  asked for, filled in health order, the rest are its fallbacks;
* a datacenter that failed the job's dispatch goes last for that job until
  its cooldown passes, and the cooldowns are bounded;
* equal datacenters split jobs by rendezvous hash -- stable per job.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import random
import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import DatacenterStatus
from hyperscale.distributed.models.coordinates import VivaldiConfig
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.routing import (
    ConstrainedPlacementPolicy,
    PlacementPlan,
    PlacementRequest,
    DatacenterCandidate,
    DatacenterLatencyEstimator,
    ExclusionReason,
    GateJobRouter,
    JobDispatchCooldowns,
    RoutingScorer,
    ScoringConfig,
)

COOLDOWN_SECONDS = 10.0
RTT_FLOOR_MS = VivaldiConfig().rtt_min_ms


class SteppedClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now


class StubCoordinateTracker:
    """Vivaldi estimates as fixed per-coordinate RTTs and qualities."""

    def __init__(self, converged: bool = True) -> None:
        self.converged = converged

    def get_config(self) -> VivaldiConfig:
        return VivaldiConfig()

    def is_converged(self) -> bool:
        return self.converged

    def estimate_rtt_ucb_ms(self, coordinate: SimpleNamespace) -> float:
        return coordinate.rtt_ms

    def coordinate_quality(self, coordinate: SimpleNamespace) -> float:
        return coordinate.quality


def coordinate(rtt_ms: float, quality: float = 1.0) -> SimpleNamespace:
    return SimpleNamespace(rtt_ms=rtt_ms, quality=quality)


def make_estimator(
    coordinates: dict[str, SimpleNamespace],
    observed: dict[str, tuple[float, float]] | None = None,
    converged: bool = True,
) -> DatacenterLatencyEstimator:
    observed = observed or {}
    return DatacenterLatencyEstimator(
        coordinate_tracker=StubCoordinateTracker(converged=converged),
        get_datacenter_coordinate=coordinates.get,
        get_observed_latency=lambda datacenter_id: observed.get(datacenter_id, (0.0, 0.0)),
    )


def candidate(
    datacenter_id: str,
    health_bucket: str = "HEALTHY",
    available_cores: int = 8,
    total_cores: int = 8,
    queue_depth: int = 0,
    total_managers: int = 1,
    healthy_managers: int = 1,
    circuit_breaker_pressure: float = 0.0,
    dispatch_latency_p95_ms: float | None = None,
) -> DatacenterCandidate:
    return DatacenterCandidate(
        datacenter_id=datacenter_id,
        health_bucket=health_bucket,
        available_cores=available_cores,
        total_cores=total_cores,
        queue_depth=queue_depth,
        total_managers=total_managers,
        healthy_managers=healthy_managers,
        circuit_breaker_pressure=circuit_breaker_pressure,
        health_severity_weight=1.0,
        slo_routing_factor=1.0,
        dispatch_latency_p95_ms=dispatch_latency_p95_ms,
    )


def make_router(
    candidates: list[DatacenterCandidate],
    coordinates: dict[str, SimpleNamespace] | None = None,
    clock: SteppedClock | None = None,
) -> GateJobRouter:
    return GateJobRouter(
        get_datacenter_candidates=lambda: candidates,
        placement_policy=ConstrainedPlacementPolicy(
            latency_estimator=make_estimator(coordinates or {}),
            scorer=RoutingScorer(ScoringConfig.from_env(Env())),
        ),
        dispatch_cooldowns=JobDispatchCooldowns(
            clock=clock or SteppedClock(),
            cooldown_seconds=COOLDOWN_SECONDS,
        ),
    )


# ---------------------------------------------------------------------------
# Latency estimation
# ---------------------------------------------------------------------------


def test_an_unknown_datacenter_is_as_far_as_the_farthest_evidenced() -> None:
    estimator = make_estimator({"dc-near": coordinate(20.0), "dc-far": coordinate(80.0)})

    estimates = estimator.estimate(["dc-near", "dc-far", "dc-unknown"])

    assert estimates == {"dc-near": 20.0, "dc-far": 80.0, "dc-unknown": 80.0}


def test_a_low_quality_coordinate_is_pulled_toward_the_farthest_evidence() -> None:
    estimator = make_estimator(
        {"dc-doubtful": coordinate(20.0, quality=0.25), "dc-far": coordinate(100.0)}
    )

    estimates = estimator.estimate(["dc-doubtful", "dc-far"])

    assert estimates["dc-doubtful"] == pytest.approx(0.25 * 20.0 + 0.75 * 100.0)


def test_a_zero_quality_coordinate_is_no_evidence() -> None:
    estimator = make_estimator(
        {"dc-near": coordinate(20.0), "dc-unsampled": coordinate(5000.0, quality=0.0)}
    )

    estimates = estimator.estimate(["dc-near", "dc-unsampled"])

    assert estimates == {"dc-near": 20.0, "dc-unsampled": 20.0}


def test_with_no_evidence_every_datacenter_is_estimated_alike() -> None:
    estimates = make_estimator({}).estimate(["dc-a", "dc-b", "dc-c"])

    assert set(estimates.values()) == {RTT_FLOOR_MS}


def test_an_unconverged_gate_ranks_by_observed_latency_alone() -> None:
    estimator = make_estimator(
        coordinates={"dc-a": coordinate(500.0), "dc-b": coordinate(1.0)},
        observed={"dc-a": (10.0, 1.0), "dc-b": (30.0, 1.0)},
        converged=False,
    )

    assert estimator.estimate(["dc-a", "dc-b"]) == {"dc-a": 10.0, "dc-b": 30.0}


def test_no_estimate_falls_below_the_rtt_floor() -> None:
    estimator = make_estimator({}, observed={"dc-instant": (0.0, 1.0)})

    assert estimator.estimate(["dc-instant"]) == {"dc-instant": RTT_FLOOR_MS}


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def test_the_load_factor_takes_its_weights_from_the_configuration() -> None:
    scorer = RoutingScorer(
        ScoringConfig(utilization_weight=0.5, queue_weight=0.25, circuit_pressure_weight=0.25)
    )

    score = scorer.score_datacenter(
        candidate(
            "dc-a",
            available_cores=4,
            total_cores=8,
            queue_depth=8,
            circuit_breaker_pressure=0.5,
        ),
        latency_ms=40.0,
    )

    assert score.load_factor == pytest.approx(1.0 + 0.5 * 0.5 + 0.25 * 0.5 + 0.25 * 0.5)
    assert score.final_score == pytest.approx(40.0 * score.load_factor)


def test_the_queue_weighs_against_the_datacenters_own_cores() -> None:
    scorer = RoutingScorer(ScoringConfig.from_env(Env()))

    small = scorer.score_datacenter(
        candidate("dc-small", available_cores=10, total_cores=10, queue_depth=10),
        latency_ms=10.0,
    )
    large = scorer.score_datacenter(
        candidate("dc-large", available_cores=1000, total_cores=1000, queue_depth=10),
        latency_ms=10.0,
    )

    assert small.load_factor > large.load_factor


def test_a_datacenter_of_unknown_capacity_counts_as_saturated() -> None:
    settings = Env()
    scorer = RoutingScorer(ScoringConfig.from_env(settings))

    score = scorer.score_datacenter(
        candidate("dc-unknown", available_cores=0, total_cores=0),
        latency_ms=10.0,
    )

    assert score.load_factor == pytest.approx(1.0 + settings.ROUTING_UTILIZATION_WEIGHT)


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def test_health_order_is_never_traded_for_latency() -> None:
    router = make_router(
        [candidate("dc-busy-near", health_bucket="BUSY"), candidate("dc-healthy-far")],
        coordinates={"dc-busy-near": coordinate(1.0), "dc-healthy-far": coordinate(1000.0)},
    )

    decision = router.route_job("job-1", 1, None)

    assert decision.primary_datacenters == ["dc-healthy-far"]
    assert decision.fallback_datacenters == ["dc-busy-near"]


def test_a_job_gets_the_datacenters_it_asked_for_filled_in_health_order() -> None:
    router = make_router(
        [
            candidate("dc-degraded", health_bucket="DEGRADED"),
            candidate("dc-busy-far", health_bucket="BUSY"),
            candidate("dc-healthy"),
            candidate("dc-busy-near", health_bucket="BUSY"),
        ],
        coordinates={
            "dc-degraded": coordinate(1.0),
            "dc-busy-far": coordinate(90.0),
            "dc-healthy": coordinate(50.0),
            "dc-busy-near": coordinate(10.0),
        },
    )

    decision = router.route_job("job-1", 3, None)

    assert decision.primary_datacenters == ["dc-healthy", "dc-busy-near", "dc-busy-far"]
    assert decision.fallback_datacenters == ["dc-degraded"]
    assert decision.worst_primary_health_bucket == "BUSY"


def test_load_decides_between_equidistant_datacenters() -> None:
    router = make_router(
        [
            candidate("dc-loaded", available_cores=1, total_cores=10),
            candidate("dc-idle", available_cores=9, total_cores=10),
        ],
        coordinates={"dc-loaded": coordinate(30.0), "dc-idle": coordinate(30.0)},
    )

    assert router.route_job("job-1", 1, None).primary_datacenters == ["dc-idle"]


def test_the_placement_constraint_bounds_where_the_job_runs() -> None:
    router = make_router(
        [candidate("dc-a"), candidate("dc-b")],
        coordinates={"dc-a": coordinate(1.0), "dc-b": coordinate(100.0)},
    )

    pinned = router.route_job("job-1", 1, {"dc-b"})
    unsatisfiable = router.route_job("job-2", 1, {"dc-elsewhere"})

    assert (pinned.primary_datacenters, pinned.fallback_datacenters) == (["dc-b"], [])
    assert unsatisfiable.primary_datacenters == []
    assert unsatisfiable.worst_primary_health_bucket is None


def test_every_excluded_datacenter_carries_its_reason() -> None:
    router = make_router(
        [
            candidate("dc-down", health_bucket="UNHEALTHY"),
            candidate("dc-starting", health_bucket="INITIALIZING"),
            candidate("dc-managerless", total_managers=0, healthy_managers=0),
            candidate("dc-tripped", total_managers=2, healthy_managers=0, circuit_breaker_pressure=1.0),
            candidate("dc-ok"),
        ]
    )

    decision = router.route_job("job-1", 1, None)

    assert decision.primary_datacenters == ["dc-ok"]
    assert decision.exclusions == {
        "dc-down": ExclusionReason.UNHEALTHY_STATUS,
        "dc-starting": ExclusionReason.INITIALIZING,
        "dc-managerless": ExclusionReason.NO_REGISTERED_MANAGERS,
        "dc-tripped": ExclusionReason.ALL_MANAGERS_CIRCUIT_OPEN,
    }


def test_a_datacenter_that_failed_the_jobs_dispatch_goes_last_until_its_cooldown_passes() -> None:
    clock = SteppedClock()
    router = make_router(
        [candidate("dc-best"), candidate("dc-next"), candidate("dc-busy", health_bucket="BUSY")],
        coordinates={
            "dc-best": coordinate(10.0),
            "dc-next": coordinate(20.0),
            "dc-busy": coordinate(30.0),
        },
        clock=clock,
    )

    router.record_dispatch_failure("job-1", "dc-best")

    cooling = router.route_job("job-1", 1, None)
    assert cooling.primary_datacenters == ["dc-next"]
    assert cooling.fallback_datacenters == ["dc-busy", "dc-best"]
    assert cooling.cooling_datacenters == frozenset({"dc-best"})
    assert router.route_job("job-2", 1, None).primary_datacenters == ["dc-best"]

    clock.now += COOLDOWN_SECONDS
    assert router.route_job("job-1", 1, None).primary_datacenters == ["dc-best"]


def test_equal_datacenters_split_jobs_by_rendezvous_hash_stable_per_job() -> None:
    router = make_router([candidate("dc-a"), candidate("dc-b")])

    first_choices = [
        router.route_job(f"job-{index}", 1, None).primary_datacenters[0]
        for index in range(200)
    ]
    repeated_choices = [
        router.route_job(f"job-{index}", 1, None).primary_datacenters[0]
        for index in range(200)
    ]

    assert first_choices == repeated_choices
    assert min(first_choices.count("dc-a"), first_choices.count("dc-b")) > 50


# ---------------------------------------------------------------------------
# Cooldown bookkeeping
# ---------------------------------------------------------------------------


def test_expired_cooldowns_are_swept_even_for_jobs_never_routed_again() -> None:
    clock = SteppedClock()
    cooldowns = JobDispatchCooldowns(clock=clock, cooldown_seconds=COOLDOWN_SECONDS)

    for index in range(100):
        cooldowns.record_failure(f"job-{index}", "dc-a")
    assert cooldowns.tracked_job_count() == 100

    clock.now += COOLDOWN_SECONDS
    cooldowns.record_failure("job-late", "dc-a")

    assert cooldowns.tracked_job_count() == 1


def test_a_finished_job_drops_its_cooldowns() -> None:
    cooldowns = JobDispatchCooldowns(clock=SteppedClock(), cooldown_seconds=COOLDOWN_SECONDS)
    cooldowns.record_failure("job-1", "dc-a")

    cooldowns.clear_job("job-1")

    assert cooldowns.cooling_datacenters("job-1") == frozenset()
    assert cooldowns.tracked_job_count() == 0


def test_reading_a_jobs_expired_cooldowns_releases_them() -> None:
    clock = SteppedClock()
    cooldowns = JobDispatchCooldowns(clock=clock, cooldown_seconds=COOLDOWN_SECONDS)
    cooldowns.record_failure("job-1", "dc-a")
    clock.now += COOLDOWN_SECONDS / 2
    cooldowns.record_failure("job-1", "dc-b")

    clock.now += COOLDOWN_SECONDS / 2
    assert cooldowns.cooling_datacenters("job-1") == frozenset({"dc-b"})

    clock.now += COOLDOWN_SECONDS / 2
    assert cooldowns.cooling_datacenters("job-1") == frozenset()
    assert cooldowns.tracked_job_count() == 0


# ---------------------------------------------------------------------------
# The gate's selection
# ---------------------------------------------------------------------------


def make_gate(
    candidates: list[DatacenterCandidate],
    health_by_datacenter: dict[str, str],
) -> GateServer:
    gate = object.__new__(GateServer)
    gate._job_router = make_router(candidates)
    gate._health_coordinator = SimpleNamespace(
        get_all_datacenter_health=lambda: {
            datacenter_id: DatacenterStatus(dc_id=datacenter_id, health=health)
            for datacenter_id, health in health_by_datacenter.items()
        }
    )
    gate._task_runner = SimpleNamespace(run=lambda *args, **kwargs: None)
    gate._udp_logger = SimpleNamespace(log=AsyncMock())
    gate._host = "127.0.0.1"
    gate._tcp_port = 9000
    gate._node_id = SimpleNamespace(short="gate-a")
    return gate


async def test_with_no_eligible_datacenter_the_job_is_refused_as_unhealthy() -> None:
    gate = make_gate(
        [candidate("dc-down", health_bucket="UNHEALTHY")],
        {"dc-down": "unhealthy"},
    )

    assert await gate._select_datacenters_with_fallback(1, None, "job-1") == ([], [], "unhealthy")


async def test_a_short_selection_while_a_datacenter_initializes_is_refused_as_initializing() -> None:
    gate = make_gate(
        [candidate("dc-up"), candidate("dc-starting", health_bucket="INITIALIZING")],
        {"dc-up": "healthy", "dc-starting": "initializing"},
    )

    assert await gate._select_datacenters_with_fallback(2, None, "job-1") == ([], [], "initializing")
    assert await gate._select_datacenters_with_fallback(1, None, "job-1") == (["dc-up"], [], "healthy")


async def test_a_routed_job_reports_the_worst_bucket_among_its_primaries() -> None:
    gate = make_gate(
        [candidate("dc-healthy"), candidate("dc-busy", health_bucket="BUSY")],
        {"dc-healthy": "healthy", "dc-busy": "busy"},
    )

    assert await gate._select_datacenters_with_fallback(2, None, "job-1") == (
        ["dc-healthy", "dc-busy"],
        [],
        "busy",
    )


def test_a_replacement_is_never_a_datacenter_the_job_already_occupies() -> None:
    router = make_router(
        [candidate("dc-running"), candidate("dc-lost"), candidate("dc-free", health_bucket="BUSY")],
        coordinates={
            "dc-running": coordinate(1.0),
            "dc-lost": coordinate(2.0),
            "dc-free": coordinate(500.0),
        },
    )

    replacement = router.route_job(
        "job-1",
        1,
        None,
        occupied_datacenters=frozenset({"dc-running", "dc-lost"}),
    )
    pinned_and_occupied = router.route_job(
        "job-1",
        1,
        {"dc-running"},
        occupied_datacenters=frozenset({"dc-running"}),
    )

    assert (replacement.primary_datacenters, replacement.fallback_datacenters) == (["dc-free"], [])
    assert pinned_and_occupied.primary_datacenters == []


# ---------------------------------------------------------------------------
# AD-36 Part 11 counters
# ---------------------------------------------------------------------------


def test_the_router_counts_its_decisions_exclusions_cooldowns_and_fallbacks() -> None:
    candidates = [candidate("dc-down", health_bucket="UNHEALTHY"), candidate("dc-busy", health_bucket="BUSY")]
    router = make_router(candidates)

    router.route_job("job-1", 1, None)
    router.route_job("job-2", 1, None)
    router.route_job("job-3", 1, {"dc-nowhere"})
    router.record_dispatch_failure("job-1", "dc-busy")
    router.record_fallback_used("dc-busy", "dc-other")

    assert router.get_metrics() == {
        "decision:BUSY": 2,
        "decision:none": 1,
        "exclusion:unhealthy_status": 3,
        "fallback:dc-busy>dc-other": 1,
        "cooldowns": 1,
    }


# ---------------------------------------------------------------------------
# AD-36 Part 12 success criteria 1 and 2 (3 -- failover speed -- rides the
# detection timings and is measured by the gate fault simulations)
# ---------------------------------------------------------------------------

# A spread of datacenter round trips, near to far.
CRITERIA_RTTS_MS = {"dc-1": 12.0, "dc-2": 35.0, "dc-3": 70.0, "dc-4": 140.0, "dc-5": 260.0}
CRITERIA_JOBS = 2000


def median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2


def test_routing_halves_the_median_round_trip_of_random_routing() -> None:
    """Criterion 1: at least 50% lower median RTT than random routing."""
    router = make_router(
        [candidate(datacenter_id) for datacenter_id in CRITERIA_RTTS_MS],
        coordinates={datacenter_id: coordinate(rtt_ms) for datacenter_id, rtt_ms in CRITERIA_RTTS_MS.items()},
    )
    draw = random.Random(36)

    routed = [
        CRITERIA_RTTS_MS[router.route_job(f"job-{index}", 1, None).primary_datacenters[0]]
        for index in range(CRITERIA_JOBS)
    ]
    randomly_placed = [CRITERIA_RTTS_MS[draw.choice(sorted(CRITERIA_RTTS_MS))] for _ in range(CRITERIA_JOBS)]

    assert median(routed) <= 0.5 * median(randomly_placed), (median(routed), median(randomly_placed))


def test_equivalent_datacenters_share_jobs_evenly() -> None:
    """Criterion 2: across datacenters routing cannot tell apart, the
    coefficient of variation of the jobs each receives stays under 0.3."""
    datacenter_ids = sorted(CRITERIA_RTTS_MS)
    router = make_router(
        [candidate(datacenter_id) for datacenter_id in datacenter_ids],
        coordinates={datacenter_id: coordinate(50.0) for datacenter_id in datacenter_ids},
    )

    jobs_per_datacenter = dict.fromkeys(datacenter_ids, 0)
    for index in range(CRITERIA_JOBS):
        jobs_per_datacenter[router.route_job(f"job-{index}", 1, None).primary_datacenters[0]] += 1

    counts = list(jobs_per_datacenter.values())
    mean = sum(counts) / len(counts)
    standard_deviation = (sum((count - mean) ** 2 for count in counts) / len(counts)) ** 0.5
    assert standard_deviation / mean < 0.3, jobs_per_datacenter


# ---------------------------------------------------------------------------
# D-62 placement policy: dispatch latency budget, pluggability
# ---------------------------------------------------------------------------

LATENCY_BUDGET_MS = 100.0


def test_a_latency_budget_keeps_the_job_off_a_datacenter_whose_p95_exceeds_it() -> None:
    # dc-slow is AD-36's choice (idle, nearer) but answers dispatches over
    # the budget; dc-fast is busier and farther but within it.
    router = make_router(
        [
            candidate("dc-slow", dispatch_latency_p95_ms=LATENCY_BUDGET_MS * 3),
            candidate("dc-fast", available_cores=2, dispatch_latency_p95_ms=LATENCY_BUDGET_MS / 2),
        ],
        coordinates={"dc-slow": coordinate(10.0), "dc-fast": coordinate(40.0)},
    )

    unbudgeted = router.route_job("job-1", 1, None)
    budgeted = router.route_job("job-2", 1, None, dispatch_latency_budget_ms=LATENCY_BUDGET_MS)

    assert unbudgeted.primary_datacenters == ["dc-slow"]
    assert (budgeted.primary_datacenters, budgeted.fallback_datacenters) == (["dc-fast"], [])
    assert budgeted.exclusions == {"dc-slow": ExclusionReason.OVER_DISPATCH_LATENCY_BUDGET}
    assert not budgeted.latency_budget_relaxed
    assert router.get_metrics()["exclusion:over_dispatch_latency_budget"] == 1


def test_a_datacenter_without_a_gradeable_digest_meets_any_budget() -> None:
    router = make_router(
        [candidate("dc-unmeasured"), candidate("dc-slow", dispatch_latency_p95_ms=LATENCY_BUDGET_MS * 2)],
        coordinates={"dc-unmeasured": coordinate(40.0), "dc-slow": coordinate(10.0)},
    )

    decision = router.route_job("job-1", 1, None, dispatch_latency_budget_ms=LATENCY_BUDGET_MS)

    assert decision.primary_datacenters == ["dc-unmeasured"]


def test_when_no_datacenter_meets_the_budget_the_job_goes_nearest_to_it() -> None:
    # Both over; dc-near-budget is AD-36's last choice but the least over.
    router = make_router(
        [
            candidate("dc-far-over", dispatch_latency_p95_ms=LATENCY_BUDGET_MS * 5),
            candidate("dc-near-budget", available_cores=1, dispatch_latency_p95_ms=LATENCY_BUDGET_MS * 2),
        ],
        coordinates={"dc-far-over": coordinate(10.0), "dc-near-budget": coordinate(80.0)},
    )

    decision = router.route_job("job-1", 1, None, dispatch_latency_budget_ms=LATENCY_BUDGET_MS)

    assert (decision.primary_datacenters, decision.fallback_datacenters) == (["dc-near-budget"], ["dc-far-over"])
    assert decision.latency_budget_relaxed
    assert decision.exclusions == {}


def test_fewer_datacenters_within_the_budget_than_asked_for_fill_from_the_nearest_over() -> None:
    router = make_router(
        [
            candidate("dc-within", dispatch_latency_p95_ms=LATENCY_BUDGET_MS / 2),
            candidate("dc-far-over", dispatch_latency_p95_ms=LATENCY_BUDGET_MS * 5),
            candidate("dc-near-over", dispatch_latency_p95_ms=LATENCY_BUDGET_MS * 2),
        ],
        coordinates={"dc-within": coordinate(50.0), "dc-far-over": coordinate(10.0), "dc-near-over": coordinate(80.0)},
    )

    decision = router.route_job("job-1", 2, None, dispatch_latency_budget_ms=LATENCY_BUDGET_MS)

    assert decision.primary_datacenters == ["dc-within", "dc-near-over"]
    assert decision.fallback_datacenters == ["dc-far-over"]
    assert decision.latency_budget_relaxed


def test_spillover_never_moves_a_share_further_over_the_budget_than_its_primary() -> None:
    router = make_router(
        [
            candidate("dc-within", dispatch_latency_p95_ms=LATENCY_BUDGET_MS / 2),
            candidate("dc-unmeasured"),
            candidate("dc-near-over", dispatch_latency_p95_ms=LATENCY_BUDGET_MS * 2),
            candidate("dc-far-over", dispatch_latency_p95_ms=LATENCY_BUDGET_MS * 5),
        ]
    )
    fallbacks = ["dc-unmeasured", "dc-near-over", "dc-far-over"]

    from_within = router.spillover_candidates("job-1", "dc-within", fallbacks, LATENCY_BUDGET_MS)
    from_near_over = router.spillover_candidates(
        "job-1", "dc-near-over", ["dc-within", "dc-far-over"], LATENCY_BUDGET_MS
    )
    unbudgeted = router.spillover_candidates("job-1", "dc-within", fallbacks, 0.0)

    assert from_within == ["dc-unmeasured"]
    assert from_near_over == ["dc-within"]
    assert unbudgeted == fallbacks


class ReversedPlacementPolicy:
    """A policy of an operator's own: every candidate, in reverse order."""

    def place(self, request: PlacementRequest, candidates: list[DatacenterCandidate]) -> PlacementPlan:
        return PlacementPlan(ordered=candidates[::-1], scores={}, exclusions={}, latency_budget_relaxed=False)

    def spillover_candidates(
        self,
        request: PlacementRequest,
        primary_datacenter: str,
        fallback_datacenters: list[str],
        candidates: list[DatacenterCandidate],
    ) -> list[str]:
        return []


def test_the_router_places_by_whatever_policy_it_is_given() -> None:
    router = GateJobRouter(
        get_datacenter_candidates=lambda: [candidate("dc-a"), candidate("dc-b"), candidate("dc-c")],
        placement_policy=ReversedPlacementPolicy(),
        dispatch_cooldowns=JobDispatchCooldowns(clock=SteppedClock(), cooldown_seconds=COOLDOWN_SECONDS),
    )

    decision = router.route_job("job-1", 2, None)

    assert (decision.primary_datacenters, decision.fallback_datacenters) == (["dc-c", "dc-b"], ["dc-a"])
    assert router.spillover_candidates("job-1", "dc-c", ["dc-a"], LATENCY_BUDGET_MS) == []
