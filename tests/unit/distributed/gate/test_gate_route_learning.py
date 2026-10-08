"""
AD-45 adaptive route learning, as the gate wires it.

The gate sampled each job's whole run (dispatch to final result) -- a
duration the job's workflows set, not the datacenter -- and blended it
with Vivaldi RTT in milliseconds; the router's Vivaldi step then
overwrote the blend, and it looked coordinates up by datacenter id,
which nothing is keyed by, so neither signal reached a routing decision.
The tracker read a module-global clock, ignored the configured latency
cap and enabled flag, and its confidence fell from full to zero at the
staleness bound instead of decaying across it.

* a datacenter's sample is its time to accept a dispatch, from the first
  manager tried, recorded on the gate's clock; a failed dispatch is not
  sampled;
* confidence ramps with samples and decays linearly to zero at the
  staleness bound; a datacenter with none left is forgotten;
* samples are capped, and a sample taken at clock zero counts;
* the latency estimator reads each datacenter's coordinate from its
  managers and blends observed latency into the Vivaldi prediction by
  confidence, so both signals reach the score;
* with adaptive routing disabled the gate offers no observed evidence.
"""

import json
from types import SimpleNamespace

import pytest

from hyperscale.logging.hyperscale_logging_models import ObservedLatencyRecorded
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.gate.config import derive_datacenter_leader_failover_seconds
from hyperscale.distributed.nodes.gate.dispatch_coordinator import GateDispatchCoordinator
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.models.coordinates import VivaldiConfig
from hyperscale.distributed.models.network_coordinate import NetworkCoordinate
from hyperscale.distributed.swim.coordinates.coordinate_tracker import CoordinateTracker
from hyperscale.distributed.routing import (
    ConstrainedPlacementPolicy,
    BlendedLatencyScorer,
    BlendedScoringConfig,
    DatacenterCandidate,
    DatacenterLatencyEstimator,
    GateJobRouter,
    JobDispatchCooldowns,
    ObservedLatencyTracker,
    RoutingScorer,
    ScoringConfig,
)

STALENESS_SECONDS = 300.0
NEAR_DATACENTER = "dc-near"
UNPLACED_DATACENTER = "dc-unplaced"
NEAR_RTT_MS = 20.0


class SteppedClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now


def make_tracker(clock: SteppedClock, **settings) -> ObservedLatencyTracker:
    env = Env(
        ADAPTIVE_ROUTING_EWMA_ALPHA=0.5,
        ADAPTIVE_ROUTING_MIN_SAMPLES=2,
        ADAPTIVE_ROUTING_MAX_STALENESS_SECONDS=STALENESS_SECONDS,
        **settings,
    )
    return ObservedLatencyTracker(config=BlendedScoringConfig.from_env(env), clock=clock)


@pytest.mark.asyncio
async def test_confidence_ramps_with_samples_and_decays_linearly_with_age() -> None:
    clock = SteppedClock()
    tracker = make_tracker(clock)

    await tracker.record_job_latency(NEAR_DATACENTER, 40.0)
    assert tracker.get_observed_latency(NEAR_DATACENTER) == (40.0, 0.5)

    await tracker.record_job_latency(NEAR_DATACENTER, 60.0)
    assert tracker.get_observed_latency(NEAR_DATACENTER) == (50.0, 1.0)

    clock.now += STALENESS_SECONDS * 0.2
    assert tracker.get_observed_latency(NEAR_DATACENTER)[1] == pytest.approx(0.8)

    clock.now += STALENESS_SECONDS * 0.3
    assert tracker.get_observed_latency(NEAR_DATACENTER)[1] == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_a_datacenter_without_confidence_is_forgotten() -> None:
    clock = SteppedClock()
    tracker = make_tracker(clock)
    await tracker.record_job_latency(NEAR_DATACENTER, 40.0)

    clock.now += STALENESS_SECONDS
    assert await tracker.cleanup_stale_entries() == []

    clock.now += 1.0
    assert tracker.get_observed_latency(NEAR_DATACENTER)[1] == 0.0
    assert await tracker.cleanup_stale_entries() == [NEAR_DATACENTER]
    assert tracker.get_observed_latency(NEAR_DATACENTER) == (0.0, 0.0)


@pytest.mark.asyncio
async def test_samples_are_capped() -> None:
    tracker = make_tracker(SteppedClock(), ADAPTIVE_ROUTING_LATENCY_CAP_MS=1000.0)

    await tracker.record_job_latency(NEAR_DATACENTER, 5000.0)

    assert tracker.get_observed_latency(NEAR_DATACENTER)[0] == 1000.0


@pytest.mark.asyncio
async def test_a_sample_taken_at_clock_zero_counts() -> None:
    tracker = make_tracker(SteppedClock(now=0.0))

    await tracker.record_job_latency(NEAR_DATACENTER, 40.0)

    assert tracker.get_observed_latency(NEAR_DATACENTER) == (40.0, 0.5)
    assert tracker.get_metrics()["per_dc"][NEAR_DATACENTER]["stale"] is False


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


def make_dispatching_coordinator(
    clock: SteppedClock,
    tracker: ObservedLatencyTracker,
    manager_outcomes: dict[tuple[str, int], tuple[float, bool]],
    logger: RecordingLogger | None = None,
) -> tuple[GateDispatchCoordinator, list[tuple[str, tuple[str, int], float]]]:
    """A coordinator whose managers answer after the given delays."""
    manager_successes: list[tuple[str, tuple[str, int], float]] = []
    coordinator = object.__new__(GateDispatchCoordinator)
    coordinator._clock = clock
    coordinator._logger = logger if logger is not None else RecordingLogger()
    coordinator._observed_latency_tracker = tracker
    coordinator._datacenter_managers = {NEAR_DATACENTER: list(manager_outcomes)}
    coordinator._datacenter_leader_failover_seconds = derive_datacenter_leader_failover_seconds(Env())
    coordinator._manager_selector = SimpleNamespace(
        ordered_managers=lambda datacenter, job_id, managers: managers,
        record_success=lambda datacenter, manager, latency_ms: manager_successes.append(
            (datacenter, manager, latency_ms)
        ),
        record_failure=lambda datacenter, manager: None,
    )
    coordinator._task_runner = SimpleNamespace(run=lambda *args, **kwargs: None)
    coordinator._confirm_manager_for_dc = lambda datacenter, manager: None
    coordinator._suspect_manager_for_dc = lambda datacenter, manager: None
    coordinator._record_forward_throughput_event = lambda: None
    coordinator._record_forward_attempt_event = lambda: None

    async def answer(
        datacenter: str,
        manager_addr: tuple[str, int],
        submission: object,
        retry_deadline_at: float,
    ) -> tuple[tuple[str, int] | None, str | None]:
        delay_seconds, accepted = manager_outcomes[manager_addr]
        clock.now += delay_seconds
        return (manager_addr, None) if accepted else (None, "refused")

    coordinator._try_dispatch_to_manager = answer
    return coordinator, manager_successes


@pytest.mark.asyncio
async def test_a_datacenter_is_sampled_at_its_time_to_accept_a_dispatch() -> None:
    clock = SteppedClock()
    tracker = make_tracker(clock)
    refusing_manager = ("10.0.0.1", 9000)
    accepting_manager = ("10.0.0.2", 9000)
    logger = RecordingLogger()
    coordinator, manager_successes = make_dispatching_coordinator(
        clock,
        tracker,
        {refusing_manager: (2.0, False), accepting_manager: (1.0, True)},
        logger,
    )

    accepted, _, accepted_by = await coordinator._try_dispatch_to_dc("job-1", NEAR_DATACENTER, object())

    assert (accepted, accepted_by) == (True, accepting_manager)
    assert tracker.get_observed_latency(NEAR_DATACENTER)[0] == pytest.approx(3000.0)
    assert manager_successes == [(NEAR_DATACENTER, accepting_manager, pytest.approx(1000.0))]
    # AD-45: the sample is reported as it is folded in.
    (recorded,) = [entry for entry in logger.entries if isinstance(entry, ObservedLatencyRecorded)]
    assert (recorded.datacenter_id, recorded.sample_count) == (NEAR_DATACENTER, 1)
    assert recorded.latency_ms == pytest.approx(3000.0)


@pytest.mark.asyncio
async def test_a_failed_dispatch_is_not_sampled() -> None:
    clock = SteppedClock()
    tracker = make_tracker(clock)
    coordinator, _ = make_dispatching_coordinator(clock, tracker, {("10.0.0.1", 9000): (2.0, False)})

    accepted, _, _ = await coordinator._try_dispatch_to_dc("job-1", NEAR_DATACENTER, object())

    assert accepted is False
    assert tracker.get_observed_latency(NEAR_DATACENTER) == (0.0, 0.0)


class StubCoordinateTracker:
    """Vivaldi estimates as fixed per-coordinate RTTs of full quality, from
    a converged local coordinate, so routing treats them as evidence."""

    def get_config(self) -> VivaldiConfig:
        return VivaldiConfig()

    def is_converged(self) -> bool:
        return True

    def estimate_rtt_ucb_ms(self, coordinate: SimpleNamespace) -> float:
        return coordinate.rtt_ms

    def coordinate_quality(self, coordinate: SimpleNamespace) -> float:
        return 1.0


def make_candidate(datacenter_id: str) -> DatacenterCandidate:
    return DatacenterCandidate(
        datacenter_id=datacenter_id,
        health_bucket="HEALTHY",
        available_cores=8,
        total_cores=8,
        queue_depth=0,
        total_managers=1,
        healthy_managers=1,
        circuit_breaker_pressure=0.0,
        health_severity_weight=1.0,
        slo_routing_factor=1.0,
    )


def make_router(
    coordinates: dict[str, SimpleNamespace],
    observed: dict[str, tuple[float, float]],
) -> GateJobRouter:
    return GateJobRouter(
        get_datacenter_candidates=lambda: [
            make_candidate(NEAR_DATACENTER),
            make_candidate(UNPLACED_DATACENTER),
        ],
        placement_policy=ConstrainedPlacementPolicy(
            latency_estimator=DatacenterLatencyEstimator(
            coordinate_tracker=StubCoordinateTracker(),
            get_datacenter_coordinate=coordinates.get,
            get_observed_latency=lambda datacenter_id: observed.get(datacenter_id, (0.0, 0.0)),
        ),
            scorer=RoutingScorer(ScoringConfig.from_env(Env())),
        ),
        dispatch_cooldowns=JobDispatchCooldowns(clock=SteppedClock(), cooldown_seconds=10.0),
    )


def test_observed_latency_is_blended_into_the_vivaldi_prediction_by_confidence() -> None:
    near_coordinate = {NEAR_DATACENTER: SimpleNamespace(rtt_ms=NEAR_RTT_MS)}
    estimator = DatacenterLatencyEstimator(
        coordinate_tracker=StubCoordinateTracker(),
        get_datacenter_coordinate=near_coordinate.get,
        get_observed_latency=lambda datacenter_id: (40.0, 0.5) if datacenter_id == NEAR_DATACENTER else (0.0, 0.0),
    )

    estimates = estimator.estimate([NEAR_DATACENTER, UNPLACED_DATACENTER])

    assert estimates[NEAR_DATACENTER] == pytest.approx(0.5 * 40.0 + 0.5 * NEAR_RTT_MS)
    # Nothing is known about the unplaced datacenter: it is as far as the
    # farthest evidence the gate holds (the near datacenter's 40ms sample).
    assert estimates[UNPLACED_DATACENTER] == pytest.approx(40.0)


def test_the_blended_latency_decides_the_route() -> None:
    coordinates = {
        NEAR_DATACENTER: SimpleNamespace(rtt_ms=NEAR_RTT_MS),
        UNPLACED_DATACENTER: SimpleNamespace(rtt_ms=NEAR_RTT_MS * 2),
    }
    by_prediction = make_router(coordinates, observed={})
    observed_faster = {NEAR_DATACENTER: (1000.0, 1.0), UNPLACED_DATACENTER: (1.0, 1.0)}
    by_observation = make_router(coordinates, observed=observed_faster)

    assert by_prediction.route_job("job-1", 1, None).primary_datacenters == [NEAR_DATACENTER]
    assert by_observation.route_job("job-1", 1, None).primary_datacenters == [UNPLACED_DATACENTER]


MANAGER_LEADER_UDP = ("10.0.0.5", 9001)
MANAGER_FOLLOWER_UDP = ("10.0.0.6", 9001)


def manager_coordinate_payload(first_component: float) -> bytes:
    """A manager's Vivaldi piggyback, as SWIM carries it after ``#|v``."""
    dimensions = VivaldiConfig().dimensions
    coordinate = NetworkCoordinate(
        vec=[first_component] + [0.0] * (dimensions - 1),
        height=0.1,
        adjustment=0.0,
        error=0.5,
        sample_count=3,
    )
    return json.dumps(coordinate.to_dict()).encode()


def gate_learning_coordinates(best_heartbeat_by_datacenter: dict) -> GateServer:
    """A gate whose SWIM layer learns coordinates through its real intake
    (``_process_vivaldi_piggyback`` into its own ``CoordinateTracker``)."""
    gate = object.__new__(GateServer)
    gate._vivaldi_config = VivaldiConfig()
    gate._coordinate_tracker = CoordinateTracker(config=gate._vivaldi_config)
    gate._pending_probe_start = {}
    gate._clock = SteppedClock()
    gate._health_coordinator = SimpleNamespace(
        get_best_manager_heartbeat=lambda datacenter_id: best_heartbeat_by_datacenter.get(
            datacenter_id, (None, 0, 0)
        )
    )
    return gate


def manager_heartbeat(node_id: str, udp_address: tuple[str, int]) -> SimpleNamespace:
    return SimpleNamespace(node_id=node_id, udp_host=udp_address[0], udp_port=udp_address[1])


@pytest.mark.asyncio
async def test_a_datacenter_is_reached_at_its_most_authoritative_managers_coordinate() -> None:
    """SWIM records a manager's coordinate under the UDP address it came
    from; the gate reads the datacenter's coordinate under the UDP address
    its best manager's heartbeat names. Keyed by node id, the lookup never
    found one and AD-36 routed without Vivaldi."""
    gate = gate_learning_coordinates(
        {NEAR_DATACENTER: (manager_heartbeat("manager-leader", MANAGER_LEADER_UDP), 2, 2)}
    )

    await gate._process_vivaldi_piggyback(manager_coordinate_payload(7.0), MANAGER_LEADER_UDP)
    await gate._process_vivaldi_piggyback(manager_coordinate_payload(-3.0), MANAGER_FOLLOWER_UDP)

    near_coordinate = gate._get_datacenter_coordinate(NEAR_DATACENTER)
    assert near_coordinate is not None
    assert near_coordinate.vec[0] == 7.0
    assert gate._get_datacenter_coordinate(UNPLACED_DATACENTER) is None


@pytest.mark.asyncio
async def test_an_ack_measured_coordinate_is_read_under_the_same_key() -> None:
    """An ack to the gate's own probe takes the RTT-measuring branch
    (``update_peer_coordinate``); it lands under the same key."""
    gate = gate_learning_coordinates(
        {NEAR_DATACENTER: (manager_heartbeat("manager-leader", MANAGER_LEADER_UDP), 1, 1)}
    )
    gate._pending_probe_start[MANAGER_LEADER_UDP] = gate._clock.monotonic() - 0.02

    await gate._process_vivaldi_piggyback(manager_coordinate_payload(4.0), MANAGER_LEADER_UDP)

    assert gate._get_datacenter_coordinate(NEAR_DATACENTER) is not None


@pytest.mark.asyncio
async def test_a_heartbeat_naming_no_udp_address_has_no_coordinate() -> None:
    gate = gate_learning_coordinates(
        {NEAR_DATACENTER: (manager_heartbeat("manager-leader", ("", 0)), 1, 1)}
    )
    await gate._process_vivaldi_piggyback(manager_coordinate_payload(7.0), MANAGER_LEADER_UDP)

    assert gate._get_datacenter_coordinate(NEAR_DATACENTER) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("adaptive_routing_enabled", "expected_evidence"),
    [(True, (40.0, 1.0)), (False, (0.0, 0.0))],
)
async def test_the_gate_takes_observed_latency_only_with_adaptive_routing_enabled(
    adaptive_routing_enabled: bool,
    expected_evidence: tuple[float, float],
) -> None:
    tracker = make_tracker(SteppedClock())
    await tracker.record_job_latency(NEAR_DATACENTER, 40.0)
    await tracker.record_job_latency(NEAR_DATACENTER, 40.0)
    scorer = BlendedLatencyScorer(tracker, adaptive_routing_enabled=adaptive_routing_enabled)

    assert scorer.get_observed_latency(NEAR_DATACENTER) == expected_evidence
