"""
AD-52 section 10: a datacenter founded again is a new datacenter to route.

A datacenter whose managers all lost their data founds a new cluster under
the same datacenter id. The gate kept what it learned of the old
incarnation -- its observed dispatch latency, the SLO violation it was
timing -- and routed the new one by it. Now the gate remembers which
cluster each datacenter's managers last answered as; a view of a
different cluster forgets the datacenter's learned latency and its SLO
violation clock, once, logged -- and nothing of any other datacenter's.

Driven through the real ``GateServer._reconcile_datacenter_managers`` with
the real ``ObservedLatencyTracker`` and ``SLOHealthClassifier``.
"""

import asyncio
from types import SimpleNamespace

import pytest

from hyperscale.distributed.cluster.models.cluster_view import ClusterView
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.routing import BlendedScoringConfig, ObservedLatencyTracker
from hyperscale.distributed.slo.latency_observation import LatencyObservation
from hyperscale.distributed.slo.latency_slo import LatencySLO
from hyperscale.distributed.slo.slo_config import SLOConfig
from hyperscale.distributed.slo.slo_health_classifier import SLOHealthClassifier
from hyperscale.logging.hyperscale_logging_models import DatacenterRegenerated

REGENERATED_DATACENTER = "dc-east"
OTHER_DATACENTER = "dc-west"
MANAGER_ADDR = ("10.0.0.10", 9000)
OTHER_MANAGER_ADDR = ("10.0.1.10", 9000)
SETTINGS = Env()
LATENCY_SLO = LatencySLO.from_env(SETTINGS)
SLO_CONFIG = SLOConfig.from_env(SETTINGS)


class SteppedClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        # A real log write yields to the loop.
        await asyncio.sleep(0)
        self.entries.append(entry)


def view_of(cluster_uuid: str, manager_addr: tuple[str, int]) -> ClusterView:
    return ClusterView(
        cluster_uuid=cluster_uuid,
        applied_index=1,
        holders={},
        cohort=frozenset({manager_addr}),
        voters=frozenset(),
        learners=frozenset(),
        mode="open",
    )


def violating_observation(datacenter_id: str, now: float) -> LatencyObservation:
    return LatencyObservation(
        target_id=datacenter_id,
        p50_ms=LATENCY_SLO.p50_target_ms * 10,
        p95_ms=LATENCY_SLO.p95_target_ms * 10,
        p99_ms=LATENCY_SLO.p99_target_ms * 10,
        sample_count=LATENCY_SLO.min_sample_count,
        window_start=now - 1.0,
        window_end=now,
    )


def make_gate() -> tuple[GateServer, RecordingLogger, SteppedClock]:
    clock = SteppedClock()
    logger = RecordingLogger()
    gate = object.__new__(GateServer)
    gate._clock = clock
    gate._host, gate._tcp_port = "10.0.0.1", 9100
    gate._node_id = SimpleNamespace(short="gate-1")
    gate._udp_logger = logger
    gate._observed_latency_tracker = ObservedLatencyTracker(config=BlendedScoringConfig.from_env(SETTINGS), clock=clock)
    gate._slo_health_classifier = SLOHealthClassifier.from_env(SETTINGS)
    gate._datacenter_cluster_uuids = {}
    gate._datacenter_managers = {REGENERATED_DATACENTER: [MANAGER_ADDR], OTHER_DATACENTER: [OTHER_MANAGER_ADDR]}
    gate._declared_datacenter_managers = {}
    gate._datacenter_manager_udp = {}
    gate._joined_peer_store = None
    return gate, logger, clock


async def learn(gate: GateServer, clock: SteppedClock, datacenter_id: str) -> None:
    """A dispatch latency observed, and an SLO violation under way long
    enough to grade the datacenter."""
    await gate._observed_latency_tracker.record_job_latency(datacenter_id, 40.0)
    gate._slo_health_classifier.compute_health_signal(
        datacenter_id, LATENCY_SLO, violating_observation(datacenter_id, clock.now), clock.now
    )


def violation_is_timed(gate: GateServer, clock: SteppedClock, datacenter_id: str) -> bool:
    """Whether a violation already under way grades the datacenter: one
    the classifier is not timing reads HEALTHY at its first sight."""
    later = clock.now + max(
        SLO_CONFIG.busy_window_seconds, SLO_CONFIG.degraded_window_seconds, SLO_CONFIG.unhealthy_window_seconds
    )
    return (
        gate._slo_health_classifier.compute_health_signal(
            datacenter_id, LATENCY_SLO, violating_observation(datacenter_id, later), later
        )
        != "HEALTHY"
    )


@pytest.mark.asyncio
async def test_a_datacenter_founded_again_forgets_what_was_learned_of_it_once() -> None:
    gate, logger, clock = make_gate()
    await gate._reconcile_datacenter_managers(REGENERATED_DATACENTER, view_of("cluster-1", MANAGER_ADDR))
    await gate._reconcile_datacenter_managers(OTHER_DATACENTER, view_of("cluster-9", OTHER_MANAGER_ADDR))
    await learn(gate, clock, REGENERATED_DATACENTER)
    await learn(gate, clock, OTHER_DATACENTER)

    # The same cluster again: nothing forgotten.
    await gate._reconcile_datacenter_managers(REGENERATED_DATACENTER, view_of("cluster-1", MANAGER_ADDR))
    assert gate._observed_latency_tracker.get_observed_latency(REGENERATED_DATACENTER)[0] == 40.0
    assert not [entry for entry in logger.entries if isinstance(entry, DatacenterRegenerated)]

    # Founded again: two views of the new cluster reconciled at once
    # forget it once.
    await asyncio.gather(
        gate._reconcile_datacenter_managers(REGENERATED_DATACENTER, view_of("cluster-2", MANAGER_ADDR)),
        gate._reconcile_datacenter_managers(REGENERATED_DATACENTER, view_of("cluster-2", MANAGER_ADDR)),
    )

    assert gate._observed_latency_tracker.get_observed_latency(REGENERATED_DATACENTER) == (0.0, 0.0)
    assert not violation_is_timed(gate, clock, REGENERATED_DATACENTER)
    ((regenerated,),) = [[entry for entry in logger.entries if isinstance(entry, DatacenterRegenerated)]]
    assert (regenerated.datacenter_id, regenerated.previous_cluster_uuid, regenerated.cluster_uuid) == (
        REGENERATED_DATACENTER,
        "cluster-1",
        "cluster-2",
    )
    # The other datacenter keeps what was learned of it.
    assert gate._observed_latency_tracker.get_observed_latency(OTHER_DATACENTER)[0] == 40.0
    assert violation_is_timed(gate, clock, OTHER_DATACENTER)


@pytest.mark.asyncio
async def test_the_first_view_of_a_datacenter_forgets_nothing() -> None:
    gate, logger, clock = make_gate()
    await learn(gate, clock, REGENERATED_DATACENTER)

    await gate._reconcile_datacenter_managers(REGENERATED_DATACENTER, view_of("cluster-1", MANAGER_ADDR))

    assert gate._observed_latency_tracker.get_observed_latency(REGENERATED_DATACENTER)[0] == 40.0
    assert not [entry for entry in logger.entries if isinstance(entry, DatacenterRegenerated)]
