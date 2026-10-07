"""
SWIM failure scenarios (fixes for gaps G1-G8 in the failure scenario analysis):

- Zombie detection: a node marked DEAD that rejoins with a stale incarnation
  is rejected, until the detection window expires; expired death records
  are cleaned up.
- Incarnation persistence: incarnations survive a restart with a bump.
- Partition recovery: the cross-DC correlation detector recommends delaying
  eviction when several datacenters fail together, and calls the healed
  callbacks when the partition heals.
- Suspicion timeouts: a bounded confirmation target lets a large cluster's
  global suspicion approach its minimum timeout, and a tiny cluster never
  waits for confirmations it cannot collect.
"""

import asyncio
import pathlib
from dataclasses import dataclass, field

import pytest

from hyperscale.distributed.datacenters.cross_dc_correlation import (
    CorrelationSeverity,
    CrossDCCorrelationConfig,
    CrossDCCorrelationDetector,
)
from hyperscale.distributed.swim.detection import (
    HierarchicalConfig,
    HierarchicalFailureDetector,
    IncarnationStore,
    IncarnationTracker,
    SuspicionState,
)

# Anything these components log lands in the test's own directory.
pytestmark = pytest.mark.usefixtures("node_directory")

PARTITIONED_DATACENTERS = ["dc-west", "dc-east", "dc-north"]
ALL_DATACENTERS = [*PARTITIONED_DATACENTERS, "dc-south"]


@dataclass
class PartitionCallbackCapture:
    """Records every partition callback the correlation detector makes."""

    partition_healed_calls: list[tuple[list[str], float]] = field(default_factory=list)
    partition_detected_calls: list[tuple[list[str], float]] = field(default_factory=list)

    def on_partition_healed(self, datacenters: list[str], timestamp: float) -> None:
        self.partition_healed_calls.append((datacenters, timestamp))

    def on_partition_detected(self, datacenters: list[str], timestamp: float) -> None:
        self.partition_detected_calls.append((datacenters, timestamp))


async def test_zombie_detection_rejects_stale_incarnation() -> None:
    """A node that died at incarnation 10 must rejoin at 10 + 5 or above."""
    tracker = IncarnationTracker(
        zombie_detection_window_seconds=60.0,
        minimum_rejoin_incarnation_bump=5,
    )
    node = ("127.0.0.1", 9000)
    tracker.record_node_death(node, incarnation_at_death=10)

    assert tracker.is_potential_zombie(node, claimed_incarnation=12), (
        f"incarnation 12 is below the required {tracker.get_required_rejoin_incarnation(node)}: a zombie"
    )
    assert not tracker.is_potential_zombie(node, claimed_incarnation=15), "incarnation 15 rejoins"
    zombie_rejections = tracker.get_stats().get("zombie_rejections", 0)
    assert zombie_rejections == 1, f"one zombie rejection counted, got {zombie_rejections}"


async def test_zombie_detection_window_expiry() -> None:
    """After the detection window, a stale death record no longer rejects."""
    tracker = IncarnationTracker(
        zombie_detection_window_seconds=0.5,
        minimum_rejoin_incarnation_bump=5,
    )
    node = ("127.0.0.1", 9001)
    tracker.record_node_death(node, incarnation_at_death=10)

    assert tracker.is_potential_zombie(node, claimed_incarnation=12), "incarnation 12 is a zombie inside the window"
    await asyncio.sleep(0.6)
    assert not tracker.is_potential_zombie(node, claimed_incarnation=12), (
        "incarnation 12 rejoins once the window expired"
    )


async def test_incarnation_persists_across_restart(tmp_path: pathlib.Path) -> None:
    """G2: a restarted node's incarnation reloads above its last one plus the restart bump."""
    node_address = "127.0.0.1:9000"
    first_store = IncarnationStore(
        storage_directory=tmp_path,
        node_address=node_address,
        restart_incarnation_bump=10,
    )
    initial_incarnation = await first_store.initialize()
    await first_store.update_incarnation(initial_incarnation + 5)
    await first_store.update_incarnation(initial_incarnation + 10)
    incarnation_before_restart = await first_store.get_incarnation()

    restarted_store = IncarnationStore(
        storage_directory=tmp_path,
        node_address=node_address,
        restart_incarnation_bump=10,
    )
    reloaded_incarnation = await restarted_store.initialize()

    expected_minimum = incarnation_before_restart + 10
    assert reloaded_incarnation >= expected_minimum, (
        f"reloaded incarnation {reloaded_incarnation} is at least {expected_minimum}"
    )


async def test_partition_healed_callback_fires() -> None:
    """G6/G7: the detector calls the healed callbacks when a partition heals."""
    detector = CrossDCCorrelationDetector(
        config=CrossDCCorrelationConfig(
            correlation_window_seconds=30.0,
            low_threshold=2,
            medium_threshold=3,
            high_count_threshold=3,
            high_threshold_fraction=0.5,
            failure_confirmation_seconds=0.1,
            recovery_confirmation_seconds=0.1,
        )
    )
    capture = PartitionCallbackCapture()
    detector.register_partition_healed_callback(capture.on_partition_healed)
    detector.register_partition_detected_callback(capture.on_partition_detected)
    for datacenter in ALL_DATACENTERS:
        detector.add_datacenter(datacenter)

    for datacenter in PARTITIONED_DATACENTERS:
        detector.record_failure(datacenter, "unhealthy")
    await asyncio.sleep(0.2)
    for datacenter in PARTITIONED_DATACENTERS:
        detector.record_failure(datacenter, "unhealthy")

    decision = detector.check_correlation("dc-west")
    assert decision.severity in (CorrelationSeverity.MEDIUM, CorrelationSeverity.HIGH), (
        f"three of four datacenters failing together is a partition, got severity {decision.severity.value}"
    )
    detector.mark_partition_detected(decision.affected_datacenters)

    for datacenter in ALL_DATACENTERS:
        detector.record_recovery(datacenter)
    await asyncio.sleep(0.2)
    for datacenter in ALL_DATACENTERS:
        detector.record_recovery(datacenter)

    detector.check_partition_healed()
    assert len(capture.partition_healed_calls) >= 1, "the healed callback fired"
    assert not detector.is_in_partition(), "the detector left the partition state"


async def test_partition_detection_delays_eviction() -> None:
    """Simultaneous failures of several datacenters recommend delaying eviction."""
    detector = CrossDCCorrelationDetector(
        config=CrossDCCorrelationConfig(
            correlation_window_seconds=30.0,
            low_threshold=2,
            medium_threshold=2,
            failure_confirmation_seconds=0.1,
        )
    )
    for datacenter in ["dc-1", "dc-2", "dc-3"]:
        detector.add_datacenter(datacenter)

    detector.record_failure("dc-1", "unhealthy")
    detector.record_failure("dc-2", "unhealthy")
    await asyncio.sleep(0.2)
    detector.record_failure("dc-1", "unhealthy")
    detector.record_failure("dc-2", "unhealthy")

    decision = detector.check_correlation("dc-1")
    assert decision.should_delay_eviction, (
        f"severity {decision.severity.value} recommends delaying eviction ({decision.recommendation})"
    )


async def test_expired_death_records_are_cleaned_up() -> None:
    """cleanup_death_records removes every record older than the window."""
    tracker = IncarnationTracker(
        zombie_detection_window_seconds=0.3,
        minimum_rejoin_incarnation_bump=5,
    )
    for port_offset in range(5):
        tracker.record_node_death(("127.0.0.1", 9000 + port_offset), incarnation_at_death=10)

    await asyncio.sleep(0.4)
    cleaned_records = await tracker.cleanup_death_records()

    assert cleaned_records == 5, f"five records cleaned, got {cleaned_records}"
    remaining_records = tracker.get_stats()["active_death_records"]
    assert remaining_records == 0, f"no death records remain, got {remaining_records}"


async def test_suspicion_timeout_uses_bounded_confirmations() -> None:
    """A large cluster need not collect confirmations proportional to membership."""
    bounded_state = SuspicionState(
        node=("127.0.0.1", 9200),
        incarnation=1,
        start_time=0.0,
        min_timeout=5.0,
        max_timeout=30.0,
        n_members=50,
        required_confirmations=2,
    )
    membership_scaled_state = SuspicionState(
        node=("127.0.0.1", 9201),
        incarnation=1,
        start_time=0.0,
        min_timeout=5.0,
        max_timeout=30.0,
        n_members=50,
    )
    confirmer = ("127.0.0.1", 9300)
    bounded_state.add_confirmation(confirmer)
    membership_scaled_state.add_confirmation(confirmer)

    bounded_timeout = bounded_state.calculate_timeout()
    membership_scaled_timeout = membership_scaled_state.calculate_timeout()

    assert bounded_timeout < membership_scaled_timeout, (
        f"bounded timeout {bounded_timeout:.3f}s is below the membership-scaled {membership_scaled_timeout:.3f}s"
    )
    assert bounded_timeout < 20.0, f"bounded timeout {bounded_timeout:.3f}s is below 20s"


async def test_global_detector_sets_confirmation_target() -> None:
    """Global suspicions carry the configured confirmation target."""
    detector = HierarchicalFailureDetector(
        config=HierarchicalConfig(
            global_min_timeout=5.0,
            global_max_timeout=30.0,
            global_required_confirmations=2,
        ),
        get_n_members=lambda: 50,
    )
    try:
        target = ("127.0.0.1", 9400)
        created = await detector.suspect_global(target, 1, ("127.0.0.1", 9401))
        state = await detector.get_global_suspicion_state(target)

        assert created, "the global suspicion was created"
        assert state is not None, "the global suspicion has a state"
        assert state.required_confirmations == 2, f"target is 2, got {state.required_confirmations}"
        initial_timeout = state.calculate_timeout()
        assert initial_timeout < 20.0, f"initial timeout {initial_timeout:.3f}s is below 20s"
    finally:
        await detector.stop()


async def test_global_detector_clamps_impossible_confirmations() -> None:
    """A two-member cluster never waits for confirmations it cannot collect."""
    detector = HierarchicalFailureDetector(
        config=HierarchicalConfig(
            global_min_timeout=5.0,
            global_max_timeout=30.0,
            global_required_confirmations=2,
        ),
        get_n_members=lambda: 2,
    )
    try:
        target = ("127.0.0.1", 9500)
        created = await detector.suspect_global(target, 1, ("127.0.0.1", 9501))
        state = await detector.get_global_suspicion_state(target)

        assert created, "the global suspicion was created"
        assert state is not None, "the global suspicion has a state"
        assert state.required_confirmations == 0, f"target clamps to 0, got {state.required_confirmations}"
        initial_timeout = state.calculate_timeout()
        assert initial_timeout == state.min_timeout, (
            f"initial timeout {initial_timeout:.3f}s is the minimum {state.min_timeout}s"
        )
    finally:
        await detector.stop()
