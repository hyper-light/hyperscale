#!/usr/bin/env python3
"""
SWIM Failure Scenario Integration Tests.

Tests critical failure scenarios in the SWIM protocol implementation:
1. Zombie detection - Dead nodes rejoining with stale incarnations
2. Partition recovery - Callbacks when partitions heal
3. Incarnation persistence - Incarnations survive restarts

These tests validate the fixes implemented for gaps G1-G8 in
the failure scenario analysis.
"""

import asyncio
import os
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from hyperscale.distributed.swim.detection import (
    IncarnationTracker,
    IncarnationStore,
)
from hyperscale.distributed.datacenters.cross_dc_correlation import (
    CrossDCCorrelationDetector,
    CrossDCCorrelationConfig,
    CorrelationSeverity,
)
from hyperscale.logging.config.logging_config import LoggingConfig

_logging_config = LoggingConfig()
_logging_config.update(log_directory=os.getcwd())


@dataclass
class CallbackCapture:
    partition_healed_calls: list[tuple[list[str], float]] = field(default_factory=list)
    partition_detected_calls: list[tuple[list[str], float]] = field(
        default_factory=list
    )

    def on_partition_healed(self, datacenters: list[str], timestamp: float) -> None:
        self.partition_healed_calls.append((datacenters, timestamp))

    def on_partition_detected(self, datacenters: list[str], timestamp: float) -> None:
        self.partition_detected_calls.append((datacenters, timestamp))


async def scenario_zombie_detection_rejects_stale_incarnation() -> bool:
    """
    Test that the incarnation tracker rejects zombie nodes with stale incarnations.

    A zombie is a node that was marked DEAD but tries to rejoin with an
    incarnation lower than required (death_incarnation + minimum_bump).
    """
    print(f"\n{'=' * 70}")
    print("TEST: Zombie Detection - Rejects Stale Incarnation")
    print(f"{'=' * 70}")

    tracker = IncarnationTracker(
        zombie_detection_window_seconds=60.0,
        minimum_rejoin_incarnation_bump=5,
    )

    node = ("127.0.0.1", 9000)
    death_incarnation = 10

    print("\n[1/4] Recording node death at incarnation 10...")
    tracker.record_node_death(node, death_incarnation)

    print("\n[2/4] Checking if incarnation 12 is rejected as zombie...")
    is_zombie_12 = tracker.is_potential_zombie(node, claimed_incarnation=12)
    required = tracker.get_required_rejoin_incarnation(node)
    print(f"  Required incarnation: {required}")
    print(f"  Incarnation 12 is zombie: {is_zombie_12}")

    print("\n[3/4] Checking if incarnation 15 is accepted...")
    is_zombie_15 = tracker.is_potential_zombie(node, claimed_incarnation=15)
    print(f"  Incarnation 15 is zombie: {is_zombie_15}")

    print("\n[4/4] Verifying zombie rejection count...")
    stats = tracker.get_stats()
    rejections = stats.get("zombie_rejections", 0)
    print(f"  Zombie rejections: {rejections}")

    passed = is_zombie_12 and not is_zombie_15 and rejections == 1

    print(f"\n{'=' * 70}")
    result = "PASSED" if passed else "FAILED"
    print(f"TEST RESULT: {result}")
    print(f"{'=' * 70}")

    return passed


async def scenario_zombie_detection_window_expiry() -> bool:
    """
    Test that zombie detection expires after the window.

    After zombie_detection_window_seconds, a node should be able to
    rejoin with any incarnation since the death record is stale.
    """
    print(f"\n{'=' * 70}")
    print("TEST: Zombie Detection - Window Expiry")
    print(f"{'=' * 70}")

    tracker = IncarnationTracker(
        zombie_detection_window_seconds=0.5,
        minimum_rejoin_incarnation_bump=5,
    )

    node = ("127.0.0.1", 9001)
    death_incarnation = 10

    print("\n[1/3] Recording node death at incarnation 10...")
    tracker.record_node_death(node, death_incarnation)

    print("\n[2/3] Checking immediately - should be zombie...")
    is_zombie_immediate = tracker.is_potential_zombie(node, claimed_incarnation=12)
    print(f"  Incarnation 12 is zombie immediately: {is_zombie_immediate}")

    print("\n[3/3] Waiting for window to expire and checking again...")
    await asyncio.sleep(0.6)
    is_zombie_after = tracker.is_potential_zombie(node, claimed_incarnation=12)
    print(f"  Incarnation 12 is zombie after expiry: {is_zombie_after}")

    passed = is_zombie_immediate and not is_zombie_after

    print(f"\n{'=' * 70}")
    result = "PASSED" if passed else "FAILED"
    print(f"TEST RESULT: {result}")
    print(f"{'=' * 70}")

    return passed


async def scenario_incarnation_persistence() -> bool:
    """
    Test that incarnation numbers persist and reload correctly.

    This validates G2 fix - the IncarnationStore should persist
    incarnation numbers to disk and reload them on restart with
    an appropriate bump.
    """
    print(f"\n{'=' * 70}")
    print("TEST: Incarnation Persistence")
    print(f"{'=' * 70}")

    with tempfile.TemporaryDirectory() as temp_dir:
        storage_path = Path(temp_dir)
        node_address = "127.0.0.1:9000"

        print("\n[1/4] Creating initial incarnation store...")
        store1 = IncarnationStore(
            storage_directory=storage_path,
            node_address=node_address,
            restart_incarnation_bump=10,
        )
        initial_incarnation = await store1.initialize()
        print(f"  Initial incarnation: {initial_incarnation}")

        print("\n[2/4] Incrementing incarnation several times...")
        await store1.update_incarnation(initial_incarnation + 5)
        await store1.update_incarnation(initial_incarnation + 10)
        current = await store1.get_incarnation()
        print(f"  Current incarnation after updates: {current}")

        print("\n[3/4] Creating new store (simulating restart)...")
        store2 = IncarnationStore(
            storage_directory=storage_path,
            node_address=node_address,
            restart_incarnation_bump=10,
        )
        reloaded_incarnation = await store2.initialize()
        print(f"  Reloaded incarnation: {reloaded_incarnation}")

        print("\n[4/4] Verifying incarnation is higher than before restart...")
        expected_minimum = current + 10
        is_higher = reloaded_incarnation >= expected_minimum
        print(f"  Expected minimum: {expected_minimum}")
        print(f"  Is higher: {is_higher}")

        passed = is_higher

    print(f"\n{'=' * 70}")
    result = "PASSED" if passed else "FAILED"
    print(f"TEST RESULT: {result}")
    print(f"{'=' * 70}")

    return passed


async def scenario_partition_healed_callback() -> bool:
    """
    Test that partition healed callbacks are invoked correctly.

    This validates G6/G7 fix - the CrossDCCorrelationDetector should
    invoke callbacks when a partition heals.
    """
    print(f"\n{'=' * 70}")
    print("TEST: Partition Healed Callback")
    print(f"{'=' * 70}")

    config = CrossDCCorrelationConfig(
        correlation_window_seconds=30.0,
        low_threshold=2,
        medium_threshold=3,
        high_count_threshold=3,
        high_threshold_fraction=0.5,
        failure_confirmation_seconds=0.1,
        recovery_confirmation_seconds=0.1,
    )

    detector = CrossDCCorrelationDetector(config=config)
    capture = CallbackCapture()

    detector.register_partition_healed_callback(capture.on_partition_healed)
    detector.register_partition_detected_callback(capture.on_partition_detected)

    print("\n[1/5] Adding datacenters...")
    for dc in ["dc-west", "dc-east", "dc-north", "dc-south"]:
        detector.add_datacenter(dc)
    print("  Added 4 datacenters")

    print("\n[2/5] Recording failures to trigger partition...")
    detector.record_failure("dc-west", "unhealthy")
    detector.record_failure("dc-east", "unhealthy")
    detector.record_failure("dc-north", "unhealthy")
    await asyncio.sleep(0.2)
    detector.record_failure("dc-west", "unhealthy")
    detector.record_failure("dc-east", "unhealthy")
    detector.record_failure("dc-north", "unhealthy")

    print("\n[3/5] Checking correlation and marking partition...")
    decision = detector.check_correlation("dc-west")
    print(f"  Correlation severity: {decision.severity.value}")

    if decision.severity in (CorrelationSeverity.MEDIUM, CorrelationSeverity.HIGH):
        detector.mark_partition_detected(decision.affected_datacenters)
        print(f"  Partition marked, affected DCs: {decision.affected_datacenters}")

    print(f"  Partition detected callbacks: {len(capture.partition_detected_calls)}")

    print("\n[4/5] Recording recoveries...")
    for dc in ["dc-west", "dc-east", "dc-north", "dc-south"]:
        detector.record_recovery(dc)
    await asyncio.sleep(0.2)
    for dc in ["dc-west", "dc-east", "dc-north", "dc-south"]:
        detector.record_recovery(dc)

    print("\n[5/5] Checking if partition healed...")
    healed = detector.check_partition_healed()
    print(f"  Partition healed: {healed}")
    print(f"  Partition healed callbacks: {len(capture.partition_healed_calls)}")

    in_partition = detector.is_in_partition()
    print(f"  Still in partition: {in_partition}")

    passed = len(capture.partition_healed_calls) >= 1 and not in_partition

    print(f"\n{'=' * 70}")
    result = "PASSED" if passed else "FAILED"
    print(f"TEST RESULT: {result}")
    print(f"{'=' * 70}")

    return passed


async def scenario_partition_detection_delays_eviction() -> bool:
    """
    Test that partition detection recommends delaying eviction.

    When multiple DCs fail simultaneously, the correlation detector
    should recommend delaying eviction (should_delay_eviction=True).
    """
    print(f"\n{'=' * 70}")
    print("TEST: Partition Detection Delays Eviction")
    print(f"{'=' * 70}")

    config = CrossDCCorrelationConfig(
        correlation_window_seconds=30.0,
        low_threshold=2,
        medium_threshold=2,
        failure_confirmation_seconds=0.1,
    )

    detector = CrossDCCorrelationDetector(config=config)

    print("\n[1/3] Adding datacenters...")
    for dc in ["dc-1", "dc-2", "dc-3"]:
        detector.add_datacenter(dc)

    print("\n[2/3] Recording simultaneous failures...")
    detector.record_failure("dc-1", "unhealthy")
    detector.record_failure("dc-2", "unhealthy")
    await asyncio.sleep(0.2)
    detector.record_failure("dc-1", "unhealthy")
    detector.record_failure("dc-2", "unhealthy")

    print("\n[3/3] Checking correlation decision...")
    decision = detector.check_correlation("dc-1")
    print(f"  Severity: {decision.severity.value}")
    print(f"  Should delay eviction: {decision.should_delay_eviction}")
    print(f"  Recommendation: {decision.recommendation}")

    passed = decision.should_delay_eviction

    print(f"\n{'=' * 70}")
    result = "PASSED" if passed else "FAILED"
    print(f"TEST RESULT: {result}")
    print(f"{'=' * 70}")

    return passed


async def scenario_death_record_cleanup() -> bool:
    """
    Test that death records are cleaned up properly.

    The cleanup_death_records method should remove records older
    than the zombie detection window.
    """
    print(f"\n{'=' * 70}")
    print("TEST: Death Record Cleanup")
    print(f"{'=' * 70}")

    tracker = IncarnationTracker(
        zombie_detection_window_seconds=0.3,
        minimum_rejoin_incarnation_bump=5,
    )

    print("\n[1/3] Recording multiple node deaths...")
    nodes = [("127.0.0.1", 9000 + i) for i in range(5)]
    for node in nodes:
        tracker.record_node_death(node, incarnation_at_death=10)

    stats_before = tracker.get_stats()
    print(f"  Active death records before: {stats_before['active_death_records']}")

    print("\n[2/3] Waiting for records to expire...")
    await asyncio.sleep(0.4)

    print("\n[3/3] Running cleanup and checking...")
    cleaned = await tracker.cleanup_death_records()
    stats_after = tracker.get_stats()
    print(f"  Records cleaned: {cleaned}")
    print(f"  Active death records after: {stats_after['active_death_records']}")

    passed = cleaned == 5 and stats_after["active_death_records"] == 0

    print(f"\n{'=' * 70}")
    result = "PASSED" if passed else "FAILED"
    print(f"TEST RESULT: {result}")
    print(f"{'=' * 70}")

    return passed


async def run_all_scenarios() -> dict[str, bool]:
    results = {}

    scenarios = [
        (
            "zombie_detection_rejects_stale",
            scenario_zombie_detection_rejects_stale_incarnation,
        ),
        ("zombie_detection_window_expiry", scenario_zombie_detection_window_expiry),
        ("incarnation_persistence", scenario_incarnation_persistence),
        ("partition_healed_callback", scenario_partition_healed_callback),
        (
            "partition_detection_delays_eviction",
            scenario_partition_detection_delays_eviction,
        ),
        ("death_record_cleanup", scenario_death_record_cleanup),
    ]

    for name, scenario_func in scenarios:
        try:
            results[name] = await scenario_func()
        except Exception:
            import traceback

            print(f"\nScenario {name} failed with exception:")
            traceback.print_exc()
            results[name] = False

    return results


def print_summary(results: dict[str, bool]) -> None:
    print(f"\n{'=' * 70}")
    print("FAILURE SCENARIOS TEST SUMMARY")
    print(f"{'=' * 70}")

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, result in results.items():
        status = "PASS" if result else "FAIL"
        print(f"  {name}: [{status}]")

    print(f"\n  Total: {passed}/{total} scenarios passed")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    results = asyncio.run(run_all_scenarios())
    print_summary(results)

    all_passed = all(results.values())
    sys.exit(0 if all_passed else 1)
