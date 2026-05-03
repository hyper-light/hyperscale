#!/usr/bin/env python3
"""AD-42 Phase E integration tests for SLO end-to-end gossip.

Exercises the full manager-records-latency → ManagerHeartbeat
SLO fields → gate ingestion → AD-36 routing factor application
pipeline. Each test stops short of standing up real gate/manager
servers — the wire-format and aggregation logic round-trip is
sufficient to verify the contract.

Coverage:

  1. test_slo_summary_round_trip — SLOSummary wire format
     round-trips losslessly.
  2. test_manager_heartbeat_carries_slo — ManagerHeartbeat
     embeds and exposes all 7 SLO fields.
  3. test_gate_ingests_freshest_summary — multiple managers in
     one DC; gate picks the freshest reporter.
  4. test_routing_score_deprioritizes_violators — DC violating
     SLO scores higher (worse) than compliant DC with identical
     other inputs.
  5. test_neutral_baseline_when_no_observations — DCs with no
     latency observations route as if compliant (factor=1.0).

Run as::

    python tests/integration/slo/test_slo_dissemination.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from hyperscale.distributed.models import ManagerHeartbeat
from hyperscale.distributed.nodes.gate.models.dc_health_state import (
    DCHealthState,
)
from hyperscale.distributed.routing.routing_state import (
    DatacenterRoutingScore,
)
from hyperscale.distributed.slo import SLOSummary
from hyperscale.distributed.slo.latency_observation import LatencyObservation


_FAILURES: list[str] = []


def check(condition: bool, label: str) -> None:
    if condition:
        print(f"  ✓ {label}")
    else:
        print(f"  ✗ {label}")
        _FAILURES.append(label)


# ============================================================================
# Test 1 — SLOSummary wire round-trip
# ============================================================================


def test_slo_summary_round_trip() -> None:
    print("\n[1] SLOSummary wire-format round-trip")

    obs = LatencyObservation(
        target_id="workflows",
        p50_ms=42.0,
        p95_ms=160.0,
        p99_ms=350.0,
        sample_count=200,
        window_start=0.0,
        window_end=300.0,
    )
    summary = SLOSummary.from_observation(observation=obs)
    encoded = summary.to_bytes()
    restored = SLOSummary.from_bytes(encoded)
    check(restored is not None, "decode produced a value")
    check(restored == summary, "wire round-trip preserves all fields")

    # Empty baseline behavior
    empty = SLOSummary.empty()
    check(empty.is_empty(), "empty summary reports is_empty()")
    check(
        empty.compliance_score == 1.0,
        "empty compliance_score is neutral 1.0",
    )
    check(
        empty.routing_factor == 1.0,
        "empty routing_factor is neutral 1.0",
    )

    # Malformed bytes return None instead of raising
    check(
        SLOSummary.from_bytes(b"not:enough") is None,
        "malformed bytes safely return None",
    )


# ============================================================================
# Test 2 — ManagerHeartbeat carries SLO fields
# ============================================================================


def test_manager_heartbeat_carries_slo() -> None:
    print("\n[2] ManagerHeartbeat embeds SLO fields with neutral defaults")

    # Default heartbeat — peers that pre-date Phase E see neutral
    # baseline.
    hb_default = ManagerHeartbeat(
        node_id="m1",
        datacenter="dc-east",
        is_leader=True,
        term=1,
        version=1,
        active_jobs=0,
        active_workflows=0,
        worker_count=1,
        healthy_worker_count=1,
        available_cores=4,
        total_cores=4,
    )
    check(hb_default.slo_p50_ms == 0.0, "default slo_p50_ms = 0.0")
    check(
        hb_default.slo_compliance_score == 1.0,
        "default slo_compliance_score = 1.0 (neutral)",
    )
    check(
        hb_default.slo_routing_factor == 1.0,
        "default slo_routing_factor = 1.0 (neutral)",
    )
    check(hb_default.slo_sample_count == 0, "default slo_sample_count = 0")

    # Populated heartbeat
    hb_populated = ManagerHeartbeat(
        node_id="m1",
        datacenter="dc-east",
        is_leader=True,
        term=1,
        version=1,
        active_jobs=0,
        active_workflows=0,
        worker_count=2,
        healthy_worker_count=2,
        available_cores=4,
        total_cores=4,
        slo_p50_ms=42.0,
        slo_p95_ms=160.0,
        slo_p99_ms=350.0,
        slo_sample_count=200,
        slo_compliance_score=0.78,
        slo_routing_factor=0.91,
        slo_updated_at=300.0,
    )
    check(hb_populated.slo_p99_ms == 350.0, "populated p99 propagates")
    check(
        hb_populated.slo_routing_factor == 0.91,
        "populated routing_factor propagates",
    )


# ============================================================================
# Test 3 — Gate picks freshest manager's SLO summary per DC
# ============================================================================


def test_gate_ingests_freshest_summary() -> None:
    print("\n[3] Gate picks freshest manager's SLO summary")

    state = DCHealthState()

    # Stale manager
    stale = ManagerHeartbeat(
        node_id="m-stale",
        datacenter="dc-east",
        is_leader=False,
        term=1,
        version=1,
        active_jobs=0,
        active_workflows=0,
        worker_count=2,
        healthy_worker_count=2,
        available_cores=4,
        total_cores=4,
        slo_p50_ms=20.0,
        slo_p95_ms=80.0,
        slo_p99_ms=120.0,
        slo_sample_count=50,
        slo_compliance_score=0.5,
        slo_routing_factor=0.7,
        slo_updated_at=100.0,
    )
    state.update_manager_status("dc-east", ("127.0.0.1", 9000), stale, 100.0)

    # Fresh manager (later updated_at)
    fresh = ManagerHeartbeat(
        node_id="m-fresh",
        datacenter="dc-east",
        is_leader=True,
        term=1,
        version=1,
        active_jobs=0,
        active_workflows=0,
        worker_count=2,
        healthy_worker_count=2,
        available_cores=4,
        total_cores=4,
        slo_p50_ms=85.0,
        slo_p95_ms=350.0,
        slo_p99_ms=900.0,
        slo_sample_count=180,
        slo_compliance_score=1.4,
        slo_routing_factor=1.6,
        slo_updated_at=400.0,
    )
    state.update_manager_status("dc-east", ("127.0.0.1", 9002), fresh, 400.0)

    summary = state.get_dc_slo_summary("dc-east")
    check(
        summary.p99_ms == 900.0,
        f"DC summary reflects fresh manager's p99 (got {summary.p99_ms})",
    )
    check(
        summary.routing_factor == 1.6,
        f"DC summary uses fresh routing_factor (got {summary.routing_factor})",
    )
    check(
        summary.updated_at == 400.0,
        "DC summary timestamp matches freshest reporter",
    )

    # All-DC map present
    all_summaries = state.get_all_dc_slo_summaries()
    check("dc-east" in all_summaries, "dc-east appears in all-DC summary map")


# ============================================================================
# Test 4 — Routing score deprioritizes SLO violators
# ============================================================================


def test_routing_score_deprioritizes_violators() -> None:
    print("\n[4] Routing score deprioritizes SLO violators")

    # Same inputs except slo_routing_factor.
    compliant = DatacenterRoutingScore.calculate(
        datacenter_id="dc-compliant",
        health_bucket="healthy",
        rtt_ucb_ms=50.0,
        utilization=0.4,
        queue_depth=2,
        circuit_breaker_pressure=0.0,
        coordinate_quality=1.0,
        slo_routing_factor=0.8,
    )
    neutral = DatacenterRoutingScore.calculate(
        datacenter_id="dc-neutral",
        health_bucket="healthy",
        rtt_ucb_ms=50.0,
        utilization=0.4,
        queue_depth=2,
        circuit_breaker_pressure=0.0,
        coordinate_quality=1.0,
        slo_routing_factor=1.0,
    )
    violating = DatacenterRoutingScore.calculate(
        datacenter_id="dc-violating",
        health_bucket="healthy",
        rtt_ucb_ms=50.0,
        utilization=0.4,
        queue_depth=2,
        circuit_breaker_pressure=0.0,
        coordinate_quality=1.0,
        slo_routing_factor=2.5,
    )

    check(
        compliant.final_score < neutral.final_score,
        f"compliant DC scores better than neutral "
        f"({compliant.final_score:.2f} < {neutral.final_score:.2f})",
    )
    check(
        neutral.final_score < violating.final_score,
        f"violating DC scores worst "
        f"({neutral.final_score:.2f} < {violating.final_score:.2f})",
    )
    # Routing prefers lowest score; violating DC ranks last.
    sorted_by_score = sorted(
        [compliant, neutral, violating], key=lambda s: s.final_score
    )
    check(
        sorted_by_score[0].datacenter_id == "dc-compliant",
        "compliant DC ranks first",
    )
    check(
        sorted_by_score[2].datacenter_id == "dc-violating",
        "violating DC ranks last",
    )


# ============================================================================
# Test 5 — DCs with no observations get neutral baseline
# ============================================================================


def test_neutral_baseline_when_no_observations() -> None:
    print("\n[5] DCs with no observations route as if compliant")

    state = DCHealthState()
    # Manager registered but slo_sample_count=0 (default — no
    # workflow latencies observed yet).
    hb = ManagerHeartbeat(
        node_id="m1",
        datacenter="dc-fresh",
        is_leader=True,
        term=1,
        version=1,
        active_jobs=0,
        active_workflows=0,
        worker_count=2,
        healthy_worker_count=2,
        available_cores=4,
        total_cores=4,
    )
    state.update_manager_status("dc-fresh", ("127.0.0.1", 9000), hb, 100.0)

    summary = state.get_dc_slo_summary("dc-fresh")
    check(summary.is_empty(), "DC with no samples reports empty summary")
    check(
        summary.routing_factor == 1.0,
        "no-sample summary returns neutral routing_factor",
    )


# ============================================================================
# Runner
# ============================================================================


def main() -> int:
    print("=" * 72)
    print("AD-42 PHASE E INTEGRATION TESTS")
    print("=" * 72)

    test_slo_summary_round_trip()
    test_manager_heartbeat_carries_slo()
    test_gate_ingests_freshest_summary()
    test_routing_score_deprioritizes_violators()
    test_neutral_baseline_when_no_observations()

    print()
    if _FAILURES:
        print(f"=== {len(_FAILURES)} FAILURE(S) ===")
        for failure in _FAILURES:
            print(f"  - {failure}")
        return 1
    print("=== ALL AD-42 PHASE E INTEGRATION CHECKS PASSED ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
