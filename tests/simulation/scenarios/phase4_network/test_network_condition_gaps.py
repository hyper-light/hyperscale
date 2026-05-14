"""
Phase 4 network-condition gap scenarios.

Acceptance criteria covered here:

* Latency drift: a link's injected RTT grows over time and clears cleanly.
* Uniform packet loss: 1%, 5%, and 20% manager-link loss do not break SWIM
  convergence in a stable 3-manager DC.
* Drop burst: a short full-loss window expires without a false DEAD state.
* Bandwidth cap: throttling is modeled as bounded transport delay, not an
  unbounded queue or orphaned task.
* UDP reordering + duplication: SWIM remains converged under datagram
  reordering and duplicate delivery.
* TCP mid-stream reset: a reset during workflow_dispatch is surfaced as a
  clean transport failure and the workflow reaches terminal completion.
"""

import asyncio
import random

import pytest

from hyperscale.distributed.testing.workflows import SimpleWorkflow
from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    ExpectAllWorkflowsComplete,
    ExpectCompletionWithin,
    HarnessTimeouts,
    Submission,
    SubmissionPattern,
    WorkloadSpec,
    dc_has_leader,
    manager_has_n_peers,
    wait_until,
)


def _l2_spec(base_port: int, workers: int = 1) -> ClusterSpec:
    """Single-DC 3-manager topology for Phase 4 link-fault tests."""
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=workers, cores_per_worker=1),
        },
        env=EnvOverrides(request_timeout="4s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=75.0),
    )


def _simple_workload(timeout_seconds: float = 45.0) -> WorkloadSpec:
    """One small workflow with explicit terminal-completion expectations."""
    return WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], SimpleWorkflow)],
                dc_count=1,
                timeout_seconds=timeout_seconds,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=[
            ExpectAllWorkflowsComplete(["SimpleWorkflow"]),
            ExpectCompletionWithin(seconds=timeout_seconds),
        ],
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_latency_drift_link_delay_increases_and_clears() -> None:
    """Latency drift ramps from 10ms to 250ms and remains bounded."""
    spec = _l2_spec(base_port=22900)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="latency_drift_link_delay_increases_and_clears",
    ) as cluster:
        managers = cluster.managers("main")
        src, dst = managers[0], managers[1]

        await cluster.faults.latency_drift(
            start_ms=10.0,
            end_ms=250.0,
            duration_seconds=0.5,
            src=src,
            dst=dst,
        )
        early_delay = cluster.faults.delay_seconds(
            src.node_id,
            dst.node_id,
            random.Random(1),
        )
        await asyncio.sleep(0.7)
        late_delay = cluster.faults.delay_seconds(
            src.node_id,
            dst.node_id,
            random.Random(1),
        )

        assert 0.0 < early_delay < late_delay
        assert 0.20 <= late_delay <= 0.30
        await wait_until(
            dc_has_leader(managers),
            timeout=30.0,
            description="leader remains available under latency drift",
        )

        await cluster.faults.clear_network_faults()
        assert all(count == 0 for count in cluster.faults.network_fault_summary().values())


@pytest.mark.asyncio
@pytest.mark.simulation
@pytest.mark.parametrize(
    ("probability", "base_port"),
    [(0.01, 23000), (0.05, 23100), (0.20, 23200)],
)
async def test_uniform_packet_drop_rates_preserve_swim_convergence(
    probability: float,
    base_port: int,
) -> None:
    """Uniform 1%, 5%, and 20% manager-link loss preserves peer convergence."""
    spec = _l2_spec(base_port=base_port)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name=f"uniform_packet_drop_{int(probability * 100)}pct",
    ) as cluster:
        managers = cluster.managers("main")
        for src in managers:
            for dst in managers:
                if src.node_id == dst.node_id:
                    continue
                await cluster.faults.drop_rate(probability, src=src, dst=dst)

        assert cluster.faults.network_fault_summary()["drops"] == 6
        await asyncio.sleep(6.0)

        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, 2),
                timeout=45.0,
                poll=0.5,
                description=f"{manager.node_id} keeps peers under {probability:.0%} loss",
            )

        await cluster.faults.clear_network_faults()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_drop_burst_expires_without_false_dead() -> None:
    """A 200ms full-loss burst expires and all managers keep peer convergence."""
    spec = _l2_spec(base_port=23300)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="drop_burst_expires_without_false_dead",
    ) as cluster:
        managers = cluster.managers("main")
        await cluster.faults.drop_burst(
            duration_seconds=0.2,
            src=managers[0],
            dst=managers[1],
        )
        assert cluster.faults.network_fault_summary()["drops"] == 1

        await asyncio.sleep(0.5)
        assert cluster.faults.network_fault_summary()["drops"] == 0
        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, 2),
                timeout=30.0,
                poll=0.5,
                description=f"{manager.node_id} keeps peers after drop burst",
            )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_bandwidth_cap_throttles_without_breaking_quorum() -> None:
    """A constrained manager link adds bounded delay and quorum remains available."""
    spec = _l2_spec(base_port=23400)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="bandwidth_cap_throttles_without_breaking_quorum",
    ) as cluster:
        managers = cluster.managers("main")
        src, dst = managers[0], managers[1]

        await cluster.faults.bandwidth_cap(
            bytes_per_second=512.0,
            burst_bytes=128.0,
            src=src,
            dst=dst,
        )
        assert cluster.faults.network_fault_summary()["bandwidth_caps"] == 1
        delay_seconds = cluster.faults.bandwidth_delay_seconds(
            src.node_id,
            dst.node_id,
            payload_size_bytes=2048,
        )
        assert 0.0 < delay_seconds < 5.0

        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="leader remains available under bandwidth cap",
        )
        await cluster.faults.clear_network_faults()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_udp_reordering_and_duplication_keep_swim_converged() -> None:
    """UDP reordering and duplicate datagrams do not double-count or drop peers."""
    spec = _l2_spec(base_port=23500)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="udp_reordering_and_duplication_keep_swim_converged",
    ) as cluster:
        managers = cluster.managers("main")
        observer, impaired_peer = managers[0], managers[1]
        await cluster.faults.reorder(
            probability=1.0,
            delay_ms=125.0,
            jitter_ms=25.0,
            src=observer,
            dst=impaired_peer,
            protocol="udp",
        )
        await cluster.faults.duplicate(
            probability=1.0,
            copies=1,
            src=observer,
            dst=impaired_peer,
            protocol="udp",
        )
        summary = cluster.faults.network_fault_summary()
        assert summary["reorders"] == 1
        assert summary["duplicates"] == 1

        await asyncio.sleep(5.0)
        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, 2),
                timeout=45.0,
                poll=0.5,
                description=f"{manager.node_id} keeps peers under UDP reorder/duplicate",
            )

        await cluster.faults.clear_network_faults()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_tcp_mid_stream_reset_during_dispatch_recovers() -> None:
    """A TCP reset on the first workflow_dispatch is retried, not hung."""
    spec = _l2_spec(base_port=23600, workers=2)
    workload = _simple_workload(timeout_seconds=60.0)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="tcp_mid_stream_reset_during_dispatch_recovers",
    ) as cluster:
        await cluster.faults.tcp_reset(
            probability=1.0,
            action="workflow_dispatch",
            count=1,
        )
        assert cluster.faults.network_fault_summary()["tcp_resets"] == 1

        async with cluster.workload(workload) as driver:
            await driver.submit_and_wait()

        assert cluster.faults.network_fault_summary()["tcp_resets"] == 0
