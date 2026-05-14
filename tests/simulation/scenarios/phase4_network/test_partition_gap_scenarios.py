"""
Phase 4 partition gap scenarios.

Acceptance criteria covered here:

* Three-way manager split: independent partition rules can create a true
  multi-way split, and healing restores full peer convergence with at most
  one leader.
* Gate-tier split: gate SWIM membership tolerates a partitioned gate and
  reconverges after heal.
* Quorum-isolating partition: a minority-side manager rejects job submits
  with a clear error and does not silently queue writes without quorum.
"""

import asyncio

import pytest

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.testing.workflows import SimpleWorkflow
from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    HarnessTimeouts,
    ServerHandle,
    dc_has_leader,
    gate_cluster_formed,
    manager_has_n_peers,
    wait_until,
)


def _l2_spec(base_port: int) -> ClusterSpec:
    """Single-DC 3-manager topology for quorum partition scenarios."""
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=1, cores_per_worker=1),
        },
        env=EnvOverrides(request_timeout="4s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=75.0),
    )


def _l3_gate_spec(base_port: int) -> ClusterSpec:
    """Three gates fronting one small DC for gate-tier partition coverage."""
    return ClusterSpec(
        gates=3,
        datacenters={
            "main": DCSpec(managers=1, workers=1, cores_per_worker=1),
        },
        env=EnvOverrides(request_timeout="4s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=90.0),
    )


def _find_leader(managers: list[ServerHandle]) -> ServerHandle:
    """Return the currently elected manager leader."""
    for manager in managers:
        if manager.instance.is_leader():
            return manager
    raise AssertionError("no leader elected")


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_three_way_manager_partition_then_heal() -> None:
    """A three-way split heals back to full peer convergence."""
    spec = _l2_spec(base_port=23700)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="three_way_manager_partition_then_heal",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="initial leader before three-way partition",
        )

        await cluster.faults.partition([managers[0]], [managers[1]])
        await cluster.faults.partition([managers[0]], [managers[2]])
        await cluster.faults.partition([managers[1]], [managers[2]])
        assert cluster.faults.network_fault_summary()["partitions"] == 3

        await asyncio.sleep(4.0)
        await cluster.faults.heal_partition()
        assert cluster.faults.network_fault_summary()["partitions"] == 0

        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, 2),
                timeout=60.0,
                poll=0.5,
                description=f"{manager.node_id} reconverges after three-way heal",
            )
        await wait_until(
            lambda: sum(1 for manager in managers if manager.instance.is_leader()) <= 1,
            timeout=45.0,
            poll=0.5,
            description="at most one leader after three-way heal",
        )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_gate_tier_partition_then_heal() -> None:
    """A partitioned gate rejoins and the gate cluster reconverges."""
    spec = _l3_gate_spec(base_port=23800)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="gate_tier_partition_then_heal",
    ) as cluster:
        gates = cluster.gates
        await wait_until(
            gate_cluster_formed(gates, expected_peers=2),
            timeout=60.0,
            description="initial gate cluster formed",
        )

        victim = gates[0]
        survivors = gates[1:]
        await cluster.faults.partition([victim], survivors)
        await asyncio.sleep(4.0)
        await wait_until(
            gate_cluster_formed(survivors, expected_peers=1),
            timeout=45.0,
            poll=0.5,
            description="surviving gates keep quorum-side peer",
        )

        await cluster.faults.heal_partition()
        await wait_until(
            gate_cluster_formed(gates, expected_peers=2),
            timeout=60.0,
            poll=0.5,
            description="gate cluster reconverges after heal",
        )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_quorum_isolating_partition_rejects_submits() -> None:
    """A quorum-isolated manager refuses writes instead of queuing them."""
    spec = _l2_spec(base_port=23900)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="quorum_isolating_partition_rejects_submits",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="initial leader before quorum-isolating partition",
        )
        isolated_manager = _find_leader(managers)
        majority_side = [
            manager
            for manager in managers
            if manager.node_id != isolated_manager.node_id
        ]

        await cluster.faults.partition([isolated_manager], majority_side)
        await wait_until(
            lambda: not isolated_manager.instance._has_quorum_available(),
            timeout=45.0,
            poll=0.5,
            description="isolated manager observes quorum loss",
        )

        client = HyperscaleClient(
            host=cluster.spec.host,
            port=cluster.reserve_client_port(),
            env=Env(MERCURY_SYNC_LOG_LEVEL="error"),
            managers=[(isolated_manager.host, isolated_manager.tcp_port)],
        )
        await client.start()
        try:
            client._config.submission_max_retries = 0
            with pytest.raises(
                RuntimeError,
                match="(?i)(no quorum|not dc leader|job rejected)",
            ):
                await client.submit_job(
                    workflows=[([], SimpleWorkflow())],
                    vus=1,
                    timeout_seconds=10.0,
                )
        finally:
            await client.stop()

        for manager in managers:
            assert not manager.instance._manager_state.iter_job_submissions()
