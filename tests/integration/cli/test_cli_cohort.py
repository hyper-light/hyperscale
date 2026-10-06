"""
E2E: a datacenter's manager cohort and its gate, formed from CLI flags.

Three real `hyperscale run manager` processes share one datacenter. Each
is given the whole cohort (--managers/--manager-udp; each skips its own
entry) and the gate (--gates/--gate-udp). A worker is seeded with every
manager. The gate needs no joins: the managers report to it. Asserts:

1. The cohort elects exactly one leader. Three managers given no peers
   would each be a cohort of one and each elect itself.
2. Every manager heartbeats the gate.
3. A job submitted through the gate completes.

Bounds come from the nodes' own configuration: boot by --boot-timeout;
the election by the cluster stabilization timeout plus one full election
round (base timeout, jitter, pre-vote); the first heartbeat by the
manager's heartbeat interval plus that send's timeout.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import signal
import sys
import time

import cloudpickle
import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.manager.config import create_manager_config_from_env
from hyperscale.graph import Workflow, step
from tests.integration.cli.node_processes import (
    BOOT_TIMEOUT_SECONDS,
    LOCALHOST,
    NODE_BLOCK,
    boot,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    stop_all,
    worker_block,
)

ENV = Env()
DATACENTER = "dc-cohort"
COHORT_SIZE = 3
WORKER_CORES = 1
CLIENT_BLOCK = 2  # client tcp + its udp (port + 1)
_MANAGER_CONFIG = create_manager_config_from_env(LOCALHOST, 1, 2, ENV)
ELECTION_BOUND_SECONDS = (
    _MANAGER_CONFIG.cluster_stabilization_timeout_seconds
    + ENV.LEADER_ELECTION_TIMEOUT_BASE
    + ENV.LEADER_ELECTION_TIMEOUT_JITTER
    + ENV.LEADER_PRE_VOTE_TIMEOUT
)
HEARTBEAT_BOUND_SECONDS = (
    _MANAGER_CONFIG.heartbeat_interval_seconds
    + _MANAGER_CONFIG.tcp_timeout_short_seconds
)
JOB_BOUND_SECONDS = BOOT_TIMEOUT_SECONDS


class CohortProbeWorkflow(Workflow):
    """One action step that returns at once."""

    vus = 1

    @step()
    async def probe(self) -> dict[str, str]:
        return {"status": "ok"}


# Shipped by value like a user's script-defined workflow (the manager's
# restricted unpickler admits no tests-tree module by reference).
cloudpickle.register_pickle_by_value(sys.modules[__name__])


@pytest.fixture
def run_marker() -> str:
    return f"cli-cohort-{time.monotonic_ns()}"


async def test_a_cli_cohort_elects_one_leader_and_runs_a_job_through_its_gate(
    run_marker: str,
) -> None:
    *manager_starts, gate_start, worker_start, client_start = reserve_port_blocks(
        [NODE_BLOCK] * COHORT_SIZE + [NODE_BLOCK, worker_block(WORKER_CORES), CLIENT_BLOCK]
    )
    manager_tcp = [f"{LOCALHOST}:{start}" for start in manager_starts]
    manager_udp = [f"{LOCALHOST}:{start + 1}" for start in manager_starts]
    gate = node_at("gate", gate_start, run_marker)
    managers = [
        node_at(
            "manager",
            start,
            run_marker,
            "--datacenter", DATACENTER,
            "--managers", *manager_tcp,
            "--manager-udp", *manager_udp,
            "--gates", gate.address,
            "--gate-udp", f"{LOCALHOST}:{gate_start + 1}",
            # The gate heartbeat log is a DEBUG line.
            "--log-level", "debug",
        )
        for start in manager_starts
    ]
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--datacenter", DATACENTER,
        "--workers", str(WORKER_CORES),
        "--managers", *manager_tcp,
    )
    nodes = [gate, *managers, worker]
    client = HyperscaleClient(
        host=LOCALHOST,
        port=client_start,
        env=ENV,
        gates=[(LOCALHOST, gate.tcp_port)],
    )

    try:
        await boot(*nodes)

        leaders = await _leaders_after(managers, within=ELECTION_BOUND_SECONDS)
        assert len(leaders) == 1, [manager.address for manager in leaders]

        for manager in managers:
            assert await manager.wait_for_output(
                "Sent heartbeat to 1/1 gates", within=HEARTBEAT_BOUND_SECONDS
            ), f"manager {manager.address} never heartbeated the gate"

        await client.start()
        job_id = await _submit_until_accepted(client, within=BOOT_TIMEOUT_SECONDS)
        result = await client.wait_for_job(job_id, timeout=JOB_BOUND_SECONDS)
        assert result.status == "completed", result

        await client.stop()
        await stop_all(nodes, signal.SIGTERM, whole_group=False)

    finally:
        await client.stop()
        await kill_remaining(nodes)


async def _leaders_after(managers, within: float) -> list:
    """The managers that announced leadership, once one has and the
    election window has passed (a second leader would announce in it)."""
    deadline = time.monotonic() + within
    while time.monotonic() < deadline and not any(
        _announced_leadership(manager) for manager in managers
    ):
        await asyncio.sleep(0.1)

    await asyncio.sleep(max(0.0, deadline - time.monotonic()))
    return [manager for manager in managers if _announced_leadership(manager)]


def _announced_leadership(manager) -> bool:
    return any("Became LEADER" in line for line in manager.lines)


async def _submit_until_accepted(client: HyperscaleClient, within: float) -> str:
    """The gate refuses work until a datacenter has reported capacity."""
    deadline = time.monotonic() + within
    while True:
        try:
            return await client.submit_job(
                workflows=[([], CohortProbeWorkflow())],
                vus=1,
                timeout_seconds=JOB_BOUND_SECONDS,
            )

        except Exception:
            if time.monotonic() >= deadline:
                raise

            await asyncio.sleep(1.0)
