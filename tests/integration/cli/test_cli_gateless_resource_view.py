"""
E2E (AD-41 Part 4): a datacenter's resource view with no gate -- real
`hyperscale run manager|worker` processes forming a three-manager cohort, a
real executor pool burning real CPU, a real client submitting straight to
the managers.

A client can run jobs on one datacenter without a gate, so every manager
must hold the view a gate would: its own report and each peer's, by the
managers' resource gossip. While the job burns, EVERY manager's ping must
show:
- all three managers' reports (each manager reports the workload of the
  jobs it leads; the cohort's reports sum to the datacenter's);
- the datacenter's capacity exactly as the worker registered it (100 per
  allotted core);
- a running workload above zero.
Once the job is cancelled the workload must drain back to zero on every
manager.

Bounds come from the configuration under test: the view must appear while
the workflow still burns (its duration); the drain is bounded by the
resource staleness threshold plus one gossip round (one heartbeat interval
plus the gossip send's timeout).
"""

import asyncio
import signal
import time

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import ManagerPingResponse
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.manager.config import create_manager_config_from_env
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
from tests.integration.cli.test_cli_resource_guard import (
    BURN_DURATION_SECONDS,
    _submit_until_accepted,
)

ENV = Env()
DATACENTER = "dc-gateless"
COHORT_SIZE = 3
WORKER_CORES = 1
CLIENT_BLOCK = 2  # client tcp + its udp (port + 1)
POLL_INTERVAL_SECONDS = 0.5
_MANAGER_CONFIG = create_manager_config_from_env(LOCALHOST, 1, 2, ENV)
GOSSIP_ROUND_BOUND_SECONDS = (
    _MANAGER_CONFIG.heartbeat_interval_seconds
    + _MANAGER_CONFIG.tcp_timeout_short_seconds
)
DRAIN_BOUND_SECONDS = ENV.RESOURCE_VIEW_STALENESS_SECONDS + GOSSIP_ROUND_BOUND_SECONDS


@pytest.fixture
def run_marker() -> str:
    return f"cli-gateless-resource-view-{time.monotonic_ns()}"


async def test_every_manager_holds_the_datacenters_resource_view(run_marker: str) -> None:
    *manager_starts, worker_start, client_start = reserve_port_blocks(
        [NODE_BLOCK] * COHORT_SIZE + [worker_block(WORKER_CORES), CLIENT_BLOCK]
    )
    manager_tcp = [f"{LOCALHOST}:{start}" for start in manager_starts]
    manager_udp = [f"{LOCALHOST}:{start + 1}" for start in manager_starts]
    managers = [
        node_at(
            "manager",
            start,
            run_marker,
            "--datacenter", DATACENTER,
            "--managers", *manager_tcp,
            "--manager-udp", *manager_udp,
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
    nodes = [*managers, worker]
    client = HyperscaleClient(
        host=LOCALHOST,
        port=client_start,
        env=ENV,
        managers=[(LOCALHOST, manager.tcp_port) for manager in managers],
    )
    try:
        await boot(*nodes)
        await client.start()

        job_id = await _submit_until_accepted(client, within=BOOT_TIMEOUT_SECONDS)

        burning = await _wait_for_every_manager(
            client,
            [manager.tcp_port for manager in managers],
            lambda answer: (
                answer.resources is not None
                and answer.resources.reporting_manager_count == COHORT_SIZE
                and answer.resources.workload_cpu_percent > 0.0
            ),
            within=BURN_DURATION_SECONDS,
        )
        for answer in burning:
            resources = answer.resources
            assert resources.datacenter == DATACENTER, resources
            assert resources.cpu_capacity_percent == 100.0 * WORKER_CORES, resources
            assert resources.cpu_pressure == min(
                1.0, resources.workload_cpu_percent / resources.cpu_capacity_percent
            ), resources

        await client.cancel_job(job_id, reason="gateless resource view observed")

        await _wait_for_every_manager(
            client,
            [manager.tcp_port for manager in managers],
            lambda answer: (
                answer.resources is not None and answer.resources.workload_cpu_percent == 0.0
            ),
            within=DRAIN_BOUND_SECONDS,
        )

        await client.stop()
        await stop_all(nodes, signal.SIGTERM, whole_group=False)
    finally:
        await client.stop()
        await kill_remaining(nodes)


async def _wait_for_every_manager(
    client: HyperscaleClient,
    manager_ports: list[int],
    matches,
    within: float,
) -> list[ManagerPingResponse]:
    """Ping every manager until each one's answer satisfies ``matches``."""
    deadline = time.monotonic() + within
    answers: list[ManagerPingResponse] = []
    while time.monotonic() < deadline:
        answers = [
            await client.ping_manager((LOCALHOST, manager_port)) for manager_port in manager_ports
        ]
        if all(matches(answer) for answer in answers):
            return answers
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(
        f"not every manager matched within {within}s; last answers: "
        f"{[answer.resources for answer in answers]}"
    )
