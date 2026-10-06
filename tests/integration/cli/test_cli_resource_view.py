"""
E2E (AD-41): a gate's view of a datacenter's resource pressure -- real
`hyperscale run gate|manager|worker` processes joined with a real
`hyperscale join`, a real executor pool burning real CPU, a real client.

A CPU-burning workflow is submitted through the gate. The job's leader
accounts the workflow's measured CPU and memory, reports it to the gate
on its heartbeat, and the gate's datacenter list must show:
- the datacenter's capacity exactly as the worker registered it (100 per
  allotted core; the worker host's memory, in the whole MiB the worker
  reports);
- a running workload above zero and the pressure it implies.
Once the job is cancelled the workload must drain back to zero.

Bounds come from the configuration under test: the view must appear
while the workflow still burns (its duration); the drain is bounded by
the resource staleness threshold plus one manager-to-gate heartbeat.
"""

import asyncio
import signal
import time

import psutil
import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import DatacenterInfo
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
    run_join,
    stop_all,
    worker_block,
)
from tests.integration.cli.test_cli_resource_guard import (
    BURN_DURATION_SECONDS,
    SimBurnWorkflow,
)

ENV = Env()
WORKER_CORES = 1
CLIENT_BLOCK = 2
DATACENTER = "default"
BYTES_PER_MEGABYTE = 1024 * 1024
POLL_INTERVAL_SECONDS = 0.5
# The manager's gate heartbeat loop sleeps one interval, then sends with
# the short TCP timeout (manager/server.py _gate_heartbeat_loop).
_MANAGER_CONFIG = create_manager_config_from_env(LOCALHOST, 1, 2, ENV)
HEARTBEAT_BOUND_SECONDS = (
    _MANAGER_CONFIG.heartbeat_interval_seconds
    + _MANAGER_CONFIG.tcp_timeout_short_seconds
)
DRAIN_BOUND_SECONDS = ENV.RESOURCE_VIEW_STALENESS_SECONDS + HEARTBEAT_BOUND_SECONDS


@pytest.fixture
def run_marker() -> str:
    return f"cli-resource-view-{time.monotonic_ns()}"


async def test_gate_reports_datacenter_resource_pressure(run_marker: str) -> None:
    worker_start, manager_start, gate_start, join_start, client_start = reserve_port_blocks(
        [worker_block(WORKER_CORES), NODE_BLOCK, NODE_BLOCK, CLIENT_BLOCK, CLIENT_BLOCK]
    )
    manager = node_at("manager", manager_start, run_marker)
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--workers", str(WORKER_CORES),
        "--managers", manager.address,
    )
    gate = node_at("gate", gate_start, run_marker)
    nodes = [manager, worker, gate]
    client = HyperscaleClient(
        host=LOCALHOST,
        port=client_start,
        env=Env(),
        gates=[(LOCALHOST, gate.tcp_port)],
    )
    try:
        await boot(manager, worker, gate)
        returncode, output = await run_join(manager.address, gate.address, join_start)
        assert returncode == 0, output
        await client.start()

        job_id = await _submit_until_accepted(client, within=BOOT_TIMEOUT_SECONDS)

        burning = await _wait_for_datacenter(
            client,
            gate.tcp_port,
            lambda info: info.resources is not None and info.resources.workload_cpu_percent > 0.0,
            within=BURN_DURATION_SECONDS,
        )
        resources = burning.resources
        assert resources.cpu_capacity_percent == 100.0 * WORKER_CORES, resources
        assert resources.memory_capacity_bytes == (
            psutil.virtual_memory().total // BYTES_PER_MEGABYTE * BYTES_PER_MEGABYTE
        ), resources
        assert resources.reporting_manager_count == 1, resources
        assert resources.cpu_pressure == min(
            1.0, resources.workload_cpu_percent / resources.cpu_capacity_percent
        ), resources
        assert 0.0 < resources.memory_pressure < 1.0, resources

        await client.cancel_job(job_id, reason="resource view observed")

        drained = await _wait_for_datacenter(
            client,
            gate.tcp_port,
            lambda info: info.resources is not None and info.resources.workload_cpu_percent == 0.0,
            within=DRAIN_BOUND_SECONDS,
        )
        assert drained.resources.cpu_pressure == 0.0, drained.resources

        await client.stop()
        await stop_all(nodes, signal.SIGTERM, whole_group=False)
    finally:
        await client.stop()
        await kill_remaining(nodes)


async def _wait_for_datacenter(
    client: HyperscaleClient,
    gate_port: int,
    matches,
    within: float,
) -> DatacenterInfo:
    """Poll the gate's datacenter list until DATACENTER satisfies ``matches``."""
    deadline = time.monotonic() + within
    last_seen: DatacenterInfo | None = None
    while time.monotonic() < deadline:
        response = await client.get_datacenters(addr=(LOCALHOST, gate_port))
        for info in response.datacenters:
            if info.dc_id == DATACENTER:
                last_seen = info
                if matches(info):
                    return info
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"datacenter view never matched within {within:.1f}s; last seen: {last_seen}")


async def _submit_until_accepted(client: HyperscaleClient, within: float) -> str:
    """The gate rejects work until its datacenter has capacity; retry."""
    deadline = time.monotonic() + within
    while True:
        try:
            return await client.submit_job(
                workflows=[([], SimBurnWorkflow())],
                vus=1,
                timeout_seconds=float(BURN_DURATION_SECONDS + BOOT_TIMEOUT_SECONDS),
            )
        except Exception:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(1.0)
