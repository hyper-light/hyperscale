"""
E2E: `hyperscale job status` and `hyperscale job cancel` against a running
gateless cluster, real processes.

A manager and a worker run a job that burns for a minute, submitted by one
client; separate `hyperscale job` processes -- knowing only the job id and
the managers -- see it running, cancel it, and see it cancelled. Asserts:

1. `job status` reports the job running.
2. `job cancel` cancels it (exit 0).
3. `job status` then reports it cancelled.
4. `job status` of an unknown job id exits 1.

Bounds come from configuration: boot by --boot-timeout; the cancellation's
completion by the managers' cancellation timeout.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import time

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from tests.integration.cli.node_processes import (
    CLI_TEST_AUTH_SECRET,
    BOOT_TIMEOUT_SECONDS,
    LOCALHOST,
    NODE_BLOCK,
    boot,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    run_job_command,
    worker_block,
)
from tests.integration.cli.test_cli_resource_guard import _submit_until_accepted

ENV = Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET)
DATACENTER = "dc-job-commands"
WORKER_CORES = 1
CLIENT_BLOCK = 2  # client tcp + its udp (port + 1)


@pytest.fixture
def run_marker() -> str:
    return f"cli-job-commands-{time.monotonic_ns()}"


async def test_a_job_is_seen_running_cancelled_and_seen_cancelled(run_marker: str) -> None:
    manager_start, worker_start, client_start, *command_clients = reserve_port_blocks(
        [NODE_BLOCK, worker_block(WORKER_CORES), CLIENT_BLOCK, CLIENT_BLOCK, CLIENT_BLOCK, CLIENT_BLOCK, CLIENT_BLOCK]
    )
    manager = node_at("manager", manager_start, run_marker, "--datacenter", DATACENTER)
    worker = node_at(
        "worker", worker_start, run_marker,
        "--datacenter", DATACENTER, "--workers", str(WORKER_CORES), "--managers", manager.address,
    )
    nodes = [manager, worker]
    client = HyperscaleClient(host=LOCALHOST, port=client_start, env=ENV, managers=[(LOCALHOST, manager.tcp_port)])
    managers = [manager.address]

    try:
        await boot(*nodes)
        await client.start()
        job_id = await _submit_until_accepted(client, within=BOOT_TIMEOUT_SECONDS)

        returncode, output = await _status_until(job_id, managers, command_clients[0], "running")
        assert returncode == 0 and f"job {job_id}: running" in output, output

        returncode, output = await run_job_command("cancel", job_id, managers, command_clients[1])
        assert returncode == 0 and "cancelled" in output, output

        returncode, output = await _status_until(job_id, managers, command_clients[2], "cancelled")
        assert returncode == 0 and f"job {job_id}: cancelled" in output, output

        returncode, output = await run_job_command("status", "no-such-job", managers, command_clients[3])
        assert returncode == 1 and "no node asked knows job no-such-job" in output, output

    finally:
        await client.stop()
        await kill_remaining(nodes)


async def _status_until(job_id: str, managers: list[str], client_port: int, status: str) -> tuple[int, str]:
    """`hyperscale job status` until it reports ``status`` -- the job
    dispatches after its workers register, and a cancellation completes
    once its workflows stop -- within the managers' cancellation timeout."""
    deadline = time.monotonic() + ENV.CANCELLED_WORKFLOW_TIMEOUT
    while True:
        returncode, output = await run_job_command("status", job_id, managers, client_port)
        if f"job {job_id}: {status}" in output or time.monotonic() >= deadline:
            return returncode, output
        await asyncio.sleep(ENV.MANAGER_HEARTBEAT_INTERVAL)
