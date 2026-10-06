"""
E2E: `hyperscale job status` against a gate, at each consistency level --
real processes.

A gate, a manager and a worker run a job submitted through the gate. Status
read from the gate -- at eventual, session, bounded_staleness and strong
consistency -- reports the job running, and an unknown job id exits 1.

The gate used to answer no client's status query: its handler was named
`receive_job_status_request` while clients sent `job_status`, so every
`hyperscale job status --gates` failed for want of a handler (and the
client's gate status poll with it); the only end-to-end status test asked
managers.

Bounds come from configuration: boot by --boot-timeout; the job's dispatch
within the managers' cancellation timeout, as in test_cli_job_commands.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import time

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from tests.integration.cli.node_processes import (
    BOOT_TIMEOUT_SECONDS,
    LOCALHOST,
    NODE_BLOCK,
    boot,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    run_job_command,
    run_join,
    worker_block,
)
from tests.integration.cli.test_cli_resource_guard import _submit_until_accepted

ENV = Env()
DATACENTER = "dc-gated-status"
WORKER_CORES = 1
CLIENT_BLOCK = 2  # client tcp + its udp (port + 1)
LEVELS = ("eventual", "session", "bounded_staleness", "strong")


@pytest.fixture
def run_marker() -> str:
    return f"cli-gated-status-{time.monotonic_ns()}"


async def test_a_gate_answers_job_status_at_every_consistency_level(run_marker: str) -> None:
    gate_start, manager_start, worker_start, join_client, submit_client, *command_clients = reserve_port_blocks(
        [NODE_BLOCK, NODE_BLOCK, worker_block(WORKER_CORES), CLIENT_BLOCK, CLIENT_BLOCK]
        + [CLIENT_BLOCK] * (len(LEVELS) + 1)
    )
    gate = node_at("gate", gate_start, run_marker)
    manager = node_at("manager", manager_start, run_marker, "--datacenter", DATACENTER)
    worker = node_at(
        "worker", worker_start, run_marker,
        "--datacenter", DATACENTER, "--workers", str(WORKER_CORES), "--managers", manager.address,
    )
    nodes = [gate, manager, worker]
    client = HyperscaleClient(host=LOCALHOST, port=submit_client, env=ENV, gates=[(LOCALHOST, gate.tcp_port)])
    gates = [gate.address]

    try:
        await boot(*nodes)
        returncode, output = await run_join(manager.address, gate.address, join_client)
        assert returncode == 0, output
        await client.start()
        job_id = await _submit_until_accepted(client, within=BOOT_TIMEOUT_SECONDS)

        for level, command_client in zip(LEVELS, command_clients):
            returncode, output = await _status_until(
                job_id, gates, command_client, "running", ("--consistency", level, "--max-staleness", "30s")
            )
            assert returncode == 0 and f"job {job_id}: running" in output, (level, output)

        returncode, output = await run_job_command(
            "status", "no-such-job", gates, command_clients[-1], tier_flag="--gates"
        )
        assert returncode == 1 and "no node asked knows job no-such-job" in output, output

    finally:
        await client.stop()
        await kill_remaining(nodes)


async def _status_until(
    job_id: str, gates: list[str], client_port: int, status: str, extra_arguments: tuple[str, ...]
) -> tuple[int, str]:
    """`hyperscale job status --gates` until it reports ``status`` -- the job
    dispatches after its workers register -- within the managers'
    cancellation timeout."""
    deadline = time.monotonic() + ENV.CANCELLED_WORKFLOW_TIMEOUT
    while True:
        returncode, output = await run_job_command(
            "status", job_id, gates, client_port, tier_flag="--gates", extra_arguments=extra_arguments
        )
        if f"job {job_id}: {status}" in output or time.monotonic() >= deadline:
            return returncode, output
        await asyncio.sleep(ENV.MANAGER_HEARTBEAT_INTERVAL)
