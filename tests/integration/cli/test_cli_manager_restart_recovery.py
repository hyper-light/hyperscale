"""
E2E (AD-38, live CLI): a manager killed mid-job and restarted from its
data directory resumes the job -- real `hyperscale run manager` /
`hyperscale run worker` processes, a real executor pool, a real client.

`hyperscale run manager --data-directory DIR` keeps the job ledger WAL,
the persisted submission, and the idempotency WAL in DIR. A job is
durable once the manager acknowledges it (the ledger records and the
submission payload are written before the ack), so the manager is
SIGKILLed right after the ack, while the workflow is still running, and
restarted at the same address on the same DIR. The restarted manager must
recover the job from its WAL and RESUME it, and the client must see it
complete.

Bounds come from the configuration under test: the workflow's own
duration plus the nodes' boot bound, twice over (the original run and
the resumed one).
"""

import asyncio
import shutil
import signal
import sys
import tempfile
import time

import cloudpickle
import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
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

WORKER_CORES = 1
CLIENT_BLOCK = 2
# Long enough that the manager is killed while the workflow still runs.
WORKFLOW_SECONDS = 20.0
JOB_BOUND_SECONDS = 2 * (WORKFLOW_SECONDS + BOOT_TIMEOUT_SECONDS)


class SimLongStepWorkflow(Workflow):
    """One action step that runs for WORKFLOW_SECONDS."""

    vus = 1

    @step()
    async def hold(self) -> dict[str, str]:
        await asyncio.sleep(WORKFLOW_SECONDS)
        return {"status": "ok"}


# Shipped by value like a user's script-defined workflow (the manager's
# restricted unpickler admits no tests-tree module by reference).
cloudpickle.register_pickle_by_value(sys.modules[__name__])


@pytest.fixture
def run_marker() -> str:
    return f"cli-restart-{time.monotonic_ns()}"


async def test_restarted_manager_resumes_job_from_its_data_directory(run_marker: str) -> None:
    manager_start, worker_start, client_start = reserve_port_blocks(
        [NODE_BLOCK, worker_block(WORKER_CORES), CLIENT_BLOCK]
    )
    data_directory = tempfile.mkdtemp(prefix="hyperscale-manager-restart-")
    first_manager = node_at("manager", manager_start, run_marker, "--data-directory", data_directory)
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--workers", str(WORKER_CORES),
        "--managers", first_manager.address,
    )
    restarted_manager = node_at("manager", manager_start, run_marker, "--data-directory", data_directory)
    nodes = [first_manager, worker, restarted_manager]
    client = HyperscaleClient(
        host=LOCALHOST,
        port=client_start,
        env=Env(),
        managers=[(LOCALHOST, first_manager.tcp_port)],
    )
    try:
        await boot(first_manager, worker)
        await client.start()

        job_id = await _submit_until_accepted(client, within=BOOT_TIMEOUT_SECONDS)

        exited, _survivors = await first_manager.stop(signal.SIGKILL, whole_group=True)
        assert exited, "the first manager outlived SIGKILL"

        await boot(restarted_manager)
        assert await restarted_manager.wait_for_output(
            f"Recovered job {job_id} RESUMED", within=BOOT_TIMEOUT_SECONDS
        ), "the restarted manager did not resume the job from its WAL:\n" + "".join(
            restarted_manager.lines[-40:]
        )

        result = await client.wait_for_job(job_id, timeout=JOB_BOUND_SECONDS)
        assert result.status == "completed", result

        await stop_all([worker, restarted_manager], signal.SIGTERM, whole_group=False)
    finally:
        await client.stop()
        try:
            await kill_remaining(nodes)
        finally:
            shutil.rmtree(data_directory)


async def _submit_until_accepted(client: HyperscaleClient, within: float) -> str:
    """The manager rejects work until a worker registered capacity; retry."""
    deadline = time.monotonic() + within
    while True:
        try:
            return await client.submit_job(
                workflows=[([], SimLongStepWorkflow())],
                vus=1,
                timeout_seconds=JOB_BOUND_SECONDS,
            )
        except Exception:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(1.0)
