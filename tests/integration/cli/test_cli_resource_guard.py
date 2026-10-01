"""
E2E (AD-41): a workflow over its resource budget is killed — real
`hyperscale run manager` / `hyperscale run worker` processes, a real
executor pool measuring real CPU, and a real client.

The manager runs with a CPU budget a busy workflow cannot stay under
(exported through the environment, which `hyperscale run` now reads),
with short graces. A workflow that burns CPU for far longer than those
graces is submitted; the manager must warn, then kill it — before the
workflow's own duration would have ended it — and the job must end
unsuccessfully rather than complete.

Bounds come from the configuration under test: the kill must land
before the workflow's duration elapses (otherwise it was never killed);
the client waits that duration plus the node boot bound.
"""

import asyncio
import signal
import sys
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
# A budget any CPU-bound workflow exceeds with certainty, and graces short
# enough that the kill lands long before the workflow would end.
CPU_BUDGET_PERCENT = 1.0
WARNING_GRACE_SECONDS = 1.0
KILL_GRACE_SECONDS = 1.0
BURN_DURATION_SECONDS = 60


# Busy slice length: short enough that the executor's event loop (and
# with it progress reporting) keeps running between slices.
BURN_SLICE_SECONDS = 0.05


class SimBurnWorkflow(Workflow):
    """One action step that spins the CPU for BURN_DURATION_SECONDS.

    (A step that calls no engine client is an ACTION hook: it runs once,
    so the burn lives inside the step rather than in ``duration``, which
    only governs TEST workflows.)
    """

    vus = 1

    @step()
    async def burn(self) -> dict[str, str]:
        deadline = time.perf_counter() + BURN_DURATION_SECONDS
        while time.perf_counter() < deadline:
            slice_end = time.perf_counter() + BURN_SLICE_SECONDS
            while time.perf_counter() < slice_end:
                pass
            await asyncio.sleep(0)
        return {"status": "ok"}


# Shipped by value like a user's script-defined workflow (the manager's
# restricted unpickler admits no tests-tree module by reference).
cloudpickle.register_pickle_by_value(sys.modules[__name__])

GUARD_ENVIRONMENT = {
    "RESOURCE_GUARD_ENABLED": "true",
    "RESOURCE_GUARD_MAX_CPU_PERCENT": str(CPU_BUDGET_PERCENT),
    "RESOURCE_GUARD_WARNING_GRACE_SECONDS": str(WARNING_GRACE_SECONDS),
    "RESOURCE_GUARD_KILL_GRACE_SECONDS": str(KILL_GRACE_SECONDS),
}


@pytest.fixture
def run_marker() -> str:
    return f"cli-guard-{time.monotonic_ns()}"


async def test_over_budget_workflow_is_killed_before_it_would_finish(run_marker: str) -> None:
    manager_start, worker_start, client_start = reserve_port_blocks(
        [NODE_BLOCK, worker_block(WORKER_CORES), CLIENT_BLOCK]
    )
    manager = node_at("manager", manager_start, run_marker, environment=GUARD_ENVIRONMENT)
    worker = node_at(
        "worker",
        worker_start,
        run_marker,
        "--workers", str(WORKER_CORES),
        "--managers", manager.address,
    )
    nodes = [manager, worker]
    client = HyperscaleClient(
        host=LOCALHOST,
        port=client_start,
        env=Env(),
        managers=[(LOCALHOST, manager.tcp_port)],
    )
    try:
        await boot(manager, worker)
        await client.start()

        submitted_at = time.monotonic()
        job_id = await _submit_until_accepted(client, within=BOOT_TIMEOUT_SECONDS)
        result = await client.wait_for_job(
            job_id, timeout=BURN_DURATION_SECONDS + BOOT_TIMEOUT_SECONDS
        )
        ended_after = time.monotonic() - submitted_at

        assert await manager.wait_for_output("cpu_exceeded", within=0.0), (
            "no resource warning/kill logged:\n" + "".join(manager.lines[-40:])
        )
        assert await manager.wait_for_output("Resource kill of workflow", "requested", within=0.0), (
            "".join(manager.lines[-40:])
        )
        assert result.status != "completed", result
        assert ended_after < BURN_DURATION_SECONDS, (
            f"job ended after {ended_after:.1f}s: it ran to its own duration, it was not killed"
        )
    finally:
        await client.stop()
        try:
            await stop_all(nodes, signal.SIGTERM, whole_group=False)
        finally:
            await kill_remaining(nodes)


async def _submit_until_accepted(client: HyperscaleClient, within: float) -> str:
    """The manager rejects work until a worker registered capacity; retry."""
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
