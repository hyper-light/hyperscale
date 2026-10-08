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

A job can also carry its own budget: with the manager's default budget
left generous, a job whose own CPU limit the workflow cannot stay under is
killed the same way; and a manager with resource guards disabled rejects
a job that sets a budget, explicitly, rather than run it unenforced.

THROTTLE: a load-generating (TEST) workflow above its budget's throttle
line but never certainly over its kill line has its concurrency cut --
the worker's executor applies it -- and runs to completion instead of
being killed.

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
from hyperscale.distributed.resources.resource_budget import ResourceBudget
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse
from tests.integration.cli.node_processes import (
    CLI_TEST_AUTH_SECRET,
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

# THROTTLE scenario: any load generator is above 85% of a 10% CPU budget,
# while the kill line (100x the budget) is out of reach of one core.
THROTTLE_CPU_BUDGET_PERCENT = 10.0
UNREACHABLE_KILL_THRESHOLD = 100.0
LOAD_VUS = 50
LOAD_DURATION_SECONDS = 20
HTTP_OK = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: text/plain\r\n\r\nok"

GUARD_ENVIRONMENT = {
    "RESOURCE_GUARD_ENABLED": "true",
    "RESOURCE_GUARD_MAX_CPU_PERCENT": str(CPU_BUDGET_PERCENT),
    "RESOURCE_GUARD_WARNING_GRACE_SECONDS": str(WARNING_GRACE_SECONDS),
    "RESOURCE_GUARD_KILL_GRACE_SECONDS": str(KILL_GRACE_SECONDS),
}
GUARDS_DISABLED_ENVIRONMENT = {"RESOURCE_GUARD_ENABLED": "false"}
# The same tight limits, carried by the job instead of the manager's env.
JOB_BUDGET = ResourceBudget(
    max_cpu_percent=CPU_BUDGET_PERCENT,
    max_memory_bytes=Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET).RESOURCE_GUARD_MAX_MEMORY_BYTES,
    warning_threshold=Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET).RESOURCE_GUARD_WARNING_THRESHOLD,
    throttle_threshold=Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET).RESOURCE_GUARD_THROTTLE_THRESHOLD,
    kill_threshold=Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET).RESOURCE_GUARD_KILL_THRESHOLD,
    warning_grace_seconds=WARNING_GRACE_SECONDS,
    kill_grace_seconds=KILL_GRACE_SECONDS,
)
GUARDS_DISABLED_REJECTION = "resource guards are disabled"
THROTTLE_BUDGET = ResourceBudget(
    max_cpu_percent=THROTTLE_CPU_BUDGET_PERCENT,
    max_memory_bytes=Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET).RESOURCE_GUARD_MAX_MEMORY_BYTES,
    warning_threshold=Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET).RESOURCE_GUARD_WARNING_THRESHOLD,
    throttle_threshold=Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET).RESOURCE_GUARD_THROTTLE_THRESHOLD,
    kill_threshold=UNREACHABLE_KILL_THRESHOLD,
    warning_grace_seconds=WARNING_GRACE_SECONDS,
    kill_grace_seconds=KILL_GRACE_SECONDS,
)


def load_workflow_class(target_url: str) -> type[Workflow]:
    """A TEST workflow (an engine-client step) hammering ``target_url`` --
    built per run, because the URL carries a port reserved for the run."""

    async def hit(self, url: URL = target_url) -> HTTPResponse:
        return await self.client.http.get(url)

    return type(
        "SimLoadWorkflow",
        (Workflow,),
        {"vus": LOAD_VUS, "duration": f"{LOAD_DURATION_SECONDS}s", "hit": step()(hit)},
    )


async def _answer_ok(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """A minimal keep-alive HTTP/1.1 responder: 200 "ok" per request."""
    try:
        while await reader.readuntil(b"\r\n\r\n"):
            writer.write(HTTP_OK)
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        writer.close()


@pytest.fixture
def run_marker() -> str:
    return f"cli-guard-{time.monotonic_ns()}"


@pytest.mark.parametrize(
    ("manager_environment", "resource_budget"),
    [(GUARD_ENVIRONMENT, None), ({}, JOB_BUDGET)],
    ids=["manager_default_budget", "job_own_budget"],
)
async def test_over_budget_workflow_is_killed_before_it_would_finish(
    run_marker: str,
    manager_environment: dict[str, str],
    resource_budget: ResourceBudget | None,
) -> None:
    manager_start, worker_start, client_start = reserve_port_blocks(
        [NODE_BLOCK, worker_block(WORKER_CORES), CLIENT_BLOCK]
    )
    manager = node_at("manager", manager_start, run_marker, environment=manager_environment)
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
        env=Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET),
        managers=[(LOCALHOST, manager.tcp_port)],
    )
    try:
        await boot(manager, worker)
        await client.start()

        submitted_at = time.monotonic()
        job_id = await _submit_until_accepted(
            client, within=BOOT_TIMEOUT_SECONDS, resource_budget=resource_budget
        )
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
        # The killed workflow failed, and the job fails on it at once with the
        # kill's reason -- not at its AD-34 timeout (a workflow whose
        # sub-workflows all come back CANCELLED outside a job cancel was
        # left unmarked, so the job's arithmetic never closed).
        assert result.status == "failed", result
        assert "resource budget exceeded" in (result.error or ""), result
        assert ended_after < BURN_DURATION_SECONDS, (
            f"job ended after {ended_after:.1f}s: it ran to its own duration, it was not killed"
        )
    finally:
        await client.stop()
        try:
            await stop_all(nodes, signal.SIGTERM, whole_group=False)
        finally:
            await kill_remaining(nodes)


async def test_workflow_over_its_throttle_line_is_throttled_and_completes(run_marker: str) -> None:
    manager_start, worker_start, client_start, http_start = reserve_port_blocks(
        [NODE_BLOCK, worker_block(WORKER_CORES), CLIENT_BLOCK, 1]
    )
    http_server = await asyncio.start_server(_answer_ok, LOCALHOST, http_start)
    manager = node_at("manager", manager_start, run_marker)
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
        env=Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET),
        managers=[(LOCALHOST, manager.tcp_port)],
    )
    load_workflow = load_workflow_class(f"http://{LOCALHOST}:{http_start}/")
    try:
        await boot(manager, worker)
        await client.start()

        job_id = await _submit_until_accepted(
            client,
            within=BOOT_TIMEOUT_SECONDS,
            resource_budget=THROTTLE_BUDGET,
            workflow=load_workflow(),
            duration_seconds=LOAD_DURATION_SECONDS,
        )
        result = await client.wait_for_job(job_id, timeout=LOAD_DURATION_SECONDS + BOOT_TIMEOUT_SECONDS)

        assert await manager.wait_for_output("Resource throttle of workflow", "concurrency cap", within=0.0), (
            "no applied throttle logged:\n" + "".join(manager.lines[-40:])
        )
        assert not await manager.wait_for_output("Resource kill of workflow", within=0.0), (
            "".join(manager.lines[-40:])
        )
        assert result.status == "completed", result
    finally:
        await client.stop()
        http_server.close()
        await http_server.wait_closed()
        try:
            await stop_all(nodes, signal.SIGTERM, whole_group=False)
        finally:
            await kill_remaining(nodes)


async def test_job_budget_is_rejected_when_guards_are_disabled(run_marker: str) -> None:
    manager_start, worker_start, client_start = reserve_port_blocks(
        [NODE_BLOCK, worker_block(WORKER_CORES), CLIENT_BLOCK]
    )
    manager = node_at("manager", manager_start, run_marker, environment=GUARDS_DISABLED_ENVIRONMENT)
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
        env=Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET),
        managers=[(LOCALHOST, manager.tcp_port)],
    )
    try:
        await boot(manager, worker)
        await client.start()

        rejection = await _rejection_once_capacity_exists(
            client, within=BOOT_TIMEOUT_SECONDS, resource_budget=JOB_BUDGET
        )
        assert GUARDS_DISABLED_REJECTION in rejection, rejection
    finally:
        await client.stop()
        try:
            await stop_all(nodes, signal.SIGTERM, whole_group=False)
        finally:
            await kill_remaining(nodes)


async def _submit_burn(
    client: HyperscaleClient,
    resource_budget: ResourceBudget | None,
    workflow: Workflow | None = None,
    duration_seconds: float = BURN_DURATION_SECONDS,
) -> str:
    return await client.submit_job(
        workflows=[([], workflow if workflow is not None else SimBurnWorkflow())],
        vus=1,
        timeout_seconds=float(duration_seconds + BOOT_TIMEOUT_SECONDS),
        resource_budget=resource_budget,
    )


async def _submit_until_accepted(
    client: HyperscaleClient,
    within: float,
    resource_budget: ResourceBudget | None = None,
    workflow: Workflow | None = None,
    duration_seconds: float = BURN_DURATION_SECONDS,
) -> str:
    """The manager rejects work until a worker registered capacity; retry."""
    deadline = time.monotonic() + within
    while True:
        try:
            return await _submit_burn(client, resource_budget, workflow, duration_seconds)
        except Exception:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(1.0)


async def _rejection_once_capacity_exists(
    client: HyperscaleClient,
    within: float,
    resource_budget: ResourceBudget,
) -> str:
    """The rejection the manager gives once it has capacity (earlier ones
    are for missing capacity); fails if the job is ever accepted."""
    deadline = time.monotonic() + within
    while True:
        try:
            job_id = await _submit_burn(client, resource_budget)
        except Exception as rejection:
            if GUARDS_DISABLED_REJECTION in str(rejection) or time.monotonic() >= deadline:
                return str(rejection)
            await asyncio.sleep(1.0)
            continue
        raise AssertionError(f"job {job_id} with a budget was accepted with guards disabled")
