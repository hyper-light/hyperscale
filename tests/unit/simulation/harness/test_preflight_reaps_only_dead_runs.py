"""
The supervisor's preflight reaps only processes left by a DEAD harness run.

Every harness-spawned process carries its run id. The preflight reaped
any process carrying a run id other than its own -- including those of a
harness run still in progress in another session on the same machine, so
concurrent scenario runs killed each other's worker executor pools
("Worker process pool failed to start", hung or failed workloads). A
process whose owning harness process still lives is now left alone; one
whose owner is gone (or never recorded one) is still reaped.
"""

import asyncio
import os
import sys

import pytest

from tests.simulation.harness.cluster_spec import ClusterSpec
from tests.simulation.harness.port_allocator import PortAllocator
from tests.simulation.harness.supervisor import Supervisor

FOREIGN_RUN_ID = "foreign-run"
# Long enough to outlive the scan; the test terminates it.
TAGGED_PROCESS_SLEEP = "import time; time.sleep(3600)"


async def _spawn_tagged(owner_pid: int | None) -> asyncio.subprocess.Process:
    environment = {**os.environ, "HYPERSCALE_HARNESS_RUN_ID": FOREIGN_RUN_ID}
    environment.pop("HYPERSCALE_HARNESS_OWNER_PID", None)
    if owner_pid is not None:
        environment["HYPERSCALE_HARNESS_OWNER_PID"] = str(owner_pid)
    return await asyncio.create_subprocess_exec(sys.executable, "-c", TAGGED_PROCESS_SLEEP, env=environment)


async def _exited_pid() -> int:
    exited = await asyncio.create_subprocess_exec(sys.executable, "-c", "pass")
    await exited.wait()
    return exited.pid


def _supervisor() -> Supervisor:
    spec = ClusterSpec(gates=0, datacenters={})
    return Supervisor(timeouts=spec.timeouts, ports=PortAllocator(host=spec.host))


async def _scanned_pids(supervisor: Supervisor) -> set[int]:
    zombies = await asyncio.get_running_loop().run_in_executor(None, supervisor._scan_zombie_processes)
    return {zombie.pid for zombie in zombies}


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["live", "dead", "unrecorded"])
async def test_preflight_reaps_only_processes_whose_owner_is_gone(owner: str) -> None:
    owner_pid = {"live": os.getpid(), "dead": await _exited_pid(), "unrecorded": None}[owner]
    tagged = await _spawn_tagged(owner_pid)
    try:
        scanned = await _scanned_pids(_supervisor())
        assert (tagged.pid in scanned) is (owner != "live")
    finally:
        tagged.terminate()
        await tagged.wait()
