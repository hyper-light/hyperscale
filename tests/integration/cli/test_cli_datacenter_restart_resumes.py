"""
E2E (D1, live CLI): a datacenter whose managers all lose power comes back
as the same cluster -- real `hyperscale run manager` processes, each with
its own `--data-directory`.

Before D1 every Raft group was volatile: when every manager restarted at
once, no member survived to keep the membership group, so the cohort
founded a new cluster (a new ``cluster_uuid``) and every gate saw a
regenerated datacenter. Now each manager's Raft store keeps its identity
and every group's term, vote and log:

* all three SIGKILLed and restarted on their directories resume as the
  same members of the same cluster (``member_resumed``), and
  ``hyperscale cluster`` reads the same ``cluster_uuid``;
* a manager whose directory was wiped comes back as a new member of that
  same cluster, beside the two that resumed.

Bounds come from configuration: boot by --boot-timeout; forming (or
re-forming) within the boot bound.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import re
import shutil
import signal
import tempfile
import time

import pytest

from tests.integration.cli.node_processes import (
    BOOT_TIMEOUT_SECONDS,
    LOCALHOST,
    NODE_BLOCK,
    boot,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    run_cluster_status,
    stop_all,
)

DATACENTER = "dc-restart-resumes"
COHORT_SIZE = 3
CLIENT_BLOCK = 2
CLUSTER_STATUS = re.compile(r"cluster (\S+) \(mode")


@pytest.fixture
def run_marker() -> str:
    return f"cli-dc-restart-{time.monotonic_ns()}"


def _manager(start: int, marker: str, cohort_starts: list[int], data_directory: str):
    return node_at(
        "manager",
        start,
        marker,
        "--datacenter", DATACENTER,
        "--managers", *[f"{LOCALHOST}:{cohort_start}" for cohort_start in cohort_starts],
        "--manager-udp", *[f"{LOCALHOST}:{cohort_start + 1}" for cohort_start in cohort_starts],
        "--data-directory", data_directory,
        "--log-level", "debug",
    )


async def _cluster_uuid(manager_address: str, client_port: int) -> str:
    returncode, output = await run_cluster_status(manager_address, client_port)
    assert returncode == 0, output
    match = CLUSTER_STATUS.search(output)
    assert match is not None, output
    return match.group(1)


async def _formed(managers) -> None:
    for manager in managers:
        assert await manager.wait_for_output("formed: member", within=BOOT_TIMEOUT_SECONDS), (
            f"manager {manager.address} never formed its membership:\n" + "".join(manager.lines[-40:])
        )


@pytest.mark.parametrize("wiped_managers", [0, 1])
async def test_a_datacenter_restarted_whole_comes_back_as_the_same_cluster(
    run_marker: str, wiped_managers: int
) -> None:
    *cohort_starts, client_before, client_after = reserve_port_blocks(
        [NODE_BLOCK] * COHORT_SIZE + [CLIENT_BLOCK] * 2
    )
    data_directories = [tempfile.mkdtemp(prefix="hyperscale-dc-restart-") for _ in cohort_starts]
    first_run = [
        _manager(start, run_marker, cohort_starts, directory)
        for start, directory in zip(cohort_starts, data_directories)
    ]
    second_run = [
        _manager(start, run_marker, cohort_starts, directory)
        for start, directory in zip(cohort_starts, data_directories)
    ]
    nodes = [*first_run, *second_run]
    try:
        await boot(*first_run)
        await _formed(first_run)
        cluster_before = await _cluster_uuid(first_run[0].address, client_before)

        # Power loss: every manager at once.
        await stop_all(first_run, signal.SIGKILL, whole_group=True)
        for directory in data_directories[:wiped_managers]:
            shutil.rmtree(directory)

        await boot(*second_run)
        await _formed(second_run)
        resumed = second_run[wiped_managers:]
        for manager in resumed:
            assert await manager.wait_for_output("member_resumed", within=BOOT_TIMEOUT_SECONDS), (
                f"manager {manager.address} did not resume from its disk:\n" + "".join(manager.lines[-40:])
            )
        for manager in second_run[:wiped_managers]:
            assert not await manager.wait_for_output("member_resumed", within=0.0), (
                f"manager {manager.address} resumed from a wiped disk"
            )

        assert await _cluster_uuid(second_run[-1].address, client_after) == cluster_before

        await stop_all(second_run, signal.SIGTERM, whole_group=False)
    finally:
        try:
            await kill_remaining(nodes)
        finally:
            for directory in data_directories:
                shutil.rmtree(directory, ignore_errors=True)
