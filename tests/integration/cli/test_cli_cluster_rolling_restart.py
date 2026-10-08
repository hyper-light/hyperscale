"""
E2E (AD-52 section 13 + D1, live CLI): a rolling restart of a datacenter's
managers keeps every one of them in the cluster -- real
`hyperscale run manager` processes, each with its own `--data-directory`.

A StatefulSet's rolling update stops each manager with SIGTERM, highest
ordinal first, and starts it again on the same address and volume before
moving on. SIGTERM drains: the manager has its cluster release its address
before it exits. Coming back, it resumes its membership group from its disk
-- a group whose leader no longer replicates to it, so it never becomes
operable on its own. It used to sit out of the cohort, "joining", until the
tombstone retention (CLUSTER_TOMBSTONE_RETENTION_SECONDS, ten minutes)
abandoned the group; its workers registered with a manager that could take
no work. Now it asks the leader its founders name to take it back.

Asserts, for each manager in turn: it drains and exits on SIGTERM, and once
restarted on its directory resumes (``member_resumed``) and is a member of
the formed cluster again within the boot bound -- far inside the retention.
At the end, `hyperscale cluster` lists all three as voters.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import re
import shutil
import signal
import tempfile
import time

import pytest

from hyperscale.distributed.env import Env
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

DATACENTER = "dc-rolling-restart"
COHORT_SIZE = 3
CLIENT_BLOCK = 2
VOTERS_LINE = re.compile(r"^voters\s+(.*)$", re.MULTILINE)
# How long a manager its cluster no longer counts used to wait before it
# could come back: the bound below must be far inside it to tell them apart.
TOMBSTONE_RETENTION_SECONDS = Env.model_fields["CLUSTER_TOMBSTONE_RETENTION_SECONDS"].default


@pytest.fixture
def run_marker() -> str:
    return f"cli-rolling-restart-{time.monotonic_ns()}"


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


async def _formed_as_member(manager) -> None:
    assert await manager.wait_for_output("formed: member", within=BOOT_TIMEOUT_SECONDS), (
        f"manager {manager.address} never formed as a member of its cluster:\n" + "".join(manager.lines[-40:])
    )


async def test_a_rolling_restart_keeps_every_manager_in_the_cluster(run_marker: str) -> None:
    assert BOOT_TIMEOUT_SECONDS < TOMBSTONE_RETENTION_SECONDS
    *cohort_starts, client_start = reserve_port_blocks([NODE_BLOCK] * COHORT_SIZE + [CLIENT_BLOCK])
    data_directories = [tempfile.mkdtemp(prefix="hyperscale-rolling-restart-") for _ in cohort_starts]
    running = [
        _manager(start, run_marker, cohort_starts, directory)
        for start, directory in zip(cohort_starts, data_directories)
    ]
    nodes = list(running)
    try:
        await boot(*running)
        for manager in running:
            await _formed_as_member(manager)

        # Highest ordinal first, one at a time, as a StatefulSet rolls.
        for index in reversed(range(COHORT_SIZE)):
            stopping_since = time.monotonic()
            exited, survivors = await running[index].stop(signal.SIGTERM, whole_group=False)
            assert exited and survivors == [], f"manager {running[index].address} did not stop: {survivors}"
            print(f"manager {running[index].address} stopped in {time.monotonic() - stopping_since:.1f}s")

            restarted = _manager(cohort_starts[index], run_marker, cohort_starts, data_directories[index])
            nodes.append(restarted)
            running[index] = restarted
            await boot(restarted)
            assert await restarted.wait_for_output("member_resumed", within=BOOT_TIMEOUT_SECONDS), (
                f"manager {restarted.address} did not resume from its disk:\n" + "".join(restarted.lines[-40:])
            )
            await _formed_as_member(restarted)

        returncode, output = await run_cluster_status(running[0].address, client_start)
        assert returncode == 0, output
        voters_line = VOTERS_LINE.search(output)
        assert voters_line is not None, output
        assert len(voters_line.group(1).split()) == COHORT_SIZE, output

        await stop_all(running, signal.SIGTERM, whole_group=False)
    finally:
        try:
            await kill_remaining(nodes)
        finally:
            for directory in data_directories:
                shutil.rmtree(directory, ignore_errors=True)
