"""
E2E: growing a datacenter's manager cohort at runtime (AD-52
``ResizeCluster``), real processes.

Three `hyperscale run manager` processes form one cohort. Asserts:

1. `hyperscale resize --add` grows the cohort by the fourth address, and
   `hyperscale cluster` -- a linearizable read -- shows it at once.
2. A manager launched there with the four-manager cohort joins the
   cluster's membership: `hyperscale cluster` lists it among the voters.
3. A second resize is refused while the first three still run with the
   three-manager cohort: no process may fall two resizes behind.
4. `hyperscale cluster --watch`, started before the resize, prints the
   resize and the new manager's claim as they commit, and stops on Ctrl-C.
5. `hyperscale cluster --metrics` reports the four-address cohort and the
   applied resize in Prometheus text format.

Bounds come from configuration: boot by --boot-timeout.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import signal
import time

import pytest

from hyperscale.distributed.env import Env
from tests.integration.cli.node_processes import (
    CLI_TEST_AUTH_SECRET,
    command_environment,
    BOOT_TIMEOUT_SECONDS,
    HYPERSCALE,
    LOCALHOST,
    NODE_BLOCK,
    boot,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    run_cluster_metrics,
    run_cluster_status,
    run_resize,
)

ENV = Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET)
DATACENTER = "dc-resize"
COHORT_SIZE = 3
CLIENT_BLOCK = 2  # `hyperscale resize` client tcp + its udp (port + 1)


@pytest.fixture
def run_marker() -> str:
    return f"cli-resize-{time.monotonic_ns()}"


def _manager(start: int, marker: str, cohort_starts: list[int]):
    return node_at(
        "manager",
        start,
        marker,
        "--datacenter", DATACENTER,
        "--managers", *[f"{LOCALHOST}:{cohort_start}" for cohort_start in cohort_starts],
        "--manager-udp", *[f"{LOCALHOST}:{cohort_start + 1}" for cohort_start in cohort_starts],
        "--log-level", "debug",
    )


async def test_the_cohort_grows_by_one_manager(run_marker: str) -> None:
    *founding_starts, added_start, unused_start, first_client, second_client, status_client, watch_client = (
        reserve_port_blocks([NODE_BLOCK] * (COHORT_SIZE + 2) + [CLIENT_BLOCK] * 4)
    )
    founders = [_manager(start, run_marker, founding_starts) for start in founding_starts]
    added = _manager(added_start, run_marker, [*founding_starts, added_start])
    managers = [*founders, added]
    watcher: asyncio.subprocess.Process | None = None

    try:
        await boot(*founders)
        for manager in founders:
            assert await manager.wait_for_output(
                "formed: member", within=BOOT_TIMEOUT_SECONDS
            ), f"manager {manager.address} never formed its membership"

        watcher = await asyncio.create_subprocess_exec(
            HYPERSCALE, "cluster", "--node", founders[0].address, "--watch", "--port", str(watch_client),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=command_environment(),
        )

        returncode, output = await run_resize(founders[0].address, "add", added.address, first_client)
        assert returncode == 0 and added.address in output, output
        returncode, output = await run_cluster_status(founders[1].address, status_client)
        cohort_line = next(line for line in output.splitlines() if line.startswith("cohort"))
        assert returncode == 0 and added.address in cohort_line, output

        await boot(added)
        assert await added.wait_for_output("formed: member", within=BOOT_TIMEOUT_SECONDS), (
            "".join(added.lines[-40:])
        )
        returncode, output = await _status_until_voter(founders[2].address, added.address, status_client)
        assert returncode == 0, output

        watched = await _read_until(watcher, (" resize ", f" claim ", f"@{added.address}#"), BOOT_TIMEOUT_SECONDS)
        watcher.send_signal(signal.SIGINT)
        assert await asyncio.wait_for(watcher.wait(), timeout=BOOT_TIMEOUT_SECONDS) == 130
        assert " resize " in watched and added.address in watched, watched
        assert any(" claim " in line and f"@{added.address}#" in line for line in watched.splitlines()), watched

        returncode, output = await run_cluster_metrics(founders[0].address, status_client)
        assert returncode == 0, output
        assert any(line.startswith("cluster_size{") and line.endswith(" 4") for line in output.splitlines()), output
        assert any(
            'type="cluster_resize"' in line and line.endswith(" 1") for line in output.splitlines()
        ), output

        returncode, output = await run_resize(
            founders[0].address, "add", f"{LOCALHOST}:{unused_start}", second_client
        )
        assert returncode == 1 and "older cohort" in output, output

    finally:
        if watcher is not None and watcher.returncode is None:
            watcher.kill()
            await watcher.wait()
        await kill_remaining(managers)


async def _status_until_voter(node: str, member_address: str, client_port: int) -> tuple[int, str]:
    """`hyperscale cluster` until the member is a voter: it joins as a
    learner and the leader promotes it once it holds every committed entry
    -- within a few heartbeats of joining; bounded by the boot timeout."""
    deadline = time.monotonic() + BOOT_TIMEOUT_SECONDS
    while True:
        returncode, output = await run_cluster_status(node, client_port)
        voters = next((line for line in output.splitlines() if line.startswith("voters")), "")
        if f"@{member_address}#" in voters or time.monotonic() >= deadline:
            return (returncode if f"@{member_address}#" in voters else 1), output
        await asyncio.sleep(ENV.CLUSTER_FORMATION_INTERVAL_SECONDS)


async def _read_until(process, fragments: tuple[str, ...], within: float) -> str:
    """The watch's output, read until every fragment has appeared (or
    ``within`` seconds pass)."""
    lines: list[str] = []
    deadline = time.monotonic() + within
    while not all(any(fragment in line for line in lines) for fragment in fragments):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            line = await asyncio.wait_for(process.stdout.readline(), timeout=remaining)
        except asyncio.TimeoutError:
            break
        if not line:
            break
        lines.append(line.decode(errors="replace"))
    return "".join(lines)
