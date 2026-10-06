"""
E2E: a gate follows a datacenter's manager membership (AD-52 sections
9-10), real processes.

Three `hyperscale run manager` processes form one datacenter's membership
group; `hyperscale join` joins them to a `hyperscale run gate`, which from
then on watches the datacenter's membership into its soft-state cache.
`hyperscale resize add` grows the cohort by a fourth manager, which boots
and claims its address: the gate learns it from the watch -- its routing
lists take it in, and it registers with it -- without any operator step
on the gate.

Bounds come from configuration: boot by --boot-timeout; the gate hears the
change within two watch polls (each its wait plus the request's budget).

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

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
    run_join,
    run_resize,
)

ENV = Env()
DATACENTER = "dc-gate-follows"
COHORT_SIZE = 3
CLIENT_BLOCK = 2  # a CLI client's tcp + its udp (port + 1)
# Two polls of the gate's watch: the one in flight when the change
# commits, and the next.
WATCH_BOUND_SECONDS = 2 * (ENV.CLUSTER_WATCH_WAIT_SECONDS + ENV.GATE_TCP_TIMEOUT_STANDARD)


@pytest.fixture
def run_marker() -> str:
    return f"cli-gate-follows-{time.monotonic_ns()}"


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


async def test_a_gate_learns_a_manager_a_resize_added(run_marker: str) -> None:
    *founding_starts, added_start, gate_start, join_client, resize_client = reserve_port_blocks(
        [NODE_BLOCK] * (COHORT_SIZE + 2) + [CLIENT_BLOCK] * 2
    )
    founders = [_manager(start, run_marker, founding_starts) for start in founding_starts]
    added = _manager(added_start, run_marker, [*founding_starts, added_start])
    gate = node_at("gate", gate_start, run_marker, "--log-level", "debug")
    nodes = [*founders, added, gate]

    try:
        await boot(*founders, gate)
        for manager in founders:
            assert await manager.wait_for_output("formed: member", within=BOOT_TIMEOUT_SECONDS), (
                f"manager {manager.address} never formed its membership"
            )

        returncode, output = await run_join(founders[0].address, gate.address, join_client)
        assert returncode == 0, output

        returncode, output = await run_resize(founders[0].address, "add", added.address, resize_client)
        assert returncode == 0 and added.address in output, output
        await boot(added)
        assert await added.wait_for_output("formed: member", within=BOOT_TIMEOUT_SECONDS), (
            "".join(added.lines[-40:])
        )

        assert await gate.wait_for_output(
            f"{DATACENTER}'s manager membership changed: joined ['{added.address}']",
            within=WATCH_BOUND_SECONDS,
        ), "".join(gate.lines[-60:])

    finally:
        await kill_remaining(nodes)
