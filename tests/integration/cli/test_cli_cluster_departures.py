"""
E2E: managers leaving their cluster's membership (AD-52 section 13), real
processes.

Three `hyperscale run manager` processes form one datacenter's membership
group. Asserts:

1. `hyperscale remove` of a member that still answers is refused: a live
   member would only claim its address again.
2. A manager SIGKILLed for good is removed by `hyperscale remove` without
   waiting out the tombstone retention (minutes) -- once the survivors have
   a leader, if it led them.
3. While an operator holds the membership frozen (`hyperscale membership
   --mode frozen`), that removal is refused; opened again, it goes through.
4. An operator stop (Ctrl-C to the process group) drains the manager's
   membership before it exits.

Bounds come from configuration: boot by --boot-timeout; a leader past a
dead one by the CheckQuorum window (the transport's request timeout plus
the election timeout) and a formation round to see it.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

import asyncio
import signal
import time

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.manager.config import create_manager_config_from_env
from hyperscale.distributed.raft.raft_node import ELECTION_TIMEOUT_MAX
from tests.integration.cli.node_processes import (
    CLI_TEST_AUTH_SECRET,
    BOOT_TIMEOUT_SECONDS,
    LOCALHOST,
    NODE_BLOCK,
    boot,
    kill_remaining,
    node_at,
    reserve_port_blocks,
    run_membership_mode,
    run_remove,
)

ENV = Env(MERCURY_SYNC_AUTH_SECRET=CLI_TEST_AUTH_SECRET)
DATACENTER = "dc-departures"
COHORT_SIZE = 3
CLIENT_BLOCK = 2  # `hyperscale remove` client tcp + its udp (port + 1)
_MANAGER_CONFIG = create_manager_config_from_env(LOCALHOST, 1, 2, ENV)
# A dead leader is noticed within the CheckQuorum window and replaced by
# an election; the next formation round sees the new leader.
NEW_LEADER_BOUND_SECONDS = (
    _MANAGER_CONFIG.tcp_timeout_standard_seconds
    + 2 * ELECTION_TIMEOUT_MAX
    + ENV.CLUSTER_FORMATION_INTERVAL_SECONDS
)


@pytest.fixture
def run_marker() -> str:
    return f"cli-departures-{time.monotonic_ns()}"


async def test_members_leave_by_removal_and_by_drain(run_marker: str) -> None:
    *manager_starts, first_client, second_client, third_client, mode_client = reserve_port_blocks(
        [NODE_BLOCK] * COHORT_SIZE + [CLIENT_BLOCK] * 4
    )
    manager_tcp = [f"{LOCALHOST}:{start}" for start in manager_starts]
    manager_udp = [f"{LOCALHOST}:{start + 1}" for start in manager_starts]
    managers = [
        node_at(
            "manager",
            start,
            run_marker,
            "--datacenter", DATACENTER,
            "--managers", *manager_tcp,
            "--manager-udp", *manager_udp,
            "--log-level", "debug",
        )
        for start in manager_starts
    ]
    surviving, draining, killed = managers

    try:
        await boot(*managers)
        for manager in managers:
            assert await manager.wait_for_output(
                "formed: member", within=BOOT_TIMEOUT_SECONDS
            ), f"manager {manager.address} never formed its membership"

        returncode, output = await run_remove(surviving.address, draining.address, first_client)
        assert returncode == 1 and "still answers" in output, output

        returncode, output = await run_membership_mode(surviving.address, "frozen", mode_client)
        assert returncode == 0 and "cluster mode frozen" in output, output
        killed.process.send_signal(signal.SIGKILL)
        await killed.process.wait()

        frozen_output = await _remove_until_released(
            surviving.address, killed.address, second_client, within=NEW_LEADER_BOUND_SECONDS
        )
        assert "membership is frozen" in frozen_output, frozen_output
        returncode, output = await run_membership_mode(surviving.address, "open", mode_client)
        assert returncode == 0 and "cluster mode open" in output, output

        removed_output = await _remove_until_released(
            surviving.address, killed.address, second_client, within=NEW_LEADER_BOUND_SECONDS
        )
        assert "removed" in removed_output, removed_output

        exited, survivors = await draining.stop(signal.SIGINT, whole_group=True)
        assert exited and survivors == []
        assert any("Left cluster membership as" in line for line in draining.lines), (
            "".join(draining.lines[-40:])
        )

        # The drained address is no longer held: removing it finds no member.
        # The drained manager led the survivor, which names it until it
        # elects past it.
        output = await _remove_until_answered(
            surviving.address, draining.address, third_client, within=NEW_LEADER_BOUND_SECONDS
        )
        assert "no member holds" in output, output

    finally:
        await kill_remaining(managers)


async def _remove_until_answered(node: str, member: str, client_port: int, within: float) -> str:
    """`hyperscale remove` until a leader answers it -- not a departed
    leader the survivors still name, unreachable until they elect past it."""
    deadline = time.monotonic() + within
    while True:
        _returncode, output = await run_remove(node, member, client_port)
        if "is unreachable" not in output or time.monotonic() >= deadline:
            return output
        await asyncio.sleep(ENV.CLUSTER_FORMATION_INTERVAL_SECONDS)


async def _remove_until_released(node: str, member: str, client_port: int, within: float) -> str:
    """`hyperscale remove` until it is answered by the group's leader --
    released, or refused for the cluster's mode: while the dead member led
    the survivors, they name it as leader until they elect past it."""
    deadline = time.monotonic() + within
    while True:
        returncode, output = await run_remove(node, member, client_port)
        if returncode == 0 or "membership is" in output or time.monotonic() >= deadline:
            return output
        await asyncio.sleep(ENV.CLUSTER_FORMATION_INTERVAL_SECONDS)
