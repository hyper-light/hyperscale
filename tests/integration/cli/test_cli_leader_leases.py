"""
E2E: leader leases (AD-52 section 11), real processes.

Three `hyperscale run manager` processes form one datacenter's membership
group. With `--leader-lease-enabled` on every one, linearizable status
reads (`hyperscale cluster`) are served from the leader's lease -- the
leader's exported `cluster_raft_lease_read_total` counts them. Without
the flag every read takes a quorum round of its own and the counter stays
at zero: the flag, not a default, turns leases on.

Bounds come from configuration: boot by --boot-timeout.

Run from the repo root (the commands are invoked from `.venv/bin`).
"""

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
    run_cluster_metrics,
    run_cluster_status,
)

COHORT_SIZE = 3
CLIENT_BLOCK = 2  # `hyperscale cluster` client tcp + its udp (port + 1)
# Status reads issued through each member: enough that the leader answers
# several (members pass theirs on to it).
READS_PER_MEMBER = 3


@pytest.fixture
def run_marker() -> str:
    return f"cli-leader-leases-{time.monotonic_ns()}"


async def _lease_reads_after_status_reads(run_marker: str, datacenter: str, lease_flags: tuple[str, ...]) -> int:
    """Boot a cohort with ``lease_flags``, read its status through every
    member, and return the lease reads its members export together."""
    *manager_starts, client_start = reserve_port_blocks([NODE_BLOCK] * COHORT_SIZE + [CLIENT_BLOCK])
    manager_tcp = [f"{LOCALHOST}:{start}" for start in manager_starts]
    manager_udp = [f"{LOCALHOST}:{start + 1}" for start in manager_starts]
    managers = [
        node_at(
            "manager",
            start,
            run_marker,
            "--datacenter", datacenter,
            "--managers", *manager_tcp,
            "--manager-udp", *manager_udp,
            *lease_flags,
            "--log-level", "debug",
        )
        for start in manager_starts
    ]
    try:
        await boot(*managers)
        for manager in managers:
            assert await manager.wait_for_output("formed: member", within=BOOT_TIMEOUT_SECONDS), (
                f"manager {manager.address} never formed its membership"
            )

        for manager in managers:
            for _ in range(READS_PER_MEMBER):
                returncode, output = await run_cluster_status(manager.address, client_start)
                assert returncode == 0, output

        lease_reads = 0
        for manager in managers:
            returncode, output = await run_cluster_metrics(manager.address, client_start)
            assert returncode == 0, output
            (lease_line,) = [
                line for line in output.splitlines() if line.startswith("cluster_raft_lease_read_total{")
            ]
            lease_reads += int(lease_line.rsplit(" ", 1)[1])
        return lease_reads
    finally:
        await kill_remaining(managers)


async def test_status_reads_are_served_from_the_leaders_lease(run_marker: str) -> None:
    lease_reads = await _lease_reads_after_status_reads(run_marker, "dc-leases", ("--leader-lease-enabled",))
    assert lease_reads > 0, "no status read was served from the leader's lease"


async def test_without_the_flag_every_read_takes_a_round(run_marker: str) -> None:
    lease_reads = await _lease_reads_after_status_reads(run_marker, "dc-no-leases", ())
    assert lease_reads == 0, f"{lease_reads} reads served from a lease nobody enabled"
