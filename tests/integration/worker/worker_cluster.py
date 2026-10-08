"""
A datacenter of in-process managers with workers registering to it, for
the worker integration tests: the managers seeded with one another, a
start that waits for the managers' cluster to form before the workers
boot, and a teardown that always stops every node it started.
"""

import asyncio
import contextlib
import pathlib
from collections.abc import AsyncIterator

from hyperscale.distributed.models import ManagerState
from hyperscale.distributed.nodes import ManagerServer, WorkerServer
from tests.integration.in_process_nodes import LOCALHOST, node_env, stop_nodes, wait_until

# The scripts these tests replace slept 15 s for the managers' cluster to
# form and 15 s for workers to register; these bounds are generous
# multiples of that, and each wait ends as soon as its condition holds.
MANAGER_CLUSTER_FORMATION_SECONDS = 60.0
WORKER_REGISTRATION_SECONDS = 60.0
# A worker's start spawns its executor pool (bounded by the pool's own
# WORKER_POOL_STARTUP_TIMEOUT_SECONDS) before it registers.
NODE_START_SECONDS = 120.0
NODE_STOP_SECONDS = 30.0


def new_manager_cluster(
    node_directory: pathlib.Path,
    datacenter_id: str,
    manager_tcp_ports: list[int],
    **env_overrides: str,
) -> list[ManagerServer]:
    """One manager per TCP port (UDP is TCP + 1), each seeded with every
    other manager's TCP and UDP address."""
    return [
        ManagerServer(
            host=LOCALHOST,
            tcp_port=manager_tcp_port,
            udp_port=manager_tcp_port + 1,
            env=node_env(node_directory, **env_overrides),
            dc_id=datacenter_id,
            seed_managers=[(LOCALHOST, peer_port) for peer_port in manager_tcp_ports if peer_port != manager_tcp_port],
            manager_udp_peers=[
                (LOCALHOST, peer_port + 1) for peer_port in manager_tcp_ports if peer_port != manager_tcp_port
            ],
        )
        for manager_tcp_port in manager_tcp_ports
    ]


def manager_cluster_formed(managers: list[ManagerServer]) -> bool:
    """Every manager is ACTIVE and exactly one of them leads."""
    return (
        all(manager._manager_state.manager_state_enum == ManagerState.ACTIVE for manager in managers)
        and sum(manager.is_leader() for manager in managers) == 1
    )


@contextlib.asynccontextmanager
async def running_cluster(
    managers: list[ManagerServer],
    workers: list[WorkerServer],
) -> AsyncIterator[None]:
    """Start the managers together, wait for their cluster to form, then
    start the workers together; on exit stop the workers, then the
    managers -- every node whose start was attempted, whatever failed."""
    started_managers: list[ManagerServer] = []
    started_workers: list[WorkerServer] = []
    try:
        started_managers.extend(managers)
        async with asyncio.timeout(NODE_START_SECONDS), asyncio.TaskGroup() as manager_starts:
            for manager in managers:
                manager_starts.create_task(manager.start())

        await wait_until(
            lambda: manager_cluster_formed(managers),
            within_seconds=MANAGER_CLUSTER_FORMATION_SECONDS,
            description=f"the {len(managers)} managers all ACTIVE under exactly one leader",
        )

        started_workers.extend(workers)
        async with asyncio.timeout(NODE_START_SECONDS), asyncio.TaskGroup() as worker_starts:
            for worker in workers:
                worker_starts.create_task(worker.start())

        yield
    finally:
        try:
            await stop_nodes(started_workers, within_seconds=NODE_STOP_SECONDS)
        finally:
            await stop_nodes(started_managers, within_seconds=NODE_STOP_SECONDS)
