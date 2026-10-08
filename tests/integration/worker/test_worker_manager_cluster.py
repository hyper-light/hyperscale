"""
Four workers seeded with every manager of a three-manager datacenter:
the managers form one cluster (each knows its peers, all ACTIVE, exactly
one leader), every manager tracks every worker (registration plus the
managers' cross-manager worker sync), and every worker knows every
manager and has a primary manager.
"""

import pathlib

from hyperscale.distributed.models import ManagerState
from hyperscale.distributed.nodes import ManagerServer, WorkerServer
from tests.integration.in_process_nodes import (
    LOCALHOST,
    node_env,
    reserve_cluster_ports,
    wait_until,
)
from tests.integration.worker.worker_cluster import (
    MANAGER_CLUSTER_FORMATION_SECONDS,
    WORKER_REGISTRATION_SECONDS,
    new_manager_cluster,
    running_cluster,
)

DATACENTER_ID = "DC-EAST"
MANAGER_COUNT = 3
WORKER_CORES = [4, 4, 4, 4]
LOG_LEVEL = "error"


def manager_knows_its_peers(manager: ManagerServer, managers: list[ManagerServer]) -> bool:
    """``manager``'s SWIM membership holds every other manager's UDP address."""
    known_addresses = {address for address, _ in manager._incarnation_tracker.get_all_nodes()}
    return all(
        (LOCALHOST, peer._udp_port) in known_addresses for peer in managers if peer is not manager
    )


async def test_workers_register_with_and_discover_every_manager_of_the_cluster(
    node_directory: pathlib.Path,
) -> None:
    manager_tcp_ports, worker_tcp_ports = reserve_cluster_ports(MANAGER_COUNT, WORKER_CORES)
    managers = new_manager_cluster(
        node_directory,
        DATACENTER_ID,
        manager_tcp_ports,
        MERCURY_SYNC_LOG_LEVEL=LOG_LEVEL,
    )
    seed_managers = [(LOCALHOST, manager._tcp_port) for manager in managers]
    workers = [
        WorkerServer(
            host=LOCALHOST,
            tcp_port=worker_tcp_port,
            udp_port=worker_tcp_port + 1,
            env=node_env(node_directory, MERCURY_SYNC_LOG_LEVEL=LOG_LEVEL),
            dc_id=DATACENTER_ID,
            total_cores=worker_cores,
            seed_managers=seed_managers,
        )
        for worker_tcp_port, worker_cores in zip(worker_tcp_ports, WORKER_CORES, strict=True)
    ]

    async with running_cluster(managers, workers):
        manager_ids = {manager._node_id.full for manager in managers}
        worker_ids = {worker._node_id.full for worker in workers}

        await wait_until(
            lambda: all(manager_knows_its_peers(manager, managers) for manager in managers),
            within_seconds=MANAGER_CLUSTER_FORMATION_SECONDS,
            description=f"each manager knowing its {MANAGER_COUNT - 1} manager peers in SWIM membership",
        )
        await wait_until(
            lambda: all(set(manager._manager_state.get_all_workers()) == worker_ids for manager in managers),
            within_seconds=WORKER_REGISTRATION_SECONDS,
            description=f"every manager tracking all {len(workers)} workers",
        )
        await wait_until(
            lambda: all(manager_ids <= set(worker._known_managers) for worker in workers),
            within_seconds=WORKER_REGISTRATION_SECONDS,
            description=f"every worker knowing all {MANAGER_COUNT} managers",
        )

        manager_states = {manager._node_id.short: manager._manager_state.manager_state_enum for manager in managers}
        assert all(state == ManagerState.ACTIVE for state in manager_states.values()), (
            f"every manager should be ACTIVE: {manager_states}"
        )
        leaders = [manager._node_id.short for manager in managers if manager.is_leader()]
        assert len(leaders) == 1, f"expected exactly one manager leader, got {leaders}"

        tracked_worker_ids = {
            manager._node_id.short: set(manager._manager_state.get_all_workers()) for manager in managers
        }
        assert all(tracked == worker_ids for tracked in tracked_worker_ids.values()), (
            f"cross-manager worker sync incomplete: expected {worker_ids} on every manager, got {tracked_worker_ids}"
        )

        primary_manager_ids = {worker._node_id.short: worker._primary_manager_id for worker in workers}
        assert all(primary in manager_ids for primary in primary_manager_ids.values()), (
            f"every worker should have one of the managers {manager_ids} as primary, got {primary_manager_ids}"
        )
