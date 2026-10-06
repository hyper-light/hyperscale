"""
A restarted manager supersedes its previous incarnation at the same address.

A manager that restarts comes back at the same TCP address under a new
node id. The worker kept every incarnation: resolving the address
returned the FIRST one registered -- a dead id whose circuit breaker the
failed sends to the downed process had opened -- so the worker skipped
its final results to the live manager and queued them until that stale
breaker half-opened on its own (measured: a resumed job's completion
~44s late in the crash-during-re-dispatch SIM). Every restart also left
another ManagerInfo behind.

Direct evidence (the manager's registration exchange or its own
heartbeat) now supersedes any other id at that address; second-hand
manager lists cannot displace the confirmed id. Driven through the real
WorkerRegistry.
"""

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import ManagerInfo
from hyperscale.distributed.nodes.worker.registry import WorkerRegistry
from hyperscale.distributed.swim.core import CircuitState

MANAGER_ADDR = ("10.0.0.5", 9000)
FIRST_INCARNATION = "manager-gen1"
SECOND_INCARNATION = "manager-gen2"


def select_lowest_id(manager_ids: set[str]) -> str | None:
    return min(manager_ids) if manager_ids else None


def manager_info(node_id: str) -> ManagerInfo:
    return ManagerInfo(
        node_id=node_id,
        tcp_host=MANAGER_ADDR[0],
        tcp_port=MANAGER_ADDR[1],
        udp_host=MANAGER_ADDR[0],
        udp_port=MANAGER_ADDR[1] + 1,
        datacenter="dc-1",
    )


def open_breakers(registry: WorkerRegistry, manager_id: str) -> None:
    for circuit in (
        registry.get_or_create_circuit(manager_id),
        registry.get_or_create_circuit_by_addr(MANAGER_ADDR),
    ):
        for _ in range(circuit.max_errors):
            circuit.record_error()
        assert circuit.circuit_state == CircuitState.OPEN


def make_registry_with_dead_first_incarnation() -> WorkerRegistry:
    registry = WorkerRegistry(None, circuit_breaker_config=Env().get_circuit_breaker_config(), select_manager=select_lowest_id)
    registry.confirm_manager(FIRST_INCARNATION, manager_info(FIRST_INCARNATION))
    registry.set_primary_manager(FIRST_INCARNATION)
    open_breakers(registry, FIRST_INCARNATION)
    return registry


def test_the_restarted_incarnation_supersedes_the_dead_one() -> None:
    registry = make_registry_with_dead_first_incarnation()

    registry.confirm_manager(SECOND_INCARNATION, manager_info(SECOND_INCARNATION))

    assert registry.get_manager_by_addr(MANAGER_ADDR).node_id == SECOND_INCARNATION
    assert registry.get_manager(FIRST_INCARNATION) is None
    assert not registry.is_circuit_open(SECOND_INCARNATION)
    assert not registry.is_circuit_open_by_addr(MANAGER_ADDR)
    assert registry._primary_manager_id == SECOND_INCARNATION


def test_hearsay_cannot_resurrect_a_superseded_incarnation() -> None:
    registry = make_registry_with_dead_first_incarnation()
    registry.confirm_manager(SECOND_INCARNATION, manager_info(SECOND_INCARNATION))

    registry.add_manager(FIRST_INCARNATION, manager_info(FIRST_INCARNATION))

    assert registry.get_manager(FIRST_INCARNATION) is None
    assert registry.get_manager_by_addr(MANAGER_ADDR).node_id == SECOND_INCARNATION


def test_reaping_the_confirmed_manager_releases_its_address() -> None:
    registry = WorkerRegistry(None, circuit_breaker_config=Env().get_circuit_breaker_config(), select_manager=select_lowest_id)
    registry.confirm_manager(SECOND_INCARNATION, manager_info(SECOND_INCARNATION))

    registry.remove_manager_state(SECOND_INCARNATION, MANAGER_ADDR)

    assert registry._manager_id_by_addr == {}
    registry.add_manager(FIRST_INCARNATION, manager_info(FIRST_INCARNATION))
    assert registry.get_manager(FIRST_INCARNATION) is not None
