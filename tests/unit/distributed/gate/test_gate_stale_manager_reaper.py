"""
The gate forgets managers that have gone silent.

The stale-manager reaper read the server's own manager-status maps,
which nothing ever wrote -- every heartbeat lands in GateRuntimeState --
so it never found a stale manager, and every departed manager's
heartbeat, health state and negotiated capabilities stayed forever.
Its incarnation lookups for SWIM suspect/confirm and the query-target
fallback read the same empty maps.

Driven through the real reaper step (``_cleanup_stale_manager`` over
``get_stale_manager_addrs``) on a real GateRuntimeState fed by its real
heartbeat writer:

* a manager silent past the cutoff is found and forgotten everywhere;
* a manager that is still heartbeating is kept;
* a forgotten manager that returns is re-learned from its heartbeat;
* a forgotten manager learned at runtime leaves its datacenter's address
  lists (which count the datacenter's expected managers), while an
  operator-declared one -- configured or joined -- stays in them;
* a forgotten manager leaves the datacenter health classifier
  (``DatacenterHealthManager``): its cached heartbeat stops counting
  toward the datacenter's managers and its phi detector is dropped, so
  manager churn neither inflates the count nor grows the maps; a
  datacenter whose every manager is forgotten classifies UNHEALTHY (lost),
  never INITIALIZING (warming up).
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.datacenters.datacenter_health_manager import DatacenterHealthManager
from hyperscale.distributed.env import Env
from hyperscale.distributed.health.phi_accrual_config import PhiAccrualConfig
from hyperscale.distributed.models import DatacenterHealth, ManagerHeartbeat
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState

DATACENTER = "dc-1"
SILENT_MANAGER = ("10.0.0.7", 9000)
SILENT_MANAGER_UDP = ("10.0.0.7", 9001)
LIVE_MANAGER = ("10.0.0.8", 9000)
LIVE_MANAGER_UDP = ("10.0.0.8", 9001)
SILENT_SINCE = 100.0
LIVE_SINCE = 400.0
STALE_CUTOFF = 300.0


def heartbeat(node_id: str, udp_address: tuple[str, int]):
    return SimpleNamespace(
        node_id=node_id,
        is_leader=False,
        incarnation=3,
        udp_host=udp_address[0],
        udp_port=udp_address[1],
    )


def manager_heartbeat(node_id: str, udp_address: tuple[str, int]) -> ManagerHeartbeat:
    return ManagerHeartbeat(
        node_id=node_id,
        datacenter=DATACENTER,
        is_leader=False,
        term=1,
        version=1,
        active_jobs=0,
        active_workflows=0,
        worker_count=2,
        healthy_worker_count=2,
        available_cores=8,
        total_cores=8,
        udp_host=udp_address[0],
        udp_port=udp_address[1],
    )


def datacenter_health_manager_tracking(
    managers: dict[tuple[str, int], tuple[str, int]],
    configured_managers: list[tuple[str, int]] | None = None,
) -> DatacenterHealthManager:
    """The gate's datacenter health classifier, fed one real heartbeat per
    manager (TCP address -> UDP address)."""
    health_manager = DatacenterHealthManager(
        phi_config=PhiAccrualConfig.for_manager_heartbeats(Env()),
        get_configured_managers=lambda datacenter_id: configured_managers or [],
    )
    for manager_addr, udp_address in managers.items():
        health_manager.update_manager(
            DATACENTER, manager_addr, manager_heartbeat(f"manager-{manager_addr[0]}", udp_address)
        )
    return health_manager


async def make_gate(
    declared_managers: frozenset[tuple[str, int]] = frozenset(),
) -> tuple[GateServer, GateRuntimeState]:
    state = GateRuntimeState(forward_throughput_interval_start=0.0)
    await state.update_manager_status(
        DATACENTER, SILENT_MANAGER, heartbeat("silent", SILENT_MANAGER_UDP), SILENT_SINCE
    )
    await state.update_manager_status(DATACENTER, LIVE_MANAGER, heartbeat("live", LIVE_MANAGER_UDP), LIVE_SINCE)
    state._manager_health[(DATACENTER, SILENT_MANAGER)] = object()
    state._manager_negotiated_caps[SILENT_MANAGER] = object()
    gate = object.__new__(GateServer)
    gate._modular_state = state
    gate._manager_selector = SimpleNamespace(forget_manager=lambda manager_addr: None)
    gate._circuit_breaker_manager = SimpleNamespace(remove_circuit=AsyncMock())
    gate._datacenter_managers = {DATACENTER: [SILENT_MANAGER, LIVE_MANAGER]}
    gate._datacenter_manager_udp = {DATACENTER: [SILENT_MANAGER_UDP, LIVE_MANAGER_UDP]}
    gate._declared_datacenter_managers = {DATACENTER: declared_managers}
    gate._dc_health_manager = datacenter_health_manager_tracking(
        {SILENT_MANAGER: SILENT_MANAGER_UDP, LIVE_MANAGER: LIVE_MANAGER_UDP}
    )
    return gate, state


@pytest.mark.asyncio
async def test_a_silent_manager_is_found_and_forgotten() -> None:
    gate, state = await make_gate()

    stale = state.get_stale_manager_addrs(STALE_CUTOFF)
    assert stale == [SILENT_MANAGER]
    for manager_addr in stale:
        await GateServer._cleanup_stale_manager(gate, manager_addr)

    assert state.get_manager_status(DATACENTER, SILENT_MANAGER) is None
    assert SILENT_MANAGER not in state._manager_last_status
    assert (DATACENTER, SILENT_MANAGER) not in state._manager_health
    assert SILENT_MANAGER not in state._manager_negotiated_caps
    assert state.get_manager_status(DATACENTER, LIVE_MANAGER) is not None


@pytest.mark.asyncio
async def test_a_forgotten_manager_that_returns_is_relearned() -> None:
    gate, state = await make_gate()
    await GateServer._cleanup_stale_manager(gate, SILENT_MANAGER)

    await state.update_manager_status(DATACENTER, SILENT_MANAGER, heartbeat("silent", SILENT_MANAGER_UDP), LIVE_SINCE)

    assert state.get_manager_status(DATACENTER, SILENT_MANAGER) is not None
    assert state.get_stale_manager_addrs(STALE_CUTOFF) == []


@pytest.mark.asyncio
async def test_a_forgotten_learned_manager_leaves_its_datacenter_address_lists() -> None:
    gate, _ = await make_gate()

    await GateServer._cleanup_stale_manager(gate, SILENT_MANAGER)

    assert gate._datacenter_managers[DATACENTER] == [LIVE_MANAGER]
    assert gate._datacenter_manager_udp[DATACENTER] == [LIVE_MANAGER_UDP]


@pytest.mark.asyncio
async def test_a_forgotten_declared_manager_stays_in_its_datacenter_address_lists() -> None:
    gate, state = await make_gate(declared_managers=frozenset({SILENT_MANAGER}))

    await GateServer._cleanup_stale_manager(gate, SILENT_MANAGER)

    assert state.get_manager_status(DATACENTER, SILENT_MANAGER) is None
    assert gate._datacenter_managers[DATACENTER] == [SILENT_MANAGER, LIVE_MANAGER]
    assert gate._datacenter_manager_udp[DATACENTER] == [SILENT_MANAGER_UDP, LIVE_MANAGER_UDP]


@pytest.mark.asyncio
async def test_a_datacenter_whose_last_manager_is_forgotten_leaves_no_entry() -> None:
    state = GateRuntimeState(forward_throughput_interval_start=0.0)
    await state.update_manager_status(
        DATACENTER, SILENT_MANAGER, heartbeat("silent", SILENT_MANAGER_UDP), SILENT_SINCE
    )

    await state.remove_manager(SILENT_MANAGER)

    assert DATACENTER not in state._datacenter_manager_status


@pytest.mark.asyncio
async def test_a_forgotten_manager_leaves_the_datacenter_health_classifier() -> None:
    gate, _ = await make_gate()
    health_manager = gate._dc_health_manager
    assert health_manager.get_best_manager_heartbeat(DATACENTER)[2] == 2

    await GateServer._cleanup_stale_manager(gate, SILENT_MANAGER)

    _, _, tracked_count = health_manager.get_best_manager_heartbeat(DATACENTER)
    assert tracked_count == 1
    assert health_manager.get_manager_info(DATACENTER, SILENT_MANAGER) is None
    assert (DATACENTER, SILENT_MANAGER) not in health_manager._manager_detectors
    assert health_manager.get_manager_info(DATACENTER, LIVE_MANAGER) is not None
    assert (DATACENTER, LIVE_MANAGER) in health_manager._manager_detectors


@pytest.mark.asyncio
async def test_manager_churn_does_not_grow_the_datacenter_health_classifier() -> None:
    """Managers come and go (learned at runtime, each on a new address);
    after each is reaped, only the live ones are tracked."""
    gate, _ = await make_gate()
    health_manager = gate._dc_health_manager
    for generation in range(50):
        churned_manager = (f"10.1.{generation}.1", 9000)
        health_manager.update_manager(
            DATACENTER, churned_manager, manager_heartbeat(f"churned-{generation}", (churned_manager[0], 9001))
        )
        await GateServer._cleanup_stale_manager(gate, churned_manager)

    assert health_manager.get_best_manager_heartbeat(DATACENTER)[2] == 2
    assert len(health_manager._manager_detectors) == 2


@pytest.mark.asyncio
async def test_a_datacenter_whose_every_manager_is_forgotten_is_lost_not_warming_up() -> None:
    gate, _ = await make_gate()
    gate._dc_health_manager = datacenter_health_manager_tracking(
        {SILENT_MANAGER: SILENT_MANAGER_UDP}, configured_managers=[SILENT_MANAGER]
    )

    await GateServer._cleanup_stale_manager(gate, SILENT_MANAGER)

    assert gate._dc_health_manager.get_datacenter_health(DATACENTER).health == DatacenterHealth.UNHEALTHY.value


def test_a_configured_datacenter_never_heard_from_is_warming_up() -> None:
    health_manager = datacenter_health_manager_tracking({}, configured_managers=[SILENT_MANAGER])
    health_manager.add_datacenter(DATACENTER)

    assert health_manager.get_datacenter_health(DATACENTER).health == DatacenterHealth.INITIALIZING.value


@pytest.mark.asyncio
async def test_a_forgotten_manager_that_returns_counts_again() -> None:
    gate, _ = await make_gate()
    await GateServer._cleanup_stale_manager(gate, SILENT_MANAGER)

    gate._dc_health_manager.update_manager(DATACENTER, SILENT_MANAGER, manager_heartbeat("silent", SILENT_MANAGER_UDP))

    assert gate._dc_health_manager.get_best_manager_heartbeat(DATACENTER)[2] == 2
