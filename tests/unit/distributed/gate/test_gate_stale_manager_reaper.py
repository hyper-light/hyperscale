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
* a forgotten manager that returns is re-learned from its heartbeat.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState

DATACENTER = "dc-1"
SILENT_MANAGER = ("10.0.0.7", 9000)
LIVE_MANAGER = ("10.0.0.8", 9000)
SILENT_SINCE = 100.0
LIVE_SINCE = 400.0
STALE_CUTOFF = 300.0


def heartbeat(node_id: str):
    return SimpleNamespace(node_id=node_id, is_leader=False, incarnation=3)


async def make_gate() -> tuple[GateServer, GateRuntimeState]:
    state = GateRuntimeState()
    await state.update_manager_status(DATACENTER, SILENT_MANAGER, heartbeat("silent"), SILENT_SINCE)
    await state.update_manager_status(DATACENTER, LIVE_MANAGER, heartbeat("live"), LIVE_SINCE)
    state._manager_health[(DATACENTER, SILENT_MANAGER)] = object()
    state._manager_negotiated_caps[SILENT_MANAGER] = object()
    gate = object.__new__(GateServer)
    gate._modular_state = state
    gate._manager_selector = SimpleNamespace(forget_manager=lambda manager_addr: None)
    gate._circuit_breaker_manager = SimpleNamespace(remove_circuit=AsyncMock())
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

    await state.update_manager_status(DATACENTER, SILENT_MANAGER, heartbeat("silent"), LIVE_SINCE)

    assert state.get_manager_status(DATACENTER, SILENT_MANAGER) is not None
    assert state.get_stale_manager_addrs(STALE_CUTOFF) == []


@pytest.mark.asyncio
async def test_a_datacenter_whose_last_manager_is_forgotten_leaves_no_entry() -> None:
    state = GateRuntimeState()
    await state.update_manager_status(DATACENTER, SILENT_MANAGER, heartbeat("silent"), SILENT_SINCE)

    await state.remove_manager(SILENT_MANAGER)

    assert DATACENTER not in state._datacenter_manager_status
