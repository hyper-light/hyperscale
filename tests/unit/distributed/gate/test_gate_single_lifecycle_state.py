"""
The gate has one lifecycle state and one state version.

GateServer kept its own ``_gate_state`` and ``_state_version`` beside
GateRuntimeState's. Startup sync moved only the server's state to
ACTIVE, so the ping handler -- which reads GateRuntimeState -- answered
"syncing" for the gate's whole life. Handlers bumped GateRuntimeState's
version while gossip, snapshots and state-sync comparisons read the
server's, so handler-side changes never advanced the version peers
synced against.

Driven through the real startup-sync transition and the real ping
handler over a real GateRuntimeState.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import GatePingResponse, GateState, PingRequest
from hyperscale.distributed.nodes.gate.handlers.tcp_ping import GatePingHandler
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState


def make_ping_handler(state: GateRuntimeState) -> GatePingHandler:
    return GatePingHandler(
        state=state,
        logger=SimpleNamespace(log=None),
        get_node_id=lambda: SimpleNamespace(full="gate-a-full", datacenter="dc-1"),
        get_host=lambda: "10.0.0.1",
        get_tcp_port=lambda: 9000,
        is_leader=lambda: True,
        get_current_term=lambda: 1,
        classify_dc_health=None,
        count_active_dcs=lambda: 0,
        get_all_job_ids=lambda: [],
        get_datacenter_managers=lambda: {},
    )


async def ping(handler: GatePingHandler) -> GatePingResponse:
    async def no_exception(error, context):
        raise error

    return GatePingResponse.load(
        await handler.handle_ping(("10.0.0.9", 9500), PingRequest(request_id="r-1").dump(), no_exception)
    )


@pytest.mark.asyncio
async def test_ping_reports_active_once_startup_sync_completes() -> None:
    state = GateRuntimeState(forward_throughput_interval_start=0.0)
    gate = object.__new__(GateServer)
    gate._modular_state = state
    gate.is_leader = lambda: True
    handler = make_ping_handler(state)

    assert (await ping(handler)).state == GateState.SYNCING.value
    await GateServer._complete_startup_sync(gate)
    assert (await ping(handler)).state == GateState.ACTIVE.value


@pytest.mark.asyncio
async def test_handler_and_server_version_bumps_advance_one_version() -> None:
    state = GateRuntimeState(forward_throughput_interval_start=0.0)
    gate = object.__new__(GateServer)
    gate._modular_state = state

    await state.increment_state_version()  # a handler-side change
    GateServer._increment_version(gate)  # a server/dispatch-side change

    assert state.get_state_version() == 2
    state.adopt_state_version(1)
    assert state.get_state_version() == 2, "a stale peer snapshot never lowers the version"
