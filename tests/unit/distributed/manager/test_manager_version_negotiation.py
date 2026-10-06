"""
AD-25 negotiation on the manager, owned by ManagerVersionSkewHandler
(AD-27: the server's inline gate and client negotiation are gone).

* A submitting client of another major version is refused; of the same
  major, its features are intersected with this manager's.
* A gate's negotiated capabilities are kept in ManagerState alone -- the
  handler kept a second private copy that removing the gate from state
  never cleared.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.env import Env
from hyperscale.distributed.slo import SLOConfig
from hyperscale.distributed.nodes.manager.version_skew import ManagerVersionSkewHandler
from hyperscale.distributed.protocol.version import (
    CURRENT_PROTOCOL_VERSION,
    NodeCapabilities,
    ProtocolVersion,
    get_features_for_version,
)


class RecordingTaskRunner:
    def run(self, *args, **kwargs) -> None:
        return None


def make_handler() -> tuple[ManagerVersionSkewHandler, ManagerState]:
    state = ManagerState(slo_config=SLOConfig.from_env(Env()))
    handler = ManagerVersionSkewHandler(
        state=state,
        config=SimpleNamespace(host="127.0.0.1", tcp_port=9000),
        logger=SimpleNamespace(log=AsyncMock()),
        node_id="manager-a",
        task_runner=RecordingTaskRunner(),
    )
    return handler, state


def test_a_client_of_another_major_version_is_refused() -> None:
    handler, _state = make_handler()
    other_major = ProtocolVersion(major=CURRENT_PROTOCOL_VERSION.major + 1, minor=0)
    assert handler.negotiate_with_client(other_major, "") is None


def test_a_clients_features_are_intersected_with_this_managers() -> None:
    handler, _state = make_handler()
    ours = get_features_for_version(CURRENT_PROTOCOL_VERSION)
    shared = sorted(ours)[:2]

    negotiated = handler.negotiate_with_client(
        CURRENT_PROTOCOL_VERSION, ",".join([*shared, "feature-from-the-future"])
    )

    assert negotiated == ",".join(shared)
    assert handler.negotiate_with_client(CURRENT_PROTOCOL_VERSION, "") == ""


@pytest.mark.parametrize("remove", ["handler", "state"])
async def test_a_gates_capabilities_live_in_one_store(remove: str) -> None:
    handler, state = make_handler()
    await handler.negotiate_with_gate("gate-1", NodeCapabilities.current())
    assert state.get_gate_negotiated_caps("gate-1") is handler.get_gate_capabilities("gate-1")

    if remove == "handler":
        handler.remove_gate("gate-1")
    else:
        state._gate_negotiated_caps.pop("gate-1")

    assert handler.get_gate_capabilities("gate-1") is None
    assert handler.gate_supports_feature("gate-1", next(iter(get_features_for_version(CURRENT_PROTOCOL_VERSION)))) is False
