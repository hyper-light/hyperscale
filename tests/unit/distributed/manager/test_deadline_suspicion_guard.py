"""
AD-26 deadline enforcement must not re-suspect a worker the failure
detector already suspects or has declared dead.

The guard compared the detector's status against a second NodeStatus
enum defined in nodes/manager/health.py -- a distinct type, so it never
matched, and every enforcement pass (5s) re-suspected (incarnation 0)
workers the detector already held suspected or dead. The server now
compares against the detector's own enum and the duplicate is gone.

Driven through the manager's real ``_suspect_worker_deadline_expired``
with the real detector NodeStatus.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.env import Env
from hyperscale.distributed.slo import SLOConfig
from hyperscale.distributed.swim.detection.hierarchical_failure_detector import NodeStatus

WORKER = "worker-1"


def make_manager(detector_status: NodeStatus) -> tuple[ManagerServer, AsyncMock]:
    state = ManagerState(slo_config=SLOConfig.from_env(Env()))
    state.add_worker(WORKER, SimpleNamespace(node=SimpleNamespace(host="10.0.0.7", udp_port=9101)))
    detector = SimpleNamespace(get_node_status=AsyncMock(return_value=detector_status))
    suspect = AsyncMock()
    manager = object.__new__(ManagerServer)
    manager._manager_state = state
    manager.get_hierarchical_detector = lambda: detector
    manager.suspect_node_global = suspect
    manager._host, manager._udp_port, manager._tcp_port = "10.0.0.1", 9001, 9000
    manager._udp_logger = SimpleNamespace(log=AsyncMock())
    manager._node_id = SimpleNamespace(short="manager-a")
    return manager, suspect


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [NodeStatus.SUSPECTED_GLOBAL, NodeStatus.DEAD_GLOBAL])
async def test_a_worker_already_suspected_or_dead_is_not_suspected_again(status: NodeStatus) -> None:
    manager, suspect = make_manager(status)
    await ManagerServer._suspect_worker_deadline_expired(manager, WORKER)
    suspect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [NodeStatus.ALIVE, NodeStatus.SUSPECTED_JOB])
async def test_a_worker_not_globally_suspected_is_suspected(status: NodeStatus) -> None:
    manager, suspect = make_manager(status)
    await ManagerServer._suspect_worker_deadline_expired(manager, WORKER)
    suspect.assert_awaited_once()
