"""
Manager peer state sync answers a full snapshot.

The full-snapshot branch of ``state_sync_request`` read
``ManagerState._job_progress`` -- an attribute that never existed -- so
every request behind the responder's version raised, and the handler's
error path answered ``responder_ready=False, current_version=0``: no peer
sync (the cluster-leader takeover's forced full sync included) ever
delivered state.

Driven through the real handler over a real ManagerState: a requester
behind the responder's version receives a snapshot carrying the
responder's job leadership and fence tokens.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import StateSyncRequest, StateSyncResponse
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.nodes.manager.state import ManagerState

CLUSTER_ID = "hyperscale"
ENVIRONMENT_ID = "default"


class RecordingTaskRunner:
    def __init__(self) -> None:
        self.calls: list = []

    def run(self, call, *args, **kwargs) -> None:
        self.calls.append((call, args))


def make_manager(state: ManagerState) -> tuple[ManagerServer, RecordingTaskRunner]:
    manager = object.__new__(ManagerServer)
    task_runner = RecordingTaskRunner()

    async def no_mtls_error(addr, role, requester_id):
        return None

    manager._manager_state = state
    manager._config = SimpleNamespace(
        cluster_id=CLUSTER_ID, environment_id=ENVIRONMENT_ID, datacenter_id="dc-east"
    )
    manager._node_id = SimpleNamespace(full="manager-a-full", short="manager-a")
    manager._host, manager._tcp_port = "127.0.0.1", 9000
    manager._validate_mtls_claims = no_mtls_error
    manager._task_runner = task_runner
    manager._udp_logger = SimpleNamespace(log=None)
    manager.is_leader = lambda: True
    manager._leader_election = SimpleNamespace(state=SimpleNamespace(current_term=3))
    manager._job_manager = SimpleNamespace(iter_jobs=lambda: [])
    return manager, task_runner


@pytest.mark.asyncio
async def test_a_requester_behind_the_responders_version_gets_a_full_snapshot() -> None:
    state = ManagerState()
    await state.increment_state_version()
    state._job_leaders["job-1"] = "manager-a-full"
    state._job_leader_addrs["job-1"] = ("127.0.0.1", 9000)
    state._job_fencing_tokens["job-1"] = 4
    manager, task_runner = make_manager(state)

    reply = await ManagerServer.state_sync_request(
        manager,
        ("127.0.0.1", 9100),
        StateSyncRequest(
            requester_id="manager-b-full",
            requester_role="manager",
            cluster_id=CLUSTER_ID,
            environment_id=ENVIRONMENT_ID,
            since_version=-1,
        ).dump(),
        0,
    )
    response = StateSyncResponse.load(reply)

    failures = [args for call, args in task_runner.calls if "failed" in str(args)]
    assert failures == [], failures
    assert response.current_version == state.state_version
    snapshot = response.manager_state
    assert snapshot is not None, "no snapshot: the full-sync branch failed"
    assert snapshot.job_leaders == {"job-1": "manager-a-full"}
    assert snapshot.job_fence_tokens == {"job-1": 4}
    assert snapshot.term == 3
