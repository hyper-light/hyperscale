"""
A workflow result for a job that already ended is acked stale.

Once a job completes its lease is released, so a late result for it --
e.g. a crashed generation's run the worker is still delivering -- missed
the leader path and forwarding found no leader: the manager answered
"not job leader", the worker kept retrying it until its pending-result
TTL, and each retry went out over the network (traced in the
crash-during-re-dispatch SIM: retries every ~5s after the job ended).
A result cannot change a terminal job, so the manager now acks it
stale and the worker drops it.

Driven through the real ``workflow_final_result``.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import WorkflowFinalResult, WorkflowFinalResultAck
from hyperscale.distributed.nodes.manager.server import ManagerServer

JOB = "job-1"


def final_result() -> bytes:
    return WorkflowFinalResult(
        job_id=JOB,
        workflow_id=f"dc:mgr:{JOB}:wf-1:worker-1",
        workflow_name="Workflow",
        status="completed",
        results=[],
        context_updates=b"",
    ).dump()


def make_manager(tracked_status: str | None, ledger_terminal: bool | None) -> ManagerServer:
    manager = object.__new__(ManagerServer)
    tracked_job = SimpleNamespace(status=tracked_status) if tracked_status else None
    manager._job_manager = SimpleNamespace(get_job=lambda job_id: tracked_job)
    ledger_job = SimpleNamespace(is_terminal=ledger_terminal) if ledger_terminal is not None else None
    manager._job_ledger = SimpleNamespace(get_job=lambda job_id: ledger_job)
    manager._host, manager._tcp_port = "10.0.0.1", 9000
    manager._node_id = SimpleNamespace(full="manager-a", short="manager-a")
    manager.is_leader = lambda: True
    manager.leader_path_reached = False

    def not_leader(job_id: str) -> bool:
        manager.leader_path_reached = True
        return False

    async def forward(result, data):
        return WorkflowFinalResultAck(accepted=False, manager_id="manager-a", reason="forwarded-for-test").dump()

    manager._leases = SimpleNamespace(is_job_leader=not_leader)
    manager._forward_workflow_final_result_to_leader = forward
    return manager


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tracked_status,ledger_terminal",
    [("completed", None), ("failed", None), (None, True)],
)
async def test_a_result_for_an_ended_job_is_acked_stale(tracked_status, ledger_terminal) -> None:
    manager = make_manager(tracked_status, ledger_terminal)

    ack = WorkflowFinalResultAck.load(await ManagerServer.workflow_final_result(manager, ("10.0.0.7", 9100), final_result(), 0))

    assert ack.accepted and ack.stale
    assert ack.reason == "job_terminal"
    assert not manager.leader_path_reached


@pytest.mark.asyncio
async def test_a_result_for_a_running_job_takes_the_leadership_path() -> None:
    manager = make_manager("running", None)

    ack = WorkflowFinalResultAck.load(await ManagerServer.workflow_final_result(manager, ("10.0.0.7", 9100), final_result(), 0))

    assert manager.leader_path_reached
    assert ack.reason == "forwarded-for-test"
