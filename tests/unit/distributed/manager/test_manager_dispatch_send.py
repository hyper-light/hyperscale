"""
ManagerDispatchCoordinator: sending one workflow dispatch (AD-27 -- the
single implementation, moved from the server; the coordinator's old
dispatch path, which bumped the job's fence on every dispatch outside
Raft, and its never-received quorum provisioning are gone).

Every outcome is recorded on the worker pool without touching SWIM
health:

* accepted -> success recorded, the dispatch counted for throughput;
* a readiness rejection ("draining", "capacity", ...) -> the worker's
  routing cools down;
* any other rejection -> the worker answered: delivered, not cooled;
* no answer / transport error -> transport failure recorded;
* a worker the registry does not know -> its stale pool entry purged.
The dispatch carries this manager as its job leader when unset.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.models import WorkflowDispatchAck
from hyperscale.distributed.nodes.manager.dispatch import ManagerDispatchCoordinator

WORKER = "worker-1"
WORKER_ADDR = ("10.0.0.7", 9100)
MANAGER_ADDR = ("10.0.0.1", 9000)


class RecordingPool:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def record_dispatch_success(self, worker_id: str) -> bool:
        self.calls.append(("success", worker_id))
        return True

    def record_dispatch_readiness_rejection(self, worker_id: str, error: str) -> bool:
        self.calls.append(("readiness", worker_id, error))
        return True

    def record_dispatch_transport_failure(self, worker_id: str, error: str) -> bool:
        self.calls.append(("transport", worker_id, error))
        return True

    async def deregister_worker(self, worker_id: str) -> bool:
        self.calls.append(("purged", worker_id))
        return True

    async def notify_cores_available(self) -> None:
        self.calls.append(("notified",))


def make_coordinator(send_result, known_worker: bool = True):
    pool = RecordingPool()
    stats = SimpleNamespace(record_dispatch=AsyncMock())
    registration = SimpleNamespace(node=SimpleNamespace(host=WORKER_ADDR[0], port=WORKER_ADDR[1]))
    registry = SimpleNamespace(get_worker=lambda worker_id: registration if known_worker else None)
    send_tcp = AsyncMock(side_effect=send_result) if isinstance(send_result, Exception) else AsyncMock(return_value=send_result)
    coordinator = ManagerDispatchCoordinator(
        registry=registry,
        worker_pool=pool,
        stats=stats,
        send_tcp=send_tcp,
        logger=SimpleNamespace(log=AsyncMock()),
        node_host=MANAGER_ADDR[0],
        node_port=MANAGER_ADDR[1],
        node_id="manager-a",
        dispatch_timeout_seconds=5.0,
    )
    return coordinator, pool, stats, send_tcp


def dispatch():
    return SimpleNamespace(job_leader_addr=None, dump=lambda: b"dispatch")


def ack(accepted: bool, error: str | None = None) -> tuple[bytes, int]:
    return WorkflowDispatchAck(workflow_id="wf-1", accepted=accepted, error=error).dump(), 0


@pytest.mark.asyncio
async def test_an_accepted_dispatch_is_recorded_and_counted() -> None:
    coordinator, pool, stats, send_tcp = make_coordinator(ack(True))
    sent = dispatch()

    assert await coordinator.send_workflow_dispatch(WORKER, sent) is True
    assert pool.calls == [("success", WORKER), ("notified",)]
    stats.record_dispatch.assert_awaited_once()
    assert send_tcp.await_args.args[:2] == (WORKER_ADDR, "workflow_dispatch")
    assert sent.job_leader_addr == MANAGER_ADDR


@pytest.mark.asyncio
async def test_a_readiness_rejection_cools_the_workers_routing() -> None:
    coordinator, pool, stats, _send = make_coordinator(ack(False, "Worker is draining"))
    assert await coordinator.send_workflow_dispatch(WORKER, dispatch()) is False
    assert pool.calls == [("readiness", WORKER, "Worker is draining"), ("notified",)]
    stats.record_dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_any_other_rejection_counts_as_delivered() -> None:
    coordinator, pool, _stats, _send = make_coordinator(ack(False, "duplicate workflow"))
    assert await coordinator.send_workflow_dispatch(WORKER, dispatch()) is False
    assert pool.calls == [("success", WORKER), ("notified",)]


@pytest.mark.asyncio
@pytest.mark.parametrize("send_result", [(None, 0), (ConnectionError("refused"), 0), ConnectionError("raised")])
async def test_no_answer_is_a_transport_failure(send_result) -> None:
    coordinator, pool, _stats, _send = make_coordinator(send_result)
    assert await coordinator.send_workflow_dispatch(WORKER, dispatch()) is False
    assert pool.calls[0][:2] == ("transport", WORKER)


@pytest.mark.asyncio
async def test_a_worker_the_registry_does_not_know_is_purged() -> None:
    coordinator, pool, _stats, send_tcp = make_coordinator(ack(True), known_worker=False)
    assert await coordinator.send_workflow_dispatch(WORKER, dispatch()) is False
    assert pool.calls == [("purged", WORKER), ("notified",)]
    send_tcp.assert_not_awaited()
