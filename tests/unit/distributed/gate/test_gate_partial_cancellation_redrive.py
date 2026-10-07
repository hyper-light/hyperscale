"""
A cancel some of its job's datacenters confirmed reaches the rest (AD-20).

The gate forwards a client's cancel to every datacenter the job runs in,
each manager once. The first confirmation marked the job CANCELLED -- and a
datacenter that did not confirm (its job leader mid-failover, or out of
reach) was never asked again: the client's repeat cancel was answered
"already cancelled" at the gate, and the datacenter's progress was dropped
as a terminal job's. It ran the job on for the rest of its budget.

The real ``GateCancellationHandler``, ``GateJobManager`` and
``GateRuntimeState``; the managers are the far end of ``send_tcp``, each
answering ``cancel_job`` as a manager does while it can be reached. The
gate's cancellation re-drive loop is the handler's
``redrive_pending_cancellations``, called here pass by pass.

* the datacenter that did not confirm is re-driven until it is reachable,
  then cancelled, and its confirmation is durably recorded; nothing stays
  pending;
* a datacenter the job moved to after the cancel (AD-36) is driven too;
* a job the gate retires before every datacenter confirmed is no longer
  re-driven, and nothing stays pending.
"""

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs.gates.gate_job_manager import GateJobManager
from hyperscale.distributed.models import (
    GlobalJobStatus,
    JobCancelRequest,
    JobCancelResponse,
    JobStatus,
)
from hyperscale.distributed.nodes.gate.handlers.tcp_cancellation import GateCancellationHandler
from hyperscale.distributed.nodes.gate.state import GateRuntimeState

GATE_SETTINGS = Env()
JOB_ID = "job-partial-cancel"
CLIENT_ADDRESS = ("10.0.9.1", 7000)
DATACENTER_MANAGERS = {
    "dc-east": [("10.0.1.1", 8000)],
    "dc-west": [("10.0.2.1", 8000)],
    "dc-north": [("10.0.3.1", 8000)],
}
WORKFLOWS_PER_DATACENTER = 3


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


class NodeIdentity:
    full = "gate-1-full"
    short = "gate-1"
    datacenter = "global"


class UnusedTaskRunner:
    """The cancel path runs nothing in the background."""

    def run(self, *args: object, **kwargs: object) -> None:
        raise AssertionError(f"unexpected background run: {args}")


class Datacenters:
    """The managers at the far end of the gate's ``send_tcp``: each answers
    ``cancel_job`` while its datacenter is reachable, and records it."""

    def __init__(self) -> None:
        self.unreachable: set[str] = set()
        self.cancels_received: dict[str, int] = {datacenter: 0 for datacenter in DATACENTER_MANAGERS}

    def datacenter_of(self, manager_addr: tuple[str, int]) -> str:
        [datacenter] = [
            datacenter for datacenter, managers in DATACENTER_MANAGERS.items() if tuple(manager_addr) in managers
        ]
        return datacenter

    async def send_tcp(
        self, manager_addr: tuple[str, int], action: str, payload: bytes, timeout: float
    ) -> tuple[bytes | Exception, float]:
        assert action == "cancel_job"
        datacenter = self.datacenter_of(manager_addr)
        if datacenter in self.unreachable:
            return ConnectionRefusedError(f"{manager_addr} unreachable"), 0.0
        self.cancels_received[datacenter] += 1
        request = JobCancelRequest.load(payload)
        return (
            JobCancelResponse(
                job_id=request.job_id,
                success=True,
                cancelled_workflow_count=WORKFLOWS_PER_DATACENTER,
            ).dump(),
            0.0,
        )


class Gate:
    """The gate's cancel path, its job store, and its durable cancellation record."""

    def __init__(self, target_datacenters: set[str]) -> None:
        self.datacenters = Datacenters()
        self.job_manager = GateJobManager()
        self.job_manager.set_job(JOB_ID, GlobalJobStatus(job_id=JOB_ID, status=JobStatus.RUNNING.value))
        self.job_manager.set_target_dcs(JOB_ID, target_datacenters)
        self.recorded_acks: list[str] = []
        self.handler = GateCancellationHandler(
            state=GateRuntimeState(forward_throughput_interval_start=0.0),
            logger=RecordingLogger(),
            task_runner=UnusedTaskRunner(),
            job_manager=self.job_manager,
            datacenter_managers=DATACENTER_MANAGERS,
            get_node_id=NodeIdentity,
            get_host=lambda: "10.0.0.1",
            get_tcp_port=lambda: 9000,
            check_rate_limit=self.admit,
            send_tcp=self.datacenters.send_tcp,
            record_cancellation=self.record_cancellation,
            client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_SHORT,
            manager_request_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        )

    async def admit(self, client_id: str, operation: str, handler_name: str) -> tuple[bool, float]:
        return True, 0.0

    async def record_cancellation(
        self,
        job_id: str,
        reason: str,
        requester_id: str,
        confirmed_datacenters: list[tuple[str, int]],
    ) -> None:
        self.recorded_acks.extend(datacenter for datacenter, _ in confirmed_datacenters)

    async def cancel(self) -> JobCancelResponse:
        async def raise_error(error: Exception, operation: str) -> None:
            raise error

        request = JobCancelRequest(job_id=JOB_ID, requester_id="client-1", timestamp=1.0, reason="user")
        return JobCancelResponse.load(
            await self.handler.handle_cancel_job(CLIENT_ADDRESS, request.dump(), raise_error)
        )


@pytest.mark.asyncio
async def test_an_unconfirmed_datacenter_is_cancelled_once_it_is_reached() -> None:
    gate = Gate({"dc-east", "dc-west"})
    gate.datacenters.unreachable.add("dc-west")

    response = await gate.cancel()

    assert response.success
    assert gate.job_manager.get_job(JOB_ID).status == JobStatus.CANCELLED.value
    assert gate.recorded_acks == ["dc-east"]

    # The client's repeat is answered at the gate, as before.
    assert (await gate.cancel()).already_cancelled

    # Re-driven while it stays out of reach: still not cancelled.
    await gate.handler.redrive_pending_cancellations()
    assert gate.datacenters.cancels_received == {"dc-east": 1, "dc-west": 0, "dc-north": 0}
    assert gate.recorded_acks == ["dc-east"]

    # Reached: cancelled, and its confirmation recorded.
    gate.datacenters.unreachable.clear()
    await gate.handler.redrive_pending_cancellations()
    assert gate.datacenters.cancels_received == {"dc-east": 1, "dc-west": 1, "dc-north": 0}
    assert gate.recorded_acks == ["dc-east", "dc-west"]

    # Every target confirmed: nothing is re-driven, nothing kept.
    await gate.handler.redrive_pending_cancellations()
    assert gate.datacenters.cancels_received == {"dc-east": 1, "dc-west": 1, "dc-north": 0}
    assert gate.handler._pending_cancellations == {}


@pytest.mark.asyncio
async def test_a_datacenter_the_job_moved_to_is_cancelled_too() -> None:
    gate = Gate({"dc-east", "dc-west"})
    gate.datacenters.unreachable.add("dc-west")
    assert (await gate.cancel()).success

    # AD-36: the job moved off the unreachable datacenter, to another.
    gate.job_manager.move_target_dc(JOB_ID, "dc-west", "dc-north")
    await gate.handler.redrive_pending_cancellations()

    assert gate.datacenters.cancels_received == {"dc-east": 1, "dc-west": 0, "dc-north": 1}
    assert gate.recorded_acks == ["dc-east", "dc-north"]
    assert gate.handler._pending_cancellations == {}


@pytest.mark.asyncio
async def test_a_retired_job_is_no_longer_redriven() -> None:
    gate = Gate({"dc-east", "dc-west"})
    gate.datacenters.unreachable.add("dc-west")
    assert (await gate.cancel()).success

    gate.job_manager.delete_job(JOB_ID)
    gate.datacenters.unreachable.clear()
    await gate.handler.redrive_pending_cancellations()
    await gate.handler.redrive_pending_cancellations()

    assert gate.datacenters.cancels_received == {"dc-east": 1, "dc-west": 0, "dc-north": 0}
    assert gate.handler._pending_cancellations == {}


@pytest.mark.asyncio
async def test_a_cancel_every_datacenter_confirmed_keeps_nothing() -> None:
    gate = Gate({"dc-east", "dc-west"})

    assert (await gate.cancel()).success

    assert gate.recorded_acks == ["dc-east", "dc-west"]
    assert gate.handler._pending_cancellations == {}
