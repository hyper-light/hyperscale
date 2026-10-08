"""
A gate aggregates each workflow's results once, and records the aggregate
it owes the client before sending it.

* Delivery failing answered the manager "error": it re-sent its result,
  which came back as a fresh partial -- the other datacenters' results had
  been taken for the aggregate -- and the per-workflow timeout then
  delivered a second aggregate calling them missing. The aggregate is
  recorded for the client before it is sent (and replayed when the
  client's callback registers again), so the push is accepted: "ok".
* Dedup was per ``(job, workflow, datacenter)`` sequence and recorded only
  after a successful send from the push path: a result a manager rebuilt
  under a new sequence, a duplicate of one the timeout delivered, or a
  datacenter's result arriving after the timeout recorded it missing was
  aggregated again. A workflow now has one aggregate: once its results are
  taken for it, every later push is acked.
* With no callback known, the aggregate was logged and dropped -- its
  results already taken. It is recorded like any other.

A real ``GateServer`` (never started) whose client is unreachable; its
clock does not wait out the push retries' backoff.
"""

import asyncio

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import GlobalJobStatus, JobStatus, WorkflowResultPush
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.runtime import RealClock

JOB_ID = "job-1"
WORKFLOW_ID = "wf-1"
CLIENT_CALLBACK = ("127.0.0.1", 19400)
TARGET_DATACENTERS = ["dc-a", "dc-b"]


class BackoffFreeClock:
    """The real clock, except that a sleep only yields: the client push
    retries' backoff costs the test nothing."""

    def __init__(self) -> None:
        self._clock = RealClock()

    def monotonic(self) -> float:
        return self._clock.monotonic()

    def monotonic_ns(self) -> int:
        return self._clock.monotonic_ns()

    def time(self) -> float:
        return self._clock.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(0)

    async def wait_for(self, awaitable, timeout: float | None):
        return await self._clock.wait_for(awaitable, timeout)


def make_gate() -> GateServer:
    gate = GateServer(
        host="127.0.0.1",
        tcp_port=19151,
        udp_port=19152,
        env=Env(MERCURY_SYNC_AUTH_SECRET="workflow-result-once-secret-0123456789"),
        datacenter_managers={
            "dc-a": [("127.0.0.1", 19251)],
            "dc-b": [("127.0.0.1", 19351)],
        },
        datacenter_manager_udp={
            "dc-a": [("127.0.0.1", 19252)],
            "dc-b": [("127.0.0.1", 19352)],
        },
        clock=BackoffFreeClock(),
    )

    async def client_unreachable(address, action, payload, timeout=None):
        return ConnectionRefusedError(f"{address} is not listening"), 0

    gate._send_tcp = client_unreachable
    return gate


def push_from(
    datacenter: str,
    result_sequence: int,
    callback_addr: tuple[str, int] | None = CLIENT_CALLBACK,
) -> bytes:
    return WorkflowResultPush(
        job_id=JOB_ID,
        workflow_id=WORKFLOW_ID,
        workflow_name="Checkout",
        datacenter=datacenter,
        status=JobStatus.COMPLETED.value,
        fence_token=1,
        results=[],
        callback_addr=callback_addr,
        target_dcs=TARGET_DATACENTERS,
        target_dc_count=len(TARGET_DATACENTERS),
        result_sequence=result_sequence,
    ).dump()


async def recorded_aggregates(gate: GateServer) -> list[WorkflowResultPush]:
    updates, _oldest_sequence, _latest_sequence = await gate._modular_state.get_client_updates_since(
        JOB_ID, 0
    )
    return [
        WorkflowResultPush.load(payload)
        for _sequence, message_type, payload, _recorded_at in updates
        if message_type == "workflow_result_push"
    ]


async def push(gate: GateServer, data: bytes) -> bytes:
    return await gate.workflow_result_push(("127.0.0.1", 19251), data, 0)


@pytest.mark.asyncio
async def test_an_undelivered_aggregate_is_accepted_and_a_late_result_acked() -> None:
    gate = make_gate()

    first_answer = await push(gate, push_from("dc-a", 1))
    aggregating_answer = await push(gate, push_from("dc-b", 1))
    # The manager's re-send after a lost answer, and one it rebuilt under a
    # new sequence after a failover.
    resent_answer = await push(gate, push_from("dc-b", 1))
    rebuilt_answer = await push(gate, push_from("dc-a", 2))

    aggregates = await recorded_aggregates(gate)
    assert (first_answer, aggregating_answer, resent_answer, rebuilt_answer) == (
        b"stored",
        b"ok",
        b"ok",
        b"ok",
    )
    assert [(aggregate.datacenter, aggregate.status) for aggregate in aggregates] == [
        ("aggregated", JobStatus.COMPLETED.value)
    ]
    assert gate._workflow_dc_results.get(JOB_ID, {}).get(WORKFLOW_ID) is None


@pytest.mark.asyncio
async def test_a_result_arriving_after_the_timeout_aggregated_without_it_is_acked() -> None:
    gate = make_gate()

    await push(gate, push_from("dc-a", 1))
    await gate._handle_workflow_result_timeout(JOB_ID, WORKFLOW_ID)
    late_answer = await push(gate, push_from("dc-b", 1))

    aggregates = await recorded_aggregates(gate)
    assert late_answer == b"ok"
    assert len(aggregates) == 1
    assert [result.datacenter for result in aggregates[0].per_dc_results] == ["dc-a", "dc-b"]
    assert aggregates[0].per_dc_results[1].status == "FAILED"


@pytest.mark.asyncio
async def test_an_aggregate_with_no_callback_is_recorded_not_dropped() -> None:
    gate = make_gate()
    gate._job_manager.set_job(
        JOB_ID,
        GlobalJobStatus(job_id=JOB_ID, status=JobStatus.RUNNING.value, timestamp=1.0),
    )

    await push(gate, push_from("dc-a", 1, callback_addr=None))
    answer = await push(gate, push_from("dc-b", 1, callback_addr=None))

    assert answer == b"ok"
    assert len(await recorded_aggregates(gate)) == 1
