"""
ManagerDispatchCoordinator: sending one workflow dispatch (AD-27 -- the
single implementation, moved from the server; the coordinator's old
dispatch path, which bumped the job's fence on every dispatch outside
Raft, and its never-received quorum provisioning are gone).

Every outcome is recorded on the worker pool without touching SWIM
health, and answered to the dispatcher as the outcome that decides what
a workflow no worker took costs (waits spend no retry budget; failed
deliveries do):

* accepted -> ACCEPTED; success recorded, the dispatch counted for
  throughput;
* a readiness rejection ("draining", "capacity", ...) -> NOT_READY; the
  worker's routing cools down;
* any other rejection -> REJECTED with the worker's error; the worker
  answered: delivered, not cooled;
* no answer / transport error -> UNREACHABLE; transport failure recorded;
* a worker the registry does not know -> UNROUTABLE; its stale pool entry
  purged.
The dispatch carries this manager as its job leader when unset.

Each dispatch is also the datacenter's AD-42 latency sample (dispatch ->
response); a workflow's run time had been sampled instead, so any DC that
ran a load test read as violating its SLO:

* an answered dispatch records its round trip;
* a dispatch never answered records the timeout it waited;
* a refused one (no wait, no answer) records nothing.

Each sample names the worker it timed (D-5: the manager keeps the samples
per worker too), and each send's outcome is counted for the metrics
surface (D-68): every send counted once, under the outcome it returned.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.jobs.dispatch_outcome import DispatchOutcome
from hyperscale.distributed.models import WorkflowDispatchAck
from hyperscale.distributed.nodes.manager.dispatch import ManagerDispatchCoordinator

WORKER = "worker-1"
WORKER_ADDR = ("10.0.0.7", 9100)
MANAGER_ADDR = ("10.0.0.1", 9000)
DISPATCH_TIMEOUT_SECONDS = 5.0
DISPATCHED_AT = 100.0
ROUND_TRIP_SECONDS = 0.025


class SteppingClock:
    """Reads the dispatch's send time, then its answer time."""

    def __init__(self) -> None:
        self._readings = iter((DISPATCHED_AT, DISPATCHED_AT + ROUND_TRIP_SECONDS))

    def monotonic(self) -> float:
        return next(self._readings)


class RecordingPool:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def record_dispatch_success(self, worker_id: str) -> bool:
        self.calls.append(("success", worker_id))
        return True

    async def record_dispatch_taken(self, worker_id: str, dispatch_token: str, allocated_at_version: int) -> None:
        self.calls.append(("taken", worker_id, dispatch_token, allocated_at_version))

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


def make_coordinator(send_result, known_worker: bool = True, latencies: list | None = None):
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
        dispatch_timeout_seconds=DISPATCH_TIMEOUT_SECONDS,
        clock=SteppingClock(),
        record_dispatch_latency=lambda worker_id, latency_ms, now: (
            latencies if latencies is not None else []
        ).append((worker_id, latency_ms, now)),
    )
    return coordinator, pool, stats, send_tcp


def dispatch():
    return SimpleNamespace(job_leader_addr=None, dump=lambda: b"dispatch")


# The worker's core availability version once it allocated the dispatch.
ACCEPTED_AT_VERSION = 7


def ack(accepted: bool, error: str | None = None) -> tuple[bytes, int]:
    return (
        WorkflowDispatchAck(
            workflow_id="wf-1", accepted=accepted, error=error, cores_version=ACCEPTED_AT_VERSION
        ).dump(),
        0,
    )


@pytest.mark.asyncio
async def test_an_accepted_dispatch_is_recorded_and_counted() -> None:
    coordinator, pool, stats, send_tcp = make_coordinator(ack(True))
    sent = dispatch()

    assert await coordinator.send_workflow_dispatch(WORKER, sent) == (DispatchOutcome.ACCEPTED, "")
    # The version the worker allocated the dispatch's cores at is recorded
    # against it, so the pool can tell when a report reflects it.
    assert pool.calls == [("taken", WORKER, "wf-1", ACCEPTED_AT_VERSION), ("success", WORKER), ("notified",)]
    stats.record_dispatch.assert_awaited_once()
    assert send_tcp.await_args.args[:2] == (WORKER_ADDR, "workflow_dispatch")
    assert sent.job_leader_addr == MANAGER_ADDR


@pytest.mark.asyncio
async def test_a_readiness_rejection_cools_the_workers_routing() -> None:
    coordinator, pool, stats, _send = make_coordinator(ack(False, "Worker is draining"))
    assert await coordinator.send_workflow_dispatch(WORKER, dispatch()) == (
        DispatchOutcome.NOT_READY,
        "Worker is draining",
    )
    assert pool.calls == [("readiness", WORKER, "Worker is draining"), ("notified",)]
    stats.record_dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_any_other_rejection_counts_as_delivered() -> None:
    coordinator, pool, _stats, _send = make_coordinator(ack(False, "duplicate workflow"))
    assert await coordinator.send_workflow_dispatch(WORKER, dispatch()) == (
        DispatchOutcome.REJECTED,
        "duplicate workflow",
    )
    assert pool.calls == [("success", WORKER), ("notified",)]


@pytest.mark.asyncio
@pytest.mark.parametrize("send_result", [(None, 0), (ConnectionError("refused"), 0), ConnectionError("raised")])
async def test_no_answer_is_a_transport_failure(send_result) -> None:
    coordinator, pool, _stats, _send = make_coordinator(send_result)
    outcome, _detail = await coordinator.send_workflow_dispatch(WORKER, dispatch())
    assert outcome == DispatchOutcome.UNREACHABLE
    assert pool.calls[0][:2] == ("transport", WORKER)


@pytest.mark.asyncio
async def test_a_worker_the_registry_does_not_know_is_purged() -> None:
    coordinator, pool, _stats, send_tcp = make_coordinator(ack(True), known_worker=False)
    outcome, _detail = await coordinator.send_workflow_dispatch(WORKER, dispatch())
    assert outcome == DispatchOutcome.UNROUTABLE
    assert pool.calls == [("purged", WORKER), ("notified",)]
    send_tcp.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_answered_dispatch_records_its_round_trip() -> None:
    latencies: list[tuple[str, float, float]] = []
    coordinator, _pool, _stats, _send = make_coordinator(ack(False, "duplicate workflow"), latencies=latencies)

    await coordinator.send_workflow_dispatch(WORKER, dispatch())

    ((timed_worker, latency_ms, recorded_at),) = latencies
    assert timed_worker == WORKER
    assert latency_ms == pytest.approx(ROUND_TRIP_SECONDS * 1000.0)
    assert recorded_at == DISPATCHED_AT + ROUND_TRIP_SECONDS


@pytest.mark.asyncio
async def test_a_dispatch_never_answered_records_the_timeout_it_waited() -> None:
    latencies: list[tuple[str, float, float]] = []
    coordinator, pool, _stats, _send = make_coordinator((TimeoutError(), 0), latencies=latencies)

    outcome, _detail = await coordinator.send_workflow_dispatch(WORKER, dispatch())
    assert outcome == DispatchOutcome.UNREACHABLE

    assert latencies == [(WORKER, DISPATCH_TIMEOUT_SECONDS * 1000.0, DISPATCHED_AT + ROUND_TRIP_SECONDS)]
    assert pool.calls[0][:2] == ("transport", WORKER)


@pytest.mark.asyncio
async def test_a_refused_dispatch_records_no_latency() -> None:
    latencies: list[tuple[str, float, float]] = []
    coordinator, _pool, _stats, _send = make_coordinator((ConnectionRefusedError("refused"), 0), latencies=latencies)

    await coordinator.send_workflow_dispatch(WORKER, dispatch())

    assert latencies == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("send_result", "known_worker", "expected_outcome"),
    [
        (ack(True), True, DispatchOutcome.ACCEPTED),
        (ack(False, "Worker is draining"), True, DispatchOutcome.NOT_READY),
        (ack(False, "duplicate workflow"), True, DispatchOutcome.REJECTED),
        ((None, 0), True, DispatchOutcome.UNREACHABLE),
        ((ConnectionError("refused"), 0), True, DispatchOutcome.UNREACHABLE),
        (ConnectionError("raised"), True, DispatchOutcome.UNREACHABLE),
        (ack(True), False, DispatchOutcome.UNROUTABLE),
    ],
)
async def test_each_send_is_counted_once_under_its_outcome(send_result, known_worker, expected_outcome) -> None:
    coordinator, _pool, _stats, _send = make_coordinator(send_result, known_worker=known_worker)
    assert set(coordinator.dispatch_outcome_counts().values()) == {0}

    outcome, _detail = await coordinator.send_workflow_dispatch(WORKER, dispatch())

    assert outcome == expected_outcome
    counts = coordinator.dispatch_outcome_counts()
    assert counts.pop(expected_outcome.value) == 1
    assert set(counts.values()) == {0}
    assert DispatchOutcome.WITHHELD.value not in counts
