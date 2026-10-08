"""
Replicated commits run outside the ledger lock, in each job's append order.

Every write path committed while holding the ledger's single lock. At
LOCAL that cost nothing; a REGIONAL commit is a consensus round trip that
can wait out an election, so one job's replication stalled every other
job's ledger writes for as long as it took.

Pinned against a real ledger (SimFilesystem) and a controllable regional
replicator: a blocked job does not block another job; each job's entries
reach the replicator in append order, also when a waiting commit is
cancelled; a checkpoint taken mid-replication cannot compact the entry
(its REGIONAL mark still lands); a cancelled commit still marks its
entry applied; sequencing state drains to empty.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import msgspec
import pytest

from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.job_commit_sequencer import JobCommitSequencer
from hyperscale.distributed.ledger.job_ledger import JobLedger
from hyperscale.distributed.ledger.wal.wal_entry import WALEntry
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

LEDGER_PATHS = {
    "wal_path": Path("/node/ledger/wal"),
    "checkpoint_dir": Path("/node/ledger/checkpoints"),
    "archive_dir": Path("/node/ledger/archive"),
    "region_code": "dc-east",
    "gate_id": "gate-1",
    "clock": new_hybrid_logical_clock(),
}
REGIONAL = DurabilityLevel.REGIONAL
# Failure-detection ceiling: every awaited step here completes in a few
# loop iterations when correct; a regression would otherwise hang.
LIVENESS_CEILING_SECONDS = 5.0


class GatedReplicator:
    """Records (job_id, event_type) per call; blocks jobs whose gate is held."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, JobEventType]] = []
        self.gates: dict[str, asyncio.Event] = {}

    def hold(self, job_id: str) -> None:
        self.gates[job_id] = asyncio.Event()

    def release(self, job_id: str) -> None:
        self.gates.pop(job_id).set()

    async def __call__(self, entry: WALEntry) -> bool:
        job_id = msgspec.msgpack.decode(entry.payload)[0]
        self.calls.append((job_id, entry.event_type))
        if (gate := self.gates.get(job_id)) is not None:
            await gate.wait()
        return True


async def _open(replicator: GatedReplicator) -> JobLedger:
    return await JobLedger.open(
        filesystem=SimFilesystem(),
        regional_replicator=replicator,
        **LEDGER_PATHS,
    )


async def _create(ledger: JobLedger, job_id: str):
    return await ledger.create_job(
        spec_hash=b"spec",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        job_id=job_id,
        durability=REGIONAL,
    )


async def _until(predicate) -> None:
    async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
        while not predicate():
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_a_blocked_job_does_not_block_another_job() -> None:
    replicator = GatedReplicator()
    ledger = await _open(replicator)
    replicator.hold("job-slow")
    slow = asyncio.create_task(_create(ledger, "job-slow"))
    await _until(lambda: ("job-slow", JobEventType.JOB_CREATED) in replicator.calls)

    async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
        _, fast_result = await _create(ledger, "job-fast")

    assert fast_result.level_achieved == REGIONAL
    assert not slow.done()
    replicator.release("job-slow")
    _, slow_result = await slow
    assert slow_result.level_achieved == REGIONAL
    await ledger.close()


@pytest.mark.asyncio
async def test_a_jobs_entries_replicate_in_append_order() -> None:
    replicator = GatedReplicator()
    ledger = await _open(replicator)
    replicator.hold("job-1")
    create = asyncio.create_task(_create(ledger, "job-1"))
    await _until(lambda: len(replicator.calls) == 1)

    accept = asyncio.create_task(
        ledger.accept_job("job-1", "dc-east", worker_count=1, durability=REGIONAL)
    )
    cancel = asyncio.create_task(
        ledger.request_cancellation("job-1", "operator", "client-1", durability=REGIONAL)
    )
    await asyncio.sleep(0.01)
    assert replicator.calls == [("job-1", JobEventType.JOB_CREATED)]

    replicator.release("job-1")
    await asyncio.gather(create, accept, cancel)

    assert replicator.calls == [
        ("job-1", JobEventType.JOB_CREATED),
        ("job-1", JobEventType.JOB_ACCEPTED),
        ("job-1", JobEventType.JOB_CANCELLATION_REQUESTED),
    ]
    await ledger.close()


@pytest.mark.asyncio
async def test_cancelled_waiter_keeps_later_entries_behind_its_predecessor() -> None:
    replicator = GatedReplicator()
    ledger = await _open(replicator)
    replicator.hold("job-1")
    create = asyncio.create_task(_create(ledger, "job-1"))
    await _until(lambda: len(replicator.calls) == 1)
    accept = asyncio.create_task(
        ledger.accept_job("job-1", "dc-east", worker_count=1, durability=REGIONAL)
    )
    cancel = asyncio.create_task(
        ledger.request_cancellation("job-1", "operator", "client-1", durability=REGIONAL)
    )
    await asyncio.sleep(0.01)

    accept.cancel()
    await asyncio.sleep(0.01)
    assert replicator.calls == [("job-1", JobEventType.JOB_CREATED)]

    replicator.release("job-1")
    await create
    await cancel
    with pytest.raises(asyncio.CancelledError):
        await accept

    assert replicator.calls == [
        ("job-1", JobEventType.JOB_CREATED),
        ("job-1", JobEventType.JOB_CANCELLATION_REQUESTED),
    ]
    assert ledger._wal.get_pending_entries() == []
    await ledger.close()


@pytest.mark.asyncio
async def test_checkpoint_mid_replication_cannot_compact_the_entry() -> None:
    replicator = GatedReplicator()
    ledger = await _open(replicator)
    replicator.hold("job-1")
    create = asyncio.create_task(_create(ledger, "job-1"))
    await _until(lambda: len(replicator.calls) == 1)

    await ledger.checkpoint()
    replicator.release("job-1")
    _, result = await create

    assert result.error is None
    assert result.level_achieved == REGIONAL
    await ledger.close()


@pytest.mark.asyncio
async def test_cancelled_commit_still_marks_its_entry_applied() -> None:
    replicator = GatedReplicator()
    ledger = await _open(replicator)
    replicator.hold("job-1")
    create = asyncio.create_task(_create(ledger, "job-1"))
    await _until(lambda: len(replicator.calls) == 1)

    create.cancel()
    with pytest.raises(asyncio.CancelledError):
        await create

    assert ledger._wal.get_pending_entries() == []
    assert ledger.get_job("job-1") is not None
    await ledger.close()


@pytest.mark.asyncio
async def test_sequencer_state_drains_after_many_jobs() -> None:
    replicator = GatedReplicator()
    ledger = await _open(replicator)

    await asyncio.gather(*(_create(ledger, f"job-{index}") for index in range(200)))

    assert ledger._commit_sequencer.in_flight_job_count == 0
    assert ledger._wal.get_pending_entries() == []
    await ledger.close()


@pytest.mark.asyncio
async def test_sequencer_runs_turns_in_reservation_order_under_random_delays() -> None:
    sequencer = JobCommitSequencer()
    started: list[int] = []

    async def commit(index: int, delay: float) -> int:
        started.append(index)
        await asyncio.sleep(delay)
        return index

    turns = [sequencer.reserve("job-1") for _ in range(50)]
    delays = [((index * 7919) % 13) / 1000 for index in range(50)]
    runs = [
        sequencer.run("job-1", predecessor, turn, lambda index=index: commit(index, delays[index]))
        for index, (predecessor, turn) in enumerate(turns)
    ]
    # Started newest-first: only the turn chain can put them back in order.
    results = await asyncio.gather(*reversed(runs))

    assert started == list(range(50))
    assert results == list(reversed(range(50)))
    assert sequencer.in_flight_job_count == 0
