"""
AD-38 takeover of a job's ledger record: JobLedgerReplica (what a group
member mirrors) and JobLedger.adopt_replicated_history (what the member
taking the job over writes into its own ledger).

Pinned against real ledgers on SimFilesystem: the replica applies events
through the recovery applier in commit order and releases a job whole;
an unreplayable event is loud; adoption makes the job known to the new
leader's ledger so its later events land, survives a restart like any
other WAL history, is a no-op for a job the ledger already holds, and a
terminal history settles to the completed cache exactly like recovery.
"""

from __future__ import annotations

from pathlib import Path

import msgspec
import pytest

from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.job_ledger import JobLedger
from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
from hyperscale.distributed.ledger.wal.wal_entry import WALEntry
from tests.simulation.harness.sim import SimFilesystem


def _paths(node: str) -> dict:
    return {
        "wal_path": Path(f"/{node}/ledger/wal"),
        "checkpoint_dir": Path(f"/{node}/ledger/checkpoints"),
        "archive_dir": Path(f"/{node}/ledger/archive"),
        "region_code": "dc-east",
        "gate_id": node,
        "node_id": 1,
    }


class CapturingReplicator:
    """Stands in for the job's Raft group: commits and keeps what it saw."""

    def __init__(self) -> None:
        self.entries: list[WALEntry] = []

    async def __call__(self, entry: WALEntry) -> bool:
        self.entries.append(entry)
        return True


async def _open(filesystem: SimFilesystem, node: str, replicator) -> JobLedger:
    return await JobLedger.open(
        filesystem=filesystem, regional_replicator=replicator, **_paths(node)
    )


async def _leader_history(*, terminal: bool) -> tuple[list[tuple[JobEventType, bytes]], CapturingReplicator]:
    """Drive a real leader ledger and return the events its group saw."""
    replicator = CapturingReplicator()
    leader = await _open(SimFilesystem(), "leader", replicator)
    await leader.create_job(
        spec_hash=b"spec",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        job_id="job-1",
        durability=DurabilityLevel.REGIONAL,
    )
    await leader.accept_job("job-1", "dc-east", worker_count=2, durability=DurabilityLevel.REGIONAL)
    if terminal:
        await leader.complete_job(
            "job-1",
            final_status="completed",
            total_completed=1,
            total_failed=0,
            duration_ms=10,
            durability=DurabilityLevel.REGIONAL,
        )
    await leader.close()
    return [(entry.event_type, entry.payload) for entry in replicator.entries], replicator


def _replica_of(history: list[tuple[JobEventType, bytes]]) -> JobLedgerReplica:
    replica = JobLedgerReplica()
    for event_type, payload in history:
        replica.apply(msgspec.msgpack.decode(payload)[0], event_type, payload)
    return replica


@pytest.mark.asyncio
async def test_replica_mirrors_events_in_order_and_releases_the_job_whole() -> None:
    history, _ = await _leader_history(terminal=False)

    replica = _replica_of(history)

    assert [event_type for event_type, _ in replica.history("job-1")] == [
        JobEventType.JOB_CREATED,
        JobEventType.JOB_ACCEPTED,
    ]
    assert replica.job_state("job-1").fence_token >= 1
    replica.release("job-1")
    assert replica.job_count == 0
    assert replica.job_state("job-1") is None
    assert replica.history("job-1") == ()


def test_replica_refuses_an_unreplayable_event_loudly() -> None:
    replica = JobLedgerReplica()

    with pytest.raises(Exception):
        replica.apply("job-1", JobEventType.JOB_CREATED, b"\xc1 not msgpack")

    assert replica.job_count == 0


@pytest.mark.asyncio
async def test_adopted_job_takes_the_new_leaders_later_events() -> None:
    history, _ = await _leader_history(terminal=False)
    successor = await _open(SimFilesystem(), "successor", CapturingReplicator())
    assert await successor.complete_job(
        "job-1", "completed", 1, 0, 10, durability=DurabilityLevel.REGIONAL
    ) is None  # unknown before adoption: the terminal would append nothing

    adopted = await successor.adopt_replicated_history("job-1", history)
    terminal_result = await successor.complete_job(
        "job-1", "completed", 1, 0, 10, durability=DurabilityLevel.REGIONAL
    )

    assert adopted == 2
    assert terminal_result is not None and terminal_result.success
    assert successor.get_job("job-1").is_terminal
    await successor.close()


@pytest.mark.asyncio
async def test_adoption_survives_a_restart_and_is_not_repeated() -> None:
    history, _ = await _leader_history(terminal=False)
    filesystem = SimFilesystem()
    successor = await _open(filesystem, "successor", CapturingReplicator())
    await successor.adopt_replicated_history("job-1", history)
    await successor.close()

    reopened = await _open(filesystem, "successor", CapturingReplicator())

    assert reopened.get_job("job-1") is not None
    assert await reopened.adopt_replicated_history("job-1", history) == 0
    await reopened.close()


@pytest.mark.asyncio
async def test_terminal_history_settles_like_recovery() -> None:
    history, _ = await _leader_history(terminal=True)
    successor = await _open(SimFilesystem(), "successor", CapturingReplicator())

    adopted = await successor.adopt_replicated_history("job-1", history)

    assert adopted == 3
    assert successor.active_job_count == 0
    assert successor.get_job("job-1").is_terminal
    assert await successor.adopt_replicated_history("job-1", history) == 0
    await successor.close()


@pytest.mark.asyncio
async def test_adopting_an_empty_history_writes_nothing() -> None:
    successor = await _open(SimFilesystem(), "successor", CapturingReplicator())

    assert await successor.adopt_replicated_history("job-1", ()) == 0
    assert successor.job_count == 0
    await successor.close()
