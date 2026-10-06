"""
Every per-job Raft group applies one thing: committed job-ledger entries,
mirrored into the member's ``JobLedgerReplica`` (AD-38).

* Two members applying the same committed entries hold the same state and
  history -- the entry is all they apply (AD-52 section 15).
* An entry no member can apply -- a command type it does not know, bytes
  that do not decode, an event the replica refuses -- is logged and
  skipped, never raised: the log is immutable, and a raise would stop the
  tick loop that drives every job's group.
* Commands travel as msgspec, never pickle: an entry read back from a
  peer or a disk (D1) is decoded as data.
"""

from __future__ import annotations

import msgspec
import pytest

from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.events.job_event import JobCompleted, JobCreated, JobProgressReported
from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
from hyperscale.distributed.raft.ledger_state_machine import LedgerStateMachine
from hyperscale.distributed.raft.models import RaftLogEntry
from hyperscale.distributed.raft.models.ledger_append_command import LEDGER_APPEND_COMMAND, LedgerAppendCommand
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

JOB_ID = "dc-east-1-manager-1-1"


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


def committed_entries() -> list[RaftLogEntry]:
    clock = new_hybrid_logical_clock()
    events = [
        (
            JobEventType.JOB_CREATED,
            JobCreated(
                job_id=JOB_ID,
                hlc=clock.now(),
                fence_token=1,
                spec_hash=b"spec",
                assigned_datacenters=("dc-east",),
                requestor_id="client-1",
            ).to_bytes(),
        ),
        (
            JobEventType.JOB_PROGRESS_REPORTED,
            JobProgressReported(
                job_id=JOB_ID, hlc=clock.now(), fence_token=1, datacenter_id="dc-east", completed_count=3, failed_count=1
            ).to_bytes(),
        ),
        (
            JobEventType.JOB_COMPLETED,
            JobCompleted(
                job_id=JOB_ID,
                hlc=clock.now(),
                fence_token=1,
                final_status="completed",
                total_completed=10,
                total_failed=1,
                duration_ms=1500,
            ).to_bytes(),
        ),
    ]
    return [
        RaftLogEntry(
            term=1,
            index=index,
            command=msgspec.msgpack.encode(
                LedgerAppendCommand(job_id=JOB_ID, ledger_event_type=event_type, ledger_payload=payload)
            ),
            command_type=LEDGER_APPEND_COMMAND,
            job_id=JOB_ID,
            hlc=clock.now(),
        )
        for index, (event_type, payload) in enumerate(events, start=1)
    ]


async def replay(entries: list[RaftLogEntry]) -> tuple[JobLedgerReplica, RecordingLogger]:
    replica = JobLedgerReplica()
    logger = RecordingLogger()
    state_machine = LedgerStateMachine(replica, logger, "member-1")
    for entry in entries:
        await state_machine.apply(entry)
    return replica, logger


@pytest.mark.asyncio
async def test_members_applying_the_same_entries_hold_the_same_state() -> None:
    entries = committed_entries()

    first, first_logger = await replay(entries)
    second, _second_logger = await replay(entries)

    assert first.history(JOB_ID) == second.history(JOB_ID) and len(first.history(JOB_ID)) == 3
    assert msgspec.msgpack.encode(first.job_state(JOB_ID).to_dict()) == msgspec.msgpack.encode(
        second.job_state(JOB_ID).to_dict()
    )
    assert first_logger.entries == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("damage", "logged"),
    [
        (lambda entry: RaftLogEntry(entry.term, entry.index, entry.command, "update_job_status", entry.job_id, entry.hlc), "RaftWarning"),
        (lambda entry: RaftLogEntry(entry.term, entry.index, b"\xc1not-msgpack", entry.command_type, entry.job_id, entry.hlc), "RaftError"),
        (
            lambda entry: RaftLogEntry(
                entry.term,
                entry.index,
                msgspec.msgpack.encode(LedgerAppendCommand(JOB_ID, JobEventType.JOB_CREATED, b"not an event")),
                entry.command_type,
                entry.job_id,
                entry.hlc,
            ),
            "RaftError",
        ),
    ],
)
async def test_an_entry_no_member_can_apply_is_logged_and_skipped(damage, logged: str) -> None:
    entries = committed_entries()
    entries[1] = damage(entries[1])

    replica, logger = await replay(entries)

    assert [type(entry).__name__ for entry in logger.entries] == [logged]
    # The rest still applied, in order.
    assert [event_type for event_type, _payload in replica.history(JOB_ID)] == [
        JobEventType.JOB_CREATED,
        JobEventType.JOB_COMPLETED,
    ]


@pytest.mark.asyncio
async def test_releasing_a_job_drops_its_replicated_state() -> None:
    replica, _logger = await replay(committed_entries())
    LedgerStateMachine(replica, RecordingLogger(), "member-1").release_job(JOB_ID)

    assert replica.job_state(JOB_ID) is None and replica.history(JOB_ID) == ()
