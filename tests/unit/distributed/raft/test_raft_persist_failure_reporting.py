"""
A Raft state write that does not complete stops the group (fail-stop) and
re-raises; how it is reported depends on why it did not complete.

A cancelled write -- every clean shutdown cancels the write in flight --
is no error: it is reported below ERROR and named as a cancellation. A
write that fails (an ``OSError`` from the disk) is an error, reported at
ERROR with the exception's type and message.

Each test drives a real ``RaftNode`` whose durable storage runs a scripted
write, and records every entry the node hands its logger.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.raft.raft_node import RaftNode
from hyperscale.distributed.raft.store.raft_store_codec import RaftStoreRecord
from hyperscale.logging.models import Entry, LogLevel
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

from .scripted_durable_raft_storage import ScriptedDurableRaftStorage


def make_solo_node(storage: ScriptedDurableRaftStorage) -> tuple[RaftNode, MagicMock]:
    """A single-voter node over ``storage``, and the logger it reports to."""
    logger = MagicMock()
    logger.log = AsyncMock()
    node = RaftNode(
        job_id="job-1",
        node_id="solo",
        initial_voters=frozenset({"solo"}),
        member_addrs={"solo": ("127.0.0.1", 9001)},
        send_message=AsyncMock(),
        apply_command=AsyncMock(),
        on_become_leader=None,
        on_lose_leadership=None,
        logger=logger,
        configured_cluster_size=1,
        clock=new_hybrid_logical_clock(),
        may_lead=lambda: True,
        storage=storage,
    )
    return node, logger


def logged_entries(logger: MagicMock) -> list[Entry]:
    """Every entry the node handed its logger, in order."""
    return [logged_call.args[0] for logged_call in logger.log.await_args_list]


async def assert_group_stopped(node: RaftNode, storage: ScriptedDurableRaftStorage) -> None:
    """The member left the group: it neither campaigns nor writes again,
    and takes no proposals."""
    attempts_before = len(storage.write_attempts)
    await node.start_election()
    assert node.current_term == 1
    assert len(storage.write_attempts) == attempts_before
    accepted, _ = await node.propose(b"after-the-failure", "NO_OP")
    assert accepted is False


@pytest.mark.asyncio
async def test_cancelled_write_stops_the_group_and_logs_no_error() -> None:
    write_started = asyncio.Event()
    write_never_finishes = asyncio.Event()

    async def write_until_cancelled(records: list[RaftStoreRecord]) -> None:
        # Only the first write hangs: a member that wrongly kept going
        # writes again, and that write returns so the test fails rather
        # than hangs.
        if write_started.is_set():
            return
        write_started.set()
        await write_never_finishes.wait()

    storage = ScriptedDurableRaftStorage(write_until_cancelled)
    node, logger = make_solo_node(storage)

    election = asyncio.create_task(node.start_election())
    await write_started.wait()
    election.cancel()
    with pytest.raises(asyncio.CancelledError):
        await election

    entries = logged_entries(logger)
    assert [entry for entry in entries if entry.level in (LogLevel.ERROR, LogLevel.CRITICAL)] == []
    cancellation_reports = [entry for entry in entries if "cancelled" in entry.message]
    assert len(cancellation_reports) == 1
    assert "left the group: CancelledError()" in cancellation_reports[0].message
    await assert_group_stopped(node, storage)


@pytest.mark.asyncio
async def test_failed_write_stops_the_group_and_logs_the_error_with_its_type() -> None:
    async def write_into_a_full_disk(records: list[RaftStoreRecord]) -> None:
        raise OSError(28, "No space left on device")

    storage = ScriptedDurableRaftStorage(write_into_a_full_disk)
    node, logger = make_solo_node(storage)

    with pytest.raises(OSError, match="No space left on device"):
        await node.start_election()

    error_reports = [entry for entry in logged_entries(logger) if entry.level == LogLevel.ERROR]
    assert len(error_reports) == 1
    assert "left the group" in error_reports[0].message
    assert "left the group: OSError(28, 'No space left on device')" in error_reports[0].message
    await assert_group_stopped(node, storage)


@pytest.mark.asyncio
async def test_failed_write_without_a_message_still_names_its_type() -> None:
    async def write_failing_silently(records: list[RaftStoreRecord]) -> None:
        raise OSError()

    storage = ScriptedDurableRaftStorage(write_failing_silently)
    node, logger = make_solo_node(storage)

    with pytest.raises(OSError):
        await node.start_election()

    error_reports = [entry for entry in logged_entries(logger) if entry.level == LogLevel.ERROR]
    assert len(error_reports) == 1
    assert error_reports[0].message.endswith("left the group: OSError()")
    await assert_group_stopped(node, storage)
