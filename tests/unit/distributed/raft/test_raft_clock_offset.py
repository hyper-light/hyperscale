"""
AD-39 at the Raft boundary: a leader's entries carry its HLC, and a
follower refuses -- and does not adopt -- entries timestamped beyond the
offset bound ahead of its own physical clock.

* A batch with any entry beyond the bound is refused whole: nothing is
  appended, the follower's clock is untouched, the refusal is reported
  (logged, and flagged on the response).
* An entry exactly at the bound is accepted and merged into the
  follower's clock.
* The leader treats the refusal as a clock disagreement, not a log
  conflict: its view of the follower's log stays put (a plain failure,
  by contrast, backtracks it).
"""

from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.env import Env
from hyperscale.distributed.hlc import HLCTimestamp, HybridLogicalClock
from hyperscale.distributed.raft.logging_models import RaftWarning
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RaftLogEntry,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import RaftNode
from tests.unit.distributed.hlc.settable_clock import SettableClock

MAX_OFFSET_MS = Env().HLC_MAX_CLOCK_OFFSET_MS
FOLLOWER_PHYSICAL_MS = 1_790_000_000_000
MEMBERS = {"node-1", "node-2", "node-3"}
MEMBER_ADDRS = {member: ("127.0.0.1", 9000 + position) for position, member in enumerate(sorted(MEMBERS), 1)}


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list = []

    async def log(self, entry) -> None:
        self.entries.append(entry)


def make_node(node_id: str, physical_ms: int) -> tuple[RaftNode, HybridLogicalClock, AsyncMock, RecordingLogger]:
    clock = HybridLogicalClock(node_id=1, clock=SettableClock(physical_ms), max_offset_ms=MAX_OFFSET_MS)
    send_message = AsyncMock()
    logger = RecordingLogger()
    node = RaftNode(
        job_id="job-1",
        node_id=node_id,
        initial_voters=frozenset(MEMBERS),
        member_addrs=MEMBER_ADDRS,
        send_message=send_message,
        apply_command=AsyncMock(),
        on_become_leader=None,
        on_lose_leadership=None,
        logger=logger,
        configured_cluster_size=len(MEMBERS),
        clock=clock,
        may_lead=lambda: True, storage=VolatileRaftStorage()
    )
    return node, clock, send_message, logger


def make_entry(index: int, wall_ms: int, term: int = 1) -> RaftLogEntry:
    return RaftLogEntry(
        term=term,
        index=index,
        command=b"command",
        command_type="NO_OP",
        job_id="job-1",
        hlc=HLCTimestamp(wall_ms=wall_ms, logical=0, node_id=2),
    )


def append_request(entries: list[RaftLogEntry], term: int = 1, prev_log_index: int = 0) -> AppendEntries:
    return AppendEntries(
        job_id="job-1",
        term=term,
        leader_id="node-2",
        prev_log_index=prev_log_index,
        prev_log_term=term if prev_log_index else 0,
        entries=entries,
        leader_commit=0,
    )


@pytest.mark.asyncio
async def test_follower_refuses_a_batch_with_any_entry_beyond_the_offset_bound() -> None:
    follower, clock, _send, logger = make_node("node-1", FOLLOWER_PHYSICAL_MS)
    clock_before = clock.current
    beyond_bound = FOLLOWER_PHYSICAL_MS + MAX_OFFSET_MS + 1

    response = await follower.handle_append_entries(
        append_request([make_entry(1, FOLLOWER_PHYSICAL_MS), make_entry(2, beyond_bound)])
    )

    assert (response.success, response.clock_offset_rejected) == (False, True)
    assert follower.last_index_where(lambda entry: True) is None
    assert clock.current == clock_before
    assert [type(entry) for entry in logger.entries] == [RaftWarning]


@pytest.mark.asyncio
async def test_follower_accepts_and_merges_an_entry_at_the_offset_bound() -> None:
    follower, clock, _send, _logger = make_node("node-1", FOLLOWER_PHYSICAL_MS)
    at_bound = FOLLOWER_PHYSICAL_MS + MAX_OFFSET_MS

    response = await follower.handle_append_entries(append_request([make_entry(1, at_bound)]))

    assert (response.success, response.clock_offset_rejected, response.match_index) == (True, False, 1)
    assert clock.current.wall_ms == at_bound
    assert clock.now() > HLCTimestamp(wall_ms=at_bound, logical=0, node_id=2)


async def _leader_with_two_entries() -> tuple[RaftNode, AsyncMock]:
    """node-1 holds entries 1-2 from a term-1 leader, then wins term 2:
    its next index for every follower is 3."""
    leader, _clock, send_message, _logger = make_node("node-1", FOLLOWER_PHYSICAL_MS)
    await leader.handle_append_entries(
        append_request([make_entry(1, FOLLOWER_PHYSICAL_MS), make_entry(2, FOLLOWER_PHYSICAL_MS)])
    )
    await leader.start_election()
    await leader.handle_request_vote_response(
        RequestVoteResponse(job_id="job-1", term=leader.current_term, vote_granted=True, voter_id="node-2")
    )
    assert leader.role == "leader"
    return leader, send_message


async def _prev_log_index_sent_to_node_2(leader: RaftNode, send_message: AsyncMock) -> int:
    send_message.reset_mock()
    await leader.replicate_to_followers()
    (request,) = [
        call.args[1]
        for call in send_message.await_args_list
        if call.args[0] == MEMBER_ADDRS["node-2"] and isinstance(call.args[1], AppendEntries)
    ]
    return request.prev_log_index


@pytest.mark.asyncio
async def test_leader_keeps_its_follower_position_on_a_clock_offset_refusal() -> None:
    leader, send_message = await _leader_with_two_entries()
    assert await _prev_log_index_sent_to_node_2(leader, send_message) == 2

    await leader.handle_append_entries_response(
        AppendEntriesResponse(
            job_id="job-1",
            term=leader.current_term,
            success=False,
            follower_id="node-2",
            clock_offset_rejected=True,
        )
    )

    assert await _prev_log_index_sent_to_node_2(leader, send_message) == 2


@pytest.mark.asyncio
async def test_leader_backtracks_its_follower_position_on_a_log_conflict() -> None:
    leader, send_message = await _leader_with_two_entries()

    await leader.handle_append_entries_response(
        AppendEntriesResponse(job_id="job-1", term=leader.current_term, success=False, follower_id="node-2")
    )

    assert await _prev_log_index_sent_to_node_2(leader, send_message) == 1
