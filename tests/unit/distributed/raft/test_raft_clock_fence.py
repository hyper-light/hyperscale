"""
RaftNode under an AD-39 clock fence (``may_lead`` false).

A fenced node never mints timestamps for others to apply:

* it does not campaign -- not on an election timeout, not through an
  explicit ``start_election``, not when a PreVote majority it asked for
  before the fence arrives after it;
* as leader it relinquishes on its next tick, keeping the term and the
  vote it cast in it, and fails its pending proposals; a proposal made
  before that tick is refused;
* it still votes and follows, so the healthy members keep their quorum.
"""

import asyncio
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.raft.models import (
    AppendEntries,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import RaftNode
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

MEMBERS = {"node-1", "node-2", "node-3"}
MEMBER_ADDRS = {member: ("127.0.0.1", 9000 + position) for position, member in enumerate(sorted(MEMBERS), 1)}


class Fence:
    def __init__(self) -> None:
        self.fenced = False

    def may_lead(self) -> bool:
        return not self.fenced


def make_node() -> tuple[RaftNode, AsyncMock, Fence]:
    fence = Fence()
    send_message = AsyncMock()
    logger = AsyncMock()
    node = RaftNode(
        job_id="job-1",
        node_id="node-1",
        initial_voters=frozenset(MEMBERS),
        member_addrs=MEMBER_ADDRS,
        send_message=send_message,
        apply_command=AsyncMock(),
        on_become_leader=None,
        on_lose_leadership=None,
        logger=logger,
        configured_cluster_size=len(MEMBERS),
        clock=new_hybrid_logical_clock(),
        may_lead=fence.may_lead, storage=VolatileRaftStorage()
    )
    return node, send_message, fence


def sent_vote_requests(send_message: AsyncMock) -> list[RequestVote]:
    return [call.args[1] for call in send_message.await_args_list if isinstance(call.args[1], RequestVote)]


async def time_out(node: RaftNode) -> None:
    node._election_deadline = 0.0
    await node.tick()


async def elect(node: RaftNode) -> None:
    await node.start_election()
    await node.handle_request_vote_response(
        RequestVoteResponse(job_id="job-1", term=node.current_term, vote_granted=True, voter_id="node-2")
    )
    assert node.role == "leader"


def vote_request(term: int, candidate_id: str) -> RequestVote:
    return RequestVote(job_id="job-1", term=term, candidate_id=candidate_id, last_log_index=0, last_log_term=0)


@pytest.mark.asyncio
async def test_a_fenced_node_does_not_campaign_on_election_timeout() -> None:
    node, send_message, fence = make_node()
    fence.fenced = True

    await time_out(node)

    assert (node.role, node.current_term) == ("follower", 0)
    assert sent_vote_requests(send_message) == []


@pytest.mark.asyncio
async def test_a_fenced_node_ignores_an_explicit_election() -> None:
    node, send_message, fence = make_node()
    fence.fenced = True

    await node.start_election()

    assert (node.role, node.current_term) == ("follower", 0)
    assert sent_vote_requests(send_message) == []


@pytest.mark.asyncio
async def test_a_pre_vote_majority_arriving_after_the_fence_starts_no_election() -> None:
    node, send_message, fence = make_node()
    await time_out(node)
    assert all(request.pre_vote for request in sent_vote_requests(send_message))
    fence.fenced = True

    # A grant carries the voter's own (unchanged) term.
    await node.handle_request_vote_response(
        RequestVoteResponse(job_id="job-1", term=0, vote_granted=True, voter_id="node-2", pre_vote=True)
    )

    assert (node.role, node.current_term) == ("follower", 0)


@pytest.mark.asyncio
async def test_a_fenced_leader_relinquishes_on_its_next_tick_keeping_term_and_vote() -> None:
    node, _send, fence = make_node()
    await elect(node)
    term = node.current_term
    pending_proposal = asyncio.ensure_future(node.propose(b"command", "NO_OP"))
    await asyncio.sleep(0)
    fence.fenced = True

    await node.tick()

    assert (node.role, node.current_term) == ("follower", term)
    # Index 2: the term opened with its blank entry at index 1 (Raft 8).
    assert await pending_proposal == (False, 2)
    # It voted for itself in this term: no other candidate gets its vote.
    response = await node.handle_request_vote(vote_request(term, "node-3"))
    assert response.vote_granted is False


@pytest.mark.asyncio
async def test_a_fenced_leader_refuses_proposals_before_its_next_tick() -> None:
    node, _send, fence = make_node()
    await elect(node)
    fence.fenced = True

    assert await node.propose(b"command", "NO_OP") == (False, 0)


@pytest.mark.asyncio
async def test_a_fenced_node_still_votes_and_follows() -> None:
    node, _send, fence = make_node()
    fence.fenced = True

    response = await node.handle_request_vote(vote_request(1, "node-2"))
    assert response.vote_granted is True

    append = await node.handle_append_entries(
        AppendEntries(job_id="job-1", term=1, leader_id="node-2", prev_log_index=0, prev_log_term=0, entries=[], leader_commit=0)
    )
    assert append.success is True
    assert node.current_leader == "node-2"
