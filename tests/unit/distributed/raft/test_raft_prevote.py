"""
RaftNode PreVote (Raft thesis 9.6) and leader stickiness (4.2.3).

An election timeout no longer bumps the term directly: the node first
asks, statelessly, whether a majority would vote for it. The rules
pinned here: a PreVote round changes no state on either side; members
that heard from a live leader within the minimum election timeout deny
both PreVotes and real votes (and do not adopt the higher term); a stale
or behind candidate is denied; a majority of grants starts the real
election; a higher term in a PreVote answer abandons the round.
"""

import pytest

from hyperscale.distributed.raft.models import (
    AppendEntries,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import ELECTION_TIMEOUT_MAX
from tests.unit.distributed.raft.test_raft_node import make_node, make_single_node


def _pre_vote(term: int, last_log_index: int = 0, last_log_term: int = 0) -> RequestVote:
    return RequestVote(
        job_id="job-1",
        term=term,
        candidate_id="node-2",
        last_log_index=last_log_index,
        last_log_term=last_log_term,
        pre_vote=True,
    )


def _heartbeat(term: int, leader_id: str = "node-3") -> AppendEntries:
    return AppendEntries(
        job_id="job-1",
        term=term,
        leader_id=leader_id,
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )


async def _time_out(node) -> None:
    node._election_deadline = 0.0
    await node.tick()


@pytest.mark.asyncio
async def test_timeout_starts_a_pre_vote_round_without_bumping_the_term() -> None:
    node, send_mock, _ = make_node()

    await _time_out(node)

    assert node.current_term == 0
    assert node.role == "follower"
    sent = [call.args[1] for call in send_mock.await_args_list]
    assert sent and all(request.pre_vote and request.term == 1 for request in sent)


@pytest.mark.asyncio
async def test_granting_a_pre_vote_changes_no_receiver_state() -> None:
    node, _, _ = make_node()

    response = await node.handle_request_vote(_pre_vote(term=1))

    assert response.vote_granted is True
    assert response.pre_vote is True
    assert node.current_term == 0
    assert node._voted_for is None


@pytest.mark.asyncio
async def test_member_hearing_a_live_leader_denies_pre_votes_and_votes() -> None:
    node, _, _ = make_node()
    await node.handle_append_entries(_heartbeat(term=3))

    pre_vote_response = await node.handle_request_vote(_pre_vote(term=4))
    vote_response = await node.handle_request_vote(
        RequestVote(job_id="job-1", term=4, candidate_id="node-2", last_log_index=0, last_log_term=0)
    )

    assert pre_vote_response.vote_granted is False
    assert vote_response.vote_granted is False
    assert node.current_term == 3
    assert node.current_leader == "node-3"


@pytest.mark.asyncio
async def test_member_grants_once_the_leader_has_been_silent() -> None:
    node, _, _ = make_node()
    await node.handle_append_entries(_heartbeat(term=3))
    node._last_leader_contact -= ELECTION_TIMEOUT_MAX

    response = await node.handle_request_vote(_pre_vote(term=4))

    assert response.vote_granted is True
    assert node.current_term == 3


@pytest.mark.asyncio
async def test_pre_vote_for_a_term_not_ahead_is_denied() -> None:
    node, _, _ = make_node()
    await node.handle_append_entries(_heartbeat(term=3))
    node._last_leader_contact -= ELECTION_TIMEOUT_MAX

    response = await node.handle_request_vote(_pre_vote(term=3))

    assert response.vote_granted is False


@pytest.mark.asyncio
async def test_majority_of_pre_votes_starts_the_real_election() -> None:
    node, send_mock, _ = make_node()
    await _time_out(node)
    send_mock.reset_mock()

    await node.handle_request_vote_response(
        RequestVoteResponse(job_id="job-1", term=0, vote_granted=True, voter_id="node-2", pre_vote=True)
    )

    assert node.role == "candidate"
    assert node.current_term == 1
    sent = [call.args[1] for call in send_mock.await_args_list]
    assert sent and all(not request.pre_vote and request.term == 1 for request in sent)


@pytest.mark.asyncio
async def test_higher_term_in_a_pre_vote_answer_abandons_the_round() -> None:
    node, _, _ = make_node()
    await _time_out(node)

    await node.handle_request_vote_response(
        RequestVoteResponse(job_id="job-1", term=7, vote_granted=False, voter_id="node-2", pre_vote=True)
    )
    await node.handle_request_vote_response(
        RequestVoteResponse(job_id="job-1", term=7, vote_granted=True, voter_id="node-3", pre_vote=True)
    )

    assert node.current_term == 7
    assert node.role == "follower"
    assert node._pre_vote_term is None


@pytest.mark.asyncio
async def test_single_member_group_still_elects_itself_on_timeout() -> None:
    node, _, _ = make_single_node()

    await _time_out(node)

    assert node.is_leader()
    assert node.current_term == 1
