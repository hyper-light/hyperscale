"""
Raft membership safety under the two adversarial interleavings AD-52's
joint consensus exists for -- each built step by step on real
``RaftNode`` instances, every message delivered by hand.

* RESTART DOUBLE VOTE: a member's id is its process incarnation's, so a
  restarted member is a NEW member. C votes for A in term 1 and A leads;
  C restarts as C' at C's address, and candidate B's vote request for the
  same term -- addressed to C -- reaches C', which grants it. Counting C'
  would make B a second leader of term 1 (B, E and C' against A, C and D).
  Only the configuration's voters count, so B does not lead.
* JOINT CHANGE BEHIND A PARTITION: A leads {A, B, C} and moves the group
  to {A, D, E} while B and C are cut off. The new voters alone are a
  majority of the new configuration, but a change of voters must also
  win the old voters' quorum: nothing from the change on commits, so B and
  C -- who may elect a leader of the old configuration meanwhile -- can
  never lose a committed entry.
"""

import asyncio
from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import RaftNode
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

MemberAddress = tuple[str, int]
OutboundMessage = tuple[MemberAddress, MemberAddress, object]


def _address(slot: int) -> MemberAddress:
    return ("127.0.0.1", 10_000 + slot)


def _member(
    member_id: str,
    address: MemberAddress,
    initial_voters: frozenset[str],
    member_addresses: dict[str, MemberAddress],
    outbox: list[OutboundMessage],
) -> RaftNode:
    """A real RaftNode whose every send lands in ``outbox``."""

    async def send(destination: MemberAddress, message: object) -> None:
        outbox.append((address, destination, message))

    logger = MagicMock()
    logger.log = AsyncMock()
    return RaftNode(
        job_id="membership-safety",
        node_id=member_id,
        initial_voters=initial_voters,
        member_addrs=dict(member_addresses),
        send_message=send,
        apply_command=AsyncMock(),
        on_become_leader=None,
        on_lose_leadership=None,
        logger=logger,
        configured_cluster_size=len(initial_voters),
        clock=new_hybrid_logical_clock(),
        may_lead=lambda: True, storage=VolatileRaftStorage()
    )


async def _deliver(
    outbox: list[OutboundMessage],
    nodes_by_address: dict[MemberAddress, RaftNode],
    admits: Callable[[MemberAddress, MemberAddress, object], bool],
) -> None:
    """Deliver every queued message ``admits``, replies included, until
    none it admits is left; the rest stay queued."""
    while admissible := [queued for queued in outbox if admits(*queued)]:
        for queued in admissible:
            outbox.remove(queued)
        for sender, destination, message in admissible:
            receiver = nodes_by_address[destination]
            if isinstance(message, RequestVote):
                outbox.append((destination, sender, await receiver.handle_request_vote(message)))
            elif isinstance(message, RequestVoteResponse):
                await receiver.handle_request_vote_response(message)
            elif isinstance(message, AppendEntries):
                outbox.append((destination, sender, await receiver.handle_append_entries(message)))
            elif isinstance(message, AppendEntriesResponse):
                await receiver.handle_append_entries_response(message)


@pytest.mark.asyncio
async def test_a_restarted_members_new_incarnation_cannot_elect_a_second_leader() -> None:
    member_ids = ["A", "B", "C", "D", "E"]
    addresses = {member_id: _address(slot) for slot, member_id in enumerate(member_ids)}
    voters = frozenset(member_ids)
    member_addresses = dict(addresses)
    outbox: list[OutboundMessage] = []
    nodes = {
        member_id: _member(member_id, addresses[member_id], voters, member_addresses, outbox)
        for member_id in member_ids
    }
    nodes_by_address = {addresses[member_id]: node for member_id, node in nodes.items()}

    # A and B campaign for term 1 at once.
    await nodes["A"].start_election()
    await nodes["B"].start_election()
    assert (nodes["A"].current_term, nodes["B"].current_term) == (1, 1)

    # A's requests reach C and D first: A leads term 1.
    await _deliver(
        outbox,
        nodes_by_address,
        lambda sender, destination, message: (
            (sender == addresses["A"] and destination in (addresses["C"], addresses["D"]))
            or (destination == addresses["A"] and sender in (addresses["C"], addresses["D"]))
        ),
    )
    assert nodes["A"].is_leader()

    # C crashes and restarts at its address as a NEW member, C'.
    member_addresses["C'"] = addresses["C"]
    restarted = _member("C'", addresses["C"], voters, member_addresses, outbox)
    nodes_by_address[addresses["C"]] = restarted

    # B's requests for term 1 -- one addressed to C, which C' now answers --
    # reach C' and E; both grant.
    await _deliver(
        outbox,
        nodes_by_address,
        lambda sender, destination, message: (
            (sender == addresses["B"] and destination in (addresses["C"], addresses["E"]))
            or (destination == addresses["B"] and sender in (addresses["C"], addresses["E"]))
        ),
    )

    # C' is no voter: B holds two voters' votes of the three it needs.
    assert restarted.current_term == 1
    assert not nodes["B"].is_leader()
    assert [member_id for member_id, node in nodes.items() if node.is_leader()] == ["A"]


@pytest.mark.asyncio
async def test_a_change_of_voters_needs_the_old_voters_quorum_too() -> None:
    old_voters = frozenset({"A", "B", "C"})
    member_ids = ["A", "B", "C", "D", "E"]
    addresses = {member_id: _address(slot) for slot, member_id in enumerate(member_ids)}
    outbox: list[OutboundMessage] = []
    # Every member was created with the group's agreed voters: D and E
    # join through the log.
    nodes = {
        member_id: _member(member_id, addresses[member_id], old_voters, addresses, outbox)
        for member_id in member_ids
    }
    nodes_by_address = {addresses[member_id]: node for member_id, node in nodes.items()}

    def everywhere(sender: MemberAddress, destination: MemberAddress, message: object) -> bool:
        return True

    await nodes["A"].start_election()
    await _deliver(outbox, nodes_by_address, everywhere)
    assert nodes["A"].is_leader()

    # B and C are cut off from now on; A, D and E still reach each other.
    reachable = {addresses["A"], addresses["D"], addresses["E"]}

    def across_the_partition(sender: MemberAddress, destination: MemberAddress, message: object) -> bool:
        return sender in reachable and destination in reachable

    assert await nodes["A"].change_membership(frozenset({"A", "D", "E"}), frozenset())
    joint_index = nodes["A"]._log.last_index()
    assert nodes["A"].configuration.is_joint
    proposal = asyncio.create_task(nodes["A"].propose(b"after-the-change", "MEMBERSHIP_SAFETY"))
    try:
        for _heartbeat in range(5):
            await nodes["A"].replicate_to_followers()
            await _deliver(outbox, nodes_by_address, across_the_partition)
            await asyncio.sleep(0)

        # D and E hold every entry, yet nothing from the change on commits:
        # the old voters' quorum (two of A, B, C) is out of reach.
        assert nodes["D"]._log.last_index() == nodes["A"]._log.last_index()
        assert nodes["A"].commit_index < joint_index
        assert nodes["A"].configuration.is_joint
        assert not proposal.done()
    finally:
        proposal.cancel()
        try:
            await proposal
        except asyncio.CancelledError:
            pass
