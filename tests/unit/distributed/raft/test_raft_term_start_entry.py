"""
A new leader applies what its predecessor committed, unprompted (Raft 8).

An entry of an earlier term commits only behind an entry of the current
term (Raft 5.4.2). A new leader appended nothing of its own until its
group next proposed, so an entry its predecessor committed -- held in a
majority's logs, but applied by the predecessor alone before it died --
stayed unapplied on every survivor. A job group's leader that died after
recording the job's end left the end unapplied: every survivor's ledger
replica read the job as running, and the takeover re-ran a job already over.
A leader now opens its term with a blank entry, which commits everything
before it.

Real ``RaftNode`` instances, every message delivered by hand:

1. A leads term 1 and commits "job ended" with B's acknowledgement alone;
   A dies before anyone learns that it committed.
2. B leads term 2 with C's vote and applies "job ended" -- so does C --
   with no proposal made in term 2.
"""

import asyncio
import contextvars
from collections.abc import Callable, Coroutine
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.raft.models import (
    RAFT_NO_OP_COMMAND,
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)
from hyperscale.distributed.raft.raft_node import ELECTION_TIMEOUT_MAX, RaftNode
from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from tests.simulation.harness.sim import SimulationLoop, VirtualClock
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

MemberAddress = tuple[str, int]
OutboundMessage = tuple[MemberAddress, MemberAddress, object]

MEMBER_IDS = ("A", "B", "C")
ADDRESSES = {member_id: ("127.0.0.1", 10_000 + slot) for slot, member_id in enumerate(MEMBER_IDS)}
JOB_ENDED = b"job-ended"


def make_member(member_id: str, outbox: list[OutboundMessage]) -> tuple[RaftNode, AsyncMock]:
    """A real RaftNode whose every send lands in ``outbox``."""
    address = ADDRESSES[member_id]

    async def send(destination: MemberAddress, message: object) -> None:
        outbox.append((address, destination, message))

    logger = MagicMock()
    logger.log = AsyncMock()
    apply_command = AsyncMock()
    node = RaftNode(
        job_id="term-start-entry",
        node_id=member_id,
        initial_voters=frozenset(MEMBER_IDS),
        member_addrs=dict(ADDRESSES),
        send_message=send,
        apply_command=apply_command,
        on_become_leader=None,
        on_lose_leadership=None,
        logger=logger,
        configured_cluster_size=len(MEMBER_IDS),
        clock=new_hybrid_logical_clock(),
        may_lead=lambda: True, storage=VolatileRaftStorage()
    )
    return node, apply_command


async def deliver(
    outbox: list[OutboundMessage],
    nodes_by_address: dict[MemberAddress, RaftNode],
    between: frozenset[MemberAddress],
) -> None:
    """Deliver every queued message between members of ``between``,
    replies included, until none is left; the rest are dropped."""
    while outbox:
        queued_messages = list(outbox)
        outbox.clear()
        for sender, destination, message in queued_messages:
            if sender not in between or destination not in between:
                continue
            receiver = nodes_by_address[destination]
            if isinstance(message, RequestVote):
                outbox.append((destination, sender, await receiver.handle_request_vote(message)))
            elif isinstance(message, RequestVoteResponse):
                await receiver.handle_request_vote_response(message)
            elif isinstance(message, AppendEntries):
                outbox.append((destination, sender, await receiver.handle_append_entries(message)))
            elif isinstance(message, AppendEntriesResponse):
                await receiver.handle_append_entries_response(message)


def applied_commands(apply_command: AsyncMock) -> list[bytes]:
    return [call.args[0].command for call in apply_command.await_args_list]


def on_virtual_time(scenario: Callable[[], Coroutine[Any, Any, None]]) -> None:
    """Run ``scenario`` on a fresh ``SimulationLoop`` with the process
    clock on its virtual time, in a context of its own."""
    defaults = snapshot_defaults()
    loop = SimulationLoop()
    swap_defaults(clock=VirtualClock(loop))
    try:
        contextvars.copy_context().run(loop.run_until_complete, scenario())
    finally:
        restore_defaults(defaults)
        loop.close()


def test_a_new_leader_applies_what_its_dead_predecessor_committed() -> None:
    on_virtual_time(dead_leader_scenario)


async def dead_leader_scenario() -> None:
    outbox: list[OutboundMessage] = []
    members = {member_id: make_member(member_id, outbox) for member_id in MEMBER_IDS}
    nodes_by_address = {ADDRESSES[member_id]: node for member_id, (node, _) in members.items()}
    node_a, apply_a = members["A"]
    node_b, apply_b = members["B"]
    node_c, apply_c = members["C"]
    everyone = frozenset(ADDRESSES.values())

    await node_a.start_election()
    await deliver(outbox, nodes_by_address, everyone)
    assert node_a.is_leader()
    await node_a.replicate_to_followers()
    await deliver(outbox, nodes_by_address, everyone)

    # "job ended" commits on B's acknowledgement; A applies it and dies
    # before telling anyone it committed.
    pending_proposal = asyncio.ensure_future(node_a.propose(JOB_ENDED, "LEDGER_APPEND"))
    await asyncio.sleep(0)
    await deliver(outbox, nodes_by_address, frozenset({ADDRESSES["A"], ADDRESSES["B"]}))
    assert (await pending_proposal)[0] is True
    assert applied_commands(apply_a) == [JOB_ENDED]
    assert JOB_ENDED not in applied_commands(apply_b) + applied_commands(apply_c)
    outbox.clear()

    # A is gone. Its followers' lease of it lapses before B campaigns.
    await asyncio.sleep(ELECTION_TIMEOUT_MAX)
    survivors = frozenset({ADDRESSES["B"], ADDRESSES["C"]})
    await node_b.start_election()
    await deliver(outbox, nodes_by_address, survivors)
    assert node_b.is_leader() and node_b.current_term == 2
    # Three heartbeats at the coordinators' cadence -- replicate if
    # leading, then apply: the first repairs C's log (it lacks "job
    # ended", so B backs up), the second commits through B's blank entry,
    # the third carries the commit to C.
    for _ in range(3):
        await node_b.replicate_to_followers()
        await deliver(outbox, nodes_by_address, survivors)
        await node_b.apply_committed_entries()
        await node_c.apply_committed_entries()

    # Applied on both survivors with nothing proposed in term 2: the term's
    # blank entry committed it, and never reached the state machine.
    assert applied_commands(apply_b) == [JOB_ENDED]
    assert applied_commands(apply_c) == [JOB_ENDED]
    assert node_b._log.get(node_b.commit_index).command_type == RAFT_NO_OP_COMMAND
