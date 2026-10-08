"""
AD-38 replication of a node's job ledger through the job's per-job Raft group.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Protocol
import msgspec
from hyperscale.distributed.ledger.events.event_type import JobEventType

from .logging_models import RaftDebug
from .models import LedgerProposal, LedgerProposalResult
from .models.ledger_append_command import LedgerAppendCommand
from .raft_node import HEARTBEAT_INTERVAL, RaftNode
from .job_group_consensus import JobGroupConsensus

if TYPE_CHECKING:
    from hyperscale.distributed.ledger.wal.wal_entry import WALEntry
    from hyperscale.distributed.runtime import Clock
    from hyperscale.logging import Logger


class LedgerReplicator:
    """Commits a job-ledger WAL entry in the job's per-job Raft group.

    A commit means a majority of the group's members hold the entry, and
    every member mirrors committed entries into its ``JobLedgerReplica``.
    For a manager that is AD-38 REGIONAL (the datacenter's managers); for
    a gate it is the gate tier's majority. Only the group's Raft leader
    can append, and that member need not be the job's leader (job
    leadership is SWIM-coordinated), so a non-leader forwards the entry
    to the group's leader over ``forward_method``.

    Retries until committed: an attempt can fail without an answer (no
    leader yet, leader lost, transport timeout after the leader appended),
    and job-ledger events are idempotent when re-applied (absolute
    tallies, set-valued acks, a single terminal), so a duplicate from an
    uncertain attempt is harmless -- and the ledger's per-job commit turns
    mean it can only follow its own original. The commit pipeline's
    REGIONAL timeout bounds the whole effort.
    """

    __slots__ = (
        "_consensus",
        "_node_id",
        "_send_tcp",
        "_forward_method",
        "_forward_timeout_seconds",
        "_clock",
        "_logger",
    )

    def __init__(
        self,
        consensus: JobGroupConsensus,
        node_id: str,
        send_tcp: Callable[..., Awaitable[bytes | Exception | None]],
        forward_method: str,
        forward_timeout_seconds: float,
        clock: "Clock",
        logger: "Logger",
    ) -> None:
        self._consensus = consensus
        self._node_id = node_id
        self._send_tcp = send_tcp
        self._forward_method = forward_method
        self._forward_timeout_seconds = forward_timeout_seconds
        self._clock = clock
        self._logger = logger

    async def replicate(self, entry: "WALEntry") -> bool:
        """``CommitPipeline`` regional replicator: True once committed."""
        job_id = msgspec.msgpack.decode(entry.payload)[0]
        if not await self._ensure_group(job_id):
            return False

        while (node := self._consensus.get_node(job_id)) is not None:
            leader_id = node.current_leader
            if leader_id is not None and await self._propose_via(leader_id, job_id, entry):
                return True
            await self._clock.sleep(HEARTBEAT_INTERVAL)

        # The group was destroyed (job cleaned up) before the entry landed.
        return False

    async def handle_forwarded(self, proposal: LedgerProposal) -> LedgerProposalResult:
        """Propose a forwarded entry if this member leads the job's group."""
        node = self._consensus.get_node(proposal.job_id)
        if node is None or not node.is_leader():
            return LedgerProposalResult(job_id=proposal.job_id)

        committed, index = await self._consensus.propose_command(
            proposal.job_id,
            LedgerAppendCommand(
                job_id=proposal.job_id,
                ledger_event_type=JobEventType(proposal.event_type),
                ledger_payload=proposal.payload,
            ),
        )
        return LedgerProposalResult(
            job_id=proposal.job_id,
            appended=index > 0,
            committed=committed,
        )

    async def _ensure_group(self, job_id: str) -> bool:
        """The job's group here, campaigning at once if it never elected.

        A job's group is founded by the job's creator, with the voters
        every member agrees on (AD-52), before the job's first ledger
        entry: a member without the group -- one it destroyed, or never
        joined -- cannot replicate the job's entries. Founding one here
        made a group no other member joined, so nothing it appended ever
        committed. Campaigning at once instead of waiting out a randomized
        election timeout makes the creator the likely first leader and
        saves that timeout on every job's first REGIONAL commit.
        """
        if (node := self._consensus.get_node(job_id)) is None:
            await self._log_attempt(job_id, "no Raft group for the job here")
            return False
        if node.current_term == 0:
            await node.start_election()
        return True

    async def _propose_via(self, leader_id: str, job_id: str, entry: "WALEntry") -> bool:
        if leader_id == self._node_id:
            committed, _ = await self._consensus.propose_command(
                job_id,
                LedgerAppendCommand(job_id=job_id, ledger_event_type=entry.event_type, ledger_payload=entry.payload),
            )
            return committed

        if (leader_addr := self._consensus.member_address(leader_id)) is None:
            return False

        response = await self._send_tcp(
            leader_addr,
            self._forward_method,
            LedgerProposal(
                job_id=job_id,
                event_type=int(entry.event_type),
                payload=entry.payload,
            ).dump(),
            timeout=self._forward_timeout_seconds,
        )
        if isinstance(response, Exception) or not response:
            await self._log_attempt(job_id, f"forward to {leader_id} got no answer ({response!r})")
            return False

        result = LedgerProposalResult.load(response)
        if not result.committed:
            await self._log_attempt(
                job_id,
                f"{leader_id} {'appended but did not commit' if result.appended else 'was not leader'}",
            )
        return result.committed

    async def _log_attempt(self, job_id: str, outcome: str) -> None:
        await self._logger.log(
            RaftDebug(
                message=f"Ledger replication attempt not committed: {outcome}; retrying",
                node_id=self._node_id,
                job_id=job_id,
            )
        )

_REHOMED = (
    JobGroupConsensus,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
