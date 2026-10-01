"""
AD-38 REGIONAL replication of a manager's job ledger through per-job Raft.
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import msgspec

from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.raft.logging_models import RaftDebug
from hyperscale.distributed.raft.models import (
    LedgerProposal,
    LedgerProposalResult,
    RaftCommandType,
)
from hyperscale.distributed.raft.models.commands import RaftCommand
from hyperscale.distributed.raft.raft_node import HEARTBEAT_INTERVAL

if TYPE_CHECKING:
    from hyperscale.distributed.ledger.wal.wal_entry import WALEntry
    from hyperscale.distributed.raft import RaftConsensus
    from hyperscale.distributed.runtime import Clock
    from hyperscale.logging import Logger


class ManagerLedgerReplicator:
    """Makes a job-ledger WAL entry REGIONAL by committing it in the job's group.

    REGIONAL means a majority of the datacenter's managers hold the entry:
    exactly a Raft commit in the job's per-job group, whose members mirror
    committed entries into their ``JobLedgerReplica``. Only the group's
    Raft leader can append, and that member need not be the job's leader
    (AD-31 job leadership is SWIM-coordinated), so a non-leader forwards
    the entry to the group's leader.

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
        "_forward_timeout_seconds",
        "_clock",
        "_logger",
    )

    def __init__(
        self,
        consensus: "RaftConsensus",
        node_id: str,
        send_tcp: Callable[..., Awaitable[bytes | Exception | None]],
        forward_timeout_seconds: float,
        clock: "Clock",
        logger: "Logger",
    ) -> None:
        self._consensus = consensus
        self._node_id = node_id
        self._send_tcp = send_tcp
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
            _ledger_command(
                proposal.job_id, JobEventType(proposal.event_type), proposal.payload
            ),
        )
        return LedgerProposalResult(
            job_id=proposal.job_id,
            appended=index > 0,
            committed=committed,
        )

    async def _ensure_group(self, job_id: str) -> bool:
        """Join the job's group, campaigning at once if it never elected.

        The ledger's first entry for a job (``JobCreated``) usually comes
        before any other path created the group; campaigning immediately
        instead of waiting out a randomized election timeout makes the
        creator the likely first leader and saves that timeout on every
        job's first REGIONAL commit.
        """
        if not await self._consensus.create_job_raft(job_id):
            return False
        node = self._consensus.get_node(job_id)
        if node is not None and node.current_term == 0:
            await node.start_election()
        return node is not None

    async def _propose_via(self, leader_id: str, job_id: str, entry: "WALEntry") -> bool:
        if leader_id == self._node_id:
            committed, _ = await self._consensus.propose_command(
                job_id, _ledger_command(job_id, entry.event_type, entry.payload)
            )
            return committed

        if (leader_addr := self._consensus.member_address(leader_id)) is None:
            return False

        response = await self._send_tcp(
            leader_addr,
            "raft_ledger_proposal",
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
                message=f"Ledger REGIONAL attempt not committed: {outcome}; retrying",
                node_id=self._node_id,
                job_id=job_id,
            )
        )


def _ledger_command(job_id: str, event_type: JobEventType, payload: bytes) -> RaftCommand:
    return RaftCommand(
        command_type=RaftCommandType.LEDGER_APPEND,
        job_id=job_id,
        ledger_event_type=event_type,
        ledger_payload=payload,
    )
