"""
The state machine every per-job Raft group applies (AD-38): committed
job-ledger entries, mirrored into the member's ``JobLedgerReplica``.
"""

from typing import TYPE_CHECKING

import msgspec

from .logging_models import RaftError, RaftWarning
from .models import RaftLogEntry
from .models.ledger_append_command import LEDGER_APPEND_COMMAND, LedgerAppendCommand

if TYPE_CHECKING:
    from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
    from hyperscale.logging import Logger


class LedgerStateMachine:
    """Applies each committed ``LEDGER_APPEND`` entry of a node's job
    groups to its ledger replica, in commit order.

    Deterministic by construction: an entry's event and payload are all
    it applies (the replica's ``JobEventApplier`` is the one WAL recovery
    uses). An entry it cannot apply is logged and skipped rather than
    raised: the Raft log is immutable, and an exception would stop the
    tick loop that drives every job's group.
    """

    __slots__ = ("_ledger_replica", "_logger", "_node_id", "_decoder")

    def __init__(self, ledger_replica: "JobLedgerReplica", logger: "Logger", node_id: str) -> None:
        self._ledger_replica = ledger_replica
        self._logger = logger
        self._node_id = node_id
        self._decoder = msgspec.msgpack.Decoder(LedgerAppendCommand)

    async def apply(self, entry: RaftLogEntry) -> None:
        """Mirror one committed entry into the ledger replica."""
        if entry.command_type != LEDGER_APPEND_COMMAND:
            await self._logger.log(RaftWarning(
                message=f"Unknown command type: {entry.command_type}",
                node_id=self._node_id,
                job_id=entry.job_id,
            ))
            return
        try:
            command = self._decoder.decode(entry.command)
            self._ledger_replica.apply(entry.job_id, command.ledger_event_type, command.ledger_payload)
        except (msgspec.DecodeError, KeyError, TypeError, ValueError) as error:
            await self._logger.log(RaftError(
                message=f"Unreplayable ledger entry {entry.index}: {error!r}",
                node_id=self._node_id,
                job_id=entry.job_id,
                term=entry.term,
            ))

    def release_job(self, job_id: str) -> None:
        """Drop the replica's state for ``job_id``'s (destroyed) group."""
        self._ledger_replica.release(job_id)
