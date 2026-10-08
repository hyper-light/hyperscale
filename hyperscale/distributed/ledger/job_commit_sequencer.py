from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, TypeVar

CommitOutcome = TypeVar("CommitOutcome")


class JobCommitSequencer:
    """Runs each job's commits in append order without serializing jobs.

    Replicated commits must reach the job's consensus group in the order
    their entries were appended (a replica applying ``JobAccepted``
    before ``JobCreated`` drops it), yet one job's slow replication must
    not hold up any other job. Each commit takes a turn in its job's
    chain: ``reserve`` (synchronous, called while the ledger lock fixes
    the append order) links the new turn behind the job's current tail,
    and ``run`` waits for the predecessor before committing.

    A turn always resolves, and never before its predecessor: a commit
    cancelled while waiting hands its place on only once the predecessor
    finishes. A job's chain entry is dropped when its last turn
    resolves, so state is bounded by jobs with commits in flight.
    """

    __slots__ = ("_tails",)

    def __init__(self) -> None:
        self._tails: dict[str, asyncio.Future[None]] = {}

    @property
    def in_flight_job_count(self) -> int:
        """Jobs with at least one unresolved commit turn."""
        return len(self._tails)

    def reserve(self, job_id: str) -> tuple[asyncio.Future[None] | None, asyncio.Future[None]]:
        """Take the next turn for ``job_id``: (predecessor, own turn)."""
        predecessor = self._tails.get(job_id)
        turn: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._tails[job_id] = turn
        return predecessor, turn

    async def run(
        self,
        job_id: str,
        predecessor: asyncio.Future[None] | None,
        turn: asyncio.Future[None],
        commit: Callable[[], Awaitable[CommitOutcome]],
    ) -> CommitOutcome:
        """Wait for ``predecessor``, then run ``commit`` in this turn."""
        try:
            if predecessor is not None:
                # Shielded: cancelling this waiter must not cancel the
                # predecessor's turn, which its own commit resolves.
                await asyncio.shield(predecessor)
            return await commit()
        finally:
            self._release(job_id, predecessor, turn)

    def _release(
        self,
        job_id: str,
        predecessor: asyncio.Future[None] | None,
        turn: asyncio.Future[None],
    ) -> None:
        if predecessor is None or predecessor.done():
            self._finish(job_id, turn)
            return
        predecessor.add_done_callback(lambda _: self._finish(job_id, turn))

    def _finish(self, job_id: str, turn: asyncio.Future[None]) -> None:
        if not turn.done():
            turn.set_result(None)
        if self._tails.get(job_id) is turn:
            del self._tails[job_id]
