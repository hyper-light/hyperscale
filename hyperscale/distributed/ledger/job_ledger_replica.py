from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from .events.event_type import JobEventType
from .job_event_applier import JobEventApplier
from .job_state import JobState


class JobLedgerReplica:
    """A consensus member's copy of the job-ledger events its groups committed.

    Every member of a job's group applies the job's committed ledger
    entries here, in commit order, through the same ``JobEventApplier``
    that WAL recovery uses — so a replica's state is exactly what the
    owning ledger's recovery would rebuild from the same events. The raw
    event history is kept too: a member taking the job over adopts it
    into its own ledger verbatim.

    A job's state and history are released with its consensus group, so
    the replica holds only jobs whose groups this member still runs.
    """

    __slots__ = ("_event_applier", "_states", "_histories")

    def __init__(self) -> None:
        self._event_applier = JobEventApplier()
        self._states: dict[str, JobState] = {}
        self._histories: dict[str, list[tuple[JobEventType, bytes]]] = {}

    @property
    def job_count(self) -> int:
        """Jobs with replicated history held here."""
        return len(self._histories)

    def apply(self, job_id: str, event_type: JobEventType, payload: bytes) -> None:
        """Apply one committed ledger event for ``job_id``.

        Raises:
            KeyError: an event type with no applier -- a replica must not
                silently diverge from the ledger it mirrors.
        """
        self._event_applier.apply_event(event_type, payload, self._states)
        self._histories.setdefault(job_id, []).append((event_type, payload))

    def job_state(self, job_id: str) -> JobState | None:
        """The replicated state of ``job_id``, if this member holds it."""
        return self._states.get(job_id)

    def history(self, job_id: str) -> tuple[tuple[JobEventType, bytes], ...]:
        """``job_id``'s committed events in commit order (empty if unknown)."""
        return tuple(self._histories.get(job_id, ()))

    def states(self) -> Mapping[str, JobState]:
        """Read-only view of every replicated job state."""
        return MappingProxyType(self._states)

    def release(self, job_id: str) -> None:
        """Drop ``job_id``'s state and history (its group is gone)."""
        self._states.pop(job_id, None)
        self._histories.pop(job_id, None)
