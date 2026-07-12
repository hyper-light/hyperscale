"""
StatusApplyOutcome — why a client-side status write did or didn't
apply.

The order guard rejecting a write is not one condition but two, and
they demand different handling:

* ``REJECTED_STALE`` — the update lost the ordering race (an older
  poll response arriving after a fresher push, a duplicate terminal).
  This is the guard working as designed; routine.
* ``REJECTED_UNKNOWN`` — the status is not in the lifecycle
  vocabulary at all. That is a PROTOCOL surprise (a newer node
  speaking vocabulary this client does not know, or corruption) and
  must be LOGGED by the caller, never silently dropped — a swallowed
  unknown status would blind the client to real state changes with no
  trace.
"""

from enum import Enum


class StatusApplyOutcome(Enum):
    APPLIED = "applied"
    REJECTED_STALE = "rejected_stale"
    REJECTED_UNKNOWN = "rejected_unknown"

    @property
    def applied(self) -> bool:
        return self is StatusApplyOutcome.APPLIED

    @property
    def unknown_vocabulary(self) -> bool:
        return self is StatusApplyOutcome.REJECTED_UNKNOWN
