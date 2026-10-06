"""
How one worker answered one workflow dispatch (AD-54, AD-44).
"""

from enum import Enum


class DispatchOutcome(Enum):
    """
    One dispatch plan's answer, which decides what a workflow no worker
    took costs:

    * ACCEPTED: the worker took the workflow.
    * NOT_READY: the worker refused for capacity (a readiness rejection).
    * UNROUTABLE: the pool offered a worker the registry no longer knows
      (a stale pool entry, purged).
    * WITHHELD: never sent -- the dispatcher is shutting down or the job is
      being cancelled.
    * UNREACHABLE: no answer -- a transport error, a timeout or an empty
      reply.
    * REJECTED: the worker refused the workflow itself.

    NOT_READY, UNROUTABLE and WITHHELD are waits: the workflow waits for
    capacity without spending retry budget. UNREACHABLE and REJECTED are
    failed deliveries, which spend it (``BUDGETED_DISPATCH_OUTCOMES``).
    """

    ACCEPTED = "accepted"
    NOT_READY = "not_ready"
    UNROUTABLE = "unroutable"
    WITHHELD = "withheld"
    UNREACHABLE = "unreachable"
    REJECTED = "rejected"


BUDGETED_DISPATCH_OUTCOMES: frozenset[DispatchOutcome] = frozenset(
    {DispatchOutcome.UNREACHABLE, DispatchOutcome.REJECTED}
)
