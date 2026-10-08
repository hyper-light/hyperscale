from dataclasses import dataclass

from .message import Message


@dataclass(slots=True)
class JobStatusQuery(Message):
    """A job status read at a consistency level (AD-38 Part 8).

    ``consistency`` is a ``ReadConsistency`` value. For SESSION,
    ``observed_fence_token`` and ``observed_view_time`` are the newest view
    of the job the reader has seen (from an earlier answer's
    ``fence_token`` and ``view_time``): the answer is at least that new.
    For BOUNDED_STALENESS, ``max_staleness_seconds`` bounds the age of the
    view answered. ``forwarded`` marks a read a node passed on to the job's
    leader: it is answered where it lands, never passed on again.
    """

    job_id: str
    consistency: str = "eventual"
    observed_fence_token: int = 0
    observed_view_time: float = 0.0
    max_staleness_seconds: float = 0.0
    forwarded: bool = False
