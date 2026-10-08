from typing import NamedTuple


class ParsedCancelRequest(NamedTuple):
    """Normalized fields extracted from a cancel request.

    Both the AD-20 ``JobCancelRequest`` and the legacy ``CancelJob``
    wire formats are parsed into this shape so the ``cancel_job``
    handler consumes one consistent structure regardless of which
    format the sender used. ``callback_addr`` and
    ``unreachable_addrs`` are only ever populated by the newer
    ``JobCancelRequest`` path; the legacy path leaves them at
    ``None`` / empty.
    """

    job_id: str
    fence_token: int
    requester_id: str
    timestamp: float
    reason: str
    callback_addr: tuple[str, int] | None
    unreachable_addrs: frozenset[tuple[str, int]]
