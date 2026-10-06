"""Wire model ``JobCancelRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobCancelRequest(Message):
    """
    Request to cancel a running job (AD-20).

    Can be sent from:
    - Client -> Gate (global cancellation across all DCs)
    - Client -> Manager (DC-local cancellation)
    - Gate -> Manager (forwarding client request)
    - Manager -> Worker (cancel specific workflows)

    The fence_token is used for consistency:
    - If provided, only cancel if the job's current fence token matches
    - This prevents cancelling a restarted job after a crash recovery
    """

    job_id: str  # Job to cancel
    requester_id: str  # Who requested cancellation (for audit)
    timestamp: float  # When cancellation was requested
    fence_token: int = 0  # Fence token for consistency (0 = ignore)
    reason: str = ""  # Optional cancellation reason
    # Client callback address for the async
    # ``job_cancellation_complete`` push. Under leader-failover the
    # new leader may not have inherited the callback from the old
    # leader's ``_broadcast_job_leadership`` — the broadcast is
    # fire-and-forget and can be dropped by the partition or a
    # racing leader kill. Piggybacking the callback on the cancel
    # request itself gives whichever manager processes the cancel
    # a definitive fallback that doesn't depend on cross-manager
    # state-sync ordering. Optional so clients that don't need the
    # async push (e.g., in-process cancel checks) can omit it.
    callback_addr: tuple[str, int] | None = None
    # Manager TCP addresses the client demonstrably could NOT reach
    # during this cancellation attempt (connection refused, timeout).
    # This is the client's ground truth about which managers are
    # dead — strictly fresher than any single manager's SWIM view,
    # which lags Raft DC-leader election after a leader kill. A
    # freshly-elected DC leader consults this set so it (a) never
    # redirects the client to an address the client already proved
    # is unreachable, and (b) treats a cached job-leader in this set
    # as grounds to take over job leadership immediately rather than
    # waiting for its own SWIM failure detector to catch up. Without
    # it, a cancel issued inside the SWIM-lag window after a
    # leader-failover bounces between managers that all still believe
    # the dead prior leader is alive, exhausts the client's redirect
    # budget, and fails with ``redirect cycles to already-tried
    # target``. Empty/omitted on the first attempt and for clients
    # that don't track reachability.
    unreachable_addrs: list[tuple[str, int]] | None = None
