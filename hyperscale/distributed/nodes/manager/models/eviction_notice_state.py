"""
Bookkeeping for an outstanding worker-eviction notice obligation.
"""

from dataclasses import dataclass


@dataclass(slots=True)
class WorkerEvictionNoticeState:
    """Tracks the manager's obligation to tell a deregistered worker it
    was evicted.

    Created when a worker is deregistered by a failure/reap path (never
    by a plain re-registration overwrite or a sync mirror). Discharged
    when the worker acks the notice OR re-registers — whichever comes
    first. While outstanding, the notice is re-sent with capped
    exponential backoff so a worker that was wedged through the initial
    push (the usual reason it was evicted) still learns the truth when
    it recovers.
    """

    worker_id: str
    worker_tcp_addr: tuple[str, int]
    reason: str
    evicted_at: float
    last_notice_at: float
    notice_count: int = 0
