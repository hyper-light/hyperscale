"""Transient datacenter-dispatch rejection (see dispatch_coordinator)."""

from hyperscale.distributed.reliability.retry_after_error import RetryAfterError


class TransientDispatchError(RetryAfterError):
    """A manager rejected a dispatched job for a transient reason —
    mid-election ("Not DC leader"), warming up ("not accepting jobs"),
    load shedding, and the rest of the shared
    ``hyperscale.distributed.protocol.transient_errors`` vocabulary.

    Raised (rather than returned) so the dispatch path's
    ``RetryExecutor`` treats the rejection like any other retryable
    failure and re-attempts with backoff; a rejection outside the
    transient vocabulary stays a terminal ``(False, error)`` result.

    ``retry_after_seconds`` is the manager's retry hint (0.0 when it gave
    none): when it next decides, for a datacenter electing its leader, and
    so on. ``RetryExecutor`` waits it out (a ``RetryAfterError``).
    """

    def __init__(self, error: str | None, retry_after_seconds: float = 0.0) -> None:
        # ``args`` stays the error alone (its text is the dispatch's error
        # message); ``RetryAfterError.__reduce__`` pickles the hint with it.
        super().__init__(error, retry_after_seconds)
