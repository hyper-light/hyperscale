"""Every datacenter a job was offered refused it for want of room (see dispatch_coordinator)."""

from hyperscale.distributed.reliability.retry_after_error import RetryAfterError


class DatacentersWithoutRoomError(RetryAfterError):
    """No datacenter took a job, and every one that refused it said it had
    no room for it now (D-65 caps, D-67 breaker) with a retry hint.

    The gate already accepted the job, so it holds it: the job's placement
    is retried -- by the dispatch's ``RetryExecutor``, no sooner than the
    smallest hint (``retry_after_seconds``) -- until the job's own timeout.
    ``datacenters`` are the refusing datacenters, for the failure the
    timeout makes of it.
    """

    def __init__(self, message: str, retry_after_seconds: float, datacenters: tuple[str, ...]) -> None:
        super().__init__(message, retry_after_seconds)
        self.datacenters = datacenters
