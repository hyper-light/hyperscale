"""A datacenter refused a dispatched job for want of room (see dispatch_coordinator)."""

from hyperscale.distributed.reliability.retry_after_error import RetryAfterError


class DatacenterRefusedDispatchError(RetryAfterError):
    """The datacenter's leader refused a dispatched job it has no room for
    now -- a D-65 concurrency cap, or a D-67 quarantined job class -- with a
    retry hint (``JobAck.retry_after_seconds``) and an error outside the
    transient vocabulary.

    The answer is the datacenter's, not one manager's: its other managers
    would redirect to the same leader and hear the same refusal, and the
    manager is healthy -- it answered. Raised so the dispatch stops trying
    the datacenter's managers, counts no manager failure, and moves to a
    fallback datacenter. ``retry_after_seconds`` is the leader's hint.
    """
