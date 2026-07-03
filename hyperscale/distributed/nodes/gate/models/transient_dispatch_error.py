"""Transient datacenter-dispatch rejection (see dispatch_coordinator)."""


class TransientDispatchError(Exception):
    """A manager rejected a dispatched job for a transient reason —
    mid-election ("Not DC leader"), warming up ("not accepting jobs"),
    load shedding, and the rest of the shared
    ``hyperscale.distributed.protocol.transient_errors`` vocabulary.

    Raised (rather than returned) so the dispatch path's
    ``RetryExecutor`` treats the rejection like any other retryable
    failure and re-attempts with backoff; a rejection outside the
    transient vocabulary stays a terminal ``(False, error)`` result.
    """
