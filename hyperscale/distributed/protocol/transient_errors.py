"""
Transient-rejection vocabulary — the protocol-level contract for
"rejected now, retry and it may succeed" responses.

Managers and gates reject submissions/dispatches with structured
``JobAck(accepted=False, error=...)`` responses whose error text
identifies *why*: mid-election ("Not DC leader..."), warming up
("Manager is initializing, not accepting jobs"), load shedding, rate
limiting, leadership failover. Every hop that receives such an ack —
the client submitting to a manager or gate, AND the gate dispatching to
a datacenter manager — must classify these the same way: transient
rejections are retried with backoff, permanent ones fail fast.

This lived in ``hyperscale.distributed.nodes.client.config`` while the
client was the only classifier; the gate's dispatch path classifying
differently (treating every rejection as terminal) is exactly the bug
that motivated promoting it here. Keep the substrings lowercase — the
match is case-insensitive against the whole error text.
"""

TRANSIENT_ERRORS = frozenset({
    "syncing",
    "not ready",
    "election in progress",
    "no leader",
    # Quorum unavailability ("No quorum available; rejecting job
    # submission") is the AD-3 write-safety rejection: it clears when
    # the election completes or the partition heals.
    "no quorum",
    "split brain",
    "rate limit",
    "overload",
    "too many",
    "server busy",
    # Leader-redirect responses produced by a manager that knows it
    # is not the leader but couldn't resolve the leader's address
    # (e.g. cluster mid-election, peer heartbeats not yet arrived).
    # The robust path is for the manager to populate ``leader_addr``
    # from peer state — see ``ManagerServer._resolve_dc_leader_addr``.
    # When the manager genuinely cannot resolve, this transient
    # classification lets the client round-robin to another target.
    "not dc leader",
    "not job leader",
    # AD-39: the node's clock is fenced (offset beyond the bound against a
    # quorum of peers); it clears when the clock is back in agreement,
    # and meanwhile another node takes the work.
    "clock fenced",
    # Gate-forwarded cancel where no DC could confirm because manager
    # leadership is mid-failover. The gate stamps its aggregate cancel
    # error with ``GateCancellationHandler._CANCEL_RETRYABLE_MARKER``
    # ("cancellation pending leader transition") so the client retries
    # across its time budget until leadership reconverges. Keep this
    # substring in sync with that marker.
    "leader transition",
    # Manager lifecycle-state rejections ("Manager is initializing, not
    # accepting jobs" / draining states): the manager exists and will
    # (or a peer will) accept once its startup or handoff completes.
    "not accepting jobs",
    # Gate rejections while a datacenter is in its pre-first-heartbeat
    # warmup window (JobAck error "initializing" from the gate's
    # submission handler): capacity is seconds away, retry.
    "initializing",
})


def is_transient_rejection(error: str | None) -> bool:
    """Whether ``error`` names a rejection worth retrying with backoff."""
    if not error:
        return False
    error_lower = error.lower()
    return any(marker in error_lower for marker in TRANSIENT_ERRORS)
