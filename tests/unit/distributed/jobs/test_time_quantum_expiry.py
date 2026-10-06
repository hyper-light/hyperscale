"""
The protocol time-remainder epsilon contract (``protocol/time_quantum``),
pinned at both dispatch-tier deadline sites.

The frozen-instant livelock class exists whenever an EXPIRY PREDICATE
and a WAIT COMPUTATION derived from the same composed-float deadline
disagree about a sub-quantum remainder (observed live: 1.6e-11s): the
predicate says "not yet", the wait arms a timer the quantized clock
cannot honor, and the loop re-evaluates at the same instant forever.
The contract that kills the class: a remainder at or below
``TIME_REMAINDER_EPSILON_SECONDS`` IS expiry, applied by BOTH members
of each predicate/remaining pair — so these tests pin each member AND
the agreement invariant between them across the remainder spectrum.

Sites under contract:
* ``PendingWorkflow.is_retry_backoff_expired`` /
  ``remaining_retry_backoff_seconds`` (dispatch retry pacing)
* ``WorkerDispatchRoutingState.is_routable`` /
  ``remaining_cooldown_seconds`` (allocator routing cooldowns)
"""

import math

from hyperscale.distributed.jobs.worker_dispatch_routing_state import (
    WorkerDispatchRoutingState,
)
from hyperscale.distributed.models.jobs import PendingWorkflow
from hyperscale.distributed.protocol.time_quantum import (
    TIME_REMAINDER_EPSILON_SECONDS,
)

# The remainder spectrum the agreement invariant sweeps: float-artifact
# scale, both epsilon-boundary NEIGHBORS, and real waits. The exact
# boundary point is deliberately absent: these tests compose deadlines
# through the same float arithmetic production uses, so a remainder of
# exactly epsilon round-trips with ~1e-10 error at realistic monotonic
# magnitudes — the contract is only float-honest about the boundary's
# neighborhood, never the point itself.
_REMAINDER_SPECTRUM = (
    1.6e-11,  # the observed live artifact
    1e-9,
    TIME_REMAINDER_EPSILON_SECONDS * 0.5,  # inside the boundary: expiry
    TIME_REMAINDER_EPSILON_SECONDS * 2.0,  # outside: a real (tiny) wait
    1e-3,  # the wait-floor scale
    0.25,  # routing cooldown base
    1.0,  # backoff initial delay
)

_BACKOFF_DELAY_SECONDS = 4.0
_NOW = 1_000_000.0  # realistic monotonic magnitude (ulp ~1.2e-10 here)


def _pending_workflow_in_backoff(remaining_seconds: float) -> PendingWorkflow:
    """A ``PendingWorkflow`` whose retry backoff has ``remaining_seconds``
    left at ``_NOW``.

    Built bare: the two helpers under test read exactly
    ``failed_dispatch_attempts`` / ``last_dispatch_attempt`` /
    ``next_retry_delay`` and nothing else (same bare-construction
    precedent as ``test_manager_restart_truth``).
    """
    pending_workflow = object.__new__(PendingWorkflow)
    pending_workflow.failed_dispatch_attempts = 1
    pending_workflow.next_retry_delay = _BACKOFF_DELAY_SECONDS
    pending_workflow.last_dispatch_attempt = (
        _NOW - (_BACKOFF_DELAY_SECONDS - remaining_seconds)
    )
    return pending_workflow


def test_epsilon_sits_between_float_artifacts_and_real_delays() -> None:
    """The contract's load-bearing magnitude claim: epsilon must sit
    strictly above float-artifact scale at realistic monotonic
    magnitudes and strictly below the smallest real scheduling delay
    (the 1ms wait floors) — shrinking or growing it breaks one side."""
    assert math.ulp(1e7) < TIME_REMAINDER_EPSILON_SECONDS
    assert TIME_REMAINDER_EPSILON_SECONDS < 1e-3


def test_first_attempt_has_no_backoff() -> None:
    pending_workflow = object.__new__(PendingWorkflow)
    pending_workflow.failed_dispatch_attempts = 0
    pending_workflow.next_retry_delay = _BACKOFF_DELAY_SECONDS
    pending_workflow.last_dispatch_attempt = 0.0
    assert pending_workflow.is_retry_backoff_expired(_NOW) is True
    assert pending_workflow.remaining_retry_backoff_seconds(_NOW) == 0.0


def test_sub_epsilon_backoff_remainder_is_expiry() -> None:
    """The artifact regime: a positive remainder the clock cannot honor
    counts as expired on BOTH members — waiting on it was the livelock."""
    pending_workflow = _pending_workflow_in_backoff(1.6e-11)
    assert pending_workflow.is_retry_backoff_expired(_NOW) is True
    assert pending_workflow.remaining_retry_backoff_seconds(_NOW) == 0.0


def test_epsilon_boundary_neighborhood_splits_correctly() -> None:
    """Inside the boundary counts as expiry, outside stays a wait —
    tested at half/double epsilon so the deadline round-trip's ~1e-10
    composition error (the same arithmetic production performs) cannot
    flip either side."""
    inside_boundary = _pending_workflow_in_backoff(
        TIME_REMAINDER_EPSILON_SECONDS * 0.5
    )
    assert inside_boundary.is_retry_backoff_expired(_NOW) is True
    assert inside_boundary.remaining_retry_backoff_seconds(_NOW) == 0.0

    outside_boundary = _pending_workflow_in_backoff(
        TIME_REMAINDER_EPSILON_SECONDS * 2.0
    )
    assert outside_boundary.is_retry_backoff_expired(_NOW) is False
    assert outside_boundary.remaining_retry_backoff_seconds(_NOW) > 0.0


def test_real_backoff_remainder_still_paces() -> None:
    pending_workflow = _pending_workflow_in_backoff(1.0)
    assert pending_workflow.is_retry_backoff_expired(_NOW) is False
    remaining = pending_workflow.remaining_retry_backoff_seconds(_NOW)
    assert math.isclose(remaining, 1.0, rel_tol=1e-9)


def test_backoff_predicate_and_remaining_always_agree() -> None:
    """THE invariant that kills the livelock class: expiry says True
    exactly when remaining says 0.0 — eligibility and waiting can never
    disagree about any remainder."""
    for remainder_seconds in _REMAINDER_SPECTRUM:
        pending_workflow = _pending_workflow_in_backoff(remainder_seconds)
        expired = pending_workflow.is_retry_backoff_expired(_NOW)
        remaining = pending_workflow.remaining_retry_backoff_seconds(_NOW)
        assert expired == (remaining == 0.0), (
            remainder_seconds,
            expired,
            remaining,
        )
        if not expired:
            # A schedulable wait, never a sub-quantum artifact.
            assert remaining > TIME_REMAINDER_EPSILON_SECONDS


def _routing_state_suspended_for(remaining_seconds: float) -> WorkerDispatchRoutingState:
    """A routing state whose cooldown has ``remaining_seconds`` left at
    ``_NOW`` (suspension composed the same way production composes it:
    an absolute float deadline against a monotonic now)."""
    routing_state = WorkerDispatchRoutingState(
        worker_id="worker-under-test",
        base_cooldown_seconds=0.25,
        max_cooldown_seconds=30.0,
    )
    routing_state.suspended_until = _NOW + remaining_seconds
    return routing_state


def test_sub_epsilon_cooldown_remainder_is_routable() -> None:
    routing_state = _routing_state_suspended_for(1.6e-11)
    assert routing_state.is_routable(_NOW) is True
    assert routing_state.remaining_cooldown_seconds(_NOW) == 0.0


def test_real_cooldown_remainder_still_suspends() -> None:
    routing_state = _routing_state_suspended_for(0.25)
    assert routing_state.is_routable(_NOW) is False
    remaining = routing_state.remaining_cooldown_seconds(_NOW)
    assert math.isclose(remaining, 0.25, rel_tol=1e-9)


def test_cooldown_predicate_and_remaining_always_agree() -> None:
    for remainder_seconds in _REMAINDER_SPECTRUM:
        routing_state = _routing_state_suspended_for(remainder_seconds)
        routable = routing_state.is_routable(_NOW)
        remaining = routing_state.remaining_cooldown_seconds(_NOW)
        assert routable == (remaining == 0.0), (
            remainder_seconds,
            routable,
            remaining,
        )
        if not routable:
            assert remaining > TIME_REMAINDER_EPSILON_SECONDS


def test_never_suspended_worker_is_routable() -> None:
    routing_state = WorkerDispatchRoutingState(
        worker_id="worker-under-test",
        base_cooldown_seconds=0.25,
        max_cooldown_seconds=30.0,
    )
    assert routing_state.is_routable(_NOW) is True
    assert routing_state.remaining_cooldown_seconds(_NOW) == 0.0
