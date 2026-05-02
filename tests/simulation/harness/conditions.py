"""
Condition-driven waits for the simulation harness.

Replaces every hard-coded `asyncio.sleep` in scenarios with `wait_until`
predicates that finish as soon as the cluster is actually ready. On
timeout the waiter calls a diagnostic dump (Phase 2.2) so the test
report contains a snapshot of what was — and was not — true at the
moment of timeout, not just `TimeoutError: 30 s`.

Built-in predicates use only `ServerHandle.instance` attributes the
production servers already expose, so they read live state rather than
duplicating it.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tests.simulation.harness.errors import HarnessError
from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


Predicate = Callable[[], bool] | Callable[[], Awaitable[bool]]


class ConditionTimeoutError(HarnessError):
    """A `wait_until` predicate did not become True within the budget."""


@dataclass(slots=True)
class WaitContext:
    """Bookkeeping for a single `wait_until` call.

    Returned to scenarios that want to inspect how long stabilization
    actually took (useful for tuning timeouts and for liveness
    invariants in Phase 2.3).
    """

    description: str
    elapsed_seconds: float


async def wait_until(
    predicate: Predicate,
    timeout: float,
    poll: float = 0.25,
    description: str = "",
    on_fail: Callable[[], Awaitable[None]] | None = None,
) -> WaitContext:
    """Poll ``predicate`` until it returns True or ``timeout`` elapses.

    Returns a :class:`WaitContext` with the elapsed time on success.
    On timeout, calls ``on_fail`` (if provided) before raising
    :class:`ConditionTimeoutError`. ``on_fail`` is the harness's
    diagnostic dump in normal use — by running it before the exception
    propagates, the test report carries the cluster snapshot.

    Predicates may be sync or async; both forms are awaited uniformly.
    """
    start = time.monotonic()
    deadline = start + timeout
    while True:
        elapsed = time.monotonic() - start
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return WaitContext(description=description, elapsed_seconds=elapsed)
        if time.monotonic() >= deadline:
            if on_fail is not None:
                try:
                    await on_fail()
                except Exception:
                    # Diagnostic dump must never mask the original timeout.
                    pass
            raise ConditionTimeoutError(
                f"{description or 'predicate'} did not hold within {timeout}s "
                f"(polled every {poll}s)"
            )
        await asyncio.sleep(poll)


# =========================================================================
# Built-in predicates — composable with `wait_until`.
# Each returns a zero-arg predicate so scenarios can pass them directly.
# =========================================================================


def manager_has_n_peers(
    handle: ServerHandle, expected_peers: int
) -> Callable[[], bool]:
    """True once a manager has SWIM-confirmed the expected peer count.

    ``expected_peers`` does NOT include the manager itself. For a
    3-manager DC, each manager expects 2 SWIM-confirmed peers.

    Uses ``_active_manager_peer_ids`` because that is populated only when
    SWIM probes succeed and ``_on_peer_confirmed`` fires — i.e. the
    cluster is genuinely converged, not just registered. ``known``
    means "we've seen registration"; ``active`` means "we've probed and
    confirmed liveness." For a real cluster smoke, the active count is
    the correct gate; if it never reaches the expected count, that is a
    SWIM-tier defect (failure detection, leadership election, and
    cross-DC heartbeats all depend on this same path).
    """
    if handle.kind is not ServerKind.MANAGER:
        raise ValueError(
            f"manager_has_n_peers expects a MANAGER handle; got {handle.kind}"
        )

    def _predicate() -> bool:
        state = handle.instance._manager_state
        return len(state.get_active_manager_peer_ids()) >= expected_peers

    return _predicate


def manager_has_n_workers(
    handle: ServerHandle, expected_workers: int
) -> Callable[[], bool]:
    """True once a manager has registered at least the expected workers."""
    if handle.kind is not ServerKind.MANAGER:
        raise ValueError(
            f"manager_has_n_workers expects a MANAGER handle; got {handle.kind}"
        )

    def _predicate() -> bool:
        state = handle.instance._manager_state
        return state.get_worker_count() >= expected_workers

    return _predicate


def worker_subprocesses_alive(
    harness: "ClusterHarness", worker_handle: ServerHandle
) -> Callable[[], bool]:
    """True once the supervisor has snapshotted at least one subprocess PID.

    Drives the L1 lifecycle stabilization wait. The PID snapshot tick
    runs at 1 s by default, so this predicate flips ~1 s after the
    worker subprocess pool has spawned its first child.
    """
    if worker_handle.kind is not ServerKind.WORKER:
        raise ValueError(
            "worker_subprocesses_alive expects a WORKER handle"
        )

    def _predicate() -> bool:
        return bool(harness.supervisor.tracked_pids(worker_handle.node_id))

    return _predicate


def manager_is_leader(handle: ServerHandle) -> Callable[[], bool]:
    """True once a manager has won leader election for its DC.

    Job submission requires a known DC leader: the manager rejects
    submissions with ``Not DC leader, retry at leader: <hint>`` until
    its ``LocalLeaderElection`` completes pre-vote + election + lease
    update. For an L1 single-manager DC, that takes ``pre_vote_timeout
    + election_timeout``; for L2/L3 it includes broadcast/vote rounds.
    Use this predicate to gate any test that exercises the submit path
    so the test fails on a *real* dispatch defect rather than racing
    leader election.
    """
    if handle.kind is not ServerKind.MANAGER:
        raise ValueError(
            f"manager_is_leader expects a MANAGER handle; got {handle.kind}"
        )

    def _predicate() -> bool:
        return bool(handle.instance.is_leader())

    return _predicate


def dc_has_leader(handles: list[ServerHandle]) -> Callable[[], bool]:
    """True once any manager in the DC has been elected leader.

    For multi-manager DCs the test does not care *which* manager wins;
    only that at least one has, so the submit path can be exercised.
    """
    for handle in handles:
        if handle.kind is not ServerKind.MANAGER:
            raise ValueError(
                f"dc_has_leader expects MANAGER handles; got {handle.kind}"
            )

    def _predicate() -> bool:
        return any(handle.instance.is_leader() for handle in handles)

    return _predicate


def gate_cluster_formed(
    handles: list[ServerHandle], expected_peers: int
) -> Callable[[], bool]:
    """True once every gate has discovered the expected peer count."""
    for handle in handles:
        if handle.kind is not ServerKind.GATE:
            raise ValueError(
                f"gate_cluster_formed expects GATE handles; got {handle.kind}"
            )

    def _predicate() -> bool:
        for handle in handles:
            state = handle.instance._modular_state
            if state.get_active_peer_count() < expected_peers:
                return False
        return True

    return _predicate


def all_of(*predicates: Callable[[], bool]) -> Callable[[], bool]:
    """Compose: True iff every predicate returns True."""
    def _predicate() -> bool:
        return all(p() for p in predicates)
    return _predicate


def any_of(*predicates: Callable[[], bool]) -> Callable[[], bool]:
    """Compose: True if any predicate returns True."""
    def _predicate() -> bool:
        return any(p() for p in predicates)
    return _predicate
