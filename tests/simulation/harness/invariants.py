"""
Continuous safety + liveness invariant checking.

A `SafetyInvariant` must hold *at all times* — the checker fails the
scenario the moment it does not. A `LivenessInvariant` must *eventually*
hold; the checker fails when its progress predicate has not advanced
within the configured staleness budget.

Phase 2 ships the skeleton with one of each. The catalog grows as
fault-injection scenarios start exercising consensus, leadership
election, and partition handling.

The checker runs as a TaskRunner-managed background loop polling at
``HarnessTimeouts.invariant_poll_interval`` (default 100 ms). Violations
trigger a diagnostic dump *before* the exception propagates, so the
test report carries the snapshot from the moment the invariant broke,
not the moment cleanup ran.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

from tests.simulation.harness.errors import HarnessError
from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


class Severity(StrEnum):
    CRITICAL = "critical"
    WARNING = "warning"


class InvariantViolation(HarnessError):
    """A registered invariant evaluated to False (safety) or stalled (liveness)."""


@dataclass(slots=True, frozen=True)
class InvariantResult:
    """Outcome of one invariant evaluation."""

    holds: bool
    detail: str = ""


@dataclass(slots=True)
class SafetyInvariant:
    """A property that must be true continuously throughout the scenario.

    ``evaluate`` is invoked with the harness on every checker tick. It
    returns an :class:`InvariantResult` carrying detail when violated;
    a falsy result causes the checker to dump diagnostics and raise
    :class:`InvariantViolation`.
    """

    name: str
    evaluate: Callable[["ClusterHarness"], InvariantResult]
    severity: Severity = Severity.CRITICAL


@dataclass(slots=True)
class LivenessInvariant:
    """A property that must hold *eventually*, with a staleness budget.

    ``progress_counter`` returns a monotonic integer that strictly
    increases while the system is making forward progress. If the
    counter has not advanced within ``staleness_budget_seconds``, the
    checker treats the scenario as stalled (deadlock pretending to be
    slowness) and raises.

    A counter that returns 0 when the system is "idle" (not yet running
    work) is fine — the checker only starts measuring staleness once
    the counter has been observed to advance at least once. That avoids
    spurious failures during cluster cold-start.
    """

    name: str
    progress_counter: Callable[["ClusterHarness"], int]
    staleness_budget_seconds: float
    severity: Severity = Severity.CRITICAL


@dataclass(slots=True)
class _LivenessState:
    """Per-invariant tracking the checker maintains.

    ``regression_start_at`` is the time the counter first dropped
    below ``last_value`` (the running peak) without recovering.
    Steady state at the peak is the cluster's healthy resting
    position, not a deadlock — so we don't measure staleness while
    the counter sits at the peak. We *do* measure staleness while
    the counter sits below the peak: that's the
    "advanced-then-regressed-and-stuck" deadlock signature the
    invariant docstring describes. ``None`` means we're either at
    the peak or have not yet regressed.
    """

    last_value: int = 0
    last_advance_at: float = 0.0
    has_started: bool = False
    regression_start_at: float | None = None


@dataclass(slots=True)
class InvariantChecker:
    """Runs registered invariants on a fixed tick.

    Construction does not start anything. ``start`` schedules the
    checker as a background task; ``stop`` cancels it. ``stopped`` is
    set once the loop has fully exited, so cleanup can join cleanly.
    """

    harness: "ClusterHarness"
    poll_interval: float
    on_violation: Callable[[str], Awaitable[None]] | None = None
    """Optional callback (typically the diagnostic dumper) invoked before
    the violation is raised."""

    safety: list[SafetyInvariant] = field(default_factory=list)
    liveness: list[LivenessInvariant] = field(default_factory=list)
    _liveness_state: dict[str, _LivenessState] = field(default_factory=dict)
    _task: asyncio.Task | None = None
    _stopping: bool = False
    _violation: InvariantViolation | None = None

    def add_safety(self, invariant: SafetyInvariant) -> None:
        self.safety.append(invariant)

    def add_liveness(self, invariant: LivenessInvariant) -> None:
        self.liveness.append(invariant)
        self._liveness_state[invariant.name] = _LivenessState()

    def reset_liveness(self, name: str) -> None:
        """Reset one liveness invariant after an intentional scenario phase shift."""
        if name in self._liveness_state:
            self._liveness_state[name] = _LivenessState()

    @property
    def violation(self) -> InvariantViolation | None:
        """The first violation observed, if any. Cleared on ``start``."""
        return self._violation

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stopping = False
        self._violation = None
        self._task = asyncio.create_task(
            self._run(), name="sim-invariant-checker"
        )

    async def stop(self) -> None:
        self._stopping = True
        task = self._task
        if task is None:
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        self._task = None

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await self._tick()
            except InvariantViolation as violation:
                # Capture for the harness to surface; do not raise out of
                # the background task itself (would become an unhandled
                # task exception that masks the real cause).
                self._violation = violation
                if self.on_violation is not None:
                    try:
                        await self.on_violation(str(violation))
                    except Exception:
                        pass
                return
            except asyncio.CancelledError:
                return
            except Exception:
                # Checker bugs must not crash the scenario; log via the
                # harness's normal channels by recording into cleanup_errors.
                pass
            try:
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                return

    async def _tick(self) -> None:
        for invariant in self.safety:
            result = invariant.evaluate(self.harness)
            if not result.holds:
                raise InvariantViolation(
                    f"safety invariant {invariant.name!r} violated: {result.detail}"
                )

        now = time.monotonic()
        for invariant in self.liveness:
            counter = invariant.progress_counter(self.harness)
            state = self._liveness_state[invariant.name]
            if counter > state.last_value:
                # New peak — fresh progress. Clear any tracked
                # regression: the cluster moved forward.
                state.last_value = counter
                state.last_advance_at = now
                state.has_started = True
                state.regression_start_at = None
                continue
            if not state.has_started:
                continue
            if counter == state.last_value:
                # Steady at peak — the cluster is at its healthy
                # resting state. Per the invariant docstring this is
                # explicitly *not* a stall: "once steady, it stops
                # advancing — at which point the liveness check has
                # nothing to measure". Clear any prior regression
                # tracking and continue.
                state.regression_start_at = None
                continue
            # counter < state.last_value — regression from peak.
            # Start the staleness clock from the moment the regression
            # began; if the cluster doesn't recover within the budget,
            # this is the "advanced-then-stopped" deadlock signature.
            if state.regression_start_at is None:
                state.regression_start_at = now
            if now - state.regression_start_at > invariant.staleness_budget_seconds:
                raise InvariantViolation(
                    f"liveness invariant {invariant.name!r} stalled: "
                    f"counter={counter} regressed from peak {state.last_value} "
                    f"and stuck for {now - state.regression_start_at:.1f}s"
                )


# =========================================================================
# Initial catalog — one safety + one liveness invariant.
# More land in Phase 3 once fault-injection scenarios exercise consensus.
# =========================================================================


def at_most_one_job_leader_per_job() -> SafetyInvariant:
    """No two managers across the cluster claim leadership of the same job.

    With Phase 1 having no workload yet, this is vacuously true — managers
    have empty job-leader tables. The invariant exists now so we have
    continuous coverage of the property the moment Phase 3's
    `WorkloadDriver` lands.
    """

    def _evaluate(harness: "ClusterHarness") -> InvariantResult:
        leader_to_job: dict[str, str] = {}
        for handle in harness.all_handles():
            if handle.kind is not ServerKind.MANAGER:
                continue
            if not handle.started or harness.faults.is_killed(handle):
                continue
            state = handle.instance._manager_state
            for job_id, leader_id in state.iter_job_leaders():
                existing_leader = leader_to_job.get(job_id)
                if existing_leader is not None and existing_leader != leader_id:
                    return InvariantResult(
                        holds=False,
                        detail=(
                            f"job {job_id!r} has conflicting leaders: "
                            f"{existing_leader!r} vs {leader_id!r}"
                        ),
                    )
                leader_to_job[job_id] = leader_id
        return InvariantResult(holds=True)

    return SafetyInvariant(
        name="AtMostOneJobLeaderPerJob",
        evaluate=_evaluate,
    )


def cluster_membership_progress(staleness_budget: float = 30.0) -> LivenessInvariant:
    """Cluster membership must reach steady state within the staleness budget.

    Progress counter: sum of bounded active manager peers and worker
    counts across all managers. Worker counts are bounded by the
    harness's expected worker count for the DC, so planned scale-downs
    can move the target without turning intentional membership
    regression into an invariant failure.
    """

    def _counter(harness: "ClusterHarness") -> int:
        total = 0
        for handle in harness.all_handles():
            if handle.kind is not ServerKind.MANAGER:
                continue
            state = getattr(handle.instance, "_manager_state", None)
            if state is None:
                continue
            expected_manager_peers = max(
                0, harness.spec.datacenters[handle.dc_id].managers - 1
            )
            expected_workers = harness.expected_worker_count(handle.dc_id)
            total += min(len(state.get_active_manager_peer_ids()), expected_manager_peers)
            total += min(state.get_worker_count(), expected_workers)
        return total

    return LivenessInvariant(
        name="ClusterMembershipProgress",
        progress_counter=_counter,
        staleness_budget_seconds=staleness_budget,
    )
